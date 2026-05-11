from __future__ import annotations

import asyncio
from dataclasses import dataclass
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.schedulers.base import STATE_RUNNING
from apscheduler.schedulers import SchedulerNotRunningError
from aiogram import Bot, Dispatcher, Router, types
from aiogram.exceptions import TelegramUnauthorizedError
from aiogram.utils.keyboard import InlineKeyboardBuilder

from aiohttp.web_runner import AppRunner, TCPSite

from .awg_backend import (
    bootstrap_protected_peers,
    check_awg_container,
    cleanup_expired_subscriptions,
    expired_subscriptions_worker,
    get_orphan_awg_peers,
    reconcile_active_awg_state,
    reconcile_pending_awg_state,
    run_docker,
    sync_traffic_counters,
)
from .config import (
    ADMIN_ID,
    API_TOKEN,
    BROADCAST_BATCH_DELAY_SECONDS,
    BROADCAST_BATCH_SIZE,
    BROADCAST_RUNNING_STALE_SECONDS,
    CLEANUP_INTERVAL_SECONDS,
    DB_PATH,
    DOCKER_CONTAINER,
    NODE_API_PORT,
    PENDING_KEY_TTL_SECONDS,
    RECONCILIATION_INTERVAL_SECONDS,
    WG_INTERFACE,
    logger,
    maybe_set_support_username,
)
from .content_settings import get_text
from .database import (
    claim_next_broadcast_job,
    cleanup_stale_pending_keys,
    close_shared_db,
    complete_broadcast_job,
    db_health_info,
    ensure_db_ready,
    fail_stale_running_broadcast_jobs,
    get_broadcast_recipients,
    get_shared_db,
    get_subscriptions_expiring_within,
    has_subscription_notification,
    mark_subscription_notification_sent,
    update_broadcast_job_progress,
    write_audit_log,
)
from .handlers_admin import router as admin_router
from .handlers_user import router as user_router
from .middlewares import DuplicateCallbackGuardMiddleware, DuplicateMessageGuardMiddleware, RateLimitMiddleware
from .network_policy import denylist_should_refresh, denylist_sync, refresh_denylist
from .node_api import create_node_api_app, start_node_api_server, stop_node_api_server
from .payments import payment_recovery_worker
from .payments import router as payments_router
from .ui_constants import is_admin_callback_data
from .ui_constants import CB_SHOW_BUY_MENU
from .workers import WorkerPool, WorkerSpec
from .helpers import format_iso_to_moscow


# Node-scoped scheduler and background tasks
_scheduler: AsyncIOScheduler | None = None
_background_tasks: set[asyncio.Task] = set()
_task_logger = logging.getLogger(__name__)


def _schedule_node_scoped(job_func, trigger: str, node_id: str, **kwargs):
    """Регистрирует job только если текущая нода отвечает за его выполнение."""
    from .config import NODE_ID as config_node_id
    current_node = getattr(config_node_id, "NODE_ID", "default") if hasattr(config_node_id, "NODE_ID") else "default"
    # Try to get NODE_ID from config module directly
    try:
        from . import config as cfg
        current_node = getattr(cfg, "NODE_ID", "default")
    except Exception:
        current_node = "default"
    
    if current_node != node_id and node_id != "all":
        return
    if _scheduler is None:
        _task_logger.warning("Scheduler not initialized, skipping job registration for %s", job_func.__name__)
        return
    _scheduler.add_job(job_func, trigger, id=f"{node_id}_{job_func.__name__}", replace_existing=True, **kwargs)


async def _safe_background_wrapper(coro, task_name: str):
    try:
        await coro
    except asyncio.CancelledError:
        _task_logger.info(f"[{task_name}] cancelled")
    except Exception as e:
        _task_logger.error(f"[{task_name}] unhandled: {e}", exc_info=True)


def run_background(coro, task_name: str):
    task = asyncio.create_task(_safe_background_wrapper(coro, task_name), name=task_name)
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def on_startup():
    """Initialize database pool, scheduler and background tasks."""
    global _scheduler
    _scheduler = AsyncIOScheduler(timezone="UTC")
    await get_shared_db()
    _scheduler.start()
    # Пример привязки к ноде:
    from .config import EGRESS_DENYLIST_REFRESH_MINUTES, NODE_ID, SYNC_ENABLED
    from .network_policy import refresh_denylist
    from .workers import run_autobackup, sync_agent_loop
    _schedule_node_scoped(refresh_denylist, "interval", node_id=NODE_ID, minutes=EGRESS_DENYLIST_REFRESH_MINUTES)
    _schedule_node_scoped(run_autobackup, "cron", node_id=NODE_ID, hour=3, minute=0)
    if SYNC_ENABLED:
        run_background(sync_agent_loop(), "sync_agent")
    _task_logger.info("Startup complete. Scheduler and background tasks initialized.")


async def on_shutdown():
    """Graceful shutdown: cancel pending tasks and stop scheduler."""
    _task_logger.info("Shutting down: cancelling background tasks...")
    for task in _background_tasks:
        task.cancel()
    await asyncio.gather(*_background_tasks, return_exceptions=True)
    if _scheduler is not None:
        _scheduler.shutdown(wait=False)
    await close_shared_db()
    _task_logger.info("Shutdown complete.")


@dataclass(frozen=True)
class RuntimeSettings:
    cleanup_interval_seconds: int
    reconciliation_interval_seconds: int
    broadcast_batch_delay_seconds: float
    broadcast_batch_size: int
    broadcast_running_stale_seconds: int


@dataclass(frozen=True)
class RuntimeDeps:
    bot: Bot
    settings: RuntimeSettings


dp = Dispatcher()
fallback_router = Router()


dp.message.middleware(RateLimitMiddleware(ttl_seconds=2.0, max_hits=6))
dp.callback_query.middleware(RateLimitMiddleware(ttl_seconds=2.0, max_hits=8))
dp.message.middleware(DuplicateMessageGuardMiddleware())
dp.callback_query.middleware(DuplicateCallbackGuardMiddleware())


@fallback_router.callback_query()
async def fallback_callback(cb: types.CallbackQuery) -> None:
    if is_admin_callback_data(cb.data):
        await cb.answer("Нет доступа", show_alert=True)
        return
    await cb.answer(await get_text("unknown_callback_action"))


dp.include_router(payments_router)
dp.include_router(admin_router)
dp.include_router(user_router)
dp.include_router(fallback_router)


async def process_one_broadcast_job(deps: RuntimeDeps) -> bool:
    claimed = await claim_next_broadcast_job()
    if not claimed:
        return False

    job_id, admin_id, text, total, _segment = claimed
    try:
        cursor = 0
        while True:
            recipients = await get_broadcast_recipients(job_id, cursor, deps.settings.broadcast_batch_size)
            if not recipients:
                break

            batch_delivered = 0
            batch_failed = 0
            for uid in recipients:
                try:
                    await deps.bot.send_message(uid, text, disable_web_page_preview=True)
                    batch_delivered += 1
                except Exception as send_error:
                    batch_failed += 1
                    logger.warning("Broadcast job=%s user_id=%s error=%s", job_id, uid, send_error)

            cursor += len(recipients)
            await update_broadcast_job_progress(job_id, batch_delivered, batch_failed, cursor)
            await asyncio.sleep(deps.settings.broadcast_batch_delay_seconds)

        _, done_delivered, done_failed = await complete_broadcast_job(job_id, "finished")
    except Exception as error:
        error_message = f"{type(error).__name__}: {error}"[:1000]
        logger.exception("Broadcast processing failed for job_id=%s: %s", job_id, error)
        try:
            await complete_broadcast_job(job_id, "failed", error_message)
        except Exception as complete_error:
            logger.exception("Failed to mark broadcast job_id=%s as failed: %s", job_id, complete_error)
        await write_audit_log(admin_id, "broadcast_failed", f"job_id={job_id}; error={error_message}")
        try:
            await deps.bot.send_message(
                admin_id,
                (
                    "⚠️ <b>Рассылка завершилась с ошибкой</b>\n\n"
                    f"job_id=<code>{job_id}</code>\n"
                    f"Ошибка: <code>{error_message}</code>"
                ),
                parse_mode="HTML",
            )
        except Exception as notify_error:
            logger.warning("Failed to notify admin about broadcast failure job_id=%s: %s", job_id, notify_error)
        return True

    try:
        await write_audit_log(
            admin_id,
            "broadcast",
            f"job_id={job_id}; total={total}; delivered={done_delivered}; failed={done_failed}",
        )
    except Exception as audit_error:
        logger.warning("Broadcast finished but audit write failed job_id=%s: %s", job_id, audit_error)
    try:
        await deps.bot.send_message(
            admin_id,
            (
                "📢 <b>Рассылка завершена</b>\n\n"
                f"job_id=<code>{job_id}</code>\n"
                f"✅ Доставлено: <b>{done_delivered}</b>\n"
                f"❌ Ошибок: <b>{done_failed}</b>"
            ),
            parse_mode="HTML",
        )
    except Exception as notify_error:
        logger.warning("Broadcast finished but admin notify failed job_id=%s: %s", job_id, notify_error)
    return True


async def _payments_worker(deps: RuntimeDeps) -> None:
    try:
        while True:
            try:
                repaired = await payment_recovery_worker(deps.bot)
                if repaired:
                    logger.info("Payment recovery: успешно обработано %s зависших платежей", repaired)
            except Exception as error:
                logger.exception("Payment recovery worker error: %s", error)
            await asyncio.sleep(15)
    except asyncio.CancelledError:
        logger.info("Payment recovery worker cancelled")
        raise


async def _reconciliation_worker(deps: RuntimeDeps) -> None:
    try:
        while True:
            try:
                stats = await reconcile_pending_awg_state()
                if any(stats.values()):
                    logger.info("Reconciliation stats: %s", stats)
            except Exception as error:
                logger.exception("Reconciliation worker error: %s", error)
            await asyncio.sleep(deps.settings.reconciliation_interval_seconds)
    except asyncio.CancelledError:
        logger.info("Reconciliation worker cancelled")
        raise


async def _traffic_sync_worker() -> None:
    try:
        while True:
            try:
                await sync_traffic_counters()
            except Exception as error:
                logger.exception("Traffic sync worker error: %s", error)
            await asyncio.sleep(45)
    except asyncio.CancelledError:
        logger.info("Traffic sync worker cancelled")
        raise


async def _broadcast_worker(deps: RuntimeDeps) -> None:
    try:
        while True:
            try:
                recovered = await fail_stale_running_broadcast_jobs(deps.settings.broadcast_running_stale_seconds)
                if recovered:
                    logger.warning("Broadcast recovery: marked stale running jobs as failed: %s", recovered)
                processed = await process_one_broadcast_job(deps)
                if not processed:
                    await asyncio.sleep(1)
                    continue
            except Exception as error:
                logger.exception("Broadcast worker error: %s", error)
                await asyncio.sleep(2)
    except asyncio.CancelledError:
        logger.info("Broadcast worker cancelled")
        raise


async def _denylist_refresh_worker() -> None:
    try:
        while True:
            try:
                if await denylist_should_refresh():
                    await denylist_sync(run_docker)
            except Exception as error:
                logger.exception("Denylist refresh worker error: %s", error)
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("Denylist refresh worker cancelled")
        raise


async def _startup_checks(bot: Bot) -> None:
    logger.info("Запуск бота")
    logger.info("DB_PATH=%s", DB_PATH)
    logger.info("DOCKER_CONTAINER=%s WG_INTERFACE=%s", DOCKER_CONTAINER, WG_INTERFACE)

    try:
        await bot.get_me()
    except TelegramUnauthorizedError as error:
        logger.error("Telegram API вернул Unauthorized. Проверь API_TOKEN в .env и перевыпусти токен в BotFather при необходимости.")
        raise RuntimeError("Неверный API_TOKEN") from error

    await ensure_db_ready()
    await get_shared_db()

    try:
        marked_pending = await cleanup_stale_pending_keys(PENDING_KEY_TTL_SECONDS)
        if marked_pending:
            logger.warning("Помечено stale pending-ключей для repair при старте: %s", marked_pending)
    except Exception as error:
        logger.exception("Ошибка маркировки stale pending-ключей: %s", error)

    try:
        await check_awg_container()
        logger.info("Контейнер и интерфейс AWG доступны")
    except Exception as error:
        logger.exception("AWG недоступен: %s", error)
        raise RuntimeError(f"AWG недоступен: {error}") from error

    try:
        admin_chat = await bot.get_chat(ADMIN_ID)
        maybe_set_support_username(getattr(admin_chat, "username", None))
    except Exception as error:
        logger.info("Не удалось автоопределить username администратора: %s", error)

    try:
        await bootstrap_protected_peers()
    except Exception as error:
        logger.exception("Ошибка bootstrap protected peers: %s", error)

    try:
        active_sync = await reconcile_active_awg_state()
        if active_sync["restored"] or active_sync["failed"]:
            logger.info("Active reconcile stats: %s", active_sync)
    except Exception as error:
        logger.exception("Ошибка восстановления active peers: %s", error)

    try:
        touched = await sync_traffic_counters()
        if touched:
            logger.info("Traffic counters synced at startup: %s", touched)
    except Exception as error:
        logger.exception("Ошибка стартовой синхронизации трафика: %s", error)

    try:
        db_info = await db_health_info()
        orphan_count = len(await get_orphan_awg_peers())
        logger.info(
            "Проверка состояния: db_exists=%s, schema_ready=%s, runtime_ready=%s, instance_integrity=%s, valid_keys=%s, orphan_peers=%s",
            db_info["exists"],
            db_info.get("schema_ready"),
            db_info.get("runtime_ready"),
            db_info.get("instance_integrity", {}).get("state"),
            db_info["valid_keys_count"],
            orphan_count,
        )
        if db_info.get("instance_integrity", {}).get("state") == "critical":
            logger.error(
                "КРИТИЧНО: нарушена целостность инстанса: %s",
                "; ".join(str(item) for item in db_info.get("instance_integrity", {}).get("issues", [])),
            )
    except Exception as error:
        logger.exception("Ошибка стартовой диагностики: %s", error)

    try:
        cleaned = await cleanup_expired_subscriptions()
        logger.info("Стартовая очистка завершена. Очищено просроченных: %s", cleaned)
    except Exception as error:
        logger.exception("Ошибка стартовой очистки: %s", error)


async def _notify_expiring_subscriptions(bot: Bot) -> None:
    reminder_specs = (
        (72, "3d_before", "⏰ Напоминание: подписка истекает примерно через 3 дня."),
        (24, "1d_before", "⏰ Напоминание: подписка истекает примерно через 1 день."),
    )
    kb = InlineKeyboardBuilder()
    kb.button(text="Продлить подписку", callback_data=CB_SHOW_BUY_MENU)
    for hours, kind, intro in reminder_specs:
        rows = await get_subscriptions_expiring_within(hours)
        for user_id, sub_until in rows:
            if await has_subscription_notification(user_id, sub_until, kind):
                continue
            try:
                await bot.send_message(
                    user_id,
                    f"{intro}\nОкончание: {format_iso_to_moscow(sub_until)}",
                    reply_markup=kb.as_markup(),
                )
                await mark_subscription_notification_sent(user_id, sub_until, kind)
            except Exception as error:
                logger.warning("Не удалось отправить напоминание kind=%s user_id=%s: %s", kind, user_id, error)


async def main() -> None:
    bot = Bot(token=API_TOKEN)
    scheduler = AsyncIOScheduler(timezone="UTC")
    deps = RuntimeDeps(
        bot=bot,
        settings=RuntimeSettings(
            cleanup_interval_seconds=CLEANUP_INTERVAL_SECONDS,
            reconciliation_interval_seconds=RECONCILIATION_INTERVAL_SECONDS,
            broadcast_batch_delay_seconds=BROADCAST_BATCH_DELAY_SECONDS,
            broadcast_batch_size=BROADCAST_BATCH_SIZE,
            broadcast_running_stale_seconds=BROADCAST_RUNNING_STALE_SECONDS,
        ),
    )
    worker_pool = WorkerPool()
    
    # Создаём Node API приложение
    node_api_app = create_node_api_app()

    try:
        await _startup_checks(bot)
        scheduler.add_job(_notify_expiring_subscriptions, "interval", minutes=30, kwargs={"bot": bot}, id="expiring-reminders", replace_existing=True)
        scheduler.start()
        
        # Запускаем Node API сервер параллельно с ботом
        # Функция теперь возвращает фактический порт (может отличаться от NODE_API_PORT если был занят)
        actual_node_api_port = await start_node_api_server(node_api_app, NODE_API_PORT)
        
        # Обновляем конфигурацию с фактическим портом для использования в других частях
        from . import config as config_module
        if actual_node_api_port != NODE_API_PORT:
            logger.warning("NODE_API_PORT изменён с %d на %d из-за занятости порта", NODE_API_PORT, actual_node_api_port)
            # Сохраняем фактический порт в конфиг для использования в smokecheck и других функциях
            config_module.update_node_api_port(actual_node_api_port)
        
        worker_pool.start(
            [
                WorkerSpec(
                    "expired_subscriptions",
                    lambda: expired_subscriptions_worker(deps.settings.cleanup_interval_seconds),
                ),
                WorkerSpec("payment_recovery", lambda: _payments_worker(deps)),
                WorkerSpec("reconciliation", lambda: _reconciliation_worker(deps)),
                WorkerSpec("broadcast", lambda: _broadcast_worker(deps)),
                WorkerSpec("traffic_sync", _traffic_sync_worker),
                WorkerSpec("denylist_refresh", _denylist_refresh_worker),
            ]
        )
        await dp.start_polling(bot)
    finally:
        try:
            if scheduler.state == STATE_RUNNING:
                scheduler.shutdown(wait=False)
        except SchedulerNotRunningError:
            pass
        except Exception as shutdown_error:
            logger.warning("Не удалось корректно остановить scheduler: %s", shutdown_error)
        await worker_pool.stop()
        await close_shared_db()
        await stop_node_api_server(node_api_app)
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
