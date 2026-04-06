from pathlib import Path
from datetime import datetime, timedelta
import ipaddress
import re

import config

from aiogram import F, Router, types
from aiogram import Bot
from aiogram.filters import BaseFilter, Command, CommandObject

from awg_backend import (
    check_awg_container, count_free_ip_slots, delete_user_everywhere,
    delete_user_device, get_awg_peers, get_orphan_awg_peers, issue_subscription, reconcile_active_awg_state,
    reconcile_pending_awg_state, reissue_user_device, revoke_user_access, run_docker, sync_traffic_counters,
)
from config import (
    ADMIN_COMMAND_COOLDOWN_SECONDS,
    ADMIN_ID,
    AWG_HELPER_POLICY_PATH,
    DOCKER_CONTAINER,
    WG_INTERFACE,
    logger,
    save_env_value,
    set_stars_price,
)
from database import (
    clear_pending_admin_action, clear_pending_broadcast, create_broadcast_job, create_promo_code, db_health_info, disable_promo_code, fetchall, fetchone, fetchval,
    get_payment_summary_by_charge_id,
    get_latest_user_payment_summary,
    get_user_device_traffic_summary,
    get_user_total_traffic_bytes,
    list_promo_codes, list_text_overrides,
    get_metric, get_pending_jobs_stats, get_recovery_lag_seconds,
    get_pending_admin_action, get_pending_broadcast, get_recent_audit, get_referral_admin_stats, get_referral_summary, get_user_keys, get_user_meta, normalize_promo_code, pop_pending_admin_action,
    reset_text_override, set_app_setting, set_pending_admin_action, set_pending_broadcast, set_text_override, write_audit_log,
)
from helpers import escape_html, format_tg_username, get_status_text, utc_now_naive
from device_activity import render_device_activity_line
from traffic import format_bytes_compact, render_device_traffic_line
from keyboards import (
    get_admin_confirm_kb, get_admin_inline_kb, get_admin_maintenance_kb, get_admin_payments_kb, get_admin_price_confirm_kb, get_admin_prices_kb, get_admin_promocodes_kb,
    get_admin_simple_back_kb, get_broadcast_cancel_kb, get_broadcast_confirm_kb, get_open_user_card_kb,
    get_admin_network_policy_kb, get_admin_denylist_kb,
    get_admin_service_settings_kb, get_admin_text_override_item_kb, get_admin_text_overrides_kb, get_admin_add_days_confirm_kb,
)
from ui_constants import (
    BTN_ADMIN, CB_ADMIN_BACK_MAIN, CB_ADMIN_BROADCAST,
    CB_ADMIN_COMMANDS, CB_ADMIN_FIND_CHARGE, CB_ADMIN_HEALTH, CB_ADMIN_LAST_PAYMENT, CB_ADMIN_LIST, CB_ADMIN_MAINTENANCE, CB_ADMIN_MAINTENANCE_OFF, CB_ADMIN_MAINTENANCE_ON,
    CB_ADMIN_MAINTENANCE_REFRESH, CB_ADMIN_OPEN_USER_CARD_PREFIX, CB_ADMIN_PAYMENTS, CB_ADMIN_PRICE_CANCEL, CB_ADMIN_PRICE_EDIT_30, CB_ADMIN_PRICE_EDIT_7,
    CB_ADMIN_PRICE_EDIT_90, CB_ADMIN_PRICE_SAVE, CB_ADMIN_PRICES, CB_ADMIN_PROMOCODES, CB_ADMIN_PROMO_CREATE, CB_ADMIN_PROMO_DISABLE, CB_ADMIN_PROMO_LIST, CB_ADMIN_REFERRALS,
    CB_ADMIN_SERVICE_SETTINGS, CB_ADMIN_SERVICE_SUPPORT, CB_ADMIN_SERVICE_DOWNLOAD, CB_ADMIN_SERVICE_REFERRAL_TOGGLE,
    CB_ADMIN_SERVICE_INVITEE_BONUS, CB_ADMIN_SERVICE_INVITER_BONUS, CB_ADMIN_SERVICE_TORRENT_TOGGLE,
    CB_ADMIN_TEXT_OVERRIDES, CB_ADMIN_TEXT_START, CB_ADMIN_TEXT_BUY_MENU, CB_ADMIN_TEXT_RENEW_MENU, CB_ADMIN_TEXT_SUPPORT,
    CB_ADMIN_TEXT_RESET_PREFIX, CB_ADMIN_TEXT_SET_PREFIX,
    CB_ADMIN_REFRESH_HEALTH, CB_ADMIN_REFRESH_REFERRALS, CB_ADMIN_STATS, CB_ADMIN_SYNC,
    CB_ADMIN_NETWORK_POLICY, CB_ADMIN_NET_DENYLIST, CB_ADMIN_NET_SYNC_NOW,
    CB_ADMIN_DENYLIST_TOGGLE, CB_ADMIN_DENYLIST_MODE_SOFT, CB_ADMIN_DENYLIST_MODE_STRICT,
    CB_ADMIN_DENYLIST_VIEW_DOMAINS, CB_ADMIN_DENYLIST_VIEW_CIDRS, CB_ADMIN_DENYLIST_REPLACE_DOMAINS, CB_ADMIN_DENYLIST_REPLACE_CIDRS, CB_ADMIN_DENYLIST_SYNC,
    CB_BROADCAST_CANCEL, CB_BROADCAST_CONFIRM,
    CB_ADMIN_USERS_PAGE_PREFIX, CB_ADMIN_MANAGE_USER_PREFIX, CB_ADMIN_ADD_DAYS_PREFIX,
    CB_ADMIN_RETRY_ACTIVATION_PREFIX,
    CB_ADMIN_DEVICE_DELETE_PREFIX, CB_ADMIN_DEVICE_REISSUE_PREFIX,
    CB_ADMIN_REVOKE_PREFIX, CB_ADMIN_DELETE_PREFIX, CB_CONFIRM_REVOKE, CB_CANCEL_REVOKE, CB_CONFIRM_DELETE_USER,
    CB_CANCEL_DELETE_USER, CB_CONFIRM_DEVICE_DELETE,
    CB_CANCEL_DEVICE_DELETE, CB_CONFIRM_DEVICE_REISSUE, CB_CANCEL_DEVICE_REISSUE,
    CB_CONFIRM_ADD_DAYS, CB_CANCEL_ADD_DAYS,
)
from config_validate import read_helper_policy
from network_policy import denylist_sync, parse_cidrs, policy_metrics
from content_settings import TEXT_DEFAULTS, get_setting, get_text, validate_text_template
from payments import manual_retry_activation

router = Router()
admin_command_rate_limit: dict[str, object] = {}
ADMIN_USERS_PAGE_SIZE = 10
ADMIN_MANUAL_COMMANDS: tuple[tuple[str, str], ...] = (
    ("/health", "быстрая проверка готовности"),
    ("/sync_awg", "сверка ключей AWG, трафика и диагностика БД"),
    ("/stats", "краткая статистика"),
    ("/users", "короткий список пользователей"),
    ("/audit", "последние события"),
    ("/ref_stats", "сводка по рефералам"),
    ("/send TEXT", "рассылка (осторожно)"),
    ("/finduser QUERY", "поиск пользователя по id или username"),
    ("/payinfo USER_ID", "краткая сводка по последнему платежу"),
    ("/findpay CHARGE_ID", "поиск платежа по telegram_payment_charge_id"),
    ("/give USER_ID DAYS", "выдать/продлить доступ вручную"),
    ("/promo_create CODE DAYS [MAX]", "создать промокод"),
    ("/promo_list", "краткий список промокодов"),
    ("/promo_disable CODE", "отключить промокод"),
    ("/revoke USER_ID", "отключить доступ вручную (осторожно)"),
    ("/maintenance_status", "статус блокировки новых покупок"),
    ("/maintenance_on", "включить блокировку новых покупок"),
    ("/maintenance_off", "выключить блокировку новых покупок"),
    ("/netpolicy", "сводка по сетевой политике"),
    ("/denylist_status", "текущий статус denylist"),
    ("/denylist_sync", "принудительная синхронизация denylist"),
)
BROADCAST_INPUT_ACTION_KEY = "broadcast_input"
PRICE_INPUT_ACTION_KEY = "price_input"
PRICE_CONFIRM_ACTION_KEY = "price_confirm"
PAYMENT_CHARGE_INPUT_ACTION_KEY = "payment_charge_lookup_input"
PAYMENT_USER_INPUT_ACTION_KEY = "payment_user_lookup_input"
PROMO_CREATE_INPUT_ACTION_KEY = "promo_create_input"
PROMO_DISABLE_INPUT_ACTION_KEY = "promo_disable_input"
DENYLIST_DOMAINS_INPUT_ACTION_KEY = "denylist_domains_input"
DENYLIST_CIDRS_INPUT_ACTION_KEY = "denylist_cidrs_input"
SERVICE_SUPPORT_INPUT_ACTION_KEY = "service_support_input"
SERVICE_DOWNLOAD_INPUT_ACTION_KEY = "service_download_input"
SERVICE_INVITEE_BONUS_INPUT_ACTION_KEY = "service_invitee_bonus_input"
SERVICE_INVITER_BONUS_INPUT_ACTION_KEY = "service_inviter_bonus_input"
TEXT_OVERRIDE_INPUT_ACTION_KEY = "text_override_input"
ADD_DAYS_CONFIRM_ACTION_KEY = "add_days_confirm"
TEXT_OVERRIDE_ALLOWED_KEYS = {"start", "buy_menu", "renew_menu", "support_contact"}
TEXT_OVERRIDE_CALLBACK_KEY_MAP = {
    CB_ADMIN_TEXT_START: "start",
    CB_ADMIN_TEXT_BUY_MENU: "buy_menu",
    CB_ADMIN_TEXT_RENEW_MENU: "renew_menu",
    CB_ADMIN_TEXT_SUPPORT: "support_contact",
}
PRICE_TARGETS = {
    CB_ADMIN_PRICE_EDIT_7: ("STARS_PRICE_7_DAYS", "7 дней"),
    CB_ADMIN_PRICE_EDIT_30: ("STARS_PRICE_30_DAYS", "30 дней"),
    CB_ADMIN_PRICE_EDIT_90: ("STARS_PRICE_90_DAYS", "90 дней"),
}


def _render_admin_prices_text() -> str:
    return (
        "💸 <b>Цены</b>\n\n"
        f"7 дней — {config.STARS_PRICE_7_DAYS}⭐\n"
        f"30 дней — {config.STARS_PRICE_30_DAYS}⭐\n"
        f"90 дней — {config.STARS_PRICE_90_DAYS}⭐"
    )


def _parse_price_input(raw_text: str) -> int | None:
    value_text = raw_text.strip()
    if not value_text.isdigit():
        return None
    value = int(value_text)
    if value <= 0:
        return None
    return value


def _build_broadcast_preview(raw_text: str) -> str:
    preview = raw_text.strip()
    if len(preview) > 500:
        preview = f"{preview[:500]}…"
    return escape_html(preview)


async def _guard_admin_callback(cb: types.CallbackQuery, *, require_message: bool = False) -> bool:
    if cb.from_user.id != ADMIN_ID:
        await cb.answer("Нет доступа", show_alert=True)
        return False
    if require_message and not cb.message:
        await cb.answer("Сообщение недоступно", show_alert=True)
        return False
    return True


def _cleanup_admin_rate_limit(now) -> None:
    stale = [key for key, dt in admin_command_rate_limit.items() if (now - dt).total_seconds() > 3600]
    for key in stale:
        admin_command_rate_limit.pop(key, None)


def admin_command_limited(action: str, actor_id: int = ADMIN_ID) -> bool:
    now = utc_now_naive()
    _cleanup_admin_rate_limit(now)
    key = f"{actor_id}:{action}"
    last = admin_command_rate_limit.get(key)
    admin_command_rate_limit[key] = now
    return bool(last and (now - last).total_seconds() < ADMIN_COMMAND_COOLDOWN_SECONDS)


class IsAdmin(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return bool(message.from_user and message.from_user.id == ADMIN_ID)


class HasPendingBroadcastInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        pending_action = await get_pending_admin_action(ADMIN_ID, BROADCAST_INPUT_ACTION_KEY)
        return bool(pending_action)


class HasPendingPriceInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        pending_action = await get_pending_admin_action(ADMIN_ID, PRICE_INPUT_ACTION_KEY)
        return bool(pending_action)


class HasPendingPaymentLookupInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        pending_charge = await get_pending_admin_action(ADMIN_ID, PAYMENT_CHARGE_INPUT_ACTION_KEY)
        pending_user = await get_pending_admin_action(ADMIN_ID, PAYMENT_USER_INPUT_ACTION_KEY)
        return bool(pending_charge or pending_user)


class HasPendingPromoInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        pending_create = await get_pending_admin_action(ADMIN_ID, PROMO_CREATE_INPUT_ACTION_KEY)
        pending_disable = await get_pending_admin_action(ADMIN_ID, PROMO_DISABLE_INPUT_ACTION_KEY)
        return bool(pending_create or pending_disable)


class HasPendingNetworkPolicyInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        keys = (
            DENYLIST_DOMAINS_INPUT_ACTION_KEY,
            DENYLIST_CIDRS_INPUT_ACTION_KEY,
        )
        for key in keys:
            if await get_pending_admin_action(ADMIN_ID, key):
                return True
        return False


class HasPendingServiceSettingsInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        keys = (
            SERVICE_SUPPORT_INPUT_ACTION_KEY,
            SERVICE_DOWNLOAD_INPUT_ACTION_KEY,
            SERVICE_INVITEE_BONUS_INPUT_ACTION_KEY,
            SERVICE_INVITER_BONUS_INPUT_ACTION_KEY,
        )
        for key in keys:
            if await get_pending_admin_action(ADMIN_ID, key):
                return True
        return False


class HasPendingTextOverrideInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        return bool(await get_pending_admin_action(ADMIN_ID, TEXT_OVERRIDE_INPUT_ACTION_KEY))


async def _clear_network_policy_pending() -> None:
    for key in (
        DENYLIST_DOMAINS_INPUT_ACTION_KEY,
        DENYLIST_CIDRS_INPUT_ACTION_KEY,
    ):
        await clear_pending_admin_action(ADMIN_ID, key)


async def _clear_service_settings_pending() -> None:
    for key in (
        SERVICE_SUPPORT_INPUT_ACTION_KEY,
        SERVICE_DOWNLOAD_INPUT_ACTION_KEY,
        SERVICE_INVITEE_BONUS_INPUT_ACTION_KEY,
        SERVICE_INVITER_BONUS_INPUT_ACTION_KEY,
    ):
        await clear_pending_admin_action(ADMIN_ID, key)
    await clear_pending_admin_action(ADMIN_ID, TEXT_OVERRIDE_INPUT_ACTION_KEY)


async def notify_user_subscription_granted(bot: Bot, user_id: int, days: int, new_until) -> bool:
    try:
        await bot.send_message(
            user_id,
            (
                "🎁 <b>Вам выдан доступ</b>\n\n"
                f"⏳ <b>Срок:</b> +{days} дн.\n"
                f"📅 <b>Действует до:</b> {new_until.strftime('%d.%m.%Y %H:%M')}\n\n"
                "🔑 Подключение доступно в разделе <b>Подключение</b>."
            ),
            parse_mode="HTML",
        )
        return True
    except Exception as notify_error:
        logger.warning("Не удалось уведомить пользователя %s о выдаче доступа: %s", user_id, notify_error)
        return False


async def build_awg_sync_text() -> str:
    report = await run_awg_sync_report()
    return _render_awg_sync_report_text(report)


def _format_awg_sync_step(name: str, result: dict[str, object], *, keys: tuple[str, ...], label_map: dict[str, str] | None = None) -> str:
    label_map = label_map or {}
    status = str(result.get("status") or "ok")
    if status != "ok":
        error_text = str(result.get("error") or "неизвестно")
        return f"• <b>{name}:</b> ошибка — <code>{escape_html(error_text)}</code>"
    stats = result.get("stats")
    if isinstance(stats, dict):
        parts = []
        for key in keys:
            if key in stats:
                parts.append(f"{label_map.get(key, key)}={stats[key]}")
        detail = ", ".join(parts) if parts else "ok"
        return f"• <b>{name}:</b> {escape_html(detail)}"
    value = result.get("value")
    return f"• <b>{name}:</b> {escape_html(str(value))}"


def _format_awg_sync_error(error: Exception) -> str:
    return str(error).strip().replace("\n", " ")[:180] or error.__class__.__name__


async def run_awg_sync_report() -> dict[str, object]:
    report: dict[str, object] = {
        "pending": {"status": "ok", "stats": {}},
        "active": {"status": "ok", "stats": {}},
        "traffic": {"status": "ok", "value": 0},
        "db_health": {"status": "ok", "value": {}},
        "orphans": {"status": "ok", "value": []},
    }
    try:
        report["pending"] = {"status": "ok", "stats": await reconcile_pending_awg_state()}
    except Exception as error:
        logger.exception("AWG sync: pending reconcile failed: %s", error)
        report["pending"] = {"status": "error", "error": _format_awg_sync_error(error)}
    try:
        report["active"] = {"status": "ok", "stats": await reconcile_active_awg_state()}
    except Exception as error:
        logger.exception("AWG sync: active reconcile failed: %s", error)
        report["active"] = {"status": "error", "error": _format_awg_sync_error(error)}
    try:
        report["traffic"] = {"status": "ok", "value": await sync_traffic_counters()}
    except Exception as error:
        logger.exception("AWG sync: traffic sync failed: %s", error)
        report["traffic"] = {"status": "error", "error": _format_awg_sync_error(error)}
    try:
        report["db_health"] = {"status": "ok", "value": await db_health_info()}
    except Exception as error:
        logger.exception("AWG sync: db health failed: %s", error)
        report["db_health"] = {"status": "error", "error": _format_awg_sync_error(error)}
    try:
        report["orphans"] = {"status": "ok", "value": await get_orphan_awg_peers()}
    except Exception as error:
        logger.exception("AWG sync: orphan peers check failed: %s", error)
        report["orphans"] = {"status": "error", "error": _format_awg_sync_error(error)}
    return report


def _render_awg_sync_report_text(report: dict[str, object]) -> str:
    pending = report.get("pending") if isinstance(report.get("pending"), dict) else {"status": "error", "error": "bad pending"}
    active = report.get("active") if isinstance(report.get("active"), dict) else {"status": "error", "error": "bad active"}
    traffic = report.get("traffic") if isinstance(report.get("traffic"), dict) else {"status": "error", "error": "bad traffic"}
    db_block = report.get("db_health") if isinstance(report.get("db_health"), dict) else {"status": "error", "error": "bad db_health"}
    orphan_block = report.get("orphans") if isinstance(report.get("orphans"), dict) else {"status": "error", "error": "bad orphans"}
    lines = [
        "🔄 <b>Проверка и синхронизация AWG</b>",
        "",
        _format_awg_sync_step(
            "Сверка ожидающих ключей",
            pending,
            keys=("activated", "deleted", "marked_manual", "awg_removed"),
            label_map={
                "activated": "активировано",
                "deleted": "удалено",
                "marked_manual": "помечено вручную",
                "awg_removed": "удалено из AWG",
            },
        ),
        _format_awg_sync_step(
            "Сверка активных ключей",
            active,
            keys=("restored", "already_present", "failed", "skipped_invalid_secret"),
            label_map={
                "restored": "восстановлено",
                "already_present": "уже есть",
                "failed": "ошибок",
                "skipped_invalid_secret": "пропущено битых секретов",
            },
        ),
    ]
    if traffic.get("status") == "ok":
        lines.append(f"• <b>Синхронизация трафика:</b> обновлено={traffic.get('value', 0)}")
    else:
        lines.append(f"• <b>Синхронизация трафика:</b> ошибка — <code>{escape_html(str(traffic.get('error') or 'неизвестно'))}</code>")
    if db_block.get("status") == "ok":
        db_info = db_block.get("value") if isinstance(db_block.get("value"), dict) else {}
        lines.append(
            "• <b>Состояние БД:</b> "
            f"файл БД={'да' if db_info.get('exists') else 'нет'}, "
            f"таблица keys={'да' if db_info.get('keys_table_exists') else 'нет'}, "
            f"обязательные колонки={'да' if db_info.get('has_required_columns') else 'нет'}, "
            f"валидных ключей={db_info.get('valid_keys_count', 0)}"
        )
    else:
        lines.append(f"• <b>Состояние БД:</b> ошибка — <code>{escape_html(str(db_block.get('error') or 'неизвестно'))}</code>")
    if orphan_block.get("status") == "ok":
        orphans = orphan_block.get("value")
        orphan_count = len(orphans) if isinstance(orphans, list) else 0
        lines.append(f"• <b>Потерянные пиры:</b> количество={orphan_count}")
    else:
        lines.append(f"• <b>Потерянные пиры:</b> ошибка — <code>{escape_html(str(orphan_block.get('error') or 'неизвестно'))}</code>")
    return "\n".join(lines)


async def build_stats_text() -> str:
    total_users = await fetchval("SELECT COUNT(*) FROM users")
    total_with_sub = await fetchval("SELECT COUNT(*) FROM users WHERE sub_until != '0'")
    active_users = await fetchval("SELECT COUNT(*) FROM users WHERE sub_until > ?", (utc_now_naive().isoformat(),))
    total_keys = await fetchval("SELECT COUNT(*) FROM keys")
    new_24h = await fetchval(
        "SELECT COUNT(*) FROM users WHERE created_at >= ?",
        ((utc_now_naive() - timedelta(days=1)).isoformat(),),
    )
    free_slots = await count_free_ip_slots()
    orphans = await get_orphan_awg_peers()
    return (
        "📊 <b>Статистика</b>\n\n"
        f"👥 Всего пользователей: <b>{total_users}</b>\n"
        f"🟢 Активных подписок: <b>{active_users}</b>\n"
        f"🗃 Записей с sub_until != 0: <b>{total_with_sub}</b>\n"
        f"🔑 Всего ключей в БД: <b>{total_keys}</b>\n"
        f"🆕 Новых за 24ч: <b>{new_24h}</b>\n"
        f"🧩 Свободных IP: <b>{free_slots}</b>\n"
        f"👻 Потерянных peer: <b>{len(orphans)}</b>"
    )


async def build_ref_stats_text() -> str:
    stats = await get_referral_admin_stats()
    recent = "\n".join([f"• invitee={r[0]} inviter={r[1]} pay={r[2]}" for r in stats["recent"]]) or "—"
    top = "\n".join([f"• inviter={row[0]} rewards={row[1]}" for row in stats["top"]]) or "—"
    total_bonus_days = int(stats["total_bonus_days"])
    return (
        "🎁 <b>Сводка по рефералам</b>\n\n"
        f"pending=<b>{stats['pending']}</b>\n"
        f"rewarded=<b>{stats['rewarded']}</b>\n"
        f"total_bonus_days=<b>{total_bonus_days}</b>\n\n"
        f"<b>Последние начисления</b>\n{recent}\n\n"
        f"<b>Топ пригласивших</b>\n{top}"
    )




def _smoke_status_line(name: str, state: str, detail: str) -> str:
    icon = {"ok": "✅", "warning": "⚠️", "failed": "❌"}.get(state, "⚪")
    return f"{icon} {name}: {detail}"


def _hint_for_awg_target_error(error: str) -> str:
    lowered = error.lower()
    if "invalid helper policy json" in lowered or "helper policy parse failed" in lowered:
        return (
            "исправь JSON в /etc/awg-bot-helper.json, проверь container/interface "
            "и перезапусти vpn-bot.service"
        )
    if "not configured" in lowered or "missing" in lowered:
        return "проверь цель AWG в .env и перезапусти сервис"
    return "проверь контейнер, helper и сервис бота"


def _hint_for_helper_policy_error(error: str) -> str:
    lowered = error.lower()
    if "parse failed" in lowered or "json object" in lowered:
        return (
            "исправь JSON в /etc/awg-bot-helper.json "
            "(ожидается {\"container\":\"<container>\",\"interface\":\"<interface>\"}), "
            "проверь container/interface и перезапусти vpn-bot.service"
        )
    return "проверь путь и доступ к политике helper"


def _safe_policy_error_suffix(error: str, limit: int = 160) -> str:
    compact = " ".join(str(error).split())
    if len(compact) <= limit:
        return compact
    return f"{compact[:limit - 1]}…"


async def run_runtime_smokecheck() -> dict[str, object]:
    checks: list[dict[str, str]] = []

    missing_env = []
    if not DOCKER_CONTAINER:
        missing_env.append("DOCKER_CONTAINER")
    if not WG_INTERFACE:
        missing_env.append("WG_INTERFACE")
    if not AWG_HELPER_POLICY_PATH:
        missing_env.append("AWG_HELPER_POLICY_PATH")
    if missing_env:
        checks.append(
            {
                "name": "Конфигурация",
                "state": "failed",
                "detail": f"не хватает: {', '.join(missing_env)}",
                "hint": "дополни .env и перезапусти сервис",
            }
        )
    else:
        checks.append({"name": "Конфигурация", "state": "ok", "detail": "готово", "hint": ""})

    db_info = await db_health_info()
    schema_ready = db_info.get("schema_ready")
    if schema_ready is None:
        schema_ready = bool(db_info.get("is_healthy"))

    if schema_ready:
        checks.append({"name": "База данных (schema_ready)", "state": "ok", "detail": "готово", "hint": ""})
    else:
        checks.append(
            {
                "name": "База данных (schema_ready)",
                "state": "failed",
                "detail": "схема БД не готова",
                "hint": "проверь БД вручную: init/migrations/права",
            }
        )

    integrity = db_info.get("instance_integrity") if isinstance(db_info.get("instance_integrity"), dict) else {}
    if not integrity:
        integrity = {"state": "ok" if bool(db_info.get("is_healthy")) else "unknown", "issues": []}
    integrity_state = str(integrity.get("state") or "unknown")
    integrity_issues = integrity.get("issues") if isinstance(integrity.get("issues"), list) else []
    if integrity_state == "critical":
        details = "; ".join(str(item) for item in integrity_issues if str(item).strip())
        checks.append(
            {
                "name": "Целостность инстанса",
                "state": "failed",
                "detail": details or "обнаружены критичные проблемы целостности",
                "hint": "проверь backup/restore и ENCRYPTION_SECRET; состояние runtime_ready=false",
            }
        )
    elif integrity_state == "ok":
        checks.append({"name": "Целостность инстанса", "state": "ok", "detail": "готово", "hint": ""})
    else:
        checks.append(
            {
                "name": "Целостность инстанса",
                "state": "warning",
                "detail": "статус не определён",
                "hint": "проверь БД вручную",
            }
        )

    try:
        await check_awg_container()
        checks.append({"name": "Подключение к AWG", "state": "ok", "detail": "доступно", "hint": ""})
    except Exception as e:
        checks.append(
            {
                "name": "Подключение к AWG",
                "state": "failed",
                "detail": f"ошибка ({str(e)[:120]})",
                "hint": _hint_for_awg_target_error(str(e)),
            }
        )

    if AWG_HELPER_POLICY_PATH and DOCKER_CONTAINER and WG_INTERFACE:
        policy_container, policy_interface, policy_error = read_helper_policy(Path(AWG_HELPER_POLICY_PATH))
        if policy_error:
            detail = policy_error
            if "parse failed:" in policy_error:
                parser_error = policy_error.split("parse failed:", 1)[1].strip()
                detail = (
                    "ошибка чтения политики helper: /etc/awg-bot-helper.json содержит неверный JSON"
                    + (f" ({escape_html(_safe_policy_error_suffix(parser_error))})" if parser_error else "")
                )
            checks.append(
                {
                    "name": "Политика helper",
                    "state": "failed",
                    "detail": detail,
                    "hint": _hint_for_helper_policy_error(policy_error),
                }
            )
        elif policy_container != DOCKER_CONTAINER or policy_interface != WG_INTERFACE:
            checks.append(
                {
                    "name": "Политика helper",
                    "state": "warning",
                    "detail": (
                        "расхождение: "
                        f"env={DOCKER_CONTAINER}/{WG_INTERFACE}, "
                        f"policy={policy_container}/{policy_interface}"
                    ),
                    "hint": "синхронизируй политику helper с .env",
                }
            )
        else:
            checks.append({"name": "Политика helper", "state": "ok", "detail": "готово", "hint": ""})

    failed = [c for c in checks if c["state"] == "failed"]
    warnings = [c for c in checks if c["state"] == "warning"]
    if failed:
        overall = "failed"
    elif warnings:
        overall = "warning"
    else:
        overall = "ok"

    next_hint = "готово к работе"
    for item in checks:
        if item["state"] != "ok" and item.get("hint"):
            next_hint = item["hint"]
            break
    if overall == "failed" and next_hint == "готово к работе":
        next_hint = "обнаружены критичные проблемы — исправь ошибки выше и перезапусти vpn-bot.service"

    return {"overall": overall, "checks": checks, "hint": next_hint}


async def build_runtime_smokecheck_text() -> str:
    report = await run_runtime_smokecheck()
    overall = str(report["overall"])
    overall_label = {"ok": "ГОТОВО", "warning": "ЧАСТИЧНО", "failed": "ОШИБКА"}.get(overall, "НЕИЗВЕСТНО")
    lines = [
        "🧪 <b>Проверка готовности</b>",
        "",
        f"Итог: <b>{overall_label}</b>",
    ]
    for check in report["checks"]:
        lines.append(_smoke_status_line(str(check["name"]), str(check["state"]), str(check["detail"])))
    lines.append("")
    lines.append(f"➡️ Следующий шаг: <b>{report['hint']}</b>")
    return "\n".join(lines)


async def build_health_text() -> str:
    stats = await get_pending_jobs_stats()
    lag = await get_recovery_lag_seconds()
    helper_failures = await get_metric("awg_helper_failures")
    policy_stats = await policy_metrics()
    rate_drop_total = await get_metric("rate_limit_dropped_total")
    rate_drop_message = await get_metric("rate_limit_dropped_message")
    rate_drop_callback = await get_metric("rate_limit_dropped_callback")
    rate_buckets = await get_metric("rate_limit_active_buckets")
    denylist_enabled = int(await get_setting("EGRESS_DENYLIST_ENABLED", int) or 0)
    denylist_mode = await get_setting("EGRESS_DENYLIST_MODE", str) or "soft"
    denylist_block = "\n".join(_denylist_history_block(enabled=denylist_enabled, metrics=policy_stats))
    return (
        "🩺 <b>Отчёт о состоянии</b>\n\n"
        f"Получено задач: <b>{stats['received']}</b>\n"
        f"Задач в выпуске: <b>{stats['provisioning']}</b>\n"
        f"Требуют проверки: <b>{stats['needs_repair']}</b>\n"
        f"Застряли на ручной обработке: <b>{stats['stuck_manual']}</b>\n"
        f"Задержка восстановления: <b>{lag}</b>\n"
        f"Ошибки helper-сервиса: <b>{helper_failures}</b>\n"
        f"Denylist: <b>{_bool_on_off(denylist_enabled)}</b> · режим: <b>{escape_html(str(denylist_mode))}</b>\n"
        f"{denylist_block}\n"
        f"Ограничение запросов: отклонено всего: <b>{rate_drop_total}</b>\n"
        f"Ограничение запросов: отклонено сообщений: <b>{rate_drop_message}</b>\n"
        f"Ограничение запросов: отклонено кнопок: <b>{rate_drop_callback}</b>\n"
        f"Ограничение запросов: активные корзины: <b>{rate_buckets}</b>"
    )


def _bool_on_off(value: int | bool) -> str:
    return "включено" if int(value) == 1 else "выключено"




def _sync_status_text(value: int | str | None) -> str:
    try:
        return "успешно" if int(str(value or 0)) == 1 else "ошибка"
    except Exception:
        return "не было"


def _sync_time_or_ne_bilo(value: int | str | None) -> str:
    try:
        ts = int(str(value or 0))
        if ts <= 0:
            return "не было"
        return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return "не было"


def _denylist_history_block(*, enabled: int, metrics: dict[str, int]) -> list[str]:
    last_sync_ok = int(metrics.get("denylist_last_sync_ok", 0))
    last_sync_ts = int(metrics.get("denylist_last_sync_ts", 0))
    entries = int(metrics.get("denylist_entries", 0))
    errors = int(metrics.get("denylist_errors", 0))
    last_clear_ok = int(metrics.get("denylist_last_clear_ok", 0))
    if enabled == 0:
        if last_clear_ok == 1:
            current_state_line = "Текущее состояние: <b>denylist выключен, синхронизация сейчас не выполняется</b>"
            current_entries_line = "Текущих записей denylist: <b>0</b>"
        elif last_clear_ok == 0:
            current_state_line = "Текущее состояние: <b>denylist выключен, но последняя очистка завершилась ошибкой</b>"
            current_entries_line = "Текущие правила denylist: <b>нужно проверить вручную</b>"
        else:
            current_state_line = "Текущее состояние: <b>denylist выключен, состояние правил не подтверждено</b>"
            current_entries_line = "Текущие правила denylist: <b>проверь вручную или выполни sync/clear</b>"
        return [
            current_state_line,
            current_entries_line,
            f"Последний успешный sync denylist (история): <b>{_sync_time_or_ne_bilo(last_sync_ts)}</b>",
            f"Ошибки denylist: <b>{errors}</b>",
        ]
    sync_status = "не было" if last_sync_ts <= 0 else _sync_status_text(last_sync_ok)
    return [
        f"Статус последней попытки sync denylist: <b>{sync_status}</b>",
        f"Время последнего успешного sync denylist: <b>{_sync_time_or_ne_bilo(last_sync_ts)}</b>",
        f"Текущих записей denylist: <b>{entries}</b>",
        f"Ошибки denylist: <b>{errors}</b>",
    ]



async def _denylist_keyboard():
    enabled = int(await get_setting("EGRESS_DENYLIST_ENABLED", int) or 0)
    mode = str(await get_setting("EGRESS_DENYLIST_MODE", str) or "soft").strip().lower() or "soft"
    return get_admin_denylist_kb(denylist_enabled=enabled, denylist_mode=mode)


async def _send_or_edit_admin_message(cb: types.CallbackQuery, text: str, reply_markup) -> None:
    message = cb.message
    if message is not None and hasattr(message, "edit_text"):
        try:
            await message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)
            return
        except Exception:
            pass
    if message is not None:
        await message.answer(text, parse_mode="HTML", reply_markup=reply_markup)


async def _render_network_policy_text() -> str:
    policy_stats = await policy_metrics()
    deny_enabled = int(await get_setting("EGRESS_DENYLIST_ENABLED", int) or 0)
    deny_mode = str(await get_setting("EGRESS_DENYLIST_MODE", str) or "soft").strip().lower()
    deny_refresh = int(await get_setting("EGRESS_DENYLIST_REFRESH_MINUTES", int) or 30)
    history_block = "\n".join(_denylist_history_block(enabled=deny_enabled, metrics=policy_stats))
    return (
        "🌐 <b>Сеть</b>\n\n"
        f"AWG: <b>{escape_html(DOCKER_CONTAINER)} / {escape_html(WG_INTERFACE)}</b>\n"
        f"Denylist: <b>{_bool_on_off(deny_enabled)}</b>\n"
        f"Режим denylist: <b>{escape_html(deny_mode)}</b>\n"
        f"Интервал обновления denylist: <b>{deny_refresh} мин</b>\n\n"
        f"{history_block}"
    )


def _normalize_support_username(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    username = raw[1:] if raw.startswith("@") else raw
    if not re.fullmatch(r"[A-Za-z0-9_]{1,32}", username):
        return ""
    return f"@{username}"


async def _render_service_settings_text() -> str:
    referral_enabled = int(await get_setting("REFERRAL_ENABLED", int) or 0)
    invitee_bonus = int(await get_setting("REFERRAL_INVITEE_BONUS_DAYS", int) or 5)
    inviter_bonus = int(await get_setting("REFERRAL_INVITER_BONUS_DAYS", int) or 3)
    torrent_enabled = int(await get_setting("TORRENT_POLICY_TEXT_ENABLED", int) or 0)
    support_username = str(getattr(config, "SUPPORT_USERNAME", "") or "").strip() or "не задан"
    download_url = str(getattr(config, "DOWNLOAD_URL", "") or "").strip() or "не задан"
    return (
        "⚙️ <b>Настройки сервиса</b>\n\n"
        f"🆘 Поддержка: <b>{escape_html(support_username)}</b>\n"
        f"🔗 Ссылка: <code>{escape_html(download_url)}</code>\n"
        f"🎁 Рефералы: <b>{_bool_on_off(referral_enabled)}</b>\n"
        f"🎁 Бонус другу: <b>{invitee_bonus} дн.</b>\n"
        f"🏅 Бонус пригласившему: <b>{inviter_bonus} дн.</b>\n"
        f"⚠️ Предупреждение о торрентах: <b>{_bool_on_off(torrent_enabled)}</b>"
    )


def _normalize_domains_multiline(raw_text: str) -> str:
    seen: set[str] = set()
    values: list[str] = []
    for line in raw_text.splitlines():
        domain = line.strip().lower().strip(".")
        if not domain:
            continue
        if domain not in seen:
            seen.add(domain)
            values.append(domain)
    return ",".join(values)


def _normalize_cidrs_multiline(raw_text: str) -> str:
    values: list[str] = []
    seen: set[str] = set()
    for line in raw_text.splitlines():
        raw = line.strip()
        if not raw:
            continue
        normalized = str(ipaddress.ip_network(raw, strict=False))
        if normalized not in seen:
            seen.add(normalized)
            values.append(normalized)
    return ",".join(values)


def _users_page_kb(rows: list[tuple[int, str]], page: int, total_pages: int) -> types.InlineKeyboardMarkup:
    keyboard: list[list[types.InlineKeyboardButton]] = []
    for uid, label in rows:
        keyboard.append([
            types.InlineKeyboardButton(text=f"👤 {label}", callback_data=f"{CB_ADMIN_MANAGE_USER_PREFIX}{uid}_{page}"),
        ])

    nav_row: list[types.InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(types.InlineKeyboardButton(text="⬅️ Назад", callback_data=f"{CB_ADMIN_USERS_PAGE_PREFIX}{page - 1}"))
    nav_row.append(types.InlineKeyboardButton(text=f"📄 {page + 1}/{max(total_pages, 1)}", callback_data="noop"))
    if page + 1 < total_pages:
        nav_row.append(types.InlineKeyboardButton(text="➡️ Далее", callback_data=f"{CB_ADMIN_USERS_PAGE_PREFIX}{page + 1}"))
    keyboard.append(nav_row)
    return types.InlineKeyboardMarkup(inline_keyboard=keyboard)


def _user_manage_kb(
    uid: int,
    page: int,
    *,
    show_retry_activation: bool = False,
    device_nums: list[int] | None = None,
) -> types.InlineKeyboardMarkup:
    rows: list[list[types.InlineKeyboardButton]] = [
        [
            types.InlineKeyboardButton(text="+1 день", callback_data=f"{CB_ADMIN_ADD_DAYS_PREFIX}{uid}_1_{page}"),
            types.InlineKeyboardButton(text="+7 дней", callback_data=f"{CB_ADMIN_ADD_DAYS_PREFIX}{uid}_7_{page}"),
            types.InlineKeyboardButton(text="+30 дней", callback_data=f"{CB_ADMIN_ADD_DAYS_PREFIX}{uid}_30_{page}"),
        ],
        [
            types.InlineKeyboardButton(text="⛔ Отключить", callback_data=f"{CB_ADMIN_REVOKE_PREFIX}{uid}_{page}"),
            types.InlineKeyboardButton(text="🗑 Удалить", callback_data=f"{CB_ADMIN_DELETE_PREFIX}{uid}_{page}"),
        ],
    ]
    if show_retry_activation:
        rows.append(
            [
                types.InlineKeyboardButton(
                    text="🛠 Повторить активацию",
                    callback_data=f"{CB_ADMIN_RETRY_ACTIVATION_PREFIX}{uid}_{page}",
                ),
            ]
        )
    if device_nums:
        for device_num in device_nums:
            rows.append(
                [
                    types.InlineKeyboardButton(
                        text=f"🗑 Устр. {device_num}",
                        callback_data=f"{CB_ADMIN_DEVICE_DELETE_PREFIX}{uid}_{device_num}_{page}",
                    ),
                    types.InlineKeyboardButton(
                        text=f"♻️ Перевыпуск {device_num}",
                        callback_data=f"{CB_ADMIN_DEVICE_REISSUE_PREFIX}{uid}_{device_num}_{page}",
                    ),
                ]
            )
    rows.extend([
        [types.InlineKeyboardButton(text="🔄 Обновить карточку", callback_data=f"{CB_ADMIN_MANAGE_USER_PREFIX}{uid}_{page}")],
        [types.InlineKeyboardButton(text="⬅️ К списку", callback_data=f"{CB_ADMIN_USERS_PAGE_PREFIX}{page}")],
    ])
    return types.InlineKeyboardMarkup(inline_keyboard=rows)


def _is_retry_activation_relevant(payment_summary: dict | None, has_keys: bool) -> bool:
    if not payment_summary or has_keys:
        return False
    payment_status = str(payment_summary.get("status") or "")
    activation_status = str(payment_summary.get("last_provision_status") or "")
    retryable_payment_statuses = {"received", "provisioning", "needs_repair", "failed", "stuck_manual"}
    retryable_activation_statuses = {"payment_received", "provisioning", "ready_config_pending", "needs_repair", "failed", "stuck_manual"}
    return payment_status in retryable_payment_statuses or activation_status in retryable_activation_statuses


def _operator_next_step(payment_status: str | None, activation_status: str | None, has_keys: bool) -> str:
    if has_keys:
        return "ожидание/закрыть: доступ уже выдан"
    if payment_status in {"stuck_manual", "failed"} or activation_status in {"stuck_manual", "failed", "needs_repair"}:
        return "проверить: аудит + при необходимости выдать вручную"
    if payment_status in {"needs_repair", "provisioning", "received"} or activation_status in {"payment_received", "provisioning", "ready_config_pending"}:
        return "синхронизация/ожидание: дождаться восстановления, затем обновить карточку"
    if payment_status == "applied" and not has_keys:
        return "ручная выдача: подписка активна, но ключа нет"
    return "ожидание/синхронизация: обновить карточку после /sync_awg"


async def _build_admin_device_activity_lines(uid: int) -> list[str]:
    key_rows = await fetchall(
        """
        SELECT device_num, public_key
        FROM keys
        WHERE user_id = ?
          AND state = 'active'
          AND public_key NOT LIKE 'pending:%'
        ORDER BY device_num
        """,
        (uid,),
    )
    if not key_rows:
        return ["• нет активных устройств"]

    runtime_available = True
    peer_by_public_key: dict[str, dict] = {}
    try:
        runtime_peers = await get_awg_peers()
        peer_by_public_key = {
            str(peer.get("public_key") or "").strip(): peer
            for peer in runtime_peers
            if str(peer.get("public_key") or "").strip()
        }
    except Exception:
        runtime_available = False

    now = utc_now_naive()
    lines: list[str] = []
    for device_num, public_key in key_rows:
        peer = peer_by_public_key.get(str(public_key).strip())
        lines.append(
            render_device_activity_line(
                device_num=int(device_num),
                has_runtime_peer=peer is not None,
                last_handshake_at=peer.get("latest_handshake_at") if peer else None,
                runtime_available=runtime_available,
                now=now,
            )
        )
    return lines


async def _build_admin_device_traffic_lines(uid: int) -> list[str]:
    rows = await get_user_device_traffic_summary(uid)
    if not rows:
        return ["• Всего трафика — 0 B"]

    lines = [
        render_device_traffic_line(
            int(row["device_num"]),
            int(row["rx_bytes_total"]),
            int(row["tx_bytes_total"]),
        )
        for row in rows
    ]
    total_bytes = await get_user_total_traffic_bytes(uid)
    lines.append(f"• Всего трафика — {format_bytes_compact(total_bytes)}")
    return lines


async def _render_users_page(target_message: types.Message, page: int) -> None:
    total_users = (await fetchone("SELECT COUNT(*) FROM users"))[0]
    if total_users == 0:
        await target_message.answer("Список пользователей пуст.")
        return
    total_pages = max(1, (total_users + ADMIN_USERS_PAGE_SIZE - 1) // ADMIN_USERS_PAGE_SIZE)
    page = max(0, min(page, total_pages - 1))
    offset = page * ADMIN_USERS_PAGE_SIZE
    rows = await fetchall(
        """
        SELECT user_id, sub_until
        FROM users
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
        """,
        (ADMIN_USERS_PAGE_SIZE, offset),
    )
    labels: list[tuple[int, str]] = []
    lines = [f"👥 <b>Пользователи</b> (страница {page + 1}/{total_pages})\n"]
    for uid, sub_until in rows:
        status_text, until_text = get_status_text(sub_until)
        tg_username, _ = await get_user_meta(uid)
        short_name = format_tg_username(tg_username)
        labels.append((uid, f"{uid} — {short_name}"))
        lines.append(f"• <code>{uid}</code> — {short_name} — {status_text} — {until_text}")
    await target_message.answer(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=_users_page_kb(labels, page, total_pages),
    )


def _payment_admin_details(payment_summary: dict | None) -> tuple[str, str, str]:
    if not payment_summary:
        return ("нет платежей", "—", "—")
    payment_line = f"{payment_summary['status']} · {payment_summary['amount']} {payment_summary['currency']}"
    activation_line = str(payment_summary.get("last_provision_status") or "—")
    charge_id = str(payment_summary.get("payment_id") or "—")
    return payment_line, activation_line, charge_id


def _render_payment_lookup_text(payment_summary: dict) -> str:
    return (
        "💳 <b>Платёж</b>\n\n"
        f"🆔 user_id: <code>{int(payment_summary.get('user_id') or 0)}</code>\n"
        f"🧾 Charge ID: <code>{escape_html(str(payment_summary.get('payment_id') or '—'))}</code>\n"
        f"📌 Статус: <b>{escape_html(str(payment_summary.get('status') or '—'))}</b>\n"
        f"💰 Сумма: <b>{payment_summary.get('amount')} {escape_html(str(payment_summary.get('currency') or '—'))}</b>\n"
        f"📦 Данные платежа: <code>{escape_html(str(payment_summary.get('payload') or '—'))}</code>\n"
        f"🕒 Создан: <code>{escape_html(str(payment_summary.get('created_at') or '—'))}</code>\n"
        f"🚦 Статус активации: <b>{escape_html(str(payment_summary.get('last_provision_status') or '—'))}</b>"
    )


async def _send_user_manage_card(target_message: types.Message, uid: int, page: int) -> None:
    row = await fetchone("SELECT sub_until FROM users WHERE user_id = ?", (uid,))
    if not row:
        await target_message.answer("Пользователь не найден.")
        return
    sub_until = row[0]
    status_text, until_text = get_status_text(sub_until)
    tg_username, first_name = await get_user_meta(uid)
    keys = await get_user_keys(uid)
    payment_summary = await get_latest_user_payment_summary(uid)
    referral = await get_referral_summary(uid)
    admin_device_rows = await fetchall(
        """
        SELECT device_num
        FROM keys
        WHERE user_id = ?
          AND public_key NOT LIKE 'pending:%'
          AND state = 'active'
        ORDER BY device_num
        """,
        (uid,),
    )
    admin_device_nums = [int(row[0]) for row in admin_device_rows]
    connection_status = "готово" if keys else "нет ключа"
    payment_line, activation_line, charge_id = _payment_admin_details(payment_summary)
    operator_step = _operator_next_step(payment_summary["status"], activation_line, bool(keys)) if payment_summary else "wait"
    show_retry_activation = _is_retry_activation_relevant(payment_summary, bool(keys))
    retry_hint = "\n🧰 Повтор активации: <b>доступен</b> для ручного запуска" if show_retry_activation else ""
    activity_lines = await _build_admin_device_activity_lines(uid)
    traffic_lines = await _build_admin_device_traffic_lines(uid)
    activity_text = "\n".join(activity_lines)
    traffic_text = "\n".join(traffic_lines)
    await target_message.answer(
        (
            "🛠 <b>Управление пользователем</b>\n\n"
            f"🆔 <code>{uid}</code>\n"
            f"👤 Имя: {escape_html(first_name)}\n"
            f"✈️ Telegram: {format_tg_username(tg_username)}\n"
            f"📌 Статус: {status_text}\n"
            f"📅 До: <b>{until_text}</b>\n"
            f"🔑 Подключение: <b>{connection_status}</b> (устройств: {len(keys)})\n"
            f"💸 Последний платёж: <b>{payment_line}</b>\n"
            f"🚦 Активация: <b>{activation_line}</b>\n"
            f"🧾 Charge ID: <code>{escape_html(charge_id)}</code>\n"
            "↩️ Возврат: обрабатывается оператором вручную по user_id и telegram_payment_charge_id.\n"
            "Автоматический сценарий возврата в selfhost MVP пока не реализован.\n"
            f"➡️ Шаг оператора: <b>{operator_step}</b>\n"
            f"🎁 Рефералы: приглашено {referral['invited_count']} · с бонусом {referral['rewarded_count']}\n\n"
            "📶 Активность устройств:\n"
            f"{activity_text}\n\n"
            "📊 Трафик:\n"
            f"{traffic_text}"
            f"{retry_hint}"
        ),
        parse_mode="HTML",
        reply_markup=_user_manage_kb(
            uid,
            page,
            show_retry_activation=show_retry_activation,
            device_nums=admin_device_nums,
        ),
    )


def build_admin_manual_commands_text() -> str:
    lines = ["⌨️ <b>Ручные admin-команды</b>", ""]
    for command, description in ADMIN_MANUAL_COMMANDS:
        lines.append(f"• <code>{command}</code> — {description}")
    return "\n".join(lines)


@router.message(F.text == BTN_ADMIN, IsAdmin())
async def admin_panel(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    stats_text = await build_stats_text()
    db_info = await db_health_info()
    db_status = "🟢 Нормально" if db_info["is_healthy"] else "🟡 Нужна проверка"
    await message.answer(
        stats_text + f"\n🗄 Статус БД: <b>{db_status}</b>",
        parse_mode="HTML",
        reply_markup=get_admin_inline_kb(),
    )


@router.callback_query(F.data == CB_ADMIN_BACK_MAIN)
async def admin_back_main(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await _send_or_edit_admin_message(cb, "⚙️ <b>Админ-меню</b>", get_admin_inline_kb())
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_COMMANDS)
async def admin_manual_commands(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await _send_or_edit_admin_message(
        cb,
        build_admin_manual_commands_text(),
        get_admin_simple_back_kb(CB_ADMIN_BACK_MAIN),
    )
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_PAYMENTS)
async def admin_payments_screen(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await clear_pending_admin_action(ADMIN_ID, PAYMENT_CHARGE_INPUT_ACTION_KEY)
    await clear_pending_admin_action(ADMIN_ID, PAYMENT_USER_INPUT_ACTION_KEY)
    await _send_or_edit_admin_message(cb, "💳 <b>Платежи</b>", get_admin_payments_kb())
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_FIND_CHARGE)
async def admin_payments_find_charge_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, PAYMENT_USER_INPUT_ACTION_KEY)
    await set_pending_admin_action(ADMIN_ID, PAYMENT_CHARGE_INPUT_ACTION_KEY, {"action": PAYMENT_CHARGE_INPUT_ACTION_KEY})
    await cb.message.answer("Введите Charge ID платежа")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_LAST_PAYMENT)
async def admin_payments_latest_by_user_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, PAYMENT_CHARGE_INPUT_ACTION_KEY)
    await set_pending_admin_action(ADMIN_ID, PAYMENT_USER_INPUT_ACTION_KEY, {"action": PAYMENT_USER_INPUT_ACTION_KEY})
    await cb.message.answer("Введите user_id")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_PRICES)
async def admin_prices(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await _send_or_edit_admin_message(cb, _render_admin_prices_text(), get_admin_prices_kb())
    await cb.answer()


@router.callback_query(F.data.in_(set(PRICE_TARGETS.keys())))
async def admin_prices_start_edit(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    target = PRICE_TARGETS.get(cb.data)
    if not target:
        await cb.answer("Некорректный тариф", show_alert=True)
        return
    maintenance_enabled = int(await get_setting("MAINTENANCE_MODE", int) or 0) == 1
    if not maintenance_enabled:
        await cb.answer("Сначала включите /maintenance_on, затем изменяйте цену.", show_alert=True)
        return
    env_key, label = target
    current_value = int(getattr(config, env_key))
    await clear_pending_admin_action(ADMIN_ID, PRICE_CONFIRM_ACTION_KEY)
    await set_pending_admin_action(ADMIN_ID, PRICE_INPUT_ACTION_KEY, {"env_key": env_key, "label": label})
    await cb.message.answer(f"Введите новую цену для «{label}» в ⭐. Текущая: {current_value}⭐")
    await cb.answer()


@router.message(IsAdmin(), F.text, ~F.text.startswith("/"), HasPendingPriceInput())
async def admin_prices_capture_input(message: types.Message):
    action = await get_pending_admin_action(ADMIN_ID, PRICE_INPUT_ACTION_KEY)
    if not action:
        return
    new_value = _parse_price_input(message.text or "")
    if new_value is None:
        await message.answer("Нужно положительное целое число.")
        return
    env_key = str(action.get("env_key", ""))
    label = str(action.get("label", ""))
    old_value = int(getattr(config, env_key, 0))
    await clear_pending_admin_action(ADMIN_ID, PRICE_INPUT_ACTION_KEY)
    await set_pending_admin_action(
        ADMIN_ID,
        PRICE_CONFIRM_ACTION_KEY,
        {"env_key": env_key, "label": label, "old": old_value, "new": new_value},
    )
    await message.answer(
        (
            f"{label}\n"
            f"Было: {old_value}⭐\n"
            f"Станет: {new_value}⭐"
        ),
        reply_markup=get_admin_price_confirm_kb(),
    )


@router.callback_query(F.data == CB_ADMIN_PRICE_SAVE)
async def admin_prices_save(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    action = await pop_pending_admin_action(ADMIN_ID, PRICE_CONFIRM_ACTION_KEY)
    await clear_pending_admin_action(ADMIN_ID, PRICE_INPUT_ACTION_KEY)
    if not action:
        await cb.answer("Нет ожидающего действия", show_alert=True)
        return
    env_key = str(action.get("env_key", ""))
    label = str(action.get("label", ""))
    new_value = int(action.get("new", 0))
    old_value, saved_value = set_stars_price(env_key, new_value)
    await write_audit_log(ADMIN_ID, "admin_price_updated", f"key={env_key}; old={old_value}; new={saved_value}")
    await cb.message.answer(
        (
            f"✅ Сохранено: {label}\n"
            f"{old_value}⭐ → {saved_value}⭐"
        ),
    )
    await cb.message.answer(
        _render_admin_prices_text(),
        parse_mode="HTML",
        reply_markup=get_admin_prices_kb(),
    )
    await cb.answer("Сохранено")


@router.callback_query(F.data == CB_ADMIN_PRICE_CANCEL)
async def admin_prices_cancel(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, PRICE_INPUT_ACTION_KEY)
    await clear_pending_admin_action(ADMIN_ID, PRICE_CONFIRM_ACTION_KEY)
    await cb.message.answer(
        _render_admin_prices_text(),
        parse_mode="HTML",
        reply_markup=get_admin_prices_kb(),
    )
    await cb.answer("Отменено")


@router.callback_query(F.data == CB_ADMIN_STATS)
async def admin_stats_cb(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await _send_or_edit_admin_message(
        cb,
        await build_stats_text(),
        get_admin_simple_back_kb(CB_ADMIN_BACK_MAIN, refresh_cb=CB_ADMIN_STATS),
    )
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_SYNC)
async def admin_sync_awg(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    try:
        await _send_or_edit_admin_message(
            cb,
            await build_awg_sync_text(),
            get_admin_simple_back_kb(CB_ADMIN_BACK_MAIN, refresh_cb=CB_ADMIN_SYNC),
        )
        await cb.answer("Синхронизация AWG выполнена")
    except Exception as e:
        logger.exception("Ошибка admin_sync_awg: %s", e)
        await cb.answer("❌ Ошибка проверки", show_alert=True)


@router.callback_query(F.data == CB_ADMIN_NETWORK_POLICY)
async def admin_network_policy_screen(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await _send_or_edit_admin_message(cb, await _render_network_policy_text(), get_admin_network_policy_kb())
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_NET_DENYLIST)
async def admin_network_policy_denylist_screen(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _send_or_edit_admin_message(cb, "🛡 <b>Настройки denylist</b>", await _denylist_keyboard())
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_NET_SYNC_NOW)
async def admin_network_policy_sync_now(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await denylist_sync(run_docker)
    await write_audit_log(ADMIN_ID, "admin_network_policy_sync", "manual_sync=1")
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_DENYLIST_TOGGLE)
async def admin_denylist_toggle(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    enabled = int(await get_setting("EGRESS_DENYLIST_ENABLED", int) or 0)
    new_value = "0" if enabled == 1 else "1"
    await set_app_setting("EGRESS_DENYLIST_ENABLED", new_value, updated_by=ADMIN_ID)
    await denylist_sync(run_docker)
    await write_audit_log(ADMIN_ID, "admin_denylist_enabled_set", f"value={new_value}")
    await cb.message.answer(
        f"✅ denylist: {_bool_on_off(int(new_value))}. Синхронизация выполнена."
    )
    await cb.answer()


@router.callback_query(F.data.in_({CB_ADMIN_DENYLIST_MODE_SOFT, CB_ADMIN_DENYLIST_MODE_STRICT}))
async def admin_denylist_mode_set(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    mode = "strict" if cb.data == CB_ADMIN_DENYLIST_MODE_STRICT else "soft"
    await set_app_setting("EGRESS_DENYLIST_MODE", mode, updated_by=ADMIN_ID)
    await denylist_sync(run_docker)
    await write_audit_log(ADMIN_ID, "admin_denylist_mode_set", f"value={mode}")
    await cb.message.answer(f"✅ Режим denylist: {mode}.")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_DENYLIST_VIEW_DOMAINS)
async def admin_denylist_view_domains(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    domains = str(await get_setting("EGRESS_DENYLIST_DOMAINS", str) or "")
    lines = [item.strip() for item in domains.split(",") if item.strip()]
    body = "\n".join(f"• {escape_html(item)}" for item in lines[:100]) if lines else "Список пуст."
    await cb.message.answer(f"🧾 <b>Список доменов denylist</b>\n{body}", parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_DENYLIST_VIEW_CIDRS)
async def admin_denylist_view_cidrs(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    cidrs = str(await get_setting("EGRESS_DENYLIST_CIDRS", str) or "")
    lines = [item.strip() for item in cidrs.split(",") if item.strip()]
    body = "\n".join(f"• {escape_html(item)}" for item in lines[:100]) if lines else "Список пуст."
    await cb.message.answer(f"🧾 <b>Список CIDR denylist</b>\n{body}", parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_DENYLIST_REPLACE_DOMAINS)
async def admin_denylist_replace_domains_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await set_pending_admin_action(ADMIN_ID, DENYLIST_DOMAINS_INPUT_ACTION_KEY, {"action": DENYLIST_DOMAINS_INPUT_ACTION_KEY})
    await cb.message.answer("Отправьте список доменов: один домен на строку.")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_DENYLIST_REPLACE_CIDRS)
async def admin_denylist_replace_cidrs_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await set_pending_admin_action(ADMIN_ID, DENYLIST_CIDRS_INPUT_ACTION_KEY, {"action": DENYLIST_CIDRS_INPUT_ACTION_KEY})
    await cb.message.answer("Отправьте CIDR списком: одна сеть на строку.")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_DENYLIST_SYNC)
async def admin_denylist_sync_now(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await denylist_sync(run_docker)
    await write_audit_log(ADMIN_ID, "admin_denylist_sync", "manual_sync=1")
    await cb.message.answer("✅ Синхронизация denylist выполнена.")
    await cb.answer("Готово")

@router.callback_query(F.data == CB_ADMIN_LIST)
async def admin_list_all(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await _render_users_page(cb.message, 0)
    await cb.answer()


@router.callback_query(F.data.startswith(CB_ADMIN_USERS_PAGE_PREFIX))
async def admin_users_page(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        page = int(cb.data.removeprefix(CB_ADMIN_USERS_PAGE_PREFIX))
        await _render_users_page(cb.message, page)
        await cb.answer("Открыто")
    except ValueError:
        await cb.answer("Некорректный номер страницы", show_alert=True)
    except Exception as e:
        logger.exception("Ошибка admin_users_page: %s", e)
        await cb.answer("❌ Не удалось открыть страницу", show_alert=True)


@router.callback_query(F.data.startswith(CB_ADMIN_MANAGE_USER_PREFIX))
async def admin_manage_user(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        _, _, _, uid_raw, page_raw = cb.data.split("_", 4)
        uid = int(uid_raw)
        page = int(page_raw)
        await _send_user_manage_card(cb.message, uid, page)
        await cb.answer("Открыто")
    except ValueError:
        await cb.answer("Некорректный user_id", show_alert=True)
    except Exception as e:
        logger.exception("Ошибка admin_manage_user: %s", e)
        await cb.answer("❌ Не удалось открыть карточку пользователя", show_alert=True)


@router.callback_query(F.data.startswith(CB_ADMIN_ADD_DAYS_PREFIX))
async def admin_add_days_btn(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        _, _, _, uid_raw, days_raw, page_raw = cb.data.split("_", 5)
        uid = int(uid_raw)
        days = int(days_raw)
        page = int(page_raw)
        if days >= 30:
            await clear_pending_admin_action(ADMIN_ID, ADD_DAYS_CONFIRM_ACTION_KEY)
            await set_pending_admin_action(
                ADMIN_ID,
                ADD_DAYS_CONFIRM_ACTION_KEY,
                {"uid": uid, "days": days, "page": page},
            )
            await cb.message.answer(
                (
                    "⚠️ <b>Подтвердите действие</b>\n\n"
                    f"Выдать пользователю <code>{uid}</code> <b>+{days} дней</b>?"
                ),
                parse_mode="HTML",
                reply_markup=get_admin_add_days_confirm_kb(),
            )
            await cb.answer("Нужно подтверждение")
            return
        if admin_command_limited(f"admin_add_{days}", cb.from_user.id):
            await cb.answer("Слишком часто", show_alert=True)
            return
        new_until = await issue_subscription(uid, days)
        notified = await notify_user_subscription_granted(cb.bot, uid, days, new_until)
        await write_audit_log(ADMIN_ID, f"admin_add_{days}", f"target={uid}; until={new_until.isoformat()}; notified={int(notified)}")
        await cb.answer(f"✅ +{days} дней пользователю {uid}")
        await cb.message.answer(
            (
                f"✅ <b>Пользователю выдано +{days} дней</b>\n\n"
                f"🆔 <code>{uid}</code>\n"
                f"📅 До: <b>{new_until.strftime('%d.%m.%Y %H:%M')}</b>"
            ),
            parse_mode="HTML",
            reply_markup=_user_manage_kb(uid, page),
        )
        if not notified:
            await cb.message.answer("⚠️ Доступ выдан, но уведомление пользователю отправить не удалось.")
    except Exception as e:
        logger.exception("Ошибка admin_add_days_btn: %s", e)
        await cb.answer("❌ Не удалось продлить доступ", show_alert=True)


@router.callback_query(F.data == CB_CONFIRM_ADD_DAYS)
async def admin_add_days_confirm(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    action = await pop_pending_admin_action(ADMIN_ID, ADD_DAYS_CONFIRM_ACTION_KEY)
    if not action:
        await cb.answer("Нет ожидающего действия", show_alert=True)
        return
    uid = int(action.get("uid", 0))
    days = int(action.get("days", 0))
    page = int(action.get("page", 0))
    if uid <= 0 or days <= 0:
        await cb.answer("Некорректные параметры", show_alert=True)
        return
    if admin_command_limited(f"admin_add_{days}", cb.from_user.id):
        await cb.answer("Слишком часто", show_alert=True)
        return
    new_until = await issue_subscription(uid, days)
    notified = await notify_user_subscription_granted(cb.bot, uid, days, new_until)
    await write_audit_log(ADMIN_ID, f"admin_add_{days}", f"target={uid}; until={new_until.isoformat()}; notified={int(notified)}")
    await cb.answer(f"✅ +{days} дней пользователю {uid}")
    await cb.message.answer(
        (
            f"✅ <b>Пользователю выдано +{days} дней</b>\n\n"
            f"🆔 <code>{uid}</code>\n"
            f"📅 До: <b>{new_until.strftime('%d.%m.%Y %H:%M')}</b>"
        ),
        parse_mode="HTML",
        reply_markup=_user_manage_kb(uid, page),
    )
    if not notified:
        await cb.message.answer("⚠️ Доступ выдан, но уведомление пользователю отправить не удалось.")


@router.callback_query(F.data == CB_CANCEL_ADD_DAYS)
async def admin_add_days_cancel(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, ADD_DAYS_CONFIRM_ACTION_KEY)
    await cb.answer("Отменено")
    if cb.message:
        await cb.message.answer("❌ Выдача дней отменена.")


@router.callback_query(F.data.startswith(CB_ADMIN_RETRY_ACTIVATION_PREFIX))
async def admin_retry_activation_btn(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        _, _, _, uid_raw, page_raw = cb.data.split("_", 4)
        uid = int(uid_raw)
        page = int(page_raw)
        if admin_command_limited(f"admin_retry_activation_{uid}", cb.from_user.id):
            await cb.answer("Слишком часто: подождите перед повтором активации.", show_alert=True)
            return

        payment_summary = await get_latest_user_payment_summary(uid)
        if not payment_summary:
            await write_audit_log(ADMIN_ID, "manual_retry_noop", f"target={uid}; reason=no_payment")
            await cb.message.answer(
                "ℹ️ Нет платежей для повтора активации. Нечего запускать повторно.",
                reply_markup=_user_manage_kb(uid, page),
            )
            await cb.answer("Нечего повторять")
            return

        payment_id = str(payment_summary["payment_id"])
        await write_audit_log(ADMIN_ID, "manual_retry_requested", f"target={uid}; payment_id={payment_id}")
        result = await manual_retry_activation(payment_id, bot=cb.bot)
        result_code = result.get("result", "unknown")
        result_message = result.get("message", "Без деталей.")
        if result_code == "succeeded":
            await write_audit_log(ADMIN_ID, "manual_retry_succeeded", f"target={uid}; payment_id={payment_id}")
            outcome = "✅ Повтор активации успешен"
        elif result_code in {"no_payment", "already_applied", "in_progress", "not_retryable", "no_op"}:
            await write_audit_log(ADMIN_ID, "manual_retry_noop", f"target={uid}; payment_id={payment_id}; result={result_code}")
            outcome = "ℹ️ Повтор активации не требуется"
        else:
            await write_audit_log(ADMIN_ID, "manual_retry_failed", f"target={uid}; payment_id={payment_id}; result={result_code}")
            outcome = "⚠️ Повтор активации не удался"

        await cb.message.answer(
            (
                f"{outcome}\n\n"
                f"🆔 <code>{uid}</code>\n"
                f"💳 Charge ID: <code>{payment_id}</code>\n"
                f"🧩 Результат: <b>{escape_html(result_code)}</b>\n"
                f"📝 Детали: {escape_html(result_message)}\n\n"
                "Следующий шаг: обновите карточку; если статус не меняется — проверьте журнал и выдайте доступ вручную."
            ),
            parse_mode="HTML",
            reply_markup=_user_manage_kb(uid, page),
        )
        await cb.answer("Повтор обработан")
    except ValueError:
        await cb.answer("Некорректные параметры действия", show_alert=True)
    except Exception as e:
        logger.exception("Ошибка admin_retry_activation_btn: %s", e)
        await write_audit_log(ADMIN_ID, "manual_retry_failed", f"error={str(e)[:300]}")
        await cb.answer("❌ Не удалось повторить активацию", show_alert=True)


@router.callback_query(F.data.startswith(CB_ADMIN_DEVICE_DELETE_PREFIX))
async def admin_device_delete_btn(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        _, _, _, uid_raw, device_num_raw, page_raw = cb.data.split("_", 5)
        uid = int(uid_raw)
        device_num = int(device_num_raw)
        page = int(page_raw)
    except ValueError:
        await cb.answer("Некорректные параметры действия", show_alert=True)
        return
    await set_pending_admin_action(
        ADMIN_ID,
        "device_delete",
        {"action": "device_delete", "target": uid, "device_num": device_num, "page": page},
    )
    await cb.message.answer(
        (
            "⚠️ <b>Подтвердите удаление устройства</b>\n\n"
            f"Пользователь: <code>{uid}</code>\n"
            f"Устройство: <b>{device_num}</b>\n\n"
            "Будет удалён только выбранный peer."
        ),
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text="✅ Подтвердить", callback_data=CB_CONFIRM_DEVICE_DELETE)],
                [types.InlineKeyboardButton(text="❌ Отмена", callback_data=CB_CANCEL_DEVICE_DELETE)],
            ]
        ),
    )
    await cb.answer()


@router.callback_query(F.data == CB_CONFIRM_DEVICE_DELETE)
async def confirm_device_delete(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    action = await pop_pending_admin_action(ADMIN_ID, "device_delete")
    if not action or action.get("action") != "device_delete":
        await cb.answer("Нет ожидающего действия", show_alert=True)
        return
    uid = int(action["target"])
    device_num = int(action["device_num"])
    page = int(action.get("page", 0))
    try:
        result = await delete_user_device(uid, device_num)
        await write_audit_log(
            ADMIN_ID,
            "admin_device_delete",
            f"target={uid}; device_num={device_num}; status={result['status']}; removed_runtime={int(result.get('removed_runtime', False))}",
        )
        if result["status"] == "not_found":
            await cb.message.answer(
                (
                    "ℹ️ <b>Устройство уже отсутствует</b>\n\n"
                    f"🆔 <code>{uid}</code>\n"
                    f"📱 Устройство: <b>{device_num}</b>\n\n"
                    "Обновите карточку пользователя и проверьте активность/состояние."
                ),
                parse_mode="HTML",
                reply_markup=_user_manage_kb(uid, page),
            )
            await cb.answer("Нечего удалять")
            return
        await cb.message.answer(
            (
                "✅ <b>Устройство удалено</b>\n\n"
                f"🆔 <code>{uid}</code>\n"
                f"📱 Устройство: <b>{device_num}</b>\n\n"
                "Дальше: обновите карточку; если не помогло — проверьте активность/состояние."
            ),
            parse_mode="HTML",
            reply_markup=_user_manage_kb(uid, page),
        )
        await cb.answer("Готово")
    except Exception as e:
        logger.exception("Ошибка confirm_device_delete: %s", e)
        await write_audit_log(ADMIN_ID, "admin_device_delete_failed", f"target={uid}; device_num={device_num}; error={str(e)[:200]}")
        await cb.answer("❌ Не удалось удалить устройство", show_alert=True)


@router.callback_query(F.data == CB_CANCEL_DEVICE_DELETE)
async def cancel_device_delete(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, "device_delete")
    await cb.message.answer("❌ Удаление устройства отменено")
    await cb.answer("Отменено")


@router.callback_query(F.data.startswith(CB_ADMIN_DEVICE_REISSUE_PREFIX))
async def admin_device_reissue_btn(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        _, _, _, uid_raw, device_num_raw, page_raw = cb.data.split("_", 5)
        uid = int(uid_raw)
        device_num = int(device_num_raw)
        page = int(page_raw)
    except ValueError:
        await cb.answer("Некорректные параметры действия", show_alert=True)
        return
    await set_pending_admin_action(
        ADMIN_ID,
        "device_reissue",
        {"action": "device_reissue", "target": uid, "device_num": device_num, "page": page},
    )
    await cb.message.answer(
        (
            "⚠️ <b>Подтвердите перевыпуск конфига устройства</b>\n\n"
            f"Пользователь: <code>{uid}</code>\n"
            f"Устройство: <b>{device_num}</b>\n\n"
            "Будет заменён только один peer в этом slot."
        ),
        parse_mode="HTML",
        reply_markup=types.InlineKeyboardMarkup(
            inline_keyboard=[
                [types.InlineKeyboardButton(text="✅ Подтвердить", callback_data=CB_CONFIRM_DEVICE_REISSUE)],
                [types.InlineKeyboardButton(text="❌ Отмена", callback_data=CB_CANCEL_DEVICE_REISSUE)],
            ]
        ),
    )
    await cb.answer()


@router.callback_query(F.data == CB_CONFIRM_DEVICE_REISSUE)
async def confirm_device_reissue(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    action = await pop_pending_admin_action(ADMIN_ID, "device_reissue")
    if not action or action.get("action") != "device_reissue":
        await cb.answer("Нет ожидающего действия", show_alert=True)
        return
    uid = int(action["target"])
    device_num = int(action["device_num"])
    page = int(action.get("page", 0))
    try:
        result = await reissue_user_device(uid, device_num)
        await write_audit_log(ADMIN_ID, "admin_device_reissue", f"target={uid}; device_num={device_num}; status={result['status']}")
        if result["status"] == "not_found":
            await cb.message.answer(
                (
                    "ℹ️ <b>Перевыпуск не требуется</b>\n\n"
                    f"🆔 <code>{uid}</code>\n"
                    f"📱 Устройство: <b>{device_num}</b>\n\n"
                    "Устройство уже отсутствует. Обновите карточку пользователя."
                ),
                parse_mode="HTML",
                reply_markup=_user_manage_kb(uid, page),
            )
            await cb.answer("Нечего перевыпускать")
            return
        await cb.message.answer(
            (
                "♻️ <b>Конфиг устройства перевыпущен</b>\n\n"
                f"🆔 <code>{uid}</code>\n"
                f"📱 Устройство: <b>{device_num}</b>\n\n"
                "Дальше: отправьте пользователю новый конфиг через стандартный сценарий «Подключение». "
                "Если не помогло — проверьте активность/состояние."
            ),
            parse_mode="HTML",
            reply_markup=_user_manage_kb(uid, page),
        )
        await cb.answer("Готово")
    except Exception as e:
        logger.exception("Ошибка confirm_device_reissue: %s", e)
        await write_audit_log(ADMIN_ID, "admin_device_reissue_failed", f"target={uid}; device_num={device_num}; error={str(e)[:200]}")
        await cb.answer("❌ Не удалось перевыпустить устройство", show_alert=True)


@router.callback_query(F.data == CB_CANCEL_DEVICE_REISSUE)
async def cancel_device_reissue(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, "device_reissue")
    await cb.message.answer("❌ Перевыпуск устройства отменён")
    await cb.answer("Отменено")


@router.callback_query(F.data.startswith(CB_ADMIN_REVOKE_PREFIX))
async def admin_revoke_btn(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        _, _, uid_raw, page_raw = cb.data.split("_", 3)
        uid = int(uid_raw)
        page = int(page_raw)
    except ValueError:
        await cb.answer("Некорректные параметры действия", show_alert=True)
        return
    await set_pending_admin_action(ADMIN_ID, "revoke", {"action": "revoke", "target": uid, "page": page})
    await cb.message.answer(
        (
            "⚠️ <b>Подтвердите отключение доступа</b>\n\n"
            f"Пользователь: <code>{uid}</code>"
        ),
        parse_mode="HTML",
        reply_markup=get_admin_confirm_kb("revoke"),
    )
    await cb.answer()


@router.callback_query(F.data == CB_CONFIRM_REVOKE)
async def confirm_revoke(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    action = await pop_pending_admin_action(ADMIN_ID, "revoke")
    if not action or action.get("action") != "revoke":
        await cb.answer("Нет ожидающего действия", show_alert=True)
        return
    uid = int(action["target"])
    page = int(action.get("page", 0))
    try:
        removed = await revoke_user_access(uid)
        await write_audit_log(ADMIN_ID, "admin_revoke", f"target={uid}; removed={removed}")
        await cb.message.answer(
            (
                f"⛔ <b>Доступ отключён</b>\n\n"
                f"🆔 <code>{uid}</code>\n"
                f"🔌 Удалено peer: <b>{removed}</b>"
            ),
            parse_mode="HTML",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ К списку", callback_data=f"{CB_ADMIN_USERS_PAGE_PREFIX}{page}")]]
            ),
        )
        await cb.answer("Готово")
    except Exception as e:
        logger.exception("Ошибка confirm_revoke: %s", e)
        await cb.answer("❌ Не удалось отключить пользователя", show_alert=True)


@router.callback_query(F.data == CB_CANCEL_REVOKE)
async def cancel_revoke(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, "revoke")
    await cb.message.answer("❌ Отключение отменено")
    await cb.answer("Отменено")


@router.callback_query(F.data.startswith(CB_ADMIN_DELETE_PREFIX))
async def admin_del_user(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    try:
        _, _, uid_raw, page_raw = cb.data.split("_", 3)
        uid = int(uid_raw)
        page = int(page_raw)
    except ValueError:
        await cb.answer("Некорректные параметры действия", show_alert=True)
        return
    await set_pending_admin_action(ADMIN_ID, "delete_user", {"action": "delete_user", "target": uid, "page": page})
    await cb.message.answer(
        (
            "⚠️ <b>Подтвердите полное удаление пользователя</b>\n\n"
            f"Пользователь: <code>{uid}</code>"
        ),
        parse_mode="HTML",
        reply_markup=get_admin_confirm_kb("delete_user"),
    )
    await cb.answer()


@router.callback_query(F.data == CB_CONFIRM_DELETE_USER)
async def confirm_delete_user(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    action = await pop_pending_admin_action(ADMIN_ID, "delete_user")
    if not action or action.get("action") != "delete_user":
        await cb.answer("Нет ожидающего действия", show_alert=True)
        return
    uid = int(action["target"])
    page = int(action.get("page", 0))
    try:
        peers_count, _ = await delete_user_everywhere(uid)
        await write_audit_log(ADMIN_ID, "admin_delete_user", f"target={uid}; removed={peers_count}")
        await cb.message.answer(
            (
                f"🗑 <b>Пользователь удалён</b>\n\n"
                f"🆔 <code>{uid}</code>\n"
                f"🔌 Удалено peer: <b>{peers_count}</b>"
            ),
            parse_mode="HTML",
            reply_markup=types.InlineKeyboardMarkup(
                inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ К списку", callback_data=f"{CB_ADMIN_USERS_PAGE_PREFIX}{page}")]]
            ),
        )
        await cb.answer("Готово")
    except Exception as e:
        logger.exception("Ошибка confirm_delete_user: %s", e)
        await cb.answer(f"❌ Не удалось удалить пользователя: {str(e)[:120]}", show_alert=True)


@router.callback_query(F.data == CB_CANCEL_DELETE_USER)
async def cancel_delete_user(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, "delete_user")
    await cb.message.answer("❌ Удаление отменено")
    await cb.answer("Отменено")


@router.callback_query(F.data == CB_ADMIN_BROADCAST)
async def admin_broadcast_btn(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await cb.answer()
    await clear_pending_broadcast(ADMIN_ID)
    await set_pending_admin_action(ADMIN_ID, BROADCAST_INPUT_ACTION_KEY, {"action": BROADCAST_INPUT_ACTION_KEY})
    users_total = int(await fetchval("SELECT COUNT(*) FROM users"))
    await cb.message.answer(
        (
            "📢 <b>Рассылка</b>\n\n"
            f"Сейчас в базе: <b>{users_total}</b> пользователей.\n\n"
            "Отправьте текст рассылки одним сообщением.\n"
            "Перед отправкой будет обязательное подтверждение."
        ),
        parse_mode="HTML",
        reply_markup=get_broadcast_cancel_kb(),
    )


@router.callback_query(F.data == CB_BROADCAST_CONFIRM)
async def broadcast_confirm(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    text = await get_pending_broadcast(ADMIN_ID)
    if not text:
        await cb.answer("Нет ожидающей рассылки", show_alert=True)
        return
    job_id = await create_broadcast_job(ADMIN_ID, text)
    await clear_pending_broadcast(ADMIN_ID)
    await write_audit_log(ADMIN_ID, "broadcast_queued", f"job_id={job_id}")
    await cb.message.answer(
        (
            "📢 <b>Рассылка поставлена в очередь</b>\n\n"
            f"job_id: <code>{job_id}</code>\n"
            "Отправка идёт в фоне; итог придёт отдельным сообщением.\n"
            "Снимок получателей будет зафиксирован воркером при старте задачи."
        ),
        parse_mode="HTML",
    )
    await cb.answer("Поставлено в очередь")


@router.callback_query(F.data == CB_BROADCAST_CANCEL)
async def broadcast_cancel(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_broadcast(ADMIN_ID)
    await clear_pending_admin_action(ADMIN_ID, BROADCAST_INPUT_ACTION_KEY)
    await write_audit_log(ADMIN_ID, "broadcast_cancel", "")
    await cb.message.answer("❌ Рассылка отменена")
    await cb.answer("Отменено")


@router.message(IsAdmin(), F.text, ~F.text.startswith("/"), HasPendingBroadcastInput())
async def broadcast_capture_text(message: types.Message):
    if message.text.startswith("/"):
        return
    text = message.text.strip()
    if not text:
        await message.answer("Текст пустой. Отправьте сообщение для рассылки или нажмите «Отменить».", reply_markup=get_broadcast_cancel_kb())
        return

    await set_pending_broadcast(ADMIN_ID, text)
    await clear_pending_admin_action(ADMIN_ID, BROADCAST_INPUT_ACTION_KEY)
    users_total = int(await fetchval("SELECT COUNT(*) FROM users"))
    await message.answer(
        (
            "📢 <b>Подтвердите рассылку</b>\n\n"
            f"Получателей (по текущей базе): <b>{users_total}</b>\n\n"
            f"Текст:\n{_build_broadcast_preview(text)}"
        ),
        parse_mode="HTML",
        reply_markup=get_broadcast_confirm_kb(),
    )


@router.callback_query(F.data == CB_ADMIN_REFERRALS)
@router.callback_query(F.data == CB_ADMIN_REFRESH_REFERRALS)
async def admin_referrals_summary(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await cb.message.answer(
        await build_ref_stats_text(),
        parse_mode="HTML",
        reply_markup=get_admin_simple_back_kb(CB_ADMIN_BACK_MAIN, CB_ADMIN_REFRESH_REFERRALS),
    )
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_SERVICE_SETTINGS)
async def admin_service_settings_screen(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    referral_enabled = int(await get_setting("REFERRAL_ENABLED", int) or 0)
    torrent_enabled = int(await get_setting("TORRENT_POLICY_TEXT_ENABLED", int) or 0)
    await cb.message.answer(
        await _render_service_settings_text(),
        parse_mode="HTML",
        reply_markup=get_admin_service_settings_kb(referral_enabled, torrent_enabled),
    )
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_SERVICE_SUPPORT)
async def admin_service_support_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_service_settings_pending()
    await set_pending_admin_action(ADMIN_ID, SERVICE_SUPPORT_INPUT_ACTION_KEY, {"action": SERVICE_SUPPORT_INPUT_ACTION_KEY})
    await cb.message.answer("Введите username поддержки (пример: @support).")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_SERVICE_DOWNLOAD)
async def admin_service_download_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_service_settings_pending()
    await set_pending_admin_action(ADMIN_ID, SERVICE_DOWNLOAD_INPUT_ACTION_KEY, {"action": SERVICE_DOWNLOAD_INPUT_ACTION_KEY})
    await cb.message.answer("Введите ссылку на загрузку (не пустую).")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_SERVICE_REFERRAL_TOGGLE)
async def admin_service_referral_toggle(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    enabled = int(await get_setting("REFERRAL_ENABLED", int) or 0)
    new_value = "0" if enabled == 1 else "1"
    await set_app_setting("REFERRAL_ENABLED", new_value, updated_by=ADMIN_ID)
    await write_audit_log(ADMIN_ID, "admin_referral_enabled_set", f"value={new_value}")
    await admin_service_settings_screen(cb)


@router.callback_query(F.data == CB_ADMIN_SERVICE_INVITEE_BONUS)
async def admin_service_invitee_bonus_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_service_settings_pending()
    await set_pending_admin_action(ADMIN_ID, SERVICE_INVITEE_BONUS_INPUT_ACTION_KEY, {"action": SERVICE_INVITEE_BONUS_INPUT_ACTION_KEY})
    await cb.message.answer("Введите бонус другу в днях (целое > 0).")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_SERVICE_INVITER_BONUS)
async def admin_service_inviter_bonus_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_service_settings_pending()
    await set_pending_admin_action(ADMIN_ID, SERVICE_INVITER_BONUS_INPUT_ACTION_KEY, {"action": SERVICE_INVITER_BONUS_INPUT_ACTION_KEY})
    await cb.message.answer("Введите бонус пригласившему в днях (целое > 0).")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_SERVICE_TORRENT_TOGGLE)
async def admin_service_torrent_toggle(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    enabled = int(await get_setting("TORRENT_POLICY_TEXT_ENABLED", int) or 0)
    new_value = "0" if enabled == 1 else "1"
    await set_app_setting("TORRENT_POLICY_TEXT_ENABLED", new_value, updated_by=ADMIN_ID)
    await write_audit_log(ADMIN_ID, "admin_torrent_policy_text_enabled_set", f"value={new_value}")
    await admin_service_settings_screen(cb)


@router.callback_query(F.data == CB_ADMIN_HEALTH)
@router.callback_query(F.data == CB_ADMIN_REFRESH_HEALTH)
async def admin_health_summary(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await _send_or_edit_admin_message(
        cb,
        await build_runtime_smokecheck_text(),
        get_admin_simple_back_kb(CB_ADMIN_BACK_MAIN, refresh_cb=CB_ADMIN_REFRESH_HEALTH),
    )
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_TEXT_OVERRIDES)
async def admin_text_overrides_screen(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    overrides_count = len(await list_text_overrides())
    await cb.message.answer(
        f"📝 <b>Переопределения текстов</b>\nАктивных переопределений: <b>{overrides_count}</b>",
        parse_mode="HTML",
        reply_markup=get_admin_text_overrides_kb(),
    )
    await cb.answer()


@router.callback_query(F.data.in_(set(TEXT_OVERRIDE_CALLBACK_KEY_MAP.keys())))
async def admin_text_override_view_key(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    key = TEXT_OVERRIDE_CALLBACK_KEY_MAP.get(cb.data)
    if not key:
        await cb.answer("Неизвестный ключ", show_alert=True)
        return
    text = await get_text(key)
    await cb.message.answer(
        f"📝 <b>Шаблон: {key}</b>\n\n{text}",
        parse_mode="HTML",
        reply_markup=get_admin_text_override_item_kb(key),
    )
    await cb.answer()


@router.callback_query(F.data.startswith(CB_ADMIN_TEXT_SET_PREFIX))
async def admin_text_override_set_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_service_settings_pending()
    key = cb.data.removeprefix(CB_ADMIN_TEXT_SET_PREFIX)
    if key not in TEXT_OVERRIDE_ALLOWED_KEYS:
        await cb.answer("Ключ недоступен", show_alert=True)
        return
    await set_pending_admin_action(ADMIN_ID, TEXT_OVERRIDE_INPUT_ACTION_KEY, {"action": TEXT_OVERRIDE_INPUT_ACTION_KEY, "key": key})
    await cb.message.answer(f"Отправьте новый текст для <b>{key}</b>.", parse_mode="HTML")
    await cb.answer()


@router.callback_query(F.data.startswith(CB_ADMIN_TEXT_RESET_PREFIX))
async def admin_text_override_reset(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    key = cb.data.removeprefix(CB_ADMIN_TEXT_RESET_PREFIX)
    if key not in TEXT_OVERRIDE_ALLOWED_KEYS:
        await cb.answer("Ключ недоступен", show_alert=True)
        return
    await reset_text_override(key)
    await write_audit_log(ADMIN_ID, "admin_text_override_reset", f"key={key}")
    await cb.message.answer(
        f"✅ Сброшен шаблон: <b>{key}</b>\n\n{TEXT_DEFAULTS.get(key, '')}",
        parse_mode="HTML",
        reply_markup=get_admin_text_override_item_kb(key),
    )
    await cb.answer("Сброшено")


@router.callback_query(F.data == CB_ADMIN_MAINTENANCE)
@router.callback_query(F.data == CB_ADMIN_MAINTENANCE_REFRESH)
async def admin_maintenance_screen(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    enabled = int(await get_setting("MAINTENANCE_MODE", int) or 0) == 1
    status_line = "🟠 Техработы: ВКЛ" if enabled else "🟢 Техработы: ВЫКЛ"
    await cb.message.answer(status_line, reply_markup=get_admin_maintenance_kb(enabled))
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_MAINTENANCE_ON)
async def admin_maintenance_on_cb(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await set_app_setting("MAINTENANCE_MODE", "1", updated_by=cb.from_user.id)
    await write_audit_log(cb.from_user.id, "maintenance_enabled", "purchase_flow=frozen")
    await cb.message.answer("🟠 Техработы включены.", reply_markup=get_admin_maintenance_kb(True))
    await cb.answer("Включено")


@router.callback_query(F.data == CB_ADMIN_MAINTENANCE_OFF)
async def admin_maintenance_off_cb(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await set_app_setting("MAINTENANCE_MODE", "0", updated_by=cb.from_user.id)
    await write_audit_log(cb.from_user.id, "maintenance_disabled", "purchase_flow=active")
    await cb.message.answer("🟢 Техработы выключены.", reply_markup=get_admin_maintenance_kb(False))
    await cb.answer("Выключено")


@router.callback_query(F.data == CB_ADMIN_PROMOCODES)
async def admin_promocodes_screen(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await clear_pending_admin_action(ADMIN_ID, PROMO_CREATE_INPUT_ACTION_KEY)
    await clear_pending_admin_action(ADMIN_ID, PROMO_DISABLE_INPUT_ACTION_KEY)
    await cb.message.answer("🎟 <b>Промокоды</b>", parse_mode="HTML", reply_markup=get_admin_promocodes_kb())
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_PROMO_LIST)
async def admin_promocodes_list(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    rows = await list_promo_codes(limit=20)
    if not rows:
        await cb.message.answer("Промокодов пока нет.", reply_markup=get_admin_promocodes_kb())
        await cb.answer()
        return
    lines = [f"🎟 <b>Промокоды ({len(rows)})</b>\n"]
    for code, days, max_activations, used_count, is_active, _created_at in rows:
        max_text = str(max_activations) if max_activations is not None else "∞"
        status = "on" if int(is_active) == 1 else "off"
        lines.append(f"• <code>{code}</code> | +{int(days)}д | {int(used_count)}/{max_text} | {status}")
    await cb.message.answer("\n".join(lines), parse_mode="HTML", reply_markup=get_admin_promocodes_kb())
    await cb.answer("Готово")


@router.callback_query(F.data == CB_ADMIN_PROMO_CREATE)
async def admin_promocodes_create_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, PROMO_DISABLE_INPUT_ACTION_KEY)
    await set_pending_admin_action(ADMIN_ID, PROMO_CREATE_INPUT_ACTION_KEY, {"action": PROMO_CREATE_INPUT_ACTION_KEY})
    await cb.message.answer("Формат: CODE DAYS [MAX]")
    await cb.answer()


@router.callback_query(F.data == CB_ADMIN_PROMO_DISABLE)
async def admin_promocodes_disable_start(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    await clear_pending_admin_action(ADMIN_ID, PROMO_CREATE_INPUT_ACTION_KEY)
    await set_pending_admin_action(ADMIN_ID, PROMO_DISABLE_INPUT_ACTION_KEY, {"action": PROMO_DISABLE_INPUT_ACTION_KEY})
    await cb.message.answer("Введите CODE для отключения")
    await cb.answer()


@router.callback_query(F.data.startswith(CB_ADMIN_OPEN_USER_CARD_PREFIX))
async def admin_open_user_card_from_payment(cb: types.CallbackQuery):
    if not await _guard_admin_callback(cb):
        return
    raw = cb.data.removeprefix(CB_ADMIN_OPEN_USER_CARD_PREFIX)
    try:
        uid_raw, page_raw = raw.split("_", 1)
        await _send_user_manage_card(cb.message, int(uid_raw), int(page_raw))
        await cb.answer("Открыто")
    except Exception:
        await cb.answer("Некорректные параметры", show_alert=True)


@router.message(IsAdmin(), F.text, ~F.text.startswith("/"), HasPendingPaymentLookupInput())
async def admin_payment_lookup_capture_input(message: types.Message):
    raw = (message.text or "").strip()
    charge_pending = await get_pending_admin_action(ADMIN_ID, PAYMENT_CHARGE_INPUT_ACTION_KEY)
    user_pending = await get_pending_admin_action(ADMIN_ID, PAYMENT_USER_INPUT_ACTION_KEY)

    if charge_pending:
        if not raw:
            await message.answer("Charge ID не указан.")
            return
        await clear_pending_admin_action(ADMIN_ID, PAYMENT_CHARGE_INPUT_ACTION_KEY)
        payment_summary = await get_payment_summary_by_charge_id(raw)
        if not payment_summary:
            await message.answer("Платёж не найден.", reply_markup=get_admin_payments_kb())
            return
        await message.answer(
            _render_payment_lookup_text(payment_summary),
            parse_mode="HTML",
            reply_markup=get_open_user_card_kb(int(payment_summary["user_id"])),
        )
        return

    if user_pending:
        await clear_pending_admin_action(ADMIN_ID, PAYMENT_USER_INPUT_ACTION_KEY)
        if not raw.isdigit():
            await message.answer("Нужен числовой user_id.")
            return
        uid = int(raw)
        payment_summary = await get_latest_user_payment_summary(uid)
        if not payment_summary:
            await message.answer("Платежей не найдено.", reply_markup=get_admin_payments_kb())
            return
        await message.answer(
            _render_payment_lookup_text(payment_summary),
            parse_mode="HTML",
            reply_markup=get_open_user_card_kb(uid),
        )


@router.message(IsAdmin(), F.text, ~F.text.startswith("/"), HasPendingPromoInput())
async def admin_promo_capture_input(message: types.Message):
    raw = (message.text or "").strip()
    create_pending = await get_pending_admin_action(ADMIN_ID, PROMO_CREATE_INPUT_ACTION_KEY)
    disable_pending = await get_pending_admin_action(ADMIN_ID, PROMO_DISABLE_INPUT_ACTION_KEY)

    if create_pending:
        parts = raw.split()
        if len(parts) < 2:
            await message.answer("Формат: CODE DAYS [MAX]")
            return
        try:
            code = normalize_promo_code(parts[0])
            days = int(parts[1])
            max_activations = int(parts[2]) if len(parts) > 2 else None
            if days <= 0 or (max_activations is not None and max_activations <= 0):
                raise ValueError
            created = await create_promo_code(code, days, max_activations, created_by=message.from_user.id)
            await clear_pending_admin_action(ADMIN_ID, PROMO_CREATE_INPUT_ACTION_KEY)
            if not created:
                await message.answer("⚠️ Такой промокод уже существует.", reply_markup=get_admin_promocodes_kb())
                return
            await write_audit_log(message.from_user.id, "promo_created", f"code={code}; days={days}; max={max_activations or 0}")
            await message.answer(f"✅ Промокод <code>{code}</code> создан.", parse_mode="HTML", reply_markup=get_admin_promocodes_kb())
        except ValueError:
            await message.answer("Ошибка формата. Формат: CODE DAYS [MAX]")
        return

    if disable_pending:
        code = normalize_promo_code(raw)
        if not code:
            await message.answer("Введите корректный CODE.")
            return
        disabled = await disable_promo_code(code)
        await clear_pending_admin_action(ADMIN_ID, PROMO_DISABLE_INPUT_ACTION_KEY)
        if not disabled:
            await message.answer("⚠️ Промокод не найден или уже выключен.", reply_markup=get_admin_promocodes_kb())
            return
        await write_audit_log(message.from_user.id, "promo_disabled", f"code={code}")
        await message.answer(f"✅ Промокод <code>{code}</code> отключён.", parse_mode="HTML", reply_markup=get_admin_promocodes_kb())


@router.message(IsAdmin(), F.text, ~F.text.startswith("/"), HasPendingServiceSettingsInput())
async def admin_service_settings_capture_input(message: types.Message):
    raw = (message.text or "").strip()
    if not raw:
        await message.answer("Пустое значение не сохранено.")
        return
    if await get_pending_admin_action(ADMIN_ID, SERVICE_SUPPORT_INPUT_ACTION_KEY):
        normalized = _normalize_support_username(raw)
        if not normalized:
            await message.answer("Введите корректный username Telegram.")
            return
        save_env_value("SUPPORT_USERNAME", normalized)
        await clear_pending_admin_action(ADMIN_ID, SERVICE_SUPPORT_INPUT_ACTION_KEY)
        await write_audit_log(ADMIN_ID, "admin_support_username_set", f"value={normalized}")
        await message.answer(f"✅ Поддержка: {normalized}")
        return
    if await get_pending_admin_action(ADMIN_ID, SERVICE_DOWNLOAD_INPUT_ACTION_KEY):
        save_env_value("DOWNLOAD_URL", raw)
        await clear_pending_admin_action(ADMIN_ID, SERVICE_DOWNLOAD_INPUT_ACTION_KEY)
        await write_audit_log(ADMIN_ID, "admin_download_url_set", f"value={raw[:120]}")
        await message.answer("✅ Ссылка на загрузку сохранена.")
        return
    invitee_pending = await get_pending_admin_action(ADMIN_ID, SERVICE_INVITEE_BONUS_INPUT_ACTION_KEY)
    inviter_pending = await get_pending_admin_action(ADMIN_ID, SERVICE_INVITER_BONUS_INPUT_ACTION_KEY)
    if not invitee_pending and not inviter_pending:
        return
    if not raw.isdigit() or int(raw) <= 0:
        await message.answer("Нужно положительное целое число.")
        return
    key = "REFERRAL_INVITEE_BONUS_DAYS" if invitee_pending else "REFERRAL_INVITER_BONUS_DAYS"
    action_key = SERVICE_INVITEE_BONUS_INPUT_ACTION_KEY if invitee_pending else SERVICE_INVITER_BONUS_INPUT_ACTION_KEY
    await set_app_setting(key, raw, updated_by=ADMIN_ID)
    await clear_pending_admin_action(ADMIN_ID, action_key)
    await write_audit_log(ADMIN_ID, "admin_referral_bonus_set", f"key={key};value={raw}")
    await message.answer("✅ Значение сохранено.")


@router.message(IsAdmin(), F.text, ~F.text.startswith("/"), HasPendingTextOverrideInput())
async def admin_text_override_capture_input(message: types.Message):
    action = await get_pending_admin_action(ADMIN_ID, TEXT_OVERRIDE_INPUT_ACTION_KEY)
    if not action:
        return
    key = str(action.get("key") or "").strip()
    if key not in TEXT_OVERRIDE_ALLOWED_KEYS:
        await clear_pending_admin_action(ADMIN_ID, TEXT_OVERRIDE_INPUT_ACTION_KEY)
        await message.answer("Ключ недоступен.")
        return
    value = (message.text or "").strip()
    if not value:
        await message.answer("Пустой текст не сохранён.")
        return
    valid, error = await validate_text_template(key, value)
    if not valid:
        await message.answer(f"Шаблон не сохранён: {escape_html(error)}", parse_mode="HTML")
        return
    await set_text_override(key, value, updated_by=ADMIN_ID)
    await clear_pending_admin_action(ADMIN_ID, TEXT_OVERRIDE_INPUT_ACTION_KEY)
    await write_audit_log(ADMIN_ID, "admin_text_override_set", f"key={key}")
    await message.answer(f"✅ Сохранено: <b>{key}</b>", parse_mode="HTML", reply_markup=get_admin_text_override_item_kb(key))


@router.message(IsAdmin(), F.text, ~F.text.startswith("/"), HasPendingNetworkPolicyInput())
async def admin_network_policy_capture_input(message: types.Message):
    raw = (message.text or "").strip()
    deny_domains_pending = await get_pending_admin_action(ADMIN_ID, DENYLIST_DOMAINS_INPUT_ACTION_KEY)
    deny_cidrs_pending = await get_pending_admin_action(ADMIN_ID, DENYLIST_CIDRS_INPUT_ACTION_KEY)
    if not deny_domains_pending and not deny_cidrs_pending:
        return

    if deny_domains_pending:
        normalized = _normalize_domains_multiline(raw)
        await clear_pending_admin_action(ADMIN_ID, DENYLIST_DOMAINS_INPUT_ACTION_KEY)
        await set_app_setting("EGRESS_DENYLIST_DOMAINS", normalized, updated_by=ADMIN_ID)
        await denylist_sync(run_docker)
        await write_audit_log(ADMIN_ID, "admin_denylist_domains_set", f"count={len([x for x in normalized.split(',') if x])}")
        await message.answer("✅ Список доменов denylist обновлён.")
        return

    if deny_cidrs_pending:
        try:
            normalized = _normalize_cidrs_multiline(raw)
            parse_cidrs(normalized)
        except Exception:
            await message.answer("Некорректный CIDR в списке. Настройки не изменены.")
            return
        await clear_pending_admin_action(ADMIN_ID, DENYLIST_CIDRS_INPUT_ACTION_KEY)
        await set_app_setting("EGRESS_DENYLIST_CIDRS", normalized, updated_by=ADMIN_ID)
        await denylist_sync(run_docker)
        await write_audit_log(ADMIN_ID, "admin_denylist_cidrs_set", f"count={len([x for x in normalized.split(',') if x])}")
        await message.answer("✅ Список CIDR denylist обновлён.")
        return

@router.message(Command("give"), IsAdmin())
async def give_manual(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    if admin_command_limited("give", message.from_user.id):
        await message.answer("⏳ Слишком частый вызов /give")
        return
    if not command.args:
        await message.answer("Формат: <code>/give ID [ДНИ]</code>\nПо умолчанию: 30 дней", parse_mode="HTML")
        return
    try:
        parts = command.args.split()
        uid = int(parts[0])
        days = int(parts[1]) if len(parts) > 1 else 30
        if days <= 0:
            await message.answer("Количество дней должно быть больше 0.")
            return
        new_until = await issue_subscription(uid, days)
        notified = await notify_user_subscription_granted(message.bot, uid, days, new_until)
        await write_audit_log(ADMIN_ID, "give", f"target={uid}; days={days}; until={new_until.isoformat()}; notified={int(notified)}")
        await message.answer(
            (
                f"✅ Доступ продлён на {days} дней пользователю <code>{uid}</code>\n"
                f"📅 Действует до: <b>{new_until.strftime('%d.%m.%Y %H:%M')}</b>"
            ),
            parse_mode="HTML",
        )
        if not notified:
            await message.answer("⚠️ Доступ выдан, но уведомление пользователю отправить не удалось.")
    except ValueError:
        await message.answer("Ошибка формата. Пример: <code>/give 123456789 30</code> или <code>/give 123456789</code>", parse_mode="HTML")
    except Exception as e:
        logger.exception("Ошибка /give: %s", e)
        await message.answer("❌ Не удалось выдать доступ.")


@router.message(Command("promo_create"), IsAdmin())
async def promo_create_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    if not command.args:
        await message.answer("Формат: <code>/promo_create CODE DAYS [MAX]</code>", parse_mode="HTML")
        return
    try:
        parts = command.args.split()
        if len(parts) < 2:
            raise ValueError
        code = normalize_promo_code(parts[0])
        days = int(parts[1])
        max_activations = int(parts[2]) if len(parts) > 2 else None
        if days <= 0 or (max_activations is not None and max_activations <= 0):
            raise ValueError
        created = await create_promo_code(code, days, max_activations, created_by=message.from_user.id)
        if not created:
            await message.answer("⚠️ Такой промокод уже существует.")
            return
        await write_audit_log(message.from_user.id, "promo_created", f"code={code}; days={days}; max={max_activations or 0}")
        max_text = str(max_activations) if max_activations is not None else "∞"
        await message.answer(f"✅ Промокод <code>{code}</code> создан: +{days} дней, лимит: {max_text}.", parse_mode="HTML")
    except ValueError:
        await message.answer("Ошибка формата. Пример: <code>/promo_create SPRING10 10 50</code>", parse_mode="HTML")
    except Exception as e:
        logger.exception("Ошибка /promo_create: %s", e)
        await message.answer("❌ Не удалось создать промокод.")


@router.message(Command("promo_list"), IsAdmin())
async def promo_list_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    limit = 20
    if command.args:
        try:
            limit = max(1, min(50, int(command.args)))
        except ValueError:
            pass
    rows = await list_promo_codes(limit=limit)
    if not rows:
        await message.answer("Промокодов пока нет.")
        return
    lines = [f"🎟 <b>Промокоды ({len(rows)})</b>\n"]
    for code, days, max_activations, used_count, is_active, _created_at in rows:
        max_text = str(max_activations) if max_activations is not None else "∞"
        status = "on" if int(is_active) == 1 else "off"
        lines.append(f"• <code>{code}</code> | +{int(days)}д | {int(used_count)}/{max_text} | {status}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("promo_disable"), IsAdmin())
async def promo_disable_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    code = normalize_promo_code(command.args or "")
    if not code:
        await message.answer("Формат: <code>/promo_disable CODE</code>", parse_mode="HTML")
        return
    try:
        disabled = await disable_promo_code(code)
        if not disabled:
            await message.answer("⚠️ Промокод не найден или уже выключен.")
            return
        await write_audit_log(message.from_user.id, "promo_disabled", f"code={code}")
        await message.answer(f"✅ Промокод <code>{code}</code> отключён.", parse_mode="HTML")
    except Exception as e:
        logger.exception("Ошибка /promo_disable: %s", e)
        await message.answer("❌ Не удалось отключить промокод.")


@router.message(Command("revoke"), IsAdmin())
async def revoke_user_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    if not command.args:
        await message.answer("Формат: <code>/revoke ID</code>", parse_mode="HTML")
        return
    try:
        uid = int(command.args)
        await set_pending_admin_action(ADMIN_ID, "revoke", {"action": "revoke", "target": uid})
        await message.answer(
            f"⚠️ Подтвердите отключение пользователя <code>{uid}</code>",
            parse_mode="HTML",
            reply_markup=get_admin_confirm_kb("revoke"),
        )
    except Exception as e:
        logger.exception("Ошибка /revoke: %s", e)
        await message.answer("❌ Не удалось подготовить отключение пользователя")


@router.message(Command("users"), IsAdmin())
async def list_users_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    rows = await fetchall("SELECT user_id, sub_until FROM users ORDER BY created_at DESC LIMIT 50")
    if not rows:
        await message.answer("Пользователей пока нет.")
        return
    lines = ["👥 <b>Последние пользователи</b>\n"]
    for uid, sub_until in rows:
        status_text, until_text = get_status_text(sub_until)
        tg_username, _ = await get_user_meta(uid)
        lines.append(f"• <code>{uid}</code> — {format_tg_username(tg_username)} — {status_text} — {until_text}")
    await message.answer("\n".join(lines), parse_mode="HTML")


@router.message(Command("finduser"), IsAdmin())
async def find_user_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    query = (command.args or "").strip()
    if not query:
        await message.answer("Формат: <code>/finduser QUERY</code>\nQUERY: user_id или username/@username", parse_mode="HTML")
        return
    if query.isdigit():
        uid = int(query)
        row = await fetchone("SELECT 1 FROM users WHERE user_id = ?", (uid,))
        if not row:
            await message.answer("Пользователь с таким user_id не найден.")
            return
        await _send_user_manage_card(message, uid, 0)
        return

    needle = query.lstrip("@").lower()
    exact_rows = await fetchall(
        """
        SELECT user_id, tg_username
        FROM users
        WHERE LOWER(COALESCE(tg_username, '')) = ?
        ORDER BY created_at DESC
        LIMIT 3
        """,
        (needle,),
    )
    if exact_rows:
        uid = int(exact_rows[0][0])
        await _send_user_manage_card(message, uid, 0)
        return

    rows = await fetchall(
        """
        SELECT user_id, tg_username
        FROM users
        WHERE LOWER(COALESCE(tg_username, '')) LIKE ?
        ORDER BY created_at DESC
        LIMIT 5
        """,
        (f"%{needle}%",),
    )
    if not rows:
        await message.answer("Совпадений не найдено.")
        return
    kb_rows = [
        [
            types.InlineKeyboardButton(
                text=f"👤 {uid} — {format_tg_username(username)}",
                callback_data=f"{CB_ADMIN_MANAGE_USER_PREFIX}{uid}_0",
            )
        ]
        for uid, username in rows
    ]
    await message.answer(
        "Найдено несколько пользователей. Откройте карточку:",
        reply_markup=types.InlineKeyboardMarkup(inline_keyboard=kb_rows),
    )


@router.message(Command("payinfo"), IsAdmin())
async def payinfo_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    if not command.args or not command.args.strip().isdigit():
        await message.answer("Формат: <code>/payinfo USER_ID</code>", parse_mode="HTML")
        return
    uid = int(command.args.strip())
    payment_summary = await get_latest_user_payment_summary(uid)
    if not payment_summary:
        await message.answer("Платежей не найдено.")
        return
    await message.answer(_render_payment_lookup_text(payment_summary), parse_mode="HTML", reply_markup=get_open_user_card_kb(uid))


@router.message(Command("findpay"), IsAdmin())
async def findpay_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    charge_id = (command.args or "").strip()
    if not charge_id:
        await message.answer("Формат: <code>/findpay CHARGE_ID</code>", parse_mode="HTML")
        return
    payment_summary = await get_payment_summary_by_charge_id(charge_id)
    if not payment_summary:
        await message.answer("Платёж не найден.")
        return
    await message.answer(
        _render_payment_lookup_text(payment_summary),
        parse_mode="HTML",
        reply_markup=get_open_user_card_kb(int(payment_summary["user_id"])),
    )


@router.message(Command("stats"), IsAdmin())
async def stats_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await message.answer(await build_stats_text(), parse_mode="HTML")


@router.message(Command("audit"), IsAdmin())
async def audit_cmd(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    limit = 20
    if command.args:
        try:
            limit = max(1, min(100, int(command.args)))
        except ValueError:
            pass
    try:
        rows = await get_recent_audit(limit=limit)
        if not rows:
            await message.answer("Журнал действий пуст.")
            return
        lines = [f"📜 <b>Последние события ({len(rows)})</b>\n"]
        for row_id, user_id, action, details, created_at in rows:
            lines.append(
                f"#{row_id} | <code>{user_id}</code> | <b>{action}</b>\n"
                f"{created_at}\n"
                f"{details or '-'}\n"
            )
        await message.answer("\n".join(lines), parse_mode="HTML")
    except Exception as e:
        logger.exception("Ошибка /audit: %s", e)
        await message.answer("❌ Не удалось получить audit log.")


@router.message(Command("sync_awg"), IsAdmin())
async def sync_awg_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    try:
        await message.answer(await build_awg_sync_text(), parse_mode="HTML")
    except Exception as e:
        logger.exception("Ошибка /sync_awg: %s", e)
        await message.answer("❌ Ошибка проверки AWG.")


@router.message(Command("send"), IsAdmin())
async def broadcast_prepare(message: types.Message, command: CommandObject):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    if admin_command_limited("send", message.from_user.id):
        await message.answer("⏳ Слишком частый вызов /send")
        return
    if not command.args:
        await clear_pending_broadcast(ADMIN_ID)
        await set_pending_admin_action(ADMIN_ID, BROADCAST_INPUT_ACTION_KEY, {"action": BROADCAST_INPUT_ACTION_KEY})
        await message.answer(
            "Отправьте текст рассылки одним сообщением.",
            reply_markup=get_broadcast_cancel_kb(),
        )
        return
    text = command.args.strip()
    if not text:
        await message.answer("Текст пустой. Отправьте сообщение после <code>/send</code>.", parse_mode="HTML")
        return
    await set_pending_broadcast(ADMIN_ID, text)
    await clear_pending_admin_action(ADMIN_ID, BROADCAST_INPUT_ACTION_KEY)
    users_total = int(await fetchval("SELECT COUNT(*) FROM users"))
    await message.answer(
        (
            "📢 <b>Подтвердите рассылку</b>\n\n"
            f"Получателей (по текущей базе): <b>{users_total}</b>\n\n"
            f"Текст:\n{_build_broadcast_preview(text)}"
        ),
        parse_mode="HTML",
        reply_markup=get_broadcast_confirm_kb(),
    )


@router.message(Command("health"), IsAdmin())
async def health_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await message.answer(await build_runtime_smokecheck_text(), parse_mode="HTML")


@router.message(Command("maintenance_status"), IsAdmin())
async def maintenance_status_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    enabled = int(await get_setting("MAINTENANCE_MODE", int) or 0) == 1
    status_line = "🟠 Техработы: ВКЛ (покупки заморожены)" if enabled else "🟢 Техработы: ВЫКЛ (покупки доступны)"
    await message.answer(status_line)


@router.message(Command("maintenance_on"), IsAdmin())
async def maintenance_on_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    enabled = int(await get_setting("MAINTENANCE_MODE", int) or 0) == 1
    if enabled:
        await message.answer("🟠 Техработы уже включены: новые покупки заморожены.")
        return
    await set_app_setting("MAINTENANCE_MODE", "1", updated_by=message.from_user.id)
    await write_audit_log(message.from_user.id, "maintenance_enabled", "purchase_flow=frozen")
    await message.answer(
        "🟠 Техработы включены: новые покупки временно заморожены.\n"
        "💡 При необходимости отправьте ручной broadcast через /send."
    )


@router.message(Command("maintenance_off"), IsAdmin())
async def maintenance_off_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    enabled = int(await get_setting("MAINTENANCE_MODE", int) or 0) == 1
    if not enabled:
        await message.answer("🟢 Техработы уже выключены: покупки доступны.")
        return
    await set_app_setting("MAINTENANCE_MODE", "0", updated_by=message.from_user.id)
    await write_audit_log(message.from_user.id, "maintenance_disabled", "purchase_flow=active")
    await message.answer("🟢 Техработы выключены: новые покупки снова доступны.")


@router.message(Command("netpolicy"), IsAdmin())
async def netpolicy_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await message.answer(await _render_network_policy_text(), parse_mode="HTML", reply_markup=get_admin_network_policy_kb())


@router.message(Command("denylist_status"), IsAdmin())
async def denylist_status_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    enabled = int(await get_setting("EGRESS_DENYLIST_ENABLED", int) or 0)
    mode = str(await get_setting("EGRESS_DENYLIST_MODE", str) or "soft")
    refresh_minutes = int(await get_setting("EGRESS_DENYLIST_REFRESH_MINUTES", int) or 30)
    metrics = await policy_metrics()
    history_block = "\n".join(_denylist_history_block(enabled=enabled, metrics=metrics))
    await message.answer(
        (
            "🛡 <b>Статус denylist</b>\n"
            f"Denylist: <b>{_bool_on_off(enabled)}</b>\n"
            f"Режим: <b>{escape_html(mode)}</b>\n"
            f"Обновление: <b>{refresh_minutes} мин</b>\n"
            f"{history_block}"
        ),
        parse_mode="HTML",
    )


@router.message(Command("denylist_sync"), IsAdmin())
async def denylist_sync_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await denylist_sync(run_docker)
    await write_audit_log(message.from_user.id, "admin_denylist_sync", "manual_sync=1")
    await message.answer("✅ Синхронизация denylist выполнена.")


@router.message(Command("ref_stats"), IsAdmin())
async def ref_stats_cmd(message: types.Message):
    await _clear_network_policy_pending()
    await _clear_service_settings_pending()
    await message.answer(await build_ref_stats_text(), parse_mode="HTML")
