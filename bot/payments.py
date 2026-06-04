import asyncio
import json
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Awaitable, Callable

from aiogram import Bot, F, Router, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, LabeledPrice, PreCheckoutQuery

from awg_backend import check_awg_container, issue_subscription
import config
from config import (
    AWG_HELPER_POLICY_PATH,
    DOCKER_CONTAINER,
    ADMIN_ID,
    PAYMENT_MAX_ATTEMPTS,
    PAYMENT_PROVISIONING_LEASE_SECONDS,
    PAYMENT_RETRY_DELAY_SECONDS,
    CONFIGS_PER_USER,
    PURCHASE_CLICK_COOLDOWN_SECONDS,
    PURCHASE_RATE_LIMIT_TTL_SECONDS,
    WG_INTERFACE,
    PLATEGA_MERCHANT_ID,
    PLATEGA_SECRET_KEY,
    logger,
)
from platega_integration import PLATEGA_METHOD_SBP_QR
from config_validate import read_helper_policy
from database import (
    claim_payment_and_job_for_provisioning,
    db_health_info,
    ensure_user_exists,
    fetchone,
    finalize_payment_and_job,
    mark_ready_notification_sent,
    get_provisioning_attempt_count,
    get_payment_status,
    get_repairable_payments,
    update_last_provision_status,
    mark_payment_stuck_manual,
    payment_already_processed,
    persistent_guard_hit,
    save_payment,
    upsert_payment_precheck,
    mark_payment_precheck_status,
    update_payment_status,
    update_payment_status_by_order,
    write_audit_log,
    get_user_keys,
    get_payment_by_order,
)
from helpers import utc_now_naive
from keyboards import (
    get_buy_confirm_kb,
    get_buy_inline_kb,
    get_payment_method_selection_kb,
    get_platega_payment_kb,
    get_post_payment_kb,
)
from content_settings import get_text
from referrals import (
    apply_referral_recurring_inviter_reward,
    apply_referral_rewards_on_first_payment,
    notify_inviter_about_referral_recurring_reward,
    notify_inviter_about_referral_reward,
)
from texts import get_payment_result_text
from ui_constants import (
    CB_BUY_30, CB_BUY_7, CB_BUY_90, CB_BUY_PAY_30, CB_BUY_PAY_7, CB_BUY_PAY_90,
    CB_PLATEGA_BUY_30, CB_PLATEGA_BUY_7, CB_PLATEGA_BUY_90,
    CB_SHOW_BUY_MENU, CB_PAY_STARS_PREFIX, CB_PAY_PLATEGA_PREFIX,
    CB_PLATEGA_PAY_PREFIX, CB_PLATEGA_CHECK_PREFIX,
    CB_TARIFF_7, CB_TARIFF_30, CB_TARIFF_90,
    CB_OPEN_SUPPORT, CB_OPEN_CONFIGS, CB_CHECK_ACTIVATION_STATUS,
)
from maintenance import get_purchase_maintenance_text, is_purchase_maintenance_enabled
from platega_service import platega_service
from platega_integration import PlategaPaymentService

router = Router()
purchase_rate_limit: dict[int, object] = {}
pending_invoices: dict[int, dict[str, int | str]] = {}
_checkout_readiness_cache = {"ok": True, "reason": "", "expires_at": None}
CHECKOUT_READINESS_TTL_SECONDS = 12
CRITICAL_ERRORS_LOG = Path(config.DB_PATH).resolve().parent / "critical_errors.log"

# Initialize Platega payment service (lazy - will be used when credentials are available)
_platega_service: PlategaPaymentService | None = None


def get_platega_service() -> PlategaPaymentService | None:
    """Get Platega payment service instance if credentials are configured."""
    global _platega_service
    if not PLATEGA_MERCHANT_ID or not PLATEGA_SECRET_KEY:
        return None
    if _platega_service is None:
        _platega_service = PlategaPaymentService(
            merchant_id=PLATEGA_MERCHANT_ID,
            secret=PLATEGA_SECRET_KEY,
        )
    return _platega_service


def get_tariffs_stars() -> dict[str, dict[str, int | str]]:
    return {
        "sub_7": {"days": 7, "amount": int(config.STARS_PRICE_7_DAYS), "currency": "XTR", "method": "stars"},
        "sub_30": {"days": 30, "amount": int(config.STARS_PRICE_30_DAYS), "currency": "XTR", "method": "stars"},
        "sub_90": {"days": 90, "amount": int(config.STARS_PRICE_90_DAYS), "currency": "XTR", "method": "stars"},
    }


def get_tariffs_platega() -> dict[str, dict[str, int | str]]:
    return {
        "sub_7": {"days": 7, "amount": int(config.PLATEGA_PRICE_7_DAYS), "currency": "RUB", "method": "platega"},
        "sub_30": {"days": 30, "amount": int(config.PLATEGA_PRICE_30_DAYS), "currency": "RUB", "method": "platega"},
        "sub_90": {"days": 90, "amount": int(config.PLATEGA_PRICE_90_DAYS), "currency": "RUB", "method": "platega"},
    }


def get_tariffs() -> dict[str, dict[str, int | str]]:
    """Объединяет все тарифы (Stars + Platega) для совместимости."""
    tariffs = get_tariffs_stars()
    tariffs.update(get_tariffs_platega())
    return tariffs


def _cleanup_purchase_rate_limit(now):
    stale = [uid for uid, dt in purchase_rate_limit.items() if (now - dt).total_seconds() > PURCHASE_RATE_LIMIT_TTL_SECONDS]
    for uid in stale:
        purchase_rate_limit.pop(uid, None)


def is_purchase_rate_limited(user_id: int) -> tuple[bool, int]:
    now = utc_now_naive()
    _cleanup_purchase_rate_limit(now)
    last = purchase_rate_limit.get(user_id)
    if not last:
        purchase_rate_limit[user_id] = now
        return False, 0
    delta = (now - last).total_seconds()
    if delta < PURCHASE_CLICK_COOLDOWN_SECONDS:
        return True, int(PURCHASE_CLICK_COOLDOWN_SECONDS - delta) + 1
    purchase_rate_limit[user_id] = now
    return False, 0


async def is_purchase_rate_limited_persistent(user_id: int, action: str) -> tuple[bool, int]:
    hit = await persistent_guard_hit("purchase", user_id, action, PURCHASE_CLICK_COOLDOWN_SECONDS)
    if hit:
        return True, PURCHASE_CLICK_COOLDOWN_SECONDS
    return False, 0


def remember_pending_invoice(user_id: int, chat_id: int, message_id: int, payload: str) -> None:
    pending_invoices[user_id] = {
        "chat_id": chat_id,
        "message_id": message_id,
        "payload": payload,
    }


def clear_pending_invoice_state(user_id: int) -> None:
    pending_invoices.pop(user_id, None)


async def clear_pending_invoice_for_user(bot: Bot, user_id: int) -> bool:
    pending = pending_invoices.pop(user_id, None)
    if not pending:
        return False
    chat_id = int(pending["chat_id"])
    message_id = int(pending["message_id"])
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramBadRequest as error:
        logger.debug("Pending invoice cleanup skipped for user=%s: %s", user_id, error)
    except Exception as error:
        logger.warning("Pending invoice cleanup failed for user=%s: %s", user_id, error)
    return True


async def _send_stars_invoice(bot: Bot, chat_id: int, payload: str, title: str, label: str, amount: int) -> types.Message:
    return await bot.send_invoice(
        chat_id=chat_id,
        title=title,
        description=f"Доступ для {CONFIGS_PER_USER} устройств",
        payload=payload,
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label=label, amount=amount)],
    )


async def _send_or_edit_payment_screen(cb: types.CallbackQuery, text: str, *, reply_markup=None) -> None:
    message = cb.message
    # Безопасно отвечаем на callback, чтобы бот не падал при таймаутах
    try:
        await cb.answer(show_alert=False)
    except TelegramBadRequest:
        pass
    except Exception as e:
        logger.debug("Unexpected error answering callback: %s", e)

    if message is not None and hasattr(message, "edit_text"):
        try:
            await message.edit_text(text=text, parse_mode="HTML", reply_markup=reply_markup)
            return
        except TelegramBadRequest as error:
            error_str = str(error).lower()
            if "message is not modified" in error_str:
                return
            if "message to edit not found" in error_str or "have no rights" in error_str:
                pass  # Сообщение удалено - отправим новое
            else:
                logger.debug("Payment screen edit fallback: %s", error)
        except Exception as error:
            logger.warning("Payment screen edit unexpected error: %s", error)
    if message is not None:
        try:
            await message.answer(text, parse_mode="HTML", reply_markup=reply_markup)
        except TelegramBadRequest as send_error:
            logger.warning("Payment screen send fallback failed: %s", send_error)


async def _show_buy_confirmation(cb: types.CallbackQuery, payload: str, method: str = "stars") -> None:
    if method == "platega":
        tariffs = get_tariffs_platega()
    else:
        tariffs = get_tariffs_stars()
    if payload not in tariffs:
        logger.warning("Unknown tariff payload in buy confirmation: payload=%s method=%s", payload, method)
        await cb.answer("Неизвестный тариф", show_alert=False)
        await _send_or_edit_payment_screen(cb, "Неизвестный тариф. Выберите тариф заново.", reply_markup=get_buy_inline_kb())
        return
    tariff_info = {
        "sub_7": {"days": 7, "stars": get_tariffs_stars()["sub_7"]["amount"], "rub": config.PLATEGA_PRICE_7_DAYS},
        "sub_30": {"days": 30, "stars": get_tariffs_stars()["sub_30"]["amount"], "rub": config.PLATEGA_PRICE_30_DAYS},
        "sub_90": {"days": 90, "stars": get_tariffs_stars()["sub_90"]["amount"], "rub": config.PLATEGA_PRICE_90_DAYS},
    }
    info = tariff_info.get(payload, tariff_info["sub_30"])
    
    text = await get_text(
        "payment_method_selection",
        tariff_days=info["days"],
        stars_price=info["stars"],
        rub_price=info["rub"],
        configs_per_user=CONFIGS_PER_USER,
    )
    await _send_or_edit_payment_screen(cb, text, reply_markup=get_buy_confirm_kb(payload, method=method))


async def checkout_readiness() -> tuple[bool, str]:
    now_ts = utc_now_naive().timestamp()
    expires_at = _checkout_readiness_cache.get("expires_at")
    if isinstance(expires_at, (int, float)) and now_ts < expires_at:
        return bool(_checkout_readiness_cache["ok"]), str(_checkout_readiness_cache["reason"])
    ok = True
    reason = ""
    try:
        if not DOCKER_CONTAINER or not WG_INTERFACE:
            raise RuntimeError("missing_awg_target")
        policy_container, policy_interface, policy_error = read_helper_policy(Path(AWG_HELPER_POLICY_PATH))
        if policy_error:
            raise RuntimeError(policy_error)
        if policy_container != DOCKER_CONTAINER or policy_interface != WG_INTERFACE:
            raise RuntimeError("helper policy mismatch")
        db_info = await db_health_info()
        if not db_info.get("is_healthy"):
            raise RuntimeError("database_not_ready")
        await check_awg_container()
    except Exception as e:
        ok = False
        reason = str(e)[:200]
    _checkout_readiness_cache["ok"] = ok
    _checkout_readiness_cache["reason"] = reason
    _checkout_readiness_cache["expires_at"] = now_ts + CHECKOUT_READINESS_TTL_SECONDS
    return ok, reason


@router.callback_query(F.data.startswith(CB_PAY_STARS_PREFIX))
async def pay_stars_handler(cb: types.CallbackQuery, bot: Bot):
    """Handle Stars payment method selection."""
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    
    payload = cb.data.split(":", 1)[1]
    tariff = get_tariffs_stars().get(payload)
    if not tariff:
        await _send_or_edit_payment_screen(cb, "❌ Ошибка\n\nНеизвестный тариф. Попробуйте выбрать другой.")
        return
    
    await cb.answer(show_alert=False)
    await clear_pending_invoice_for_user(bot, cb.from_user.id)
    
    # Проверка на доступность сообщения
    if cb.message is None:
        logger.debug("Stars payment: message is None for user=%s", cb.from_user.id)
        await cb.bot.send_message(
            cb.from_user.id,
            "⚠️ Не удалось создать счёт: сообщение недоступно. Попробуйте снова.",
            reply_markup=get_buy_inline_kb(),
        )
        return
    
    invoice_message = await _send_stars_invoice(
        bot, 
        cb.message.chat.id, 
        payload, 
        f"Свободный Интернет на {tariff['days']} дней",
        f"{tariff['days']} дней доступа",
        int(tariff["amount"]),
    )
    remember_pending_invoice(cb.from_user.id, cb.message.chat.id, invoice_message.message_id, payload)


@router.callback_query(F.data.startswith(CB_PAY_PLATEGA_PREFIX))
async def pay_platega_handler(cb: types.CallbackQuery, bot: Bot):
    """Handle Platega payment method selection - create payment transaction."""
    # Сразу отвечаем на callback, чтобы Telegram не считал его устаревшим
    try:
        await cb.answer(show_alert=False)
    except TelegramBadRequest:
        pass

    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    
    platega_service = get_platega_service()
    if not platega_service:
        await _send_or_edit_payment_screen(cb, "❌ Оплата через СБП временно недоступна\n\nОбратитесь к администратору или выберите другой способ оплаты.")
        logger.warning("Platega credentials not configured")
        return
    
    payload = cb.data.split(":", 1)[1]
    tariff_info = {
        "sub_7": {"days": 7, "rub": config.PLATEGA_PRICE_7_DAYS},
        "sub_30": {"days": 30, "rub": config.PLATEGA_PRICE_30_DAYS},
        "sub_90": {"days": 90, "rub": config.PLATEGA_PRICE_90_DAYS},
    }
    info = tariff_info.get(payload)
    if not info:
        await _send_or_edit_payment_screen(cb, "❌ Ошибка\n\nНеизвестный тариф. Попробуйте выбрать другой.")
        return
    
    try:
        # Формируем order_id для передачи в Platega (будет возвращен в callback)
        order_id = f"{cb.from_user.id}:{payload}"
        
        # Формируем URL для webhook callback
        webhook_domain = config.PLATEGA_WEBHOOK_DOMAIN or config.PUBLIC_HOST
        if webhook_domain:
            return_url = f"https://{webhook_domain}/webhook?status=success"
            failed_url = f"https://{webhook_domain}/webhook?status=failed"
        else:
            return_url = None
            failed_url = None
        
        # Create Platega payment transaction
        payment_result = await platega_service.create_payment(
            amount=float(info["rub"]),
            currency="RUB",
            description=f"Подписка на {info['days']} дней",
            payload=order_id,  # Передаем order_id как payload для callback
            return_url=return_url,
            failed_url=failed_url,
            payment_method=PLATEGA_METHOD_SBP_QR,
        )
        
        if not payment_result:
            raise RuntimeError("Failed to create Platega payment - empty response")
        
        transaction_id = payment_result["transaction_id"]
        payment_url = payment_result.get("payment_url", "")
        
        # Save payment to database with order_id for later lookup in webhook
        await save_payment(
            telegram_payment_charge_id=transaction_id,
            provider_payment_charge_id="",
            user_id=cb.from_user.id,
            payload=payload,
            amount=int(info["rub"]),
            currency="RUB",
            payment_method="platega",
            status="pending",
            raw_payload_json=json.dumps(payment_result),
        )
        
        # Show payment button with redirect URL
        text = await get_text(
            "platega_payment_pending",
            amount=info["rub"],
            days=info["days"],
        )
        
        if cb.message is not None:
            await cb.message.edit_text(
                text=text,
                parse_mode="HTML",
                reply_markup=get_platega_payment_kb(transaction_id, payload, payment_url),
            )
        else:
            logger.debug("pay_platega_handler: message is None for user=%s", cb.from_user.id)
            await cb.bot.send_message(
                cb.from_user.id,
                text=text,
                parse_mode="HTML",
                reply_markup=get_platega_payment_kb(transaction_id, payload, payment_url),
            )
        
    except Exception as e:
        logger.exception("Failed to create Platega payment: %s", e)
        await _send_or_edit_payment_screen(cb, "Не удалось создать платёж. Попробуйте позже или выберите другой способ оплаты.")


@router.callback_query(F.data.startswith(CB_PLATEGA_PAY_PREFIX))
async def platega_pay_button_handler(cb: types.CallbackQuery):
    """Handle click on Platega payment button - send redirect URL."""
    try:
        await cb.answer(show_alert=False)
    except TelegramBadRequest:
        pass
        
    data = cb.data.split(":", 1)[1]
    parts = data.split(":", 1)
    transaction_id = parts[0]
    payload = parts[1] if len(parts) > 1 else ""
    
    platega_service = get_platega_service()
    if not platega_service:
        await _send_or_edit_payment_screen(cb, "❌ Оплата через СБП временно недоступна\n\nПопробуйте позже или выберите другой способ оплаты.")
        return
    
    try:
        # Get payment status to retrieve redirect URL
        await platega_service.check_payment_status(transaction_id)
        
        # Send redirect URL to user
        tariff_info = {
            "sub_7": {"days": 7, "rub": config.PLATEGA_PRICE_7_DAYS},
            "sub_30": {"days": 30, "rub": config.PLATEGA_PRICE_30_DAYS},
            "sub_90": {"days": 90, "rub": config.PLATEGA_PRICE_90_DAYS},
        }
        info = tariff_info.get(payload, {"days": 30, "rub": 0})
        
        text = (
            f"💳 <b>Оплата {info['days']} дней — {info['rub']}₽</b>\n\n"
            f"Перейдите по ссылке для оплаты:\n"
            f"<code>{"https://pay.platega.io/?id=" + transaction_id + "&mh=" + config.PLATEGA_MERCHANT_ID}</code>\n\n"
            f"Или нажмите кнопку ниже:"
        )
        
        redirect_url = f"https://pay.platega.io/?id={transaction_id}&mh={config.PLATEGA_MERCHANT_ID}"
        kb_rows = []
        if redirect_url and redirect_url.strip():
            kb_rows.append([
                types.InlineKeyboardButton(
                    text="💳 Открыть страницу оплаты",
                    url=redirect_url,
                )
            ])
        reply_markup = types.InlineKeyboardMarkup(inline_keyboard=kb_rows) if kb_rows else None
        
        await _send_or_edit_payment_screen(cb, text, reply_markup=reply_markup)
    except Exception as e:
        logger.exception("Failed to get Platega payment URL: %s", e)
        await _send_or_edit_payment_screen(cb, "Не удалось получить ссылку на оплату. Попробуйте позже.")


@router.callback_query(F.data.startswith(CB_PLATEGA_CHECK_PREFIX))
async def platega_check_status_handler(cb: types.CallbackQuery, bot: Bot):
    """Handle Platega payment status check."""
    transaction_id = cb.data.split(":", 1)[1]
    await _platega_check_status_logic(cb, transaction_id)


async def platega_check_status_by_id(cb: types.CallbackQuery, transaction_id: str):
    """Check Platega payment status by transaction ID (for external calls)."""
    await _platega_check_status_logic(cb, transaction_id)


async def _platega_check_status_logic(cb: types.CallbackQuery, transaction_id: str):
    """Internal logic for checking Platega payment status."""
    try:
        await cb.answer("⏳ Проверяю статус...", show_alert=False)
    except TelegramBadRequest:
        pass
        
    platega_service = get_platega_service()
    if not platega_service:
        await _send_or_edit_payment_screen(cb, "❌ Сервис оплаты временно недоступен\n\nПопробуйте позже.")
        return
    
    try:
        status_result = await platega_service.check_payment_status(transaction_id)
        status = status_result["status"]
        
        # Get payment from database
        from database import fetchone
        row = await fetchone(
            "SELECT payload FROM payments WHERE telegram_payment_charge_id = ?",
            (transaction_id,)
        )
        
        if not row:
            await _send_or_edit_payment_screen(cb, "Платёж не найден в базе данных.")
            return
        
        payload = str(row[0])
        
        if platega_service.is_success_status(status):
            # Payment successful - update status but don't issue key yet
            await update_payment_status(transaction_id, "received")
            
            # Show success message with connect button
            tariff = get_tariffs().get(payload)
            if not tariff:
                await _send_or_edit_payment_screen(cb, "Неизвестный тариф")
                return
            
            success_text = await get_text("payment_success")
            next_step_text = await get_text("payment_next_step", configs_per_user=CONFIGS_PER_USER)
            
            if cb.message is not None:
                await cb.message.edit_text(
                    text=f"{success_text}\n{next_step_text}",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="🔑 Подключиться", callback_data=CB_OPEN_CONFIGS)],
                            [InlineKeyboardButton(text="⏱ Проверить статус активации", callback_data=CB_CHECK_ACTIVATION_STATUS)],
                            [InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)],
                        ]
                    ),
                )
            else:
                logger.debug("platega_check_status_handler: message is None for user=%s", cb.from_user.id)
                await cb.bot.send_message(
                    cb.from_user.id,
                    f"{success_text}\n{next_step_text}",
                    parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup(
                        inline_keyboard=[
                            [InlineKeyboardButton(text="🔑 Подключиться", callback_data=CB_OPEN_CONFIGS)],
                            [InlineKeyboardButton(text="⏱ Проверить статус активации", callback_data=CB_CHECK_ACTIVATION_STATUS)],
                            [InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)],
                        ]
                    ),
                )
        
        elif platega_service.is_pending_status(status):
            sbp_url = f"https://pay.platega.io/?id={transaction_id}&mh={config.PLATEGA_MERCHANT_ID}"
            await _send_or_edit_payment_screen(
                cb, 
                f"⏳ Платёж ещё в обработке. Ожидайте подтверждения от банка.\nСсылка на оплату: <a href='{sbp_url}'>оплатить через СБП</a>",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Оплатить через СБП", url=sbp_url)],
                        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"{CB_PLATEGA_CHECK_PREFIX}{transaction_id}")],
                        [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_SHOW_BUY_MENU)]
                    ]
                )
            )
        
        elif platega_service.is_failed_status(status):
            await update_payment_status(transaction_id, "failed")
            sbp_url = f"https://pay.platega.io/?id={transaction_id}&mh={config.PLATEGA_MERCHANT_ID}"
            await _send_or_edit_payment_screen(
                cb, 
                f"❌ Платёж отменён или не удался. Попробуйте снова.\nСсылка на оплату: <a href='{sbp_url}'>оплатить через СБП</a>",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Оплатить через СБП", url=sbp_url)],
                        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"{CB_PLATEGA_CHECK_PREFIX}{transaction_id}")],
                        [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_SHOW_BUY_MENU)]
                    ]
                )
            )
        
        else:
            sbp_url = f"https://pay.platega.io/?id={transaction_id}&mh={config.PLATEGA_MERCHANT_ID}"
            await _send_or_edit_payment_screen(
                cb, 
                f"Статус платежа: {status}\nСсылка на оплату: <a href='{sbp_url}'>оплатить через СБП</a>",
                reply_markup=InlineKeyboardMarkup(
                    inline_keyboard=[
                        [InlineKeyboardButton(text="💳 Оплатить через СБП", url=sbp_url)],
                        [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"{CB_PLATEGA_CHECK_PREFIX}{transaction_id}")],
                        [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_SHOW_BUY_MENU)]
                    ]
                )
            )
            
    except Exception as e:
        logger.exception("Failed to check Platega payment status: %s", e)
        await _send_or_edit_payment_screen(cb, "Не удалось проверить статус платежа. Попробуйте позже.")


@router.callback_query(F.data == CB_TARIFF_7)
async def tariff_7_days(cb: types.CallbackQuery):
    """Выбор тарифа 7 дней -> показ способов оплаты."""
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _show_payment_method_selection(cb, "sub_7")


@router.callback_query(F.data == CB_TARIFF_30)
async def tariff_30_days(cb: types.CallbackQuery):
    """Выбор тарифа 30 дней -> показ способов оплаты."""
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _show_payment_method_selection(cb, "sub_30")


@router.callback_query(F.data == CB_TARIFF_90)
async def tariff_90_days(cb: types.CallbackQuery):
    """Выбор тарифа 90 дней -> показ способов оплаты."""
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _show_payment_method_selection(cb, "sub_90")


async def _show_payment_method_selection(cb: types.CallbackQuery, payload: str) -> None:
    """Показать выбор способа оплаты для выбранного тарифа."""
    tariff_info = {
        "sub_7": {"days": 7, "stars": config.STARS_PRICE_7_DAYS, "rub": config.PLATEGA_PRICE_7_DAYS},
        "sub_30": {"days": 30, "stars": config.STARS_PRICE_30_DAYS, "rub": config.PLATEGA_PRICE_30_DAYS},
        "sub_90": {"days": 90, "stars": config.STARS_PRICE_90_DAYS, "rub": config.PLATEGA_PRICE_90_DAYS},
    }
    info = tariff_info.get(payload, tariff_info["sub_30"])
    
    text = (
        f"<b>📦 Тариф: {info['days']} дней</b>\n\n"
        f"💳 <b>Способы оплаты:</b>\n"
        f"• Telegram Stars — {info['stars']}⭐\n"
        f"• СБП (QR) — {info['rub']}₽\n\n"
        f"Выберите удобный способ оплаты:"
    )
    await _send_or_edit_payment_screen(cb, text, reply_markup=get_payment_method_selection_kb(payload))


@router.callback_query(F.data == CB_BUY_7)
async def buy_7_days(cb: types.CallbackQuery):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _show_buy_confirmation(cb, "sub_7", method="stars")


@router.callback_query(F.data == CB_BUY_30)
async def buy_30_days(cb: types.CallbackQuery):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _show_buy_confirmation(cb, "sub_30", method="stars")


@router.callback_query(F.data == CB_BUY_90)
async def buy_90_days(cb: types.CallbackQuery):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _show_buy_confirmation(cb, "sub_90", method="stars")


@router.callback_query(F.data == CB_PLATEGA_BUY_7)
async def platega_buy_7_days(cb: types.CallbackQuery, bot: Bot):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _create_platega_payment(cb, bot, "sub_7")


@router.callback_query(F.data == CB_PLATEGA_BUY_30)
async def platega_buy_30_days(cb: types.CallbackQuery, bot: Bot):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _create_platega_payment(cb, bot, "sub_30")


@router.callback_query(F.data == CB_PLATEGA_BUY_90)
async def platega_buy_90_days(cb: types.CallbackQuery, bot: Bot):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _create_platega_payment(cb, bot, "sub_90")


async def _create_platega_payment(cb: types.CallbackQuery, bot: Bot, sub_type: str):
    """Создать платеж через Platega и показать ссылку на оплату."""
    platega_service = get_platega_service()
    if not platega_service:
        await _send_or_edit_payment_screen(cb, "❌ Оплата через СБП временно недоступна. Обратитесь к администратору.")
        logger.warning("Platega credentials not configured")
        return
    
    tariff = get_tariffs_platega().get(sub_type)
    if not tariff:
        await _send_or_edit_payment_screen(cb, "❌ Неверный тариф")
        return
    
    user_id = cb.from_user.id
    order_id = f"{user_id}:{sub_type}"
    
    # Формируем URL для webhook callback
    webhook_domain = config.PLATEGA_WEBHOOK_DOMAIN or config.PUBLIC_HOST
    if webhook_domain:
        return_url = f"https://{webhook_domain}/webhook?status=success"
        failed_url = f"https://{webhook_domain}/webhook?status=failed"
    else:
        return_url = None
        failed_url = None
    
    try:
        # Создаем платеж через Platega
        payment_data = await platega_service.create_payment(
            amount=float(tariff["amount"]),
            currency="RUB",
            description=f"Подписка на {tariff['days']} дней",
            payload=order_id,
            return_url=return_url,
            failed_url=failed_url,
            payment_method=PLATEGA_METHOD_SBP_QR,
        )
        
        if not payment_data:
            await _send_or_edit_payment_screen(cb, "❌ Ошибка создания платежа. Попробуйте позже.")
            logger.error(f"Failed to create Platega payment for user {user_id}")
            return
        
        transaction_id = payment_data["transaction_id"]
        payment_url = payment_data.get("payment_url", "")
        
        # Сохраняем платеж в БД
        raw_payload = {
            "invoice_payload": sub_type,
            "currency": "RUB",
            "total_amount": float(tariff["amount"]),
            "platega_transaction_id": transaction_id,
            "payment_url": payment_url,
        }
        
        await ensure_user_exists(user_id, cb.from_user.username, cb.from_user.first_name)
        await save_payment(
            telegram_payment_charge_id=transaction_id,
            provider_payment_charge_id=transaction_id,
            user_id=user_id,
            payload=sub_type,
            amount=float(tariff["amount"]),
            currency="RUB",
            payment_method="platega",
            status="pending",
            raw_payload_json=json.dumps(raw_payload, ensure_ascii=False),
        )
        
        # Формируем сообщение со ссылкой на оплату
        text = (
            f"<b>Оплата подписки ({tariff['days']} дней)</b>\n\n"
            f"Сумма: <b>{tariff['amount']}₽</b>\n"
            f"Способ оплаты: <b>СБП</b>\n\n"
            f"Нажмите кнопку ниже для перехода на страницу оплаты:\n"
            f"QR-код уже отображается на странице оплаты."
        )
        
        # Формируем клавиатуру с проверкой URL
        kb_rows = [
            [InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"{CB_PLATEGA_CHECK_PREFIX}{transaction_id}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_SHOW_BUY_MENU)]
        ]
        if payment_url and payment_url.strip():
            kb_rows.insert(0, [InlineKeyboardButton(text="💳 Открыть страницу оплаты", url=payment_url)])
        kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)
        
        await _send_or_edit_payment_screen(cb, text, reply_markup=kb)
        
        # Запускаем фоновую проверку статуса платежа
        asyncio.create_task(_poll_payment_status(cb.bot, transaction_id, user_id, sub_type, cb.message.chat.id))
        
    except Exception as e:
        logger.exception(f"Error in _create_platega_payment: {e}")
        await _send_or_edit_payment_screen(cb, "❌ Ошибка при создании платежа. Попробуйте позже или обратитесь в поддержку.")


@router.callback_query(F.data == "payment_method_stars")
async def select_payment_method_stars(cb: types.CallbackQuery):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    
    if cb.message is None:
        logger.debug("payment_method_stars: message is None for user=%s", cb.from_user.id)
        await cb.bot.send_message(
            cb.from_user.id,
            "<b>Выбран способ оплаты: Telegram Stars</b>\n\nВыберите тариф:",
            parse_mode="HTML",
            reply_markup=get_buy_inline_kb(),
        )
        return
    
    text = (
        "<b>Выбран способ оплаты: Telegram Stars</b>\n\n"
        "Выберите тариф:\n"
        f"• 7 дней — {config.STARS_PRICE_7_DAYS}⭐\n"
        f"• 30 дней — {config.STARS_PRICE_30_DAYS}⭐\n"
        f"• 90 дней — {config.STARS_PRICE_90_DAYS}⭐"
    )
    rows = [
        [InlineKeyboardButton(text=f"7 дней — {config.STARS_PRICE_7_DAYS}⭐", callback_data=CB_BUY_7)],
        [InlineKeyboardButton(text=f"30 дней — {config.STARS_PRICE_30_DAYS}⭐", callback_data=CB_BUY_30)],
        [InlineKeyboardButton(text=f"90 дней — {config.STARS_PRICE_90_DAYS}⭐", callback_data=CB_BUY_90)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_SHOW_BUY_MENU)],
    ]
    await cb.message.edit_text(text=text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data == "payment_method_platega")
async def select_payment_method_platega(cb: types.CallbackQuery):
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    
    if cb.message is None:
        logger.debug("payment_method_platega: message is None for user=%s", cb.from_user.id)
        await cb.bot.send_message(
            cb.from_user.id,
            "<b>Выбран способ оплаты: СБП (QR)</b>\n\nВыберите тариф:",
            parse_mode="HTML",
            reply_markup=get_buy_inline_kb(),
        )
        return
    
    text = (
        "<b>Выбран способ оплаты: СБП (QR)</b>\n\n"
        "Способ оплаты: <b>Система Быстрых Платежей</b>\n"
        "Номер способа оплаты: <b>2</b>\n\n"
        "Выберите тариф:\n"
        f"• 7 дней — {config.PLATEGA_PRICE_7_DAYS}₽\n"
        f"• 30 дней — {config.PLATEGA_PRICE_30_DAYS}₽\n"
        f"• 90 дней — {config.PLATEGA_PRICE_90_DAYS}₽"
    )
    rows = [
        [InlineKeyboardButton(text=f"7 дней — {config.PLATEGA_PRICE_7_DAYS}₽", callback_data=CB_PLATEGA_BUY_7)],
        [InlineKeyboardButton(text=f"30 дней — {config.PLATEGA_PRICE_30_DAYS}₽", callback_data=CB_PLATEGA_BUY_30)],
        [InlineKeyboardButton(text=f"90 дней — {config.PLATEGA_PRICE_90_DAYS}₽", callback_data=CB_PLATEGA_BUY_90)],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_SHOW_BUY_MENU)],
    ]
    await cb.message.edit_text(text=text, parse_mode="HTML", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


async def _send_invoice_from_confirm(cb: types.CallbackQuery, bot: Bot, *, callback_action: str, payload: str, title: str, label: str, amount: int) -> None:
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    mem_limited, mem_wait = is_purchase_rate_limited(cb.from_user.id)
    persistent_limited, persistent_wait = await is_purchase_rate_limited_persistent(cb.from_user.id, callback_action)
    limited = persistent_limited or mem_limited
    if limited:
        wait_seconds = max(mem_wait, persistent_wait, 1)
        await _send_or_edit_payment_screen(cb, f"⏱ Подождите {wait_seconds} сек.\n\nСлишком частые запросы. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await clear_pending_invoice_for_user(bot, cb.from_user.id)
    
    # Проверка на доступность сообщения
    if cb.message is None:
        logger.debug("_send_invoice_from_confirm: message is None for user=%s", cb.from_user.id)
        await cb.bot.send_message(
            cb.from_user.id,
            "⚠️ Не удалось создать счёт: сообщение недоступно. Попробуйте снова.",
            reply_markup=get_buy_inline_kb(),
        )
        return
    
    invoice_message = await _send_stars_invoice(bot, cb.message.chat.id, payload, title, label, amount)
    remember_pending_invoice(cb.from_user.id, cb.message.chat.id, invoice_message.message_id, payload)


@router.callback_query(F.data == CB_BUY_PAY_7)
async def buy_pay_7_days(cb: types.CallbackQuery, bot: Bot):
    """Create Stars invoice for 7 days tariff."""
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _send_invoice_from_confirm(
        cb, bot,
        callback_action="buy_pay_7",
        payload="sub_7",
        title="Свободный Интернет на 7 дней",
        label="7 дней доступа",
        amount=int(get_tariffs_stars()["sub_7"]["amount"]),
    )


@router.callback_query(F.data == CB_BUY_PAY_30)
async def buy_pay_30_days(cb: types.CallbackQuery, bot: Bot):
    """Create Stars invoice for 30 days tariff."""
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _send_invoice_from_confirm(
        cb, bot,
        callback_action="buy_pay_30",
        payload="sub_30",
        title="Свободный Интернет на 30 дней",
        label="30 дней доступа",
        amount=int(get_tariffs_stars()["sub_30"]["amount"]),
    )


@router.callback_query(F.data == CB_BUY_PAY_90)
async def buy_pay_90_days(cb: types.CallbackQuery, bot: Bot):
    """Create Stars invoice for 90 days tariff."""
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_payment_screen(cb, "⏳ Технические работы\n\nПокупка временно недоступна. Попробуйте позже.")
        return
    await cb.answer(show_alert=False)
    await _send_invoice_from_confirm(
        cb, bot,
        callback_action="buy_pay_90",
        payload="sub_90",
        title="Свободный Интернет на 90 дней",
        label="90 дней доступа",
        amount=int(get_tariffs_stars()["sub_90"]["amount"]),
    )


# Обработчик platega_pay_handler удален - теперь используется _create_platega_payment из кнопок CB_PLATEGA_BUY_*


async def _poll_payment_status(bot: Bot, transaction_id: str, user_id: int, sub_type: str, chat_id: int):
    """
    Фоновая задача для опроса статуса платежа Platega.
    Проверяет статус с экспоненциальной задержкой в течение 3 минут.
    Основной механизм подтверждения — вебхук, этот цикл — вспомогательный.
    """
    max_attempts = 18  # ~3 минуты с экспоненциальной задержкой
    
    for attempt in range(max_attempts):
        # Экспоненциальная задержка: 5, 10, 20, 40... секунд (максимум 60 сек)
        exponential_delay = min(5 * (2 ** attempt), 60)
        await asyncio.sleep(exponential_delay)
        
        try:
            status_result = await platega_service.check_status(transaction_id)
            # API возвращает dict со статусом, а не просто строку
            status = status_result.get("status") if isinstance(status_result, dict) else status_result
            logger.info(f"Payment poll attempt {attempt + 1}/{max_attempts}: transaction={transaction_id}, status={status}")
            
            if status == "CONFIRMED":
                logger.info(f"Payment confirmed via polling for user {user_id}, transaction {transaction_id}")
                
                # Атомарно проверяем и обновляем статус платежа в БД
                order_id = f"{user_id}:{sub_type}"
                from database import get_payment_by_order, update_payment_status_by_order
                
                existing_payment = await get_payment_by_order(order_id)
                if existing_payment and existing_payment.get("status") in ("paid", "applied"):
                    logger.info(f"Payment already processed for order {order_id}, skipping activation")
                    return
                
                tariff = get_tariffs().get(sub_type)
                if tariff:
                    from database import save_payment
                    raw_payload = {
                        "invoice_payload": sub_type,
                        "currency": "RUB",
                        "total_amount": float(tariff["amount"]),
                        "platega_transaction_id": transaction_id,
                    }
                    await save_payment(
                        telegram_payment_charge_id=transaction_id,
                        provider_payment_charge_id=transaction_id,
                        user_id=user_id,
                        payload=sub_type,
                        amount=float(tariff["amount"]),
                        currency="RUB",
                        payment_method="platega",
                        status="received",
                        raw_payload_json=json.dumps(raw_payload, ensure_ascii=False),
                    )
                    logger.info(f"Payment saved to DB before activation for user {user_id}, transaction {transaction_id}")
                
                # Активируем подписку
                success = await activate_subscription(user_id, sub_type, "platega", transaction_id, bot=bot)
                
                if success:
                    # Обновляем статус платежа в БД
                    await update_payment_status_by_order(order_id, "paid", transaction_id)
                    
                    # Уведомляем пользователя
                    try:
                        await bot.send_message(
                            chat_id=chat_id,
                            text="<b>✅ Оплата подтверждена!</b>\n\nКлюч будет отправлен вам в ближайшее время.",
                            parse_mode="HTML"
                        )
                    except Exception as e:
                        logger.warning(f"Failed to send payment confirmation to user {user_id}: {e}")
                else:
                    logger.error(f"Failed to activate subscription after payment confirmation for user {user_id}")
                
                return  # Выход из цикла проверки
                
            elif status in ("CANCELED", "CHARGEBACKED"):
                logger.info(f"Payment {status} via polling for transaction {transaction_id}")
                order_id = f"{user_id}:{sub_type}"
                await update_payment_status_by_order(order_id, status.lower(), transaction_id)
                return  # Выход из цикла проверки
            
            # Продолжаем опрос если PENDING или None
        except Exception as e:
            logger.exception(f"Error polling payment status for {transaction_id}: {e}")
            continue
    
    logger.info(f"Payment polling stopped for {transaction_id} after {max_attempts} attempts. Waiting for webhook.")
    
    logger.warning(f"Payment polling timed out for transaction {transaction_id} after {max_attempts} attempts")


@router.callback_query(F.data.startswith(CB_PLATEGA_CHECK_PREFIX))
async def platega_check_payment_handler(cb: types.CallbackQuery):
    """Обработчик проверки статуса платежа пользователем."""
    transaction_id = cb.data.replace(CB_PLATEGA_CHECK_PREFIX, "")
    
    if not transaction_id:
        await _send_or_edit_payment_screen(cb, "❌ Ошибка\n\nНеверный ID транзакции. Попробуйте снова.")
        return
    
    await cb.answer(show_alert=False)
    
    # Проверяем статус платежа в Platega
    status_result = await platega_service.check_status(transaction_id)
    status = status_result.get("status") if isinstance(status_result, dict) else status_result
    
    # Если статус CONFIRMED - активируем подписку
    if status == "CONFIRMED":
        # Получаем информацию о платеже из БД, чтобы узнать user_id и sub_type
        from database import get_payment_by_transaction_id
        payment_info = await get_payment_by_transaction_id(transaction_id)
        
        if payment_info:
            user_id = payment_info.get("user_id")
            # Определяем тип подписки из invoice_payload или order_id
            order_id = payment_info.get("order_id", "")
            sub_type = payment_info.get("invoice_payload", "")
            
            # Если sub_type не найден напрямую, пробуем извлечь из order_id (формат "user_id:sub_type")
            if not sub_type and ":" in order_id:
                sub_type = order_id.split(":")[1] if len(order_id.split(":")) > 1 else ""
            
            if user_id and sub_type:
                logger.info(f"Manual payment confirmation via button for user {user_id}, transaction {transaction_id}")
                
                # Проверяем, не обработан ли уже этот платеж
                existing = await get_payment_by_order(f"{user_id}:{sub_type}")
                if existing and existing.get("status") in ("paid", "applied"):
                    text = (
                        "<b>✅ Оплата уже подтверждена!</b>\n\n"
                        "Подписка активирована, ключ был отправлен ранее."
                    )
                    kb = types.InlineKeyboardMarkup(inline_keyboard=[
                        [types.InlineKeyboardButton(text="🔙 В меню покупки", callback_data=CB_SHOW_BUY_MENU)]
                    ])
                else:
                    # Активируем подписку
                    success = await activate_subscription(user_id, sub_type, "platega", transaction_id, bot=cb.bot)
                    
                    if success:
                        text = (
                            "<b>✅ Оплата подтверждена!</b>\n\n"
                            "Ключ будет отправлен вам в ближайшее время."
                        )
                        kb = types.InlineKeyboardMarkup(inline_keyboard=[
                            [types.InlineKeyboardButton(text="🔙 В меню покупки", callback_data=CB_SHOW_BUY_MENU)]
                        ])
                    else:
                        text = (
                            "<b>⚠️ Произошла ошибка при активации</b>\n\n"
                            "Пожалуйста, обратитесь к поддержке."
                        )
                        kb = types.InlineKeyboardMarkup(inline_keyboard=[
                            [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{config.get_support_username()}")],
                            [types.InlineKeyboardButton(text="🔙 В меню покупки", callback_data=CB_SHOW_BUY_MENU)]
                        ])
            else:
                text = (
                    "<b>⚠️ Не найдена информация о заказе</b>\n\n"
                    f"Статус в Platega: {status}\n"
                    "Обратитесь к поддержке."
                )
                kb = types.InlineKeyboardMarkup(inline_keyboard=[
                    [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{config.get_support_username()}")],
                    [types.InlineKeyboardButton(text="🔙 В меню покупки", callback_data=CB_SHOW_BUY_MENU)]
                ])
        else:
            text = (
                "<b>⚠️ Платеж не найден в базе</b>\n\n"
                f"Статус в Platega: {status}\n"
                "Возможно, вы создали платеж в другом боте."
            )
            kb = types.InlineKeyboardMarkup(inline_keyboard=[
                [types.InlineKeyboardButton(text="📞 Поддержка", url=f"https://t.me/{config.get_support_username()}")],
                [types.InlineKeyboardButton(text="🔙 В меню покупки", callback_data=CB_SHOW_BUY_MENU)]
            ])
    
    elif status == "PENDING":
        sbp_url = f"https://pay.platega.io/?id={transaction_id}&mh={config.PLATEGA_MERCHANT_ID}"
        text = (
            "<b>⏳ Платеж еще не подтвержден</b>\n\n"
            "Пожалуйста, дождитесь подтверждения или попробуйте снова позже.\n"
            f"Ссылка на оплату: <a href='{sbp_url}'>оплатить через СБП</a>\n"
            "Не закрывайте это сообщение — кнопки оплаты остаются активными."
        )
        # Сохраняем все оригинальные кнопки для защиты от дурака
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="💳 Оплатить через СБП", url=sbp_url)],
            [types.InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"{CB_PLATEGA_CHECK_PREFIX}{transaction_id}")],
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data=CB_SHOW_BUY_MENU)]
        ])
    elif status == "CANCELED":
        text = (
            "<b>❌ Платеж отменен</b>\n\n"
            "Вы можете попробовать создать новый платеж."
        )
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔙 В меню покупки", callback_data=CB_SHOW_BUY_MENU)]
        ])
    else:
        sbp_url = f"https://pay.platega.io/?id={transaction_id}&mh={config.PLATEGA_MERCHANT_ID}"
        text = (
            f"<b>⚠️ Статус платежа: {status or 'Неизвестен'}</b>\n\n"
            "Попробуйте проверить позже.\n"
            f"Ссылка на оплату: <a href='{sbp_url}'>оплатить через СБП</a>"
        )
        # Сохраняем все оригинальные кнопки для защиты от дурака
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="💳 Оплатить через СБП", url=sbp_url)],
            [types.InlineKeyboardButton(text="✅ Я оплатил", callback_data=f"{CB_PLATEGA_CHECK_PREFIX}{transaction_id}")],
            [types.InlineKeyboardButton(text="❌ Отмена", callback_data=CB_SHOW_BUY_MENU)]
        ])
    
    # Пытаемся отредактировать сообщение, игнорируя ошибку "не изменено"
    try:
        await cb.message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=kb
        )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            # Игнорируем, если контент не изменился (пользователь нажал кнопку дважды)
            await cb.answer("Статус не изменился, ожидаем оплату...", show_alert=False)
        else:
            # Пробрасываем другие ошибки
            raise


@router.pre_checkout_query()
async def pre_checkout(q: PreCheckoutQuery, bot: Bot):
    if await is_purchase_maintenance_enabled():
        await bot.answer_pre_checkout_query(
            q.id,
            ok=False,
            error_message=await get_purchase_maintenance_text(),
        )
        return
    tariff = get_tariffs().get(q.invoice_payload)
    if not tariff:
        await bot.answer_pre_checkout_query(q.id, ok=False, error_message=await get_text("payment_payload_error"))
        return
    if q.currency != tariff["currency"]:
        await bot.answer_pre_checkout_query(q.id, ok=False, error_message=await get_text("payment_currency_error"))
        return
    if q.total_amount != tariff["amount"]:
        await bot.answer_pre_checkout_query(q.id, ok=False, error_message=await get_text("payment_amount_error"))
        return
    ready, reason = await checkout_readiness()
    if not ready:
        logger.warning("pre_checkout rejected: readiness degraded: %s", reason)
        await bot.answer_pre_checkout_query(
            q.id,
            ok=False,
            error_message=await get_text("precheckout_unavailable"),
        )
        return
    try:
        q_user = getattr(q, "from_user", None)
        q_user_id = int(getattr(q_user, "id", 0) or 0)
        q_username = getattr(q_user, "username", None)
        q_first_name = getattr(q_user, "first_name", None)
        if q_user_id > 0:
            await ensure_user_exists(q_user_id, q_username, q_first_name)
            await upsert_payment_precheck(q.id, q_user_id, q.invoice_payload, status="precheck_passed")
            # Security & Reliability: helper path is checked before pre-checkout confirmation
            await check_awg_container()
    except Exception as e:
        logger.exception("pre_checkout precheck failed: %s", e)
        if "q_user_id" in locals() and q_user_id > 0:
            await upsert_payment_precheck(q.id, q_user_id, q.invoice_payload, status="failed", error_message=str(e)[:400])
        await bot.answer_pre_checkout_query(q.id, ok=False, error_message=await get_text("precheckout_unavailable"))
        return
    await bot.answer_pre_checkout_query(q.id, ok=True)
    await mark_payment_precheck_status(q.id, "confirmed")


async def _log_critical_delivery_error(payment_id: str, user_id: int, error: str) -> None:
    line = f"{utc_now_naive().isoformat()} payment_id={payment_id} user_id={user_id} error={error}\n"
    try:
        CRITICAL_ERRORS_LOG.parent.mkdir(parents=True, exist_ok=True)
        with CRITICAL_ERRORS_LOG.open("a", encoding="utf-8") as fp:
            fp.write(line)
    except Exception as log_error:
        logger.warning(
            "critical delivery error log write failed payment_id=%s user_id=%s: %s",
            payment_id,
            user_id,
            log_error,
        )


async def _send_user_active_config(message: types.Message, user_id: int) -> bool:
    configs = await get_user_keys(user_id)
    if not configs:
        return False
    key_id, device_num, cfg, vpn_key = configs[0]
    filename = f"config_{key_id}_device_{device_num}.conf"
    await message.answer_document(types.BufferedInputFile(cfg.encode("utf-8"), filename=filename))
    if vpn_key:
        await message.answer(f"<code>{vpn_key}</code>", parse_mode="HTML")
    return True


async def _finalize_post_payment_delivery(
    *,
    payment_id: str,
    user_id: int,
    deliver_ready: Callable[[], Awaitable[bool]],
) -> str:
    try:
        delivered = await deliver_ready()
    except Exception as delivery_error:
        await update_last_provision_status(payment_id, "ready_config_pending")
        await _log_critical_delivery_error(payment_id, user_id, str(delivery_error)[:500])
        logger.error(
            "Post-payment delivery failed after applied provisioning payment_id=%s user_id=%s",
            payment_id,
            user_id,
            exc_info=delivery_error,
        )
        await write_audit_log(user_id, "payment_delivery_failed_after_apply", f"payment_id={payment_id}; error={str(delivery_error)[:300]}")
        return "ready_config_pending"

    if not delivered:
        await update_last_provision_status(payment_id, "ready_config_pending")
        logger.warning("Post-payment config missing after applied provisioning payment_id=%s user_id=%s", payment_id, user_id)
        await write_audit_log(user_id, "payment_config_pending_after_apply", f"payment_id={payment_id}; reason=config_missing")
        return "ready_config_pending"

    try:
        await update_last_provision_status(payment_id, "ready")
    except Exception as bookkeeping_error:
        logger.error(
            "Post-payment bookkeeping failed (status=ready) payment_id=%s user_id=%s",
            payment_id,
            user_id,
            exc_info=bookkeeping_error,
        )
        await write_audit_log(
            user_id,
            "payment_post_apply_bookkeeping_failed",
            f"payment_id={payment_id}; step=update_last_provision_status; error={str(bookkeeping_error)[:300]}",
        )
    try:
        await mark_ready_notification_sent(payment_id)
    except Exception as bookkeeping_error:
        logger.error(
            "Post-payment bookkeeping failed (mark_ready_notification_sent) payment_id=%s user_id=%s",
            payment_id,
            user_id,
            exc_info=bookkeeping_error,
        )
        await write_audit_log(
            user_id,
            "payment_post_apply_bookkeeping_failed",
            f"payment_id={payment_id}; step=mark_ready_notification_sent; error={str(bookkeeping_error)[:300]}",
        )
    return "ready"


@router.message(F.successful_payment)
async def success_pay(message: types.Message):
    clear_pending_invoice_state(message.from_user.id)
    payment = message.successful_payment
    tariff = get_tariffs().get(payment.invoice_payload)
    if not tariff:
        await message.answer(await get_text("payment_payload_error"))
        return
    if payment.currency != tariff["currency"]:
        await message.answer(await get_text("payment_currency_error"))
        return
    if payment.total_amount != tariff["amount"]:
        await message.answer(await get_text("payment_amount_error"))
        return

    current_status = await get_payment_status(payment.telegram_payment_charge_id)
    if current_status == "applied" or await payment_already_processed(payment.telegram_payment_charge_id):
        await message.answer(await get_text("payment_already_processed"))
        return
    if current_status == "provisioning":
        await message.answer(await get_text("payment_already_provisioning"))
        return

    raw_payload = {
        "invoice_payload": payment.invoice_payload,
        "currency": payment.currency,
        "total_amount": payment.total_amount,
        "telegram_payment_charge_id": payment.telegram_payment_charge_id,
        "provider_payment_charge_id": payment.provider_payment_charge_id,
    }
    try:
        await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
        await save_payment(
            telegram_payment_charge_id=payment.telegram_payment_charge_id,
            provider_payment_charge_id=payment.provider_payment_charge_id,
            user_id=message.from_user.id,
            payload=payment.invoice_payload,
            amount=payment.total_amount,
            currency=payment.currency,
            payment_method=tariff["method"],
            status="received",
            raw_payload_json=json.dumps(raw_payload, ensure_ascii=False),
        )
        progress_message = await message.answer(await get_text("payment_progress_compact"))
        await update_last_provision_status(payment.telegram_payment_charge_id, "payment_received")
        await update_last_provision_status(payment.telegram_payment_charge_id, "provisioning")
        applied = await process_payment_provisioning(
            payment_id=payment.telegram_payment_charge_id,
            user_id=message.from_user.id,
            payload=payment.invoice_payload,
            days=tariff["days"],
            bot=message.bot,
        )
        if applied:
            result_status = await _finalize_post_payment_delivery(
                payment_id=payment.telegram_payment_charge_id,
                user_id=message.from_user.id,
                deliver_ready=lambda: _send_user_active_config(message, message.from_user.id),
            )
            final_text = await get_payment_result_text(result_status)
            try:
                await progress_message.edit_text(
                    text=final_text,
                    parse_mode="HTML",
                    reply_markup=get_post_payment_kb(),
                )
            except Exception:
                await message.answer(
                    text=final_text,
                    parse_mode="HTML",
                    reply_markup=get_post_payment_kb(),
                )
        else:
            pending_text = await get_payment_result_text("pending")
            try:
                await progress_message.edit_text(
                    text=pending_text,
                    parse_mode="HTML",
                    reply_markup=get_post_payment_kb(),
                )
            except Exception:
                await message.answer(
                    text=pending_text,
                    parse_mode="HTML",
                    reply_markup=get_post_payment_kb(),
                )
    except Exception as e:
        logger.exception("Ошибка обработки оплаты: %s", e)
        retry_at = (utc_now_naive() + timedelta(seconds=PAYMENT_RETRY_DELAY_SECONDS)).isoformat()
        await update_payment_status(
            payment.telegram_payment_charge_id,
            "needs_repair",
            error_message=str(e)[:500],
            next_retry_at=retry_at,
        )
        await write_audit_log(message.from_user.id, "payment_provision_failed", str(e)[:500])
        await message.answer(
            await get_text("payment_error")
        )


async def process_payment_provisioning(payment_id: str, user_id: int, payload: str, days: int, bot: Bot | None = None) -> bool:
    lock_token = str(uuid.uuid4())
    lease_expires_at = (utc_now_naive() + timedelta(seconds=PAYMENT_PROVISIONING_LEASE_SECONDS)).isoformat()
    claimed = await claim_payment_and_job_for_provisioning(payment_id, lock_token, lease_expires_at)
    if not claimed:
        current_status = await get_payment_status(payment_id)
        return current_status == "applied"

    try:
        await write_audit_log(user_id, "payment_provisioning_started", f"payment_id={payment_id}; payload={payload}")
        new_until = await issue_subscription(user_id, days, operation_id=payment_id)
        finalized = await finalize_payment_and_job(
            payment_id=payment_id,
            lock_token=lock_token,
            status="applied",
            provisioned_until=new_until.isoformat(),
        )
        if not finalized:
            raise RuntimeError("payment finalization lock lost")
        rewarded = await apply_referral_rewards_on_first_payment(user_id, payment_id)
        if rewarded:
            await notify_inviter_about_referral_reward(bot, user_id)
        else:
            recurring_rewarded = await apply_referral_recurring_inviter_reward(
                invitee_user_id=user_id,
                payment_id=payment_id,
                purchased_days=days,
            )
            if recurring_rewarded:
                await notify_inviter_about_referral_recurring_reward(bot, user_id, purchased_days=days)
        return True
    except Exception as e:
        retry_at = (utc_now_naive() + timedelta(seconds=PAYMENT_RETRY_DELAY_SECONDS)).isoformat()
        await finalize_payment_and_job(
            payment_id=payment_id,
            lock_token=lock_token,
            status="needs_repair",
            error_message=str(e)[:500],
            next_retry_at=retry_at,
        )
        attempts = await get_provisioning_attempt_count(payment_id)
        if attempts >= PAYMENT_MAX_ATTEMPTS:
            reason = f"max_attempts_exceeded attempts={attempts}; last_error={str(e)[:220]}"
            await mark_payment_stuck_manual(payment_id, reason)
            await write_audit_log(user_id, "payment_provisioning_stuck_manual", f"payment_id={payment_id}; {reason}")
        await write_audit_log(user_id, "payment_provisioning_failed", f"payment_id={payment_id}; retry_at={retry_at}; error={str(e)[:300]}")
        raise


async def activate_subscription(user_id: int, sub_type: str, payment_method: str, transaction_id: str, bot: Bot | None = None) -> bool:
    """
    Активирует подписку для пользователя после успешной оплаты через Platega.
    
    :param user_id: ID пользователя
    :param sub_type: Тип подписки (sub_7, sub_30, sub_90)
    :param payment_method: Метод оплаты ("platega")
    :param transaction_id: ID транзакции в платежной системе
    :param bot: Экземпляр бота для уведомлений
    :return: True если успешно
    """
    tariff = get_tariffs().get(sub_type)
    if not tariff:
        logger.error(f"Invalid sub_type: {sub_type}")
        return False
    
    days = int(tariff["days"])
    
    try:
        await ensure_user_exists(user_id, None, None)
        
        # Проверяем, не обработан ли уже этот платеж
        existing = await get_payment_by_order(f"{user_id}:{sub_type}")
        if existing and existing.get("status") in ("paid", "applied"):
            logger.info(f"Payment already processed for user {user_id}, sub {sub_type}")
            return True
        
        # Сохраняем платеж в БД
        raw_payload = {
            "invoice_payload": sub_type,
            "currency": "RUB",
            "total_amount": float(tariff["amount"]),
            "platega_transaction_id": transaction_id,
        }
        
        await save_payment(
            telegram_payment_charge_id=transaction_id,
            provider_payment_charge_id=transaction_id,
            user_id=user_id,
            payload=sub_type,
            amount=float(tariff["amount"]),
            currency="RUB",
            payment_method=payment_method,
            status="received",
            raw_payload_json=json.dumps(raw_payload, ensure_ascii=False),
        )
        
        # Запускаем процесс активации подписки
        applied = await process_payment_provisioning(
            payment_id=transaction_id,
            user_id=user_id,
            payload=sub_type,
            days=days,
            bot=bot,
        )
        
        if applied:
            logger.info(f"Subscription activated for user {user_id}, sub {sub_type}")
            
            # Обновляем статус платежа на "paid" после успешной активации
            order_id = f"{user_id}:{sub_type}"
            await update_payment_status_by_order(order_id, "paid", transaction_id)
            
            # Отправляем пользователю ключ
            try:
                from database import get_user_keys
                keys = await get_user_keys(user_id)
                if keys and len(keys) > 0:
                    key_data = keys[0]
                    from config import DOWNLOAD_URL
                    from texts import get_payment_result_text
                    result_text = await get_payment_result_text("success")
                    
                    message = (
                        f"{result_text}\n\n"
                        f"<b>Ваш ключ:</b>\n"
                        f"<code>{key_data['config']}</code>\n\n"
                        f"Скачайте клиент AmneziaWG: {DOWNLOAD_URL}"
                    )
                    
                    if bot:
                        await bot.send_message(user_id, message, parse_mode="HTML")
            except Exception as e:
                logger.warning(f"Failed to send key to user {user_id}: {e}")
            
            return True
        else:
            logger.warning(f"Payment provisioning returned False for user {user_id}")
            return False
            
    except Exception as e:
        logger.exception(f"Error activating subscription for user {user_id}: {e}")
        return False


async def _notify_admin_stuck(bot: Bot | None, payment_id: str, user_id: int, reason: str) -> None:
    if bot is None:
        return
    try:
        await bot.send_message(
            ADMIN_ID,
            (
                "⚠️ <b>Платёж требует ручной проверки</b>\n\n"
                f"payment_id=<code>{payment_id}</code>\n"
                f"user_id=<code>{user_id}</code>\n"
                f"reason={reason[:200]}"
            ),
            parse_mode="HTML",
        )
    except Exception as e:
        logger.warning("Не удалось отправить stuck alert администратору: %s", e)


async def payment_recovery_worker(bot: Bot | None = None) -> int:
    repaired = 0
    jobs = await get_repairable_payments(limit=25)
    for payment_id, user_id, payload in jobs:
        attempts = await get_provisioning_attempt_count(payment_id)
        if attempts >= PAYMENT_MAX_ATTEMPTS:
            reason = f"max_attempts_exceeded attempts={attempts}"
            await mark_payment_stuck_manual(payment_id, reason)
            await write_audit_log(user_id, "payment_recovery_stuck_manual", f"payment_id={payment_id}; {reason}")
            await _notify_admin_stuck(bot, payment_id, user_id, reason)
            continue
        tariff = get_tariffs().get(payload)
        if not tariff:
            await update_payment_status(payment_id, "failed", error_message="unknown payload in recovery")
            continue
        try:
            done = await process_payment_provisioning(payment_id, user_id, payload, tariff["days"], bot=bot)
            repaired += int(done)
            if done:
                async def _deliver_recovery_ready() -> bool:
                    configs = await get_user_keys(user_id)
                    if not configs:
                        return False
                    if bot is None:
                        return False
                    await bot.send_message(
                        user_id,
                        await get_text("payment_recovery_ready"),
                    )
                    return True

                result_status = await _finalize_post_payment_delivery(
                    payment_id=payment_id,
                    user_id=user_id,
                    deliver_ready=_deliver_recovery_ready,
                )
                if result_status == "ready_config_pending":
                    logger.warning(
                        "Recovery succeeded but delivery pending payment_id=%s user_id=%s",
                        payment_id,
                        user_id,
                    )
                    await write_audit_log(user_id, "payment_recovery_delivery_pending", f"payment_id={payment_id}")
        except Exception as e:
            logger.warning("Recovery failed for payment=%s: %s", payment_id, e)
            attempts = await get_provisioning_attempt_count(payment_id)
            if attempts >= PAYMENT_MAX_ATTEMPTS:
                reason = f"max_attempts_exceeded attempts={attempts}; last_error={str(e)[:180]}"
                await mark_payment_stuck_manual(payment_id, reason)
                await write_audit_log(user_id, "payment_recovery_stuck_manual", f"payment_id={payment_id}; {reason}")
                await _notify_admin_stuck(bot, payment_id, user_id, reason)
    return repaired


async def manual_retry_activation(payment_id: str, bot: Bot | None = None) -> dict[str, str]:
    row = await fetchone(
        """
        SELECT user_id, payload, status
        FROM payments
        WHERE telegram_payment_charge_id = ?
        """,
        (payment_id,),
    )
    if not row:
        return {"result": "no_payment", "message": "Платёж не найден."}

    user_id = int(row[0])
    payload = str(row[1] or "")
    status = str(row[2] or "")
    if status == "applied":
        return {"result": "already_applied", "message": "Платёж уже применён, повтор не требуется."}
    if status == "provisioning":
        return {"result": "in_progress", "message": "Активация уже выполняется recovery-процессом."}
    if status not in {"received", "needs_repair", "failed", "stuck_manual"}:
        return {"result": "not_retryable", "message": f"Текущий статус не подходит для retry: {status}"}

    tariff = get_tariffs().get(payload)
    if not tariff:
        return {"result": "unknown_payload", "message": f"Неизвестный payload={payload}. Нужна ручная проверка."}

    try:
        done = await process_payment_provisioning(payment_id, user_id, payload, tariff["days"], bot=bot)
        if done:
            async def _deliver_manual_retry_ready() -> bool:
                configs = await get_user_keys(user_id)
                if not configs:
                    return False
                if bot is None:
                    return False
                await bot.send_message(
                    user_id,
                    await get_text("payment_recovery_ready"),
                )
                return True

            result_status = await _finalize_post_payment_delivery(
                payment_id=payment_id,
                user_id=user_id,
                deliver_ready=_deliver_manual_retry_ready,
            )
            if result_status == "ready_config_pending":
                return {"result": "succeeded", "message": "Retry выполнен: доступ применён, выдача конфигурации в ожидании."}
            return {"result": "succeeded", "message": "Retry выполнен успешно, доступ выдан."}
        current_status = await get_payment_status(payment_id)
        if current_status == "applied":
            return {"result": "already_applied", "message": "Платёж уже применён."}
        if current_status == "provisioning":
            return {"result": "in_progress", "message": "Активация уже в процессе, повтор не запущен."}
        return {"result": "no_op", "message": "Нечего повторять: кейс не перешёл в provisioning."}
    except Exception as e:
        logger.warning("manual retry failed for payment=%s: %s", payment_id, e)
        return {"result": "failed", "message": f"Retry завершился ошибкой: {str(e)[:200]}"}
