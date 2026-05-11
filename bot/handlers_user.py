import re
import io

from aiogram import Bot, F, Router, types
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import BaseFilter, Command, CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder

from . import config
from .config import (
    ADMIN_ID,
    SERVER_NAME,
    USER_REISSUE_COOLDOWN_SECONDS,
    get_download_url,
    logger,
    get_support_username,
    maybe_set_support_username,
)
from .awg_backend import get_awg_peers
from .awg_backend import issue_subscription
from .awg_backend import reissue_user_device
from .database import (
    activate_promo_code,
    clear_pending_admin_action,
    ensure_user_exists,
    fetchall,
    get_latest_user_payment_summary,
    get_pending_admin_action,
    get_user_device_traffic_summary,
    get_user_keys,
    get_user_subscription,
    get_user_total_traffic_bytes,
    normalize_promo_code,
    rollback_promo_activation_reservation,
    set_pending_admin_action,
    persistent_guard_hit,
    write_audit_log,
)
from .device_activity import render_device_activity_line
from .traffic import format_bytes_compact, render_device_traffic_line
from .helpers import (
    escape_html,
    format_moscow_datetime,
    format_remaining_time,
    format_tg_username,
    get_status_text,
    subscription_is_active,
    utc_now_naive,
)
from .keyboards import (
    get_buy_inline_kb,
    get_config_post_conf_kb,
    get_configs_empty_kb,
    get_config_result_kb,
    get_configs_devices_kb,
    get_instruction_inline_kb,
    get_main_menu,
    get_promo_cancel_kb,
    get_profile_inline_kb,
    get_referrals_kb,
    get_support_back_kb,
    get_support_center_kb,
    get_support_subpage_back_kb,
    get_user_reissue_confirm_kb,
)
from .texts import (
    get_activation_status_text,
    get_instruction_with_policy_text,
    get_support_full_text,
    get_support_short_text,
)
from .ui_constants import (
    BTN_BUY,
    BTN_CONFIGS,
    BTN_PROFILE,
    BTN_SUPPORT,
    CB_CHANGE_COUNTRY_PREFIX,
    CB_CHECK_ACTIVATION_STATUS,
    CB_CONFIG_CONF_PREFIX,
    CB_CONFIG_DEVICE_PREFIX,
    CB_DELETE_CONFIRM_PREFIX,
    CB_DELETE_PREFIX,
    CB_MAIN_MENU,
    CB_OPEN_CONFIGS,
    CB_OPEN_PROFILE,
    CB_OPEN_REFERRALS,
    CB_OPEN_TRAFFIC_DEVICES,
    CB_OPEN_SUPPORT,
    CB_PICK_COUNTRY_PREFIX,
    CB_PROMO_INPUT_CANCEL,
    CB_PROMO_INPUT_START,
    CB_REISSUE_PREFIX,
    CB_SHOW_BUY_MENU,
    CB_SHOW_INSTRUCTION,
    CB_SLOT_PREFIX,
    CB_SUPPORT_CONNECTION,
    CB_SUPPORT_PAYMENT,
    CB_SUPPORT_TERMS,
    CB_SUPPORT_USEFUL,
    CB_USER_REISSUE_DEVICE_PREFIX,
    CB_USER_REISSUE_CANCEL,
    CB_USER_REISSUE_CONFIRM,
)
from .content_settings import get_setting, get_text
from .referrals import build_referral_inviter_banner_text, capture_referral_start, get_referral_screen_data
from .maintenance import get_purchase_maintenance_text, is_purchase_maintenance_enabled
from .payments import clear_pending_invoice_for_user
from .security_utils import encrypt_text, decrypt_text
from .database import (
    get_user_devices,
    create_device,
    get_active_nodes,
    get_device_by_user_and_slot,
    delete_device,
    get_user_subscription_expires_at,
    get_user_max_devices,
    get_node_by_id,
    enqueue_node_command,
)

router = Router()
USER_PROMO_INPUT_ACTION_KEY = "user_promo_input"


class HasPendingPromoInput(BaseFilter):
    async def __call__(self, message: types.Message) -> bool:
        pending_action = await get_pending_admin_action(message.from_user.id, USER_PROMO_INPUT_ACTION_KEY)
        return bool(pending_action)


def _config_filename_prefix() -> str:
    base = re.sub(r"[^\w.-]+", "_", (SERVER_NAME or "configs").strip(), flags=re.UNICODE).strip("._")
    return base or "configs"


async def _build_user_device_activity_lines(user_id: int) -> list[str]:
    key_rows = await fetchall(
        """
        SELECT device_num, public_key
        FROM keys
        WHERE user_id = ?
          AND state = 'active'
          AND public_key NOT LIKE 'pending:%'
        ORDER BY device_num
        LIMIT 2
        """,
        (user_id,),
    )
    if not key_rows:
        return ["• нет данных"]

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


async def _build_user_traffic_lines(user_id: int) -> list[str]:
    rows = await get_user_device_traffic_summary(user_id)
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
    total_bytes = await get_user_total_traffic_bytes(user_id)
    lines.append(f"• Всего трафика — {format_bytes_compact(total_bytes)}")
    return lines


async def _build_user_device_summary_line(user_id: int) -> str:
    key_rows = await fetchall(
        """
        SELECT public_key
        FROM keys
        WHERE user_id = ?
          AND state = 'active'
          AND public_key NOT LIKE 'pending:%'
        """,
        (user_id,),
    )
    active_devices = len(key_rows)
    if active_devices == 0:
        return "нет активных устройств"
    online_devices = 0
    try:
        runtime_peers = await get_awg_peers()
        public_keys = {str(row[0]).strip() for row in key_rows if str(row[0]).strip()}
        for peer in runtime_peers:
            peer_key = str(peer.get("public_key") or "").strip()
            if peer_key and peer_key in public_keys and peer.get("latest_handshake_at"):
                online_devices += 1
    except Exception:
        return f"{active_devices} активн."
    return f"{active_devices} активн. · {online_devices} онлайн"


async def _build_user_traffic_summary_line(user_id: int) -> str:
    total_bytes = await get_user_total_traffic_bytes(user_id)
    rows = await get_user_device_traffic_summary(user_id)
    if not rows:
        return "0 B"
    return f"{format_bytes_compact(total_bytes)} · устройств с трафиком: {len(rows)}"


async def _send_buy_menu(target, user_id: int):
    if await is_purchase_maintenance_enabled():
        await target.answer(await get_purchase_maintenance_text())
        return
    sub_until = await get_user_subscription(user_id)
    price_lines = [
        f"• 7 дней — {config.STARS_PRICE_7_DAYS}⭐",
        f"• 30 дней — {config.STARS_PRICE_30_DAYS}⭐",
        f"• 90 дней — {config.STARS_PRICE_90_DAYS}⭐",
    ]
    if subscription_is_active(sub_until):
        remaining = format_remaining_time(sub_until)
        await target.answer(
            await get_text("renew_menu", remaining=remaining, price_lines="\n".join(price_lines)),
            parse_mode="HTML",
            reply_markup=get_buy_inline_kb(),
        )
        return
    await target.answer(
        await get_text(
            "buy_menu",
            price_lines="\n".join(price_lines),
            configs_per_user=int(await get_setting("CONFIGS_PER_USER", int) or config.CONFIGS_PER_USER),
        ),
        parse_mode="HTML",
        reply_markup=get_buy_inline_kb(),
    )


async def _render_buy_menu_text(user_id: int) -> str:
    sub_until = await get_user_subscription(user_id)
    price_lines = [
        f"• 7 дней — {config.STARS_PRICE_7_DAYS}⭐",
        f"• 30 дней — {config.STARS_PRICE_30_DAYS}⭐",
        f"• 90 дней — {config.STARS_PRICE_90_DAYS}⭐",
    ]
    if subscription_is_active(sub_until):
        remaining = format_remaining_time(sub_until)
        return await get_text("renew_menu", remaining=remaining, price_lines="\n".join(price_lines))
    return await get_text(
        "buy_menu",
        price_lines="\n".join(price_lines),
        configs_per_user=int(await get_setting("CONFIGS_PER_USER", int) or config.CONFIGS_PER_USER),
    )


async def _send_configs_menu(target, user: types.User):
    configs = await get_user_keys(user.id)
    if not configs:
        await target.answer(
            await get_text("configs_empty"),
            parse_mode="HTML",
            reply_markup=get_configs_empty_kb(),
        )
        return

    await target.answer(
        await get_text("configs_menu"),
        parse_mode="HTML",
        reply_markup=get_configs_devices_kb(configs),
    )


async def _render_configs_menu_screen(user_id: int) -> tuple[str, types.InlineKeyboardMarkup]:
    configs = await get_user_keys(user_id)
    if not configs:
        return await get_text("configs_empty"), get_configs_empty_kb()
    return await get_text("configs_menu"), get_configs_devices_kb(configs)


async def _render_profile_screen(user: types.User) -> tuple[str, types.InlineKeyboardMarkup]:
    sub_until = await get_user_subscription(user.id)
    status_text, until_text = get_status_text(sub_until)
    tg_username = format_tg_username(user.username)
    first_name = escape_html(user.first_name)
    is_active = subscription_is_active(sub_until)
    remaining = format_remaining_time(sub_until) if is_active else "—"
    configs = await get_user_keys(user.id)
    has_connection = bool(configs)
    connection_status = "готово ✅" if has_connection else "ещё не выдано"
    device_summary = await _build_user_device_summary_line(user.id)
    traffic_summary = await _build_user_traffic_summary_line(user.id)
    referrals_enabled = int(await get_setting("REFERRAL_ENABLED", int) or 0) == 1
    return (
        await get_text(
            "profile_screen",
            user_id=user.id,
            first_name=first_name,
            tg_username=escape_html(tg_username),
            status_text=status_text,
            until_text=until_text,
            remaining=remaining,
            connection_status=connection_status,
            device_summary=device_summary,
            traffic_summary=traffic_summary,
        ),
        get_profile_inline_kb(is_active, referrals_enabled=referrals_enabled),
    )


async def _render_traffic_devices_screen(user_id: int) -> tuple[str, types.InlineKeyboardMarkup]:
    traffic_lines = await _build_user_traffic_lines(user_id)
    device_activity_lines = await _build_user_device_activity_lines(user_id)
    return (
        await get_text(
            "traffic_devices_screen",
            traffic_block="\n".join(traffic_lines),
            device_activity_block="\n".join(device_activity_lines),
        ),
        get_support_back_kb(),
    )


async def _find_user_config_by_key_id(user_id: int, key_id: int):
    configs = await get_user_keys(user_id)
    return next((item for item in configs if item[0] == key_id), None)


def _terms_text() -> str:
    return (
        "📄 <b>Краткие условия</b>\n\n"
        "• Сервис выдаёт доступ AmneziaWG для личного использования (single-server MVP).\n"
        "• Оплата даёт доступ на 7 / 30 / 90 дней.\n"
        "• После успешной оплаты выдаётся цифровой доступ (vpn:// и .conf).\n"
        "• По вопросам поддержки и возвратов: через раздел помощи."
    )


def _payment_support_text() -> str:
    return (
        "💳 <b>Поддержка по оплате</b>\n\n"
        "По вопросам оплаты и активации после оплаты напишите в поддержку и укажите ваш "
        "<code>user_id</code> из профиля."
    )


async def _send_support_center(target) -> None:
    await target.answer(
        f"{await get_support_full_text()}\n\nВыберите, с чем нужна помощь:",
        parse_mode="HTML",
        reply_markup=get_support_center_kb(),
    )


async def _send_or_edit_user_screen(
    cb: types.CallbackQuery,
    text: str,
    *,
    reply_markup=None,
    disable_web_page_preview: bool | None = None,
) -> None:
    message = cb.message
    if message is not None and hasattr(message, "edit_text"):
        try:
            kwargs = {"parse_mode": "HTML", "reply_markup": reply_markup}
            if disable_web_page_preview is not None:
                kwargs["disable_web_page_preview"] = disable_web_page_preview
            await message.edit_text(text, **kwargs)
            return
        except TelegramBadRequest as error:
            if "message is not modified" in str(error).lower():
                return
            logger.debug("User screen edit fallback due to TelegramBadRequest: %s", error)
        except Exception as error:
            logger.warning("User screen edit fallback due to unexpected error: %s", error)
    if message is not None:
        kwargs = {"parse_mode": "HTML", "reply_markup": reply_markup}
        if disable_web_page_preview is not None:
            kwargs["disable_web_page_preview"] = disable_web_page_preview
        await message.answer(text, **kwargs)


async def _clear_promo_input_pending(user_id: int) -> None:
    await clear_pending_admin_action(user_id, USER_PROMO_INPUT_ACTION_KEY)


async def _cleanup_pending_invoice_for_navigation(bot, user_id: int) -> None:
    await clear_pending_invoice_for_user(bot, user_id)


async def _start_promo_input_flow(target, user: types.User) -> None:
    await _clear_promo_input_pending(user.id)
    await set_pending_admin_action(
        user.id,
        USER_PROMO_INPUT_ACTION_KEY,
        {"action": USER_PROMO_INPUT_ACTION_KEY},
    )
    await target.answer(
        "Введите промокод одним сообщением.",
        reply_markup=get_promo_cancel_kb(),
    )


def _is_promo_cancel_text(text: str | None) -> bool:
    return str(text or "").strip().lower() in {"отмена", "cancel", "/cancel"}


async def _apply_promo_code(message: types.Message, code: str) -> None:
    try:
        activation = await activate_promo_code(message.from_user.id, code)
        status = activation["status"]
        if status == "not_found":
            await write_audit_log(message.from_user.id, "promo_activation_failed", f"code={code}; reason=not_found")
            await message.answer("❌ Промокод не найден.")
            return
        if status == "inactive":
            await write_audit_log(message.from_user.id, "promo_activation_failed", f"code={code}; reason=inactive")
            await message.answer("❌ Промокод выключен.")
            return
        if status == "exhausted":
            await write_audit_log(message.from_user.id, "promo_activation_failed", f"code={code}; reason=exhausted")
            await message.answer("❌ Лимит активаций исчерпан.")
            return
        if status == "already_used":
            await write_audit_log(message.from_user.id, "promo_activation_failed", f"code={code}; reason=already_used")
            await message.answer("❌ Этот промокод уже нельзя применить.")
            return

        bonus_days = int(activation["bonus_days"])
        operation_id = f"promo-{code}-{message.from_user.id}"
        new_until = await issue_subscription(message.from_user.id, bonus_days, operation_id=operation_id)
        await write_audit_log(
            message.from_user.id,
            "promo_activated",
            f"code={code}; days={bonus_days}; until={new_until.isoformat()}",
        )
        await message.answer(
            f"✅ Промокод применён: +{bonus_days} дней.\n📅 Доступ до: <b>{format_moscow_datetime(new_until)}</b>",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.exception("Ошибка promo apply: %s", e)
        await rollback_promo_activation_reservation(message.from_user.id, code)
        await write_audit_log(message.from_user.id, "promo_activation_failed", f"code={code}; reason=internal_error")
        await message.answer("❌ Не удалось применить промокод. Попробуйте позже.")


async def _handle_promo_input_message(message: types.Message) -> bool:
    action = await get_pending_admin_action(message.from_user.id, USER_PROMO_INPUT_ACTION_KEY)
    if not action:
        return False
    if _is_promo_cancel_text(message.text):
        await _clear_promo_input_pending(message.from_user.id)
        await message.answer("❌ Ввод промокода отменён.")
        return True
    code = normalize_promo_code(message.text or "")
    if not code:
        await message.answer("Введите промокод или нажмите «❌ Отмена».", reply_markup=get_promo_cancel_kb())
        return True
    await _clear_promo_input_pending(message.from_user.id)
    await _apply_promo_code(message, code)
    return True


async def _start_user_reissue_flow(target, user: types.User, *, key_id: int | None = None) -> None:
    sub_until = await get_user_subscription(user.id)
    if not subscription_is_active(sub_until):
        await target.answer("Сейчас активной подписки нет. Сначала оформите или продлите доступ.")
        return
    configs = await get_user_keys(user.id)
    if not configs:
        await target.answer(
            "Не найден активный конфиг для перевыпуска. Откройте «🔑 Подключение» или напишите в поддержку.",
            reply_markup=get_support_back_kb(),
        )
        return
    selected = configs[0]
    if key_id is not None:
        selected = next((item for item in configs if item[0] == key_id), configs[0])
    _, device_num, _, _ = selected
    await clear_pending_admin_action(user.id, "user_reissue_device")
    await set_pending_admin_action(
        user.id,
        "user_reissue_device",
        {"action": "user_reissue_device", "device_num": int(device_num)},
    )
    await target.answer(
        (
            "⚠️ <b>Перевыпуск доступа</b>\n\n"
            "Текущий конфиг устройства будет отключён.\n"
            "Старый vpn:// и .conf перестанут работать.\n\n"
            "Продолжить перевыпуск?"
        ),
        parse_mode="HTML",
        reply_markup=get_user_reissue_confirm_kb(),
    )


def _help_clients_kb() -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="📱 iOS", url="https://apps.apple.com/app/amnezia-vpn/id1600529902")
    kb.button(text="🤖 Android", url="https://play.google.com/store/apps/details?id=org.amnezia.vpn")
    kb.button(text="🪟 Windows", url=get_download_url())
    kb.adjust(1)
    return kb.as_markup()


@router.callback_query(F.data == "noop")
async def noop_callback(cb: types.CallbackQuery):
    await cb.answer()


@router.message(Command("start"))
async def start(message: types.Message, command: CommandObject):
    await _clear_promo_input_pending(message.from_user.id)
    await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
    referral_banner_text: str | None = None
    if command.args:
        capture_result = await capture_referral_start(message.from_user.id, command.args.strip())
        referral_banner_text = await build_referral_inviter_banner_text(capture_result)
    if message.from_user.id == ADMIN_ID:
        maybe_set_support_username(message.from_user.username)
    if referral_banner_text:
        await message.answer(referral_banner_text, parse_mode="HTML")
    await message.answer(await get_text("start"), parse_mode="HTML", reply_markup=get_main_menu(message.from_user.id, ADMIN_ID))


@router.message(Command("my_config"))
async def my_config_cmd(message: types.Message):
    await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
    configs = await get_user_keys(message.from_user.id)
    if not configs:
        await message.answer(await get_text("configs_empty"), parse_mode="HTML", reply_markup=get_instruction_inline_kb())
        return
    key_id, device_num, cfg, vpn_key = configs[0]
    await message.answer_document(
        types.BufferedInputFile(
            cfg.encode("utf-8"),
            filename=f"{_config_filename_prefix()}_device_{device_num}.conf",
        ),
        caption=f"Ваш активный конфиг (device {device_num})",
    )
    if vpn_key:
        await message.answer(f"<code>{escape_html(vpn_key)}</code>", parse_mode="HTML")


@router.message(Command("help"))
async def help_cmd(message: types.Message):
    await message.answer(
        "Выберите официальный клиент AmneziaWG для установки:",
        reply_markup=_help_clients_kb(),
    )


@router.message(Command("support"))
async def support_cmd(message: types.Message):
    await support(message)


@router.message(Command("paysupport"))
async def paysupport_cmd(message: types.Message):
    await message.answer(_payment_support_text(), parse_mode="HTML")


@router.message(Command("terms"))
async def terms_cmd(message: types.Message):
    await message.answer(_terms_text(), parse_mode="HTML")


@router.message(Command("promo"))
async def promo_cmd(message: types.Message, command: CommandObject):
    await _clear_promo_input_pending(message.from_user.id)
    await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
    code = normalize_promo_code(command.args or "")
    if not code:
        await message.answer("Формат: <code>/promo CODE</code>", parse_mode="HTML")
        return
    await _apply_promo_code(message, code)


@router.message(F.text == BTN_PROFILE)
async def profile(message: types.Message):
    await _clear_promo_input_pending(message.from_user.id)
    await _cleanup_pending_invoice_for_navigation(message.bot, message.from_user.id)
    await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if message.from_user.id == ADMIN_ID:
        maybe_set_support_username(message.from_user.username)
    text, markup = await _render_profile_screen(message.from_user)
    await message.answer(text, parse_mode="HTML", reply_markup=markup)


@router.message(F.text == BTN_CONFIGS)
async def my_keys(message: types.Message):
    await _clear_promo_input_pending(message.from_user.id)
    await _cleanup_pending_invoice_for_navigation(message.bot, message.from_user.id)
    await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if message.from_user.id == ADMIN_ID:
        maybe_set_support_username(message.from_user.username)
    await _send_configs_menu(message, message.from_user)


@router.callback_query(F.data.startswith(CB_CONFIG_DEVICE_PREFIX))
async def show_selected_device_config(cb: types.CallbackQuery):
    await ensure_user_exists(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    await cb.answer()
    try:
        key_id = int(cb.data.removeprefix(CB_CONFIG_DEVICE_PREFIX))
    except ValueError:
        await cb.answer(await get_text("config_invalid_device"), show_alert=True)
        return

    selected = await _find_user_config_by_key_id(cb.from_user.id, key_id)
    if not selected:
        await cb.message.answer(
            await get_text("config_not_found"),
            reply_markup=get_instruction_inline_kb(),
        )
        return

    _, device_num, _cfg, vpn_key = selected
    if vpn_key and vpn_key.strip():
        await _send_or_edit_user_screen(
            cb,
            await get_text("config_vpn_ready", device_num=device_num, vpn_key=escape_html(vpn_key)),
            reply_markup=get_config_result_kb(key_id),
        )
    else:
        await _send_or_edit_user_screen(
            cb,
            await get_text("config_vpn_missing"),
            reply_markup=get_configs_empty_kb(),
        )


@router.callback_query(F.data.startswith(CB_CONFIG_CONF_PREFIX))
async def send_selected_device_conf(cb: types.CallbackQuery):
    await ensure_user_exists(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    await cb.answer()
    try:
        key_id = int(cb.data.removeprefix(CB_CONFIG_CONF_PREFIX))
    except ValueError:
        await cb.answer(await get_text("config_invalid_conf_request"), show_alert=True)
        return

    selected = await _find_user_config_by_key_id(cb.from_user.id, key_id)
    if not selected:
        await cb.message.answer(
            await get_text("config_conf_not_found"),
            reply_markup=get_instruction_inline_kb(),
        )
        return

    _, device_num, cfg, _vpn_key = selected
    if cfg and cfg.strip():
        await cb.message.answer_document(
            types.BufferedInputFile(
                cfg.encode("utf-8"),
                filename=f"{_config_filename_prefix()}_device_{device_num}.conf",
            ),
            caption=await get_text("config_conf_caption", device_num=device_num),
            parse_mode="HTML",
        )
        await _send_or_edit_user_screen(cb, await get_text("config_conf_sent"), reply_markup=get_config_post_conf_kb(key_id))
    else:
        await cb.message.answer(
            await get_text("config_conf_missing"),
            reply_markup=get_configs_empty_kb(),
        )


@router.callback_query(F.data == CB_OPEN_CONFIGS)
async def open_configs_from_profile(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await _cleanup_pending_invoice_for_navigation(cb.bot, cb.from_user.id)
    await ensure_user_exists(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    if cb.from_user.id == ADMIN_ID:
        maybe_set_support_username(cb.from_user.username)
    await cb.answer()
    if not cb.message:
        await cb.answer(await get_text("callback_message_unavailable"), show_alert=True)
        return
    text, markup = await _render_configs_menu_screen(cb.from_user.id)
    await _send_or_edit_user_screen(cb, text, reply_markup=markup)


@router.callback_query(F.data == CB_OPEN_PROFILE)
async def open_profile_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await _cleanup_pending_invoice_for_navigation(cb.bot, cb.from_user.id)
    await ensure_user_exists(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    await cb.answer()
    text, markup = await _render_profile_screen(cb.from_user)
    await _send_or_edit_user_screen(cb, text, reply_markup=markup)


@router.callback_query(F.data == CB_OPEN_TRAFFIC_DEVICES)
async def open_traffic_devices_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await _cleanup_pending_invoice_for_navigation(cb.bot, cb.from_user.id)
    await ensure_user_exists(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    await cb.answer()
    text, markup = await _render_traffic_devices_screen(cb.from_user.id)
    await _send_or_edit_user_screen(cb, text, reply_markup=markup)


@router.message(F.text == BTN_SUPPORT)
async def support(message: types.Message):
    await _clear_promo_input_pending(message.from_user.id)
    await _cleanup_pending_invoice_for_navigation(message.bot, message.from_user.id)
    support_username = get_support_username()
    if not support_username:
        logger.warning("SUPPORT_USERNAME is not configured; support contact hidden from user flow")
    await _send_support_center(message)


@router.message(Command("resetdevice"))
async def reset_device_cmd(message: types.Message):
    await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
    await _start_user_reissue_flow(message, message.from_user)


@router.callback_query(F.data.startswith(CB_USER_REISSUE_DEVICE_PREFIX))
async def user_reissue_from_button(cb: types.CallbackQuery):
    await cb.answer()
    key_id: int | None = None
    if cb.data != f"{CB_USER_REISSUE_DEVICE_PREFIX}0":
        try:
            key_id = int(cb.data.removeprefix(CB_USER_REISSUE_DEVICE_PREFIX))
        except ValueError:
            key_id = None
    if cb.message:
        await _start_user_reissue_flow(cb.message, cb.from_user, key_id=key_id)


@router.callback_query(F.data == CB_USER_REISSUE_CANCEL)
async def user_reissue_cancel(cb: types.CallbackQuery):
    await cb.answer()
    await clear_pending_admin_action(cb.from_user.id, "user_reissue_device")
    if cb.message:
        await cb.message.answer("❌ Перевыпуск отменён.")


@router.callback_query(F.data == CB_USER_REISSUE_CONFIRM)
async def user_reissue_confirm(cb: types.CallbackQuery):
    await cb.answer()
    action = await get_pending_admin_action(cb.from_user.id, "user_reissue_device")
    if not action or action.get("action") != "user_reissue_device":
        if cb.message:
            await cb.message.answer("Нет ожидающего запроса на перевыпуск. Используйте /resetdevice.")
        return
    cooldown_hit = await persistent_guard_hit("user_reissue", cb.from_user.id, "current_device", USER_REISSUE_COOLDOWN_SECONDS)
    if cooldown_hit:
        if cb.message:
            await cb.message.answer(f"⏳ Слишком часто. Повторите через {USER_REISSUE_COOLDOWN_SECONDS} сек.")
        return
    try:
        device_num = int(action.get("device_num", 1))
        result = await reissue_user_device(cb.from_user.id, device_num)
        await clear_pending_admin_action(cb.from_user.id, "user_reissue_device")
        if result.get("status") != "reissued":
            if cb.message:
                await cb.message.answer(
                    "Не удалось перевыпустить устройство. Попробуйте позже или напишите в поддержку.",
                    reply_markup=get_support_back_kb(),
                )
            return
        await write_audit_log(cb.from_user.id, "user_reissue_device", f"device_num={device_num}")
        if cb.message:
            await cb.message.answer("✅ Перевыпуск выполнен. Старый конфиг отключён, используйте новый в разделе «🔑 Подключение».")
            text, markup = await _render_configs_menu_screen(cb.from_user.id)
            await _send_or_edit_user_screen(cb, text, reply_markup=markup)
    except Exception as error:
        logger.exception("Ошибка user_reissue_confirm: %s", error)
        if cb.message:
            await cb.message.answer("❌ Ошибка перевыпуска. Попробуйте позже или напишите в поддержку.", reply_markup=get_support_back_kb())


@router.callback_query(F.data == CB_CHECK_ACTIVATION_STATUS)
async def check_activation_status(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await cb.answer()
    sub_until = await get_user_subscription(cb.from_user.id)
    is_active = subscription_is_active(sub_until)
    payment_summary = await get_latest_user_payment_summary(cb.from_user.id)
    has_config = bool(await get_user_keys(cb.from_user.id))
    if not payment_summary:
        await _send_or_edit_user_screen(cb, await get_text("activation_status_no_payments"), reply_markup=get_buy_inline_kb())
        return
    status = payment_summary["last_provision_status"] or payment_summary["status"]
    if payment_summary["status"] in {"needs_repair", "stuck_manual", "failed"}:
        status = payment_summary["status"]
    await _send_or_edit_user_screen(
        cb,
        f"{await get_activation_status_text(status, has_config=has_config)}\n\n{await get_support_short_text()}",
        reply_markup=get_support_back_kb() if status in {"needs_repair", "stuck_manual", "failed"} else get_profile_inline_kb(subscription_active=is_active),
    )


@router.callback_query(F.data == CB_OPEN_SUPPORT)
async def open_support_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await _cleanup_pending_invoice_for_navigation(cb.bot, cb.from_user.id)
    await cb.answer()
    if cb.message:
        await _send_or_edit_user_screen(
            cb,
            f"{await get_support_full_text()}\n\nВыберите, с чем нужна помощь:",
            reply_markup=get_support_center_kb(),
        )


@router.callback_query(F.data == CB_SUPPORT_PAYMENT)
async def support_payment_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await cb.answer()
    if cb.message:
        await _send_or_edit_user_screen(cb, _payment_support_text(), reply_markup=get_support_subpage_back_kb())


@router.callback_query(F.data == CB_SUPPORT_CONNECTION)
async def support_connection_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await cb.answer()
    if cb.message:
        await _send_or_edit_user_screen(
            cb,
            await get_instruction_with_policy_text(),
            reply_markup=get_support_subpage_back_kb(),
            disable_web_page_preview=True,
        )


@router.callback_query(F.data == CB_SUPPORT_TERMS)
async def support_terms_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await cb.answer()
    if cb.message:
        await _send_or_edit_user_screen(cb, _terms_text(), reply_markup=get_support_subpage_back_kb())


@router.callback_query(F.data == CB_SUPPORT_USEFUL)
async def support_useful_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await cb.answer()
    if cb.message:
        await _send_or_edit_user_screen(
            cb,
            await get_text("support_useful", download_url=get_download_url()),
            reply_markup=get_support_subpage_back_kb(),
            disable_web_page_preview=True,
        )


@router.message(F.text == BTN_BUY)
async def buy(message: types.Message):
    await _clear_promo_input_pending(message.from_user.id)
    await _cleanup_pending_invoice_for_navigation(message.bot, message.from_user.id)
    await ensure_user_exists(message.from_user.id, message.from_user.username, message.from_user.first_name)
    if message.from_user.id == ADMIN_ID:
        maybe_set_support_username(message.from_user.username)
    await _send_buy_menu(message, message.from_user.id)


@router.callback_query(F.data == CB_OPEN_REFERRALS)
async def referrals_from_profile(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await _cleanup_pending_invoice_for_navigation(cb.bot, cb.from_user.id)
    await ensure_user_exists(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    if int(await get_setting("REFERRAL_ENABLED", int) or 0) != 1:
        await cb.answer()
        if cb.message:
            await _send_or_edit_user_screen(cb, await get_text("referral_unavailable"))
        return
    await cb.answer()
    me = await cb.bot.get_me()
    bot_username = getattr(me, "username", "") or "bot"
    data = await get_referral_screen_data(cb.from_user.id, bot_username)
    await _send_or_edit_user_screen(
        cb,
        await get_text(
            "referral_screen",
            ref_link=data["link"],
            invited_count=data["invited_count"],
            rewarded_count_first_payment=data["rewarded_count_first_payment"],
            inviter_first_payment_bonus_days_total=data["inviter_first_payment_bonus_days_total"],
            inviter_bonus_days_total=data["inviter_bonus_days_total"],
            inviter_recurring_bonus_days_total=data["inviter_recurring_bonus_days_total"],
            friends_bonus_days_total=data["friends_bonus_days_total"],
            user_invitee_bonus_days_total=data["user_invitee_bonus_days_total"],
            overall_bonus_days_total=data["overall_bonus_days_total"],
            # Backward compatibility for custom text overrides.
            rewarded_count=data["rewarded_count"],
            bonus_days=data["bonus_days"],
            invitee_bonus_days_total=data["invitee_bonus_days_total"],
            recurring_min_purchase_days=int(await get_setting("REFERRAL_RECURRING_MIN_PURCHASE_DAYS", int) or 30),
        ),
        reply_markup=get_referrals_kb(),
    )


@router.callback_query(F.data == CB_SHOW_BUY_MENU)
async def show_buy_menu_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await _cleanup_pending_invoice_for_navigation(cb.bot, cb.from_user.id)
    await ensure_user_exists(cb.from_user.id, cb.from_user.username, cb.from_user.first_name)
    await cb.answer()
    if not cb.message:
        await cb.answer(await get_text("callback_message_unavailable"), show_alert=True)
        return
    if await is_purchase_maintenance_enabled():
        await _send_or_edit_user_screen(cb, await get_purchase_maintenance_text())
        return
    await _send_or_edit_user_screen(cb, await _render_buy_menu_text(cb.from_user.id), reply_markup=get_buy_inline_kb())


@router.callback_query(F.data == CB_SHOW_INSTRUCTION)
async def show_instruction_callback(cb: types.CallbackQuery):
    await _clear_promo_input_pending(cb.from_user.id)
    await cb.answer()
    if cb.message:
        await _send_or_edit_user_screen(
            cb,
            await get_instruction_with_policy_text(),
            reply_markup=get_support_back_kb(),
            disable_web_page_preview=True,
        )


@router.callback_query(F.data == CB_PROMO_INPUT_START)
async def promo_input_start_callback(cb: types.CallbackQuery):
    await cb.answer()
    if cb.message:
        await _start_promo_input_flow(cb.message, cb.from_user)


@router.callback_query(F.data == CB_PROMO_INPUT_CANCEL)
async def promo_input_cancel_callback(cb: types.CallbackQuery):
    await cb.answer()
    await _clear_promo_input_pending(cb.from_user.id)
    if cb.message:
        await cb.message.answer("❌ Ввод промокода отменён.")


@router.message(HasPendingPromoInput())
async def promo_input_pending_message(message: types.Message):
    await _handle_promo_input_message(message)


# =============================================================================
# ФАЗА 3: Inline-интерфейс управления слотами
# =============================================================================

async def render_main_menu(bot: Bot, chat_id: int, message_id: int | None = None) -> None:
    """
    Рендерит главное меню пользователя со слотами устройств.
    Использует edit_message_text если message_id передан, иначе send_message.
    """
    user_id = chat_id
    
    # Получаем данные о подписке
    subscription_expires_at = await get_user_subscription_expires_at(user_id)
    max_devices = await get_user_max_devices(user_id)
    
    # Проверяем активна ли подписка
    now = utc_now_naive()
    is_active = False
    expires_display = "— "
    if subscription_expires_at:
        try:
            expires_dt = datetime.fromisoformat(subscription_expires_at.replace("Z", "+00:00"))
            if expires_dt.tzinfo is None:
                expires_dt = expires_dt.replace(tzinfo=timezone.utc)
            is_active = expires_dt > now
            expires_display = format_moscow_datetime(expires_dt)[:10]
        except (ValueError, TypeError):
            expires_display = "— "
            is_active = False
    
    # Получаем устройства пользователя
    devices = await get_user_devices(user_id)
    devices_by_slot = {d["slot_number"]: d for d in devices}
    
    # Формируем текст сообщения
    status_text = "✅ Активна" if is_active else "❌ Истекла"
    active_count = len([d for d in devices if d.get("status") == "active"])
    
    text = (
        f"🔐 <b>Подписка</b>: {status_text} до {expires_display}\n"
        f"📦 <b>Устройств</b>: {active_count} / {max_devices}\n\n"
        f"Нажмите на слот, чтобы настроить или управлять конфигурацией:"
    )
    
    # Формируем клавиатуру со слотами
    builder = InlineKeyboardBuilder()
    
    for slot_num in range(1, max_devices + 1):
        device = devices_by_slot.get(slot_num)
        
        if not is_active:
            # Подписка истекла - все слоты заблокированы
            builder.button(text=f"🔒 Конфиг #{slot_num}", callback_data="blocked")
        elif device and device.get("status") == "active":
            # Слот занят
            flag = device.get("flag_emoji") or "🌍"
            country = device.get("country") or "Unknown"
            builder.button(text=f"{flag} Конфиг #{slot_num}", callback_data=f"{CB_SLOT_PREFIX}{slot_num}")
        else:
            # Слот пуст
            builder.button(text=f"⚪ Не настроено #{slot_num}", callback_data=f"{CB_SLOT_PREFIX}{slot_num}")
    
    # Дополнительные кнопки внизу
    builder.adjust(max_devices)  # Кнопки слотов в один ряд по max_devices
    
    # Нижние кнопки
    bottom_buttons = [
        [InlineKeyboardButton(text="💳 Купить / Продлить", callback_data=CB_SHOW_BUY_MENU)],
        [InlineKeyboardButton(text="🎁 Промокод", callback_data=CB_PROMO_INPUT_START)],
        [InlineKeyboardButton(text="👥 Рефералы", callback_data=CB_OPEN_REFERRALS)],
        [InlineKeyboardButton(text="🆘 Поддержка", callback_data=CB_OPEN_SUPPORT)],
    ]
    
    final_keyboard = InlineKeyboardMarkup(
        inline_keyboard=builder.inline_keyboard + bottom_buttons
    )
    
    try:
        if message_id is not None:
            await bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                parse_mode="HTML",
                reply_markup=final_keyboard,
            )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="HTML",
                reply_markup=final_keyboard,
            )
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            return  # Ничего не делаем, сообщение не изменилось
        logger.warning("Ошибка при рендере главного меню: %s", e)
        raise


@router.callback_query(F.data == CB_MAIN_MENU)
async def main_menu_callback(cb: types.CallbackQuery):
    """Возврат к главному меню."""
    await cb.answer()
    if cb.message:
        await render_main_menu(cb.bot, cb.from_user.id, cb.message.message_id)


@router.callback_query(F.data.startswith(CB_SLOT_PREFIX))
async def slot_callback(cb: types.CallbackQuery):
    """Обработка нажатия на слот."""
    # Парсим номер слота
    try:
        slot_number = int(cb.data.split(":")[1])
    except (IndexError, ValueError):
        await cb.answer("⛔️ Ошибка формата", show_alert=True)
        return
    
    user_id = cb.from_user.id
    
    # Проверяем активность подписки
    subscription_expires_at = await get_user_subscription_expires_at(user_id)
    now = utc_now_naive()
    is_active = False
    if subscription_expires_at:
        try:
            expires_dt = datetime.fromisoformat(subscription_expires_at.replace("Z", "+00:00"))
            if expires_dt.tzinfo is None:
                expires_dt = expires_dt.replace(tzinfo=timezone.utc)
            is_active = expires_dt > now
        except (ValueError, TypeError):
            pass
    
    if not is_active:
        await cb.answer("⛔️ Подписка истекла. Продлите доступ.", show_alert=True)
        return
    
    # Получаем устройство
    device = await get_device_by_user_and_slot(user_id, slot_number)
    
    if not device:
        # Пустой слот - показываем выбор страны
        nodes = await get_active_nodes()
        
        builder = InlineKeyboardBuilder()
        for node in nodes:
            flag = node.get("flag_emoji") or "🌍"
            country = node.get("country") or "Unknown"
            builder.button(
                text=f"{flag} {country}",
                callback_data=f"{CB_PICK_COUNTRY_PREFIX}{slot_number}:{node['id']}"
            )
        
        builder.button(text="🔙 Назад", callback_data=CB_MAIN_MENU)
        builder.adjust(2)  # 2 кнопки в ряд
        
        text = f"⚙️ <b>Настройка слота #{slot_number}</b>\n\nВыберите страну для подключения:"
        
        try:
            await cb.message.edit_text(
                text=text,
                parse_mode="HTML",
                reply_markup=builder.as_markup(),
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
        await cb.answer()
    else:
        # Занятый слот - показываем меню управления
        flag = device.get("flag_emoji") or "🌍"
        country = device.get("country") or "Unknown"
        
        # Форматируем дату истечения
        expires_display = expires_display = subscription_expires_at[:10] if subscription_expires_at else "—"
        
        text = (
            f"📱 <b>Конфиг #{slot_number}</b> | {flag} {country}\n"
            f"Активен до: {expires_display}"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Перевыпустить", callback_data=f"{CB_REISSUE_PREFIX}{slot_number}")
        builder.button(text="🌍 Сменить страну", callback_data=f"{CB_CHANGE_COUNTRY_PREFIX}{slot_number}")
        builder.button(text="🗑 Удалить", callback_data=f"{CB_DELETE_PREFIX}{slot_number}")
        builder.button(text="🔙 Назад", callback_data=CB_MAIN_MENU)
        builder.adjust(2)
        
        try:
            await cb.message.edit_text(
                text=text,
                parse_mode="HTML",
                reply_markup=builder.as_markup(),
            )
        except TelegramBadRequest as e:
            if "message is not modified" not in str(e).lower():
                raise
        await cb.answer()


@router.callback_query(F.data.startswith(CB_PICK_COUNTRY_PREFIX))
async def pick_country_callback(cb: types.CallbackQuery):
    """Выбор страны и создание устройства."""
    # Парсим callback_data: pick_country:{slot}:{node_id}
    try:
        parts = cb.data.split(":")
        slot_number = int(parts[1])
        node_id = int(parts[2])
    except (IndexError, ValueError):
        await cb.answer("⛔️ Ошибка формата", show_alert=True)
        return
    
    user_id = cb.from_user.id
    
    # Проверяем лимит ноды
    node = await get_node_by_id(node_id)
    if not node:
        await cb.answer("⛔️ Сервер недоступен", show_alert=True)
        return
    
    if node["active_configs"] >= node["capacity"]:
        await cb.answer("⛔️ Лимит сервера достигнут. Выберите другой.", show_alert=True)
        return
    
    await cb.answer("⏳ Генерация ключей...")
    
    try:
        # Генерируем ключи WireGuard
        private_key, public_key = await generate_keypair()
        psk = await generate_psk()
        
        # Шифруем чувствительные данные
        private_key_enc = encrypt_text(private_key)
        psk_enc = encrypt_text(psk)
        
        # Создаём устройство в БД
        device_id = await create_device(
            user_id=user_id,
            slot_number=slot_number,
            node_id=node_id,
            public_key=public_key,
            private_key_enc=private_key_enc,
            psk_enc=psk_enc,
        )
        
        # Отправляем команду на удалённую ноду для добавления peer
        await enqueue_node_command(
            node_id=node_id,
            action="add_peer",
            payload={
                "public_key": public_key,
                "allowed_ips": "0.0.0.0/0",
                "preshared_key": psk,
            },
        )
        
        # Генерируем конфиг AmneziaWG с параметрами ноды
        server_ip_endpoint = f"{node['ip']}:{node['port']}"
        config_text = build_client_config(
            private_key=private_key,
            ip=node["ip"],
            psk_key=psk,
            server_ip=server_ip_endpoint,
            server_pub_key=node.get("server_public_key", ""),
        )
        
        # Добавляем параметры AmneziaWG
        awg_params = {
            "Jc": node.get("s1", ""),
            "Jmin": node.get("s2", ""),
            "Jmax": node.get("s3", ""),
            "S1": node.get("s4", ""),
            "H1": node.get("h1", ""),
            "H2": node.get("h2", ""),
            "H3": node.get("h3", ""),
            "H4": node.get("h4", ""),
        }
        
        # Формируем полный конфиг с параметрами AmneziaWG
        full_config = f"[Interface]\nAddress = {node['ip']}/32\nDNS = 8.8.8.8, 8.8.4.4\nPrivateKey = {private_key}\n"
        for key, value in awg_params.items():
            if value:
                full_config += f"{key} = {value}\n"
        full_config += f"\n[Peer]\nPublicKey = {node.get('server_public_key', '')}\nPresharedKey = {psk}\nAllowedIPs = 0.0.0.0/0\nEndpoint = {node['ip']}:{node['port']}\nPersistentKeepalive = 25\n"
        
        # Отправляем конфиг файлом
        config_file = types.InputFile(
            io.BytesIO(full_config.encode("utf-8")),
            filename=f"config_slot_{slot_number}.conf"
        )
        
        await cb.bot.send_document(
            chat_id=user_id,
            document=config_file,
            caption=f"📄 <b>Конфигурация для слота #{slot_number}</b>\n\n"
                    f"🇩🇪 Страна: {node.get('country', 'Unknown')}\n"
                    f"🌐 Сервер: {node.get('name', 'N/A')}\n\n"
                    f"<b>Как импортировать:</b>\n"
                    f"1. Скачайте файл .conf\n"
                    f"2. Откройте клиент AmneziaWG\n"
                    f"3. Добавьте туннель из файла\n"
                    f"4. Подключитесь",
            parse_mode="HTML",
        )
        
        # Обновляем главное меню
        await render_main_menu(cb.bot, user_id, cb.message.message_id)
        
        logger.info(
            "Создано устройство: user_id=%s, slot=%s, node_id=%s, public_key=%s...",
            user_id, slot_number, node_id, public_key[:20]
        )
        
    except Exception as e:
        logger.exception("Ошибка при создании устройства: %s", e)
        await cb.answer("❌ Ошибка создания конфига. Попробуйте позже.", show_alert=True)


@router.callback_query(F.data.startswith(CB_REISSUE_PREFIX))
async def reissue_callback(cb: types.CallbackQuery):
    """Перевыпуск ключей для устройства."""
    try:
        slot_number = int(cb.data.split(":")[1])
    except (IndexError, ValueError):
        await cb.answer("⛔️ Ошибка формата", show_alert=True)
        return
    
    user_id = cb.from_user.id
    
    # Находим устройство
    device = await get_device_by_user_and_slot(user_id, slot_number)
    if not device:
        await cb.answer("⛔️ Устройство не найдено", show_alert=True)
        return
    
    await cb.answer("⏳ Перевыпуск ключей...")
    
    try:
        # Генерируем новые ключи
        private_key, public_key = await generate_keypair()
        psk = await generate_psk()
        
        # Шифруем
        private_key_enc = encrypt_text(private_key)
        psk_enc = encrypt_text(psk)
        
        # Обновляем устройство в БД (через delete + create для данного слота)
        db = await open_db()
        try:
            await db.execute("BEGIN IMMEDIATE")
            
            # Удаляем старую запись
            await db.execute(
                "DELETE FROM devices WHERE user_id = ? AND slot_number = ?",
                (user_id, slot_number)
            )
            
            now_iso = utc_now_naive().isoformat()
            
            # Создаём новую запись
            cursor = await db.execute(
                """
                INSERT INTO devices (user_id, slot_number, node_id, public_key, private_key_enc, psk_enc, status, created_at, last_reissued_at)
                VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (user_id, slot_number, device["node_id"], public_key, private_key_enc, psk_enc, now_iso, now_iso)
            )
            
            await db.commit()
        finally:
            await db.close()
        
        # Генерируем новый конфиг
        node = await get_node_by_id(device["node_id"])
        full_config = f"[Interface]\nAddress = {node['ip']}/32\nDNS = 8.8.8.8, 8.8.4.4\nPrivateKey = {private_key}\n"
        awg_params = {
            "Jc": node.get("s1", ""),
            "Jmin": node.get("s2", ""),
            "Jmax": node.get("s3", ""),
            "S1": node.get("s4", ""),
            "H1": node.get("h1", ""),
            "H2": node.get("h2", ""),
            "H3": node.get("h3", ""),
            "H4": node.get("h4", ""),
        }
        for key, value in awg_params.items():
            if value:
                full_config += f"{key} = {value}\n"
        full_config += f"\n[Peer]\nPublicKey = {node.get('server_public_key', '')}\nPresharedKey = {psk}\nAllowedIPs = 0.0.0.0/0\nEndpoint = {node['ip']}:{node['port']}\nPersistentKeepalive = 25\n"
        
        # Отправляем новый конфиг
        config_file = types.InputFile(
            io.BytesIO(full_config.encode("utf-8")),
            filename=f"config_slot_{slot_number}_reissued.conf"
        )
        
        await cb.bot.send_document(
            chat_id=user_id,
            document=config_file,
            caption=f"🔄 <b>Конфиг перевыпущен!</b>\n\n"
                    f"Старый ключ деактивирован. Используйте новый файл.",
            parse_mode="HTML",
        )
        
        # Возвращаемся к меню слота
        flag = node.get("flag_emoji") or "🌍"
        country = node.get("country") or "Unknown"
        subscription_expires_at = await get_user_subscription_expires_at(user_id)
        expires_display = subscription_expires_at[:10] if subscription_expires_at else "—"
        
        text = (
            f"📱 <b>Конфиг #{slot_number}</b> | {flag} {country}\n"
            f"Активен до: {expires_display}\n\n"
            f"✅ Ключи перевыпущены"
        )
        
        builder = InlineKeyboardBuilder()
        builder.button(text="🔄 Перевыпустить", callback_data=f"{CB_REISSUE_PREFIX}{slot_number}")
        builder.button(text="🌍 Сменить страну", callback_data=f"{CB_CHANGE_COUNTRY_PREFIX}{slot_number}")
        builder.button(text="🗑 Удалить", callback_data=f"{CB_DELETE_PREFIX}{slot_number}")
        builder.button(text="🔙 Назад", callback_data=CB_MAIN_MENU)
        builder.adjust(2)
        
        await cb.message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
        
        logger.info(
            "Перевыпущено устройство: user_id=%s, slot=%s, public_key=%s...",
            user_id, slot_number, public_key[:20]
        )
        
    except Exception as e:
        logger.exception("Ошибка при перевыпуске: %s", e)
        await cb.answer("❌ Ошибка перевыпуска. Попробуйте позже.", show_alert=True)


@router.callback_query(F.data.startswith(CB_CHANGE_COUNTRY_PREFIX))
async def change_country_callback(cb: types.CallbackQuery):
    """Смена страны для устройства."""
    try:
        slot_number = int(cb.data.split(":")[1])
    except (IndexError, ValueError):
        await cb.answer("⛔️ Ошибка формата", show_alert=True)
        return
    
    user_id = cb.from_user.id
    
    # Находим текущее устройство
    device = await get_device_by_user_and_slot(user_id, slot_number)
    if not device:
        await cb.answer("⛔️ Устройство не найдено", show_alert=True)
        return
    
    # Удаляем старое устройство (освобождаем слот на старой ноде)
    result = await delete_device(device["id"])
    if not result.get("deleted"):
        await cb.answer("⛔️ Ошибка удаления старого конфига", show_alert=True)
        return
    
    # Показываем выбор стран (как для пустого слота)
    nodes = await get_active_nodes()
    
    builder = InlineKeyboardBuilder()
    for node in nodes:
        flag = node.get("flag_emoji") or "🌍"
        country = node.get("country") or "Unknown"
        builder.button(
            text=f"{flag} {country}",
            callback_data=f"{CB_PICK_COUNTRY_PREFIX}{slot_number}:{node['id']}"
        )
    
    builder.button(text="🔙 Назад", callback_data=CB_MAIN_MENU)
    builder.adjust(2)
    
    text = f"🌍 <b>Смена страны для слота #{slot_number}</b>\n\nВыберите новую страну:"
    
    try:
        await cb.message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise
    
    await cb.answer()


@router.callback_query(F.data.startswith(CB_DELETE_PREFIX))
async def delete_callback(cb: types.CallbackQuery):
    """Первый этап удаления - запрос подтверждения."""
    try:
        slot_number = int(cb.data.split(":")[1])
    except (IndexError, ValueError):
        await cb.answer("⛔️ Ошибка формата", show_alert=True)
        return
    
    user_id = cb.from_user.id
    
    # Проверяем что устройство существует
    device = await get_device_by_user_and_slot(user_id, slot_number)
    if not device:
        await cb.answer("⛔️ Устройство не найдено", show_alert=True)
        return
    
    # Показываем подтверждение
    text = (
        f"⚠️ <b>Удалить Конфиг #{slot_number}?</b>\n\n"
        f"Это действие нельзя отменить.\n\n"
        f"Текущая страна: {device.get('country', 'Unknown')}"
    )
    
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да, удалить", callback_data=f"{CB_DELETE_CONFIRM_PREFIX}{slot_number}")
    builder.button(text="❌ Отмена", callback_data=f"{CB_SLOT_PREFIX}{slot_number}")
    builder.adjust(1)
    
    try:
        await cb.message.edit_text(
            text=text,
            parse_mode="HTML",
            reply_markup=builder.as_markup(),
        )
    except TelegramBadRequest as e:
        if "message is not modified" not in str(e).lower():
            raise
    
    await cb.answer()


@router.callback_query(F.data.startswith(CB_DELETE_CONFIRM_PREFIX))
async def delete_confirm_callback(cb: types.CallbackQuery):
    """Второй этап удаления - подтверждение и выполнение."""
    try:
        slot_number = int(cb.data.split(":")[1])
    except (IndexError, ValueError):
        await cb.answer("⛔️ Ошибка формата", show_alert=True)
        return
    
    user_id = cb.from_user.id
    
    # Находим устройство
    device = await get_device_by_user_and_slot(user_id, slot_number)
    if not device:
        await cb.answer("⛔️ Устройство уже удалено", show_alert=True)
        return
    
    # Удаляем устройство
    result = await delete_device(device["id"])
    if not result.get("deleted"):
        await cb.answer("❌ Ошибка удаления", show_alert=True)
        return
    
    await cb.answer("✅ Устройство удалено")
    
    # Обновляем главное меню
    await render_main_menu(cb.bot, user_id, cb.message.message_id)
    
    logger.info("Удалено устройство: user_id=%s, slot=%s", user_id, slot_number)


# =============================================================================
# КОНЕЦ ФАЗЫ 3
# =============================================================================


@router.message()
async def fallback_message(message: types.Message):
    if message.text and message.text.startswith("/"):
        await message.answer(await get_text("unknown_slash"))
        return
    await message.answer(
        await get_text("unknown_message"),
        reply_markup=get_main_menu(message.from_user.id, ADMIN_ID),
    )
