from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup

import config
from ui_constants import (
    BTN_ADMIN, BTN_BUY, BTN_CONFIGS, BTN_PROFILE, BTN_SUPPORT,
    CB_ADMIN_BACK_MAIN, CB_ADMIN_BROADCAST, CB_ADMIN_COMMANDS, CB_ADMIN_HEALTH, CB_ADMIN_LIST, CB_ADMIN_MAINTENANCE,
    CB_ADMIN_MAINTENANCE_OFF, CB_ADMIN_MAINTENANCE_ON, CB_ADMIN_MAINTENANCE_REFRESH, CB_ADMIN_PAYMENTS, CB_ADMIN_PRICE_CANCEL, CB_ADMIN_PRICE_EDIT_30,
    CB_ADMIN_PRICE_EDIT_7, CB_ADMIN_PRICE_EDIT_90, CB_ADMIN_PRICE_SAVE, CB_ADMIN_PRICES, CB_ADMIN_REFERRALS,
    CB_ADMIN_SERVICE_SETTINGS, CB_ADMIN_SERVICE_SUPPORT, CB_ADMIN_SERVICE_DOWNLOAD, CB_ADMIN_SERVICE_REFERRAL_TOGGLE,
    CB_ADMIN_SERVICE_INVITEE_BONUS, CB_ADMIN_SERVICE_INVITER_BONUS, CB_ADMIN_SERVICE_TORRENT_TOGGLE,
    CB_ADMIN_TEXT_OVERRIDES, CB_ADMIN_TEXT_START, CB_ADMIN_TEXT_BUY_MENU, CB_ADMIN_TEXT_RENEW_MENU, CB_ADMIN_TEXT_SUPPORT,
    CB_ADMIN_TEXT_SET_PREFIX,
    CB_ADMIN_TEXT_RESET_PREFIX,
    CB_ADMIN_PROMOCODES, CB_ADMIN_PROMO_CREATE, CB_ADMIN_PROMO_DISABLE, CB_ADMIN_PROMO_LIST,
    CB_ADMIN_STATS, CB_ADMIN_SYNC, CB_ADMIN_NETWORK_POLICY,
    CB_ADMIN_NET_DENYLIST, CB_ADMIN_NET_SYNC_NOW,
    CB_ADMIN_DENYLIST_TOGGLE, CB_ADMIN_DENYLIST_MODE, CB_ADMIN_DENYLIST_VIEW_DOMAINS, CB_ADMIN_DENYLIST_VIEW_CIDRS,
    CB_ADMIN_DENYLIST_REPLACE_DOMAINS, CB_ADMIN_DENYLIST_REPLACE_CIDRS, CB_ADMIN_DENYLIST_SYNC,
    CB_ADMIN_DENYLIST_MODE_SOFT, CB_ADMIN_DENYLIST_MODE_STRICT,
    CB_ADMIN_FIND_CHARGE, CB_ADMIN_LAST_PAYMENT, CB_ADMIN_OPEN_USER_CARD_PREFIX,
    CB_BROADCAST_CANCEL, CB_BROADCAST_CONFIRM, CB_BUY_30, CB_BUY_7, CB_BUY_90,
    CB_CHECK_ACTIVATION_STATUS,
    CB_CONFIG_CONF_PREFIX, CB_CONFIG_DEVICE_PREFIX, CB_OPEN_CONFIGS, CB_OPEN_PROFILE, CB_OPEN_REFERRALS,
    CB_OPEN_SUPPORT,
    CB_PROMO_INPUT_CANCEL, CB_PROMO_INPUT_START,
    CB_SHOW_BUY_MENU, CB_SHOW_INSTRUCTION, CB_USER_REISSUE_CANCEL, CB_USER_REISSUE_CONFIRM,
    CB_SUPPORT_BACK, CB_SUPPORT_CONNECTION, CB_SUPPORT_PAYMENT, CB_SUPPORT_TERMS, CB_USER_REISSUE_DEVICE_PREFIX,
    CB_CONFIRM_ADD_DAYS, CB_CANCEL_ADD_DAYS,
)


def get_main_menu(user_id: int, admin_id: int) -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton(text=BTN_PROFILE), KeyboardButton(text=BTN_BUY)],
        [KeyboardButton(text=BTN_CONFIGS), KeyboardButton(text=BTN_SUPPORT)],
    ]
    if user_id == admin_id:
        rows.append([KeyboardButton(text=BTN_ADMIN)])
    return ReplyKeyboardMarkup(
        keyboard=rows,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие...",
    )


def get_buy_inline_kb() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"7 дней — {config.STARS_PRICE_7_DAYS}⭐", callback_data=CB_BUY_7)],
        [InlineKeyboardButton(text=f"30 дней — {config.STARS_PRICE_30_DAYS}⭐", callback_data=CB_BUY_30)],
        [InlineKeyboardButton(text=f"90 дней — {config.STARS_PRICE_90_DAYS}⭐", callback_data=CB_BUY_90)],
        [InlineKeyboardButton(text="📖 Как подключиться", callback_data=CB_SHOW_INSTRUCTION)],
        [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_profile_inline_kb(subscription_active: bool, *, referrals_enabled: bool = True) -> InlineKeyboardMarkup:
    rows = []
    if subscription_active:
        rows.append([InlineKeyboardButton(text="🔄 Продлить доступ", callback_data=CB_SHOW_BUY_MENU)])
    else:
        rows.append([InlineKeyboardButton(text="💳 Купить / Продлить", callback_data=CB_SHOW_BUY_MENU)])
    rows.append([InlineKeyboardButton(text="🔑 Подключение", callback_data=CB_OPEN_CONFIGS)])
    rows.append([InlineKeyboardButton(text="🎟 Ввести промокод", callback_data=CB_PROMO_INPUT_START)])
    if referrals_enabled:
        rows.append([InlineKeyboardButton(text="🎁 Рефералы", callback_data=CB_OPEN_REFERRALS)])
    rows.append([InlineKeyboardButton(text="⏱ Статус активации", callback_data=CB_CHECK_ACTIVATION_STATUS)])
    rows.append([InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)])
    rows.append([InlineKeyboardButton(text="📖 Как подключиться", callback_data=CB_SHOW_INSTRUCTION)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_instruction_inline_kb() -> InlineKeyboardMarkup:
    return _single_button_kb(_guide_row())


def _guide_row() -> list[InlineKeyboardButton]:
    return [InlineKeyboardButton(text="📖 Как подключиться", callback_data=CB_SHOW_INSTRUCTION)]


def _single_button_kb(row: list[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[row])


def get_configs_devices_kb(configs: list[tuple[int, int, str, str]]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text=f"📱 Устройство {device_num}", callback_data=f"{CB_CONFIG_DEVICE_PREFIX}{key_id}")]
        for key_id, device_num, _, _ in configs
    ]
    rows.append(_guide_row())
    rows.append([InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_config_result_kb(key_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📄 Выдать .conf файл (для опытных)", callback_data=f"{CB_CONFIG_CONF_PREFIX}{key_id}")],
            [InlineKeyboardButton(text="♻️ Перевыпустить это устройство", callback_data=f"{CB_USER_REISSUE_DEVICE_PREFIX}{key_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к устройствам", callback_data=CB_OPEN_CONFIGS)],
            [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
            [InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)],
            _guide_row(),
        ]
    )


def get_config_post_conf_kb(key_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📄 Отправить .conf ещё раз", callback_data=f"{CB_CONFIG_CONF_PREFIX}{key_id}")],
            [InlineKeyboardButton(text="♻️ Перевыпустить это устройство", callback_data=f"{CB_USER_REISSUE_DEVICE_PREFIX}{key_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к устройствам", callback_data=CB_OPEN_CONFIGS)],
            [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
            [InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)],
            _guide_row(),
        ]
    )


def get_post_payment_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔑 Получить подключение", callback_data=CB_OPEN_CONFIGS)],
            [InlineKeyboardButton(text="⏱ Проверить статус активации", callback_data=CB_CHECK_ACTIVATION_STATUS)],
            [InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)],
            _guide_row(),
        ]
    )


def get_support_center_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 Помощь с оплатой", callback_data=CB_SUPPORT_PAYMENT)],
            [InlineKeyboardButton(text="🔌 Помощь с подключением", callback_data=CB_SUPPORT_CONNECTION)],
            [InlineKeyboardButton(text="📄 Краткие условия", callback_data=CB_SUPPORT_TERMS)],
            [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
        ]
    )


def get_support_back_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
        ]
    )


def get_referrals_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
            [InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)],
        ]
    )


def get_promo_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_PROMO_INPUT_CANCEL)],
            [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
        ]
    )


def get_configs_empty_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            _guide_row(),
            [InlineKeyboardButton(text="🆘 Помощь и поддержка", callback_data=CB_OPEN_SUPPORT)],
            [InlineKeyboardButton(text="⬅️ В профиль", callback_data=CB_OPEN_PROFILE)],
        ]
    )


def get_admin_inline_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="👥 Пользователи", callback_data=CB_ADMIN_LIST), InlineKeyboardButton(text="💳 Платежи", callback_data=CB_ADMIN_PAYMENTS)],
            [InlineKeyboardButton(text="⚙️ Настройки сервиса", callback_data=CB_ADMIN_SERVICE_SETTINGS), InlineKeyboardButton(text="🌐 Сеть", callback_data=CB_ADMIN_NETWORK_POLICY)],
            [InlineKeyboardButton(text="📊 Статистика", callback_data=CB_ADMIN_STATS), InlineKeyboardButton(text="🩺 Состояние", callback_data=CB_ADMIN_HEALTH)],
            [InlineKeyboardButton(text="🔄 Синхронизация", callback_data=CB_ADMIN_SYNC), InlineKeyboardButton(text="🟠 Техработы", callback_data=CB_ADMIN_MAINTENANCE)],
            [InlineKeyboardButton(text="💸 Цены", callback_data=CB_ADMIN_PRICES), InlineKeyboardButton(text="🎟 Промокоды", callback_data=CB_ADMIN_PROMOCODES)],
            [InlineKeyboardButton(text="🎁 Рефералы", callback_data=CB_ADMIN_REFERRALS), InlineKeyboardButton(text="📢 Рассылка", callback_data=CB_ADMIN_BROADCAST)],
            [InlineKeyboardButton(text="📝 Тексты", callback_data=CB_ADMIN_TEXT_OVERRIDES)],
            [InlineKeyboardButton(text="⌨️ Команды", callback_data=CB_ADMIN_COMMANDS)],
        ]
    )


def get_admin_prices_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Изменить 7 дней", callback_data=CB_ADMIN_PRICE_EDIT_7)],
            [InlineKeyboardButton(text="Изменить 30 дней", callback_data=CB_ADMIN_PRICE_EDIT_30)],
            [InlineKeyboardButton(text="Изменить 90 дней", callback_data=CB_ADMIN_PRICE_EDIT_90)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)],
        ]
    )


def get_admin_price_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Сохранить", callback_data=CB_ADMIN_PRICE_SAVE)],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_ADMIN_PRICE_CANCEL)],
        ]
    )


def get_broadcast_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=CB_BROADCAST_CONFIRM)],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=CB_BROADCAST_CANCEL)],
        ]
    )


def get_broadcast_cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить", callback_data=CB_BROADCAST_CANCEL)],
        ]
    )


def get_user_reissue_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="♻️ Да, перевыпустить", callback_data=CB_USER_REISSUE_CONFIRM)],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_USER_REISSUE_CANCEL)],
        ]
    )


def get_admin_confirm_kb(action_key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_{action_key}")],
            [InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_{action_key}")],
        ]
    )


def get_admin_simple_back_kb(back_cb: str, refresh_cb: str | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    if refresh_cb:
        rows.append([InlineKeyboardButton(text="🔄 Обновить", callback_data=refresh_cb)])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def get_admin_payments_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔎 Поиск по Charge ID", callback_data=CB_ADMIN_FIND_CHARGE)],
            [InlineKeyboardButton(text="👤 Последний платёж по user_id", callback_data=CB_ADMIN_LAST_PAYMENT)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)],
        ]
    )


def get_open_user_card_kb(user_id: int, page: int = 0) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Открыть карточку пользователя", callback_data=f"{CB_ADMIN_OPEN_USER_CARD_PREFIX}{user_id}_{page}")],
        ]
    )


def get_admin_maintenance_kb(enabled: bool) -> InlineKeyboardMarkup:
    status_button = InlineKeyboardButton(
        text="🟢 Выключить" if enabled else "🟠 Включить",
        callback_data=CB_ADMIN_MAINTENANCE_OFF if enabled else CB_ADMIN_MAINTENANCE_ON,
    )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [status_button],
            [InlineKeyboardButton(text="🔄 Обновить", callback_data=CB_ADMIN_MAINTENANCE_REFRESH)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)],
        ]
    )


def get_admin_promocodes_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🎟 Список", callback_data=CB_ADMIN_PROMO_LIST)],
            [InlineKeyboardButton(text="➕ Создать", callback_data=CB_ADMIN_PROMO_CREATE)],
            [InlineKeyboardButton(text="⛔ Отключить", callback_data=CB_ADMIN_PROMO_DISABLE)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)],
        ]
    )


def get_admin_network_policy_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🛡 Denylist", callback_data=CB_ADMIN_NET_DENYLIST)],
            [InlineKeyboardButton(text="🔄 Синхронизировать", callback_data=CB_ADMIN_NET_SYNC_NOW)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)],
        ]
    )


def get_admin_denylist_kb(*, denylist_enabled: int, denylist_mode: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"🛡 Denylist: {'ВКЛ' if int(denylist_enabled) == 1 else 'ВЫКЛ'}", callback_data=CB_ADMIN_DENYLIST_TOGGLE)],
            [InlineKeyboardButton(text=f"Режим denylist: {denylist_mode}", callback_data=CB_ADMIN_NET_DENYLIST)],
            [InlineKeyboardButton(text="Мягкий режим", callback_data=CB_ADMIN_DENYLIST_MODE_SOFT), InlineKeyboardButton(text="Строгий режим", callback_data=CB_ADMIN_DENYLIST_MODE_STRICT)],
            [InlineKeyboardButton(text="👁 Домены", callback_data=CB_ADMIN_DENYLIST_VIEW_DOMAINS), InlineKeyboardButton(text="👁 CIDR", callback_data=CB_ADMIN_DENYLIST_VIEW_CIDRS)],
            [InlineKeyboardButton(text="✏️ Заменить домены", callback_data=CB_ADMIN_DENYLIST_REPLACE_DOMAINS)],
            [InlineKeyboardButton(text="✏️ Заменить CIDR", callback_data=CB_ADMIN_DENYLIST_REPLACE_CIDRS)],
            [InlineKeyboardButton(text="🔄 Синхронизировать denylist", callback_data=CB_ADMIN_DENYLIST_SYNC)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_NETWORK_POLICY)],
        ]
    )


def get_admin_service_settings_kb(ref_enabled: int, torrent_enabled: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🆘 Поддержка", callback_data=CB_ADMIN_SERVICE_SUPPORT)],
            [InlineKeyboardButton(text="🔗 Ссылка на загрузку", callback_data=CB_ADMIN_SERVICE_DOWNLOAD)],
            [InlineKeyboardButton(text=f"🎁 Рефералы: {'ВКЛ' if ref_enabled == 1 else 'ВЫКЛ'}", callback_data=CB_ADMIN_SERVICE_REFERRAL_TOGGLE)],
            [InlineKeyboardButton(text="🎁 Бонус другу", callback_data=CB_ADMIN_SERVICE_INVITEE_BONUS)],
            [InlineKeyboardButton(text="🏅 Бонус пригласившему", callback_data=CB_ADMIN_SERVICE_INVITER_BONUS)],
            [InlineKeyboardButton(text=f"⚠️ Предупреждение о торрентах: {'ВКЛ' if torrent_enabled == 1 else 'ВЫКЛ'}", callback_data=CB_ADMIN_SERVICE_TORRENT_TOGGLE)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)],
        ]
    )


def get_admin_add_days_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, выдать +30 дней", callback_data=CB_CONFIRM_ADD_DAYS)],
            [InlineKeyboardButton(text="❌ Отмена", callback_data=CB_CANCEL_ADD_DAYS)],
        ]
    )


def get_admin_text_overrides_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="start", callback_data=CB_ADMIN_TEXT_START)],
            [InlineKeyboardButton(text="buy_menu", callback_data=CB_ADMIN_TEXT_BUY_MENU)],
            [InlineKeyboardButton(text="renew_menu", callback_data=CB_ADMIN_TEXT_RENEW_MENU)],
            [InlineKeyboardButton(text="support_contact", callback_data=CB_ADMIN_TEXT_SUPPORT)],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=CB_ADMIN_BACK_MAIN)],
        ]
    )


def get_admin_text_override_item_kb(key: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Заменить", callback_data=f"{CB_ADMIN_TEXT_SET_PREFIX}{key}")],
            [InlineKeyboardButton(text="♻️ Сбросить", callback_data=f"{CB_ADMIN_TEXT_RESET_PREFIX}{key}")],
            [InlineKeyboardButton(text="⬅️ К списку", callback_data=CB_ADMIN_TEXT_OVERRIDES)],
        ]
    )
