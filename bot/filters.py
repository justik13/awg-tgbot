"""
Централизованные фильтры для Telegram бота.
Устраняет дублирование классов фильтров между handlers_user.py и handlers_admin.py
"""
from __future__ import annotations

from aiogram.filters import BaseFilter
from aiogram import types

from database import get_pending_admin_action
from config import ADMIN_ID

# =============================================================================
# USER FILTERS
# =============================================================================

USER_PROMO_INPUT_ACTION_KEY = "user_promo_input"


class UserHasPendingPromoInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода промокода у обычного пользователя"""
    async def __call__(self, message: types.Message) -> bool:
        pending_action = await get_pending_admin_action(message.from_user.id, USER_PROMO_INPUT_ACTION_KEY)
        return bool(pending_action)


# =============================================================================
# ADMIN FILTERS
# =============================================================================

ADMIN_COMMAND_COOLDOWN_SECONDS = 3  # Можно импортировать из config если нужно


class IsAdmin(BaseFilter):
    """Фильтр для проверки что сообщение от администратора"""
    async def __call__(self, message: types.Message) -> bool:
        return message.from_user.id == ADMIN_ID


class AdminHasPendingBroadcastInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода рассылки"""
    async def __call__(self, message: types.Message) -> bool:
        from config import ADMIN_ID
        pending = await get_pending_admin_action(ADMIN_ID, "broadcast_input")
        return bool(pending)


class AdminHasPendingPriceInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода цены"""
    async def __call__(self, message: types.Message) -> bool:
        from config import ADMIN_ID
        pending = await get_pending_admin_action(ADMIN_ID, "price_input")
        return bool(pending)


class AdminHasPendingPaymentLookupInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода поиска платежа"""
    async def __call__(self, message: types.Message) -> bool:
        from config import ADMIN_ID
        PAYMENT_CHARGE_INPUT_ACTION_KEY = "payment_charge_input"
        PAYMENT_USER_INPUT_ACTION_KEY = "payment_user_input"
        pending_charge = await get_pending_admin_action(ADMIN_ID, PAYMENT_CHARGE_INPUT_ACTION_KEY)
        pending_user = await get_pending_admin_action(ADMIN_ID, PAYMENT_USER_INPUT_ACTION_KEY)
        return bool(pending_charge or pending_user)


class AdminHasPendingPromoInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода промокода у администратора"""
    async def __call__(self, message: types.Message) -> bool:
        from config import ADMIN_ID
        PROMO_CREATE_INPUT_ACTION_KEY = "promo_create_input"
        PROMO_DISABLE_INPUT_ACTION_KEY = "promo_disable_input"
        pending_create = await get_pending_admin_action(ADMIN_ID, PROMO_CREATE_INPUT_ACTION_KEY)
        pending_disable = await get_pending_admin_action(ADMIN_ID, PROMO_DISABLE_INPUT_ACTION_KEY)
        return bool(pending_create or pending_disable)


class AdminHasPendingNetworkPolicyInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода сетевой политики"""
    async def __call__(self, message: types.Message) -> bool:
        from config import ADMIN_ID
        DENYLIST_DOMAINS_INPUT_ACTION_KEY = "denylist_domains_input"
        DENYLIST_CIDRS_INPUT_ACTION_KEY = "denylist_cidrs_input"
        keys = (DENYLIST_DOMAINS_INPUT_ACTION_KEY, DENYLIST_CIDRS_INPUT_ACTION_KEY)
        for key in keys:
            if await get_pending_admin_action(ADMIN_ID, key):
                return True
        return False


class AdminHasPendingServiceSettingsInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода настроек сервиса"""
    async def __call__(self, message: types.Message) -> bool:
        from config import ADMIN_ID
        SERVICE_SUPPORT_INPUT_ACTION_KEY = "service_support_input"
        SERVICE_DOWNLOAD_INPUT_ACTION_KEY = "service_download_input"
        SERVICE_NAME_INPUT_ACTION_KEY = "service_name_input"
        keys = (SERVICE_SUPPORT_INPUT_ACTION_KEY, SERVICE_DOWNLOAD_INPUT_ACTION_KEY, SERVICE_NAME_INPUT_ACTION_KEY)
        for key in keys:
            if await get_pending_admin_action(ADMIN_ID, key):
                return True
        return False


class AdminHasPendingTextOverrideInput(BaseFilter):
    """Фильтр для проверки наличия ожидающего ввода переопределения текста"""
    async def __call__(self, message: types.Message) -> bool:
        from config import ADMIN_ID
        TEXT_OVERRIDE_INPUT_ACTION_KEY = "text_override_input"
        pending = await get_pending_admin_action(ADMIN_ID, TEXT_OVERRIDE_INPUT_ACTION_KEY)
        return bool(pending)
