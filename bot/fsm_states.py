"""
FSM States Groups for the bot.

This module defines all FSM states used across the bot for managing user flows.
Each state group corresponds to a specific domain/feature.
"""

from aiogram.fsm.state import State, StatesGroup


class CatalogStates(StatesGroup):
    """Состояния для навигации по каталогу тарифов."""
    
    browsing = State()        # Просмотр каталога
    tariff_selected = State() # Тариф выбран, ждем способ оплаты


class PaymentStates(StatesGroup):
    """Состояния для процесса оплаты."""
    
    stars_pending = State()   # Ждем оплату Stars (pre-checkout passed)
    sbp_pending = State()     # Ждем оплату СБП (payment created, waiting webhook)
    checking_status = State() # Проверяем статус платежа (user clicked check)
    processing = State()      # Платеж обрабатывается (webhook received, provisioning)


class SubscriptionStates(StatesGroup):
    """Состояния для управления подпиской."""
    
    device_selected = State() # Устройство выбрано
    config_sent = State()     # Конфиг отправлен, ждем действие
    reissue_confirm = State() # Ожидание подтверждения перевыпуска


class ProfileStates(StatesGroup):
    """Состояния для профиля пользователя."""
    
    viewing = State()   # Просмотр профиля
    editing = State()   # Редактирование профиля (если будет)


class SupportStates(StatesGroup):
    """Состояния для поддержки."""
    
    browsing = State()        # Просмотр разделов поддержки
    waiting_message = State() # Ждем сообщение для отправки в поддержку


class AdminStates(StatesGroup):
    """Состояния для админ-панели."""
    
    price_edit = State()      # Редактирование цены
    text_override = State()   # Переопределение текста
    broadcast_confirm = State() # Подтверждение рассылки
    user_manage = State()     # Управление пользователем


# ============================================================================
# GLOBAL STATE MANAGEMENT HELPERS
# ============================================================================

async def reset_all_states(state) -> None:
    """
    Глобальный сброс всех состояний.
    Используется при возврате в главное меню или после завершения flow.
    """
    await state.clear()


def get_state_group_for_callback(callback_data: str) -> type[StatesGroup] | None:
    """
    Определяет, какой StateGroup относится к callback.
    Используется для валидации переходов между состояниями.
    """
    if not callback_data:
        return None
    
    # Payment callbacks
    if callback_data.startswith(("pay_stars:", "pay_platega:", "platega_pay:", "platega_check:", "buy_pay_")):
        return PaymentStates
    
    # Catalog callbacks
    if callback_data.startswith(("tariff_sub_", "buy_", "platega_buy_")):
        return CatalogStates
    
    # Subscription callbacks
    if callback_data.startswith(("config_device_", "config_conf_", "user_reissue_")):
        return SubscriptionStates
    
    # Support callbacks
    if callback_data.startswith(("support_", "open_support")):
        return SupportStates
    
    # Admin callbacks
    if callback_data.startswith("a:"):
        return AdminStates
    
    return None
