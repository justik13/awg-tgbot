"""
Migration Utils - Backward Compatibility and State Management.

This module provides utilities for gradual migration from old callback format
to new architecture while maintaining full backward compatibility.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery

from config import logger
from callbacks import (
    BACKWARD_COMPAT_MAP,
    LEGACY_CALLBACKS,
    is_legacy_callback,
    get_new_callback_for_legacy,
)
from fsm_states import reset_all_states

if TYPE_CHECKING:
    from typing import Any


# ============================================================================
# CALLBACK MIGRATION HELPERS
# ============================================================================

def translate_callback_if_needed(callback_data: str) -> str:
    """
    Translates legacy callback to new format if mapping exists.
    
    During migration period, both old and new handlers will work.
    This function allows gradual transition by translating callbacks
    at the entry point.
    
    Args:
        callback_data: Original callback data
        
    Returns:
        New callback format if mapping exists, otherwise original
    """
    # Direct mapping
    if callback_data in BACKWARD_COMPAT_MAP:
        new_cb = BACKWARD_COMPAT_MAP[callback_data]
        logger.debug("Translating callback: %s -> %s", callback_data, new_cb)
        return new_cb
    
    # Prefix-based translation for patterns
    # Example: config_device_1 -> subscription:config:1
    if callback_data.startswith("config_device_"):
        device_id = callback_data.replace("config_device_", "", 1)
        return f"subscription:config:{device_id}"
    
    if callback_data.startswith("config_conf_"):
        device_id = callback_data.replace("config_conf_", "", 1)
        return f"subscription:config:{device_id}"
    
    # No translation needed
    return callback_data


def should_handle_as_legacy(callback_data: str) -> bool:
    """Check if callback should be handled by legacy handler."""
    return is_legacy_callback(callback_data)


# ============================================================================
# FSM STATE GUARDS
# ============================================================================

async def safe_reset_state(state: FSMContext, reason: str = "navigation") -> None:
    """
    Safely resets FSM state with logging.
    
    Use this before entering a new flow or when returning to main menu.
    
    Args:
        state: FSM context
        reason: Reason for reset (for logging)
    """
    current_state = await state.get_state()
    if current_state:
        logger.debug("Resetting FSM state: %s (reason: %s)", current_state, reason)
    await reset_all_states(state)


async def ensure_state_clear(cb: CallbackQuery, state: FSMContext) -> None:
    """
    Ensures state is cleared after callback handling.
    
    Wrapper for handlers that should not leave dangling states.
    """
    try:
        yield
    finally:
        await safe_reset_state(state, "handler_complete")


# ============================================================================
# IDEMPOTENCY DECORATORS FOR HANDLERS
# ============================================================================

from idempotency import (
    payment_idempotency,
    payment_click_guard,
    global_click_guard,
    action_rate_limiter,
    idempotent,
    rate_limited,
)


def payment_idempotent_handler(operation_name: str):
    """
    Decorator for payment handlers with idempotency guarantee.
    
    Usage:
        @router.callback_query(...)
        @payment_idempotent_handler('stars:buy:sub_30')
        async def handle_payment(cb: CallbackQuery, state: FSMContext):
            ...
    """
    return idempotent(operation_name)


def guarded_callback(ttl_seconds: float = 2.0, operation_id_prefix: str | None = None):
    """
    Decorator to guard against rapid duplicate clicks.
    
    Usage:
        @router.callback_query(...)
        @guarded_callback(ttl_seconds=3.0)
        async def handle_click(cb: CallbackQuery):
            ...
    
    Args:
        ttl_seconds: Time-to-live for the guard key
        operation_id_prefix: Optional prefix for operation ID. If provided,
            will be combined with callback data to form the guard key.
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            cb = kwargs.get('cb') or (args[0] if args else None)
            if not cb or not hasattr(cb, 'from_user'):
                return await func(*args, **kwargs)
            
            user_id = cb.from_user.id
            callback_data = getattr(cb, 'data', '') or ''
            
            # Build operation ID with optional prefix
            if operation_id_prefix:
                op_id = f"{operation_id_prefix}:{callback_data}"
            else:
                op_id = callback_data
            
            # Use payment guard for payment callbacks, global for others
            if any(prefix in op_id for prefix in ['pay_', 'platega_', 'payment:']):
                guard = payment_click_guard
            else:
                guard = global_click_guard
            
            if guard.is_duplicate(user_id, op_id):
                logger.debug(
                    "Duplicate click guarded: user=%s operation=%s",
                    user_id, op_id
                )
                await cb.answer(show_alert=False)
                return None
            
            return await func(*args, **kwargs)
        
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator


# ============================================================================
# PAYMENT FLOW STATE MACHINES
# ============================================================================

from fsm_states import PaymentStates, CatalogStates, SubscriptionStates


async def enter_payment_state_stars(
    state: FSMContext,
    user_id: int,
    tariff: str,
    invoice_message_id: int
) -> None:
    """
    Enters stars payment pending state.
    
    Args:
        state: FSM context
        user_id: User ID
        tariff: Tariff type (sub_7, sub_30, sub_90)
        invoice_message_id: ID of invoice message for cleanup
    """
    await state.update_data(
        payment_flow="stars",
        payment_tariff=tariff,
        invoice_message_id=invoice_message_id
    )
    await state.set_state(PaymentStates.stars_pending)
    logger.info(
        "Entered stars payment state: user=%s tariff=%s invoice_msg=%s",
        user_id, tariff, invoice_message_id
    )


async def enter_payment_state_sbp(
    state: FSMContext,
    user_id: int,
    tariff: str,
    amount: float | int
) -> None:
    """
    Enters SBP payment pending state.
    
    Args:
        state: FSM context
        user_id: User ID
        tariff: Tariff type
        amount: Payment amount
    """
    await state.update_data(
        payment_flow="sbp",
        payment_tariff=tariff,
        payment_amount=amount
    )
    await state.set_state(PaymentStates.sbp_pending)
    logger.info(
        "Entered SBP payment state: user=%s tariff=%s amount=%s",
        user_id, tariff, amount
    )


async def exit_payment_state_success(state: FSMContext) -> None:
    """Exits payment state after successful payment."""
    await state.clear()
    logger.debug("Payment state cleared after success")


async def exit_payment_state_failed(state: FSMContext) -> None:
    """Exits payment state after failed/timed out payment."""
    data = await state.get_data()
    logger.info(
        "Payment state cleared after failure: flow=%s tariff=%s",
        data.get('payment_flow'),
        data.get('payment_tariff')
    )
    await state.clear()


async def reset_fsm_state_safe(state: FSMContext, force: bool = False) -> None:
    """
    Safely resets FSM state with logging.
    
    Args:
        state: FSM context
        force: If True, clear even if in critical state
    """
    try:
        current_state = await state.get_state()
        if current_state:
            logger.debug("Resetting FSM state from %s", current_state)
        await state.clear()
    except Exception as e:
        logger.error("Failed to reset FSM state: %s", e)
        if force:
            try:
                await state.clear()
            except Exception:
                pass


async def update_payment_fsm_state(
    state: FSMContext,
    status: str,
    extra_data: dict | None = None
) -> None:
    """
    Updates payment FSM state with status and extra data.
    
    Args:
        state: FSM context
        status: Payment status (waiting_invoice, waiting_payment, payment_received, etc.)
        extra_data: Additional data to store
    """
    data = await state.get_data()
    data['payment_status'] = status
    if extra_data:
        data.update(extra_data)
    await state.set_data(data)
    logger.debug(
        "Updated payment FSM state: status=%s data=%s",
        status, extra_data or {}
    )


# ============================================================================
# ENTRY POINTS AND RESET PATHS
# ============================================================================

class FlowEntryPoint:
    """
    Defines entry points for each major flow.
    
    Each entry point:
    1. Clears any previous state
    2. Sets initial state for the flow
    3. Logs the transition
    """
    
    @staticmethod
    async def start_catalog(cb: CallbackQuery, state: FSMContext) -> None:
        """Entry point for catalog browsing flow."""
        await safe_reset_state(state, "entering_catalog")
        await state.set_state(CatalogStates.browsing)
        logger.info("User %s entered catalog flow", cb.from_user.id)
    
    @staticmethod
    async def start_payment(cb: CallbackQuery, state: FSMContext, tariff: str) -> None:
        """Entry point for payment flow."""
        await safe_reset_state(state, "entering_payment")
        await state.update_data(payment_tariff=tariff)
        await state.set_state(CatalogStates.tariff_selected)
        logger.info("User %s entered payment flow for tariff %s", cb.from_user.id, tariff)
    
    @staticmethod
    async def start_profile(cb: CallbackQuery, state: FSMContext) -> None:
        """Entry point for profile view flow."""
        await safe_reset_state(state, "entering_profile")
        await state.set_state(ProfileStates.viewing)
        logger.info("User %s entered profile flow", cb.from_user.id)
    
    @staticmethod
    async def start_support(cb: CallbackQuery, state: FSMContext) -> None:
        """Entry point for support flow."""
        await safe_reset_state(state, "entering_support")
        await state.set_state(SupportStates.browsing)
        logger.info("User %s entered support flow", cb.from_user.id)


# ============================================================================
# RECOVERY FROM DANGLING STATES
# ============================================================================

async def recover_from_dangling_state(
    cb: CallbackQuery,
    state: FSMContext
) -> bool:
    """
    Recovers from dangling/inconsistent state.
    
    Call this at the beginning of handlers to detect and fix
    inconsistent state situations.
    
    Returns:
        True if recovery was performed, False if state was consistent
    """
    current_state = await state.get_state()
    
    if not current_state:
        return False  # No state to recover from
    
    user_id = cb.from_user.id
    logger.warning(
        "Detected potentially dangling state: user=%s state=%s",
        user_id, current_state
    )
    
    # Strategy: Always reset on navigation callbacks
    nav_callbacks = ['nav:home', 'nav:profile', 'nav:support', 'nav:catalog']
    if cb.data in nav_callbacks or cb.data == 'open_profile':
        await safe_reset_state(state, "recovery_navigation")
        return True
    
    # For payment states, check if payment is still valid
    if current_state.startswith('PaymentStates'):
        data = await state.get_data()
        # If no payment data, clear state
        if not data.get('payment_flow'):
            await safe_reset_state(state, "recovery_payment_no_data")
            return True
    
    # Default: don't auto-recover, let handler decide
    return False
