"""
Callback Constants - Unified Architecture.

Format: action:context:payload

This module provides backward compatibility with old callbacks during migration.
"""

# ============================================================================
# NEW UNIFIED CALLBACK FORMAT (action:context:payload)
# ============================================================================

# --- NAVIGATION ---
NAV_HOME = "nav:home"
NAV_BACK_PREFIX = "nav:back:"  # nav:back:{page}
NAV_PROFILE = "nav:profile"
NAV_SUPPORT = "nav:support"
NAV_CATALOG = "nav:catalog"

# --- CATALOG / TARIFFS ---
CATALOG_VIEW_PREFIX = "catalog:view:"      # catalog:view:{tariff_id}
CATALOG_SELECT_PREFIX = "catalog:select:"  # catalog:select:{tariff_id}

# Tariff IDs
TARIFF_7D = "sub_7"
TARIFF_30D = "sub_30"
TARIFF_90D = "sub_90"

# New tariff selection callbacks
CB_TARIFF_7 = "tariff_sub_7"       # Keep for backward compat
CB_TARIFF_30 = "tariff_sub_30"     # Keep for backward compat
CB_TARIFF_90 = "tariff_sub_90"     # Keep for backward compat

# New format (will be used in future)
CB_CATALOG_7 = f"{CATALOG_SELECT_PREFIX}{TARIFF_7D}"
CB_CATALOG_30 = f"{CATALOG_SELECT_PREFIX}{TARIFF_30D}"
CB_CATALOG_90 = f"{CATALOG_SELECT_PREFIX}{TARIFF_90D}"

# --- PAYMENT ---
PAYMENT_START_STARS_PREFIX = "payment:start:stars:"   # payment:start:stars:{tariff}
PAYMENT_START_SBP_PREFIX = "payment:start:sbp:"       # payment:start:sbp:{tariff}
PAYMENT_CHECK_PREFIX = "payment:check:"               # payment:check:{txn_id}
PAYMENT_STATUS_PREFIX = "payment:status:"             # payment:status:{txn_id}

# Legacy payment callbacks (keep for backward compat)
CB_PAY_STARS_PREFIX = "pay_stars:"
CB_PAY_PLATEGA_PREFIX = "pay_platega:"
CB_PLATEGA_PAY_PREFIX = "platega_pay:"
CB_PLATEGA_CHECK_PREFIX = "platega_check:"

# Old buy callbacks (DEPRECATED but still supported)
CB_BUY_7 = "buy_7"
CB_BUY_30 = "buy_30"
CB_BUY_90 = "buy_90"
CB_BUY_PAY_7 = "buy_pay_7"
CB_BUY_PAY_30 = "buy_pay_30"
CB_BUY_PAY_90 = "buy_pay_90"
CB_PLATEGA_BUY_7 = "platega_buy_7"
CB_PLATEGA_BUY_30 = "platega_buy_30"
CB_PLATEGA_BUY_90 = "platega_buy_90"

# --- SUBSCRIPTION ---
SUBSCRIPTION_BUY_PREFIX = "subscription:buy:"       # subscription:buy:{tariff}
SUBSCRIPTION_RENEW_PREFIX = "subscription:renew:"   # subscription:renew:{tariff}
SUBSCRIPTION_DEVICES = "subscription:devices"
SUBSCRIPTION_CONFIG_PREFIX = "subscription:config:" # subscription:config:{device_id}

# Legacy config callbacks (keep for backward compat)
CB_CONFIG_DEVICE_PREFIX = "config_device_"
CB_CONFIG_CONF_PREFIX = "config_conf_"
CB_OPEN_CONFIGS = "open_configs"

# --- PROFILE ---
PROFILE_VIEW = "profile:view"
PROFILE_TRAFFIC = "profile:traffic"
PROFILE_REFERRALS = "profile:referrals"

# Legacy profile callbacks
CB_OPEN_PROFILE = "open_profile"
CB_OPEN_REFERRALS = "open_referrals"
CB_OPEN_TRAFFIC_DEVICES = "open_traffic_devices"

# --- SUPPORT ---
SUPPORT_BROWSE = "support:browse"
SUPPORT_PAYMENT = "support:payment"
SUPPORT_CONNECTION = "support:connection"
SUPPORT_TERMS = "support:terms"
SUPPORT_USEFUL = "support:useful"

# Legacy support callbacks
CB_OPEN_SUPPORT = "open_support"
CB_SUPPORT_PAYMENT = "support_payment"
CB_SUPPORT_CONNECTION = "support_connection"
CB_SUPPORT_TERMS = "support_terms"
CB_SUPPORT_USEFUL = "support_useful"

# --- PROMO CODES ---
PROMO_INPUT_START = "promo_input_start"
PROMO_INPUT_CANCEL = "promo_input_cancel"

# --- USER REISSUE ---
USER_REISSUE_CONFIRM = "user_reissue_confirm"
USER_REISSUE_CANCEL = "user_reissue_cancel"
USER_REISSUE_DEVICE_PREFIX = "user_reissue_device_"

# --- STATUS CHECKS ---
CB_CHECK_ACTIVATION_STATUS = "check_activation_status"
CB_CHECK_PAYMENT_STATUS = "check_payment_status"
CB_SHOW_INSTRUCTION = "show_instruction"
CB_SHOW_BUY_MENU = "show_buy_menu"

# ============================================================================
# ADMIN CALLBACKS (prefix a:)
# ============================================================================

ADMIN_CALLBACK_PREFIX = "a:"

CB_ADMIN_NOOP = "a:noop"

# Main admin menu
CB_ADMIN_LIST = "a:al"
CB_ADMIN_STATS = "a:as"
CB_ADMIN_SYNC = "a:sy"
CB_ADMIN_BROADCAST = "a:bc"
CB_ADMIN_COMMANDS = "a:ac"
CB_ADMIN_PRICES = "a:ap"
CB_ADMIN_PAYMENTS = "a:pm"
CB_ADMIN_PROMOCODES = "a:pc"
CB_ADMIN_MAINTENANCE = "a:mt"
CB_ADMIN_REFERRALS = "a:rf"
CB_ADMIN_SERVICE_SETTINGS = "a:ss"
CB_ADMIN_TEXT_OVERRIDES = "a:to"
CB_ADMIN_HEALTH = "a:hl"
CB_ADMIN_NETWORK_POLICY = "a:np"

# Price editing
CB_ADMIN_PRICE_EDIT_7 = "a:pe:7"
CB_ADMIN_PRICE_EDIT_30 = "a:pe:30"
CB_ADMIN_PRICE_EDIT_90 = "a:pe:90"
CB_ADMIN_PRICE_SAVE = "a:ps"
CB_ADMIN_PRICE_CANCEL = "a:px"

# Platega price editing
CB_ADMIN_PLATEGA_PRICE_EDIT_7 = "a:ppe:7"
CB_ADMIN_PLATEGA_PRICE_EDIT_30 = "a:ppe:30"
CB_ADMIN_PLATEGA_PRICE_EDIT_90 = "a:ppe:90"

# Service settings
CB_ADMIN_SERVICE_SUPPORT = "a:ss:su"
CB_ADMIN_SERVICE_DOWNLOAD = "a:ss:du"
CB_ADMIN_SERVICE_REFERRAL_TOGGLE = "a:ss:rt"
CB_ADMIN_SERVICE_INVITEE_BONUS = "a:ss:ib"
CB_ADMIN_SERVICE_INVITER_BONUS = "a:ss:rb"
CB_ADMIN_SERVICE_REF_RECURRING_BONUS = "a:ss:rrb"
CB_ADMIN_SERVICE_REF_RECURRING_MIN = "a:ss:rrm"
CB_ADMIN_SERVICE_TORRENT_TOGGLE = "a:ss:tt"

# Text overrides
CB_ADMIN_TEXT_START = "a:to:start"
CB_ADMIN_TEXT_BUY_MENU = "a:to:buy"
CB_ADMIN_TEXT_RENEW_MENU = "a:to:renew"
CB_ADMIN_TEXT_SUPPORT = "a:to:support"
CB_ADMIN_TEXT_SET_PREFIX = "a:to:set:"
CB_ADMIN_TEXT_RESET_PREFIX = "a:to:reset:"
CB_ADMIN_TEXT_VIEW_PREFIX = "a:to:view:"

# Payments management
CB_ADMIN_FIND_CHARGE = "a:pm:fc"
CB_ADMIN_LAST_PAYMENT = "a:pm:lp"
CB_ADMIN_PROBLEM_ACTIVATIONS = "a:pm:pa"
CB_ADMIN_PROBLEM_ACTIVATIONS_PAGE_PREFIX = "a:pm:pa:p:"
CB_ADMIN_OPEN_USER_CARD_PROBLEM_PREFIX = "a:pm:pa:uc:"
CB_ADMIN_MANAGE_USER_PROBLEM_PREFIX = "a:pm:pa:mu:"
CB_ADMIN_RETRY_ACTIVATION_PROBLEM_PREFIX = "a:pm:pa:ra:"

# Maintenance
CB_ADMIN_MAINTENANCE_ON = "a:mt:on"
CB_ADMIN_MAINTENANCE_OFF = "a:mt:off"
CB_ADMIN_MAINTENANCE_REFRESH = "a:mt:r"

# Promocodes
CB_ADMIN_PROMO_LIST = "a:pc:l"
CB_ADMIN_PROMO_CREATE = "a:pc:c"
CB_ADMIN_PROMO_DISABLE = "a:pc:d"

# User management
CB_ADMIN_OPEN_USER_CARD_PREFIX = "a:uc:"
CB_ADMIN_USERS_PAGE_PREFIX = "a:up:"
CB_ADMIN_MANAGE_USER_PREFIX = "a:um:"
CB_ADMIN_ADD_DAYS_PREFIX = "a:ud:"
CB_ADMIN_REVOKE_PREFIX = "a:ur:"
CB_ADMIN_DELETE_PREFIX = "a:udel:"
CB_ADMIN_RETRY_ACTIVATION_PREFIX = "a:ura:"
CB_ADMIN_DEVICE_DELETE_PREFIX = "a:udd:"
CB_ADMIN_DEVICE_REISSUE_PREFIX = "a:udi:"

# Confirmation dialogs
CB_CONFIRM_REVOKE = "a:cf:r"
CB_CANCEL_REVOKE = "a:cf:xr"
CB_CONFIRM_DELETE_USER = "a:cf:du"
CB_CANCEL_DELETE_USER = "a:cf:xdu"
CB_CONFIRM_DEVICE_DELETE = "a:cf:dd"
CB_CANCEL_DEVICE_DELETE = "a:cf:xdd"
CB_CONFIRM_DEVICE_REISSUE = "a:cf:dr"
CB_CANCEL_DEVICE_REISSUE = "a:cf:xdr"
CB_CONFIRM_ADD_DAYS = "a:ud:c"
CB_CANCEL_ADD_DAYS = "a:ud:x"

# Network policy
CB_ADMIN_NET_DENYLIST = "a:np:d"
CB_ADMIN_NET_SYNC_NOW = "a:np:s"
CB_ADMIN_DENYLIST_TOGGLE = "a:np:d:t"
CB_ADMIN_DENYLIST_VIEW_DOMAINS = "a:np:d:vd"
CB_ADMIN_DENYLIST_VIEW_CIDRS = "a:np:d:vc"
CB_ADMIN_DENYLIST_REPLACE_DOMAINS = "a:np:d:rd"
CB_ADMIN_DENYLIST_REPLACE_CIDRS = "a:np:d:rc"
CB_ADMIN_DENYLIST_SYNC = "a:np:d:sy"
CB_ADMIN_DENYLIST_MODE_SOFT = "a:np:d:m:s"
CB_ADMIN_DENYLIST_MODE_STRICT = "a:np:d:m:st"

# Navigation
CB_ADMIN_BACK_MAIN = "a:bk:m"
CB_ADMIN_REFRESH_REFERRALS = "a:rf:r"
CB_ADMIN_REFRESH_HEALTH = "a:rf:h"

# Broadcast
CB_BROADCAST_CONFIRM = "a:bc:cf"
CB_BROADCAST_CANCEL = "a:bc:x"
CB_BROADCAST_SEGMENT_PREFIX = "a:bc:s:"

# ============================================================================
# BACKWARD COMPATIBILITY MAPPINGS
# ============================================================================

# Maps old callback names to new ones for gradual migration
# During migration, both old and new handlers will work
BACKWARD_COMPAT_MAP = {
    # Old tariff callbacks -> new format (if needed in future)
    "buy_7": CB_CATALOG_7,
    "buy_30": CB_CATALOG_30,
    "buy_90": CB_CATALOG_90,
    
    # Payment methods can stay as-is since they're already functional
}

# Set of all legacy callbacks that should still be handled
LEGACY_CALLBACKS = {
    CB_BUY_7, CB_BUY_30, CB_BUY_90,
    CB_BUY_PAY_7, CB_BUY_PAY_30, CB_BUY_PAY_90,
    CB_PLATEGA_BUY_7, CB_PLATEGA_BUY_30, CB_PLATEGA_BUY_90,
    CB_CONFIG_DEVICE_PREFIX,  # prefix match
    CB_CONFIG_CONF_PREFIX,    # prefix match
}


def is_legacy_callback(data: str) -> bool:
    """Check if callback is from legacy format."""
    if data in LEGACY_CALLBACKS:
        return True
    
    # Check prefixes
    for prefix in LEGACY_CALLBACKS:
        if prefix.endswith('_') or prefix.endswith(':'):
            if data.startswith(prefix):
                return True
    
    return False


def get_new_callback_for_legacy(data: str) -> str | None:
    """Get new callback equivalent for legacy callback."""
    return BACKWARD_COMPAT_MAP.get(data)


def is_admin_callback_data(data: str | None) -> bool:
    """Check if callback is admin callback."""
    return bool(data and data.startswith(ADMIN_CALLBACK_PREFIX))


# Button texts (unchanged from ui_constants.py)
BTN_PROFILE = "👤 Профиль"
BTN_CONFIGS = "🔑 Подключение"
BTN_BUY = "💳 Купить / Продлить"
BTN_SUPPORT = "🆘 Поддержка"
BTN_ADMIN = "⚙️ Админка"
