import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram.filters import CommandObject

sys.path.append(str(Path(__file__).resolve().parents[1] / 'bot'))

import handlers_user
from ui_constants import BTN_PROMO


class DummyMessage:
    def __init__(self, text=''):
        self.text = text
        self.from_user = SimpleNamespace(id=42, username='u', first_name='U')
        self.answer = AsyncMock()


def test_promo_button_starts_pending_mode(monkeypatch):
    message = DummyMessage(text=BTN_PROMO)
    monkeypatch.setattr(handlers_user, 'ensure_user_exists', AsyncMock())
    monkeypatch.setattr(handlers_user, 'set_pending_admin_action', AsyncMock())
    monkeypatch.setattr(handlers_user, 'clear_pending_admin_action', AsyncMock())

    asyncio.run(handlers_user.promo_from_menu(message))

    handlers_user.set_pending_admin_action.assert_awaited_once()
    message.answer.assert_awaited()


def test_profile_inline_promo_starts_same_flow(monkeypatch):
    cb = SimpleNamespace(
        data='promo_input_start',
        from_user=SimpleNamespace(id=42, username='u', first_name='U'),
        message=SimpleNamespace(answer=AsyncMock()),
        answer=AsyncMock(),
    )
    monkeypatch.setattr(handlers_user, 'set_pending_admin_action', AsyncMock())
    monkeypatch.setattr(handlers_user, 'clear_pending_admin_action', AsyncMock())

    asyncio.run(handlers_user.promo_input_start_callback(cb))

    handlers_user.set_pending_admin_action.assert_awaited_once()
    cb.message.answer.assert_awaited()


def test_pending_promo_valid_input_applies_shared_path(monkeypatch):
    message = DummyMessage(text=' spring10 ')
    monkeypatch.setattr(handlers_user, 'get_pending_admin_action', AsyncMock(return_value={'action': 'user_promo_input'}))
    monkeypatch.setattr(handlers_user, 'clear_pending_admin_action', AsyncMock())
    monkeypatch.setattr(handlers_user, '_apply_promo_code', AsyncMock())

    handled = asyncio.run(handlers_user._handle_promo_input_message(message))

    assert handled is True
    handlers_user._apply_promo_code.assert_awaited_once_with(message, 'SPRING10')


def test_pending_promo_cancel_clears_state(monkeypatch):
    message = DummyMessage(text='отмена')
    monkeypatch.setattr(handlers_user, 'get_pending_admin_action', AsyncMock(return_value={'action': 'user_promo_input'}))
    monkeypatch.setattr(handlers_user, 'clear_pending_admin_action', AsyncMock())

    handled = asyncio.run(handlers_user._handle_promo_input_message(message))

    assert handled is True
    handlers_user.clear_pending_admin_action.assert_awaited_once_with(42, handlers_user.USER_PROMO_INPUT_ACTION_KEY)


def test_promo_command_clears_pending_then_fallback_works(monkeypatch):
    message = DummyMessage(text='/promo spring10')
    command = CommandObject(prefix='/', command='promo', mention=None, args='spring10')
    monkeypatch.setattr(handlers_user, 'ensure_user_exists', AsyncMock())
    monkeypatch.setattr(handlers_user, 'clear_pending_admin_action', AsyncMock())
    monkeypatch.setattr(handlers_user, '_apply_promo_code', AsyncMock())

    asyncio.run(handlers_user.promo_cmd(message, command))

    handlers_user.clear_pending_admin_action.assert_awaited_once_with(42, handlers_user.USER_PROMO_INPUT_ACTION_KEY)
    handlers_user._apply_promo_code.assert_awaited_once_with(message, 'SPRING10')


def test_top_level_flows_clear_stale_promo_pending(monkeypatch):
    message = DummyMessage()
    monkeypatch.setattr(handlers_user, 'clear_pending_admin_action', AsyncMock())
    monkeypatch.setattr(handlers_user, 'ensure_user_exists', AsyncMock())
    monkeypatch.setattr(handlers_user, 'capture_referral_start', AsyncMock())
    monkeypatch.setattr(handlers_user, 'get_text', AsyncMock(return_value='ok'))
    monkeypatch.setattr(handlers_user, 'get_main_menu', lambda *_: None)
    asyncio.run(handlers_user.start(message, CommandObject(prefix='/', command='start', mention=None, args=None)))

    monkeypatch.setattr(handlers_user, '_send_buy_menu', AsyncMock())
    asyncio.run(handlers_user.buy(message))

    monkeypatch.setattr(handlers_user, '_send_configs_menu', AsyncMock())
    asyncio.run(handlers_user.my_keys(message))

    monkeypatch.setattr(handlers_user, '_send_support_center', AsyncMock())
    asyncio.run(handlers_user.support(message))

    monkeypatch.setattr(handlers_user, 'get_user_subscription', AsyncMock(return_value=None))
    monkeypatch.setattr(handlers_user, 'get_status_text', lambda *_: ('не активна', '—'))
    monkeypatch.setattr(handlers_user, 'format_tg_username', lambda *_: '@u')
    monkeypatch.setattr(handlers_user, 'escape_html', lambda x: x)
    monkeypatch.setattr(handlers_user, 'subscription_is_active', lambda *_: False)
    monkeypatch.setattr(handlers_user, 'get_user_keys', AsyncMock(return_value=[]))
    monkeypatch.setattr(handlers_user, 'get_latest_user_payment_summary', AsyncMock(return_value=None))
    monkeypatch.setattr(handlers_user, '_build_user_device_activity_lines', AsyncMock(return_value=['-']))
    monkeypatch.setattr(handlers_user, '_build_user_traffic_lines', AsyncMock(return_value=['-']))
    monkeypatch.setattr(handlers_user, 'get_support_short_text', AsyncMock(return_value='support'))
    monkeypatch.setattr(handlers_user, 'get_setting', AsyncMock(return_value=0))
    monkeypatch.setattr(handlers_user, 'get_profile_inline_kb', lambda *_args, **_kwargs: None)
    asyncio.run(handlers_user.profile(message))

    assert handlers_user.clear_pending_admin_action.await_count >= 5


def test_referrals_and_guide_clear_stale_promo_pending(monkeypatch):
    message = DummyMessage()
    message.bot = SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(username='bot')))
    monkeypatch.setattr(handlers_user, '_clear_promo_input_pending', AsyncMock())
    monkeypatch.setattr(handlers_user, 'ensure_user_exists', AsyncMock())
    monkeypatch.setattr(handlers_user, 'get_referral_screen_data', AsyncMock(return_value={'link': 'l', 'invited_count': 0, 'rewarded_count': 0, 'bonus_days': 0}))
    monkeypatch.setattr(handlers_user, 'get_text', AsyncMock(return_value='ok'))
    monkeypatch.setattr(handlers_user, 'get_instruction_with_policy_text', AsyncMock(return_value='guide'))

    asyncio.run(handlers_user.referrals_screen(message, message.bot))
    asyncio.run(handlers_user.guide(message))

    assert handlers_user._clear_promo_input_pending.await_count == 2


def test_navigation_callbacks_clear_stale_promo_pending(monkeypatch):
    cb = SimpleNamespace(
        from_user=SimpleNamespace(id=42, username='u', first_name='U'),
        message=SimpleNamespace(answer=AsyncMock()),
        answer=AsyncMock(),
    )
    monkeypatch.setattr(handlers_user, '_clear_promo_input_pending', AsyncMock())
    monkeypatch.setattr(handlers_user, 'ensure_user_exists', AsyncMock())
    monkeypatch.setattr(handlers_user, '_send_configs_menu', AsyncMock())
    monkeypatch.setattr(handlers_user, '_send_support_center', AsyncMock())
    monkeypatch.setattr(handlers_user, 'is_purchase_maintenance_enabled', AsyncMock(return_value=False))
    monkeypatch.setattr(handlers_user, 'get_setting', AsyncMock(return_value=3))
    monkeypatch.setattr(handlers_user, 'get_instruction_with_policy_text', AsyncMock(return_value='guide'))
    monkeypatch.setattr(handlers_user, 'get_support_short_text', AsyncMock(return_value='support'))
    monkeypatch.setattr(handlers_user, 'get_support_full_text', AsyncMock(return_value='support full'))
    monkeypatch.setattr(handlers_user, 'get_text', AsyncMock(return_value='ok'))
    monkeypatch.setattr(handlers_user, 'get_user_keys', AsyncMock(return_value=[]))
    monkeypatch.setattr(handlers_user, 'get_user_subscription', AsyncMock(return_value=None))
    monkeypatch.setattr(handlers_user, 'subscription_is_active', lambda *_: False)

    asyncio.run(handlers_user.open_configs_from_profile(cb))
    asyncio.run(handlers_user.open_support_callback(cb))
    asyncio.run(handlers_user.show_buy_menu_callback(cb))
    asyncio.run(handlers_user.show_instruction_callback(cb))
    asyncio.run(handlers_user.support_payment_callback(cb))
    asyncio.run(handlers_user.support_connection_callback(cb))
    asyncio.run(handlers_user.support_terms_callback(cb))
    asyncio.run(handlers_user.support_back_callback(cb))

    assert handlers_user._clear_promo_input_pending.await_count == 8


def test_pending_promo_filter_does_not_match_without_state(monkeypatch):
    message = DummyMessage(text='hello')
    monkeypatch.setattr(handlers_user, 'get_pending_admin_action', AsyncMock(return_value=None))

    has_pending = asyncio.run(handlers_user.HasPendingPromoInput()(message))

    assert has_pending is False


def test_pending_promo_filter_matches_only_with_state(monkeypatch):
    message = DummyMessage(text='promo')
    monkeypatch.setattr(handlers_user, 'get_pending_admin_action', AsyncMock(return_value={'action': handlers_user.USER_PROMO_INPUT_ACTION_KEY}))

    has_pending = asyncio.run(handlers_user.HasPendingPromoInput()(message))

    assert has_pending is True


def test_fallback_reachable_when_no_pending_promo_input(monkeypatch):
    message = DummyMessage(text='something unknown')
    monkeypatch.setattr(handlers_user, 'get_pending_admin_action', AsyncMock(return_value=None))
    monkeypatch.setattr(handlers_user, 'get_text', AsyncMock(return_value='unknown'))
    monkeypatch.setattr(handlers_user, 'get_main_menu', lambda *_: None)

    has_pending = asyncio.run(handlers_user.HasPendingPromoInput()(message))
    assert has_pending is False

    asyncio.run(handlers_user.fallback_message(message))
    message.answer.assert_awaited_once()
