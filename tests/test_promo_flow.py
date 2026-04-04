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
    monkeypatch.setattr(handlers_user, 'get_profile_inline_kb', lambda *_: None)
    asyncio.run(handlers_user.profile(message))

    assert handlers_user.clear_pending_admin_action.await_count >= 5
