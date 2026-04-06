import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from datetime import datetime

from aiogram.exceptions import TelegramBadRequest

sys.path.append(str(Path(__file__).resolve().parents[1] / 'bot'))

import handlers_user
import handlers_admin
import payments
from keyboards import get_config_post_conf_kb, get_support_center_kb, get_configs_devices_kb
from ui_constants import CB_CANCEL_ADD_DAYS, CB_CONFIRM_ADD_DAYS, is_admin_callback_data


class DummyMessage:
    def __init__(self):
        self.answer = AsyncMock()
        self.answer_document = AsyncMock()
        self.edit_text = AsyncMock()
        self.chat = SimpleNamespace(id=1)


class DummyCallback:
    def __init__(self, data='x'):
        self.data = data
        self.from_user = SimpleNamespace(id=42, username='u', first_name='U')
        self.message = DummyMessage()
        self.answer = AsyncMock()
        self.bot = SimpleNamespace()


def test_conf_delivery_has_explicit_back_navigation(monkeypatch):
    cb = DummyCallback(data=f"{handlers_user.CB_CONFIG_CONF_PREFIX}1")
    monkeypatch.setattr(handlers_user, 'ensure_user_exists', AsyncMock())
    monkeypatch.setattr(handlers_user, '_find_user_config_by_key_id', AsyncMock(return_value=(1, 2, 'conf', 'vpn://x')))
    monkeypatch.setattr(handlers_user, 'get_text', AsyncMock(side_effect=['cap', 'sent']))

    asyncio.run(handlers_user.send_selected_device_conf(cb))

    cb.message.answer_document.assert_awaited_once()
    cb.message.edit_text.assert_awaited_once()
    assert cb.message.edit_text.await_args.args[0] == "sent"


def test_post_conf_keyboard_does_not_repeat_issue_wording():
    kb = get_config_post_conf_kb(1)
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert not any("Выдать .conf файл" in text for text in texts)
    assert any("ещё раз" in text for text in texts)


def test_support_center_has_no_self_loop_back_label():
    kb = get_support_center_kb()
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert "⬅️ К разделу помощи" not in texts
    assert "⬅️ В профиль" in texts


def test_configs_screen_contains_profile_back():
    kb = get_configs_devices_kb([(1, 1, "conf", "vpn://1")])
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert "⬅️ В профиль" in texts


def test_support_back_returns_to_support_center(monkeypatch):
    cb = DummyCallback(data=handlers_user.CB_SUPPORT_BACK)
    monkeypatch.setattr(handlers_user, '_clear_promo_input_pending', AsyncMock())
    monkeypatch.setattr(handlers_user, 'get_support_full_text', AsyncMock(return_value='support-center'))

    asyncio.run(handlers_user.support_back_callback(cb))

    cb.message.edit_text.assert_awaited_once()
    assert 'support-center' in cb.message.edit_text.await_args.args[0]


def test_admin_health_stats_sync_use_back_navigation(monkeypatch):
    cb = DummyCallback()
    cb.from_user = SimpleNamespace(id=handlers_admin.ADMIN_ID)
    monkeypatch.setattr(handlers_admin, '_clear_service_settings_pending', AsyncMock())
    monkeypatch.setattr(handlers_admin, '_clear_network_policy_pending', AsyncMock())
    monkeypatch.setattr(handlers_admin, 'build_runtime_smokecheck_text', AsyncMock(return_value='health'))
    monkeypatch.setattr(handlers_admin, 'build_stats_text', AsyncMock(return_value='stats'))
    monkeypatch.setattr(handlers_admin, 'build_awg_sync_text', AsyncMock(return_value='sync'))

    asyncio.run(handlers_admin.admin_health_summary(cb))
    asyncio.run(handlers_admin.admin_stats_cb(cb))
    asyncio.run(handlers_admin.admin_sync_awg(cb))

    assert cb.message.edit_text.await_count == 3


def test_admin_add_days_30_requires_confirmation(monkeypatch):
    cb = DummyCallback(data=f"{handlers_admin.CB_ADMIN_ADD_DAYS_PREFIX}77_30_0")
    cb.from_user = SimpleNamespace(id=handlers_admin.ADMIN_ID)
    monkeypatch.setattr(handlers_admin, 'clear_pending_admin_action', AsyncMock())
    monkeypatch.setattr(handlers_admin, 'set_pending_admin_action', AsyncMock())
    monkeypatch.setattr(handlers_admin, 'issue_subscription', AsyncMock())

    asyncio.run(handlers_admin.admin_add_days_btn(cb))

    handlers_admin.set_pending_admin_action.assert_awaited_once()
    handlers_admin.issue_subscription.assert_not_called()


def test_payment_success_uses_single_progress_message(monkeypatch):
    payment_data = SimpleNamespace(
        invoice_payload='sub_30',
        currency='XTR',
        total_amount=30,
        telegram_payment_charge_id='tg-1',
        provider_payment_charge_id='pr-1',
    )
    progress_message = SimpleNamespace(edit_text=AsyncMock())
    message = SimpleNamespace(
        successful_payment=payment_data,
        from_user=SimpleNamespace(id=42, username='u', first_name='U'),
        bot=SimpleNamespace(),
        answer=AsyncMock(return_value=progress_message),
    )

    monkeypatch.setattr(payments.config, 'STARS_PRICE_30_DAYS', 30)
    monkeypatch.setattr(payments, 'get_payment_status', AsyncMock(return_value=None))
    monkeypatch.setattr(payments, 'payment_already_processed', AsyncMock(return_value=False))
    monkeypatch.setattr(payments, 'ensure_user_exists', AsyncMock())
    monkeypatch.setattr(payments, 'save_payment', AsyncMock())
    monkeypatch.setattr(payments, 'update_last_provision_status', AsyncMock())
    monkeypatch.setattr(payments, 'process_payment_provisioning', AsyncMock(return_value=True))
    monkeypatch.setattr(payments, 'mark_ready_notification_sent', AsyncMock())
    monkeypatch.setattr(payments, '_send_user_active_config', AsyncMock(return_value=True))
    monkeypatch.setattr(payments, 'get_text', AsyncMock(return_value='progress'))
    monkeypatch.setattr(payments, 'get_payment_result_text', AsyncMock(return_value='ready'))
    monkeypatch.setattr(payments, 'get_post_payment_kb', lambda: None)

    asyncio.run(payments.success_pay(message))

    message.answer.assert_awaited_once_with('progress')
    progress_message.edit_text.assert_awaited_once()


def test_user_send_or_edit_noop_does_not_fallback_to_new_message():
    message = DummyMessage()
    message.edit_text = AsyncMock(side_effect=TelegramBadRequest(None, "Bad Request: message is not modified"))
    cb = DummyCallback()
    cb.message = message

    asyncio.run(handlers_user._send_or_edit_user_screen(cb, "screen"))

    message.answer.assert_not_called()


def test_admin_send_or_edit_noop_does_not_fallback_to_new_message():
    message = DummyMessage()
    message.edit_text = AsyncMock(side_effect=TelegramBadRequest(None, "Bad Request: message is not modified"))
    cb = DummyCallback()
    cb.from_user = SimpleNamespace(id=handlers_admin.ADMIN_ID)
    cb.message = message

    asyncio.run(handlers_admin._send_or_edit_admin_message(cb, "screen", reply_markup=None))

    message.answer.assert_not_called()


def test_send_or_edit_fallback_kept_for_real_edit_errors():
    message = DummyMessage()
    message.edit_text = AsyncMock(side_effect=TelegramBadRequest(None, "Bad Request: message can't be edited"))
    cb = DummyCallback()
    cb.message = message

    asyncio.run(handlers_user._send_or_edit_user_screen(cb, "screen"))

    message.answer.assert_awaited_once()


def test_admin_add_days_callbacks_are_in_admin_namespace():
    assert is_admin_callback_data(CB_CONFIRM_ADD_DAYS) is True
    assert is_admin_callback_data(CB_CANCEL_ADD_DAYS) is True


def test_admin_add_days_confirm_callback_still_works(monkeypatch):
    cb = DummyCallback(data=CB_CONFIRM_ADD_DAYS)
    cb.from_user = SimpleNamespace(id=handlers_admin.ADMIN_ID)
    cb.bot = SimpleNamespace()
    monkeypatch.setattr(handlers_admin, "pop_pending_admin_action", AsyncMock(return_value={"uid": 77, "days": 30, "page": 0}))
    monkeypatch.setattr(handlers_admin, "admin_command_limited", lambda *_: False)
    monkeypatch.setattr(handlers_admin, "issue_subscription", AsyncMock(return_value=datetime(2026, 1, 1, 12, 0)))
    monkeypatch.setattr(handlers_admin, "notify_user_subscription_granted", AsyncMock(return_value=True))
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())

    asyncio.run(handlers_admin.admin_add_days_confirm(cb))

    handlers_admin.issue_subscription.assert_awaited_once_with(77, 30)
    cb.message.answer.assert_awaited_once()


def test_admin_add_days_cancel_callback_still_works(monkeypatch):
    cb = DummyCallback(data=CB_CANCEL_ADD_DAYS)
    cb.from_user = SimpleNamespace(id=handlers_admin.ADMIN_ID)
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())

    asyncio.run(handlers_admin.admin_add_days_cancel(cb))

    handlers_admin.clear_pending_admin_action.assert_awaited_once()
    cb.message.answer.assert_awaited_once()
