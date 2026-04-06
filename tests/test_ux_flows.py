import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.append(str(Path(__file__).resolve().parents[1] / 'bot'))

import handlers_user
import handlers_admin
import payments


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

    cb.message.answer.assert_awaited_once()
    kwargs = cb.message.answer.await_args.kwargs
    assert kwargs['reply_markup'] is not None


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
