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
from keyboards import get_config_post_conf_kb, get_support_center_kb, get_configs_devices_kb, get_profile_inline_kb, get_buy_inline_kb, get_admin_inline_kb, get_support_back_kb
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
    monkeypatch.setattr(handlers_user, 'get_user_subscription', AsyncMock(return_value=None))
    monkeypatch.setattr(handlers_user, 'get_status_text', lambda *_: ('не активна', '—'))
    monkeypatch.setattr(handlers_user, 'format_tg_username', lambda *_: '@u')
    monkeypatch.setattr(handlers_user, 'escape_html', lambda x: x)
    monkeypatch.setattr(handlers_user, 'subscription_is_active', lambda *_: False)
    monkeypatch.setattr(handlers_user, 'get_user_keys', AsyncMock(return_value=[]))
    monkeypatch.setattr(handlers_user, '_build_user_device_summary_line', AsyncMock(return_value='devices'))
    monkeypatch.setattr(handlers_user, '_build_user_traffic_summary_line', AsyncMock(return_value='traffic'))
    monkeypatch.setattr(handlers_user, 'get_setting', AsyncMock(return_value=0))
    monkeypatch.setattr(handlers_user, 'get_text', AsyncMock(return_value='profile'))

    asyncio.run(handlers_user.support_back_callback(cb))

    cb.message.edit_text.assert_awaited_once()
    assert 'profile' in cb.message.edit_text.await_args.args[0]


def test_profile_hub_contains_core_actions():
    kb = get_profile_inline_kb(subscription_active=True, referrals_enabled=True)
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert "🔄 Продлить доступ" in texts
    assert "🔑 Подключение" in texts
    assert "📊 Трафик и устройства" in texts
    assert "🎟 Ввести промокод" in texts
    assert "🎁 Рефералы" in texts
    assert "🆘 Помощь и поддержка" in texts
    assert "📖 Как подключиться" in texts


def test_profile_screen_is_short_summary_without_legacy_lines(monkeypatch):
    monkeypatch.setattr(handlers_user, "get_user_subscription", AsyncMock(return_value=None))
    monkeypatch.setattr(handlers_user, "get_status_text", lambda *_: ("не активна", "—"))
    monkeypatch.setattr(handlers_user, "format_tg_username", lambda *_: "@u")
    monkeypatch.setattr(handlers_user, "escape_html", lambda x: x)
    monkeypatch.setattr(handlers_user, "subscription_is_active", lambda *_: False)
    monkeypatch.setattr(handlers_user, "get_user_keys", AsyncMock(return_value=[]))
    monkeypatch.setattr(handlers_user, "_build_user_device_summary_line", AsyncMock(return_value="нет активных устройств"))
    monkeypatch.setattr(handlers_user, "_build_user_traffic_summary_line", AsyncMock(return_value="0 B"))
    monkeypatch.setattr(handlers_user, "get_setting", AsyncMock(return_value=0))
    monkeypatch.setattr(handlers_user, "get_text", AsyncMock(return_value="profile text\nТрафик:\nУстройства:"))

    text, _ = asyncio.run(handlers_user._render_profile_screen(SimpleNamespace(id=42, username="u", first_name="U")))

    assert "Последняя оплата" not in text
    assert "Дальше:" not in text
    assert "Поддержка:" not in text
    assert "Трафик:" in text
    assert "Устройства:" in text


def test_traffic_devices_screen_contains_detailed_blocks(monkeypatch):
    monkeypatch.setattr(handlers_user, "_build_user_traffic_lines", AsyncMock(return_value=["• dev1 — 1 GB", "• Всего трафика — 1 GB"]))
    monkeypatch.setattr(handlers_user, "_build_user_device_activity_lines", AsyncMock(return_value=["• dev1 — активен"]))
    monkeypatch.setattr(
        handlers_user,
        "get_text",
        AsyncMock(
            side_effect=lambda _key, **kwargs: (
                "📊 Трафик и устройства\n"
                f"{kwargs['traffic_block']}\n"
                "Последняя активность\n"
                f"{kwargs['device_activity_block']}"
            )
        ),
    )

    text, markup = asyncio.run(handlers_user._render_traffic_devices_screen(42))

    assert "Трафик и устройства" in text
    assert "dev1 — 1 GB" in text
    assert "Последняя активность" in text
    assert markup.inline_keyboard[0][0].text == "⬅️ В профиль"


def test_guide_and_buy_have_profile_back():
    buy_kb = get_buy_inline_kb()
    buy_texts = [button.text for row in buy_kb.inline_keyboard for button in row]
    assert "⬅️ В профиль" in buy_texts

    guide_back_kb = get_support_back_kb()
    guide_back_texts = [button.text for row in guide_back_kb.inline_keyboard for button in row]
    assert guide_back_texts == ["⬅️ В профиль"]


def test_admin_menu_labels_are_russian_only_for_main_sections():
    kb = get_admin_inline_kb()
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert "👥 Пользователи" in texts
    assert "💳 Платежи" in texts
    assert "📊 Статистика" in texts
    assert "🩺 Состояние" in texts
    assert "🔄 Синхронизация" in texts
    assert "🟠 Техработы" in texts
    assert "👥 Users" not in texts
    assert "💳 Payments" not in texts


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
    mark_ready = AsyncMock(return_value=True)
    monkeypatch.setattr(payments, 'mark_ready_notification_sent', mark_ready)
    monkeypatch.setattr(payments, '_send_user_active_config', AsyncMock(return_value=True))
    monkeypatch.setattr(payments, 'get_text', AsyncMock(return_value='progress'))
    monkeypatch.setattr(payments, 'get_payment_result_text', AsyncMock(return_value='ready'))
    monkeypatch.setattr(payments, 'get_post_payment_kb', lambda: None)

    asyncio.run(payments.success_pay(message))

    message.answer.assert_awaited_once_with('progress')
    progress_message.edit_text.assert_awaited_once()
    mark_ready.assert_awaited_once_with('tg-1')


def test_success_payment_config_missing_sets_pending_without_ready_notice(monkeypatch):
    payment_data = SimpleNamespace(
        invoice_payload='sub_30',
        currency='XTR',
        total_amount=30,
        telegram_payment_charge_id='tg-2',
        provider_payment_charge_id='pr-2',
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
    update_status = AsyncMock()
    monkeypatch.setattr(payments, 'update_last_provision_status', update_status)
    monkeypatch.setattr(payments, 'process_payment_provisioning', AsyncMock(return_value=True))
    mark_ready = AsyncMock(return_value=True)
    monkeypatch.setattr(payments, 'mark_ready_notification_sent', mark_ready)
    monkeypatch.setattr(payments, '_send_user_active_config', AsyncMock(return_value=False))
    monkeypatch.setattr(payments, 'write_audit_log', AsyncMock())
    monkeypatch.setattr(payments, 'get_text', AsyncMock(return_value='progress'))
    monkeypatch.setattr(payments, 'get_payment_result_text', AsyncMock(return_value='pending'))
    monkeypatch.setattr(payments, 'get_post_payment_kb', lambda: None)

    asyncio.run(payments.success_pay(message))

    mark_ready.assert_not_awaited()
    update_status.assert_any_await('tg-2', 'ready_config_pending')


def test_success_payment_delivery_exception_sets_pending_without_ready_notice(monkeypatch):
    payment_data = SimpleNamespace(
        invoice_payload='sub_30',
        currency='XTR',
        total_amount=30,
        telegram_payment_charge_id='tg-3',
        provider_payment_charge_id='pr-3',
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
    update_status = AsyncMock()
    monkeypatch.setattr(payments, 'update_last_provision_status', update_status)
    monkeypatch.setattr(payments, 'process_payment_provisioning', AsyncMock(return_value=True))
    mark_ready = AsyncMock(return_value=True)
    monkeypatch.setattr(payments, 'mark_ready_notification_sent', mark_ready)
    monkeypatch.setattr(payments, '_send_user_active_config', AsyncMock(side_effect=RuntimeError('delivery down')))
    monkeypatch.setattr(payments, 'write_audit_log', AsyncMock())
    monkeypatch.setattr(payments, '_log_critical_delivery_error', AsyncMock())
    monkeypatch.setattr(payments, 'get_text', AsyncMock(return_value='progress'))
    monkeypatch.setattr(payments, 'get_payment_result_text', AsyncMock(return_value='pending'))
    monkeypatch.setattr(payments, 'get_post_payment_kb', lambda: None)

    asyncio.run(payments.success_pay(message))

    mark_ready.assert_not_awaited()
    update_status.assert_any_await('tg-3', 'ready_config_pending')


def test_recovery_worker_delivery_pending_does_not_send_ready(monkeypatch):
    bot = SimpleNamespace(send_message=AsyncMock())
    monkeypatch.setattr(payments, 'get_repairable_payments', AsyncMock(return_value=[('tg-4', 77, 'sub_30')]))
    monkeypatch.setattr(payments, 'get_provisioning_attempt_count', AsyncMock(return_value=0))
    monkeypatch.setattr(payments, 'process_payment_provisioning', AsyncMock(return_value=True))
    monkeypatch.setattr(payments.config, 'STARS_PRICE_30_DAYS', 30)
    monkeypatch.setattr(payments, 'get_user_keys', AsyncMock(return_value=[]))
    monkeypatch.setattr(payments, 'update_last_provision_status', AsyncMock())
    monkeypatch.setattr(payments, 'mark_ready_notification_sent', AsyncMock(return_value=True))
    write_audit = AsyncMock()
    monkeypatch.setattr(payments, 'write_audit_log', write_audit)

    repaired = asyncio.run(payments.payment_recovery_worker(bot))

    assert repaired == 1
    bot.send_message.assert_not_awaited()
    payments.mark_ready_notification_sent.assert_not_awaited()
    payments.update_last_provision_status.assert_any_await('tg-4', 'ready_config_pending')
    write_audit.assert_any_await(77, 'payment_recovery_delivery_pending', 'payment_id=tg-4')


def test_success_payment_bookkeeping_failure_after_delivery_is_not_fatal(monkeypatch):
    payment_data = SimpleNamespace(
        invoice_payload='sub_30',
        currency='XTR',
        total_amount=30,
        telegram_payment_charge_id='tg-5',
        provider_payment_charge_id='pr-5',
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
    monkeypatch.setattr(payments, 'process_payment_provisioning', AsyncMock(return_value=True))
    monkeypatch.setattr(payments, '_send_user_active_config', AsyncMock(return_value=True))
    monkeypatch.setattr(payments, 'update_last_provision_status', AsyncMock(side_effect=[None, None, RuntimeError('db down')]))
    monkeypatch.setattr(payments, 'mark_ready_notification_sent', AsyncMock(return_value=True))
    monkeypatch.setattr(payments, 'write_audit_log', AsyncMock())
    monkeypatch.setattr(payments, 'update_payment_status', AsyncMock())
    monkeypatch.setattr(payments, 'get_text', AsyncMock(side_effect=['progress', 'payment_error']))
    monkeypatch.setattr(payments, 'get_payment_result_text', AsyncMock(return_value='ready'))
    monkeypatch.setattr(payments, 'get_post_payment_kb', lambda: None)

    asyncio.run(payments.success_pay(message))

    payments.update_payment_status.assert_not_awaited()
    message.answer.assert_awaited_once_with('progress')
    progress_message.edit_text.assert_awaited_once()


def test_manual_retry_delivery_success_sets_ready(monkeypatch):
    monkeypatch.setattr(payments, "fetchone", AsyncMock(return_value=(77, "sub_30", "needs_repair")))
    monkeypatch.setattr(payments.config, "STARS_PRICE_30_DAYS", 30)
    monkeypatch.setattr(payments, "process_payment_provisioning", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "get_user_keys", AsyncMock(return_value=[(1, 1, "conf", "vpn://1")]))
    bot = SimpleNamespace(send_message=AsyncMock())
    monkeypatch.setattr(payments, "update_last_provision_status", AsyncMock())
    monkeypatch.setattr(payments, "mark_ready_notification_sent", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "write_audit_log", AsyncMock())
    monkeypatch.setattr(payments, "get_text", AsyncMock(return_value="ok"))

    result = asyncio.run(payments.manual_retry_activation("tg-manual-1", bot=bot))

    assert result["result"] == "succeeded"
    assert "успешно" in result["message"]
    payments.update_last_provision_status.assert_awaited_with("tg-manual-1", "ready")
    bot.send_message.assert_awaited_once_with(77, "ok")


def test_manual_retry_config_missing_sets_ready_config_pending(monkeypatch):
    monkeypatch.setattr(payments, "fetchone", AsyncMock(return_value=(77, "sub_30", "needs_repair")))
    monkeypatch.setattr(payments.config, "STARS_PRICE_30_DAYS", 30)
    monkeypatch.setattr(payments, "process_payment_provisioning", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "get_user_keys", AsyncMock(return_value=[]))
    bot = SimpleNamespace(send_message=AsyncMock())
    monkeypatch.setattr(payments, "update_last_provision_status", AsyncMock())
    monkeypatch.setattr(payments, "mark_ready_notification_sent", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "write_audit_log", AsyncMock())

    result = asyncio.run(payments.manual_retry_activation("tg-manual-2", bot=bot))

    assert result["result"] == "succeeded"
    assert "в ожидании" in result["message"]
    payments.update_last_provision_status.assert_awaited_with("tg-manual-2", "ready_config_pending")
    payments.mark_ready_notification_sent.assert_not_awaited()
    bot.send_message.assert_not_awaited()


def test_manual_retry_delivery_exception_sets_ready_config_pending(monkeypatch):
    monkeypatch.setattr(payments, "fetchone", AsyncMock(return_value=(77, "sub_30", "needs_repair")))
    monkeypatch.setattr(payments.config, "STARS_PRICE_30_DAYS", 30)
    monkeypatch.setattr(payments, "process_payment_provisioning", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "get_user_keys", AsyncMock(return_value=[(1, 1, "conf", "vpn://1")]))
    bot = SimpleNamespace(send_message=AsyncMock(side_effect=RuntimeError("send failed")))
    monkeypatch.setattr(payments, "update_last_provision_status", AsyncMock())
    monkeypatch.setattr(payments, "mark_ready_notification_sent", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "write_audit_log", AsyncMock())
    monkeypatch.setattr(payments, "_log_critical_delivery_error", AsyncMock())
    monkeypatch.setattr(payments, "get_text", AsyncMock(return_value="ok"))

    result = asyncio.run(payments.manual_retry_activation("tg-manual-3", bot=bot))

    assert result["result"] == "succeeded"
    assert "в ожидании" in result["message"]
    payments.update_last_provision_status.assert_awaited_with("tg-manual-3", "ready_config_pending")
    payments.mark_ready_notification_sent.assert_not_awaited()


def test_manual_retry_bookkeeping_failure_after_delivery_is_not_fatal(monkeypatch):
    monkeypatch.setattr(payments, "fetchone", AsyncMock(return_value=(77, "sub_30", "needs_repair")))
    monkeypatch.setattr(payments.config, "STARS_PRICE_30_DAYS", 30)
    monkeypatch.setattr(payments, "process_payment_provisioning", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "get_user_keys", AsyncMock(return_value=[(1, 1, "conf", "vpn://1")]))
    bot = SimpleNamespace(send_message=AsyncMock())
    monkeypatch.setattr(payments, "update_last_provision_status", AsyncMock(side_effect=[RuntimeError("db down")]))
    monkeypatch.setattr(payments, "mark_ready_notification_sent", AsyncMock(return_value=True))
    monkeypatch.setattr(payments, "write_audit_log", AsyncMock())
    monkeypatch.setattr(payments, "get_text", AsyncMock(return_value="ok"))

    result = asyncio.run(payments.manual_retry_activation("tg-manual-4", bot=bot))

    assert result["result"] == "succeeded"
    assert "успешно" in result["message"]
    payments.mark_ready_notification_sent.assert_awaited_once_with("tg-manual-4")


def test_send_user_active_config_sends_conf_and_vpn_key_without_qr(monkeypatch):
    message = SimpleNamespace(
        answer_document=AsyncMock(),
        answer=AsyncMock(),
        answer_photo=AsyncMock(),
    )
    monkeypatch.setattr(
        payments,
        "get_user_keys",
        AsyncMock(return_value=[(11, 2, "[Interface]\nPrivateKey=test", "vpn://example")]),
    )

    sent = asyncio.run(payments._send_user_active_config(message, 42))

    assert sent is True
    message.answer_document.assert_awaited_once()
    message.answer.assert_awaited_once_with("<code>vpn://example</code>", parse_mode="HTML")
    message.answer_photo.assert_not_called()


def test_send_user_active_config_returns_false_when_no_configs(monkeypatch):
    message = SimpleNamespace(
        answer_document=AsyncMock(),
        answer=AsyncMock(),
        answer_photo=AsyncMock(),
    )
    monkeypatch.setattr(payments, "get_user_keys", AsyncMock(return_value=[]))

    sent = asyncio.run(payments._send_user_active_config(message, 42))

    assert sent is False
    message.answer_document.assert_not_called()
    message.answer.assert_not_called()
    message.answer_photo.assert_not_called()


def test_log_critical_delivery_error_is_fail_safe(monkeypatch):
    monkeypatch.setattr(payments, "CRITICAL_ERRORS_LOG", Path("/proc/1/critical_errors.log"))

    asyncio.run(payments._log_critical_delivery_error("pay-1", 42, "boom"))


def test_buy_tariff_opens_confirm_screen_without_invoice(monkeypatch):
    cb = DummyCallback(data=payments.CB_BUY_30)
    monkeypatch.setattr(payments, "is_purchase_maintenance_enabled", AsyncMock(return_value=False))
    monkeypatch.setattr(payments, "get_text", AsyncMock(return_value="confirm"))
    send_invoice_mock = AsyncMock()
    monkeypatch.setattr(payments, "_send_stars_invoice", send_invoice_mock)

    asyncio.run(payments.buy_30_days(cb))

    cb.message.edit_text.assert_awaited_once()
    send_invoice_mock.assert_not_called()


def test_confirm_screen_pay_sends_invoice(monkeypatch):
    cb = DummyCallback(data=payments.CB_BUY_PAY_30)
    bot = SimpleNamespace(send_invoice=AsyncMock(return_value=SimpleNamespace(message_id=101)), delete_message=AsyncMock())
    monkeypatch.setattr(payments, "is_purchase_maintenance_enabled", AsyncMock(return_value=False))
    monkeypatch.setattr(payments, "is_purchase_rate_limited", lambda *_: (False, 0))
    monkeypatch.setattr(payments, "is_purchase_rate_limited_persistent", AsyncMock(return_value=(False, 0)))

    asyncio.run(payments.buy_pay_30_days(cb, bot))

    bot.send_invoice.assert_awaited_once()


def test_confirm_screen_pay_remembers_pending_invoice(monkeypatch):
    payments.pending_invoices.clear()
    cb = DummyCallback(data=payments.CB_BUY_PAY_30)
    bot = SimpleNamespace(send_invoice=AsyncMock(return_value=SimpleNamespace(message_id=777)), delete_message=AsyncMock())
    monkeypatch.setattr(payments, "is_purchase_maintenance_enabled", AsyncMock(return_value=False))
    monkeypatch.setattr(payments, "is_purchase_rate_limited", lambda *_: (False, 0))
    monkeypatch.setattr(payments, "is_purchase_rate_limited_persistent", AsyncMock(return_value=(False, 0)))

    asyncio.run(payments.buy_pay_30_days(cb, bot))

    assert payments.get_pending_invoice(cb.from_user.id) == {"chat_id": 1, "message_id": 777, "payload": "sub_30"}


def test_open_profile_cleans_pending_invoice_and_state(monkeypatch):
    payments.pending_invoices.clear()
    payments.remember_pending_invoice(42, 1, 333, "sub_30")
    cb = DummyCallback(data=handlers_user.CB_OPEN_PROFILE)
    cb.bot = SimpleNamespace(delete_message=AsyncMock())
    monkeypatch.setattr(handlers_user, "_clear_promo_input_pending", AsyncMock())
    monkeypatch.setattr(handlers_user, "ensure_user_exists", AsyncMock())
    monkeypatch.setattr(handlers_user, "_render_profile_screen", AsyncMock(return_value=("profile", None)))

    asyncio.run(handlers_user.open_profile_callback(cb))

    cb.bot.delete_message.assert_awaited_once_with(chat_id=1, message_id=333)
    assert payments.get_pending_invoice(42) is None


def test_success_payment_clears_pending_invoice_state(monkeypatch):
    payments.pending_invoices.clear()
    payments.remember_pending_invoice(42, 1, 444, "sub_30")
    payment_data = SimpleNamespace(
        invoice_payload="bad_payload",
        currency="XTR",
        total_amount=30,
        telegram_payment_charge_id="tg-1",
        provider_payment_charge_id="pr-1",
    )
    message = SimpleNamespace(
        successful_payment=payment_data,
        from_user=SimpleNamespace(id=42, username="u", first_name="U"),
        bot=SimpleNamespace(),
        answer=AsyncMock(),
    )
    monkeypatch.setattr(payments, "get_text", AsyncMock(return_value="payload_error"))

    asyncio.run(payments.success_pay(message))

    assert payments.get_pending_invoice(42) is None


def test_new_buy_attempt_replaces_old_pending_invoice(monkeypatch):
    payments.pending_invoices.clear()
    payments.remember_pending_invoice(42, 1, 500, "sub_7")
    cb = DummyCallback(data=payments.CB_BUY_PAY_30)
    bot = SimpleNamespace(send_invoice=AsyncMock(return_value=SimpleNamespace(message_id=501)), delete_message=AsyncMock())
    monkeypatch.setattr(payments, "is_purchase_maintenance_enabled", AsyncMock(return_value=False))
    monkeypatch.setattr(payments, "is_purchase_rate_limited", lambda *_: (False, 0))
    monkeypatch.setattr(payments, "is_purchase_rate_limited_persistent", AsyncMock(return_value=(False, 0)))

    asyncio.run(payments.buy_pay_30_days(cb, bot))

    bot.delete_message.assert_awaited_once_with(chat_id=1, message_id=500)
    assert payments.get_pending_invoice(42) == {"chat_id": 1, "message_id": 501, "payload": "sub_30"}


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
