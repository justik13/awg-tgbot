import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram.filters import CommandObject

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import handlers_admin
import network_policy
from keyboards import get_admin_inline_kb


def _cb(data: str):
    return SimpleNamespace(
        data=data,
        from_user=SimpleNamespace(id=handlers_admin.ADMIN_ID),
        message=SimpleNamespace(answer=AsyncMock()),
        answer=AsyncMock(),
    )


def _msg(text: str):
    return SimpleNamespace(
        text=text,
        from_user=SimpleNamespace(id=handlers_admin.ADMIN_ID),
        answer=AsyncMock(),
    )


def test_admin_keyboard_has_button_first_sections():
    kb = get_admin_inline_kb()
    texts = [button.text for row in kb.inline_keyboard for button in row]
    assert "💳 Платежи" in texts
    assert "🟠 Техработы" in texts
    assert "🩺 Состояние" in texts
    assert "🎟 Промокоды" in texts
    assert "🌐 Сеть" in texts
    assert "⚙️ Настройки сервиса" in texts
    assert "📝 Тексты" in texts


def test_payment_lookup_by_charge_uses_trim_and_shows_jump(monkeypatch):
    message = _msg("  tg-1  ")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[{"a": 1}, None]))
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(
        handlers_admin,
        "get_payment_summary_by_charge_id",
        AsyncMock(return_value={"user_id": 42, "payment_id": "tg-1", "status": "applied", "amount": 100, "currency": "XTR", "payload": "p", "created_at": "now", "last_provision_status": "ok"}),
    )
    asyncio.run(handlers_admin.admin_payment_lookup_capture_input(message))
    handlers_admin.get_payment_summary_by_charge_id.assert_awaited_once_with("tg-1")
    assert message.answer.await_count == 1


def test_open_user_card_jump_reuses_existing_flow(monkeypatch):
    cb = _cb("a:uc:55_0")
    monkeypatch.setattr(handlers_admin, "_send_user_manage_card", AsyncMock())
    asyncio.run(handlers_admin.admin_open_user_card_from_payment(cb))
    handlers_admin._send_user_manage_card.assert_awaited_once()


def test_maintenance_toggle_buttons(monkeypatch):
    cb_on = _cb(handlers_admin.CB_ADMIN_MAINTENANCE_ON)
    monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_maintenance_on_cb(cb_on))
    handlers_admin.set_app_setting.assert_awaited_once()

    cb_off = _cb(handlers_admin.CB_ADMIN_MAINTENANCE_OFF)
    monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_maintenance_off_cb(cb_off))
    handlers_admin.set_app_setting.assert_awaited_once()


def test_findpay_slash_fallback(monkeypatch):
    message = _msg("/findpay tg-2")
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(
        handlers_admin,
        "get_payment_summary_by_charge_id",
        AsyncMock(return_value={"user_id": 77, "payment_id": "tg-2", "status": "received", "amount": 10, "currency": "XTR", "payload": "x", "created_at": "now", "last_provision_status": "payment_received"}),
    )
    asyncio.run(handlers_admin.findpay_cmd(message, CommandObject(prefix="/", command="findpay", mention=None, args="tg-2")))
    message.answer.assert_awaited_once()


def test_network_policy_screen_renders_state(monkeypatch):
    monkeypatch.setattr(handlers_admin, "policy_metrics", AsyncMock(return_value={
        "qos_errors": 1,
        "denylist_errors": 2,
        "qos_last_sync_ok": 1,
        "denylist_last_sync_ok": 1,
        "denylist_last_sync_ts": 123,
        "denylist_entries": 5,
    }))
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[1, 150, 0, 1, "soft", 30]))
    text = asyncio.run(handlers_admin._render_network_policy_text())
    assert "QoS: <b>включено</b>" in text
    assert "Скорость по умолчанию: <b>150 Mbit/s</b>" in text
    assert "Режим denylist: <b>soft</b>" in text
    assert "Последняя синхронизация QoS: <b>1</b>" in text


def test_service_settings_support_and_download_update(monkeypatch):
    message_support = _msg("@ops")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[{"a": 1}, None, None, None]))
    monkeypatch.setattr(handlers_admin, "save_env_value", lambda *_: None)
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_service_settings_capture_input(message_support))
    assert message_support.answer.await_count == 1

    message_download = _msg("https://example.com/app")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[None, {"a": 1}, None, None]))
    monkeypatch.setattr(handlers_admin, "save_env_value", lambda *_: None)
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_service_settings_capture_input(message_download))
    assert message_download.answer.await_count == 1


def test_service_settings_referral_controls_update(monkeypatch):
    cb = _cb(handlers_admin.CB_ADMIN_SERVICE_REFERRAL_TOGGLE)
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[1, 1, 1, 1, 5, 3, 1]))
    monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    monkeypatch.setattr(handlers_admin, "admin_service_settings_screen", AsyncMock())
    asyncio.run(handlers_admin.admin_service_referral_toggle(cb))
    handlers_admin.set_app_setting.assert_awaited()


def test_qos_toggle_updates_setting_and_sync(monkeypatch):
    cb = _cb(handlers_admin.CB_ADMIN_QOS_TOGGLE)
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(return_value=0))
    monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_qos_toggle(cb))
    handlers_admin.set_app_setting.assert_awaited_once_with("QOS_ENABLED", "1", updated_by=handlers_admin.ADMIN_ID)
    handlers_admin.sync_qos_state.assert_awaited_once()


def test_qos_default_rate_update_via_pending_input(monkeypatch):
    message = _msg("40")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[{"action": "x"}, None, None, None]))
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_network_policy_capture_input(message))
    handlers_admin.set_app_setting.assert_awaited_once_with("DEFAULT_KEY_RATE_MBIT", "40", updated_by=handlers_admin.ADMIN_ID)


def test_device_speed_override_and_reset(monkeypatch):
    message = _msg("10")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[None, {"uid": 11, "device_num": 1, "page": 0}, None, None]))
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "set_active_key_rate_limit", AsyncMock(return_value=True))
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_send_user_manage_card", AsyncMock())
    asyncio.run(handlers_admin.admin_network_policy_capture_input(message))
    handlers_admin.set_active_key_rate_limit.assert_awaited_once_with(11, 1, 10)

    cb = _cb(f"{handlers_admin.CB_ADMIN_DEVICE_SPEED_RESET_PREFIX}11_1_0")
    monkeypatch.setattr(handlers_admin, "set_active_key_rate_limit", AsyncMock(return_value=True))
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_send_user_manage_card", AsyncMock())
    asyncio.run(handlers_admin.admin_device_speed_reset(cb))
    handlers_admin.set_active_key_rate_limit.assert_awaited_once_with(11, 1, None)


def test_device_speed_missing_active_device_does_not_sync(monkeypatch):
    message = _msg("15")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[None, {"uid": 11, "device_num": 2, "page": 0}, None, None]))
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "set_active_key_rate_limit", AsyncMock(return_value=False))
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    asyncio.run(handlers_admin.admin_network_policy_capture_input(message))
    handlers_admin.sync_qos_state.assert_not_awaited()
    message.answer.assert_awaited()


def test_top_level_admin_screens_clear_network_policy_pending(monkeypatch):
    cb = _cb(handlers_admin.CB_ADMIN_COMMANDS)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    asyncio.run(handlers_admin.admin_manual_commands(cb))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()


def test_stats_and_sync_callbacks_clear_network_policy_pending(monkeypatch):
    cb_stats = _cb(handlers_admin.CB_ADMIN_STATS)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "build_stats_text", AsyncMock(return_value="ok"))
    asyncio.run(handlers_admin.admin_stats_cb(cb_stats))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()

    cb_sync = _cb(handlers_admin.CB_ADMIN_SYNC)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    monkeypatch.setattr(handlers_admin, "denylist_sync", AsyncMock())
    monkeypatch.setattr(handlers_admin, "build_awg_sync_text", AsyncMock(return_value="sync"))
    asyncio.run(handlers_admin.admin_sync_awg(cb_sync))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()


def test_stats_slash_command_clears_network_policy_pending(monkeypatch):
    message = _msg("/stats")
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "build_stats_text", AsyncMock(return_value="stats"))
    asyncio.run(handlers_admin.stats_cmd(message))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()


def test_top_level_slash_commands_clear_service_pending(monkeypatch):
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "build_runtime_smokecheck_text", AsyncMock(return_value="ok"))
    asyncio.run(handlers_admin.health_cmd(_msg("/health")))
    handlers_admin._clear_service_settings_pending.assert_awaited()

    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_render_network_policy_text", AsyncMock(return_value="ok"))
    asyncio.run(handlers_admin.netpolicy_cmd(_msg("/netpolicy")))
    handlers_admin._clear_service_settings_pending.assert_awaited()


def test_qos_status_command_clears_both_pending_and_uses_polished_wording(monkeypatch):
    message = _msg("/qos_status")
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[1, 150, 0]))
    monkeypatch.setattr(handlers_admin, "policy_metrics", AsyncMock(return_value={"qos_last_sync_ok": 1, "qos_errors": 0}))
    asyncio.run(handlers_admin.qos_status_cmd(message))
    handlers_admin._clear_service_settings_pending.assert_awaited_once()
    handlers_admin._clear_network_policy_pending.assert_awaited_once()
    rendered = message.answer.await_args.args[0]
    assert "Скорость по умолчанию: 150 Mbit/s" in rendered
    assert "Включено: включено" in rendered
    assert "Последняя синхронизация QoS: 1" in rendered


def test_health_text_uses_readable_russian_labels(monkeypatch):
    monkeypatch.setattr(
        handlers_admin,
        "get_pending_jobs_stats",
        AsyncMock(return_value={"received": 4, "provisioning": 2, "needs_repair": 1, "stuck_manual": 0}),
    )
    monkeypatch.setattr(handlers_admin, "get_recovery_lag_seconds", AsyncMock(return_value=15))
    monkeypatch.setattr(handlers_admin, "get_metric", AsyncMock(side_effect=[3, 9, 5, 4, 7]))
    monkeypatch.setattr(
        handlers_admin,
        "policy_metrics",
        AsyncMock(
            return_value={
                "qos_errors": 1,
                "denylist_errors": 2,
                "qos_last_sync_ok": 1,
                "denylist_last_sync_ok": 0,
                "denylist_last_sync_ts": 123,
                "denylist_entries": 6,
            }
        ),
    )
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[1, "soft", 1, 0]))
    text = asyncio.run(handlers_admin.build_health_text())
    assert "Получено задач: <b>4</b>" in text
    assert "Застряли на ручной обработке: <b>0</b>" in text
    assert "Ошибки QoS: <b>1</b>" in text
    assert "Время последней синхронизации denylist: <b>123</b>" in text
    assert "Ограничение запросов: отклонено кнопок: <b>4</b>" in text


def test_denylist_sync_command_uses_polished_wording(monkeypatch):
    message = _msg("/denylist_sync")
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "denylist_sync", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.denylist_sync_cmd(message))
    rendered = message.answer.await_args.args[0]
    assert rendered == "✅ Синхронизация denylist выполнена."


def test_payment_charge_prompt_and_empty_value_are_polished(monkeypatch):
    cb = _cb(handlers_admin.CB_ADMIN_FIND_CHARGE)
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "set_pending_admin_action", AsyncMock())
    asyncio.run(handlers_admin.admin_payments_find_charge_start(cb))
    assert cb.message.answer.await_args.args[0] == "Введите Charge ID платежа"

    message = _msg("   ")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[{"a": 1}, None]))
    asyncio.run(handlers_admin.admin_payment_lookup_capture_input(message))
    assert message.answer.await_args.args[0] == "Charge ID не указан."


def test_denylist_domains_and_cidrs_input(monkeypatch):
    assert handlers_admin._normalize_domains_multiline(" ExAmple.com\nexample.com \n\nTest.ORG ") == "example.com,test.org"
    assert handlers_admin._normalize_cidrs_multiline("10.0.0.1/24\n10.0.0.0/24") == "10.0.0.0/24"

    message = _msg("bad-cidr")
    monkeypatch.setattr(handlers_admin, "get_pending_admin_action", AsyncMock(side_effect=[None, None, None, {"a": 1}]))
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
    monkeypatch.setattr(handlers_admin, "denylist_sync", AsyncMock())
    asyncio.run(handlers_admin.admin_network_policy_capture_input(message))
    handlers_admin.set_app_setting.assert_not_awaited()


def test_denylist_sync_action(monkeypatch):
    cb = _cb(handlers_admin.CB_ADMIN_DENYLIST_SYNC)
    monkeypatch.setattr(handlers_admin, "denylist_sync", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_denylist_sync_now(cb))
    handlers_admin.denylist_sync.assert_awaited_once()


def test_qos_rate_fallback_default_is_150(monkeypatch):
    monkeypatch.setattr(network_policy, "get_setting", AsyncMock(return_value=None))
    value = asyncio.run(network_policy.qos_rate_for_key(None))
    assert value == 150


def test_text_override_screen_validates_and_resets(monkeypatch):
    message = _msg("🆘 <b>Поддержка</b>\n\nПо всем вопросам пишите: <b>{support_username}</b>")
    monkeypatch.setattr(
        handlers_admin,
        "get_pending_admin_action",
        AsyncMock(return_value={"key": "support_contact"}),
    )
    monkeypatch.setattr(handlers_admin, "set_text_override", AsyncMock())
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_text_override_capture_input(message))
    handlers_admin.set_text_override.assert_awaited_once()

    cb = _cb(f"{handlers_admin.CB_ADMIN_TEXT_RESET_PREFIX}support_contact")
    monkeypatch.setattr(handlers_admin, "reset_text_override", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_text_override_reset(cb))
    handlers_admin.reset_text_override.assert_awaited_once_with("support_contact")


def test_top_level_sections_clear_service_and_text_pending(monkeypatch):
    cb = _cb(handlers_admin.CB_ADMIN_PAYMENTS)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "clear_pending_admin_action", AsyncMock())
    asyncio.run(handlers_admin.admin_payments_screen(cb))
    handlers_admin._clear_service_settings_pending.assert_awaited_once()

    cb_text = _cb(handlers_admin.CB_ADMIN_TEXT_OVERRIDES)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "list_text_overrides", AsyncMock(return_value=[]))
    asyncio.run(handlers_admin.admin_text_overrides_screen(cb_text))
    handlers_admin._clear_service_settings_pending.assert_awaited_once()


def test_starting_service_and_text_inputs_clears_cross_pending(monkeypatch):
    cb_support = _cb(handlers_admin.CB_ADMIN_SERVICE_SUPPORT)
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "set_pending_admin_action", AsyncMock())
    asyncio.run(handlers_admin.admin_service_support_start(cb_support))
    handlers_admin._clear_service_settings_pending.assert_awaited_once()

    cb_text = _cb(f"{handlers_admin.CB_ADMIN_TEXT_SET_PREFIX}start")
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "set_pending_admin_action", AsyncMock())
    asyncio.run(handlers_admin.admin_text_override_set_start(cb_text))
    handlers_admin._clear_service_settings_pending.assert_awaited_once()


def test_support_username_validation():
    assert handlers_admin._normalize_support_username("@good_name_1") == "@good_name_1"
    assert handlers_admin._normalize_support_username("goodname") == "@goodname"
    assert handlers_admin._normalize_support_username("bad name") == ""
    assert handlers_admin._normalize_support_username("@bad-name") == ""


def test_required_admin_slash_commands_clear_network_policy_pending(monkeypatch):
    handlers = [
        (handlers_admin.list_users_cmd, (_msg("/users"),)),
        (handlers_admin.find_user_cmd, (_msg("/finduser"), CommandObject(prefix="/", command="finduser", mention=None, args=""))),
        (handlers_admin.payinfo_cmd, (_msg("/payinfo"), CommandObject(prefix="/", command="payinfo", mention=None, args=""))),
        (handlers_admin.findpay_cmd, (_msg("/findpay"), CommandObject(prefix="/", command="findpay", mention=None, args=""))),
        (handlers_admin.audit_cmd, (_msg("/audit"), CommandObject(prefix="/", command="audit", mention=None, args=""))),
        (handlers_admin.maintenance_status_cmd, (_msg("/maintenance_status"),)),
        (handlers_admin.maintenance_on_cmd, (_msg("/maintenance_on"),)),
        (handlers_admin.maintenance_off_cmd, (_msg("/maintenance_off"),)),
        (handlers_admin.qos_status_cmd, (_msg("/qos_status"),)),
        (handlers_admin.denylist_status_cmd, (_msg("/denylist_status"),)),
        (handlers_admin.denylist_sync_cmd, (_msg("/denylist_sync"),)),
    ]

    for fn, args in handlers:
        monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
        monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
        monkeypatch.setattr(handlers_admin, "fetchall", AsyncMock(return_value=[]))
        monkeypatch.setattr(handlers_admin, "get_recent_audit", AsyncMock(return_value=[]))
        monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(return_value=0))
        monkeypatch.setattr(handlers_admin, "policy_metrics", AsyncMock(return_value={
            "qos_last_sync_ok": 1, "qos_errors": 0, "denylist_last_sync_ok": 1,
            "denylist_last_sync_ts": 0, "denylist_entries": 0, "denylist_errors": 0,
        }))
        monkeypatch.setattr(handlers_admin, "denylist_sync", AsyncMock())
        monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
        monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
        asyncio.run(fn(*args))
        handlers_admin._clear_network_policy_pending.assert_awaited_once()
        handlers_admin._clear_service_settings_pending.assert_awaited_once()


def test_callback_answers_and_maintenance_messages_are_polished(monkeypatch):
    cb_sync = _cb(handlers_admin.CB_ADMIN_QOS_SYNC)
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_qos_sync_now(cb_sync))
    cb_sync.answer.assert_awaited_once_with("Готово")

    cb_on = _cb(handlers_admin.CB_ADMIN_MAINTENANCE_ON)
    monkeypatch.setattr(handlers_admin, "set_app_setting", AsyncMock())
    monkeypatch.setattr(handlers_admin, "write_audit_log", AsyncMock())
    asyncio.run(handlers_admin.admin_maintenance_on_cb(cb_on))
    assert "Техработы включены" in cb_on.message.answer.await_args.args[0]


def test_remaining_top_level_slash_commands_clear_network_policy_pending(monkeypatch):
    commands = [
        (handlers_admin.give_manual, (_msg("/give"), CommandObject(prefix="/", command="give", mention=None, args=""))),
        (handlers_admin.promo_create_cmd, (_msg("/promo_create"), CommandObject(prefix="/", command="promo_create", mention=None, args=""))),
        (handlers_admin.promo_list_cmd, (_msg("/promo_list"), CommandObject(prefix="/", command="promo_list", mention=None, args=""))),
        (handlers_admin.promo_disable_cmd, (_msg("/promo_disable"), CommandObject(prefix="/", command="promo_disable", mention=None, args=""))),
        (handlers_admin.revoke_user_cmd, (_msg("/revoke"), CommandObject(prefix="/", command="revoke", mention=None, args=""))),
        (handlers_admin.broadcast_prepare, (_msg("/send"), CommandObject(prefix="/", command="send", mention=None, args=""))),
    ]

    for fn, args in commands:
        monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
        monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
        monkeypatch.setattr(handlers_admin, "admin_command_limited", lambda *_: False)
        monkeypatch.setattr(handlers_admin, "clear_pending_broadcast", AsyncMock())
        monkeypatch.setattr(handlers_admin, "set_pending_admin_action", AsyncMock())
        monkeypatch.setattr(handlers_admin, "list_promo_codes", AsyncMock(return_value=[]))
        asyncio.run(fn(*args))
        handlers_admin._clear_network_policy_pending.assert_awaited_once()
        handlers_admin._clear_service_settings_pending.assert_awaited_once()


def test_admin_maintenance_screen_uses_tehwork_wording(monkeypatch):
    cb = _cb(handlers_admin.CB_ADMIN_MAINTENANCE)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(return_value=1))
    asyncio.run(handlers_admin.admin_maintenance_screen(cb))
    assert "Техработы: ВКЛ" in cb.message.answer.await_args.args[0]
