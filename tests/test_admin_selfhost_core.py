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
    assert "🟠 Maintenance" in texts
    assert "🩺 Состояние" in texts
    assert "🎟 Промокоды" in texts
    assert "🌐 Сеть" in texts


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
    assert "QoS enabled: <b>ON</b>" in text
    assert "Default rate: <b>150 Mbit/s</b>" in text
    assert "Denylist mode: <b>soft</b>" in text


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
    asyncio.run(handlers_admin.admin_manual_commands(cb))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()


def test_stats_and_sync_callbacks_clear_network_policy_pending(monkeypatch):
    cb_stats = _cb(handlers_admin.CB_ADMIN_STATS)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "build_stats_text", AsyncMock(return_value="ok"))
    asyncio.run(handlers_admin.admin_stats_cb(cb_stats))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()

    cb_sync = _cb(handlers_admin.CB_ADMIN_SYNC)
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "sync_qos_state", AsyncMock())
    monkeypatch.setattr(handlers_admin, "denylist_sync", AsyncMock())
    monkeypatch.setattr(handlers_admin, "build_awg_sync_text", AsyncMock(return_value="sync"))
    asyncio.run(handlers_admin.admin_sync_awg(cb_sync))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()


def test_stats_slash_command_clears_network_policy_pending(monkeypatch):
    message = _msg("/stats")
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "build_stats_text", AsyncMock(return_value="stats"))
    asyncio.run(handlers_admin.stats_cmd(message))
    handlers_admin._clear_network_policy_pending.assert_awaited_once()


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
