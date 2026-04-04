import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram.filters import CommandObject

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import handlers_admin
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
