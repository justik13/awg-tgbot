import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiogram.filters import CommandObject

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import handlers_admin
import network_policy
from keyboards import get_admin_denylist_kb, get_admin_inline_kb


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
    assert "🌐 Сеть" in texts


def test_network_policy_keyboards_have_explicit_labels():
    deny_kb = get_admin_denylist_kb(denylist_enabled=0, denylist_mode="soft")
    deny_texts = [button.text for row in deny_kb.inline_keyboard for button in row]
    assert "Denylist: ВЫКЛ" in deny_texts
    assert "Режим denylist: soft" in deny_texts


def test_policy_metrics_keeps_denylist_fields(monkeypatch):
    monkeypatch.setattr(network_policy, "get_metric", AsyncMock(side_effect=[4, 1, 2, 7]))
    metrics = asyncio.run(network_policy.policy_metrics())
    assert metrics["denylist_last_sync_ts"] == 2


def test_network_policy_screen_renders_state(monkeypatch):
    monkeypatch.setattr(handlers_admin, "policy_metrics", AsyncMock(return_value={
        "denylist_errors": 2,
        "denylist_last_sync_ok": 1,
        "denylist_last_sync_ts": 123,
        "denylist_entries": 5,
    }))
    monkeypatch.setattr(handlers_admin, "DOCKER_CONTAINER", "amnezia-awg2")
    monkeypatch.setattr(handlers_admin, "WG_INTERFACE", "awg0")
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[1, "soft", 30]))
    text = asyncio.run(handlers_admin._render_network_policy_text())
    assert "Режим denylist: <b>soft</b>" in text
    assert "AWG: <b>amnezia-awg2 / awg0</b>" in text


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


def test_open_user_card_jump_reuses_existing_flow(monkeypatch):
    cb = _cb("a:uc:55_0")
    monkeypatch.setattr(handlers_admin, "_send_user_manage_card", AsyncMock())
    asyncio.run(handlers_admin.admin_open_user_card_from_payment(cb))
    handlers_admin._send_user_manage_card.assert_awaited_once()
