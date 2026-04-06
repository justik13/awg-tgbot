import asyncio
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import app
import handlers_admin
import handlers_user
from helpers import get_status_text
from device_activity import format_handshake_timestamp
from aiogram.filters import CommandObject


def test_get_status_text_renders_moscow_time():
    status, until_text = get_status_text("2999-01-01T00:00:00")

    assert status == "🟢 Активен"
    assert until_text == "01.01.2999 03:00"


def test_notify_expiring_subscriptions_uses_moscow_format(monkeypatch):
    fake_bot = SimpleNamespace(send_message=AsyncMock())
    monkeypatch.setattr(app, "get_subscriptions_expiring_within", AsyncMock(return_value=[(42, "2026-01-01T00:00:00")]))
    monkeypatch.setattr(app, "has_subscription_notification", AsyncMock(return_value=False))
    monkeypatch.setattr(app, "mark_subscription_notification_sent", AsyncMock())

    asyncio.run(app._notify_expiring_subscriptions(fake_bot))

    sent_text = fake_bot.send_message.await_args_list[0].args[1]
    assert "Окончание: 01.01.2026 03:00" in sent_text
    assert "T00:00" not in sent_text


def test_admin_sync_and_payment_created_at_rendered_in_moscow():
    assert handlers_admin._sync_time_or_ne_bilo(0) == "не было"
    assert handlers_admin._sync_time_or_ne_bilo(1) == "01.01.1970 03:00"

    text = handlers_admin._render_payment_lookup_text(
        {
            "user_id": 7,
            "payment_id": "ch_1",
            "status": "paid",
            "amount": 100,
            "currency": "XTR",
            "payload": "sub_30",
            "created_at": "2026-01-01T00:00:00",
            "last_provision_status": "done",
        }
    )
    assert "🕒 Создан: <code>01.01.2026 03:00</code>" in text


def test_user_promo_success_message_uses_moscow(monkeypatch):
    message = SimpleNamespace(
        from_user=SimpleNamespace(id=42),
        answer=AsyncMock(),
    )
    monkeypatch.setattr(handlers_user, "activate_promo_code", AsyncMock(return_value={"status": "ok", "bonus_days": 7}))
    monkeypatch.setattr(handlers_user, "issue_subscription", AsyncMock(return_value=datetime(2026, 1, 1, 0, 0)))
    monkeypatch.setattr(handlers_user, "write_audit_log", AsyncMock())

    asyncio.run(handlers_user._apply_promo_code(message, "SPRING"))

    sent_text = message.answer.await_args_list[-1].args[0]
    assert "📅 Доступ до: <b>01.01.2026 03:00</b>" in sent_text


def test_support_top_level_cleans_pending_invoice(monkeypatch):
    message = SimpleNamespace(
        from_user=SimpleNamespace(id=42),
        bot=SimpleNamespace(),
    )
    monkeypatch.setattr(handlers_user, "_clear_promo_input_pending", AsyncMock())
    monkeypatch.setattr(handlers_user, "_cleanup_pending_invoice_for_navigation", AsyncMock())
    monkeypatch.setattr(handlers_user, "_send_support_center", AsyncMock())

    asyncio.run(handlers_user.support(message))

    handlers_user._cleanup_pending_invoice_for_navigation.assert_awaited_once_with(message.bot, 42)


def test_device_activity_handshake_timestamp_uses_moscow():
    assert format_handshake_timestamp(datetime(2026, 1, 1, 0, 0)) == "01.01 03:00"


def test_admin_audit_command_renders_created_at_in_moscow(monkeypatch):
    message = SimpleNamespace(
        from_user=SimpleNamespace(id=handlers_admin.ADMIN_ID),
        answer=AsyncMock(),
    )
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(
        handlers_admin,
        "get_recent_audit",
        AsyncMock(return_value=[(1, 42, "evt", "details", "2026-01-01T00:00:00")]),
    )

    asyncio.run(
        handlers_admin.audit_cmd(
            message,
            CommandObject(prefix="/", command="audit", mention=None, args=None),
        )
    )

    text = message.answer.await_args.args[0]
    assert "01.01.2026 03:00" in text
