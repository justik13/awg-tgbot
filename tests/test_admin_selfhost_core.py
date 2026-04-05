import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

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


def test_network_policy_screen_disabled_denylist_shows_history_and_no_raw_metrics(monkeypatch):
    monkeypatch.setattr(handlers_admin, "policy_metrics", AsyncMock(return_value={
        "denylist_errors": 3,
        "denylist_last_sync_ok": 1,
        "denylist_last_sync_ts": 1710000000,
        "denylist_entries": 44,
    }))
    monkeypatch.setattr(handlers_admin, "DOCKER_CONTAINER", "amnezia-awg2")
    monkeypatch.setattr(handlers_admin, "WG_INTERFACE", "awg0")
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[0, "soft", 30]))

    text = asyncio.run(handlers_admin._render_network_policy_text())

    assert "Denylist: <b>выключено</b>" in text
    assert "синхронизация сейчас не выполняется" in text
    assert "Текущих записей denylist: <b>0</b>" in text
    assert "Последний успешный sync denylist (история): <b>09.03.2024 16:00</b>" in text
    assert "1710000000" not in text
    assert "denylist_last_sync_ok" not in text


def test_health_text_denylist_uses_human_labels(monkeypatch):
    monkeypatch.setattr(handlers_admin, "get_pending_jobs_stats", AsyncMock(return_value={
        "received": 1,
        "provisioning": 0,
        "needs_repair": 0,
        "stuck_manual": 0,
    }))
    monkeypatch.setattr(handlers_admin, "get_recovery_lag_seconds", AsyncMock(return_value=0))
    monkeypatch.setattr(
        handlers_admin,
        "get_metric",
        AsyncMock(side_effect=[0, 2, 5, 6, 7]),
    )
    monkeypatch.setattr(handlers_admin, "policy_metrics", AsyncMock(return_value={
        "denylist_errors": 0,
        "denylist_last_sync_ok": 1,
        "denylist_last_sync_ts": 0,
        "denylist_entries": 12,
    }))
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[1, "soft"]))

    text = asyncio.run(handlers_admin.build_health_text())

    assert "Denylist: <b>включено</b> · режим: <b>soft</b>" in text
    assert "Последний sync denylist: <b>не было</b>" in text
    assert "Последняя синхронизация denylist: <b>1</b>" not in text


def test_denylist_status_cmd_no_raw_sync_values(monkeypatch):
    message = _msg("/denylist_status")
    monkeypatch.setattr(handlers_admin, "_clear_service_settings_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "_clear_network_policy_pending", AsyncMock())
    monkeypatch.setattr(handlers_admin, "get_setting", AsyncMock(side_effect=[1, "strict", 15]))
    monkeypatch.setattr(handlers_admin, "policy_metrics", AsyncMock(return_value={
        "denylist_errors": 2,
        "denylist_last_sync_ok": 0,
        "denylist_last_sync_ts": 1710000000,
        "denylist_entries": 8,
    }))

    asyncio.run(handlers_admin.denylist_status_cmd(message))
    sent = message.answer.await_args.args[0]

    assert "Последний sync denylist: <b>ошибка</b>" in sent
    assert "Время последнего sync denylist: <b>09.03.2024 16:00</b>" in sent
    assert "Последняя синхронизация denylist: 0" not in sent
    assert "1710000000" not in sent


def test_denylist_sync_disabled_resets_entries_after_clear(monkeypatch):
    run_docker = AsyncMock()
    monkeypatch.setattr(network_policy, "get_setting", AsyncMock(return_value=0))
    monkeypatch.setattr(network_policy, "denylist_clear", AsyncMock())
    monkeypatch.setattr(network_policy, "set_metric", AsyncMock())
    monkeypatch.setattr(network_policy, "increment_metric", AsyncMock())

    asyncio.run(network_policy.denylist_sync(run_docker))

    network_policy.denylist_clear.assert_awaited_once_with(run_docker)
    network_policy.set_metric.assert_awaited_once_with("denylist_entries", 0)
    network_policy.increment_metric.assert_not_called()


def test_denylist_sync_disabled_counts_errors_on_clear_failure(monkeypatch):
    run_docker = AsyncMock()
    monkeypatch.setattr(network_policy, "get_setting", AsyncMock(return_value=0))
    monkeypatch.setattr(network_policy, "denylist_clear", AsyncMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(network_policy, "set_metric", AsyncMock())
    monkeypatch.setattr(network_policy, "increment_metric", AsyncMock())
    logger_mock = SimpleNamespace(warning=Mock())
    monkeypatch.setattr(network_policy, "logger", logger_mock)

    asyncio.run(network_policy.denylist_sync(run_docker))

    network_policy.increment_metric.assert_awaited_once_with("denylist_errors")
    network_policy.set_metric.assert_not_called()
    logger_mock.warning.assert_called_once()


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


def test_smokecheck_marks_invalid_helper_policy_json_as_failed(monkeypatch):
    monkeypatch.setattr(handlers_admin, "DOCKER_CONTAINER", "amnezia-awg")
    monkeypatch.setattr(handlers_admin, "WG_INTERFACE", "awg0")
    monkeypatch.setattr(handlers_admin, "AWG_HELPER_POLICY_PATH", "/etc/awg-bot-helper.json")
    monkeypatch.setattr(handlers_admin, "db_health_info", AsyncMock(return_value={"is_healthy": True}))
    monkeypatch.setattr(handlers_admin, "check_awg_container", AsyncMock(return_value=None))
    monkeypatch.setattr(
        handlers_admin,
        "read_helper_policy",
        lambda _path: ("", "", "helper policy parse failed: Expecting property name enclosed in double quotes: line 1 column 2 (char 1)"),
    )

    report = asyncio.run(handlers_admin.run_runtime_smokecheck())
    checks = {item["name"]: item for item in report["checks"]}

    assert report["overall"] == "failed"
    assert checks["Политика helper"]["state"] == "failed"
    assert "/etc/awg-bot-helper.json" in checks["Политика helper"]["detail"]
    assert "Expecting property name enclosed in double quotes" in checks["Политика helper"]["detail"]
    assert "исправь JSON в /etc/awg-bot-helper.json" in report["hint"]


def test_smokecheck_awg_error_from_helper_policy_sets_critical_hint(monkeypatch):
    monkeypatch.setattr(handlers_admin, "DOCKER_CONTAINER", "amnezia-awg")
    monkeypatch.setattr(handlers_admin, "WG_INTERFACE", "awg0")
    monkeypatch.setattr(handlers_admin, "AWG_HELPER_POLICY_PATH", "/etc/awg-bot-helper.json")
    monkeypatch.setattr(handlers_admin, "db_health_info", AsyncMock(return_value={"is_healthy": True}))
    monkeypatch.setattr(
        handlers_admin,
        "check_awg_container",
        AsyncMock(side_effect=RuntimeError("helper exec failed: invalid helper policy json: bad value")),
    )
    monkeypatch.setattr(handlers_admin, "read_helper_policy", lambda _path: ("amnezia-awg", "awg0", ""))

    report = asyncio.run(handlers_admin.run_runtime_smokecheck())

    assert report["overall"] == "failed"
    assert "исправь JSON в /etc/awg-bot-helper.json" in report["hint"]
    assert report["hint"] != "готово к работе"


def test_build_runtime_smokecheck_text_escapes_parser_error_html(monkeypatch):
    monkeypatch.setattr(handlers_admin, "DOCKER_CONTAINER", "amnezia-awg")
    monkeypatch.setattr(handlers_admin, "WG_INTERFACE", "awg0")
    monkeypatch.setattr(handlers_admin, "AWG_HELPER_POLICY_PATH", "/etc/awg-bot-helper.json")
    monkeypatch.setattr(handlers_admin, "db_health_info", AsyncMock(return_value={"is_healthy": True}))
    monkeypatch.setattr(handlers_admin, "check_awg_container", AsyncMock(return_value=None))
    monkeypatch.setattr(
        handlers_admin,
        "read_helper_policy",
        lambda _path: ("", "", "helper policy parse failed: bad <tag>& value"),
    )

    text = asyncio.run(handlers_admin.build_runtime_smokecheck_text())

    assert "bad &lt;tag&gt;&amp; value" in text
    assert "bad <tag>& value" not in text
