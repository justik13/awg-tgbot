import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import payments


def _expire_checkout_cache() -> None:
    payments._checkout_readiness_cache["expires_at"] = 0
    payments._checkout_readiness_cache["ok"] = True
    payments._checkout_readiness_cache["reason"] = ""


def test_checkout_readiness_handles_three_value_helper_policy_contract(monkeypatch):
    _expire_checkout_cache()
    monkeypatch.setattr(payments, "DOCKER_CONTAINER", "amnezia-awg")
    monkeypatch.setattr(payments, "WG_INTERFACE", "awg0")
    monkeypatch.setattr(payments, "read_helper_policy", lambda _path: ("", "", "helper policy parse failed: bad json"))
    monkeypatch.setattr(payments, "db_health_info", AsyncMock(return_value={"is_healthy": True}))
    monkeypatch.setattr(payments, "check_awg_container", AsyncMock(return_value=None))

    ok, reason = asyncio.run(payments.checkout_readiness())

    assert ok is False
    assert "helper policy parse failed: bad json" in reason


def test_checkout_readiness_fails_on_policy_mismatch(monkeypatch):
    _expire_checkout_cache()
    monkeypatch.setattr(payments, "DOCKER_CONTAINER", "amnezia-awg")
    monkeypatch.setattr(payments, "WG_INTERFACE", "awg0")
    monkeypatch.setattr(payments, "read_helper_policy", lambda _path: ("other", "wg1", ""))
    monkeypatch.setattr(payments, "db_health_info", AsyncMock(return_value={"is_healthy": True}))
    monkeypatch.setattr(payments, "check_awg_container", AsyncMock(return_value=None))

    ok, reason = asyncio.run(payments.checkout_readiness())

    assert ok is False
    assert reason == "helper policy mismatch"
