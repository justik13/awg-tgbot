import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from apscheduler.schedulers import SchedulerNotRunningError

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import app


class _FakeScheduler:
    def __init__(self, *_, **__):
        self.state = 0
        self.shutdown_called = False

    def add_job(self, *_, **__):
        return None

    def start(self):
        self.state = 1

    def shutdown(self, *_, **__):
        self.shutdown_called = True
        raise SchedulerNotRunningError()


class _FakeWorkerPool:
    def start(self, *_args, **_kwargs):
        return None

    async def stop(self):
        return None


class _FakeBot:
    def __init__(self, *_, **__):
        self.session = SimpleNamespace(close=AsyncMock())


def test_main_preserves_startup_error_without_scheduler_secondary_error(monkeypatch):
    fake_scheduler = _FakeScheduler()
    monkeypatch.setattr(app, "Bot", _FakeBot)
    monkeypatch.setattr(app, "AsyncIOScheduler", lambda *args, **kwargs: fake_scheduler)
    monkeypatch.setattr(app, "WorkerPool", _FakeWorkerPool)
    monkeypatch.setattr(app, "_startup_checks", AsyncMock(side_effect=RuntimeError("invalid helper policy json")))
    monkeypatch.setattr(app, "close_shared_db", AsyncMock())

    try:
        asyncio.run(app.main())
    except RuntimeError as e:
        assert str(e) == "invalid helper policy json"
    else:
        raise AssertionError("expected RuntimeError")

    assert fake_scheduler.shutdown_called is False
