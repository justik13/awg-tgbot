import asyncio
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import app
import database


def _init_test_db(monkeypatch, tmp_path):
    asyncio.run(database.close_shared_db())
    db_path = tmp_path / "broadcast.db"
    monkeypatch.setattr(database, "DB_PATH", str(db_path))
    asyncio.run(database.ensure_db_ready())
    return db_path


def _runtime_deps(bot):
    return app.RuntimeDeps(
        bot=bot,
        settings=app.RuntimeSettings(
            cleanup_interval_seconds=300,
            reconciliation_interval_seconds=45,
            broadcast_batch_delay_seconds=0,
            broadcast_batch_size=20,
            broadcast_running_stale_seconds=60,
        ),
    )


def test_broadcast_exception_after_claim_does_not_stay_running(monkeypatch, tmp_path):
    _init_test_db(monkeypatch, tmp_path)
    asyncio.run(database.execute("INSERT OR REPLACE INTO users (user_id, created_at) VALUES (?, ?)", (101, "2026-01-01T00:00:00")))
    job_id = asyncio.run(database.create_broadcast_job(1, "hello"))

    monkeypatch.setattr(app, "get_broadcast_recipients", AsyncMock(side_effect=RuntimeError("boom in recipients")))
    bot = SimpleNamespace(send_message=AsyncMock())

    processed = asyncio.run(app.process_one_broadcast_job(_runtime_deps(bot)))

    row = asyncio.run(
        database.fetchone(
            "SELECT status, last_error FROM broadcast_jobs WHERE id = ?",
            (job_id,),
        )
    )
    assert processed is True
    assert row[0] == "failed"


def test_broadcast_failed_job_stores_last_error(monkeypatch, tmp_path):
    _init_test_db(monkeypatch, tmp_path)
    asyncio.run(database.execute("INSERT OR REPLACE INTO users (user_id, created_at) VALUES (?, ?)", (101, "2026-01-01T00:00:00")))
    job_id = asyncio.run(database.create_broadcast_job(1, "hello"))

    monkeypatch.setattr(app, "get_broadcast_recipients", AsyncMock(side_effect=ValueError("planned failure")))
    bot = SimpleNamespace(send_message=AsyncMock())

    asyncio.run(app.process_one_broadcast_job(_runtime_deps(bot)))

    row = asyncio.run(
        database.fetchone(
            "SELECT status, last_error FROM broadcast_jobs WHERE id = ?",
            (job_id,),
        )
    )
    assert row[0] == "failed"
    assert "ValueError: planned failure" in row[1]


def test_stale_running_broadcast_job_is_marked_failed(monkeypatch, tmp_path):
    _init_test_db(monkeypatch, tmp_path)
    asyncio.run(
        database.execute(
            """
            INSERT INTO broadcast_jobs (
                admin_id, text, status, total_count, delivered_count, failed_count, offset_cursor,
                created_at, started_at, updated_at
            ) VALUES (?, ?, 'running', 10, 4, 1, 5, ?, ?, ?)
            """,
            (1, "hello", "2026-01-01T00:00:00", "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
    )
    job_id = asyncio.run(database.fetchval("SELECT MAX(id) FROM broadcast_jobs"))
    asyncio.run(
        database.execute(
            "INSERT INTO broadcast_job_targets (job_id, user_id, created_at) VALUES (?, 101, '2026-01-01T00:00:00')",
            (job_id,),
        )
    )

    recovered = asyncio.run(database.fail_stale_running_broadcast_jobs(60))

    row = asyncio.run(database.fetchone("SELECT status, last_error FROM broadcast_jobs WHERE id = ?", (job_id,)))
    targets_left = asyncio.run(database.fetchval("SELECT COUNT(*) FROM broadcast_job_targets WHERE job_id = ?", (job_id,)))
    assert recovered == 1
    assert row[0] == "failed"
    assert "stale_running_recovered_at=" in row[1]
    assert targets_left == 0


def test_broadcast_happy_path_still_finishes(monkeypatch, tmp_path):
    _init_test_db(monkeypatch, tmp_path)
    asyncio.run(database.execute("INSERT OR REPLACE INTO users (user_id, created_at) VALUES (?, ?)", (101, "2026-01-01T00:00:00")))
    asyncio.run(database.execute("INSERT OR REPLACE INTO users (user_id, created_at) VALUES (?, ?)", (102, "2026-01-01T00:00:00")))
    job_id = asyncio.run(database.create_broadcast_job(1, "hello"))

    bot = SimpleNamespace(send_message=AsyncMock())

    processed = asyncio.run(app.process_one_broadcast_job(_runtime_deps(bot)))

    row = asyncio.run(
        database.fetchone(
            "SELECT status, delivered_count, failed_count FROM broadcast_jobs WHERE id = ?",
            (job_id,),
        )
    )
    assert processed is True
    assert row[0] == "finished"
    assert row[1] == 2
    assert row[2] == 0
    assert bot.send_message.await_count == 3
