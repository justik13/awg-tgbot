import asyncio
import sqlite3
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1] / "bot"))

import database


def _prepare_min_schema(path: Path) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute("CREATE TABLE users (user_id INTEGER PRIMARY KEY, sub_until TEXT NOT NULL DEFAULT '0', created_at TEXT NOT NULL)")
        con.execute(
            """
            CREATE TABLE keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                device_num INTEGER NOT NULL,
                public_key TEXT NOT NULL,
                config TEXT NOT NULL,
                ip TEXT NOT NULL,
                client_private_key TEXT,
                psk_key TEXT
            )
            """
        )
        con.execute("CREATE TABLE payments (telegram_payment_charge_id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, payload TEXT NOT NULL, amount INTEGER NOT NULL, created_at TEXT NOT NULL)")
        con.execute("CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, action TEXT NOT NULL, details TEXT, created_at TEXT NOT NULL)")
        con.commit()
    finally:
        con.close()


def test_db_health_marks_existing_instance_empty_db_as_critical(monkeypatch, tmp_path):
    db_path = tmp_path / "runtime.db"
    _prepare_min_schema(db_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute("INSERT INTO payments (telegram_payment_charge_id, user_id, payload, amount, created_at) VALUES ('pay-1', 1, 'x', 10, '2026-01-01T00:00:00')")
        con.commit()
    finally:
        con.close()

    monkeypatch.setattr(database, "DB_PATH", str(db_path))

    info = asyncio.run(database.db_health_info())

    assert info["schema_ready"] is True
    assert info["instance_integrity"]["state"] == "critical"
    assert info["runtime_ready"] is False
    assert info["is_healthy"] is False


def test_db_health_marks_encryption_mismatch_as_critical(monkeypatch, tmp_path):
    db_path = tmp_path / "runtime.db"
    _prepare_min_schema(db_path)
    con = sqlite3.connect(db_path)
    try:
        con.execute(
            "INSERT INTO keys (user_id, device_num, public_key, config, ip, client_private_key, psk_key) VALUES (1, 1, 'pk1', 'cfg', '10.0.0.2', 'enc:v2:bad:token', 'enc:v2:bad:token')"
        )
        con.execute("INSERT INTO users (user_id, sub_until, created_at) VALUES (1, '0', '2026-01-01T00:00:00')")
        con.commit()
    finally:
        con.close()

    monkeypatch.setattr(database, "DB_PATH", str(db_path))
    monkeypatch.setattr(database, "decrypt_text", lambda _value: (_ for _ in ()).throw(RuntimeError("bad secret")))

    info = asyncio.run(database.db_health_info())

    assert info["instance_integrity"]["state"] == "critical"
    assert "ENCRYPTION_SECRET" in info["instance_integrity"]["issues"][0]
    assert info["runtime_ready"] is False
