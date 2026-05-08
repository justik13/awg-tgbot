"""
Тест атомарности active_configs и проверки capacity.

Проверяет:
1. Атомарное увеличение active_configs при создании устройства
2. Отказ при превышении capacity (race condition protection)
3. Корректную обработку ошибок при заполненной ноде
"""

import asyncio
import pytest
import aiosqlite
from pathlib import Path
from datetime import datetime, timezone


# Импортируем функции из проекта
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "bot"))

from database import create_device_with_capacity_check, utc_now_naive


pytest_plugins = ('pytest_asyncio',)


@pytest.fixture
async def test_db():
    """Создаёт тестовую БД в памяти."""
    db_path = ":memory:"
    
    db = await aiosqlite.connect(db_path)
    await db.execute("PRAGMA journal_mode=WAL")
    
    # Создаём таблицу nodes
    await db.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            ip TEXT NOT NULL,
            port INTEGER NOT NULL,
            s1 TEXT, s2 TEXT, s3 TEXT, s4 TEXT,
            h1 TEXT, h2 TEXT, h3 TEXT, h4 TEXT,
            country TEXT, flag_emoji TEXT,
            is_visible INTEGER DEFAULT 1,
            capacity INTEGER DEFAULT 50,
            active_configs INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            api_token TEXT UNIQUE,
            api_token_hash TEXT,
            last_seen TEXT,
            params_hash TEXT,
            denylist_version TEXT DEFAULT 'v0',
            created_at TEXT DEFAULT (datetime('now'))
        )
    """)
    
    # Создаём таблицу devices
    await db.execute("""
        CREATE TABLE IF NOT EXISTS devices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            slot_number INTEGER NOT NULL,
            node_id INTEGER NOT NULL,
            public_key TEXT NOT NULL,
            private_key_enc TEXT NOT NULL,
            psk_enc TEXT NOT NULL,
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT (datetime('now')),
            last_reissued_at TEXT,
            UNIQUE(user_id, slot_number),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(node_id) REFERENCES nodes(id)
        )
    """)
    
    await db.commit()
    
    yield db
    
    await db.close()


@pytest.fixture
def mock_utc_now(monkeypatch):
    """Мокает utc_now_naive для консистентных тестов."""
    fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("database.utc_now_naive", lambda: fixed_time.replace(tzinfo=None))


async def insert_test_node(db, capacity: int = 3, active_configs: int = 0):
    """Вставляет тестовую ноду."""
    now_iso = datetime.now(timezone.utc).isoformat()
    await db.execute("""
        INSERT INTO nodes (
            name, ip, port, status, capacity, active_configs,
            api_token, api_token_hash, last_seen, created_at
        ) VALUES (?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?)
    """, (
        "test_node", "192.168.1.1", 51820,
        capacity, active_configs,
        "test-token-uuid", "test-token-hash", now_iso, now_iso
    ))
    await db.commit()
    
    async with db.execute("SELECT last_insert_rowid()") as cursor:
        row = await cursor.fetchone()
        return row[0]


class TestDeviceCreationCapacityCheck:
    """Тесты проверки capacity при создании устройств."""
    
    @pytest.mark.asyncio
    async def test_create_device_success(self, test_db, mock_utc_now, monkeypatch):
        """Успешное создание устройства когда есть место."""
        node_id = await insert_test_node(test_db, capacity=3, active_configs=0)
        
        # Мокаем open_db чтобы возвращала наш тестовый connection
        monkeypatch.setattr("database.open_db", lambda: test_db.__aenter__())
        
        device_id, error = await create_device_with_capacity_check(
            user_id=123,
            slot_number=1,
            node_id=node_id,
            public_key="testpubkey123456789012345678901234567890123=",
            private_key_enc="enc:private_key",
            psk_enc="enc:psk_key",
        )
        
        assert error is None
        assert device_id is not None
        assert device_id > 0
        
        # Проверяем что active_configs увеличился
        async with test_db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1
    
    @pytest.mark.asyncio
    async def test_create_device_capacity_exceeded(self, test_db, mock_utc_now, monkeypatch):
        """Отказ при превышении capacity."""
        # Создаём ноду с capacity=2 и уже заполненными active_configs=2
        node_id = await insert_test_node(test_db, capacity=2, active_configs=2)
        
        monkeypatch.setattr("database.open_db", lambda: test_db.__aenter__())
        
        device_id, error = await create_device_with_capacity_check(
            user_id=123,
            slot_number=1,
            node_id=node_id,
            public_key="testpubkey123456789012345678901234567890123=",
            private_key_enc="enc:private_key",
            psk_enc="enc:psk_key",
        )
        
        assert error is not None
        assert "Лимит сервера достигнут" in error
        assert device_id is None
    
    @pytest.mark.asyncio
    async def test_create_device_race_condition_protection(self, test_db, mock_utc_now, monkeypatch):
        """Защита от race condition при одновременном создании."""
        node_id = await insert_test_node(test_db, capacity=2, active_configs=1)
        
        monkeypatch.setattr("database.open_db", lambda: test_db.__aenter__())
        
        # Создаём два устройства почти одновременно
        async def create_device_task(user_id: int):
            return await create_device_with_capacity_check(
                user_id=user_id,
                slot_number=1,
                node_id=node_id,
                public_key=f"testpubkey{user_id}{'=' * 40}",
                private_key_enc="enc:private_key",
                psk_enc="enc:psk_key",
            )
        
        # Запускаем параллельно
        results = await asyncio.gather(
            create_device_task(101),
            create_device_task(102),
            create_device_task(103),  # Третий должен получить отказ
        )
        
        # Считаем успешные создания
        successful = [r for r in results if r[1] is None]
        failed = [r for r in results if r[1] is not None]
        
        # Должно быть максимум 2 успешных (capacity=2, было 1 занято)
        assert len(successful) <= 1  # Только одно свободное место осталось
        assert len(failed) >= 2  # Остальные получили отказ
        
        # Проверяем final state
        async with test_db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 2  # capacity не превышен
    
    @pytest.mark.asyncio
    async def test_create_device_node_not_found(self, test_db, mock_utc_now, monkeypatch):
        """Обработка несуществующей ноды."""
        monkeypatch.setattr("database.open_db", lambda: test_db.__aenter__())
        
        device_id, error = await create_device_with_capacity_check(
            user_id=123,
            slot_number=1,
            node_id=99999,  # Несуществующий ID
            public_key="testpubkey123456789012345678901234567890123=",
            private_key_enc="enc:private_key",
            psk_enc="enc:psk_key",
        )
        
        assert error is not None
        assert "Нода не найдена" in error
        assert device_id is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
