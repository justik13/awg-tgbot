"""
Тест атомарности active_configs и проверки capacity.

Проверяет:
1. Атомарное увеличение active_configs при создании устройства
2. Отказ при превышении capacity (race condition protection)
3. Корректную обработку ошибок при заполненной ноде

ВАЖНО: Использует временные файлы БД вместо :memory: для избежания
конфликтов потоков aiosqlite при быстром создании/закрытии соединений.
"""

import asyncio
import pytest
import os
from pathlib import Path
from datetime import datetime, timezone


# Импортируем функции из проекта после настройки окружения
import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "bot"))


pytest_plugins = ('pytest_asyncio',)


@pytest.fixture(autouse=True)
def setup_test_env(tmp_path, monkeypatch):
    """
    Настраивает изолированную среду для каждого теста.
    Использует временный файл БД вместо :memory:.
    """
    db_file = tmp_path / "test_awg.db"
    
    # Устанавливаем необходимые переменные окружения ДО импорта модулей
    monkeypatch.setenv("API_TOKEN", "test_token_12345")
    monkeypatch.setenv("ADMIN_ID", "123456789")
    monkeypatch.setenv("SERVER_PUBLIC_KEY", "testpubkey123456789012345678901234567890123=")
    monkeypatch.setenv("SERVER_IP", "8.8.8.8:51820")
    monkeypatch.setenv("ENCRYPTION_SECRET", "testsecret12345678901234567890")
    monkeypatch.setenv("BOT_TOKEN", "1234567890:AABBccDDeeFFggHHiiJJkkLLmmNNooppQQrrs")
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("CONFIG_AUTODETECT_ON_IMPORT", "0")
    
    # Импортируем и инициализируем БД после настройки окружения
    from database import init_db
    asyncio.run(init_db())
    
    yield db_file


@pytest.fixture
def mock_utc_now(monkeypatch):
    """Мокает utc_now_naive для консистентных тестов."""
    fixed_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("bot.database.utc_now_naive", lambda: fixed_time.replace(tzinfo=None))


async def get_open_db():
    """Вспомогательная функция для открытия соединения с БД."""
    from database import open_db
    return await open_db()


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
    async def test_create_device_success(self, setup_test_env, mock_utc_now):
        """Успешное создание устройства когда есть место."""
        from database import create_device_with_capacity_check, open_db
        
        db = await open_db()
        node_id = await insert_test_node(db, capacity=3, active_configs=0)
        await db.close()
        
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
        db = await open_db()
        async with db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1
        await db.close()
    
    @pytest.mark.asyncio
    async def test_create_device_capacity_exceeded(self, setup_test_env, mock_utc_now):
        """Отказ при превышении capacity."""
        from database import create_device_with_capacity_check, open_db
        
        # Создаём ноду с capacity=2 и уже заполненными active_configs=2
        db = await open_db()
        node_id = await insert_test_node(db, capacity=2, active_configs=2)
        await db.close()
        
        device_id, error = await create_device_with_capacity_check(
            user_id=123,
            slot_number=1,
            node_id=node_id,
            public_key="testpubkey123456789012345678901234567890123=",
            private_key_enc="enc:private_key",
            psk_enc="enc:psk_key",
        )
        
        assert error is not None
        assert "Лимит сервера достигнут" in error or "capacity" in error.lower() or "заполнена" in error.lower()
        assert device_id is None
    
    @pytest.mark.asyncio
    async def test_create_device_race_condition_protection(self, setup_test_env, mock_utc_now):
        """Защита от race condition при одновременном создании."""
        from database import create_device_with_capacity_check, open_db
        
        # Создаём ноду с capacity=2, active_configs=1 (1 слот свободен)
        db = await open_db()
        node_id = await insert_test_node(db, capacity=2, active_configs=1)
        await db.close()
        
        results = []
        
        # Создаём два устройства почти одновременно
        async def create_device_task(user_id: int):
            result = await create_device_with_capacity_check(
                user_id=user_id,
                slot_number=1,
                node_id=node_id,
                public_key=f"testpubkey{user_id}{'=' * 40}",
                private_key_enc="enc:private_key",
                psk_enc="enc:psk_key",
            )
            results.append(result)
        
        # Запускаем параллельно 3 попытки (но свободен только 1 слот)
        tasks = [
            create_device_task(101),
            create_device_task(102),
            create_device_task(103),
        ]
        await asyncio.gather(*tasks)
        
        # Считаем успешные создания
        successful = [r for r in results if r[1] is None]
        failed = [r for r in results if r[1] is not None]
        
        # Должно быть максимум 1 успешное (capacity=2, было 1 занято)
        assert len(successful) <= 1, f"Race condition: {len(successful)} устройств создано"
        assert len(failed) >= 2, "Не все запросы получили отказ"
        
        # Проверяем final state
        db = await open_db()
        async with db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 2, f"active_configs={row[0]}, ожидалось 2"
        await db.close()
    
    @pytest.mark.asyncio
    async def test_create_device_node_not_found(self, setup_test_env, mock_utc_now):
        """Обработка несуществующей ноды."""
        from database import create_device_with_capacity_check
        
        device_id, error = await create_device_with_capacity_check(
            user_id=123,
            slot_number=1,
            node_id=99999,  # Несуществующий ID
            public_key="testpubkey123456789012345678901234567890123=",
            private_key_enc="enc:private_key",
            psk_enc="enc:psk_key",
        )
        
        assert error is not None
        assert "Нода не найдена" in error or "not found" in error.lower()
        assert device_id is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
