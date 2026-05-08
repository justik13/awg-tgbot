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
import os
import sys
import pytest
import pytest_asyncio
from datetime import datetime, timezone
from pathlib import Path


# =============================================================================
# НАСТРОЙКА ПУТЕЙ И ОКРУЖЕНИЯ ПЕРЕД ИМПОРТОМ МОДУЛЕЙ ПРОЕКТА
# =============================================================================

# Добавляем bot в sys.path для корректных импортов
BOT_DIR = Path(__file__).parent.parent / "bot"
sys.path.insert(0, str(BOT_DIR))


@pytest.fixture(autouse=True)
def setup_test_env(tmp_path, monkeypatch):
    """
    Настраивает изолированную среду для каждого теста.
    Использует временный файл БД вместо :memory:.
    Устанавливает переменные окружения ДО импорта модулей проекта.
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

    yield db_file


# =============================================================================
# АСИНХРОННАЯ ФИКСТУРА БД
# =============================================================================

@pytest_asyncio.fixture
async def test_db(setup_test_env):
    """
    Асинхронная фикстура для инициализации БД.
    Запускает init_db() и миграцию Phase 1 для создания всех таблиц.
    Гарантирует закрытие соединений после теста.
    """
    from database import init_db, close_shared_db
    from migration_phase1 import run_migration

    # Инициализируем БД и запускаем миграцию
    await init_db()
    await run_migration(dry_run=False)

    yield setup_test_env

    # Очищаем shared connection после теста
    await close_shared_db()


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

async def get_open_db():
    """Вспомогательная функция для открытия соединения с БД."""
    from database import open_db
    return await open_db()


async def insert_test_node(db, capacity: int = 3, active_configs: int = 0):
    """Вставляет тестовую ноду и тестового пользователя."""
    now_iso = datetime.now(timezone.utc).isoformat()

    # Создаём тестового пользователя (требуется для FK devices -> users)
    await db.execute("""
        INSERT OR IGNORE INTO users (user_id, sub_until, created_at)
        VALUES (?, ?, ?)
    """, (123, '0', now_iso))

    # Генерируем уникальный api_token для каждой ноды
    import uuid
    unique_token = f"test-token-{uuid.uuid4()}"
    unique_token_hash = f"hash-{uuid.uuid4()}"

    await db.execute("""
        INSERT INTO nodes (
            name, ip, port, status, capacity, active_configs,
            api_token, api_token_hash, last_seen, created_at
        ) VALUES (?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?)
    """, (
        "test_node", "192.168.1.1", 51820,
        capacity, active_configs,
        unique_token, unique_token_hash, now_iso, now_iso
    ))
    await db.commit()

    async with db.execute("SELECT last_insert_rowid()") as cursor:
        row = await cursor.fetchone()
        return row[0]


# =============================================================================
# ТЕСТЫ
# =============================================================================

class TestDeviceCreationCapacityCheck:
    """Тесты проверки capacity при создании устройств."""

    @pytest.mark.asyncio
    async def test_create_device_success(self, test_db):
        """Успешное создание устройства когда есть место."""
        from database import create_device_with_capacity_check

        db = await get_open_db()
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
        db = await get_open_db()
        async with db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1
        await db.close()

    @pytest.mark.asyncio
    async def test_create_device_capacity_exceeded(self, test_db):
        """Отказ при превышении capacity."""
        from database import create_device_with_capacity_check

        # Создаём ноду с capacity=2 и уже заполненными active_configs=2
        db = await get_open_db()
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
    async def test_create_device_race_condition_protection(self, test_db):
        """Защита от race condition при одновременном создании."""
        from database import create_device_with_capacity_check, open_db

        # Очищаем таблицу devices и создаём ноду с capacity=1, active_configs=0
        # Это гарантирует, что только одно устройство может быть создано
        db = await get_open_db()
        
        # Очищаем devices чтобы избежать конфликтов UNIQUE(user_id, slot_number)
        await db.execute("DELETE FROM devices")
        await db.execute("DELETE FROM nodes")
        # Создаём тестовых пользователей (требуется для FK devices -> users)
        from datetime import datetime, timezone
        now_iso = datetime.now(timezone.utc).isoformat()
        for uid in [101, 102, 103]:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, sub_until, created_at) VALUES (?, ?, ?)",
                (uid, '0', now_iso)
            )
        await db.commit()
        
        node_id = await insert_test_node(db, capacity=1, active_configs=0)
        await db.close()

        results = []

        # Создаём устройства почти одновременно
        # Используем разные slot_number для каждого пользователя
        async def create_device_task(user_id: int, slot_number: int):
            result = await create_device_with_capacity_check(
                user_id=user_id,
                slot_number=slot_number,
                node_id=node_id,
                public_key=f"testpubkey{user_id}{'=' * 40}",
                private_key_enc="enc:private_key",
                psk_enc="enc:psk_key",
            )
            results.append(result)

        # Запускаем параллельно 3 попытки (но свободен только 1 слот - capacity=1)
        tasks = [
            create_device_task(101, 1),
            create_device_task(102, 2),
            create_device_task(103, 3),
        ]
        await asyncio.gather(*tasks)

        # Считаем успешные создания
        successful = [r for r in results if r[1] is None]
        failed = [r for r in results if r[1] is not None]

        # Должно быть максимум 1 успешное (capacity=1)
        assert len(successful) == 1, f"Ожидалось 1 успешное создание, получилось {len(successful)}"
        assert len(failed) == 2, f"Ожидалось 2 отказа, получилось {len(failed)}"

        # Проверяем final state - active_configs должен быть 1
        db = await get_open_db()
        async with db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
            assert row[0] == 1, f"active_configs={row[0]}, ожидалось 1"
        await db.close()

    @pytest.mark.asyncio
    async def test_create_device_node_not_found(self, test_db):
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
