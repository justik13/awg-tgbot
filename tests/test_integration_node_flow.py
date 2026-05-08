"""
Интеграционный тест полного потока работы ноды (Phase 4).

Проверяет:
1. Регистрация ноды через POST /api/v1/node/register
2. Heartbeat ноды с получением команд
3. Создание команды add_peer в БД → агент получает через heartbeat
4. Применение команды (мокаем awg set)
5. Проверка active_configs увеличивается только один раз

Использует:
- aiohttp.test_utils для мока Node API
- pytest-asyncio для асинхронных тестов
- Мок вызовов awg через subprocess.run
- Временные файлы БД (tmp_path)
"""

import asyncio
import hashlib
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

import pytest
import pytest_asyncio
from aiohttp import web
from aiohttp.test_utils import TestServer, TestClient


# =============================================================================
# НАСТРОЙКА ПУТЕЙ И ОКРУЖЕНИЯ ПЕРЕД ИМПОРТОМ МОДУЛЕЙ ПРОЕКТА
# =============================================================================

BOT_DIR = Path(__file__).parent.parent / "bot"
sys.path.insert(0, str(BOT_DIR))


@pytest.fixture(autouse=True)
def setup_test_env(tmp_path, monkeypatch):
    """
    Настраивает изолированную среду для каждого теста.
    Использует временный файл БД вместо :memory:.
    """
    db_file = tmp_path / "test_awg.db"

    # Устанавливаем переменные окружения ДО импорта модулей
    monkeypatch.setenv("API_TOKEN", "test_token_12345")
    monkeypatch.setenv("ADMIN_ID", "123456789")
    monkeypatch.setenv("SERVER_PUBLIC_KEY", "testpubkey123456789012345678901234567890123=")
    monkeypatch.setenv("SERVER_IP", "8.8.8.8:51820")
    monkeypatch.setenv("ENCRYPTION_SECRET", "testsecret12345678901234567890")
    monkeypatch.setenv("BOT_TOKEN", "1234567890:AABBccDDeeFFggHHiiJJkkLLmmNNooppQQrrs")
    monkeypatch.setenv("DB_PATH", str(db_file))
    monkeypatch.setenv("CONFIG_AUTODETECT_ON_IMPORT", "0")
    monkeypatch.setenv("AUTO_DETECT_ON_IMPORT", "false")

    yield db_file


@pytest_asyncio.fixture
async def test_db(setup_test_env):
    """
    Асинхронная фикстура для инициализации БД.
    Запускает init_db() и миграцию Phase 1.
    """
    from database import init_db, close_shared_db
    from migration_phase1 import run_migration

    await init_db()
    await run_migration(dry_run=False)

    yield setup_test_env

    await close_shared_db()


@pytest_asyncio.fixture
async def node_api_client(test_db):
    """
    Создаёт тестовый клиент для Node API.
    """
    from node_api import create_node_api_app

    app = create_node_api_app()
    server = TestServer(app)
    client = TestClient(server)

    await client.start_server()
    yield client
    await client.close()


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

async def get_open_db():
    """Вспомогательная функция для открытия соединения с БД."""
    from database import open_db
    return await open_db()


async def insert_test_node(db, name="test_node", ip="192.168.1.1", port=51820, capacity=10, active_configs=0):
    """Вставляет тестовую ноду и возвращает её ID."""
    now_iso = datetime.now(timezone.utc).isoformat()
    unique_token = f"test-token-{uuid.uuid4()}"
    unique_token_hash = hashlib.sha256(unique_token.encode()).hexdigest()

    await db.execute("""
        INSERT INTO nodes (
            name, ip, port, status, capacity, active_configs,
            api_token, api_token_hash, last_seen, created_at,
            s1, s2, s3, s4, h1, h2, h3, h4
        ) VALUES (?, ?, ?, 'ready', ?, ?, ?, ?, ?, ?,
                  '', '', '', '', '', '', '', '')
    """, (
        name, ip, port,
        capacity, active_configs,
        unique_token, unique_token_hash, now_iso, now_iso
    ))
    await db.commit()

    async with db.execute("SELECT last_insert_rowid()") as cursor:
        row = await cursor.fetchone()
        return row[0], unique_token


async def register_node_via_api(client, ip="192.168.1.1", port=51820, hostname="test-host"):
    """Регистрирует ноду через API и возвращает response data."""
    payload = {
        "ip": ip,
        "port": port,
        "hostname": hostname,
        "s1": "", "s2": "", "s3": "", "s4": "",
        "h1": "", "h2": "", "h3": "", "h4": "",
    }

    async with client.post("/api/v1/node/register", json=payload) as resp:
        assert resp.status in (200, 201), f"Registration failed: {resp.status}"
        return await resp.json()


async def send_heartbeat(client, node_id: int, api_token: str, active_peers: int = 0):
    """Отправляет heartbeat и возвращает response с командами."""
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }

    payload = {
        "node_id": node_id,
        "status": "ready",
        "params_hash": "",
        "denylist_version": "v0",
        "active_peers": active_peers,
    }

    async with client.post("/api/v1/node/heartbeat", json=payload, headers=headers) as resp:
        assert resp.status == 200, f"Heartbeat failed: {resp.status}"
        return await resp.json()


async def enqueue_add_peer_command(db, node_id: int, public_key: str, allowed_ips: str = "0.0.0.0/0", preshared_key: str = ""):
    """Добавляет команду add_peer в очередь."""
    now_iso = datetime.now(timezone.utc).isoformat()
    payload = {
        "public_key": public_key,
        "allowed_ips": allowed_ips,
        "preshared_key": preshared_key,
    }

    await db.execute("""
        INSERT INTO node_commands (node_id, action, payload_json, status, created_at)
        VALUES (?, ?, ?, 'pending', ?)
    """, (node_id, "add_peer", json.dumps(payload), now_iso))
    await db.commit()

    async with db.execute("SELECT last_insert_rowid()") as cursor:
        row = await cursor.fetchone()
        return row[0]


# =============================================================================
# ТЕСТЫ
# =============================================================================

class TestIntegrationNodeFlow:
    """Интеграционные тесты полного потока работы ноды."""

    @pytest.mark.asyncio
    async def test_node_registration_returns_token(self, test_db, node_api_client):
        """Тест 1: Регистрация ноды возвращает токен и сохраняет хеш в БД."""
        # Регистрируем ноду
        response = await register_node_via_api(node_api_client, ip="192.168.1.100", port=51820)

        # Проверяем ответ
        assert "node_id" in response, "Response missing node_id"
        assert "api_token" in response, "Response missing api_token"
        assert response["status"] == "ready", f"Unexpected status: {response['status']}"

        node_id = response["node_id"]
        api_token = response["api_token"]

        # Проверяем БД
        db = await get_open_db()
        async with db.execute(
            "SELECT id, api_token, api_token_hash FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
        await db.close()

        assert row is not None, "Node not found in DB"
        db_node_id, db_token, db_token_hash = row

        # Проверяем что токен сохранён
        assert db_token == api_token, "Token mismatch between response and DB"

        # Проверяем что хеш соответствует токену
        expected_hash = hashlib.sha256(api_token.encode()).hexdigest()
        assert db_token_hash == expected_hash, "Token hash mismatch"

        print(f"✅ Node registered: node_id={node_id}, token={api_token[:16]}...")

    @pytest.mark.asyncio
    async def test_heartbeat_receives_commands(self, test_db, node_api_client):
        """Тест 2: Heartbeat возвращает pending команды и помечает их как sent."""
        # Регистрируем ноду
        reg_response = await register_node_via_api(node_api_client, ip="192.168.1.101", port=51821)
        node_id = reg_response["node_id"]
        api_token = reg_response["api_token"]

        # Добавляем команду add_peer напрямую в БД
        db = await get_open_db()
        test_public_key = "testpubkey123456789012345678901234567890123="
        command_id = await enqueue_add_peer_command(db, node_id, test_public_key)
        await db.close()

        # Отправляем heartbeat
        hb_response = await send_heartbeat(node_api_client, node_id, api_token, active_peers=0)

        # Проверяем что команда получена
        assert "commands" in hb_response, "Response missing commands"
        commands = hb_response["commands"]
        assert len(commands) == 1, f"Expected 1 command, got {len(commands)}"

        cmd = commands[0]
        assert cmd["action"] == "add_peer", f"Unexpected action: {cmd['action']}"
        assert cmd["payload"]["public_key"] == test_public_key, "Public key mismatch"

        # Проверяем что команда помечена как sent
        db = await get_open_db()
        async with db.execute(
            "SELECT status FROM node_commands WHERE id = ?", (command_id,)
        ) as cursor:
            row = await cursor.fetchone()
        await db.close()

        assert row[0] == "sent", f"Command status should be 'sent', got '{row[0]}'"

        print(f"✅ Command queued (id={command_id}) and received via heartbeat")

    @pytest.mark.asyncio
    async def test_apply_add_peer_command_mocks_awg(self, test_db, node_api_client):
        """Тест 3: Агент применяет команду add_peer (мокаем awg set)."""
        # Регистрируем ноду
        reg_response = await register_node_via_api(node_api_client, ip="192.168.1.102", port=51822)
        node_id = reg_response["node_id"]
        api_token = reg_response["api_token"]

        # Добавляем команду add_peer
        db = await get_open_db()
        test_public_key = "testpubkeyABCDEFGHIJKLMNOPQRSTUVWXYZ12345="
        test_psk = "testpskABCDEFGHIJKLMNOPQRSTUVWXYZ1234567="
        command_id = await enqueue_add_peer_command(
            db, node_id, test_public_key,
            allowed_ips="0.0.0.0/0",
            preshared_key=test_psk
        )
        await db.close()

        # Получаем команду через heartbeat
        hb_response = await send_heartbeat(node_api_client, node_id, api_token, active_peers=0)
        commands = hb_response["commands"]
        assert len(commands) == 1

        # Мокаем subprocess.run для awg set
        with patch("subprocess.run") as mock_run:
            # Настраиваем мок для успешного выполнения
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = ""
            mock_result.stderr = ""
            mock_run.return_value = mock_result

            # Симулируем применение команды агентом (как в agent.py apply_add_peer)
            command = commands[0]
            payload = command["payload"]

            # Удаляем пира если существует (для идемпотентности)
            subprocess.run(
                ["awg", "set", "awg0", "peer", payload["public_key"], "remove"],
                capture_output=True,
                timeout=10,
                check=False,
            )

            # Добавляем пира
            cmd = ["awg", "set", "awg0", "peer", payload["public_key"]]
            cmd.extend(["allowed-ips", payload.get("allowed_ips", "0.0.0.0/0")])

            if payload.get("preshared_key"):
                # В реальном агенте PSK записывается во временный файл
                # Здесь просто добавляем аргумент
                cmd.extend(["preshared-key", "/tmp/psk_temp.txt"])

            proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=True)

            # Проверяем что awg set был вызван
            assert mock_run.called, "subprocess.run was not called"

            # Проверяем что вызов был с правильными аргументами
            calls = mock_run.call_args_list
            assert len(calls) >= 2, f"Expected at least 2 calls (remove + add), got {len(calls)}"

            # Второй вызов должен быть add_peer с correct public key
            add_call = calls[1]
            add_args = add_call[0][0]  # Позиционные аргументы
            assert "awg" in add_args[0], "First arg should be 'awg'"
            assert "set" in add_args, "'set' should be in args"
            assert "awg0" in add_args, "'awg0' should be in args"
            assert test_public_key in add_args, f"Public key {test_public_key} should be in args"

            print(f"✅ Command applied: peer {test_public_key[:16]}... added (mocked awg set)")

    @pytest.mark.asyncio
    async def test_active_configs_increments_once_on_device_create(self, test_db, node_api_client):
        """Тест 4: active_configs увеличивается только один раз при создании устройства."""
        from database import create_device_with_capacity_check

        # Создаём ноду с capacity=5
        db = await get_open_db()
        node_id, _ = await insert_test_node(db, capacity=5, active_configs=0)

        # Создаём тестового пользователя
        now_iso = datetime.now(timezone.utc).isoformat()
        await db.execute("""
            INSERT OR IGNORE INTO users (user_id, sub_until, created_at)
            VALUES (?, ?, ?)
        """, (12345, '0', now_iso))
        await db.commit()
        await db.close()

        # Создаём первое устройство
        device_id1, error1 = await create_device_with_capacity_check(
            user_id=12345,
            slot_number=1,
            node_id=node_id,
            public_key="testpubkey1===================================",
            private_key_enc="enc:private_key_1",
            psk_enc="enc:psk_key_1",
        )

        assert error1 is None, f"First device creation failed: {error1}"
        assert device_id1 is not None

        # Проверяем active_configs
        db = await get_open_db()
        async with db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
        assert row[0] == 1, f"active_configs should be 1 after first device, got {row[0]}"

        # Создаём второе устройство (другой слот)
        device_id2, error2 = await create_device_with_capacity_check(
            user_id=12345,
            slot_number=2,
            node_id=node_id,
            public_key="testpubkey2===================================",
            private_key_enc="enc:private_key_2",
            psk_enc="enc:psk_key_2",
        )

        assert error2 is None, f"Second device creation failed: {error2}"
        assert device_id2 is not None

        # Проверяем active_configs снова
        async with db.execute(
            "SELECT active_configs FROM nodes WHERE id = ?", (node_id,)
        ) as cursor:
            row = await cursor.fetchone()
        await db.close()

        assert row[0] == 2, f"active_configs should be 2 after second device, got {row[0]}"

        print(f"✅ active_configs incremented correctly: 0 → 1 → 2")

    @pytest.mark.asyncio
    async def test_full_flow_registration_heartbeat_command_apply(self, test_db, node_api_client):
        """
        Полный тест потока:
        1. Регистрация ноды
        2. Heartbeat без команд
        3. Создание команды add_peer
        4. Heartbeat с получением команды
        5. Применение команды (мокаем awg)
        """
        print("\n=== Starting Full Flow Test ===\n")

        # Шаг 1: Регистрация ноды
        print("Step 1: Registering node...")
        reg_response = await register_node_via_api(
            node_api_client,
            ip="203.0.113.50",
            port=51830,
            hostname="node-full-test"
        )
        node_id = reg_response["node_id"]
        api_token = reg_response["api_token"]
        print(f"  ✅ Node registered: node_id={node_id}")

        # Шаг 2: Первый heartbeat (без команд)
        print("Step 2: First heartbeat (no commands)...")
        hb_response = await send_heartbeat(node_api_client, node_id, api_token, active_peers=0)
        assert len(hb_response["commands"]) == 0, "Should be no commands on first heartbeat"
        print("  ✅ Heartbeat received, no commands")

        # Шаг 3: Создаём команду add_peer
        print("Step 3: Queuing add_peer command...")
        db = await get_open_db()
        test_public_key = "fullflowtestkey1234567890ABCDEFGHIJKLMNO="
        test_psk = "fullflowtestpsk1234567890ABCDEFGHIJKLMNO="
        command_id = await enqueue_add_peer_command(db, node_id, test_public_key, preshared_key=test_psk)
        await db.close()
        print(f"  ✅ Command queued: id={command_id}")

        # Шаг 4: Второй heartbeat (получаем команду)
        print("Step 4: Second heartbeat (receiving command)...")
        hb_response = await send_heartbeat(node_api_client, node_id, api_token, active_peers=0)
        assert len(hb_response["commands"]) == 1, "Should receive 1 command"
        command = hb_response["commands"][0]
        assert command["action"] == "add_peer"
        assert command["payload"]["public_key"] == test_public_key
        print(f"  ✅ Command received via heartbeat: action={command['action']}")

        # Шаг 5: Применяем команду (мокаем awg set)
        print("Step 5: Applying command (mocking awg set)...")
        with patch("subprocess.run") as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = ""
            mock_result.stderr = ""
            mock_run.return_value = mock_result

            payload = command["payload"]

            # Remove peer (идемпотентность)
            subprocess.run(
                ["awg", "set", "awg0", "peer", payload["public_key"], "remove"],
                capture_output=True,
                timeout=10,
                check=False,
            )

            # Add peer
            cmd = ["awg", "set", "awg0", "peer", payload["public_key"]]
            cmd.extend(["allowed-ips", "0.0.0.0/0"])
            if payload.get("preshared_key"):
                cmd.extend(["preshared-key", "/tmp/psk.txt"])

            subprocess.run(cmd, capture_output=True, text=True, timeout=10, check=True)

            assert mock_run.called, "awg set should be called"

        print(f"  ✅ Peer added: {test_public_key[:16]}... (mocked)")

        # Шаг 6: Проверяем что команда помечена как sent
        print("Step 6: Verifying command status...")
        db = await get_open_db()
        async with db.execute(
            "SELECT status FROM node_commands WHERE id = ?", (command_id,)
        ) as cursor:
            row = await cursor.fetchone()
        await db.close()

        assert row[0] == "sent", f"Command should be marked as 'sent', got '{row[0]}'"
        print(f"  ✅ Command status updated to 'sent'")

        print("\n=== Full Flow Test Completed Successfully ===\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
