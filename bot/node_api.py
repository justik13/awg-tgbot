"""
HTTPS API для оркестрации удалённых нод (Phase 4).

Main-сервер принимает исходящие HTTPS-запросы от агентов нод:
- POST /api/v1/node/register — регистрация новой ноды
- POST /api/v1/node/heartbeat — heartbeat с получением команд

Все эндпоинты требуют заголовок Authorization: Bearer <NODE_TOKEN>.
Архитектура: Node calls Main (polling-модель через heartbeat).
"""

import hashlib
import json
import os
import signal
import socket
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from typing import Any

from aiohttp import web
from aiohttp.web import Request, StreamResponse
from typing import Callable, Awaitable

from .config import logger, ADMIN_ID, API_TOKEN
from .database import open_db, fetchone, execute, enqueue_node_command, get_pending_commands
import aiohttp


# =============================================================================
# AUTH MIDDLEWARE
# =============================================================================


@web.middleware
async def auth_middleware(request: web.Request, handler: Callable[[Request], Awaitable[StreamResponse]]) -> StreamResponse:
    """Middleware для проверки NODE_TOKEN из заголовка Authorization."""
    # Эндпоинты регистрации не требуют токена (он генерируется после)
    if request.path == "/api/v1/node/register":
        return await handler(request)
    
    # Все остальные эндпоинты требуют авторизации
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        logger.warning("Auth failed: missing or invalid Authorization header")
        return web.json_response(
            {"error": "Missing or invalid Authorization header"},
            status=401,
        )
    
    token = auth_header[7:]  # Remove "Bearer " prefix
    
    # Вычисляем SHA-256 хэш предоставленного токена
    provided_token_hash = hashlib.sha256(token.encode()).hexdigest()
    
    # Проверяем токен в БД через сравнение хешей
    db = await open_db()
    try:
        async with db.execute(
            "SELECT id, status, api_token FROM nodes WHERE api_token_hash = ?",
            (provided_token_hash,),
        ) as cursor:
            row = await cursor.fetchone()
        
        if not row:
            logger.warning("Auth failed: invalid token hash")
            return web.json_response(
                {"error": "Invalid node token"},
                status=401,
            )
        
        node_id, status, stored_token = row
        if status != "ready":
            logger.warning("Auth failed: node not ready (status=%s)", status)
            return web.json_response(
                {"error": f"Node not ready (status={status})"},
                status=403,
            )
        
        # Дополнительная проверка: сравниваем полный токен (защита от коллизий хешей)
        if stored_token != token:
            logger.warning("Auth failed: token mismatch after hash match (collision?)")
            return web.json_response(
                {"error": "Invalid node token"},
                status=401,
            )
        
        # Сохраняем node_id в request context
        request["node_id"] = node_id
    finally:
        await db.close()
    
    return await handler(request)


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def compute_params_hash(
    port: int,
    s1: str, s2: str, s3: str, s4: str,
    h1: str, h2: str, h3: str, h4: str,
) -> str:
    """Вычисляет SHA256 хэш параметров awg0 для детектирования дрейфа."""
    params_str = f"{port}{s1}{s2}{s3}{s4}{h1}{h2}{h3}{h4}"
    return hashlib.sha256(params_str.encode()).hexdigest()


# =============================================================================
# API HANDLERS
# =============================================================================


async def handle_register(request: web.Request) -> web.Response:
    """
    POST /api/v1/node/register
    
    Payload: {
        "ip": str,
        "port": int,
        "s1..s4": str,
        "h1..h4": str,
        "hostname": str
    }
    
    Response: { "node_id": int, "api_token": str, "status": "ready" }
    """
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"error": "Invalid JSON payload"},
            status=400,
        )
    
    # Валидация обязательных полей
    required_fields = ["ip", "port", "hostname"]
    for field in required_fields:
        if field not in payload:
            return web.json_response(
                {"error": f"Missing required field: {field}"},
                status=400,
            )
    
    ip = payload["ip"]
    port = payload["port"]
    hostname = payload["hostname"]
    
    # Параметры awg0 (опционально, могут быть заполнены позже)
    s1 = payload.get("s1", "")
    s2 = payload.get("s2", "")
    s3 = payload.get("s3", "")
    s4 = payload.get("s4", "")
    h1 = payload.get("h1", "")
    h2 = payload.get("h2", "")
    h3 = payload.get("h3", "")
    h4 = payload.get("h4", "")
    
    # Вычисляем хэш параметров
    params_hash = compute_params_hash(port, s1, s2, s3, s4, h1, h2, h3, h4)
    
    db = await open_db()
    try:
        await db.execute("BEGIN IMMEDIATE")
        
        # Проверяем, есть ли уже нода с таким IP+port
        async with db.execute(
            "SELECT id, api_token FROM nodes WHERE ip = ? AND port = ?",
            (ip, port),
        ) as cursor:
            existing = await cursor.fetchone()
        
        now_iso = datetime.now(timezone.utc).isoformat()
        
        if existing:
            # Ноде уже существует — обновляем last_seen и параметры
            node_id, api_token = existing
            await db.execute(
                """
                UPDATE nodes
                SET last_seen = ?, params_hash = ?, s1 = ?, s2 = ?, s3 = ?, s4 = ?,
                    h1 = ?, h2 = ?, h3 = ?, h4 = ?, status = 'ready'
                WHERE id = ?
                """,
                (now_iso, params_hash, s1, s2, s3, s4, h1, h2, h3, h4, node_id),
            )
            await db.commit()
            
            logger.info("Node re-registered: node_id=%s ip=%s port=%s", node_id, ip, port)
            
            return web.json_response({
                "node_id": node_id,
                "api_token": api_token,
                "status": "ready",
            })
        else:
            # Создаём новую ноду
            api_token = str(uuid.uuid4())
            api_token_hash = hashlib.sha256(api_token.encode()).hexdigest()
            
            cursor = await db.execute(
                """
                INSERT INTO nodes (
                    name, ip, port, s1, s2, s3, s4, h1, h2, h3, h4,
                    status, api_token, api_token_hash, params_hash, last_seen, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?, ?)
                """,
                (
                    hostname, ip, port, s1, s2, s3, s4, h1, h2, h3, h4,
                    api_token, api_token_hash, params_hash, now_iso, now_iso,
                ),
            )
            node_id = cursor.lastrowid
            await db.commit()
            
            logger.info("Node registered: node_id=%s ip=%s port=%s hostname=%s", node_id, ip, port, hostname)
            
            return web.json_response({
                "node_id": node_id,
                "api_token": api_token,
                "status": "ready",
            }, status=201)
    finally:
        await db.close()


async def handle_heartbeat(request: web.Request) -> web.Response:
    """
    POST /api/v1/node/heartbeat
    
    Payload: {
        "node_id": int,
        "status": str,
        "params_hash": str,
        "denylist_version": str,
        "active_peers": int
    }
    
    Response: {
        "status": "ok",
        "commands": [...],
        "denylist_version": str
    }
    """
    node_id = request["node_id"]
    
    try:
        payload = await request.json()
    except json.JSONDecodeError:
        return web.json_response(
            {"error": "Invalid JSON payload"},
            status=400,
        )
    
    node_status = payload.get("status", "unknown")
    params_hash = payload.get("params_hash", "")
    denylist_version = payload.get("denylist_version", "v0")
    active_peers = payload.get("active_peers", 0)
    
    db = await open_db()
    try:
        await db.execute("BEGIN IMMEDIATE")
        
        # Получаем текущие данные ноды для проверки drift
        async with db.execute(
            "SELECT params_hash, name FROM nodes WHERE id = ?",
            (node_id,),
        ) as cursor:
            row = await cursor.fetchone()
        
        if not row:
            await db.rollback()
            return web.json_response(
                {"error": "Node not found"},
                status=404,
            )
        
        stored_params_hash, node_name = row
        
        # Обновляем last_seen, status, active_configs
        now_iso = datetime.now(timezone.utc).isoformat()
        await db.execute(
            """
            UPDATE nodes
            SET last_seen = ?, status = ?, active_configs = ?
            WHERE id = ?
            """,
            (now_iso, node_status, active_peers, node_id),
        )
        
        # Проверяем drift параметров
        params_drift = False
        if stored_params_hash and params_hash and stored_params_hash != params_hash:
            params_drift = True
            logger.warning(
                "Params drift detected: node_id=%s name=%s stored_hash=%s reported_hash=%s",
                node_id, node_name, stored_params_hash[:16], params_hash[:16],
            )
        
        await db.commit()
        
        # Получаем pending команды
        commands = await get_pending_commands(node_id)
        
        # Если есть drift — отправляем уведомление админу
        if params_drift:
            logger.warning(
                "PARAMS DRIFT ALERT: node_id=%s name=%s requires attention",
                node_id, node_name,
            )
            # Отправляем уведомление в Telegram
            await send_node_alert(
                f"⚠️ <b>Params Drift Detected</b>\n\n"
                f"Node: <code>{node_name}</code> (ID: {node_id})\n"
                f"Требуется проверка конфигурации awg0 на ноде."
            )
        
        # Проверяем offline статус
        if node_status == "degraded" or node_status == "offline":
            logger.warning(
                "NODE OFFLINE ALERT: node_id=%s name=%s status=%s",
                node_id, node_name, node_status,
            )
            await send_node_alert(
                f"🔴 <b>Node Offline/Degraded</b>\n\n"
                f"Node: <code>{node_name}</code> (ID: {node_id})\n"
                f"Status: <code>{node_status}</code>\n"
                f"Последний heartbeat: {now_iso}"
            )
        
        logger.debug(
            "Heartbeat received: node_id=%s name=%s status=%s peers=%s commands=%d",
            node_id, node_name, node_status, active_peers, len(commands),
        )
        
        return web.json_response({
            "status": "ok",
            "commands": commands,
            "denylist_version": denylist_version,  # Возвращаем ту же версию (агент сам управляет)
        })
    finally:
        await db.close()


async def send_node_alert(message: str) -> None:
    """Отправляет уведомление админу о проблемах с нодой."""
    if not ADMIN_ID or not API_TOKEN:
        logger.warning("Cannot send node alert: ADMIN_ID or API_TOKEN not configured")
        return
    
    try:
        async with aiohttp.ClientSession() as session:
            url = f"https://api.telegram.org/bot{API_TOKEN}/sendMessage"
            payload = {
                "chat_id": ADMIN_ID,
                "text": message,
                "parse_mode": "HTML",
            }
            async with session.post(url, json=payload) as resp:
                if resp.status != 200:
                    logger.warning("Failed to send node alert: status=%d", resp.status)
                else:
                    logger.info("Node alert sent to admin: %s", message[:50])
    except Exception as e:
        logger.error("Failed to send node alert: %s", e)


# =============================================================================
# PORT CLEANUP HELPERS
# =============================================================================


def cleanup_port_processes(port: int) -> None:
    """
    Находит и останавливает процессы python/python3/node на указанном порту.
    Вызывается перед попыткой запуска Node API сервера.
    """
    try:
        # Используем ss для нахождения процесса на порту
        result = subprocess.run(
            ["ss", "-tlnp"],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode != 0:
            return
        
        # Ищем строки с нужным портом
        for line in result.stdout.splitlines():
            if f":{port}" in line and "pid=" in line:
                # Извлекаем PID
                import re
                pid_match = re.search(r'pid=(\d+)', line)
                if not pid_match:
                    continue
                
                pid = int(pid_match.group(1))
                
                # Проверяем, существует ли процесс
                if not os.path.exists(f"/proc/{pid}"):
                    continue
                
                # Получаем имя процесса
                try:
                    with open(f"/proc/{pid}/comm", "r") as f:
                        proc_name = f.read().strip()
                except (IOError, OSError):
                    continue
                
                # Проверяем, относится ли процесс к python/node
                if proc_name in ("python", "python3", "node"):
                    logger.warning(
                        "Обнаружен остаточный процесс на порту %d: PID=%d, cmd=%s",
                        port, pid, proc_name
                    )
                    
                    # Пробуем остановить gracefully
                    try:
                        os.kill(pid, signal.SIGTERM)
                        logger.info("Отправлен SIGTERM процессу PID=%d", pid)
                    except OSError:
                        pass
                    
                    # Ждём немного
                    import time
                    time.sleep(1)
                    
                    # Если процесс всё ещё жив - принудительная остановка
                    if os.path.exists(f"/proc/{pid}"):
                        try:
                            os.kill(pid, signal.SIGKILL)
                            logger.info("Процесс PID=%d уничтожен принудительно", pid)
                        except OSError:
                            pass
                else:
                    logger.warning(
                        "На порту %d обнаружен процесс %s (PID=%d), не относящийся к боту. "
                        "Требуется ручная проверка.",
                        port, proc_name, pid
                    )
    except Exception as e:
        logger.warning("Ошибка при очистке порта %d: %s", port, e)


def find_free_port(start_port: int = 8444, max_attempts: int = 20) -> int:
    """
    Находит первый свободный порт начиная с указанного.
    Возвращает свободный порт или исходный, если не удалось найти.
    """
    import subprocess
    
    for attempt in range(max_attempts):
        port = start_port + attempt
        try:
            # Сначала проверяем через ss - более надёжно
            result = subprocess.run(
                ["ss", "-tlnp"],
                capture_output=True,
                text=True,
                timeout=5
            )
            port_in_use = False
            if result.returncode == 0:
                for line in result.stdout.splitlines():
                    if f":{port}" in line and "LISTEN" in line:
                        port_in_use = True
                        break
            
            # Если ss не показал порт - пробуем bind для подтверждения
            if not port_in_use:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    sock.bind(('0.0.0.0', port))
                    sock.close()
                    return port  # Порт действительно свободен
                except OSError:
                    port_in_use = True
            
            if not port_in_use:
                return port
                
        except Exception:
            pass
    
    # Если не нашли свободный порт - возвращаем исходный
    return start_port


# =============================================================================
# APP FACTORY
# =============================================================================


def create_node_api_app() -> web.Application:
    """Создаёт aiohttp приложение для Node API."""
    app = web.Application(middlewares=[auth_middleware])
    
    # Регистрируем роуты
    app.router.add_post("/api/v1/node/register", handle_register)
    app.router.add_post("/api/v1/node/heartbeat", handle_heartbeat)
    
    return app


async def start_node_api_server(app: web.Application, port: int) -> int:
    """Запускает Node API сервер вместе с основным приложением бота.
    
    ARCHITECTURE DECISION: Node API критичен для multi-server синхронизации.
    При невозможности bind на порт — пробуем очистить порт и найти свободный.
    Возвращает фактически использованный порт.
    """
    from aiohttp.web_runner import AppRunner, TCPSite
    import time
    
    # Сначала пробуем очистить порт от остаточных процессов
    logger.info("Проверка порта %d на наличие остаточных процессов...", port)
    cleanup_port_processes(port)
    
    # Даём время на остановку процессов
    time.sleep(0.5)
    
    # Пробуем найти свободный порт (может быть тот же самый)
    actual_port = find_free_port(port)
    
    if actual_port != port:
        logger.info(
            "Порт %d был занят, используем свободный порт %d",
            port, actual_port
        )
    else:
        logger.info("Порт %d свободен", port)
    
    runner = AppRunner(app)
    await runner.setup()
    
    site = TCPSite(runner, "0.0.0.0", actual_port)
    try:
        await site.start()
    except OSError as e:
        if e.errno == 98:  # Address already in use
            # Пробуем ещё раз очистить порт и перезапустить
            logger.warning(
                "Порт %d всё ещё занят после очистки. Повторная попытка...",
                actual_port
            )
            cleanup_port_processes(actual_port)
            time.sleep(1)
            
            # Пробуем найти другой свободный порт
            new_port = find_free_port(actual_port + 1)
            if new_port != actual_port:
                logger.info("Переключаемся на порт %d", new_port)
                await site.stop()
                site = TCPSite(runner, "0.0.0.0", new_port)
                actual_port = new_port
                await site.start()
            else:
                logger.error(
                    "NODE API PORT CONFLICT: порт %d занят другим сервисом. "
                    "Измените NODE_API_PORT в .env на свободный порт и перезапустите бота.",
                    actual_port
                )
                raise SystemExit(1) from e
        else:
            logger.error("NODE API START ERROR: %s", e)
            raise SystemExit(1) from e
    
    logger.info("Node API server started on port %d", actual_port)
    
    # Сохраняем runner и порт для последующей остановки
    app["node_api_runner"] = runner
    app["node_api_port"] = actual_port
    
    return actual_port


async def stop_node_api_server(app: web.Application) -> None:
    """Останавливает Node API сервер при shutdown."""
    runner = app.get("node_api_runner")
    if runner:
        await runner.cleanup()
        logger.info("Node API server stopped")
