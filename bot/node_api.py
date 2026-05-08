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
import uuid
from datetime import datetime, timezone
from typing import Any

from aiohttp import web
from aiohttp.web import Request, StreamResponse
from typing import Callable, Awaitable

from config import logger
from database import open_db, fetchone, execute, enqueue_node_command, get_pending_commands


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
        
        # Если есть drift — логируем (можно отправить уведомление админу)
        if params_drift:
            # Здесь можно добавить отправку уведомления ADMIN_ID в Telegram
            logger.warning(
                "PARAMS DRIFT ALERT: node_id=%s name=%s requires attention",
                node_id, node_name,
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


async def start_node_api_server(app: web.Application, port: int) -> None:
    """Запускает Node API сервер вместе с основным приложением бота."""
    from aiohttp.web_runner import AppRunner, TCPSite
    
    runner = AppRunner(app)
    await runner.setup()
    
    site = TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    logger.info("Node API server started on port %d", port)
    
    # Сохраняем runner для последующей остановки
    app["node_api_runner"] = runner


async def stop_node_api_server(app: web.Application) -> None:
    """Останавливает Node API сервер при shutdown."""
    runner = app.get("node_api_runner")
    if runner:
        await runner.cleanup()
        logger.info("Node API server stopped")
