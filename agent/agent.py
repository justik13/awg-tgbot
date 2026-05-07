"""
Легковесный агент для удалённых нод (Phase 4).

Агент запускается на VPS как systemd-сервис и:
1. Регистрируется на Main-сервере через HTTPS API
2. Отправляет heartbeat каждые 60 сек
3. Получает и применяет команды (add_peer, remove_peer, update_denylist)
4. Работает в режиме graceful degradation при недоступности Main

Архитектура: Node calls Main (polling-модель).
"""

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import aiohttp

# =============================================================================
# CONFIGURATION
# =============================================================================

AGENT_DIR = Path("/opt/amnezia/agent")
ENV_FILE = AGENT_DIR / "node.env"
DENYLIST_FILE = AGENT_DIR / "denylist.rules"
STATE_FILE = AGENT_DIR / "state.json"

HEARTBEAT_INTERVAL_SEC = 60
MAX_OFFLINE_MINUTES = 5
RETRY_BACKOFF_BASE_SEC = 5
MAX_RETRY_BACKOFF_SEC = 120

AWG_INTERFACE = "awg0"


# =============================================================================
# LOGGING
# =============================================================================

def log(level: str, message: str, *args) -> None:
    """Простое логирование в stdout/stderr."""
    timestamp = datetime.now(timezone.utc).isoformat()
    msg = message.format(*args) if args else message
    line = f"[{timestamp}] [{level}] {msg}"
    print(line, file=sys.stderr if level in ("ERROR", "WARNING") else sys.stdout)
    try:
        log_file = AGENT_DIR / "agent.log"
        with open(log_file, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def log_info(message: str, *args) -> None:
    log("INFO", message, *args)


def log_warning(message: str, *args) -> None:
    log("WARNING", message, *args)


def log_error(message: str, *args) -> None:
    log("ERROR", message, *args)


# =============================================================================
# STATE MANAGEMENT
# =============================================================================

class AgentState:
    def __init__(self):
        self.node_id: Optional[int] = None
        self.api_token: Optional[str] = None
        self.main_api_url: str = ""
        self.last_heartbeat_success: Optional[datetime] = None
        self.last_params_hash: str = ""
        self.denylist_version: str = "v0"
        self.status: str = "starting"
    
    def load(self) -> bool:
        """Загружает состояние из файла."""
        if not STATE_FILE.exists():
            return False
        
        try:
            with open(STATE_FILE, "r") as f:
                data = json.load(f)
            
            self.node_id = data.get("node_id")
            self.api_token = data.get("api_token")
            self.main_api_url = data.get("main_api_url", "")
            self.last_params_hash = data.get("last_params_hash", "")
            self.denylist_version = data.get("denylist_version", "v0")
            
            if data.get("last_heartbeat_success"):
                self.last_heartbeat_success = datetime.fromisoformat(data["last_heartbeat_success"])
            
            return True
        except Exception as e:
            log_error("Failed to load state: %s", e)
            return False
    
    def save(self) -> None:
        """Сохраняет состояние в файл."""
        try:
            AGENT_DIR.mkdir(parents=True, exist_ok=True)
            data = {
                "node_id": self.node_id,
                "api_token": self.api_token,
                "main_api_url": self.main_api_url,
                "last_params_hash": self.last_params_hash,
                "denylist_version": self.denylist_version,
                "status": self.status,
            }
            if self.last_heartbeat_success:
                data["last_heartbeat_success"] = self.last_heartbeat_success.isoformat()
            
            with open(STATE_FILE, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            log_error("Failed to save state: %s", e)


# =============================================================================
# AWG INTERFACE READING
# =============================================================================

def read_awg0_params() -> dict[str, Any]:
    """
    Читает параметры интерфейса awg0 через `awg show awg0`.
    Только read-only операция.
    """
    result = {
        "port": 0,
        "s1": "", "s2": "", "s3": "", "s4": "",
        "h1": "", "h2": "", "h3": "", "h4": "",
    }
    
    try:
        proc = subprocess.run(
            ["awg", "show", AWG_INTERFACE],
            capture_output=True,
            text=True,
            timeout=10,
        )
        
        if proc.returncode != 0:
            log_warning("awg show failed: %s", proc.stderr.strip())
            return result
        
        output = proc.stdout
        
        # Парсим ListenPort
        for line in output.split("\n"):
            line = line.strip()
            if line.startswith("listen_port:"):
                try:
                    result["port"] = int(line.split(":")[1].strip())
                except ValueError:
                    pass
            
            # Парсим параметры обфускации
            elif line.startswith("s1:"):
                result["s1"] = line.split(":", 1)[1].strip()
            elif line.startswith("s2:"):
                result["s2"] = line.split(":", 1)[1].strip()
            elif line.startswith("s3:"):
                result["s3"] = line.split(":", 1)[1].strip()
            elif line.startswith("s4:"):
                result["s4"] = line.split(":", 1)[1].strip()
            elif line.startswith("h1:"):
                result["h1"] = line.split(":", 1)[1].strip()
            elif line.startswith("h2:"):
                result["h2"] = line.split(":", 1)[1].strip()
            elif line.startswith("h3:"):
                result["h3"] = line.split(":", 1)[1].strip()
            elif line.startswith("h4:"):
                result["h4"] = line.split(":", 1)[1].strip()
    
    except FileNotFoundError:
        log_warning("awg command not found")
    except subprocess.TimeoutExpired:
        log_warning("awg show timed out")
    except Exception as e:
        log_error("Failed to read awg0 params: %s", e)
    
    return result


def compute_params_hash(params: dict[str, Any]) -> str:
    """Вычисляет SHA256 хэш параметров."""
    params_str = (
        f"{params['port']}"
        f"{params['s1']}{params['s2']}{params['s3']}{params['s4']}"
        f"{params['h1']}{params['h2']}{params['h3']}{params['h4']}"
    )
    return hashlib.sha256(params_str.encode()).hexdigest()


def count_active_peers() -> int:
    """Считает количество активных пиров в awg0."""
    try:
        proc = subprocess.run(
            ["awg", "show", AWG_INTERFACE, "latest-handshakes"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        
        if proc.returncode != 0:
            return 0
        
        # Считаем строки с пирами (каждая строка - пир)
        lines = [l.strip() for l in proc.stdout.strip().split("\n") if l.strip()]
        return len(lines)
    
    except Exception:
        return 0


# =============================================================================
# COMMAND APPLICATION
# =============================================================================

def apply_add_peer(payload: dict[str, Any]) -> bool:
    """
    Идемпотентно добавляет пира.
    payload: { "public_key": str, "allowed_ips": str, "preshared_key": str }
    """
    public_key = payload.get("public_key")
    allowed_ips = payload.get("allowed_ips", "0.0.0.0/0")
    preshared_key = payload.get("preshared_key", "")
    
    if not public_key:
        log_error("add_peer: missing public_key")
        return False
    
    try:
        # Сначала удаляем пира если существует (для идемпотентности)
        subprocess.run(
            ["awg", "set", AWG_INTERFACE, "peer", public_key, "remove"],
            capture_output=True,
            timeout=10,
            check=False,
        )
        
        # Добавляем пира
        cmd = ["awg", "set", AWG_INTERFACE, "peer", public_key]
        cmd.extend(["allowed-ips", allowed_ips])
        
        if preshared_key:
            # Записываем PSK во временный файл
            psk_file = AGENT_DIR / f"psk_{public_key[:8]}.txt"
            with open(psk_file, "w") as f:
                f.write(preshared_key)
            cmd.extend(["preshared-key", str(psk_file)])
        
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        
        if proc.returncode != 0:
            log_error("add_peer failed: %s", proc.stderr.strip())
            return False
        
        log_info("add_peer: added peer %s", public_key[:16])
        return True
    
    except Exception as e:
        log_error("add_peer exception: %s", e)
        return False


def apply_remove_peer(payload: dict[str, Any]) -> bool:
    """
    Идемпотентно удаляет пира.
    payload: { "public_key": str }
    """
    public_key = payload.get("public_key")
    
    if not public_key:
        log_error("remove_peer: missing public_key")
        return False
    
    try:
        proc = subprocess.run(
            ["awg", "set", AWG_INTERFACE, "peer", public_key, "remove"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        
        # Возвращаем успех даже если пир не найден (идемпотентность)
        log_info("remove_peer: removed peer %s", public_key[:16])
        return True
    
    except Exception as e:
        log_error("remove_peer exception: %s", e)
        return False


def apply_update_denylist(payload: dict[str, Any]) -> bool:
    """
    Обновляет denylist правила.
    payload: { "rules": [str], "version": str }
    """
    rules = payload.get("rules", [])
    version = payload.get("version", "v0")
    
    try:
        # Записываем новые правила
        with open(DENYLIST_FILE, "w") as f:
            for rule in rules:
                f.write(rule + "\n")
        
        # Здесь должен быть вызов скрипта применения правил (nftables/iptables)
        # Для MVP просто логируем
        log_info("update_denylist: applied %d rules (version=%s)", len(rules), version)
        return True
    
    except Exception as e:
        log_error("update_denylist exception: %s", e)
        return False


def apply_command(command: dict[str, Any]) -> bool:
    """Применяет команду от Main."""
    action = command.get("action")
    payload = command.get("payload", {})
    
    log_info("Applying command: action=%s", action)
    
    if action == "add_peer":
        return apply_add_peer(payload)
    elif action == "remove_peer":
        return apply_remove_peer(payload)
    elif action == "update_denylist":
        return apply_update_denylist(payload)
    else:
        log_warning("Unknown command action: %s", action)
        return False


# =============================================================================
# REGISTRATION & HEARTBEAT
# =============================================================================

async def register_node(session: aiohttp.ClientSession, main_url: str, params: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Регистрирует ноду на Main-сервере."""
    url = main_url.rstrip("/") + "/api/v1/node/register"
    
    hostname = os.environ.get("HOSTNAME", "unknown")
    
    payload = {
        "ip": params.get("public_ip", "0.0.0.0"),
        "port": params["port"],
        "s1": params["s1"],
        "s2": params["s2"],
        "s3": params["s3"],
        "s4": params["s4"],
        "h1": params["h1"],
        "h2": params["h2"],
        "h3": params["h3"],
        "h4": params["h4"],
        "hostname": hostname,
    }
    
    try:
        async with session.post(url, json=payload) as resp:
            if resp.status == 201 or resp.status == 200:
                data = await resp.json()
                log_info("Node registered: node_id=%s", data.get("node_id"))
                return data
            else:
                error_text = await resp.text()
                log_error("Registration failed: status=%d %s", resp.status, error_text)
                return None
    except Exception as e:
        log_error("Registration exception: %s", e)
        return None


async def send_heartbeat(
    session: aiohttp.ClientSession,
    main_url: str,
    api_token: str,
    node_id: int,
    status: str,
    params_hash: str,
    denylist_version: str,
    active_peers: int,
) -> Optional[dict[str, Any]]:
    """Отправляет heartbeat на Main-сервер."""
    url = main_url.rstrip("/") + "/api/v1/node/heartbeat"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "node_id": node_id,
        "status": status,
        "params_hash": params_hash,
        "denylist_version": denylist_version,
        "active_peers": active_peers,
    }
    
    try:
        async with session.post(url, json=payload, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data
            else:
                error_text = await resp.text()
                log_warning("Heartbeat failed: status=%d %s", resp.status, error_text)
                return None
    except Exception as e:
        log_warning("Heartbeat exception: %s", e)
        return None


# =============================================================================
# MAIN LOOP
# =============================================================================

async def run_agent() -> None:
    """Основной цикл агента."""
    state = AgentState()
    state.load()
    
    # Читаем параметры awg0
    awg_params = read_awg0_params()
    params_hash = compute_params_hash(awg_params)
    
    # Если нет регистрации — ждём ручного запуска registration
    if not state.api_token or not state.main_api_url:
        log_warning("No registration found. Please run registration first.")
        log_warning("Set MAIN_API_URL in environment and restart, or create node.env manually.")
        
        # Проверяем env переменную для автоматической регистрации
        main_url = os.environ.get("MAIN_API_URL", "")
        if main_url:
            log_info("MAIN_API_URL found, attempting auto-registration...")
            state.main_api_url = main_url
            
            async with aiohttp.ClientSession() as session:
                result = await register_node(session, main_url, awg_params)
                
                if result:
                    state.node_id = result.get("node_id")
                    state.api_token = result.get("api_token")
                    state.save()
                    log_info("Auto-registration successful!")
                else:
                    log_error("Auto-registration failed. Exiting.")
                    sys.exit(1)
        else:
            # Ждём пока админ создаст node.env вручную
            log_info("Waiting for node.env to be created...")
            while not state.api_token:
                await asyncio.sleep(10)
                state.load()
    
    # Основной цикл heartbeat
    retry_backoff = RETRY_BACKOFF_BASE_SEC
    consecutive_failures = 0
    
    log_info("Starting heartbeat loop (interval=%ds)", HEARTBEAT_INTERVAL_SEC)
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # Обновляем параметры
                awg_params = read_awg0_params()
                current_params_hash = compute_params_hash(awg_params)
                active_peers = count_active_peers()
                
                # Определяем статус
                now = datetime.now(timezone.utc)
                if state.last_heartbeat_success:
                    offline_minutes = (now - state.last_heartbeat_success).total_seconds() / 60
                    if offline_minutes > MAX_OFFLINE_MINUTES:
                        state.status = "degraded"
                    else:
                        state.status = "ready"
                else:
                    state.status = "starting"
                
                # Отправляем heartbeat
                response = await send_heartbeat(
                    session=session,
                    main_url=state.main_api_url,
                    api_token=state.api_token,
                    node_id=state.node_id,
                    status=state.status,
                    params_hash=current_params_hash,
                    denylist_version=state.denylist_version,
                    active_peers=active_peers,
                )
                
                if response:
                    # Успех
                    state.last_heartbeat_success = now
                    state.last_params_hash = current_params_hash
                    consecutive_failures = 0
                    retry_backoff = RETRY_BACKOFF_BASE_SEC
                    
                    # Применяем команды
                    commands = response.get("commands", [])
                    if commands:
                        log_info("Received %d commands", len(commands))
                        for cmd in commands:
                            success = apply_command(cmd)
                            if not success:
                                log_warning("Command failed: %s", cmd.get("action"))
                    
                    state.save()
                    log_info(
                        "Heartbeat OK: status=%s peers=%d commands=%d",
                        state.status, active_peers, len(commands),
                    )
                else:
                    # Ошибка
                    consecutive_failures += 1
                    log_warning(
                        "Heartbeat failed (%d consecutive failures, backoff=%ds)",
                        consecutive_failures, retry_backoff,
                    )
                    
                    # Graceful degradation: продолжаем работать с последним состоянием
                    if consecutive_failures >= 5:
                        state.status = "offline"
                        state.save()
                    
                    # Ждём перед retry
                    await asyncio.sleep(retry_backoff)
                    retry_backoff = min(retry_backoff * 2, MAX_RETRY_BACKOFF_SEC)
                    continue
                
            except Exception as e:
                log_error("Heartbeat loop exception: %s", e)
                consecutive_failures += 1
            
            # Ждём до следующего heartbeat
            await asyncio.sleep(HEARTBEAT_INTERVAL_SEC)


def main() -> None:
    """Точка входа."""
    log_info("Agent starting (pid=%d)", os.getpid())
    log_info("Agent dir: %s", AGENT_DIR)
    
    # Создаём директорию если нет
    AGENT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Запускаем основной цикл
    try:
        asyncio.run(run_agent())
    except KeyboardInterrupt:
        log_info("Agent stopped by user")
    except Exception as e:
        log_error("Fatal error: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
