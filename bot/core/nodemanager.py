"""
NodeManager - Remote VPN node management via SSH.

This module handles all communication with remote VPN servers using asyncssh.
It manages Amnezia WireGuard peer operations on remote nodes.
"""
from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

import asyncssh

from ..database.models import Server, Device


logger = logging.getLogger(__name__)


@dataclass
class PeerInfo:
    """Information about a WireGuard peer."""
    public_key: str
    ip_address: str
    rx_bytes: int = 0
    tx_bytes: int = 0
    last_handshake: Optional[datetime] = None
    
    @classmethod
    def parse_from_awg_show(cls, output: str) -> list["PeerInfo"]:
        """Parse `awg show` output into PeerInfo objects."""
        peers = []
        current_peer = {}
        
        for line in output.splitlines():
            line = line.strip()
            if not line:
                continue
            
            if line.lower().startswith("peer:"):
                if current_peer.get("public_key"):
                    peers.append(cls(**current_peer))
                current_peer = {"public_key": line.split(":", 1)[1].strip()}
            elif line.lower().startswith("allowed ips:"):
                allowed_ips = line.split(":", 1)[1].strip()
                # Extract /32 IP
                for ip in allowed_ips.split(","):
                    ip = ip.strip()
                    if "/32" in ip:
                        current_peer["ip_address"] = ip.split("/")[0]
                        break
            elif line.lower().startswith("transfer:"):
                transfer_part = line.split(":", 1)[1].strip()
                # Parse "X.XX GiB received, Y.YY GiB sent"
                rx_match = re.search(r"([\d.]+)\s*([A-Za-z]+)\s+received", transfer_part, re.IGNORECASE)
                tx_match = re.search(r"([\d.]+)\s*([A-Za-z]+)\s+sent", transfer_part, re.IGNORECASE)
                
                if rx_match:
                    current_peer["rx_bytes"] = cls._parse_bytes(rx_match.group(1), rx_match.group(2))
                if tx_match:
                    current_peer["tx_bytes"] = cls._parse_bytes(tx_match.group(1), tx_match.group(2))
            elif line.lower().startswith("latest handshake:"):
                handshake_str = line.split(":", 1)[1].strip()
                if handshake_str and handshake_str.lower() not in ("none", "never", "n/a"):
                    current_peer["last_handshake"] = cls._parse_handshake(handshake_str)
        
        if current_peer.get("public_key"):
            peers.append(cls(**current_peer))
        
        return peers
    
    @staticmethod
    def _parse_bytes(value: str, unit: str) -> int:
        """Parse byte value with unit to bytes."""
        multipliers = {
            "B": 1,
            "BYTE": 1,
            "BYTES": 1,
            "KB": 1024,
            "KIB": 1024,
            "MB": 1024 ** 2,
            "MIB": 1024 ** 2,
            "GB": 1024 ** 3,
            "GIB": 1024 ** 3,
            "TB": 1024 ** 4,
            "TIB": 1024 ** 4,
        }
        multiplier = multipliers.get(unit.upper(), 1)
        return int(float(value) * multiplier)
    
    @staticmethod
    def _parse_handshake(handshake_str: str) -> Optional[datetime]:
        """Parse handshake time string (e.g., '5 minutes ago') to datetime."""
        now = datetime.now(timezone.utc)
        handshake_str = handshake_str.lower().replace("ago", "").strip()
        
        total_seconds = 0
        parts = [p.strip() for p in handshake_str.split(",")]
        
        for part in parts:
            match = re.match(r"(\d+)\s+(second|minute|hour|day|week)s?", part)
            if not match:
                continue
            
            amount = int(match.group(1))
            unit = match.group(2)
            
            if unit == "second":
                total_seconds += amount
            elif unit == "minute":
                total_seconds += amount * 60
            elif unit == "hour":
                total_seconds += amount * 3600
            elif unit == "day":
                total_seconds += amount * 86400
            elif unit == "week":
                total_seconds += amount * 7 * 86400
        
        if total_seconds > 0:
            from datetime import timedelta
            return now - timedelta(seconds=total_seconds)
        
        return None


@dataclass
class NodeConnection:
    """SSH connection to a VPN node."""
    host: str
    port: int
    username: str
    client_keys: list[Path] = field(default_factory=list)
    password: Optional[str] = None
    known_hosts: Optional[Path] = None
    connect_timeout: int = 10
    command_timeout: int = 30
    
    _connection: Optional[asyncssh.SSHClientConnection] = None
    
    async def connect(self) -> None:
        """Establish SSH connection."""
        if self._connection is not None and not self._connection.is_closed():
            return
        
        try:
            self._connection = await asyncssh.connect(
                host=self.host,
                port=self.port,
                username=self.username,
                client_keys=self.client_keys if self.client_keys else None,
                password=self.password,
                known_hosts=self.known_hosts,
                connect_timeout=self.connect_timeout,
            )
            logger.info(f"Connected to VPN node {self.username}@{self.host}:{self.port}")
        except Exception as e:
            logger.error(f"Failed to connect to VPN node {self.host}: {e}")
            raise
    
    async def disconnect(self) -> None:
        """Close SSH connection."""
        if self._connection and not self._connection.is_closed():
            self._connection.close()
            await self._connection.wait_closed()
            self._connection = None
            logger.info(f"Disconnected from VPN node {self.host}")
    
    async def run_command(self, command: str, input_data: Optional[str] = None) -> str:
        """Run a command on the remote node."""
        if self._connection is None or self._connection.is_closed():
            await self.connect()
        
        try:
            result = await asyncio.wait_for(
                self._connection.run(command, input=input_data),
                timeout=self.command_timeout,
            )
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() or f"Command failed with code {result.returncode}"
                raise RuntimeError(error_msg)
            
            return result.stdout.strip()
        except asyncio.TimeoutError:
            logger.error(f"Command timeout on {self.host}: {command}")
            raise RuntimeError(f"Command timeout after {self.command_timeout}s")
    
    async def check_connection(self) -> bool:
        """Check if SSH connection is alive."""
        if self._connection is None or self._connection.is_closed():
            return False
        
        try:
            await self.run_command("echo ping")
            return True
        except Exception:
            return False


class NodeManager:
    """
    Manages remote VPN nodes via SSH.
    
    Handles all Amnezia WireGuard operations on remote servers including:
    - Adding/removing peers
    - Getting peer statistics
    - Health checks
    - Configuration generation
    """
    
    def __init__(
        self,
        ssh_key_path: str | Path,
        ssh_user: str = "root",
        wg_interface: str = "wg0",
        awg_container: Optional[str] = None,
    ):
        """
        Initialize NodeManager.
        
        Args:
            ssh_key_path: Path to SSH private key for connecting to nodes
            ssh_user: Default SSH username (can be overridden per-server)
            wg_interface: WireGuard interface name on remote nodes
            awg_container: Docker container name for AWG (if using Docker)
        """
        self.ssh_key_path = Path(ssh_key_path).expanduser()
        self.ssh_user = ssh_user
        self.wg_interface = wg_interface
        self.awg_container = awg_container
        
        self._connections: dict[int, NodeConnection] = {}
        self._lock = asyncio.Lock()
        
        if not self.ssh_key_path.exists():
            logger.warning(f"SSH key not found at {self.ssh_key_path}")
    
    def _get_connection(self, server: Server) -> NodeConnection:
        """Get or create a connection for a server."""
        if server.id not in self._connections:
            self._connections[server.id] = NodeConnection(
                host=server.ip,
                port=server.ssh_port,
                username=server.ssh_user or self.ssh_user,
                client_keys=[self.ssh_key_path],
            )
        return self._connections[server.id]
    
    async def get_connection(self, server: Server) -> NodeConnection:
        """Get connection and ensure it's established."""
        async with self._lock:
            conn = self._get_connection(server)
            await conn.connect()
            return conn
    
    async def close_connection(self, server_id: int) -> None:
        """Close connection to a specific server."""
        async with self._lock:
            if server_id in self._connections:
                await self._connections[server_id].disconnect()
                del self._connections[server_id]
    
    async def close_all_connections(self) -> None:
        """Close all active connections."""
        async with self._lock:
            for conn in self._connections.values():
                await conn.disconnect()
            self._connections.clear()
    
    def _docker_cmd(self, cmd: list[str]) -> str:
        """Wrap command in docker exec if using container."""
        if self.awg_container:
            return f"docker exec -i {self.awg_container} {' '.join(cmd)}"
        return " ".join(cmd)
    
    async def check_awg_status(self, server: Server) -> bool:
        """Check if AWG is running on the server."""
        conn = await self.get_connection(server)
        
        if self.awg_container:
            cmd = self._docker_cmd(["awg", "show", self.wg_interface])
        else:
            cmd = f"awg show {self.wg_interface}"
        
        try:
            output = await conn.run_command(cmd)
            return "interface:" in output.lower()
        except Exception as e:
            logger.error(f"AWG status check failed for server {server.id}: {e}")
            return False
    
    async def get_peers(self, server: Server) -> list[PeerInfo]:
        """Get all peers from a server."""
        conn = await self.get_connection(server)
        
        if self.awg_container:
            cmd = self._docker_cmd(["awg", "show", self.wg_interface])
        else:
            cmd = f"awg show {self.wg_interface}"
        
        output = await conn.run_command(cmd)
        return PeerInfo.parse_from_awg_show(output)
    
    async def add_peer(
        self,
        server: Server,
        public_key: str,
        ip_address: str,
        psk_key: str,
    ) -> None:
        """Add a new peer to the server."""
        conn = await self.get_connection(server)
        
        if self.awg_container:
            # Use docker exec with stdin for PSK
            cmd = self._docker_cmd([
                "awg", "set", self.wg_interface,
                "peer", public_key,
                "preshared-key", "/dev/stdin",
                "allowed-ips", f"{ip_address}/32"
            ])
            await conn.run_command(cmd, input_data=psk_key)
        else:
            # Write PSK to temp file and use it
            temp_psk = f"/tmp/psk_{public_key[:8]}"
            await conn.run_command(f"echo '{psk_key}' > {temp_psk}")
            cmd = (
                f"awg set {self.wg_interface} "
                f"peer {public_key} "
                f"preshared-key {temp_psk} "
                f"allowed-ips {ip_address}/32"
            )
            await conn.run_command(cmd)
            await conn.run_command(f"rm -f {temp_psk}")
        
        logger.info(f"Added peer {public_key[:16]}... to server {server.id} with IP {ip_address}")
    
    async def remove_peer(self, server: Server, public_key: str) -> None:
        """Remove a peer from the server."""
        conn = await self.get_connection(server)
        
        if self.awg_container:
            cmd = self._docker_cmd([
                "awg", "set", self.wg_interface,
                "peer", public_key,
                "remove"
            ])
        else:
            cmd = f"awg set {self.wg_interface} peer {public_key} remove"
        
        await conn.run_command(cmd)
        logger.info(f"Removed peer {public_key[:16]}... from server {server.id}")
    
    async def get_server_public_key(self, server: Server) -> str:
        """Get the server's WireGuard public key."""
        conn = await self.get_connection(server)
        
        # Get private key first, then derive public key
        if self.awg_container:
            privkey_cmd = self._docker_cmd(["awg", "show", self.wg_interface, "-s"])
            output = await conn.run_command(privkey_cmd)
            
            # Parse private key from output
            for line in output.splitlines():
                if "private key:" in line.lower():
                    priv_key = line.split(":")[1].strip()
                    pubkey_cmd = self._docker_cmd(["awg", "pubkey"])
                    pub_key = await conn.run_command(pubkey_cmd, input_data=priv_key)
                    return pub_key.strip()
        else:
            # Read from file
            key_file = f"/etc/wireguard/{self.wg_interface}.conf"
            output = await conn.run_command(f"cat {key_file}")
            for line in output.splitlines():
                if line.strip().startswith("PrivateKey"):
                    priv_key = line.split("=")[1].strip()
                    # This approach won't work without the private key
                    # Better to read the public key directly
                    pass
            
            # Alternative: read public key from config or generate
            pub_key_file = f"/etc/wireguard/{self.wg_interface}.pub"
            try:
                return await conn.run_command(f"cat {pub_key_file}")
            except Exception:
                pass
        
        raise RuntimeError("Could not retrieve server public key")
    
    async def health_check(self, server: Server) -> dict[str, Any]:
        """
        Perform health check on a server.
        
        Returns dict with:
        - ssh_connected: bool
        - awg_running: bool
        - peer_count: int
        - total_rx: int
        - total_tx: int
        """
        result = {
            "ssh_connected": False,
            "awg_running": False,
            "peer_count": 0,
            "total_rx": 0,
            "total_tx": 0,
            "error": None,
        }
        
        try:
            conn = await self.get_connection(server)
            result["ssh_connected"] = await conn.check_connection()
            
            if not result["ssh_connected"]:
                result["error"] = "SSH connection failed"
                return result
            
            result["awg_running"] = await self.check_awg_status(server)
            
            if result["awg_running"]:
                peers = await self.get_peers(server)
                result["peer_count"] = len(peers)
                result["total_rx"] = sum(p.rx_bytes for p in peers)
                result["total_tx"] = sum(p.tx_bytes for p in peers)
            
        except Exception as e:
            result["error"] = str(e)
            logger.error(f"Health check failed for server {server.id}: {e}")
        
        return result
    
    async def sync_denylist(
        self,
        server: Server,
        denylist_cidrs: list[str],
        vpn_subnet: str,
    ) -> None:
        """Sync egress denylist to server."""
        conn = await self.get_connection(server)
        
        # Create nftables script
        script_lines = [
            "#!/usr/bin/nft -f",
            "table inet filter {",
            "  set awg_denylist {",
            "    type ipv4_addr",
            "    flags interval",
            "  }",
            "}",
        ]
        
        # Add denylist entries
        if denylist_cidrs:
            elements = ", ".join(denylist_cidrs)
            script_lines.append(f"add element inet filter awg_denylist {{ {elements} }}")
        
        script = "\n".join(script_lines)
        
        # Execute script
        await conn.run_command("nft -f -", input_data=script)
        logger.info(f"Synced {len(denylist_cidrs)} denylist entries to server {server.id}")
