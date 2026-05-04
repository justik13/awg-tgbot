"""
Amnezia WireGuard configuration generator.

Generates client configurations with all AWG-specific obfuscation parameters.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Optional

from ..database.models import Server


@dataclass
class AWGSettings:
    """Amnezia WireGuard obfuscation settings."""
    
    # Jumble settings
    jc: str = "1000"  # Jumble Count
    jmin: str = "1000"  # Jumble Min
    jmax: str = "2000"  # Jumble Max
    
    # Split settings
    s1: str = "0"  # Split 1
    s2: str = "0"  # Split 2
    s3: str = "0"  # Split 3
    s4: str = "0"  # Split 4
    
    # Hole settings
    h1: str = "1"  # Hole 1
    h2: str = "2"  # Hole 2
    h3: str = "3"  # Hole 3
    h4: str = "4"  # Hole 4
    
    # Init packet settings
    i1: str = "1"  # Init 1
    i2: str = "2"  # Init 2
    i3: str = "3"  # Init 3
    i4: str = "4"  # Init 4
    i5: str = "5"  # Init 5
    
    @classmethod
    def from_server(cls, server: Server) -> "AWGSettings":
        """Create AWGSettings from server's awg_settings JSON."""
        settings = server.awg_settings if isinstance(server.awg_settings, dict) else {}
        if isinstance(settings, str):
            settings = json.loads(settings)
        
        return cls(
            jc=settings.get("Jc", cls.jc),
            jmin=settings.get("Jmin", cls.jmin),
            jmax=settings.get("Jmax", cls.jmax),
            s1=settings.get("S1", cls.s1),
            s2=settings.get("S2", cls.s2),
            s3=settings.get("S3", cls.s3),
            s4=settings.get("S4", cls.s4),
            h1=settings.get("H1", cls.h1),
            h2=settings.get("H2", cls.h2),
            h3=settings.get("H3", cls.h3),
            h4=settings.get("H4", cls.h4),
            i1=settings.get("I1", cls.i1),
            i2=settings.get("I2", cls.i2),
            i3=settings.get("I3", cls.i3),
            i4=settings.get("I4", cls.i4),
            i5=settings.get("I5", cls.i5),
        )
    
    def to_dict(self) -> dict[str, str]:
        """Convert to dictionary for config generation."""
        return {
            "Jc": self.jc,
            "Jmin": self.jmin,
            "Jmax": self.jmax,
            "S1": self.s1,
            "S2": self.s2,
            "S3": self.s3,
            "S4": self.s4,
            "H1": self.h1,
            "H2": self.h2,
            "H3": self.h3,
            "H4": self.h4,
            "I1": self.i1,
            "I2": self.i2,
            "I3": self.i3,
            "I4": self.i4,
            "I5": self.i5,
        }
    
    def to_config_lines(self) -> str:
        """Convert to WireGuard config file lines."""
        lines = []
        for key, value in self.to_dict().items():
            if value and str(value).strip():
                lines.append(f"{key} = {value}")
        return "\n".join(lines)


@dataclass
class ClientConfig:
    """Complete WireGuard client configuration."""
    
    # Interface settings
    private_key: str
    ip_address: str
    
    # Peer (server) settings - required, no defaults
    server_public_key: str
    server_psk_key: str
    server_endpoint: str  # host:port
    
    # Optional settings with defaults
    dns_servers: list[str] = field(default_factory=lambda: ["8.8.8.8", "8.8.4.4"])
    mtu: int = 1420
    awg_settings: AWGSettings = field(default_factory=AWGSettings)
    allowed_ips: list[str] = field(default_factory=lambda: ["0.0.0.0/0"])
    persistent_keepalive: int = 25
    profile_name: Optional[str] = None
    
    def generate_wg_conf(self) -> str:
        """Generate standard WireGuard config file content."""
        lines = [
            "[Interface]",
            f"Address = {self.ip_address}/32",
            f"DNS = {', '.join(self.dns_servers)}",
            f"PrivateKey = {self.private_key}",
            f"MTU = {self.mtu}",
        ]
        
        # Add AWG-specific settings
        awg_lines = self.awg_settings.to_config_lines()
        if awg_lines:
            lines.append(awg_lines)
        
        lines.extend([
            "",
            "[Peer]",
            f"PublicKey = {self.server_public_key}",
            f"PresharedKey = {self.server_psk_key}",
            f"AllowedIPs = {', '.join(self.allowed_ips)}",
            f"Endpoint = {self.server_endpoint}",
            f"PersistentKeepalive = {self.persistent_keepalive}",
        ])
        
        return "\n".join(lines) + "\n"
    
    def generate_amnezia_json(self) -> dict[str, Any]:
        """
        Generate Amnezia VPN app JSON configuration.
        
        This is the format used by Amnezia VPN clients for importing configs.
        """
        host, port = self._parse_endpoint()
        subnet_address = ".".join(self.ip_address.split(".")[:3]) + ".0"
        
        last_config = {
            **self.awg_settings.to_dict(),
            "allowed_ips": self.allowed_ips,
            "clientId": self.private_key,  # Note: Amnezia uses private key as clientId
            "client_ip": self.ip_address,
            "client_priv_key": self.private_key,
            "config": self.generate_wg_conf(),
            "hostName": host,
            "mtu": self.mtu,
            "persistent_keep_alive": self.persistent_keepalive,
            "port": port,
            "psk_key": self.server_psk_key,
            "server_pub_key": self.server_public_key,
        }
        
        profile_name = self.profile_name or "VPN Profile"
        
        return {
            "containers": [
                {
                    "awg": {
                        **self.awg_settings.to_dict(),
                        "last_config": json.dumps(last_config, ensure_ascii=False, indent=4),
                        "port": str(port),
                        "protocol_version": "amnezia-wg-1.0",
                        "subnet_address": subnet_address,
                        "transport_proto": "udp",
                    },
                    "container": "amnezia-wg",
                }
            ],
            "defaultContainer": "amnezia-wg",
            "description": profile_name,
            "dns1": self.dns_servers[0] if self.dns_servers else "8.8.8.8",
            "dns2": self.dns_servers[1] if len(self.dns_servers) > 1 else "8.8.4.4",
            "hostName": host,
            "nameOverriddenByUser": True,
        }
    
    def _parse_endpoint(self) -> tuple[str, int]:
        """Parse endpoint string into host and port."""
        if ":" in self.server_endpoint:
            parts = self.server_endpoint.rsplit(":", 1)
            return parts[0], int(parts[1])
        return self.server_endpoint, 51820  # Default WG port
    
    def generate_qr_data(self) -> str:
        """Generate data for QR code (standard wg:// URL)."""
        return f"wg://{self.generate_wg_conf()}"


def build_client_config(
    server: Server,
    device_private_key: str,
    device_psk_key: str,
    device_ip: str,
    device_name: Optional[str] = None,
    dns_servers: Optional[list[str]] = None,
) -> ClientConfig:
    """
    Build a complete client configuration for a device.
    
    Args:
        server: The VPN server to connect to
        device_private_key: Device's WireGuard private key
        device_psk_key: Preshared key for the connection
        device_ip: IP address assigned to the device
        device_name: Optional name for the device/profile
        dns_servers: Optional custom DNS servers
    
    Returns:
        Complete ClientConfig object
    """
    awg_settings = AWGSettings.from_server(server)
    
    # Parse server endpoint
    server_endpoint = server.ip
    if ":" not in server_endpoint:
        # Add default port if not specified
        server_endpoint = f"{server_endpoint}:51820"
    
    return ClientConfig(
        private_key=device_private_key,
        ip_address=device_ip,
        dns_servers=dns_servers or ["8.8.8.8", "1.1.1.1"],
        awg_settings=awg_settings,
        server_public_key=server.server_public_key,
        server_psk_key=device_psk_key,
        server_endpoint=server_endpoint,
        profile_name=device_name,
    )
