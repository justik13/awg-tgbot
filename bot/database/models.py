"""
SQLAlchemy ORM models for awg-tgbot v2.

This module defines the database schema for the distributed VPN management system.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    UniqueConstraint,
    Index,
    event,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    validates,
)


def utc_now() -> datetime:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    """Base class for all ORM models."""
    
    pass


class User(Base):
    """Telegram user model."""
    
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    telegram_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False, index=True)
    status: Mapped[str] = mapped_column(
        String(20), 
        default="active", 
        nullable=False
    )  # active, banned, deleted
    first_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=utc_now, 
        onupdate=utc_now, 
        nullable=False
    )
    
    # Relationships
    subscriptions: Mapped[list["Subscription"]] = relationship(
        "Subscription", 
        back_populates="user", 
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("idx_users_status", "status"),
        Index("idx_users_created_at", "created_at"),
    )
    
    @validates("status")
    def validate_status(self, key: str, value: str) -> str:
        valid_statuses = {"active", "banned", "deleted"}
        if value not in valid_statuses:
            raise ValueError(f"Invalid status: {value}. Must be one of {valid_statuses}")
        return value
    
    def is_active(self) -> bool:
        return self.status == "active"


class Subscription(Base):
    """User subscription model. One subscription grants 3 device slots."""
    
    __tablename__ = "subscriptions"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("users.id", ondelete="CASCADE"), 
        nullable=False, 
        index=True
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    max_devices: Mapped[int] = mapped_column(Integer, default=3, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=utc_now, 
        onupdate=utc_now, 
        nullable=False
    )
    
    # Relationships
    user: Mapped["User"] = relationship("User", back_populates="subscriptions")
    devices: Mapped[list["Device"]] = relationship(
        "Device", 
        back_populates="subscription", 
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("idx_subscriptions_user_expires", "user_id", "expires_at"),
    )
    
    def is_active(self) -> bool:
        return self.expires_at > utc_now()
    
    def device_count(self) -> int:
        return len([d for d in self.devices if d.is_active()])
    
    def can_add_device(self) -> bool:
        return self.is_active() and self.device_count() < self.max_devices


class Server(Base):
    """VPN server node model."""
    
    __tablename__ = "servers"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    ip: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    country_code: Mapped[str] = mapped_column(String(2), nullable=False, index=True)  # ISO 3166-1 alpha-2
    city: Mapped[str] = mapped_column(String(128), nullable=False)
    
    # SSH connection settings
    ssh_port: Mapped[int] = mapped_column(Integer, default=22, nullable=False)
    ssh_user: Mapped[str] = mapped_column(String(64), default="root", nullable=False)
    
    # AWG settings stored as JSON
    # Contains: Jc, Jmin, Jmax, S1, S2, H1, H2, H3, H4, I1-I5, etc.
    awg_settings: Mapped[dict[str, Any]] = mapped_column(
        Text, 
        default=lambda: json.dumps({}), 
        nullable=False
    )
    
    # Server's WireGuard public key
    server_public_key: Mapped[str] = mapped_column(String(64), nullable=False)
    
    # VPN subnet for this server (e.g., "10.0.1")
    vpn_subnet_prefix: Mapped[str] = mapped_column(String(16), nullable=False)
    
    # Server status
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)
    last_health_check: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=utc_now, 
        onupdate=utc_now, 
        nullable=False
    )
    
    # Relationships
    devices: Mapped[list["Device"]] = relationship(
        "Device", 
        back_populates="server", 
        cascade="all, delete-orphan"
    )
    stats: Mapped[list["ServerStats"]] = relationship(
        "ServerStats", 
        back_populates="server", 
        cascade="all, delete-orphan"
    )
    
    __table_args__ = (
        Index("idx_servers_country_active", "country_code", "is_active"),
    )
    
    @validates("country_code")
    def validate_country_code(self, key: str, value: str) -> str:
        if len(value) != 2 or not value.isalpha():
            raise ValueError("Country code must be 2 letters (ISO 3166-1 alpha-2)")
        return value.upper()
    
    def get_awg_setting(self, key: str, default: Any = None) -> Any:
        """Get a specific AWG setting value."""
        settings = self.awg_settings if isinstance(self.awg_settings, dict) else json.loads(self.awg_settings)
        return settings.get(key, default)
    
    def set_awg_setting(self, key: str, value: Any) -> None:
        """Set a specific AWG setting value."""
        if isinstance(self.awg_settings, str):
            settings = json.loads(self.awg_settings)
        else:
            settings = self.awg_settings or {}
        settings[key] = value
        self.awg_settings = settings


class Device(Base):
    """VPN device configuration model."""
    
    __tablename__ = "devices"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    subscription_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("subscriptions.id", ondelete="CASCADE"), 
        nullable=False, 
        index=True
    )
    server_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("servers.id", ondelete="RESTRICT"), 
        nullable=False, 
        index=True
    )
    
    # WireGuard keys
    pub_key: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)
    # PrivateKey клиента не хранится в БД
    psk_key: Mapped[str] = mapped_column(String(64), nullable=False)  # Should be encrypted at rest
    
    # Network settings
    ip_address: Mapped[str] = mapped_column(String(16), nullable=False)
    
    # Configuration metadata
    conf_name: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # Device status
    state: Mapped[str] = mapped_column(
        String(20), 
        default="active", 
        nullable=False
    )  # active, revoked, expired
    state_updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Traffic statistics (cached from server)
    rx_bytes_total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tx_bytes_total: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    last_activity: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=utc_now, 
        onupdate=utc_now, 
        nullable=False
    )
    
    # Relationships
    subscription: Mapped["Subscription"] = relationship("Subscription", back_populates="devices")
    server: Mapped["Server"] = relationship("Server", back_populates="devices")
    
    __table_args__ = (
        UniqueConstraint("subscription_id", "conf_name", name="uq_subscription_conf_name"),
        Index("idx_devices_state", "state"),
    )
    
    @validates("state")
    def validate_state(self, key: str, value: str) -> str:
        valid_states = {"active", "revoked", "expired"}
        if value not in valid_states:
            raise ValueError(f"Invalid state: {value}. Must be one of {valid_states}")
        return value
    
    def is_active(self) -> bool:
        return self.state == "active"


class ServerStats(Base):
    """Server statistics history model."""
    
    __tablename__ = "server_stats"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    server_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("servers.id", ondelete="CASCADE"), 
        nullable=False, 
        index=True
    )
    
    total_peers: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    rx_bytes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    tx_bytes: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    
    recorded_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, nullable=False, index=True)
    
    # Relationships
    server: Mapped["Server"] = relationship("Server", back_populates="stats")
    
    __table_args__ = (
        Index("idx_server_stats_server_recorded", "server_id", "recorded_at"),
    )


class Payment(Base):
    """Payment transaction model (migrated from v1)."""
    
    __tablename__ = "payments"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        Integer, 
        ForeignKey("users.id", ondelete="CASCADE"), 
        nullable=False, 
        index=True
    )
    
    # Payment identifiers
    telegram_payment_charge_id: Mapped[Optional[str]] = mapped_column(String(128), unique=True, nullable=True)
    provider_payment_charge_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    
    # Payment details
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(3), default="XTR", nullable=False)
    payment_method: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    
    # Status tracking
    status: Mapped[str] = mapped_column(String(32), default="received", nullable=False)
    last_provision_status: Mapped[str] = mapped_column(String(32), default="payment_received", nullable=False)
    
    # Provisioning
    subscription_days: Mapped[int] = mapped_column(Integer, nullable=True)
    provisioned_until: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Error handling
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    
    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utc_now, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, 
        default=utc_now, 
        onupdate=utc_now, 
        nullable=False
    )
    
    __table_args__ = (
        Index("idx_payments_status", "status"),
        Index("idx_payments_user_created", "user_id", "created_at"),
    )
