"""
Data access layer for awg-tgbot v2.

Provides repository classes for database operations.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Optional, Sequence, Any
from sqlalchemy import select, func, and_, or_, update, delete
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from .models import (
    Base,
    User,
    Subscription,
    Server,
    Device,
    ServerStats,
    Payment,
    utc_now,
)


class UserRepository:
    """Repository for User operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_by_telegram_id(self, telegram_id: int) -> Optional[User]:
        """Get user by Telegram ID."""
        result = await self.session.execute(
            select(User).where(User.telegram_id == telegram_id)
        )
        return result.scalar_one_or_none()
    
    async def create(
        self, 
        telegram_id: int, 
        first_name: Optional[str] = None,
        username: Optional[str] = None,
    ) -> User:
        """Create a new user."""
        user = User(
            telegram_id=telegram_id,
            first_name=first_name,
            username=username,
        )
        self.session.add(user)
        await self.session.flush()
        return user
    
    async def get_or_create(
        self, 
        telegram_id: int, 
        first_name: Optional[str] = None,
        username: Optional[str] = None,
    ) -> User:
        """Get existing user or create new one."""
        user = await self.get_by_telegram_id(telegram_id)
        if user is None:
            user = await self.create(telegram_id, first_name, username)
        elif first_name or username:
            # Update user info if provided
            if first_name:
                user.first_name = first_name
            if username:
                user.username = username
        return user
    
    async def ban(self, user: User) -> None:
        """Ban a user."""
        user.status = "banned"
        user.updated_at = utc_now()
    
    async def unban(self, user: User) -> None:
        """Unban a user."""
        user.status = "active"
        user.updated_at = utc_now()
    
    async def delete(self, user: User) -> None:
        """Mark user as deleted."""
        user.status = "deleted"
        user.updated_at = utc_now()
    
    async def count_active_users(self) -> int:
        """Count active users."""
        result = await self.session.execute(
            select(func.count()).where(User.status == "active")
        )
        return result.scalar() or 0
    
    async def get_all_active(self, limit: int = 1000) -> Sequence[User]:
        """Get all active users."""
        result = await self.session.execute(
            select(User)
            .where(User.status == "active")
            .limit(limit)
        )
        return result.scalars().all()


class SubscriptionRepository:
    """Repository for Subscription operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_user_subscription(self, user_id: int) -> Optional[Subscription]:
        """Get the latest active subscription for a user."""
        result = await self.session.execute(
            select(Subscription)
            .where(Subscription.user_id == user_id)
            .order_by(Subscription.expires_at.desc())
        )
        return result.scalar_one_or_none()
    
    async def create(
        self, 
        user_id: int, 
        expires_at: datetime,
        max_devices: int = 3,
    ) -> Subscription:
        """Create a new subscription."""
        subscription = Subscription(
            user_id=user_id,
            expires_at=expires_at,
            max_devices=max_devices,
        )
        self.session.add(subscription)
        await self.session.flush()
        return subscription
    
    async def extend(
        self, 
        subscription: Subscription, 
        days: int,
    ) -> Subscription:
        """Extend subscription by given days."""
        now = utc_now()
        # If already expired, start from now; otherwise extend from current expiry
        if subscription.expires_at < now:
            subscription.expires_at = now + timedelta(days=days)
        else:
            subscription.expires_at = subscription.expires_at + timedelta(days=days)
        
        subscription.updated_at = now
        return subscription
    
    async def get_expiring_soon(self, hours: int = 24) -> Sequence[Subscription]:
        """Get subscriptions expiring within specified hours."""
        threshold = utc_now() + timedelta(hours=hours)
        result = await self.session.execute(
            select(Subscription)
            .where(
                and_(
                    Subscription.expires_at > utc_now(),
                    Subscription.expires_at <= threshold,
                )
            )
            .options(selectinload(Subscription.user))
        )
        return result.scalars().all()
    
    async def get_expired(self) -> Sequence[Subscription]:
        """Get all expired subscriptions."""
        result = await self.session.execute(
            select(Subscription)
            .where(Subscription.expires_at < utc_now())
        )
        return result.scalars().all()


class ServerRepository:
    """Repository for Server (VPN node) operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_by_id(self, server_id: int) -> Optional[Server]:
        """Get server by ID."""
        result = await self.session.execute(
            select(Server).where(Server.id == server_id)
        )
        return result.scalar_one_or_none()
    
    async def get_active_servers(self) -> Sequence[Server]:
        """Get all active servers."""
        result = await self.session.execute(
            select(Server).where(Server.is_active == True)
        )
        return result.scalars().all()
    
    async def get_servers_by_country(self, country_code: str) -> Sequence[Server]:
        """Get active servers in a specific country."""
        result = await self.session.execute(
            select(Server)
            .where(
                and_(
                    Server.country_code == country_code.upper(),
                    Server.is_active == True,
                )
            )
        )
        return result.scalars().all()
    
    async def get_available_countries(self) -> Sequence[tuple[str, str]]:
        """Get list of available countries with active servers."""
        result = await self.session.execute(
            select(Server.country_code, Server.city)
            .where(Server.is_active == True)
            .distinct()
            .order_by(Server.country_code)
        )
        return result.all()
    
    async def create(
        self,
        name: str,
        ip: str,
        country_code: str,
        city: str,
        server_public_key: str,
        vpn_subnet_prefix: str,
        ssh_port: int = 22,
        ssh_user: str = "root",
        awg_settings: Optional[dict[str, Any]] = None,
    ) -> Server:
        """Create a new VPN server."""
        server = Server(
            name=name,
            ip=ip,
            country_code=country_code.upper(),
            city=city,
            ssh_port=ssh_port,
            ssh_user=ssh_user,
            server_public_key=server_public_key,
            vpn_subnet_prefix=vpn_subnet_prefix,
            awg_settings=awg_settings or {},
        )
        self.session.add(server)
        await self.session.flush()
        return server
    
    async def deactivate(self, server: Server) -> None:
        """Deactivate a server."""
        server.is_active = False
        server.updated_at = utc_now()
    
    async def activate(self, server: Server) -> None:
        """Activate a server."""
        server.is_active = True
        server.updated_at = utc_now()
    
    async def update_health_check(self, server: Server) -> None:
        """Update last health check timestamp."""
        server.last_health_check = utc_now()
        server.updated_at = utc_now()
    
    async def count_servers(self) -> int:
        """Count total servers."""
        result = await self.session.execute(select(func.count()).select_from(Server))
        return result.scalar() or 0
    
    async def count_active_servers(self) -> int:
        """Count active servers."""
        result = await self.session.execute(
            select(func.count()).where(Server.is_active == True)
        )
        return result.scalar() or 0


class DeviceRepository:
    """Repository for Device operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_by_id(self, device_id: int) -> Optional[Device]:
        """Get device by ID."""
        result = await self.session.execute(
            select(Device).where(Device.id == device_id)
        )
        return result.scalar_one_or_none()
    
    async def get_by_pub_key(self, pub_key: str) -> Optional[Device]:
        """Get device by public key."""
        result = await self.session.execute(
            select(Device).where(Device.pub_key == pub_key)
        )
        return result.scalar_one_or_none()
    
    async def get_subscription_devices(
        self, 
        subscription_id: int,
        active_only: bool = True,
    ) -> Sequence[Device]:
        """Get all devices for a subscription."""
        query = select(Device).where(Device.subscription_id == subscription_id)
        if active_only:
            query = query.where(Device.state == "active")
        result = await self.session.execute(query)
        return result.scalars().all()
    
    async def create(
        self,
        subscription_id: int,
        server_id: int,
        pub_key: str,
        priv_key: str,
        psk_key: str,
        ip_address: str,
        conf_name: str,
    ) -> Device:
        """Create a new device configuration."""
        device = Device(
            subscription_id=subscription_id,
            server_id=server_id,
            pub_key=pub_key,
            priv_key=priv_key,
            psk_key=psk_key,
            ip_address=ip_address,
            conf_name=conf_name,
        )
        self.session.add(device)
        await self.session.flush()
        return device
    
    async def revoke(self, device: Device) -> None:
        """Revoke a device."""
        device.state = "revoked"
        device.state_updated_at = utc_now()
        device.updated_at = utc_now()
    
    async def expire(self, device: Device) -> None:
        """Mark device as expired."""
        device.state = "expired"
        device.state_updated_at = utc_now()
        device.updated_at = utc_now()
    
    async def update_traffic(
        self, 
        device: Device, 
        rx_bytes: int, 
        tx_bytes: int,
    ) -> None:
        """Update traffic statistics for a device."""
        device.rx_bytes_total = rx_bytes
        device.tx_bytes_total = tx_bytes
        device.last_activity = utc_now()
        device.updated_at = utc_now()
    
    async def count_active_for_subscription(self, subscription_id: int) -> int:
        """Count active devices for a subscription."""
        result = await self.session.execute(
            select(func.count())
            .where(
                and_(
                    Device.subscription_id == subscription_id,
                    Device.state == "active",
                )
            )
        )
        return result.scalar() or 0


class ServerStatsRepository:
    """Repository for ServerStats operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def record_stats(
        self,
        server_id: int,
        total_peers: int,
        rx_bytes: int,
        tx_bytes: int,
    ) -> ServerStats:
        """Record server statistics."""
        stats = ServerStats(
            server_id=server_id,
            total_peers=total_peers,
            rx_bytes=rx_bytes,
            tx_bytes=tx_bytes,
        )
        self.session.add(stats)
        await self.session.flush()
        return stats
    
    async def get_latest_for_server(self, server_id: int) -> Optional[ServerStats]:
        """Get latest stats for a server."""
        result = await self.session.execute(
            select(ServerStats)
            .where(ServerStats.server_id == server_id)
            .order_by(ServerStats.recorded_at.desc())
        )
        return result.scalar_one_or_none()


class PaymentRepository:
    """Repository for Payment operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def get_by_telegram_charge_id(
        self, 
        charge_id: str,
    ) -> Optional[Payment]:
        """Get payment by Telegram charge ID."""
        result = await self.session.execute(
            select(Payment).where(Payment.telegram_payment_charge_id == charge_id)
        )
        return result.scalar_one_or_none()
    
    async def create(
        self,
        user_id: int,
        amount: int,
        currency: str = "XTR",
        telegram_payment_charge_id: Optional[str] = None,
        provider_payment_charge_id: Optional[str] = None,
    ) -> Payment:
        """Create a new payment record."""
        payment = Payment(
            user_id=user_id,
            amount=amount,
            currency=currency,
            telegram_payment_charge_id=telegram_payment_charge_id,
            provider_payment_charge_id=provider_payment_charge_id,
        )
        self.session.add(payment)
        await self.session.flush()
        return payment
    
    async def mark_processed(
        self,
        payment: Payment,
        status: str = "completed",
        subscription_days: Optional[int] = None,
        provisioned_until: Optional[datetime] = None,
    ) -> None:
        """Mark payment as processed."""
        payment.status = status
        payment.last_provision_status = status
        if subscription_days:
            payment.subscription_days = subscription_days
        if provisioned_until:
            payment.provisioned_until = provisioned_until
        payment.updated_at = utc_now()
    
    async def mark_failed(
        self,
        payment: Payment,
        error_message: str,
    ) -> None:
        """Mark payment as failed."""
        payment.status = "failed"
        payment.error_message = error_message
        payment.updated_at = utc_now()
    
    async def increment_attempt(self, payment: Payment) -> None:
        """Increment payment attempt count."""
        payment.attempt_count += 1
        payment.updated_at = utc_now()
