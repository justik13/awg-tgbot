"""
Database connection management for awg-tgbot v2.

Supports both SQLite (for development/single-instance) and PostgreSQL (for production).
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    AsyncEngine,
    create_async_engine,
    async_sessionmaker,
    async_scoped_session,
)
from sqlalchemy.pool import StaticPool, NullPool


class DatabaseManager:
    """Manages database connections and sessions."""
    
    def __init__(
        self,
        database_url: str,
        echo: bool = False,
        pool_size: int = 10,
        max_overflow: int = 20,
    ):
        """
        Initialize database manager.
        
        Args:
            database_url: SQLAlchemy database URL (e.g., sqlite+aiosqlite:///bot.db or postgresql+asyncpg://...)
            echo: If True, log all SQL statements
            pool_size: Number of connections to keep in the pool
            max_overflow: Max connections beyond pool_size
        """
        self.database_url = database_url
        self.echo = echo
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        
        self._engine: Optional[AsyncEngine] = None
        self._session_factory: Optional[async_sessionmaker[AsyncSession]] = None
        self._scoped_session: Optional[async_scoped_session[AsyncSession]] = None
        self._lock = asyncio.Lock()
    
    async def initialize(self) -> None:
        """Initialize database engine and session factory."""
        async with self._lock:
            if self._engine is not None:
                return
            
            # Configure pool settings based on database type
            is_sqlite = self.database_url.startswith("sqlite")
            
            if is_sqlite:
                # SQLite uses StaticPool for single connection
                self._engine = create_async_engine(
                    self.database_url,
                    echo=self.echo,
                    poolclass=StaticPool,
                    connect_args={"check_same_thread": False},
                )
            else:
                # PostgreSQL/other databases use connection pooling
                self._engine = create_async_engine(
                    self.database_url,
                    echo=self.echo,
                    pool_size=self.pool_size,
                    max_overflow=self.max_overflow,
                )
            
            self._session_factory = async_sessionmaker(
                self._engine,
                class_=AsyncSession,
                expire_on_commit=False,
                autocommit=False,
                autoflush=False,
            )
            
            # Create scoped session for thread-local access
            self._scoped_session = async_scoped_session(
                self._session_factory,
                scopefunc=asyncio.current_task,
            )
    
    async def close(self) -> None:
        """Close database connections."""
        async with self._lock:
            if self._engine is not None:
                await self._engine.dispose()
                self._engine = None
                self._session_factory = None
                self._scoped_session = None
    
    @property
    def engine(self) -> AsyncEngine:
        """Get the async engine."""
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._engine
    
    @property
    def session(self) -> async_scoped_session[AsyncSession]:
        """Get the scoped session factory."""
        if self._scoped_session is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        return self._scoped_session
    
    @asynccontextmanager
    async def get_session(self) -> AsyncGenerator[AsyncSession, None]:
        """
        Get a database session from the pool.
        
        Usage:
            async with db_manager.get_session() as session:
                # use session
        """
        if self._session_factory is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        session = self._session_factory()
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
    
    async def create_tables(self, base) -> None:
        """
        Create all tables defined in the models.
        
        Args:
            base: SQLAlchemy Base class containing model definitions
        """
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        async with self._engine.begin() as conn:
            await conn.run_sync(base.metadata.create_all)
    
    async def drop_tables(self, base) -> None:
        """
        Drop all tables defined in the models.
        
        Args:
            base: SQLAlchemy Base class containing model definitions
        """
        if self._engine is None:
            raise RuntimeError("Database not initialized. Call initialize() first.")
        
        async with self._engine.begin() as conn:
            await conn.run_sync(base.metadata.drop_all)


# Global database manager instance
_db_manager: Optional[DatabaseManager] = None


def get_db_manager() -> DatabaseManager:
    """Get the global database manager instance."""
    if _db_manager is None:
        raise RuntimeError("Database manager not initialized. Call init_database() first.")
    return _db_manager


def init_database(
    database_url: str,
    echo: bool = False,
    pool_size: int = 10,
    max_overflow: int = 20,
) -> DatabaseManager:
    """
    Initialize the global database manager.
    
    Args:
        database_url: SQLAlchemy database URL
        echo: If True, log all SQL statements
        pool_size: Number of connections to keep in the pool
        max_overflow: Max connections beyond pool_size
    
    Returns:
        DatabaseManager instance
    """
    global _db_manager
    _db_manager = DatabaseManager(
        database_url=database_url,
        echo=echo,
        pool_size=pool_size,
        max_overflow=max_overflow,
    )
    return _db_manager
