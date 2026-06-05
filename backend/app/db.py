"""Database engine, session, and schema initialization."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.config import settings
from app.models import Base

engine = create_async_engine(
    settings.database_url,
    echo=False,
    future=True,
)

# aiosqlite does not enable foreign keys by default — toggle it per connection.
@event.listens_for(engine.sync_engine, "connect")
def _enable_sqlite_fk(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
    finally:
        cursor.close()


SessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
    class_=AsyncSession,
)


async def init_db() -> None:
    """Create all tables. Called on FastAPI startup."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # Self-healing migrations for SQLite columns added in newer versions
        for col in ["consumed_rate", "routed_rate", "failed_rate"]:
            try:
                await conn.execute(text(f"ALTER TABLE runtime_status ADD COLUMN {col} REAL DEFAULT 0.0"))
            except Exception:
                pass  # Ignore if column already exists


async def get_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency that yields an AsyncSession per request."""
    async with SessionLocal() as session:
        yield session


@asynccontextmanager
async def session_scope() -> AsyncIterator[AsyncSession]:
    """Context manager for ad-hoc sessions outside of request handling."""
    async with SessionLocal() as session:
        yield session
