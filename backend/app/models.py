"""SQLAlchemy ORM models mirroring PRD §7.

Every boolean column is stored as INTEGER (0/1). All timestamps are
ISO 8601 UTC strings. UUIDs are stored as 36-char strings.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List, Optional

from sqlalchemy import (
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
)


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Base(DeclarativeBase):
    """Declarative base for all ORM models."""


class Env(Base):
    __tablename__ = "envs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(Text, nullable=False, unique=True)
    description: Mapped[str] = mapped_column(Text, nullable=False, default="")
    enabled: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # DLQ additions (PRD §12 decision 8.A). Both nullable.
    dlq_topic: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    dlq_brokers: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(Text, nullable=False, default=_now)
    updated_at: Mapped[str] = mapped_column(Text, nullable=False, default=_now)

    source: Mapped[Optional["SourceConfig"]] = relationship(
        back_populates="env",
        uselist=False,
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    mappings: Mapped[List["Mapping"]] = relationship(
        back_populates="env",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Mapping.position",
    )
    status: Mapped[Optional["RuntimeStatus"]] = relationship(
        back_populates="env",
        uselist=False,
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    logs: Mapped[List["RuntimeLog"]] = relationship(
        back_populates="env",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class SourceConfig(Base):
    __tablename__ = "source_configs"

    env_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("envs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    brokers: Mapped[str] = mapped_column(Text, nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    consumer_group: Mapped[str] = mapped_column(Text, nullable=False)
    offset_reset: Mapped[str] = mapped_column(Text, nullable=False)
    security_protocol: Mapped[str] = mapped_column(
        Text, nullable=False, default="PLAINTEXT"
    )
    sasl_mechanism: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sasl_username: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sasl_password: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ssl_ca_location: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        CheckConstraint(
            "offset_reset IN ('earliest','latest')", name="ck_source_offset_reset"
        ),
    )

    env: Mapped[Env] = relationship(back_populates="source")


class Mapping(Base):
    __tablename__ = "mappings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    env_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("envs.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    key_path: Mapped[str] = mapped_column(Text, nullable=False)
    operator: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    case_insensitive: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )

    __table_args__ = (
        UniqueConstraint("env_id", "position", name="uq_mapping_position"),
        CheckConstraint(
            "operator IN ('equals','not_equals','contains')",
            name="ck_mapping_operator",
        ),
    )

    env: Mapped[Env] = relationship(back_populates="mappings")
    destinations: Mapped[List["Destination"]] = relationship(
        back_populates="mapping",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Destination.position",
    )


class Destination(Base):
    __tablename__ = "destinations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    mapping_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("mappings.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    use_source_broker: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )
    brokers: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    security_protocol: Mapped[str] = mapped_column(
        Text, nullable=False, default="PLAINTEXT"
    )
    sasl_mechanism: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sasl_username: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sasl_password: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ssl_ca_location: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint("mapping_id", "position", name="uq_destination_position"),
    )

    mapping: Mapped[Mapping] = relationship(back_populates="destinations")
    headers: Mapped[List["Header"]] = relationship(
        back_populates="destination",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Header.position",
    )


class Header(Base):
    __tablename__ = "headers"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    destination_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("destinations.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)
    mode: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("destination_id", "position", name="uq_header_position"),
        CheckConstraint(
            "mode IN ('static','from_message')", name="ck_header_mode"
        ),
    )

    destination: Mapped[Destination] = relationship(back_populates="headers")


class RuntimeStatus(Base):
    __tablename__ = "runtime_status"

    env_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("envs.id", ondelete="CASCADE"),
        primary_key=True,
    )
    state: Mapped[str] = mapped_column(Text, nullable=False, default="stopped")
    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    started_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    stopped_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    messages_consumed: Mapped[int] = mapped_column(
        Integer, nullable=False, default=0
    )
    messages_routed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    messages_failed: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_message_at: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    env: Mapped[Env] = relationship(back_populates="status")


class RuntimeLog(Base):
    __tablename__ = "runtime_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    env_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("envs.id", ondelete="CASCADE"),
        nullable=False,
    )
    ts: Mapped[str] = mapped_column(Text, nullable=False)
    level: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (Index("ix_runtime_logs_env_ts", "env_id", "ts"),)

    env: Mapped[Env] = relationship(back_populates="logs")
