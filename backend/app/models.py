"""SQLAlchemy ORM models mirroring PRD §7 (reshape v2).

Hierarchy:

  Env
   └─ domain_groupings (1..N, ordered by position)
       ├─ match_conditions (1..N, ordered by position)
       │   └─ values (1..N, ordered by position)
       └─ destinations (1..N, ordered by position)
           └─ headers (1..N, ordered by position)

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
    domain_groupings: Mapped[List["DomainGrouping"]] = relationship(
        back_populates="env",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="DomainGrouping.position",
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


class DomainGrouping(Base):
    __tablename__ = "domain_groupings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    env_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("envs.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False, default="")

    __table_args__ = (
        UniqueConstraint("env_id", "position", name="uq_domain_grouping_position"),
    )

    env: Mapped[Env] = relationship(back_populates="domain_groupings")
    match_conditions: Mapped[List["MatchCondition"]] = relationship(
        back_populates="domain_grouping",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="MatchCondition.position",
    )
    destinations: Mapped[List["Destination"]] = relationship(
        back_populates="domain_grouping",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="Destination.position",
    )


class MatchCondition(Base):
    __tablename__ = "match_conditions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    domain_grouping_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("domain_groupings.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    key_path: Mapped[str] = mapped_column(Text, nullable=False)
    operator: Mapped[str] = mapped_column(Text, nullable=False, default="equals")
    case_insensitive: Mapped[int] = mapped_column(
        Integer, nullable=False, default=1
    )

    __table_args__ = (
        UniqueConstraint(
            "domain_grouping_id", "position", name="uq_match_condition_position"
        ),
        CheckConstraint(
            "operator IN ('equals','not_equals','contains')",
            name="ck_match_condition_operator",
        ),
    )

    domain_grouping: Mapped[DomainGrouping] = relationship(
        back_populates="match_conditions"
    )
    values: Mapped[List["MatchConditionValue"]] = relationship(
        back_populates="match_condition",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="MatchConditionValue.position",
    )


class MatchConditionValue(Base):
    __tablename__ = "match_condition_values"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    match_condition_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("match_conditions.id", ondelete="CASCADE"),
        nullable=False,
    )
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "match_condition_id", "position",
            name="uq_match_condition_value_position",
        ),
    )

    match_condition: Mapped[MatchCondition] = relationship(back_populates="values")


class Destination(Base):
    __tablename__ = "destinations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    domain_grouping_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("domain_groupings.id", ondelete="CASCADE"),
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
        UniqueConstraint(
            "domain_grouping_id", "position", name="uq_destination_position"
        ),
    )

    domain_grouping: Mapped[DomainGrouping] = relationship(
        back_populates="destinations"
    )
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


class UnmatchedMessage(Base):
    __tablename__ = "unmatched_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    env_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("envs.id", ondelete="CASCADE"),
        nullable=False,
    )
    ts: Mapped[str] = mapped_column(Text, nullable=False)
    message: Mapped[str] = mapped_column(Text, nullable=False)

    __table_args__ = (Index("ix_unmatched_messages_env_ts", "env_id", "ts"),)

    env: Mapped[Env] = relationship()
