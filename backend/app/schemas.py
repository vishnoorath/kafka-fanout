"""Pydantic v2 in/out schemas for the REST API.

Write side accepts full env payloads (PRD §8). Read side returns the same
shape but with `sasl_password` collapsed to `null` (handled in
`app.security` after serialization, plus enforced here for typed safety).
"""
from __future__ import annotations

from datetime import datetime
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, SecretStr


# ---------- Header ----------

class HeaderIn(BaseModel):
    """A header row on a destination.

    `value` is a literal when `mode == "static"`, a JMESPath expression
    when `mode == "from_message"`. Both arrive as plain strings.
    """
    name: str
    value: str
    mode: Literal["static", "from_message"] = "static"


class HeaderOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    value: str
    mode: Literal["static", "from_message"] = "static"


# ---------- Destination ----------

class DestinationIn(BaseModel):
    use_source_broker: bool = True
    brokers: Optional[str] = None
    topic: str
    security_protocol: Literal[
        "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"
    ] = "PLAINTEXT"
    sasl_mechanism: Optional[Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None
    headers: List[HeaderIn] = Field(default_factory=list)


class DestinationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    use_source_broker: bool
    brokers: Optional[str]
    topic: str
    security_protocol: str
    sasl_mechanism: Optional[str]
    sasl_username: Optional[str]
    sasl_password: Optional[str] = None  # always None on read
    ssl_ca_location: Optional[str]
    headers: List[HeaderOut] = Field(default_factory=list)


# ---------- Mapping ----------

class MappingIn(BaseModel):
    key_path: str
    operator: Literal["equals", "not_equals", "contains"] = "equals"
    value: str
    case_insensitive: bool = True
    destinations: List[DestinationIn] = Field(default_factory=list)


class MappingOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    key_path: str
    operator: str
    value: str
    case_insensitive: bool
    destinations: List[DestinationOut] = Field(default_factory=list)


# ---------- Source ----------

class SourceConfigIn(BaseModel):
    brokers: str
    topic: str
    consumer_group: str
    offset_reset: Literal["earliest", "latest"] = "latest"
    security_protocol: Literal[
        "PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"
    ] = "PLAINTEXT"
    sasl_mechanism: Optional[Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]] = None
    sasl_username: Optional[str] = None
    sasl_password: Optional[SecretStr] = None
    ssl_ca_location: Optional[str] = None


class SourceConfigOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    brokers: str
    topic: str
    consumer_group: str
    offset_reset: str
    security_protocol: str
    sasl_mechanism: Optional[str]
    sasl_username: Optional[str]
    sasl_password: Optional[str] = None
    ssl_ca_location: Optional[str]


# ---------- Env ----------

class EnvIn(BaseModel):
    """Full env payload (write side, full replace)."""
    name: str
    description: str = ""
    enabled: bool = False
    dlq_topic: Optional[str] = None
    dlq_brokers: Optional[str] = None
    source: SourceConfigIn
    mappings: List[MappingIn] = Field(default_factory=list)


class EnvOut(BaseModel):
    """Read side. Secrets are always None."""
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    description: str
    enabled: bool
    dlq_topic: Optional[str] = None
    dlq_brokers: Optional[str] = None
    created_at: str
    updated_at: str
    source: Optional[SourceConfigOut] = None
    mappings: List[MappingOut] = Field(default_factory=list)


# ---------- Runtime ----------

class RuntimeStatusOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    env_id: str
    state: Literal["stopped", "starting", "running", "error", "stopping"]
    last_error: Optional[str] = None
    started_at: Optional[str] = None
    stopped_at: Optional[str] = None
    messages_consumed: int = 0
    messages_routed: int = 0
    messages_failed: int = 0
    last_message_at: Optional[str] = None


class RuntimeLogOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    env_id: str
    ts: str
    level: str
    message: str


# ---------- Test endpoint ----------

class TestRequest(BaseModel):
    message: dict


class TestHeaderOut(BaseModel):
    name: str
    value: str


class TestDestinationOut(BaseModel):
    topic: str
    headers: List[TestHeaderOut]


class TestResultOut(BaseModel):
    mapping_index: int
    key_path: Optional[str] = None
    resolved: Optional[object] = None
    matched: bool
    reason: Optional[str] = None
    error: Optional[str] = None
    destinations: List[TestDestinationOut] = Field(default_factory=list)


class TestResponse(BaseModel):
    results: List[TestResultOut]


# ---------- Export / Import ----------

class ExportEnvelope(BaseModel):
    version: Literal[1] = 1
    envs: List[EnvOut]


class ImportEnvelope(BaseModel):
    version: Literal[1] = 1
    envs: List[EnvIn]


# ---------- Error envelope ----------

class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict = Field(default_factory=dict)


class ErrorEnvelope(BaseModel):
    error: ErrorDetail
