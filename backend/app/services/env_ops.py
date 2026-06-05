"""Env write / read / duplicate helpers (reshape v2).

These are plain functions that operate on ORM objects. They are kept
separate from the FastAPI router so they can be unit-tested without an
HTTP layer.

Hierarchy now: `Env → DomainGrouping → (MatchCondition, Destination)`
where each `MatchCondition` has an OR-list of `values`.

Key behaviors:

* `replace_env_in_session` — atomic full-replace of an env's source
  config, domain groupings, match conditions, match condition values,
  destinations, and headers. Takes the session so it can
  `await session.flush()` between the delete-orphan cascade and the
  insert of new rows (without that, the `UNIQUE (..., position)`
  constraints trip because new rows conflict with old ones at the
  same position).
* `secret_writer` is a callable that converts a `SecretStr` to a plain
  string. The CRUD router uses the real `get_secret_value`; the import
  router passes a writer that always returns `None` (forces re-entry).
* `build_read_shape` / `serialize_secrets_stripped` produce dicts that
  match the read-side Pydantic schema, including the list ordering
  required by the UI.
"""
from __future__ import annotations

import uuid
from typing import Any, Callable, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from app import schemas
from app.models import (
    Destination,
    DomainGrouping,
    Env,
    Header,
    MatchCondition,
    MatchConditionValue,
    SourceConfig,
)


SecretWriter = Callable[[Optional[schemas.SecretStr]], Optional[str]]


def _secret_str(secret: Optional[schemas.SecretStr], writer: SecretWriter) -> Optional[str]:
    if secret is None:
        return None
    return writer(secret)


def _unwrap_source(src: SourceConfig) -> Dict[str, Any]:
    return {
        "brokers": src.brokers,
        "topic": src.topic,
        "consumer_group": src.consumer_group,
        "offset_reset": src.offset_reset,
        "security_protocol": src.security_protocol,
        "sasl_mechanism": src.sasl_mechanism,
        "sasl_username": src.sasl_username,
        "sasl_password": src.sasl_password,  # raw DB value; redaction in router
        "ssl_ca_location": src.ssl_ca_location,
    }


def _unwrap_destination(dest: Destination) -> Dict[str, Any]:
    return {
        "use_source_broker": bool(dest.use_source_broker),
        "brokers": dest.brokers,
        "topic": dest.topic,
        "security_protocol": dest.security_protocol,
        "sasl_mechanism": dest.sasl_mechanism,
        "sasl_username": dest.sasl_username,
        "sasl_password": dest.sasl_password,
        "ssl_ca_location": dest.ssl_ca_location,
        "headers": [
            {"name": h.name, "value": h.value, "mode": h.mode}
            for h in dest.headers
        ],
    }


def _unwrap_match_condition(mc: MatchCondition) -> Dict[str, Any]:
    return {
        "key_path": mc.key_path,
        "operator": mc.operator,
        "case_insensitive": bool(mc.case_insensitive),
        "values": [{"value": v.value} for v in mc.values],
    }


def _unwrap_domain_grouping(dg: DomainGrouping) -> Dict[str, Any]:
    return {
        "name": dg.name,
        "match_conditions": [_unwrap_match_condition(mc) for mc in dg.match_conditions],
        "destinations": [_unwrap_destination(d) for d in dg.destinations],
    }


def build_read_shape(env: Env) -> Dict[str, Any]:
    """Build the read-side dict for an env (does not redact secrets)."""
    return {
        "id": env.id,
        "name": env.name,
        "description": env.description,
        "enabled": bool(env.enabled),
        "delivery_mode": env.delivery_mode,
        "dlq_topic": env.dlq_topic,
        "dlq_brokers": env.dlq_brokers,
        "created_at": env.created_at,
        "updated_at": env.updated_at,
        "source": _unwrap_source(env.source) if env.source is not None else None,
        "domain_groupings": [_unwrap_domain_grouping(dg) for dg in env.domain_groupings],
    }


def serialize_secrets_stripped(env_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Strip secrets from a read-shape dict (for export)."""
    out = dict(env_dict)
    if out.get("source") is not None:
        s = dict(out["source"])
        s["sasl_password"] = None
        out["source"] = s
    for dg in out.get("domain_groupings", []) or []:
        for d in dg.get("destinations", []) or []:
            d["sasl_password"] = None
    return out


# ---------- mutations ----------


async def replace_env_in_session(
    session: AsyncSession,
    env: Env,
    payload: schemas.EnvIn,
    *,
    new_env_id: str,
    now: str,
    secret_writer: SecretWriter,
) -> None:
    """Replace env children (source, domain_groupings, match_conditions,
    match_condition_values, destinations, headers).

    Flushes the deletes from the cascade before inserting the new rows
    so the `UNIQUE (..., position)` constraints don't trip.

    Adds new ORM objects to the session but does not commit. The caller
    is responsible for `session.commit()`.
    """
    # --- source ---
    env.source = None  # type: ignore[assignment]
    src_payload = payload.source
    new_source = SourceConfig(
        env_id=new_env_id,
        brokers=src_payload.brokers,
        topic=src_payload.topic,
        consumer_group=src_payload.consumer_group,
        offset_reset=src_payload.offset_reset,
        security_protocol=src_payload.security_protocol,
        sasl_mechanism=src_payload.sasl_mechanism,
        sasl_username=src_payload.sasl_username,
        sasl_password=_secret_str(src_payload.sasl_password, secret_writer),
        ssl_ca_location=src_payload.ssl_ca_location,
    )
    env.source = new_source  # type: ignore[assignment]

    # --- domain_groupings / match_conditions / values / destinations / headers ---
    # The router always loads the env with `selectinload` before calling
    # this function, so `env.domain_groupings` is a populated
    # InstrumentedList. `.clear()` triggers the cascade
    # (`all, delete-orphan`) to mark children for deletion. We flush
    # here so DELETEs go to the DB before INSERTs do.
    env.domain_groupings.clear()
    await session.flush()

    for dg_idx, dg_payload in enumerate(payload.domain_groupings, start=1):
        new_dg = DomainGrouping(
            id=str(uuid.uuid4()),
            env_id=new_env_id,
            position=dg_idx,
            name=dg_payload.name or "",
        )
        for mc_idx, mc_payload in enumerate(dg_payload.match_conditions, start=1):
            new_mc = MatchCondition(
                id=str(uuid.uuid4()),
                domain_grouping_id=new_dg.id,
                position=mc_idx,
                key_path=mc_payload.key_path,
                operator=mc_payload.operator,
                case_insensitive=1 if mc_payload.case_insensitive else 0,
            )
            for v_idx, v_payload in enumerate(mc_payload.values, start=1):
                new_mc.values.append(
                    MatchConditionValue(
                        id=str(uuid.uuid4()),
                        position=v_idx,
                        value=v_payload.value,
                    )
                )
            new_dg.match_conditions.append(new_mc)
        for d_idx, d_payload in enumerate(dg_payload.destinations, start=1):
            new_dest = Destination(
                id=str(uuid.uuid4()),
                domain_grouping_id=new_dg.id,
                position=d_idx,
                use_source_broker=1 if d_payload.use_source_broker else 0,
                brokers=d_payload.brokers,
                topic=d_payload.topic,
                security_protocol=d_payload.security_protocol,
                sasl_mechanism=d_payload.sasl_mechanism,
                sasl_username=d_payload.sasl_username,
                sasl_password=_secret_str(d_payload.sasl_password, secret_writer),
                ssl_ca_location=d_payload.ssl_ca_location,
            )
            for h_idx, h_payload in enumerate(d_payload.headers, start=1):
                new_dest.headers.append(
                    Header(
                        id=str(uuid.uuid4()),
                        position=h_idx,
                        name=h_payload.name,
                        value=h_payload.value,
                        mode=h_payload.mode,
                    )
                )
            new_dg.destinations.append(new_dest)
        env.domain_groupings.append(new_dg)


def duplicate_env_in_session(env: Env, *, now: str) -> Env:
    """Build a new Env (in session) that is a copy of `env`.

    Secrets are NOT copied. The new env's name has ` (copy)` appended.
    """
    new_id = str(uuid.uuid4())
    new_env = Env(
        id=new_id,
        name=f"{env.name} (copy)",
        description=env.description,
        enabled=0,
        delivery_mode=env.delivery_mode,
        dlq_topic=env.dlq_topic,
        dlq_brokers=env.dlq_brokers,
        created_at=now,
        updated_at=now,
    )
    if env.source is not None:
        new_env.source = SourceConfig(
            env_id=new_id,
            brokers=env.source.brokers,
            topic=env.source.topic,
            consumer_group=env.source.consumer_group,
            offset_reset=env.source.offset_reset,
            security_protocol=env.source.security_protocol,
            sasl_mechanism=env.source.sasl_mechanism,
            sasl_username=env.source.sasl_username,
            sasl_password=None,  # not copied
            ssl_ca_location=env.source.ssl_ca_location,
        )
    for dg in env.domain_groupings:
        new_dg = DomainGrouping(
            id=str(uuid.uuid4()),
            env_id=new_id,
            position=dg.position,
            name=dg.name,
        )
        for mc in dg.match_conditions:
            new_mc = MatchCondition(
                id=str(uuid.uuid4()),
                domain_grouping_id=new_dg.id,
                position=mc.position,
                key_path=mc.key_path,
                operator=mc.operator,
                case_insensitive=mc.case_insensitive,
            )
            for v in mc.values:
                new_mc.values.append(
                    MatchConditionValue(
                        id=str(uuid.uuid4()),
                        position=v.position,
                        value=v.value,
                    )
                )
            new_dg.match_conditions.append(new_mc)
        for d in dg.destinations:
            new_dest = Destination(
                id=str(uuid.uuid4()),
                domain_grouping_id=new_dg.id,
                position=d.position,
                use_source_broker=d.use_source_broker,
                brokers=d.brokers,
                topic=d.topic,
                security_protocol=d.security_protocol,
                sasl_mechanism=d.sasl_mechanism,
                sasl_username=d.sasl_username,
                sasl_password=None,  # not copied
                ssl_ca_location=d.ssl_ca_location,
            )
            for h in d.headers:
                new_dest.headers.append(
                    Header(
                        id=str(uuid.uuid4()),
                        position=h.position,
                        name=h.name,
                        value=h.value,
                        mode=h.mode,
                    )
                )
            new_dg.destinations.append(new_dest)
        new_env.domain_groupings.append(new_dg)
    return new_env
