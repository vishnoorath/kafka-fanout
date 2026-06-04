"""CRUD + import/export router for environments.

Endpoints mirror PRD §8. All read responses redact `sasl_password`.
A full env replace (PUT) is atomic: the env row, source config, all
mappings, destinations, and headers are rewritten in a single transaction.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Response, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app import schemas
from app.db import get_session
from app.models import (
    Destination,
    Env,
    Header,
    Mapping,
    RuntimeStatus,
    SourceConfig,
)
from app.security import redact_env, redact_env_list, redact_source
from app.services.env_ops import (
    build_read_shape,
    duplicate_env_in_session,
    replace_env_in_session,
    serialize_secrets_stripped,
)

router = APIRouter(prefix="/api", tags=["envs"])


# ---------- helpers ----------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _load_env(session: AsyncSession, env_id: str) -> Env:
    """Eager-load env with source, mappings, destinations, headers."""
    stmt = (
        select(Env)
        .where(Env.id == env_id)
        .options(
            selectinload(Env.source),
            selectinload(Env.mappings).selectinload(Mapping.destinations).selectinload(
                Destination.headers
            ),
            selectinload(Env.status),
        )
    )
    result = await session.execute(stmt)
    env = result.scalar_one_or_none()
    if env is None:
        raise HTTPException(
            status_code=404,
            detail={
                "error": {
                    "code": "env_not_found",
                    "message": f"env {env_id} does not exist",
                    "details": {},
                }
            },
        )
    return env


# ---------- list / create / get ----------

@router.get("/envs", response_model=List[schemas.EnvOut])
async def list_envs(session: AsyncSession = Depends(get_session)) -> list:
    stmt = (
        select(Env)
        .options(
            selectinload(Env.source),
            selectinload(Env.mappings).selectinload(Mapping.destinations).selectinload(
                Destination.headers
            ),
            selectinload(Env.status),
        )
        .order_by(Env.name)
    )
    result = await session.execute(stmt)
    envs = result.scalars().all()
    payload = [build_read_shape(e) for e in envs]
    return redact_env_list(payload)


@router.post(
    "/envs",
    response_model=schemas.EnvOut,
    status_code=status.HTTP_201_CREATED,
)
async def create_env(
    payload: schemas.EnvIn,
    session: AsyncSession = Depends(get_session),
) -> dict:
    # Uniqueness check (DB also enforces).
    existing = await session.execute(select(Env).where(Env.name == payload.name))
    if existing.scalar_one_or_none() is not None:
        raise HTTPException(
            status_code=409,
            detail={
                "error": {
                    "code": "env_name_conflict",
                    "message": f"env name '{payload.name}' is already in use",
                    "details": {},
                }
            },
        )

    env_id = str(uuid.uuid4())
    now = _now()
    env = Env(
        id=env_id,
        name=payload.name,
        description=payload.description,
        enabled=1 if payload.enabled else 0,
        dlq_topic=payload.dlq_topic,
        dlq_brokers=payload.dlq_brokers,
        created_at=now,
        updated_at=now,
    )
    session.add(env)
    await session.flush()
    # Materialize relationship collections so replace_env_in_session can
    # call `.clear()` / `.append()` without triggering a lazy load.
    env = await _load_env(session, env_id)

    await replace_env_in_session(
        session,
        env,
        payload,
        new_env_id=env_id,
        now=now,
        secret_writer=lambda secret: secret.get_secret_value() if secret else None,
    )
    # Ensure runtime_status row exists.
    session.add(RuntimeStatus(env_id=env_id, state="stopped"))

    await session.commit()

    # Re-load eagerly for the response.
    env = await _load_env(session, env_id)
    return redact_env(build_read_shape(env))


@router.get("/envs/{env_id}", response_model=schemas.EnvOut)
async def get_env(env_id: str, session: AsyncSession = Depends(get_session)) -> dict:
    env = await _load_env(session, env_id)
    return redact_env(build_read_shape(env))


# ---------- update / delete / duplicate ----------

@router.put("/envs/{env_id}", response_model=schemas.EnvOut)
async def update_env(
    env_id: str,
    payload: schemas.EnvIn,
    session: AsyncSession = Depends(get_session),
) -> dict:
    env = await _load_env(session, env_id)
    # If name is changing, check uniqueness.
    if env.name != payload.name:
        existing = await session.execute(
            select(Env).where(Env.name == payload.name, Env.id != env_id)
        )
        if existing.scalar_one_or_none() is not None:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": {
                        "code": "env_name_conflict",
                        "message": f"env name '{payload.name}' is already in use",
                        "details": {},
                    }
                },
            )

    env.name = payload.name
    env.description = payload.description
    env.enabled = 1 if payload.enabled else 0
    env.dlq_topic = payload.dlq_topic
    env.dlq_brokers = payload.dlq_brokers
    env.updated_at = _now()

    await replace_env_in_session(
        session,
        env,
        payload,
        new_env_id=env_id,
        now=env.updated_at,
        secret_writer=lambda secret: secret.get_secret_value() if secret else None,
    )
    await session.commit()

    env = await _load_env(session, env_id)
    return redact_env(build_read_shape(env))


@router.delete("/envs/{env_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_env(
    env_id: str,
    session: AsyncSession = Depends(get_session),
) -> Response:
    env = await _load_env(session, env_id)
    # If the runtime manager has a live consumer for this env, stop it first.
    from app.main import app  # local import to avoid circular

    manager = getattr(app.state, "runtime_manager", None)
    if manager is not None:
        try:
            await manager.stop(env_id)
        except Exception:  # noqa: BLE001 — best effort during delete
            pass

    await session.delete(env)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/envs/{env_id}/duplicate",
    response_model=schemas.EnvOut,
    status_code=status.HTTP_201_CREATED,
)
async def duplicate_env(
    env_id: str,
    session: AsyncSession = Depends(get_session),
) -> dict:
    env = await _load_env(session, env_id)
    new_env = duplicate_env_in_session(env, now=_now())
    session.add(new_env)
    await session.flush()
    await session.commit()

    created = await _load_env(session, new_env.id)
    return redact_env(build_read_shape(created))


# ---------- export / import ----------

@router.get("/export", response_model=schemas.ExportEnvelope)
async def export_all(session: AsyncSession = Depends(get_session)) -> dict:
    stmt = (
        select(Env)
        .options(
            selectinload(Env.source),
            selectinload(Env.mappings).selectinload(Mapping.destinations).selectinload(
                Destination.headers
            ),
        )
        .order_by(Env.name)
    )
    result = await session.execute(stmt)
    envs = result.scalars().all()
    payload = {
        "version": 1,
        "envs": [
            serialize_secrets_stripped(build_read_shape(e)) for e in envs
        ],
    }
    return payload


@router.post("/import", response_model=List[schemas.EnvOut])
async def import_envs(
    envelope: schemas.ImportEnvelope,
    session: AsyncSession = Depends(get_session),
) -> list:
    imported: list[dict] = []
    for env_in in envelope.envs:
        existing = await session.execute(
            select(Env).where(Env.name == env_in.name)
        )
        existing_env = existing.scalar_one_or_none()
        now = _now()
        if existing_env is None:
            new_id = str(uuid.uuid4())
            env = Env(
                id=new_id,
                name=env_in.name,
                description=env_in.description,
                enabled=1 if env_in.enabled else 0,
                dlq_topic=env_in.dlq_topic,
                dlq_brokers=env_in.dlq_brokers,
                created_at=now,
                updated_at=now,
            )
            session.add(env)
            await session.flush()
            # Load with relationships so replace_env_in_session can mutate collections.
            env = await _load_env(session, new_id)
            await replace_env_in_session(
                session,
                env,
                env_in,
                new_env_id=new_id,
                now=now,
                # Import never carries secrets — explicit re-entry required.
                secret_writer=lambda _secret: None,
            )
            session.add(RuntimeStatus(env_id=new_id, state="stopped"))
        else:
            existing_env.description = env_in.description
            existing_env.enabled = 1 if env_in.enabled else 0
            existing_env.dlq_topic = env_in.dlq_topic
            existing_env.dlq_brokers = env_in.dlq_brokers
            existing_env.updated_at = now
            await replace_env_in_session(
                session,
                existing_env,
                env_in,
                new_env_id=existing_env.id,
                now=now,
                secret_writer=lambda _secret: None,
            )
    await session.commit()

    # Re-load all and return.
    stmt = (
        select(Env)
        .options(
            selectinload(Env.source),
            selectinload(Env.mappings).selectinload(Mapping.destinations).selectinload(
                Destination.headers
            ),
            selectinload(Env.status),
        )
        .order_by(Env.name)
    )
    result = await session.execute(stmt)
    envs = result.scalars().all()
    return redact_env_list([build_read_shape(e) for e in envs])
