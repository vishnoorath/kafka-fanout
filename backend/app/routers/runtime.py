"""Runtime router — start/stop/reset-offsets/status/logs/test."""
from __future__ import annotations

import logging
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Request
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
    SourceConfig,
)
from app.runtime.matcher import build_headers, evaluate_condition
from app.services.env_ops import build_read_shape

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["runtime"])


def _manager(request: Request):
    manager = getattr(request.app.state, "runtime_manager", None)
    if manager is None:
        raise HTTPException(
            status_code=503,
            detail={
                "error": {
                    "code": "runtime_unavailable",
                    "message": "runtime manager not initialized",
                    "details": {},
                }
            },
        )
    return manager


# ---------- start / stop / reset ----------


@router.post("/envs/{env_id}/start")
async def start_env(env_id: str, request: Request):
    manager = _manager(request)
    await manager.start(env_id)
    return {"ok": True}


@router.post("/envs/{env_id}/stop")
async def stop_env(env_id: str, request: Request):
    manager = _manager(request)
    await manager.stop(env_id)
    return {"ok": True}


@router.post("/envs/{env_id}/reset-offsets")
async def reset_offsets(env_id: str, request: Request):
    manager = _manager(request)
    await manager.reset_offsets(env_id)
    return {"ok": True}


# ---------- status / logs ----------


@router.get("/envs/{env_id}/status", response_model=schemas.RuntimeStatusOut)
async def get_status(env_id: str, request: Request):
    manager = _manager(request)
    row = await manager.status(env_id)
    if row is None:
        raise HTTPException(status_code=404, detail={"error": {"code": "status_not_found", "message": f"no status for env {env_id}", "details": {}}})
    return row


@router.get("/envs/{env_id}/logs", response_model=List[schemas.RuntimeLogOut])
async def get_logs(
    env_id: str,
    request: Request,
    limit: int = 200,
):
    manager = _manager(request)
    rows = await manager.recent_logs(env_id, limit=limit)
    return rows


# ---------- test endpoint (pure compute) ----------


@router.post("/envs/{env_id}/test", response_model=schemas.TestResponse)
async def test_message(
    env_id: str,
    payload: schemas.TestRequest,
    session: AsyncSession = Depends(get_session),
):
    """Dry-run a message against the env's mappings.

    Pure compute — no Kafka I/O. Mirrors the runtime consumer's logic
    exactly: same matcher, same header builder.
    """
    stmt = (
        select(Env)
        .where(Env.id == env_id)
        .options(
            selectinload(Env.source),
            selectinload(Env.mappings).selectinload(Mapping.destinations).selectinload(
                Destination.headers
            ),
        )
    )
    env = (await session.execute(stmt)).scalar_one_or_none()
    if env is None:
        raise HTTPException(
            status_code=404,
            detail={"error": {"code": "env_not_found", "message": f"env {env_id} does not exist", "details": {}}},
        )

    env_dict = build_read_shape(env)
    results: list[dict] = []
    for idx, m in enumerate(env_dict["mappings"]):
        result = evaluate_condition(
            key_path=m["key_path"],
            operator=m["operator"],
            value=m["value"],
            case_insensitive=m["case_insensitive"],
            message=payload.message,
        )
        destinations_out: list[dict] = []
        if result.matched:
            for d in m["destinations"]:
                headers = build_headers(d["headers"], payload.message)
                destinations_out.append(
                    {
                        "topic": d["topic"],
                        "headers": [
                            {"name": name, "value": value.decode("utf-8")}
                            for name, value in headers
                        ],
                    }
                )
        results.append(
            {
                "mapping_index": idx,
                "key_path": m["key_path"],
                "resolved": result.resolved,
                "matched": result.matched,
                "reason": result.error,
                "error": result.error if result.expression_invalid else None,
                "destinations": destinations_out,
            }
        )
    return {"results": results}
