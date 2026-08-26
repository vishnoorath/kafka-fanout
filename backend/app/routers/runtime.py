"""Runtime router — start/stop/reset-offsets/status/logs/test (reshape v2)."""
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
    DomainGrouping,
    Env,
    MatchCondition,
    MatchConditionValue,
    SourceConfig,
)
from app.runtime.matcher import build_headers, evaluate_condition, evaluate_match_condition
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


@router.post("/envs/{env_id}/logs/clear")
async def clear_logs(
    env_id: str,
    request: Request,
    older_than_seconds: int = 300,
):
    """Delete runtime log rows older than the cutoff (default: 5 minutes)."""
    manager = _manager(request)
    deleted = await manager.clear_logs(env_id, older_than_seconds=older_than_seconds)
    return {"ok": True, "deleted": deleted}


# ---------- test endpoint (pure compute) ----------


@router.post("/envs/{env_id}/test", response_model=schemas.TestResponse)
async def test_message(
    env_id: str,
    payload: schemas.TestRequest,
    session: AsyncSession = Depends(get_session),
):
    """Dry-run a message against the env's domain groupings.

    Pure compute — no Kafka I/O. Mirrors the runtime consumer's logic
    exactly: same matcher, same header builder.

    For each domain grouping, evaluate every match condition. If any MC
    in a DG matches, the message fans out to all of that DG's
    destinations (with headers built from each destination's header
    list, evaluated against the message).
    """
    stmt = (
        select(Env)
        .where(Env.id == env_id)
        .options(
            selectinload(Env.source),
            selectinload(Env.domain_groupings)
                .selectinload(DomainGrouping.match_conditions)
                .selectinload(MatchCondition.values),
            selectinload(Env.domain_groupings)
                .selectinload(DomainGrouping.destinations)
                .selectinload(Destination.headers),
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
    for dg_idx, dg in enumerate(env_dict["domain_groupings"]):
        mc_results: list[dict] = []
        any_matched = False
        for mc_idx, mc in enumerate(dg["match_conditions"]):
            # `mc["values"]` from `build_read_shape` is a list of {value: str}
            # dicts; flatten to strings for the matcher.
            value_strs = [v["value"] for v in mc["values"]]
            result = evaluate_match_condition(
                key_path=mc["key_path"],
                operator=mc["operator"],
                values=value_strs,
                case_insensitive=mc["case_insensitive"],
                message=payload.message,
            )
            matched_value_index = None
            matched_value = None
            if result.matched:
                any_matched = True
                # Find which value in the list won (small lists — fine to
                # re-evaluate per-value).
                for v_idx, v in enumerate(value_strs):
                    sub = evaluate_condition(
                        key_path=mc["key_path"],
                        operator=mc["operator"],
                        value=v,
                        case_insensitive=mc["case_insensitive"],
                        message=payload.message,
                    )
                    if sub.matched:
                        matched_value_index = v_idx
                        matched_value = v
                        break
            mc_results.append(
                {
                    "match_condition_index": mc_idx,
                    "key_path": mc["key_path"],
                    "matched": result.matched,
                    "matched_value_index": matched_value_index,
                    "matched_value": matched_value,
                    "resolved": result.resolved,
                    "error": result.error,
                    "reason": result.error,
                    "expression_invalid": result.expression_invalid,
                }
            )
        destinations_out: list[dict] = []
        if any_matched:
            for d in dg["destinations"]:
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
                "domain_grouping_index": dg_idx,
                "name": dg["name"],
                "matched": any_matched,
                "match_conditions": mc_results,
                "destinations": destinations_out,
            }
        )
    return {"results": results}
