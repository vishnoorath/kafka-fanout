import asyncio
import json
import logging
from typing import AsyncGenerator
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db import get_session
from app.models import Env, RuntimeStatus, RuntimeLog
from app.services.env_ops import build_read_shape
from app.security import redact_env

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["streams"])

async def status_event_generator(
    env_id: str,
    session_factory
) -> AsyncGenerator[str, None]:
    """Generator that yields SSE events for status changes and new logs."""
    last_state = None
    last_log_id = 0
    first_run = True

    while True:
        try:
            # We open a new session scoped block to fetch fresh data from DB on each tick
            async with session_factory() as session:
                # 1. Fetch current status
                status_stmt = select(RuntimeStatus).where(RuntimeStatus.env_id == env_id)
                status_res = await session.execute(status_stmt)
                status_row = status_res.scalar_one_or_none()

                if status_row:
                    current_status = {
                        "env_id": status_row.env_id,
                        "state": status_row.state,
                        "last_error": status_row.last_error,
                        "started_at": status_row.started_at,
                        "stopped_at": status_row.stopped_at,
                        "messages_consumed": status_row.messages_consumed,
                        "messages_routed": status_row.messages_routed,
                        "messages_failed": status_row.messages_failed,
                        "last_message_at": status_row.last_message_at,
                        "consumed_rate": status_row.consumed_rate,
                        "routed_rate": status_row.routed_rate,
                        "failed_rate": status_row.failed_rate,
                        "messages_unmatched": status_row.messages_unmatched,
                    }
                    
                    # Yield status if it changed or on the first loop
                    if first_run or current_status != last_state:
                        last_state = current_status
                        yield f"event: status\ndata: {json.dumps(current_status)}\n\n"

                # 2. Fetch new logs (only after the first run, or fetch all initially up to limit)
                if first_run:
                    # On first run, return the latest 100 logs (desc then reverse)
                    log_stmt = (
                        select(RuntimeLog)
                        .where(RuntimeLog.env_id == env_id)
                        .order_by(RuntimeLog.id.desc())
                        .limit(100)
                    )
                else:
                    # On subsequent runs, only fetch logs newer than last seen
                    log_stmt = (
                        select(RuntimeLog)
                        .where(RuntimeLog.env_id == env_id)
                        .where(RuntimeLog.id > last_log_id)
                        .order_by(RuntimeLog.id.asc())
                    )

                log_res = await session.execute(log_stmt)
                log_rows = log_res.scalars().all()
                
                if first_run:
                    # reverse because we sorted desc to get the latest N
                    log_rows = list(reversed(log_rows))

                if log_rows:
                    last_log_id = max(r.id for r in log_rows)
                    logs_data = [
                        {
                            "id": r.id,
                            "env_id": r.env_id,
                            "ts": r.ts,
                            "level": r.level,
                            "message": r.message,
                        }
                        for r in log_rows
                    ]
                    yield f"event: log\ndata: {json.dumps(logs_data)}\n\n"

            first_run = False
        except (asyncio.CancelledError, GeneratorExit):
            # Client disconnected — stop gracefully
            log.debug("SSE stream closed for env %s", env_id)
            return
        except Exception as exc:
            log.warning("Error in SSE generator for env %s: %s", env_id, exc)
            yield f"event: error\ndata: {json.dumps({'error': str(exc)})}\n\n"

        await asyncio.sleep(1.0)

@router.get("/envs/{env_id}/stream")
async def stream_env(
    env_id: str,
    session: AsyncSession = Depends(get_session),
) -> StreamingResponse:
    # Verify environment exists
    existing = await session.get(Env, env_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Environment not found")

    # Access the sessionmaker from database/engine to create sessions in generator
    from app.db import session_scope
    
    return StreamingResponse(
        status_event_generator(env_id, session_scope),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no", # Disable buffering for Nginx
        }
    )
