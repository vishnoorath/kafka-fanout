"""RuntimeManager — singleton owning all per-env consumer tasks.

Constructed once in FastAPI's lifespan and stashed on `app.state`. The
routers in `app/routers/runtime.py` go through it for every start / stop
/ reset / status call. On app shutdown, `aclose_all()` stops every
running consumer and closes the producer pool.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import GroupCoordinatorNotAvailableError, UnknownTopicOrPartitionError
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import session_scope
from app.models import Env, RuntimeLog, RuntimeStatus
from app.runtime.consumer import ConsumerTask
from app.runtime.producer import DLQPublisher, ProducerPool

log = logging.getLogger(__name__)


class RuntimeManager:
    def __init__(self) -> None:
        self.pool = ProducerPool()
        self.dlq = DLQPublisher(self.pool)
        self._tasks: Dict[str, ConsumerTask] = {}
        # Per-env asyncio lock so two concurrent start() calls can't
        # both create new tasks (which previously caused a "stop then
        # shows running" race when the second start spawned a fresh
        # consumer that re-set state to "running" after Stop had
        # already settled).
        self._start_locks: Dict[str, asyncio.Lock] = {}

    def _lock_for(self, env_id: str) -> asyncio.Lock:
        lock = self._start_locks.get(env_id)
        if lock is None:
            lock = self._start_locks[env_id] = asyncio.Lock()
        return lock

    # ---------- lifecycle ----------

    async def aclose_all(self) -> None:
        log.info("runtime manager shutting down (%d tasks)", len(self._tasks))
        tasks = list(self._tasks.values())
        self._tasks.clear()
        await asyncio.gather(
            *(t.stop() for t in tasks),
            return_exceptions=True,
        )
        await self.pool.aclose_all()

    # ---------- start / stop ----------

    async def start(self, env_id: str) -> None:
        # Serialize concurrent start() calls per env so a double-click
        # (or React strict-mode double effect) can't spawn a second
        # consumer task that races against the first.
        async with self._lock_for(env_id):
            existing = self._tasks.get(env_id)
            if existing is not None:
                if existing.is_running():
                    return  # idempotent
                # The previous task finished (e.g. after Stop). Drop it
                # so a fresh start gets a clean ConsumerTask with fresh
                # in-memory counters and stop event.
                self._tasks.pop(env_id, None)
            # Wipe counters + logs from any previous run so the UI
            # doesn't show stale data when the new consumer starts.
            await self._reset_status_and_logs(env_id)
            task = ConsumerTask(env_id, self.pool, self.dlq)
            self._tasks[env_id] = task
            await task.start()

    async def stop(self, env_id: str) -> None:
        # Serialize stop() with start() so a fast Stop→Start sequence
        # can't interleave (stop finishes, then start re-checks the
        # finished task and properly drops it under the lock).
        async with self._lock_for(env_id):
            task = self._tasks.get(env_id)
            if task is None:
                return  # idempotent
            await task.stop()
            # Drop the finished task so a subsequent start() spawns a
            # fresh one (rather than re-using a task whose internal
            # _stop_event is already set, which would cause start to
            # immediately exit).
            if not task.is_running():
                self._tasks.pop(env_id, None)

    async def reset_offsets(self, env_id: str) -> None:
        """Stop the consumer, then delete the consumer group so the next
        start picks up from `earliest` per the env's `offset_reset`.

        Best-effort: if the broker is unreachable or the group does not
        exist, the operation still returns successfully (the next start
        will reset from `earliest` anyway because no offsets exist).
        """
        await self.stop(env_id)
        group_id, brokers, security = await self._load_consumer_group_info(env_id)
        if not group_id or not brokers:
            return
        admin: Optional[AIOKafkaAdminClient] = None
        try:
            admin = AIOKafkaAdminClient(
                bootstrap_servers=brokers,
                **security,
            )
            await admin.start()
        except Exception as exc:  # noqa: BLE001 — broker unreachable, etc.
            log.warning(
                "reset offsets: could not start admin client for env %s: %s",
                env_id, exc,
            )
            return
        try:
            try:
                await admin.delete_consumer_groups([group_id])
                log.info("reset offsets: deleted group %s", group_id)
            except GroupCoordinatorNotAvailableError as exc:
                log.info("reset offsets: group %s not active (ok): %s", group_id, exc)
            except UnknownTopicOrPartitionError as exc:
                log.info("reset offsets: unknown topic/partition (ok): %s", exc)
        finally:
            if admin is not None:
                try:
                    await admin.close()
                except Exception as exc:  # noqa: BLE001
                    log.warning("reset offsets: admin close failed: %s", exc)

    async def _load_consumer_group_info(self, env_id: str) -> tuple[Optional[str], Optional[str], dict]:
        from sqlalchemy.orm import selectinload

        async with session_scope() as session:
            stmt = (
                select(Env)
                .where(Env.id == env_id)
                .options(selectinload(Env.source))
            )
            env = (await session.execute(stmt)).scalar_one_or_none()
            if env is None or env.source is None:
                return None, None, {}
            from app.runtime.producer import security_dict

            sec = security_dict(
                security_protocol=env.source.security_protocol,
                sasl_mechanism=env.source.sasl_mechanism,
                sasl_username=env.source.sasl_username,
                sasl_password=env.source.sasl_password,
                ssl_ca_location=env.source.ssl_ca_location,
            )
            return env.source.consumer_group, env.source.brokers, sec

    async def _reset_status_and_logs(self, env_id: str) -> None:
        """Wipe counters, last_message_at, and all logs for the env.

        Called at the start of every `start()` so the UI shows clean
        numbers on the next poll (otherwise old counts from a previous
        run linger until the new consumer's first message). The
        RuntimeStatus row is preserved (we just zero out the counter
        columns) — the new consumer will overwrite them as it runs.
        """
        async with session_scope() as session:
            row = await session.get(RuntimeStatus, env_id)
            if row is not None:
                row.messages_consumed = 0
                row.messages_routed = 0
                row.messages_failed = 0
                row.last_message_at = None
            # Wipe logs so the "recent logs" panel doesn't show lines
            # from the previous run. The log table grows back from the
            # new consumer's appends.
            await session.execute(
                delete(RuntimeLog).where(RuntimeLog.env_id == env_id)
            )
            await session.commit()

    # ---------- status / logs ----------

    async def status(self, env_id: str) -> Optional[RuntimeStatus]:
        async with session_scope() as session:
            row = await session.get(RuntimeStatus, env_id)
            if row is not None:
                # Self-healing: if the DB says running/starting/stopping, but the
                # process restarted and we have no active task, sync it back to stopped.
                task = self._tasks.get(env_id)
                is_active = task is not None and task.is_running()
                if not is_active and row.state in ("running", "starting", "stopping"):
                    row.state = "stopped"
                    await session.commit()
            return row

    async def recent_logs(self, env_id: str, limit: int = 200) -> List[RuntimeLog]:
        limit = max(1, min(limit, 1000))
        async with session_scope() as session:
            stmt = (
                select(RuntimeLog)
                .where(RuntimeLog.env_id == env_id)
                .order_by(RuntimeLog.ts.desc(), RuntimeLog.id.desc())
                .limit(limit)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def append_log(
        self,
        env_id: str,
        level: str,
        message: str,
        max_rows_per_env: int = 500,
    ) -> None:
        async with session_scope() as session:
            row = RuntimeLog(
                env_id=env_id,
                ts=datetime.now(timezone.utc).isoformat(),
                level=level,
                message=message,
            )
            session.add(row)
            # Prune oldest rows so we keep at most max_rows_per_env.
            count_stmt = select(RuntimeLog.id).where(RuntimeLog.env_id == env_id)
            ids = (await session.execute(count_stmt)).scalars().all()
            if len(ids) > max_rows_per_env:
                from sqlalchemy import delete

                excess = len(ids) - max_rows_per_env
                await session.execute(
                    delete(RuntimeLog)
                    .where(RuntimeLog.id.in_(ids[:excess]))
                )
            await session.commit()
