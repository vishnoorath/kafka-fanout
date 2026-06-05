import asyncio
import logging
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sqlalchemy
from sqlalchemy import select

from app.db import session_scope
from app.models import Outbox, OutboxDeadLetter, OutboxWatermark, RuntimeStatus
from app.runtime.producer import ProducerPool, DLQPublisher, security_dict

log = logging.getLogger(__name__)

class OutboxDispatcher:
    RETRY_BACKOFFS = (0.5, 1.0, 2.0)
    MAX_ATTEMPTS = 5
    POLL_INTERVAL = 1.0
    BATCH_SIZE = 50

    def __init__(
        self,
        env_id: str,
        pool: ProducerPool,
        dlq: DLQPublisher,
        *,
        manager: Optional[Any] = None,
    ) -> None:
        self.env_id = env_id
        self._pool = pool
        self._dlq = dlq
        self._manager = manager
        self._task: Optional[asyncio.Task] = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name=f"outbox-dispatcher-{self.env_id}")
        log.info("Outbox dispatcher started for env %s", self.env_id)

    async def stop(self, timeout: float = 10.0) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except asyncio.TimeoutError:
            log.warning("outbox dispatcher %s did not stop within %ss, cancelling", self.env_id, timeout)
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        finally:
            self._task = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    async def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                # 1. Fetch pending rows
                rows = await self._fetch_pending_rows()
                if not rows:
                    # No pending items, sleep for poll interval
                    try:
                        await asyncio.wait_for(self._stop_event.wait(), timeout=self.POLL_INTERVAL)
                    except asyncio.TimeoutError:
                        pass
                    # Still update watermarks/metrics when idle
                    await self._update_watermark_and_status()
                    continue

                # 2. Process rows
                for row in rows:
                    if self._stop_event.is_set():
                        break
                    await self._dispatch_row(row)

                # 3. Update watermarks and runtime status counters
                await self._update_watermark_and_status()

            except Exception as exc:
                log.exception("Outbox dispatcher %s encountered an error", self.env_id)
                # Sleep briefly to avoid busy loop on error
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=2.0)
                except asyncio.TimeoutError:
                    pass

    async def _fetch_pending_rows(self) -> List[Outbox]:
        async with session_scope() as session:
            stmt = (
                select(Outbox)
                .where(Outbox.env_id == self.env_id, Outbox.dispatched_at == None)
                .order_by(Outbox.id.asc())
                .limit(self.BATCH_SIZE)
            )
            result = await session.execute(stmt)
            return list(result.scalars().all())

    async def _dispatch_row(self, row: Outbox) -> None:
        # Safely load destinations; handle NULL JSON or non-list results
        destinations = json.loads(row.destinations_json or "[]")
        if not isinstance(destinations, list):
            destinations = []
        payload = row.payload
        
        success = True
        last_error = None
        
        for dest in destinations:
            topic = dest["topic"]
            headers = [(name, val.encode("utf-8")) for name, val in (dest.get("headers") or [])]
            
            # Setup producer
            brokers = dest.get("brokers")
            if not brokers:
                last_error = "Brokers not specified in destination config"
                dest_success = False
                success = False
                break
                
            sec = security_dict(
                security_protocol=dest.get("security_protocol", "PLAINTEXT"),
                sasl_mechanism=dest.get("sasl_mechanism"),
                sasl_username=dest.get("sasl_username"),
                sasl_password=dest.get("sasl_password"),
                ssl_ca_location=dest.get("ssl_ca_location"),
            )
            
            dest_success = False
            for attempt, backoff in enumerate((0.0,) + self.RETRY_BACKOFFS):
                if backoff:
                    await asyncio.sleep(backoff)
                try:
                    producer = await self._pool.get_producer(brokers=brokers, **sec)
                    await producer.send_and_wait(
                        topic=topic,
                        value=payload,
                        headers=headers,
                    )
                    dest_success = True
                    break
                except Exception as exc:
                    last_error = str(exc)
                    log.exception(
                        "Outbox dispatcher env %s row %s -> %s attempt %d failed: %s",
                        self.env_id, row.id, topic, attempt + 1, exc
                    )
            
            if not dest_success:
                success = False
                break
                
        if success:
            # Mark as dispatched
            async with session_scope() as session:
                db_row = await session.get(Outbox, row.id)
                if db_row is not None:
                    db_row.dispatched_at = datetime.now(timezone.utc).isoformat()
                    await session.commit()
            
            await self._increment_status_counters(dispatched=1)
            log.debug("Outbox row %s successfully dispatched for env %s", row.id, self.env_id)
        else:
            new_attempts = row.attempts + 1
            if new_attempts >= self.MAX_ATTEMPTS:
                async with session_scope() as session:
                    db_row = await session.get(Outbox, row.id)
                    if db_row is not None:
                        dl = OutboxDeadLetter(
                            env_id=self.env_id,
                            outbox_id=row.id,
                            idempotency_key=row.idempotency_key,
                            payload=row.payload,
                            headers_json=row.headers_json,
                            destinations_json=row.destinations_json,
                            attempts=new_attempts,
                            last_error=last_error,
                            created_at=row.created_at,
                            dead_lettered_at=datetime.now(timezone.utc).isoformat(),
                        )
                        session.add(dl)
                        await session.delete(db_row)
                        await session.commit()
                
                await self._increment_status_counters(failed=1, dead_lettered=1)
                log.error("Outbox row %s exceeded max attempts for env %s. Moved to dead letters.", row.id, self.env_id)
                if self._manager is not None:
                    await self._manager.append_log(
                        self.env_id,
                        level="ERROR",
                        message=f"Outbox message {row.idempotency_key} failed all attempts: {last_error}. Moved to DLQ table.",
                    )
            else:
                async with session_scope() as session:
                    db_row = await session.get(Outbox, row.id)
                    if db_row is not None:
                        db_row.attempts = new_attempts
                        db_row.last_error = last_error
                        await session.commit()
                
                await self._increment_status_counters(failed=1)

    async def _increment_status_counters(self, dispatched: int = 0, failed: int = 0, dead_lettered: int = 0) -> None:
        async with session_scope() as session:
            status = await session.get(RuntimeStatus, self.env_id)
            if status is not None:
                status.outbox_dispatched_total += dispatched
                status.outbox_failed_total += failed
                status.outbox_dead_lettered_total += dead_lettered
                await session.commit()

    async def _update_watermark_and_status(self) -> None:
        async with session_scope() as session:
            # 1. Fetch oldest pending row
            stmt = (
                select(Outbox)
                .where(Outbox.env_id == self.env_id, Outbox.dispatched_at == None)
                .order_by(Outbox.id.asc())
                .limit(1)
            )
            oldest = (await session.execute(stmt)).scalar_one_or_none()
            
            # Fetch pending count
            pending_stmt = (
                select(sqlalchemy.func.count(Outbox.id))
                .where(Outbox.env_id == self.env_id, Outbox.dispatched_at == None)
            )
            pending_count = (await session.execute(pending_stmt)).scalar_one()

            oldest_id = None
            age_seconds = 0.0

            if oldest is not None:
                oldest_id = oldest.id
                try:
                    created_time = datetime.fromisoformat(oldest.created_at)
                    if created_time.tzinfo is None:
                        created_time = created_time.replace(tzinfo=timezone.utc)
                    age_seconds = (datetime.now(timezone.utc) - created_time).total_seconds()
                    age_seconds = max(0.0, age_seconds)
                except Exception:
                    pass

            # 2. Update OutboxWatermark
            watermark = await session.get(OutboxWatermark, self.env_id)
            if watermark is None:
                watermark = OutboxWatermark(
                    env_id=self.env_id,
                    oldest_undispatched_id=oldest_id,
                    oldest_undispatched_age_seconds=age_seconds,
                    updated_at=datetime.now(timezone.utc).isoformat()
                )
                session.add(watermark)
            else:
                watermark.oldest_undispatched_id = oldest_id
                watermark.oldest_undispatched_age_seconds = age_seconds
                watermark.updated_at = datetime.now(timezone.utc).isoformat()

            # 3. Update RuntimeStatus
            status = await session.get(RuntimeStatus, self.env_id)
            if status is not None:
                status.outbox_pending = pending_count
                status.oldest_outbox_age_seconds = age_seconds
                
            await session.commit()
