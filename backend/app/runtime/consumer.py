"""Per-env consumer task (reshape v2).

A `ConsumerTask` wraps one `AIOKafkaConsumer` reading from a single
source topic. For every message it:

  1. Tries to JSON-parse. On failure, publishes to the env's DLQ (if
     configured) and increments `messages_failed`. Skips fan-out.
  2. For each domain grouping in position order, evaluates every match
     condition (OR-list of values). If any match condition in a DG
     matches, the message fans out to all of that DG's destinations.
  3. For each matched destination, builds headers (resolving
     `from_message` JMESPath) and produces the original raw payload to
     the destination topic.
  4. Commits offsets after the produce batch.

Per-destination failures are isolated: a 3-attempt retry with
exponential backoff (0.5s, 1s, 2s) is wrapped around each `send`. On
final failure, the consumer logs and increments `messages_failed` but
keeps running. The source consumer never stops because of a destination
problem.

Status updates (state, counters, last_message_at) are batched to
<= 1 write per second per env via a dirty flag.
"""
from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from aiokafka import AIOKafkaConsumer
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import session_scope
from app.models import (
    Destination,
    DomainGrouping,
    Env,
    Header,
    MatchCondition,
    MatchConditionValue,
    RuntimeStatus,
    SourceConfig,
)
from app.runtime.matcher import build_headers, evaluate_match_condition
from app.runtime.producer import DLQPublisher, ProducerPool, security_dict

log = logging.getLogger(__name__)


# A simple in-memory counter store so the manager can show counters
# before they're flushed to the DB. Keyed by env_id.
class _Counters:
    def __init__(self) -> None:
        self.consumed: int = 0
        self.routed: int = 0
        self.failed: int = 0
        self.unmatched: int = 0
        self.last_message_at: Optional[str] = None
        self._dirty: bool = True
        self._last_flush: float = 0.0

        self._consumed_window: deque = deque()
        self._routed_window: deque = deque()
        self._failed_window: deque = deque()
        self._start_time: float = time.time()

        self.consumed_rate: float = 0.0
        self.routed_rate: float = 0.0
        self.failed_rate: float = 0.0

    def _add_event(self, window: deque, count: int = 1) -> None:
        now_sec = int(time.time())
        cutoff = now_sec - 10
        while window and window[0][0] < cutoff:
            window.popleft()

        if window and window[-1][0] == now_sec:
            window[-1] = (now_sec, window[-1][1] + count)
        else:
            window.append((now_sec, count))

    def _get_rate(self, window: deque) -> float:
        now_sec = int(time.time())
        cutoff = now_sec - 10
        while window and window[0][0] < cutoff:
            window.popleft()

        total = sum(item[1] for item in window)
        elapsed = max(1.0, time.time() - self._start_time)
        denom = min(10.0, elapsed)
        return round(total / denom, 2)

    def update_rates(self) -> None:
        self.consumed_rate = self._get_rate(self._consumed_window)
        self.routed_rate = self._get_rate(self._routed_window)
        self.failed_rate = self._get_rate(self._failed_window)
        self._dirty = True

    def bump_consumed(self, n: int = 1) -> None:
        self.consumed += n
        self.last_message_at = datetime.now(timezone.utc).isoformat()
        self._add_event(self._consumed_window, n)
        self.update_rates()
        self._dirty = True

    def bump_routed(self, n: int) -> None:
        self.routed += n
        self._add_event(self._routed_window, n)
        self.update_rates()
        self._dirty = True

    def bump_failed(self, n: int = 1) -> None:
        self.failed += n
        self._add_event(self._failed_window, n)
        self.update_rates()
        self._dirty = True

    def bump_unmatched(self, n: int = 1) -> None:
        self.unmatched += n
        self._dirty = True


class ConsumerTask:
    """One consumer task per env. Lifecycle: `start()` then `stop()`."""

    RETRY_BACKOFFS = (0.5, 1.0, 2.0)

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
        self._counters = _Counters()
        # The loaded env payload, set in `_run` after a successful load.
        # Used by `_send_to_destination` to fall back to source security
        # when a destination inherits the source broker.
        self._env_payload: Optional[Dict[str, Any]] = None
        self._consumer: Optional[AIOKafkaConsumer] = None

    # ---------- lifecycle ----------

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return  # idempotent
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run(), name=f"consumer-{self.env_id}")

    async def stop(self, timeout: float = 10.0) -> None:
        if self._task is None:
            return
        self._stop_event.set()
        # Stop the consumer client immediately so that the `async for msg in consumer`
        # loop yields and exits cleanly without waiting for the timeout.
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            except Exception:
                pass
        try:
            await asyncio.wait_for(self._task, timeout=timeout)
        except asyncio.TimeoutError:
            log.warning("consumer %s did not stop within %ss, cancelling", self.env_id, timeout)
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        finally:
            self._task = None

    def is_running(self) -> bool:
        return self._task is not None and not self._task.done()

    # ---------- main loop ----------

    async def _cleanup(self, consumer: Optional[AIOKafkaConsumer]) -> None:
        if consumer is not None:
            try:
                await consumer.stop()
            except Exception as exc:  # noqa: BLE001
                log.warning("consumer stop failed: %s", exc)
        await self._flush_counters(force=True)
        await self._set_state("stopped", stopped_at=_now())

    async def _run(self) -> None:
        await self._set_state("starting", last_error=None, started_at=_now(), stopped_at=None)
        env_payload: Optional[Dict[str, Any]] = None
        try:
            env_payload = await self._load_env_payload()
            if env_payload is None:
                await self._set_state("error", last_error="env not found")
                return
            self._env_payload = env_payload

            source = env_payload["source"]
            sec = security_dict(
                security_protocol=source["security_protocol"],
                sasl_mechanism=source.get("sasl_mechanism"),
                sasl_username=source.get("sasl_username"),
                sasl_password=source.get("sasl_password"),
                ssl_ca_location=source.get("ssl_ca_location"),
            )
            self._consumer = AIOKafkaConsumer(
                source["topic"],
                bootstrap_servers=source["brokers"],
                group_id=source["consumer_group"],
                auto_offset_reset=source["offset_reset"],
                enable_auto_commit=True,
                **sec,
            )
            await self._consumer.start()
            await self._set_state("running", last_error=None)

            # Periodically flush counters to the DB.
            flusher = asyncio.create_task(self._counter_flusher(), name=f"flush-{self.env_id}")

            try:
                async for msg in self._consumer:
                    if self._stop_event.is_set():
                        break
                    await self._handle_message(msg, env_payload)
            finally:
                flusher.cancel()
                try:
                    await flusher
                except (asyncio.CancelledError, Exception):  # noqa: BLE001
                    pass

            await self._flush_counters(force=True)
        except Exception as exc:  # noqa: BLE001 — surface to status, keep manager alive
            log.exception("consumer %s crashed", self.env_id)
            await self._set_state("error", last_error=str(exc))
        finally:
            consumer = self._consumer
            self._consumer = None
            await asyncio.shield(self._cleanup(consumer))

    # ---------- per-message ----------

    async def _handle_message(self, msg: Any, env_payload: Dict[str, Any]) -> None:
        raw = msg.value  # bytes
        self._counters.bump_consumed()
        # 1. Try to parse JSON.
        try:
            parsed = json.loads(raw)
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "env %s: poison message at offset %s: %s",
                self.env_id,
                msg.offset,
                exc,
            )
            await self._route_to_dlq(raw, msg, env_payload, error_message=str(exc))
            self._counters.bump_failed()
            return

        # 2. Iterate domain groupings. For each DG, evaluate every match
        #    condition (OR-list). If any MC matches, fan out to ALL
        #    destinations of that DG.
        any_dg_matched = False
        routing_tasks = []
        for dg in env_payload["domain_groupings"]:
            matched = False
            for mc in dg["match_conditions"]:
                r = evaluate_match_condition(
                    key_path=mc["key_path"],
                    operator=mc["operator"],
                    values=mc["values"],
                    case_insensitive=bool(mc["case_insensitive"]),
                    message=parsed,
                )
                if r.matched:
                    matched = True
                    break
            if not matched:
                continue
            any_dg_matched = True
            for d in dg["destinations"]:
                routing_tasks.append(
                    self._send_to_destination(raw, d, parsed, msg, env_payload, key=msg.key)
                )

        if routing_tasks:
            await asyncio.gather(*routing_tasks)

        if not any_dg_matched:
            self._counters.bump_unmatched()
            msg_str = json.dumps(parsed)
            log.debug("env %s: unmatched message: %s", self.env_id, msg_str)
            if self._manager is not None:
                await self._manager.append_log(
                    self.env_id,
                    level="WARN",
                    message=f"No matching route found for message: {msg_str}",
                )
                await self._manager.log_unmatched_message(
                    self.env_id,
                    message_payload=msg_str,
                )

    async def _send_to_destination(
        self,
        raw_value: bytes,
        dest: Dict[str, Any],
        parsed_message: dict,
        msg: Any,
        env_payload: Dict[str, Any],
        *,
        key: Optional[bytes] = None,
    ) -> None:
        brokers = dest["brokers"] or env_payload["source"]["brokers"]
        sec = security_dict(
            security_protocol=dest["security_protocol"],
            sasl_mechanism=dest.get("sasl_mechanism"),
            sasl_username=dest.get("sasl_username"),
            sasl_password=dest.get("sasl_password"),
            ssl_ca_location=dest.get("ssl_ca_location"),
        )
        producer = await self._pool.get_producer(brokers=brokers, **sec)
        headers = build_headers(dest.get("headers", []), parsed_message)
        last_exc: Optional[Exception] = None
        for attempt, backoff in enumerate((0.0,) + self.RETRY_BACKOFFS):
            if backoff:
                await asyncio.sleep(backoff)
            try:
                await producer.send_and_wait(
                    topic=dest["topic"],
                    value=raw_value,
                    key=key,
                    headers=headers,
                )
                self._counters.bump_routed(1)
                return
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                log.warning(
                    "env %s -> %s attempt %d failed: %s",
                    self.env_id, dest["topic"], attempt + 1, exc,
                )
        log.error(
            "env %s -> %s: giving up after %d attempts: %s",
            self.env_id, dest["topic"], len(self.RETRY_BACKOFFS) + 1, last_exc,
        )
        self._counters.bump_failed()
        await self._route_to_dlq(
            raw_value,
            msg,
            env_payload,
            error_message=f"Destination {dest['topic']} produce failed after retries: {last_exc}",
        )

    async def _route_to_dlq(
        self,
        raw_value: bytes,
        msg: Any,
        env_payload: Dict[str, Any],
        *,
        error_message: str,
    ) -> None:
        dlq_topic = env_payload.get("dlq_topic")
        if not dlq_topic:
            return
        dlq_brokers = env_payload.get("dlq_brokers") or env_payload["source"]["brokers"]
        # Inherit source security for the DLQ producer. PRD does not
        # specify per-DLQ security; keeping it simple.
        src = env_payload["source"]
        try:
            await self._dlq.publish(
                raw_value=raw_value,
                dlq_topic=dlq_topic,
                dlq_brokers=dlq_brokers,
                security_protocol=src["security_protocol"],
                sasl_mechanism=src.get("sasl_mechanism"),
                sasl_username=src.get("sasl_username"),
                sasl_password=src.get("sasl_password"),
                ssl_ca_location=src.get("ssl_ca_location"),
                source_topic=msg.topic,
                source_partition=msg.partition,
                source_offset=msg.offset,
                error_message=error_message,
            )
        except Exception as exc:  # noqa: BLE001 — DLQ failures must not crash the source
            log.error("env %s: DLQ publish failed: %s", self.env_id, exc)

    # ---------- state & counters ----------

    async def _set_state(
        self,
        state: str,
        *,
        last_error: Optional[str] = None,
        started_at: Optional[str] = None,
        stopped_at: Optional[str] = None,
    ) -> None:
        async with session_scope() as session:
            row = await session.get(RuntimeStatus, self.env_id)
            if row is None:
                row = RuntimeStatus(env_id=self.env_id, state=state)
                session.add(row)
            else:
                row.state = state
            if last_error is not None:
                row.last_error = last_error
            if started_at is not None:
                row.started_at = started_at
            if stopped_at is not None:
                row.stopped_at = stopped_at
            await session.commit()

    async def _counter_flusher(self) -> None:
        """Periodically flush in-memory counters to the DB (max 1Hz)."""
        import time

        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
            await self._flush_counters()

    async def _flush_counters(self, force: bool = False) -> None:
        self._counters.update_rates()
        if not force and not self._counters._dirty:
            return
        async with session_scope() as session:
            row = await session.get(RuntimeStatus, self.env_id)
            if row is None:
                row = RuntimeStatus(
                    env_id=self.env_id,
                    state="running",
                    messages_consumed=self._counters.consumed,
                    messages_routed=self._counters.routed,
                    messages_failed=self._counters.failed,
                    messages_unmatched=self._counters.unmatched,
                    last_message_at=self._counters.last_message_at,
                    consumed_rate=self._counters.consumed_rate,
                    routed_rate=self._counters.routed_rate,
                    failed_rate=self._counters.failed_rate,
                )
                session.add(row)
            else:
                row.messages_consumed = self._counters.consumed
                row.messages_routed = self._counters.routed
                row.messages_failed = self._counters.failed
                row.messages_unmatched = self._counters.unmatched
                row.last_message_at = self._counters.last_message_at
                row.consumed_rate = self._counters.consumed_rate
                row.routed_rate = self._counters.routed_rate
                row.failed_rate = self._counters.failed_rate
            await session.commit()
        self._counters._dirty = False

    # ---------- env loading ----------

    async def _load_env_payload(self) -> Optional[Dict[str, Any]]:
        """Load a self-contained dict for the env: source + domain
        groupings + match_conditions + values + destinations + headers.
        The consumer stashes the result on `self._env_payload` so
        `_send_to_destination` can fall back to source security when a
        destination inherits the source broker."""
        from sqlalchemy.orm import selectinload

        async with session_scope() as session:
            stmt = (
                select(Env)
                .where(Env.id == self.env_id)
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
                return None
            return _env_to_dict(env)


def _env_to_dict(env: Env) -> Dict[str, Any]:
    src = env.source
    return {
        "id": env.id,
        "dlq_topic": env.dlq_topic,
        "dlq_brokers": env.dlq_brokers,
        "source": {
            "brokers": src.brokers,
            "topic": src.topic,
            "consumer_group": src.consumer_group,
            "offset_reset": src.offset_reset,
            "security_protocol": src.security_protocol,
            "sasl_mechanism": src.sasl_mechanism,
            "sasl_username": src.sasl_username,
            "sasl_password": src.sasl_password,
            "ssl_ca_location": src.ssl_ca_location,
        },
        "domain_groupings": [
            {
                "name": dg.name,
                "match_conditions": [
                    {
                        "key_path": mc.key_path,
                        "operator": mc.operator,
                        "case_insensitive": bool(mc.case_insensitive),
                        "values": [v.value for v in mc.values],
                    }
                    for mc in dg.match_conditions
                ],
                "destinations": [_dest_to_dict(d) for d in dg.destinations],
            }
            for dg in env.domain_groupings
        ],
    }


def _dest_to_dict(d: Destination) -> Dict[str, Any]:
    return {
        "use_source_broker": bool(d.use_source_broker),
        "brokers": d.brokers,
        "topic": d.topic,
        "security_protocol": d.security_protocol,
        "sasl_mechanism": d.sasl_mechanism,
        "sasl_username": d.sasl_username,
        "sasl_password": d.sasl_password,
        "ssl_ca_location": d.ssl_ca_location,
        "headers": [
            {"name": h.name, "value": h.value, "mode": h.mode} for h in d.headers
        ],
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
