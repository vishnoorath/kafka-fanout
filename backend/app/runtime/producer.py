"""Per-broker producer pool.

One `AIOKafkaProducer` per unique (brokers, security) tuple, cached and
reused across destinations and messages. The pool exposes:

* `get_producer(brokers, security)` -> AIOKafkaProducer (idempotent)
* `aclose_all()` for clean shutdown
* `security_dict(security)` to convert a SourceConfig-style dict into
  the dict `aiokafka` expects (so we don't repeat the `if protocol ==
  "SASL_SSL": ...` ladder in the consumer).

`DLQProducer` is a thin wrapper used by the consumer to publish
parse-failed messages to a per-env DLQ topic.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

from aiokafka import AIOKafkaProducer

log = logging.getLogger(__name__)


# ---------- security dict builder ----------


def security_dict(
    *,
    security_protocol: str,
    sasl_mechanism: Optional[str],
    sasl_username: Optional[str],
    sasl_password: Optional[str],
    ssl_ca_location: Optional[str],
) -> Dict[str, Any]:
    """Build the dict `aiokafka.AIOKafkaProducer` / `AIOKafkaConsumer`
    expects for auth.

    `ssl_cafile` is only included when the protocol is `SSL` or
    `SASL_SSL` — aiokafka rejects it otherwise. Same for the SASL
    kwargs, which only make sense for `SASL_*` protocols.
    """
    out: Dict[str, Any] = {"security_protocol": security_protocol}
    if security_protocol in ("SASL_PLAINTEXT", "SASL_SSL"):
        out["sasl_mechanism"] = sasl_mechanism
        out["sasl_plain_username"] = sasl_username
        out["sasl_plain_password"] = sasl_password
    if security_protocol in ("SSL", "SASL_SSL") and ssl_ca_location:
        out["ssl_cafile"] = ssl_ca_location
    return out


# ---------- producer pool ----------


def _pool_key(
    brokers: str,
    security_protocol: str,
    sasl_mechanism: Optional[str],
    sasl_username: Optional[str],
    ssl_ca_location: Optional[str],
) -> Tuple[str, str, Optional[str], Optional[str], Optional[str]]:
    return (brokers, security_protocol, sasl_mechanism, sasl_username, ssl_ca_location)


class ProducerPool:
    """Caches one producer per unique broker/security config."""

    def __init__(self) -> None:
        self._producers: Dict[Tuple, AIOKafkaProducer] = {}
        self._lock = asyncio.Lock()

    async def get_producer(
        self,
        brokers: str,
        security_protocol: str,
        sasl_mechanism: Optional[str] = None,
        sasl_username: Optional[str] = None,
        sasl_password: Optional[str] = None,
        ssl_ca_location: Optional[str] = None,
    ) -> AIOKafkaProducer:
        key = _pool_key(
            brokers,
            security_protocol,
            sasl_mechanism,
            sasl_username,
            ssl_ca_location,
        )
        async with self._lock:
            existing = self._producers.get(key)
            if existing is not None:
                return existing
            sec = security_dict(
                security_protocol=security_protocol,
                sasl_mechanism=sasl_mechanism,
                sasl_username=sasl_username,
                sasl_password=sasl_password,
                ssl_ca_location=ssl_ca_location,
            )
            producer = AIOKafkaProducer(
                bootstrap_servers=brokers,
                acks="all",
                enable_idempotence=True,
                max_batch_size=131072,
                # Bound individual produce RPCs so a stuck broker can't
                # wedge the producer (and therefore shutdown) indefinitely.
                request_timeout_ms=30000,
                # Back off between retries instead of hammering the broker
                # when min.insync.replicas isn't satisfied.
                retry_backoff_ms=500,
                **sec,
            )
            await producer.start()
            self._producers[key] = producer
            log.info("producer started brokers=%s protocol=%s", brokers, security_protocol)
            return producer

    async def aclose_all(self) -> None:
        async with self._lock:
            producers = list(self._producers.values())
            self._producers.clear()
        for p in producers:
            try:
                await p.stop()
            except Exception as exc:  # noqa: BLE001
                log.warning("producer stop failed: %s", exc)


# ---------- DLQ helper ----------


class DLQPublisher:
    """Publishes poison messages to a per-env DLQ topic with an `__error` header.

    The header is a JSON object: {ts, source_topic, source_partition,
    source_offset, error}. The original raw payload is sent as the
    message body so consumers can reprocess it after a fix.
    """

    def __init__(self, pool: ProducerPool) -> None:
        self._pool = pool

    async def publish(
        self,
        *,
        raw_value: bytes,
        dlq_topic: str,
        dlq_brokers: str,
        security_protocol: str,
        sasl_mechanism: Optional[str],
        sasl_username: Optional[str],
        sasl_password: Optional[str],
        ssl_ca_location: Optional[str],
        source_topic: str,
        source_partition: int,
        source_offset: int,
        error_message: str,
        source_coord_header: Optional[bytes] = None,
    ) -> None:
        producer = await self._pool.get_producer(
            brokers=dlq_brokers,
            security_protocol=security_protocol,
            sasl_mechanism=sasl_mechanism,
            sasl_username=sasl_username,
            sasl_password=sasl_password,
            ssl_ca_location=ssl_ca_location,
        )
        error_header = json.dumps(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "source_topic": source_topic,
                "source_partition": source_partition,
                "source_offset": source_offset,
                "error": error_message,
            }
        ).encode("utf-8")
        headers = []
        if source_coord_header is not None:
            headers.append(("X-Source-Coord", source_coord_header))
        headers.append(("__error", error_header))
        await producer.send_and_wait(
            topic=dlq_topic,
            value=raw_value,
            headers=headers,
        )

