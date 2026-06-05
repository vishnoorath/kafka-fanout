### Track 0 — X-Source-Coord header (do first, smallest, de-risks everything else)

1. In `app/runtime/matcher.py`, add a tiny helper `build_x_source_coord(msg) -> tuple[str, bytes]` returning `("X-Source-Coord", f"{msg.topic}:{msg.partition}:{msg.offset}".encode("utf-8"))`. Keeps the consumer clean.
2. In `app/runtime/consumer.py` `_send_to_destination`, prepend the result of the helper to the `headers` list before calling `producer.send_and_wait`. Order doesn't matter functionally, but document the convention (system headers first).
3. In `app/runtime/consumer.py` `_route_to_dlq`, add the same header to the DLQ message headers alongside the existing `__error` header.
4. In `app/runtime/producer.py` `DLQPublisher.publish`, extend the signature to accept a `source_coord_header: Optional[bytes] = None` and include it in the headers list when provided.
5. Add a unit test that asserts `build_x_source_coord` returns the expected format and that a produced message carries the header.
6. Update the PRD §6.3 / §4.3 to document the header as always-on, system-managed, not user-editable.

### Track 1 — DB schema for outbox

7. In `app/models.py`, add three new ORM models: `Outbox`, `OutboxDeadLetter`, `OutboxWatermark`. Use the columns I sketched earlier. `Outbox.idempotency_key` has `UNIQUE` constraint. Add `idx_outbox_pending` partial index on `(env_id, id) WHERE dispatched_at IS NULL` (Postgres-only; on SQLite, a regular index is fine).
8. In `app/db.py` `init_db`, ensure the new tables are created.
9. Write a migration test: create env, write outbox row twice with same `idempotency_key`, assert second insert raises (or returns rowcount=0 with `INSERT OR IGNORE`).

### Track 2 — Outbox writer branch in `ConsumerTask`

10. In `app/runtime/consumer.py`, add a `mode: str` field to `ConsumerTask.__init__` (default `"at_least_once"`). Store it on `self`.
11. In `_run`, branch on `self._mode`:
    - `"at_least_once"`: existing flow unchanged.
    - `"outbox"`: set `enable_auto_commit=False`. New helper `_write_to_outbox(parsed, raw, msg, env_payload)` replaces the per-destination `routing_tasks` gather.
12. Implement `_write_to_outbox`:
    - Compute `idempotency_key = f"{msg.topic}:{msg.partition}:{msg.offset}"`.
    - For each matched DG, for each destination: build headers via `build_headers(...)` (existing function), then prepend `build_x_source_coord(msg)`.
    - Serialize the row: `(env_id, idempotency_key, payload=raw, headers_json=..., destinations_json=...)`.
    - Use `INSERT INTO outbox (...) ON CONFLICT(idempotency_key) DO NOTHING` (or SQLite's `INSERT OR IGNORE`).
    - On conflict: log "duplicate outbox entry, crash-recovery replay — skipping"; still advance the watermark.
    - On success: advance the in-memory `next_offset_to_commit` for the (topic, partition) pair.
13. Add a new method `_commit_offsets_periodically` that flushes `next_offset_to_commit` to Kafka via `await self._consumer.commit({TopicPartition(topic, partition): OffsetAndMetadata(offset + 1, "")})` at most 1Hz. Throttle to avoid commit storms.
14. The DLQ path inside `_handle_message` (poison message) still writes to the existing DLQ topic, but in outbox mode you can choose: either keep the current direct-to-DLQ behavior, or write a special "poison" outbox row. Recommend keeping current behavior — DLQ is a different concern.

### Track 3 — Outbox dispatcher

15. New file `app/runtime/outbox_dispatcher.py` with class `OutboxDispatcher`. Constructor takes `env_id`, `pool: ProducerPool`, `dlq: DLQPublisher`, `manager`. Mirrors the lifecycle of `ConsumerTask` (start, stop, is_running).
16. The dispatch loop:
    - Poll: `SELECT * FROM outbox WHERE env_id = ? AND dispatched_at IS NULL ORDER BY id LIMIT N`. (For multi-process safety later, switch to `FOR UPDATE SKIP LOCKED`; for v1 with one process, simple SELECT is fine.)
    - For each row: for each destination in `destinations_json`, produce via `ProducerPool.get_producer(...)` + `send_and_wait(value=row.payload, headers=row.headers, key=...)`. Use the same `RETRY_BACKOFFS` from `ConsumerTask`.
    - If all destinations succeed: `UPDATE outbox SET dispatched_at = ? WHERE id = ?`.
    - If any destination fails after retries: `UPDATE outbox SET attempts = attempts + 1, last_error = ? WHERE id = ?`. If `attempts >= max_attempts`, move to `outbox_dead_letters`.
17. Add counters to your existing `_Counters` class (or a new dispatcher-side counter): `outbox_dispatched`, `outbox_failed`, `outbox_dead_lettered`, `outbox_pending`. Wire them into the same 1Hz flusher pattern.
18. Add an `OutboxWatermark` row per env for observability ("oldest undispatched outbox row age in seconds").

### Track 4 — Manager integration

19. In `app/runtime/manager.py` `start(env_id, mode="at_least_once")`: in outbox mode, start both `ConsumerTask(env_id, pool, dlq, manager=self, mode="outbox")` and `OutboxDispatcher(env_id, pool, dlq, manager=self)`. Store both on `self._tasks` (change to a small dataclass instead of a bare `ConsumerTask`).
20. In `stop(env_id)`: stop the dispatcher first (let it finish in-flight), then stop the consumer (let it finish writing to outbox), then close producers via pool.
21. In `aclose_all()`: same order — dispatcher first, consumer second.
22. In `reset_offsets(env_id)`: in outbox mode, also `DELETE FROM outbox WHERE env_id = ?` and `DELETE FROM outbox_dead_letters WHERE env_id = ?` so the new run starts clean. Document this — it's destructive and should be UI-confirmed.
23. Add `OutboxDispatcher` import + `manager` calls in `auto_resume()` (only resume envs whose `delivery_mode == "outbox"` AND state is `running`/`starting`).

### Track 5 — Status / UI surface

24. Add a `delivery_mode` field to the env config (default `at_least_once`). Pydantic schema + SQLAlchemy column + migration. Validate at save time: `outbox` is always allowed; `at_least_once` is always allowed.
25. Add status fields to `RuntimeStatus` (or new table if cleaner): `delivery_mode`, `outbox_pending`, `outbox_dispatched_total`, `outbox_failed_total`, `outbox_dead_lettered_total`, `oldest_outbox_age_seconds`.
26. In the React UI, add a dropdown in the EnvHeader for `Delivery mode: At-least-once (default) | Outbox (EOS-like)`. Add a help tooltip explaining the trade-off (outbox needs downstream dedup).
27. In the status panel, render outbox counters when the env is in outbox mode. Add a "View dead letters" link that calls a new `GET /api/envs/{id}/outbox/dead-letters` endpoint.

### Track 6 — End-to-end testing

28. Add a docker-compose with Redpanda for local dev (so the coding agent can actually run the outbox path without a managed Kafka).
29. Write an integration test: start env in outbox mode → publish N messages → kill the dispatcher mid-flight → restart → assert no duplicates on the destination topic (consume with a test consumer that records all `X-Source-Coord` values, assert set size == N).
30. Write an integration test for the "destination cluster is down" case: stop the destination broker → produce → assert outbox rows accumulate with `attempts > 0` and `last_error` populated → start broker → assert dispatcher drains and marks rows dispatched.

### Track 7 — PRD update

31. Update the PRD with:
    - `delivery_mode` field in §4.1 and §7.
    - X-Source-Coord header documented in §4.3 and §6.3 as always-on system header.
    - New §18 "Outbox Pattern (delivery mode = outbox)" with: schema sketch, writer branch flow, dispatcher flow, crash-recovery semantics, downstream dedup requirement.
    - Acceptance criteria §10 split into "At-least-once mode" and "Outbox mode" sub-lists. The outbox-mode criteria require: no duplicates on destination after crash-recovery replay, dispatcher drains automatically when destination recovers, dead-letter table populated after max attempts, `X-Source-Coord` present on every produced message.

## Sequencing recommendation

Do **Track 0 first** (X-Source-Coord). It's 30 minutes of work, zero risk, and you immediately get the downstream dedup + tracing benefit. Then **Track 1 + 2** (schema + writer branch). That's the heart of the change. Then **Track 3** (dispatcher). Then **Track 4** (manager wiring). **Track 5–7** can run in parallel with the test work.

---

# System Specifications

## §19. Always-On System Headers (X-Source-Coord)
Every message routed through the kafka-fanout consumer (both in "at-least-once" and "outbox" delivery modes) will automatically include a system-managed, non-user-editable header:
* **Header Name**: `X-Source-Coord`
* **Format**: `<source_topic>:<partition>:<offset>` (as UTF-8 encoded string)
This header is intended for downstream consumers to perform exact message deduplication and trace processing coordinates back to the source Kafka partition.

## §20. Outbox Pattern Specifications
When `delivery_mode` is set to `"outbox"`, the system routes messages using a local database outbox table rather than direct Kafka writes.
* **Database Tables**:
  * `outbox`: Holds pending messages to be dispatched. Uses a unique constraint on `idempotency_key` (format `topic:partition:offset`) to deduplicate crash-recovery replays.
  * `outbox_dead_letters`: Holds failed messages that exceeded maximum dispatch attempts.
  * `outbox_watermark`: Tracks the oldest undispatched outbox row's age per environment.

