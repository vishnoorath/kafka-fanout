# Kafka Fan-Out Configurator — PRD

**Status:** v1 draft, ready for implementation
**Audience:** Local coding agent implementing both tiers end-to-end
**Repo layout:** Monorepo with `backend/` (FastAPI + SQLite) and `kafka-fanout-ui/` (React + Vite)

---

## 1. Overview

A two-tier tool for designing and operating **1-to-N Kafka message fan-out** routing rules, parameterized per environment (DEV / TEST / UAT / PROD or any custom name).

- **Frontend (React + Vite):** visual designer for environments, source Kafka config, conditional mappings, destination topics, and message headers. Includes a live "Test message" sandbox.
- **Backend (FastAPI + SQLite):** persists configurations, exposes a REST API for the UI, and runs in-process async Kafka workers that consume from a source topic and fan out to N destination topics per the rules defined in the UI.

The fan-out logic is **data-driven**: every behavior (which source, which conditions match, where to route, what headers to attach) comes from the config in SQLite, with zero code changes required to add or change a route.

---

## 2. Goals & Non-Goals

### Goals (v1)

- Define multiple environments, each with one source Kafka topic and any number of conditional mappings.
- Each mapping evaluates a **JMESPath** expression against the inbound message, applies one of three operators (`equals`, `not equals`, `contains`), case-insensitive by default, and routes matched messages to one or more destination topics.
- Destinations may live on the **same** Kafka cluster as the source or on a **different** cluster.
- Each produced message can carry arbitrary headers, each either a static string or a value pulled from the message via JMESPath.
- Start / stop the consumer per environment from the UI; show live runtime status and recent log lines.
- Persist everything in SQLite; export / import the full set of envs as a single JSON document.
- "Test message" panel in the UI: paste a sample JSON, see which mappings match and what headers would be produced, fully client-side (no backend call).

### Non-Goals (v1)

- Multi-tenant auth, RBAC, audit log of config changes.
- Schema registry / Avro / Protobuf — payloads are treated as JSON. Non-JSON payloads are passed through with a warning.
- Exactly-once semantics. At-least-once with idempotent producers is acceptable; document the trade-off.
- Web UI for editing the actual messages in flight.
- Horizontal scaling of workers beyond what one Python process can handle. The runtime manager exposes a clean seam for future extraction to a worker pool.
- Auto-discovery of topics / broker metadata.

---

## 3. Personas

- **Integration engineer** — primary user. Designs and maintains routing rules across environments.
- **Platform / SRE** — secondary. Operates the service, watches runtime status, restarts consumers.

---

## 4. Functional Requirements

### 4.1 Environment management

- Create, rename, delete, duplicate an environment.
- Environment has: `name` (free text, unique, required), `description` (optional), `enabled` (bool — master switch, not the same as "consumer running").
- Sidebar lists all envs. Selecting one loads its config in the main panel.

### 4.2 Source Kafka configuration (per environment, 1:1)

Fields:
- `brokers` — comma-separated host:port list, required.
- `topic` — required.
- `consumer_group` — required.
- `offset_reset` — radio: `earliest` | `latest`. Applied only when the consumer group has no committed offset for the topic.
- Security (collapsible "Advanced" section):
  - `security_protocol` — `PLAINTEXT` | `SSL` | `SASL_PLAINTEXT` | `SASL_SSL`. Default `PLAINTEXT`.
  - `sasl_mechanism` — `PLAIN` | `SCRAM-SHA-256` | `SCRAM-SHA-512`. Required when protocol is SASL_*.
  - `sasl_username`, `sasl_password` — required when SASL.
  - `ssl_ca_location` — optional path to CA bundle; required when SSL and using a private CA.

### 4.3 Message mapping (per environment, 1:N)

A mapping is a **condition + a list of destinations**. Order matters and is preserved.

**Condition fields:**
- `key_path` — JMESPath expression evaluated against the parsed JSON message. Required.
  - Example: `Message.TableName` to extract `payload.Message.TableName`.
- `operator` — `equals` | `not_equals` | `contains`. Default `equals`.
- `value` — string to compare against. Required.
- `case_insensitive` — bool, default `true`.

**Evaluation rules:**
- If `key_path` returns multiple values, treat the first match (JMESPath default). If it returns nothing, the condition does not match.
- If the resolved value is not a string, coerce with `str(value)` before comparison.
- `contains` is substring; `equals` / `not_equals` are full equality after (optional) case folding.

**Destination fields:**
- `use_source_broker` — bool, default `true`. When true, broker + security fields are inherited from the source.
- `brokers` — required when `use_source_broker = false`.
- `topic` — required.
- Security fields — same set as the source, optional (inherited from source when `use_source_broker = true`).

**Header fields (per destination, 0:N):**
- `name` — header key, required.
- `value` — required.
- `mode` — `static` | `from_message`. Default `static`.
  - `static`: header value is the literal `value`.
  - `from_message`: `value` is a JMESPath expression evaluated against the message at produce time; non-string results are coerced to `str(...)`.

### 4.4 Runtime control

- `POST /api/envs/{id}/start` — spawn the consumer task. Idempotent: no-op if already running.
- `POST /api/envs/{id}/stop` — cancel the consumer task cleanly. Idempotent.
- Consumer must reconnect with exponential backoff on transient errors.
- Per-env failure isolation: one destination failing must not stop the source consumer or other destinations.

### 4.5 Status & logs

- `GET /api/envs/{id}/status` returns: `state` (`stopped` | `starting` | `running` | `error`), `last_error`, `started_at`, `stopped_at`, `messages_consumed`, `messages_routed`, `messages_failed`, `last_message_at`.
- `GET /api/envs/{id}/logs?limit=200` returns the most recent N log lines (default 200, max 1000) for that env.
- A ring buffer of the last 500 log lines per env is kept in memory and persisted to a `runtime_logs` table (capped).

### 4.6 Test message (client-side only)

The Mappings tab has a "Test message" panel:
- A monospaced JSON editor (textarea is fine for v1).
- On change, the UI evaluates every mapping against the message and shows:
  - The resolved value of `key_path` (or an error if the JMESPath is invalid).
  - The boolean result of the operator.
  - For each matched destination: the list of headers that would be produced, with `from_message` headers showing their resolved values.
- Pure client-side JMESPath evaluation using the same library the backend uses. No network call.

### 4.7 Export / Import

- `GET /api/export` returns `{ version: 1, envs: [...] }` — one JSON document with all envs. Secrets are **omitted** from export; a re-import will require secrets to be re-entered (intentional).
- `POST /api/import` accepts the same shape; merges by `name` (existing env with that name is replaced).

### 4.8 Secrets handling

- SASL passwords are write-only in the API: accepted in `POST` / `PUT`, never returned in any `GET`.
- The UI stores a `••••••••` placeholder once a secret has been saved; leaving the field unchanged on update means "keep existing secret."

---

## 5. UI / UX Requirements

### 5.1 Layout

```
┌─────────────────┬───────────────────────────────────────────┐
│ Sidebar         │ EnvHeader (name, description, save state) │
│  + New Env      ├───────────────────────────────────────────┤
│  • DEV          │ Tabs: [ Source ]  [ Mappings (N) ]        │
│  • UAT  ◀ active│                                           │
│  • PROD         │ (active tab content)                      │
│                 │ [ Test message ] (under Mappings tab)     │
│                 │ [ Export JSON ]  [ Start / Stop ]         │
└─────────────────┴───────────────────────────────────────────┘
```

### 5.2 Sidebar

- "+ New Environment" button at top.
- List rows: name, small status pill (Stopped / Running / Error), overflow menu (Duplicate, Delete, Rename).
- Active env is highlighted.
- Empty state: centered "No environments yet — create one to get started."

### 5.3 Source tab

- Single form, grouped:
  - Connection (brokers, topic, consumer group, offset_reset).
  - Advanced (collapsible) — security protocol, SASL, SSL.
- Validation: brokers non-empty, topic non-empty, consumer group non-empty. SASL fields required when protocol is SASL_*.

### 5.4 Mappings tab

- List of mapping cards. Each card header: position, "Match: `<preview>`" (e.g. `Message.TableName equals "Cattles" [CI]`), collapse/expand.
- Expanded card body: condition editor (left) + destinations list (right or below on narrow screens).
- "+ Add mapping" at the bottom of the list.
- Empty state: "No mappings yet — add one to start routing."

### 5.5 Condition editor

- `key_path` (JMESPath) input with a one-line hint underneath: "Use JMESPath syntax, e.g. `Message.TableName` or `items[0].id`."
- Operator dropdown.
- `value` input.
- `case_insensitive` toggle.
- Live preview line: `Message.TableName equals "Cattles" (case-insensitive)`.

### 5.6 Destination editor

- `use_source_broker` toggle at the top. When on, broker + security fields are hidden / disabled with an "(inherited)" note.
- Topic input.
- Advanced (collapsible) for security.
- Headers list with add / remove. Each header row: name, value, mode (`static` | `from_message`). When `from_message` is selected, show a small "→ preview" with the evaluated value from the Test message panel (if a test message is present).

### 5.7 Test message panel

- Sticky at the bottom of the Mappings tab, collapsible.
- Per mapping: show match badge (matched / not matched / error) plus resolved value.
- Per matched mapping: list destinations with computed headers.

### 5.8 Visual style

- Single CSS file with CSS variables for colors, spacing, radii. No UI framework dependency.
- Light theme only in v1; structure the CSS so a dark theme is a one-file addition later.
- No emoji as UI icons. Plain text labels and SVG icons only if needed.

---

## 6. Technical Architecture

### 6.1 Frontend

- **Stack:** Vite 5 + React 18, plain JavaScript (no TypeScript in v1), plain CSS.
- **State:** single `useReducer` in `App`, persisted to `localStorage` for offline drafts. Authoritative state lives on the backend; the UI hydrates from `GET /api/envs` on load and re-fetches after writes.
- **HTTP:** tiny `fetch` wrapper in `src/lib/api.js`. No axios.
- **JMESPath:** `jmespath` npm package for client-side Test message evaluation.
- **Build target:** ES2020, modern browsers. No IE / legacy support.

### 6.2 Backend

- **Stack:** Python 3.11+, FastAPI, Uvicorn, SQLAlchemy 2.x (async), aiosqlite, aiokafka, pydantic v2, jmespath.
- **Process model:** single Uvicorn process. One asyncio task per active environment's consumer, managed by a singleton `RuntimeManager`. Lifecycle hooked into FastAPI `lifespan` to stop all consumers cleanly on shutdown.
- **DB file:** `backend/kafka_fanout.db` (path configurable via env var). Auto-created on first run. No Alembic — single `init_db()` on startup.
- **CORS:** allow the Vite dev origin (`http://localhost:5173`) and the production origin (configurable).

### 6.3 Runtime behavior

- **Consumer per env:** `aiokafka.AIOKafkaConsumer` started with:
  - `bootstrap_servers` = source brokers
  - `group_id` = source consumer group
  - `auto_offset_reset` = env's `offset_reset` (used only on first run; existing groups honor committed offsets)
  - `enable_auto_commit` = `True` (commit after each successful produce batch)
- **Per message:**
  1. Try `json.loads(value)`. On failure: emit a warning log, skip fan-out for this message (do not produce anywhere), increment `messages_failed`. (See open question 8.A.)
  2. For each mapping in order, evaluate the condition.
  3. For each matched mapping, for each destination: build headers (evaluating `from_message` JMESPath entries), produce.
- **Producer per broker:** one `AIOKafkaProducer` per unique broker set, pooled, reused across destinations and messages. Producer config: `acks=all`, `enable_idempotence=True`, bounded in-flight via `max_batch_size`.
- **Per-destination isolation:** each `produce()` call wrapped in try/except with a 3-attempt retry (exponential backoff 0.5s → 2s → 5s). On final failure: log + increment `messages_failed` for that destination. Do not let the exception propagate to the source consumer loop.
- **Shutdown:** on `stop()` or app shutdown, finish the in-flight message, commit offsets, close producer, close consumer. Use `asyncio.wait_for(..., timeout=10)` to avoid hanging.

### 6.4 Observability

- `runtime_status` table tracks per-env state and counters. Updated on every state transition and at most every 1s during steady-state to avoid write storms.
- `runtime_logs` table: append-only, ring-buffered (capped at 500 rows per env; oldest are pruned on insert).
- All log lines also forwarded to stdout via standard logging so they appear in the Uvicorn console.

---

## 7. Data Model (SQLite)

```text
envs
  id              TEXT PRIMARY KEY          -- uuid
  name            TEXT NOT NULL UNIQUE
  description     TEXT NOT NULL DEFAULT ''
  enabled         INTEGER NOT NULL DEFAULT 0
  created_at      TEXT NOT NULL             -- ISO 8601
  updated_at      TEXT NOT NULL

source_configs
  env_id              TEXT PRIMARY KEY REFERENCES envs(id) ON DELETE CASCADE
  brokers             TEXT NOT NULL
  topic               TEXT NOT NULL
  consumer_group      TEXT NOT NULL
  offset_reset        TEXT NOT NULL CHECK (offset_reset IN ('earliest','latest'))
  security_protocol   TEXT NOT NULL DEFAULT 'PLAINTEXT'
  sasl_mechanism      TEXT
  sasl_username       TEXT
  sasl_password       TEXT
  ssl_ca_location     TEXT

mappings
  id                  TEXT PRIMARY KEY
  env_id              TEXT NOT NULL REFERENCES envs(id) ON DELETE CASCADE
  position            INTEGER NOT NULL
  key_path            TEXT NOT NULL
  operator            TEXT NOT NULL CHECK (operator IN ('equals','not_equals','contains'))
  value               TEXT NOT NULL
  case_insensitive    INTEGER NOT NULL DEFAULT 1
  UNIQUE (env_id, position)

destinations
  id                  TEXT PRIMARY KEY
  mapping_id          TEXT NOT NULL REFERENCES mappings(id) ON DELETE CASCADE
  position            INTEGER NOT NULL
  use_source_broker   INTEGER NOT NULL DEFAULT 1
  brokers             TEXT
  topic               TEXT NOT NULL
  security_protocol   TEXT NOT NULL DEFAULT 'PLAINTEXT'
  sasl_mechanism      TEXT
  sasl_username       TEXT
  sasl_password       TEXT
  ssl_ca_location     TEXT
  UNIQUE (mapping_id, position)

headers
  id                  TEXT PRIMARY KEY
  destination_id      TEXT NOT NULL REFERENCES destinations(id) ON DELETE CASCADE
  position            INTEGER NOT NULL
  name                TEXT NOT NULL
  value               TEXT NOT NULL
  mode                TEXT NOT NULL CHECK (mode IN ('static','from_message'))
  UNIQUE (destination_id, position)

runtime_status
  env_id              TEXT PRIMARY KEY REFERENCES envs(id) ON DELETE CASCADE
  state               TEXT NOT NULL         -- stopped | starting | running | error
  last_error          TEXT
  started_at          TEXT
  stopped_at          TEXT
  messages_consumed   INTEGER NOT NULL DEFAULT 0
  messages_routed     INTEGER NOT NULL DEFAULT 0
  messages_failed     INTEGER NOT NULL DEFAULT 0
  last_message_at     TEXT

runtime_logs
  id                  INTEGER PRIMARY KEY AUTOINCREMENT
  env_id              TEXT NOT NULL REFERENCES envs(id) ON DELETE CASCADE
  ts                  TEXT NOT NULL
  level               TEXT NOT NULL
  message             TEXT NOT NULL
  INDEX (env_id, ts)
```

All boolean columns are stored as `INTEGER` (0/1). All timestamps are ISO 8601 UTC strings.

---

## 8. API Contract

Base path: `/api`. All request/response bodies are JSON.

### Auth
None in v1. The service is assumed to run on a trusted network or behind a reverse proxy.

### Error shape
```json
{ "error": { "code": "string_code", "message": "human readable", "details": {} } }
```
Status codes follow REST conventions: 200 / 201 / 204 / 400 / 404 / 409 / 422 / 500.

### Endpoints

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness probe. Returns `{ "status": "ok" }`. |
| GET | `/envs` | List envs. Secrets redacted. |
| POST | `/envs` | Create env. Body = full env config. Returns 201 with the created env (secrets redacted). |
| GET | `/envs/{id}` | Fetch full env config (secrets redacted). |
| PUT | `/envs/{id}` | Replace env config atomically. Body = full env config. |
| DELETE | `/envs/{id}` | Delete env and stop its consumer if running. |
| POST | `/envs/{id}/duplicate` | Duplicate env (secrets not copied). Returns 201. |
| POST | `/envs/{id}/start` | Start consumer. Idempotent. |
| POST | `/envs/{id}/stop` | Stop consumer. Idempotent. |
| GET | `/envs/{id}/status` | Runtime status snapshot. |
| GET | `/envs/{id}/logs?limit=200` | Recent log lines, newest first. |
| POST | `/envs/{id}/test` | Dry-run a sample message server-side. Body `{ "message": <json> }`. Returns matched mappings + computed headers per destination. |
| GET | `/export` | Export all envs (secrets omitted). |
| POST | `/import` | Import envs. Body = `{ "version": 1, "envs": [...] }`. Merge by name. |

### Request / response bodies

**Env payload (write side, full replace):**
```json
{
  "name": "UAT",
  "description": "User acceptance testing",
  "source": {
    "brokers": "kafka-uat.internal:9092",
    "topic": "source.events",
    "consumer_group": "fanout-uat",
    "offset_reset": "earliest",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "SCRAM-SHA-512",
    "sasl_username": "fanout",
    "sasl_password": "REDACTED-OR-NEW"
  },
  "mappings": [
    {
      "key_path": "Message.TableName",
      "operator": "equals",
      "value": "Cattles",
      "case_insensitive": true,
      "destinations": [
        {
          "use_source_broker": true,
          "topic": "cattles.route.A",
          "headers": [
            { "name": "X-Domain",     "value": "Connector",       "mode": "static" },
            { "name": "X-TableName",  "value": "Message.TableName","mode": "from_message" }
          ]
        }
      ]
    }
  ]
}
```

**Env payload (read side, GET responses):** identical structure, except `source.sasl_password` and any `destinations[].sasl_password` are returned as `null` (never echoed back).

**Import shape:**
```json
{
  "version": 1,
  "envs": [ /* array of env payloads without secrets */ ]
}
```

**Test endpoint request:**
```json
{ "message": { "Message": { "TableName": "Cattles", "Op": "INSERT" } } }
```

**Test endpoint response:**
```json
{
  "results": [
    {
      "mapping_index": 0,
      "key_path": "Message.TableName",
      "resolved": "Cattles",
      "matched": true,
      "destinations": [
        {
          "topic": "cattles.route.A",
          "headers": [
            { "name": "X-Domain",    "value": "Connector" },
            { "name": "X-TableName", "value": "Cattles" }
          ]
        }
      ]
    },
    {
      "mapping_index": 1,
      "matched": false,
      "reason": "resolved value did not match"
    }
  ]
}
```

---

## 9. Configuration Examples

### 9.1 Same-broker fan-out

Source `kafka:9092`, topic `orders.v1`. One mapping: any order with `country = "US"` is duplicated into `orders.us.archive` and `orders.us.audit`, both on the same broker, with a `X-Routed-By` static header.

```json
{
  "name": "PROD-orders",
  "source": {
    "brokers": "kafka:9092",
    "topic": "orders.v1",
    "consumer_group": "fanout-orders",
    "offset_reset": "earliest"
  },
  "mappings": [
    {
      "key_path": "country",
      "operator": "equals",
      "value": "US",
      "case_insensitive": true,
      "destinations": [
        {
          "use_source_broker": true,
          "topic": "orders.us.archive",
          "headers": [{ "name": "X-Routed-By", "value": "fanout-orders", "mode": "static" }]
        },
        {
          "use_source_broker": true,
          "topic": "orders.us.audit",
          "headers": [{ "name": "X-Routed-By", "value": "fanout-orders", "mode": "static" }]
        }
      ]
    }
  ]
}
```

### 9.2 Cross-broker fan-out with dynamic header

Source on broker A, dest on broker B (different security config), header `X-TableName` pulled from the message.

```json
{
  "name": "PROD-cdc",
  "source": {
    "brokers": "kafka-a.internal:9092",
    "topic": "cdc.events",
    "consumer_group": "fanout-cdc",
    "offset_reset": "latest",
    "security_protocol": "SASL_SSL",
    "sasl_mechanism": "SCRAM-SHA-512",
    "sasl_username": "fanout",
    "sasl_password": "***"
  },
  "mappings": [
    {
      "key_path": "Message.TableName",
      "operator": "contains",
      "value": "cattle",
      "case_insensitive": true,
      "destinations": [
        {
          "use_source_broker": false,
          "brokers": "kafka-b.external:9093",
          "topic": "lake.cattles",
          "security_protocol": "SSL",
          "headers": [
            { "name": "X-Domain",    "value": "Connector",         "mode": "static" },
            { "name": "X-TableName", "value": "Message.TableName", "mode": "from_message" }
          ]
        }
      ]
    }
  ]
}
```

---

## 10. Acceptance Criteria

The build is "done for v1" when all of the following are true:

**UI**
- [ ] Can create, rename, duplicate, delete an env from the sidebar.
- [ ] Source tab validates required fields and blocks Save with inline errors.
- [ ] Mappings tab supports add / remove / reorder mappings and destinations.
- [ ] Condition editor shows a live human-readable preview of the expression.
- [ ] Headers editor supports `static` and `from_message` modes with live preview when a test message is set.
- [ ] Test message panel shows per-mapping match result and per-destination computed headers in real time.
- [ ] Export / Import round-trips an env's non-secret config without loss.
- [ ] Secrets are not visible in the UI after first save (shown as `••••••••`).

**Backend**
- [ ] `GET /api/health` returns 200.
- [ ] All CRUD endpoints work and persist to SQLite.
- [ ] Secrets accepted on write, never returned on read.
- [ ] Starting a consumer subscribes to the source topic with the configured `offset_reset` and `consumer_group`.
- [ ] A JSON message with `Message.TableName = "Cattles"` against a `contains` mapping with value `"cattle"` (case-insensitive) is routed to every destination of that mapping, with all `from_message` headers resolved.
- [ ] A non-matching message produces nothing.
- [ ] A destination that is unreachable does not stop the source consumer or other destinations; the failure is recorded in `runtime_status.messages_failed` and a log line.
- [ ] A JSON parse failure on the source message logs a warning, increments `messages_failed`, and does not route the message.
- [ ] Stopping the consumer commits offsets and closes cleanly within 10s.
- [ ] Restarting the API process and calling `start` on a previously-running env resumes from the last committed offset (no replay, no loss of messages produced after the previous start).
- [ ] `POST /api/envs/{id}/start` while already running is a no-op (returns 200, state unchanged).
- [ ] `POST /api/envs/{id}/stop` while stopped is a no-op (returns 200, state unchanged).

**Cross-tier**
- [ ] Creating an env in the UI persists to SQLite and survives an API restart.
- [ ] Starting an env from the UI causes the consumer to begin producing; status reflects `running` within 5s.
- [ ] Stopping from the UI causes status to return to `stopped` within 5s.

---

## 11. Out of Scope / Future

- Authentication, RBAC, multi-user.
- Avro / Protobuf / Schema Registry.
- Dead-letter topic for poison messages (see open question 8.B).
- Horizontal worker scaling (extracting `manager.py` to a separate process / queue).
- Dark theme.
- Versioning / change history of env configs.
- Webhooks or notifications on consumer errors.

---

## 12. Open Questions (decide before or during build)

8.A. **Poison-message policy.** Default in this PRD: log warning + skip + count failure. Alternative: route the original payload + error metadata to a per-env DLQ topic. Add later if needed; flag if you want it in v1.

8.B. **Reset offsets action.** Should the UI expose a "reset consumer group offsets to earliest now" button? Cheap to add; confirm before building.

8.C. **JMESPath result typing.** When the expression returns a list or object, this PRD says "use the first value" for condition eval and `str(value)` for headers. Confirm OK, or do you want strict typing (e.g. fail the mapping if the result is not a scalar)?

8.D. **Header value coercion for `from_message`.** `str(value)` for non-strings (numbers, bools). For objects/lists, this could produce ugly values. Acceptable, or do you want JSON-stringified output?

---

## 13. Repo Layout

```
.
├── README.md
├── docker-compose.yml                # optional: API + UI + Redpanda for local dev
├── PRD.md                            # this file
├── backend/
│   ├── requirements.txt
│   ├── .env.example
│   ├── kafka_fanout.db               # gitignored, auto-created
│   └── app/
│       ├── __init__.py
│       ├── main.py                   # FastAPI app, lifespan, CORS
│       ├── config.py                 # env-var driven settings
│       ├── db.py                     # async engine, session, init_db
│       ├── models.py                 # SQLAlchemy ORM
│       ├── schemas.py                # Pydantic v2 in/out
│       ├── security.py               # secret redaction helper
│       ├── routers/
│       │   ├── __init__.py
│       │   ├── envs.py
│       │   └── runtime.py
│       └── runtime/
│           ├── __init__.py
│           ├── manager.py            # RuntimeManager: start/stop/status
│           ├── consumer.py           # per-env consumer task
│           ├── matcher.py            # pure: evaluate(mapping, json) -> bool
│           ├── producer.py           # per-broker producer pool
│           └── header_builder.py     # build headers from a destination + message
└── kafka-fanout-ui/
    ├── package.json
    ├── vite.config.js
    ├── index.html
    ├── .env.example                  # VITE_API_BASE
    └── src/
        ├── main.jsx
        ├── App.jsx
        ├── styles/
        │   ├── index.css
        │   └── tokens.css
        ├── store/
        │   └── useEnvs.js            # reducer + localStorage draft cache
        ├── lib/
        │   ├── api.js                # fetch wrappers
        │   └── jmespath-preview.js   # safe wrapper around jmespath npm
        ├── utils/
        │   ├── expression.js         # compile human-readable preview
        │   └── id.js                 # uuid generator
        └── components/
            ├── Sidebar.jsx
            ├── EnvHeader.jsx
            ├── SourcePanel.jsx
            ├── MappingsPanel.jsx
            ├── ConditionEditor.jsx
            ├── DestinationEditor.jsx
            ├── HeadersEditor.jsx
            ├── TestMessagePanel.jsx
            └── Modal.jsx
```

---

## 14. Dependencies (exact)

**Backend (`backend/requirements.txt`):**
```
fastapi>=0.115
uvicorn[standard]>=0.30
sqlalchemy>=2.0
aiosqlite>=0.20
pydantic>=2.7
aiokafka>=0.11
jmespath>=1.0
python-multipart>=0.0.9
```

**Frontend (`kafka-fanout-ui/package.json`):**
```
react ^18.3
react-dom ^18.3
jmespath ^0.16
```
Dev:
```
vite ^5.4
@vitejs/plugin-react ^4.3
```

---

## 15. Implementation Order (suggested)

1. Backend skeleton: FastAPI app, SQLite + SQLAlchemy models, `init_db`, health endpoint, CORS.
2. Backend CRUD: schemas, envs router (with secret redaction), import/export.
3. Backend runtime module: `matcher.py` + unit tests with a pure harness.
4. Backend runtime: `producer.py`, `consumer.py`, `manager.py`, runtime router, status/logs endpoints, `test` endpoint.
5. Frontend skeleton: Vite app, layout, sidebar, env header, API client.
6. Frontend Source tab with validation.
7. Frontend Mappings tab: condition editor, destinations, headers.
8. Frontend Test message panel with client-side JMESPath.
9. Frontend runtime controls (start/stop), status polling, log viewer.
10. End-to-end happy-path test with a local Kafka (Redpanda in docker-compose).
11. Acceptance checklist walk-through.

---

## 16. Definition of Done

- All acceptance criteria in §10 checked.
- `docker compose up` brings up API + UI + Kafka and the happy-path works against a real broker.
- `README.md` documents how to run both tiers and how to point them at an existing Kafka cluster.
- No secrets in any GET response, any log line, or any exported JSON.
- `matcher.py` has unit tests covering: equals / not_equals / contains, case-insensitive on/off, scalar coercion, missing key, non-string result.
