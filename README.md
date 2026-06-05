# Kafka Fan-Out Configurator

A two-tier tool for designing and operating 1-to-N Kafka message fan-out routing rules, parameterized per environment (DEV / TEST / UAT / PROD or any custom name).

- **Frontend** (React + Vite) — visual designer for environments, source Kafka config, conditional mappings, destination topics, message headers. Includes a live "Test message" sandbox.
- **Backend** (FastAPI + SQLite) — persists configurations, exposes a REST API, runs in-process async Kafka workers that consume a source topic and fan out to N destinations per the rules defined in the UI.

The fan-out logic is **data-driven**: every behavior (which source, which conditions match, where to route, what headers to attach) comes from the config in SQLite, with zero code changes required to add or change a route.

See [`PRD.md`](./PRD.md) for the full design specification.

## Architectural Choices

- **Delivery Mode – At-least-once vs Outbox**  
  Environments can be configured to use either the traditional at‑least‑once delivery (`delivery_mode = "at_least_once"`) or the *Transactional Outbox* pattern (`delivery_mode = "outbox"`). The outbox mode writes messages to a local SQLite outbox table and dispatches them via a background dispatcher, guaranteeing exactly‑once‑like semantics when downstream consumers perform deduplication.

- **System‑Managed Header – `X-Source-Coord`**  
  Every produced message now includes a non‑editable `X-Source-Coord` header (`<topic>:<partition>:<offset>`). This enables downstream services to reliably trace the source location and perform idempotent processing.

- **Dead‑Letter Handling**  
  In outbox mode, failed deliveries after the configured retry attempts are moved to an `outbox_dead_letters` table and exposed via the UI. The UI provides an *Outbox DLQ* tab for inspection.

- **Metrics & Watermarks**  
  Runtime status now exposes outbox‑specific counters (`outbox_pending`, `outbox_dispatched_total`, `outbox_failed_total`, `outbox_dead_lettered_total`) and the age of the oldest pending outbox entry, aiding operational visibility.

## Repo layout

```
.
├── PRD.md
├── README.md
├── backend/                 # FastAPI + SQLite
│   ├── requirements.txt
│   ├── .env.example
│   └── app/...
└── kafka-fanout-ui/         # Vite + React
    ├── package.json
    └── src/...
```

## Quick start

### Backend

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
uvicorn app.main:app --reload --port 8000
```

- API root: `http://localhost:8000`
- OpenAPI docs: `http://localhost:8000/docs`
- SQLite file: `backend/kafka_fanout.db` (auto-created on first run)

### Frontend

```powershell
cd kafka-fanout-ui
npm install
npm run dev
```

- UI: `http://localhost:5173`
- The Vite dev server proxies `/api/*` to the FastAPI server on `:8000`, so no CORS issues in development.

### Tests

```powershell
cd backend
pytest tests/ -v
```

## Docker (one-shot dev stack)

A `docker-compose.yml` at the repo root brings up the backend and a
production-build preview of the frontend, with the SQLite DB persisted
in a named volume.

```powershell
docker compose up --build
```

- Frontend: <http://localhost:5173>
- Backend API: <http://localhost:8000> (OpenAPI docs at `/docs`)
- SQLite file: stored in the `kafka-fanout-backend-data` named volume
  (mounted at `/app/data` inside the backend container)

To bring the stack down without losing the database:

```powershell
docker compose down
```

To wipe the database too:

```powershell
docker compose down -v
```

Override settings with environment variables in `docker-compose.yml`
or via a local override file (e.g. `docker-compose.override.yml`):

```yaml
services:
  backend:
    environment:
      CORS_ORIGINS: http://localhost:5173,http://localhost:4173
      LOG_LEVEL: DEBUG
```

**Note on the frontend image:** it uses `vite preview`, a small Node
server that serves the production build. The Vite dev proxy from
`vite.config.js` is **not** applied here — the browser talks directly
to the API on `:8000` and CORS handles the rest. If you need the dev
proxy behavior (HMR + auto-reload), run `npm run dev` locally and
`docker compose up backend` for the API.

## Pointing at an existing Kafka cluster

Edit the env in the UI, fill in:

- **Source** — `brokers` (e.g. `kafka-prod.internal:9092`), `topic`, `consumer_group`, `offset_reset` (`earliest` or `latest`).
- **Security (Advanced)** — pick `security_protocol` (e.g. `SASL_SSL`), then `sasl_mechanism`, `sasl_username`, `sasl_password`, optional `ssl_ca_location`.
- Click **Start**. The consumer task launches, subscribes to the source topic, and fans out matching messages to the configured destinations.

## Secrets

SASL passwords are write-only in the API — accepted on `POST` / `PUT`, never returned in any `GET` response. After the first save, the UI shows them as `••••••••` placeholders. To change a secret, type a new value in the field; to keep the existing one, leave it unchanged.

## DLQ (dead-letter queue)

Each env can configure an optional `dlq_topic` and (optionally) `dlq_brokers`. When a source message fails JSON parsing, the original raw payload is published to the DLQ topic with an `__error` JSON header containing `{ts, source_topic, source_partition, source_offset, error}`. If no DLQ is configured, poison messages are logged and counted as failed.

## Reset offsets

The sidebar overflow menu on each env has a **Reset offsets to earliest** action. This stops the consumer (if running), deletes the consumer group, and lets the next Start pick up from `earliest`.

## License

Internal tool — no public license.
