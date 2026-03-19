# PostgreSQL Runtime Validation

This document records the PostgreSQL runtime checks that have been executed against the current codebase.

## Validation Summary

Validated on March 18, 2026 (UTC+8 user environment, runtime checks executed against local Docker and local Python runtime).

Confirmed working:

- PostgreSQL connection smoke test
- PostgreSQL migration bootstrap via `run_migrations()`
- PostgreSQL-backed audit event insert/read path
- PostgreSQL-backed health report generation
- PostgreSQL-backed single-process `run_pipeline_collect()`
- PostgreSQL-backed API runtime for:
  - `GET /health`
  - `GET /orders`
  - `GET /audit-events`
  - `POST /pipeline/run`
- Docker Compose runtime with PostgreSQL for `api`, `scheduler`, and `postgres`
- Clean Docker Compose runtime with PostgreSQL-backed successful scheduler execution

## CI Incident Notes

The PostgreSQL validation workflow hit several real failures while being hardened for GitHub Actions. The current validation design reflects those incidents directly.

Resolved issues:

- API startup race during `/health` polling caused transient connection resets before the server was ready to serve HTTP responses.
- Multiple PostgreSQL runtime entrypoints could call `run_migrations()` concurrently during startup, creating a migration race around `schema_migrations`.
- Runtime validation previously accepted any JSON from `POST /pipeline/run`, which meant a pipeline failure could be wrapped as a structured payload and still look like a successful validation run.
- GitHub-hosted runners received `451` responses from Binance for live market-data fetches, which made PostgreSQL validation depend on a third-party API and regional availability rather than on the project runtime itself.
- GitHub Actions emitted Node 20 deprecation warnings until the workflow actions were upgraded to Node 24-compatible versions.

Final validation policy:

- PostgreSQL migration execution is serialized with a PostgreSQL advisory lock, and applied-version recording uses `ON CONFLICT DO NOTHING`.
- `POST /pipeline/run` results are treated as successful only when the returned step payload does not contain `failed` or `blocked` step states.
- Compose-based PostgreSQL validation uses fake kline input by setting `CRYPTO_USE_FAKE_KLINES=1`, so CI validates runtime/database behavior without relying on Binance availability.
- GitHub Actions workflow dependencies are pinned to Node 24-compatible versions:
  - `actions/checkout@v5`
  - `actions/setup-python@v6`
  - `actions/upload-artifact@v6`

## Commands Executed

Local smoke path:

```bash
cd /Users/alleyex/Projects/crypto
source .venv/bin/activate
pip install -r requirements.txt
docker compose --profile postgres up -d postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@127.0.0.1:5432/crypto
python scripts/run_postgres_smoke.py
```

Single-process PostgreSQL runtime checks:

```bash
CRYPTO_DB_BACKEND=postgres \
CRYPTO_DATABASE_URL=postgresql://crypto:crypto@127.0.0.1:5432/crypto \
python -c "from app.api.main import build_health_report; print(build_health_report())"
```

```bash
CRYPTO_DB_BACKEND=postgres \
CRYPTO_DATABASE_URL=postgresql://crypto:crypto@127.0.0.1:5432/crypto \
python -c "import app.pipeline.run_pipeline as p; p.kill_switch_enabled=lambda: False; p.get_kill_switch_status=lambda: {'enabled': False, 'kill_switch_file': 'runtime/kill.switch'}; print(p.run_pipeline_collect())"
```

Compose runtime validation:

```bash
export CRYPTO_DB_BACKEND=postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto
docker compose --profile postgres up --build
```

Example endpoint checks:

```bash
curl http://127.0.0.1:8000/health
curl -X POST http://127.0.0.1:8000/pipeline/run
curl "http://127.0.0.1:8000/orders?limit=5"
curl "http://127.0.0.1:8000/audit-events?limit=5"
```

Clean Compose runtime validation used an isolated runtime mount and alternate API port so existing local state did not interfere:

```bash
mkdir -p /tmp/crypto-pg-clean/runtime /tmp/crypto-pg-clean/logs /tmp/crypto-pg-clean/storage
export CRYPTO_DB_BACKEND=postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto
docker compose --profile postgres up --build
curl http://127.0.0.1:8012/health
curl -X POST http://127.0.0.1:8012/pipeline/run
curl "http://127.0.0.1:8012/orders?limit=5"
curl "http://127.0.0.1:8012/audit-events?limit=5"
```

The same validation flow is now scriptable with:

```bash
python scripts/run_postgres_compose_validation.py
```

## Expected Runtime Behavior

When PostgreSQL is active:

- `database_info.backend` should be `postgres`
- `database` fields in API and pipeline output should show the PostgreSQL DSN
- `schema_migrations` and core trading tables should exist in PostgreSQL
- `audit_events`, `orders`, and heartbeats should be readable through the API
- `api` and `scheduler` should tolerate PostgreSQL startup lag via connection retry instead of failing immediately during container startup
- compose-based CI validation should not require external Binance connectivity, because it runs with fake kline input

## Observed Clean Compose Result

The clean PostgreSQL Compose validation completed successfully with:

- `GET /health` returning `status: ok`
- `database` set to `postgresql://crypto:crypto@postgres:5432/crypto`
- scheduler log showing `run=1 signal=BUY risk=APPROVED execution=FILLED BUY`
- `POST /pipeline/run` succeeding on PostgreSQL
- `GET /orders?limit=5` returning the PostgreSQL-backed filled order
- `GET /audit-events?limit=5` returning PostgreSQL-backed audit rows

The first manual `POST /pipeline/run` after scheduler startup was rejected by cooldown, which is expected business behavior after the scheduler-created fill and does not indicate a PostgreSQL compatibility issue.

## Known Caveats

- If `runtime/kill.switch` exists in the mounted runtime directory, `/pipeline/run` will correctly return a blocked result.
- If another local service already binds host port `8000`, Compose validation must use a different published API port.
- If another local service already binds host port `5432`, the Compose validation stack should avoid publishing the PostgreSQL port to the host, or use a different published port.
- When PostgreSQL is starting, containers rely on application-level retry in `get_connection()` rather than Compose-level health-gated startup ordering.
- The project still contains SQLite-specific connection typing in Python services; runtime compatibility is ahead of full type-level cleanup.

## Relevant Runtime Knobs

- `CRYPTO_POSTGRES_CONNECT_RETRIES`
- `CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS`
- `CRYPTO_USE_FAKE_KLINES`
- `CRYPTO_FAKE_KLINE_CLOSES`

## Next Recommended Checks

1. Validate scheduler advancement over multiple iterations and confirm `/validation/soak` remains readable on PostgreSQL.
2. Continue removing residual SQLite-specific typing and assumptions from service code.
3. Decide whether PostgreSQL should remain optional or become the default runtime.
