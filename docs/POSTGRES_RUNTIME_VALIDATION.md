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

## Next Recommended Checks

1. Validate scheduler advancement over multiple iterations and confirm `/validation/soak` remains readable on PostgreSQL.
2. Continue removing residual SQLite-specific typing and assumptions from service code.
3. Decide whether PostgreSQL should remain optional or become the default runtime.
