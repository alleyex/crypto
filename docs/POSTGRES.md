# PostgreSQL

## Current State

The project defaults to SQLite. PostgreSQL is supported as an opt-in backend validated end-to-end on March 18, 2026.

Runtime configuration:

- `CRYPTO_DB_BACKEND=sqlite` (default)
- `CRYPTO_SQLITE_PATH=storage/market_data.db`
- `CRYPTO_DATABASE_URL=` — set this to enable PostgreSQL

## What Is Already Prepared

- Database backend is configurable; no hard-coded path or driver
- SQLite path is configurable via `CRYPTO_SQLITE_PATH`
- App retries PostgreSQL connections during startup to tolerate container boot lag
- Schema bootstrapping is centralized in application migrations with advisory lock and `ON CONFLICT DO NOTHING` idempotency
- All pipeline, API, and scheduler entry points run migrations from a shared entrypoint
- Candle ingest uses `ON CONFLICT ... DO NOTHING` (PostgreSQL-compatible)
- PostgreSQL migration bootstrap, audit event path, health report, and `run_pipeline_collect()` all validated

## What Still Blocks A Default Switch

- `sqlite3`-typed connections remain in some services
- Direct file-path assumptions (`storage/market_data.db`) in parts of the codebase
- No PostgreSQL integration tests in CI

## Recommended Migration Order

1. Introduce a DB driver abstraction layer
2. Remove `sqlite3.Connection` type assumptions from services
3. Normalize remaining SQL syntax for PostgreSQL compatibility
4. Add PostgreSQL integration tests
5. Add a PostgreSQL service to Docker Compose after the SQL layer is ready
6. Decide whether PostgreSQL becomes the default runtime

## Enabling PostgreSQL

**Local smoke test:**

```bash
docker compose --profile postgres up -d postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@127.0.0.1:5432/crypto
python scripts/run_postgres_smoke.py
```

**Docker Compose:**

```bash
export CRYPTO_DB_BACKEND=postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto
docker compose --profile postgres up --build
```

**Full runtime with futures collectors:**

```bash
export CRYPTO_DB_BACKEND=postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto
docker compose --profile postgres --profile futures-collectors up --build -d
```

By default, Docker runtime services use the lightweight
`requirements-runtime.txt` image to avoid pulling PPO/training dependencies
such as Stable-Baselines3 and Torch during routine deploys. If you need
full ML dependencies inside containers, override the build arg:

```bash
CRYPTO_DOCKER_REQUIREMENTS_FILE=requirements.txt docker compose --profile postgres up --build
```

This profile set brings up:

- `api`
- `scheduler`
- `postgres`
- `futures-candles`
- `futures-orderbook`
- `futures-aggtrade`
- `futures-premium`
- `futures-open-interest`
- `futures-liquidation`

**Docker staging on an alternate port:**

Use the checked-in override file to run a PostgreSQL-backed staging API and
collector set without conflicting with the existing systemd production
services:

```bash
mkdir -p staging/storage staging/logs staging/runtime
export CRYPTO_DB_BACKEND=postgres
export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto
docker compose \
  -f docker-compose.yml \
  -f docker-compose.staging.yml \
  --profile postgres \
  --profile futures-collectors \
  up --build -d api futures-candles futures-orderbook futures-aggtrade futures-premium futures-open-interest futures-liquidation
```

The staging API defaults to port `18000`.

**Production Docker runtime:**

Use the checked-in production override so rebuilds stay pinned to PostgreSQL
and `paper` execution:

```bash
python3 scripts/run_production_compose.py --build
```

**Automated Compose validation:**

```bash
python scripts/run_postgres_compose_validation.py
```

## SQLite To PostgreSQL Migration

Use the direct migration script when the SQLite source is already frozen:

```bash
python scripts/migrate_sqlite_to_postgres.py \
  --database-url postgresql://crypto:crypto@127.0.0.1:5432/crypto \
  --truncate
```

Use the freeze-and-migrate script when SQLite may still be receiving writes:

```bash
python scripts/freeze_sqlite_and_migrate_to_postgres.py \
  --database-url postgresql://crypto:crypto@127.0.0.1:5432/crypto \
  --truncate
```

The freeze flow:

1. Creates a consistent SQLite snapshot using the SQLite backup API
2. Runs PostgreSQL migrations on the target
3. Imports rows from the frozen snapshot into PostgreSQL
4. Verifies row counts between the snapshot and PostgreSQL

Recommended production cutover order:

1. Stop API, scheduler, and all collector services that write SQLite
2. Create a final frozen snapshot and migrate it into PostgreSQL
3. Review the printed count verification and confirm there are no mismatches
4. Point all services at PostgreSQL via `CRYPTO_DB_BACKEND=postgres` and `CRYPTO_DATABASE_URL=...`
5. Start services and re-check `/health`

To automate those steps on a Linux host with the project's `systemd` services, use:

```bash
python scripts/run_postgres_cutover.py \
  --database-url postgresql://crypto:crypto@127.0.0.1:5432/crypto \
  --set-postgres-env \
  --restart-services
```

The cutover helper will:

1. Stop the configured SQLite-writing services
2. Run the freeze-and-migrate workflow with `--truncate`
3. Optionally rewrite `.env` to PostgreSQL settings
4. Optionally restart the services

Use `--dry-run` first on production to review the exact service and migration commands.

For the actual migration from the current `systemd + SQLite` production layout
to `Docker + PostgreSQL`, use the dedicated runbook:

- [Production Docker + PostgreSQL Cutover](/Users/alleyex/Projects/crypto/docs/PRODUCTION_DOCKER_POSTGRES_CUTOVER.md)

## Runtime Validation Record (March 18, 2026)

Confirmed working:

- PostgreSQL connection smoke test
- Migration bootstrap via `run_migrations()`
- Audit event insert/read path
- Health report generation
- `run_pipeline_collect()`
- `GET /health`, `GET /orders`, `GET /audit-events`, `POST /pipeline/run`
- Docker Compose runtime with `api`, `scheduler`, and `postgres` services
- `POST /pipeline/run` succeeding on PostgreSQL backend

CI hardening resolved issues:

- API startup race during `/health` polling — resolved with connection retry
- Concurrent migration calls during startup — resolved with PostgreSQL advisory lock
- Pipeline failure masked as success — resolved by checking for `failed`/`blocked` step states
- Binance `451` errors on GitHub runners — resolved by using `CRYPTO_USE_FAKE_KLINES=1`
- Node 20 deprecation warnings — resolved by pinning to Node 24-compatible action versions

## Runtime Knobs

| Variable | Purpose |
|---|---|
| `CRYPTO_DB_BACKEND` | `sqlite` (default) or `postgres` |
| `CRYPTO_DATABASE_URL` | PostgreSQL DSN |
| `CRYPTO_POSTGRES_CONNECT_RETRIES` | Connection retry count |
| `CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS` | Delay between retries |
| `CRYPTO_USE_FAKE_KLINES` | Bypass live Binance fetches in CI |

## Known Caveats

- If `runtime/kill.switch` exists, `/pipeline/run` correctly returns a blocked result
- If port `8000` or `5432` is already bound, adjust published ports in Compose
- Container startup relies on application-level retry rather than Compose health-gate ordering
- The project still contains SQLite-specific connection typing; runtime compatibility is ahead of full type-level cleanup
