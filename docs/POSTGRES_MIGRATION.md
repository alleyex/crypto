# PostgreSQL Migration Path

This project still defaults to SQLite, but the PostgreSQL runtime path has now been validated end-to-end and can be enabled explicitly through environment configuration.

## Current State

Runtime configuration:

- `CRYPTO_DB_BACKEND=sqlite`
- `CRYPTO_SQLITE_PATH=storage/market_data.db`
- `CRYPTO_DATABASE_URL=` for future PostgreSQL use

The API health report now exposes `database_info` so the active backend choice is visible at runtime.

## What Is Already Prepared

- database backend is no longer hard-coded in a single path string
- SQLite path is configurable
- a future PostgreSQL DSN can be supplied via `CRYPTO_DATABASE_URL`
- the app now retries PostgreSQL connections during startup so Compose runtime does not fail just because PostgreSQL is still booting
- schema bootstrapping is centralized in explicit application migrations
- API, pipeline, and scheduler now run migrations from a shared entrypoint
- the candle ingest path now uses `ON CONFLICT ... DO NOTHING` instead of `INSERT OR IGNORE`
- a standalone PostgreSQL smoke script now verifies DSN connectivity and basic `ON CONFLICT` behavior

## What Still Blocks A Default PostgreSQL Switch

The codebase still contains SQLite-specific SQL and assumptions, including:

- `sqlite3`-typed connections across services
- direct file-path assumptions such as `storage/market_data.db`

## Recommended Migration Order

1. Introduce a DB driver abstraction layer
2. Replace SQLite-only schema bootstrapping with proper migrations
3. Remove `sqlite3.Connection` type assumptions from services
4. Normalize SQL syntax for PostgreSQL compatibility
5. Add PostgreSQL integration tests
6. Add a PostgreSQL service to Docker Compose after the SQL layer is ready

## Near-Term Goal

The practical next step is no longer "prove PostgreSQL can run". That validation is already complete.

The practical next step is:

1. reduce remaining SQLite-specific typing and assumptions
2. script repeatable PostgreSQL Compose validation
3. decide whether PostgreSQL should remain optional or become the default runtime

## Smoke Test

Minimal smoke validation now exists without switching the runtime backend:

1. start PostgreSQL with `docker compose --profile postgres up -d postgres`
2. export `CRYPTO_DATABASE_URL=postgresql://crypto:crypto@127.0.0.1:5432/crypto`
3. run `python scripts/run_postgres_smoke.py`

The smoke script checks:

- PostgreSQL connection succeeds
- `current_database()` and `current_user` are readable
- a temporary table can be created
- `INSERT ... ON CONFLICT DO NOTHING` behaves as expected

## Compose Runtime

Docker Compose can now pass PostgreSQL runtime settings through to both `api` and `scheduler`.

Example:

1. `export CRYPTO_DB_BACKEND=postgres`
2. `export CRYPTO_DATABASE_URL=postgresql://crypto:crypto@postgres:5432/crypto`
3. `docker compose --profile postgres up --build`

This keeps the default Compose path on SQLite unless you explicitly switch the backend.

## Runtime Validation Record

For the current verified PostgreSQL runtime checks and observed caveats, see:

- `docs/POSTGRES_RUNTIME_VALIDATION.md`
