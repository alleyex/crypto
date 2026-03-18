# PostgreSQL Migration Path

This project still runs on SQLite in production code today, but the database configuration is now prepared for a future backend switch.

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
- the app now fails fast with a clear error if `CRYPTO_DB_BACKEND=postgres` is selected before the SQL layer is migrated

## What Still Blocks PostgreSQL

The codebase still contains SQLite-specific SQL and assumptions, including:

- `sqlite3`-typed connections across services
- `PRAGMA table_info(...)`
- `AUTOINCREMENT`
- `INSERT OR IGNORE`
- SQLite-specific migration patterns based on `ALTER TABLE` checks
- direct file-path assumptions such as `storage/market_data.db`

## Recommended Migration Order

1. Introduce a DB driver abstraction layer
2. Replace SQLite-only schema bootstrapping with proper migrations
3. Remove `sqlite3.Connection` type assumptions from services
4. Normalize SQL syntax for PostgreSQL compatibility
5. Add PostgreSQL integration tests
6. Add a PostgreSQL service to Docker Compose after the SQL layer is ready

## Near-Term Goal

The practical next step is not "turn PostgreSQL on" yet.

The practical next step is:

1. identify each SQLite-specific SQL fragment
2. move schema management into explicit migrations
3. add one PostgreSQL-backed smoke test path
