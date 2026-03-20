import importlib
import os
import time
from typing import Any

from app.core.db import PostgresConnectionAdapter
from app.core.migrations import run_migrations


def _load_psycopg() -> Any:
    try:
        return importlib.import_module("psycopg")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is not installed. Run `pip install -r requirements.txt` before PostgreSQL smoke testing."
        ) from exc


def _connect_with_retry(database_url: str) -> Any:
    psycopg = _load_psycopg()
    retries = max(int(os.getenv("CRYPTO_POSTGRES_CONNECT_RETRIES", "15")), 1)
    delay_seconds = max(float(os.getenv("CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS", "1")), 0.0)
    last_error: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            return psycopg.connect(database_url)
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(delay_seconds)

    assert last_error is not None
    raise last_error


def run_postgres_smoke(database_url: str) -> dict[str, Any]:
    if not database_url.strip():
        raise RuntimeError("CRYPTO_DATABASE_URL is required for PostgreSQL smoke testing.")

    with _connect_with_retry(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT current_database(), current_user;")
            database_name, current_user = cursor.fetchone()
            cursor.execute(
                """
                CREATE TEMP TABLE crypto_postgres_smoke (
                    id INTEGER PRIMARY KEY,
                    note TEXT NOT NULL
                );
                """
            )
            cursor.execute(
                """
                INSERT INTO crypto_postgres_smoke (id, note)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (1, "smoke"),
            )
            cursor.execute(
                """
                INSERT INTO crypto_postgres_smoke (id, note)
                VALUES (%s, %s)
                ON CONFLICT (id) DO NOTHING;
                """,
                (1, "duplicate"),
            )
            cursor.execute("SELECT COUNT(*), MIN(note), MAX(note) FROM crypto_postgres_smoke;")
            row_count, first_note, last_note = cursor.fetchone()

    return {
        "ok": True,
        "database": str(database_name),
        "user": str(current_user),
        "temp_row_count": int(row_count),
        "temp_first_note": str(first_note),
        "temp_last_note": str(last_note),
    }


def run_postgres_migration_smoke(database_url: str) -> dict[str, Any]:
    if not database_url.strip():
        raise RuntimeError("CRYPTO_DATABASE_URL is required for PostgreSQL migration smoke testing.")

    raw_connection = _connect_with_retry(database_url)
    connection = PostgresConnectionAdapter(raw_connection)
    try:
        applied = run_migrations(connection)
        table_names = sorted(
            name
            for name in (
                "schema_migrations",
                "candles",
                "signals",
                "risk_events",
                "orders",
                "fills",
                "positions",
                "pnl_snapshots",
                "daily_realized_pnl",
                "audit_events",
                "runtime_heartbeats",
            )
        )
        rows = connection.execute(
            """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename ASC;
            """
        ).fetchall()
        existing_tables = sorted(str(row[0]) for row in rows)
    finally:
        connection.close()

    return {
        "ok": True,
        "applied_migrations": applied,
        "expected_tables": table_names,
        "existing_tables": existing_tables,
        "all_expected_tables_present": all(table in existing_tables for table in table_names),
    }
