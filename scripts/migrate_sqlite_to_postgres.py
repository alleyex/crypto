#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.db import _load_psycopg
from app.core.migrations import run_migrations
from app.core.settings import DATABASE_URL
from app.core.settings import SQLITE_PATH


TABLE_ORDER = [
    "schema_migrations",
    "candles",
    "futures_candles",
    "order_book_snapshots",
    "futures_order_book_snapshots",
    "futures_aggtrade_minutes",
    "futures_premium_metrics",
    "futures_open_interest_metrics",
    "futures_liquidation_minutes",
    "signals",
    "risk_events",
    "orders",
    "fills",
    "positions",
    "pnl_snapshots",
    "daily_realized_pnl",
    "audit_events",
    "runtime_heartbeats",
    "risk_configs",
    "portfolio_config",
    "feature_vectors",
    "training_jobs",
    "model_registry",
    "job_queue",
    "backtest_runs",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate SQLite data into PostgreSQL.")
    parser.add_argument("--sqlite-path", default=str(SQLITE_PATH), help="Path to source SQLite database.")
    parser.add_argument(
        "--database-url",
        default=DATABASE_URL,
        help="Target PostgreSQL DSN. Defaults to CRYPTO_DATABASE_URL.",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Rows per batch insert. Default: 1000")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Truncate target tables before import. Use only on a fresh cutover target.",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Optional comma-separated subset of tables to migrate.",
    )
    return parser.parse_args()


def _sqlite_tables(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name;"
    ).fetchall()
    return {str(row[0]) for row in rows if str(row[0]) != "sqlite_sequence"}


def _postgres_tables(connection: Any) -> set[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename;
            """
        )
        return {str(row[0]) for row in cursor.fetchall()}


def _sqlite_columns(connection: sqlite3.Connection, table: str) -> list[str]:
    rows = connection.execute(f"PRAGMA table_info({table});").fetchall()
    return [str(row[1]) for row in rows]


def _postgres_columns(connection: Any, table: str) -> list[str]:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position;
            """,
            (table,),
        )
        return [str(row[0]) for row in cursor.fetchall()]


def _copy_table(
    sqlite_connection: sqlite3.Connection,
    postgres_connection: Any,
    table: str,
    batch_size: int,
) -> int:
    source_columns = _sqlite_columns(sqlite_connection, table)
    target_columns = set(_postgres_columns(postgres_connection, table))
    columns = [column for column in source_columns if column in target_columns]
    if not columns:
        return 0

    sqlite_connection.row_factory = sqlite3.Row
    rows = sqlite_connection.execute(f"SELECT {', '.join(columns)} FROM {table};").fetchall()
    if not rows:
        return 0

    placeholders = ", ".join(["%s"] * len(columns))
    insert_sql = (
        f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) "
        "ON CONFLICT DO NOTHING;"
    )

    inserted = 0
    with postgres_connection.cursor() as cursor:
        for start in range(0, len(rows), batch_size):
            batch = rows[start:start + batch_size]
            params = [tuple(row[column] for column in columns) for row in batch]
            cursor.executemany(insert_sql, params)
            inserted += len(batch)
        if "id" in columns:
            cursor.execute(
                f"""
                SELECT setval(
                    pg_get_serial_sequence(%s, 'id'),
                    COALESCE((SELECT MAX(id) FROM {table}), 1),
                    true
                );
                """,
                (table,),
            )
    postgres_connection.commit()
    return inserted


def _truncate_tables(connection: Any, tables: list[str]) -> None:
    if not tables:
        return
    with connection.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE {', '.join(tables)} RESTART IDENTITY CASCADE;")
    connection.commit()


def main() -> None:
    args = parse_args()
    if not args.database_url.strip():
        raise RuntimeError("Provide --database-url or set CRYPTO_DATABASE_URL.")

    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    if not sqlite_path.exists():
        raise RuntimeError(f"SQLite database not found: {sqlite_path}")

    psycopg = _load_psycopg()
    sqlite_connection = sqlite3.connect(str(sqlite_path))
    postgres_connection = psycopg.connect(args.database_url)

    try:
        run_migrations(postgres_connection)

        source_tables = _sqlite_tables(sqlite_connection)
        target_tables = _postgres_tables(postgres_connection)
        requested_tables = (
            [item.strip() for item in args.tables.split(",") if item.strip()]
            if args.tables.strip()
            else TABLE_ORDER
        )
        tables = [table for table in requested_tables if table in source_tables and table in target_tables]

        if args.truncate:
            _truncate_tables(postgres_connection, list(reversed(tables)))

        summary: list[tuple[str, int]] = []
        for table in tables:
            copied = _copy_table(sqlite_connection, postgres_connection, table, args.batch_size)
            summary.append((table, copied))
            print(f"{table}: {copied} rows processed")

        total = sum(count for _, count in summary)
        print(f"done: {len(summary)} tables, {total} rows processed")
    finally:
        sqlite_connection.close()
        postgres_connection.close()


if __name__ == "__main__":
    main()
