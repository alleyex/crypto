#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sqlite3
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.settings import DATABASE_URL
from app.core.settings import SQLITE_PATH
from scripts.migrate_sqlite_to_postgres import TABLE_ORDER
from scripts.migrate_sqlite_to_postgres import get_table_counts_postgres
from scripts.migrate_sqlite_to_postgres import get_table_counts_sqlite
from scripts.migrate_sqlite_to_postgres import migrate_sqlite_to_postgres


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Freeze SQLite to a consistent snapshot, migrate it into PostgreSQL, and verify counts."
    )
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
        help="Truncate target tables before import. Recommended for cutover rehearsal targets.",
    )
    parser.add_argument(
        "--keep-snapshot",
        action="store_true",
        help="Keep the generated SQLite snapshot file instead of deleting it.",
    )
    parser.add_argument(
        "--tables",
        default="",
        help="Optional comma-separated subset of tables to migrate and verify.",
    )
    return parser.parse_args()


def _create_sqlite_snapshot(source_path: Path) -> Path:
    temp_file = tempfile.NamedTemporaryFile(prefix="crypto-sqlite-freeze-", suffix=".db", delete=False)
    snapshot_path = Path(temp_file.name)
    temp_file.close()

    source_connection = sqlite3.connect(str(source_path))
    snapshot_connection = sqlite3.connect(str(snapshot_path))
    try:
        source_connection.backup(snapshot_connection)
    finally:
        snapshot_connection.close()
        source_connection.close()
    return snapshot_path


def main() -> None:
    args = parse_args()
    if not args.database_url.strip():
        raise RuntimeError("Provide --database-url or set CRYPTO_DATABASE_URL.")

    sqlite_path = Path(args.sqlite_path).expanduser().resolve()
    if not sqlite_path.exists():
        raise RuntimeError(f"SQLite database not found: {sqlite_path}")

    requested_tables = (
        [item.strip() for item in args.tables.split(",") if item.strip()]
        if args.tables.strip()
        else TABLE_ORDER
    )

    snapshot_path = _create_sqlite_snapshot(sqlite_path)
    print(f"snapshot_created: {snapshot_path}")
    try:
        sqlite_counts = get_table_counts_sqlite(snapshot_path, requested_tables)
        summary = migrate_sqlite_to_postgres(
            sqlite_path=snapshot_path,
            database_url=args.database_url,
            batch_size=args.batch_size,
            truncate=args.truncate,
            tables=requested_tables,
        )
        postgres_counts = get_table_counts_postgres(args.database_url, requested_tables)

        for table, copied in summary:
            print(f"{table}: {copied} rows processed")
        total = sum(count for _, count in summary)
        print(f"done: {len(summary)} tables, {total} rows processed")
        print("count_verification:")
        for table in requested_tables:
            if table not in sqlite_counts and table not in postgres_counts:
                continue
            sqlite_count = sqlite_counts.get(table, 0)
            postgres_count = postgres_counts.get(table, 0)
            status = "OK" if sqlite_count == postgres_count else "MISMATCH"
            print(
                f"  {table}: sqlite_snapshot={sqlite_count} postgres={postgres_count} status={status}"
            )
    finally:
        if args.keep_snapshot:
            print(f"snapshot_kept: {snapshot_path}")
        else:
            snapshot_path.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
