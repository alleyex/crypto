import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.core.migrations import normalize_legacy_utc_timestamp_strings_offline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize legacy UTC timestamp strings to ISO 8601 UTC in offline batches."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Maximum rows to normalize per batch for each table/column pair.",
    )
    parser.add_argument(
        "--tables",
        type=str,
        default="schema_migrations",
        help="Comma-separated table names to normalize offline.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    table_names = {
        item.strip()
        for item in args.tables.split(",")
        if item.strip()
    }
    connection = get_connection()
    try:
        result = normalize_legacy_utc_timestamp_strings_offline(
            connection,
            batch_size=args.batch_size,
            table_names=table_names or None,
        )
    finally:
        connection.close()

    if not result:
        print("No legacy UTC timestamp strings needed normalization.")
        return 0

    print("Normalized legacy UTC timestamp strings:")
    for key in sorted(result):
        print(f"{key}: {result[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
