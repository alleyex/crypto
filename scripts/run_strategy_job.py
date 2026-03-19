import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.pipeline.strategy_job import run_strategy_job


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the strategy job once.")
    parser.add_argument(
        "--strategy",
        default=DEFAULT_STRATEGY_NAME,
        help=f"Strategy name to execute. Default: {DEFAULT_STRATEGY_NAME}",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    connection = get_connection()
    try:
        run_migrations(connection)
        result = run_strategy_job(connection, strategy_name=args.strategy)
    finally:
        connection.close()

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
