import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.core.migrations import run_migrations
from app.pipeline.strategy_job import run_strategy_job


def main() -> None:
    connection = get_connection()
    try:
        run_migrations(connection)
        result = run_strategy_job(connection)
    finally:
        connection.close()

    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
