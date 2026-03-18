import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.portfolio.positions_service import ensure_table, update_positions


def main() -> None:
    connection = get_connection()
    try:
        ensure_table(connection)
        update_positions(connection)
    finally:
        connection.close()

    print("Positions updated.")


if __name__ == "__main__":
    main()
