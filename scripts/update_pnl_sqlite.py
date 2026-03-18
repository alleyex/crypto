import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.portfolio.pnl_service import ensure_table, update_pnl_snapshots


def main() -> None:
    connection = get_connection()
    try:
        ensure_table(connection)
        snapshot_count = update_pnl_snapshots(connection)
    finally:
        connection.close()

    print(f"Saved {snapshot_count} pnl snapshot(s).")


if __name__ == "__main__":
    main()
