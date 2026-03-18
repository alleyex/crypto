import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


SELECT_PNL_SQL = """
SELECT
    id,
    symbol,
    qty,
    avg_price,
    market_price,
    unrealized_pnl,
    created_at
FROM pnl_snapshots
ORDER BY id DESC
LIMIT 5;
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        rows = connection.execute(SELECT_PNL_SQL).fetchall()
    finally:
        connection.close()

    if not rows:
        print("No pnl snapshots found.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
