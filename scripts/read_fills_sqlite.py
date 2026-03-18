import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


SELECT_FILLS_SQL = """
SELECT
    id,
    order_id,
    symbol,
    side,
    qty,
    price,
    created_at
FROM fills
ORDER BY id DESC
LIMIT 5;
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        rows = connection.execute(SELECT_FILLS_SQL).fetchall()
    finally:
        connection.close()

    if not rows:
        print("No fills found.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
