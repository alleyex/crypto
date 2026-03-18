import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


SELECT_CANDLES_SQL = """
SELECT
    symbol,
    timeframe,
    open_time,
    open,
    high,
    low,
    close,
    volume,
    close_time
FROM candles
ORDER BY open_time DESC
LIMIT 5;
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        rows = connection.execute(SELECT_CANDLES_SQL).fetchall()
    finally:
        connection.close()

    if not rows:
        print("No candles found.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
