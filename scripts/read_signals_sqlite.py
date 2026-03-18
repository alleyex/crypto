import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


SELECT_SIGNALS_SQL = """
SELECT
    id,
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    short_ma,
    long_ma,
    created_at
FROM signals
ORDER BY id DESC
LIMIT 5;
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        rows = connection.execute(SELECT_SIGNALS_SQL).fetchall()
    finally:
        connection.close()

    if not rows:
        print("No signals found.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
