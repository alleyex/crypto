import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


SELECT_RISK_EVENTS_SQL = """
SELECT
    id,
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    decision,
    reason,
    created_at
FROM risk_events
ORDER BY id DESC
LIMIT 5;
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        rows = connection.execute(SELECT_RISK_EVENTS_SQL).fetchall()
    finally:
        connection.close()

    if not rows:
        print("No risk events found.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
