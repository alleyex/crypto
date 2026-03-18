import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


SELECT_POSITIONS_SQL = """
SELECT
    symbol,
    qty,
    avg_price,
    realized_pnl,
    updated_at
FROM positions
ORDER BY symbol ASC;
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        rows = connection.execute(SELECT_POSITIONS_SQL).fetchall()
    finally:
        connection.close()

    if not rows:
        print("No positions found.")
        return

    for row in rows:
        symbol, qty, avg_price, realized_pnl, updated_at = row
        print(
            f"symbol={symbol} qty={qty} avg_price={avg_price} "
            f"realized_pnl={realized_pnl} updated_at={updated_at}"
        )


if __name__ == "__main__":
    main()
