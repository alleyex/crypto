import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


CREATE_PNL_SNAPSHOTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pnl_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_price REAL NOT NULL,
    market_price REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


SELECT_POSITIONS_SQL = """
SELECT symbol, qty, avg_price
FROM positions;
"""


SELECT_LATEST_CLOSE_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
ORDER BY open_time DESC
LIMIT 1;
"""


INSERT_PNL_SNAPSHOT_SQL = """
INSERT INTO pnl_snapshots (
    symbol,
    qty,
    avg_price,
    market_price,
    unrealized_pnl
) VALUES (?, ?, ?, ?, ?);
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        connection.execute(CREATE_PNL_SNAPSHOTS_TABLE_SQL)

        positions = connection.execute(SELECT_POSITIONS_SQL).fetchall()
        if not positions:
            print("No positions found.")
            return

        snapshot_count = 0

        for symbol, qty, avg_price in positions:
            latest_close_row = connection.execute(
                SELECT_LATEST_CLOSE_SQL,
                (symbol,),
            ).fetchone()

            if latest_close_row is None:
                continue

            market_price = float(latest_close_row[0])
            qty = float(qty)
            avg_price = float(avg_price)
            unrealized_pnl = (market_price - avg_price) * qty

            connection.execute(
                INSERT_PNL_SNAPSHOT_SQL,
                (symbol, qty, avg_price, market_price, unrealized_pnl),
            )
            snapshot_count += 1

        connection.commit()
    finally:
        connection.close()

    print(f"Saved {snapshot_count} pnl snapshot(s).")


if __name__ == "__main__":
    main()
