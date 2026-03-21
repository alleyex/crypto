import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


SELECT_ORDERS_SQL = """
SELECT
    id,
    client_order_id,
    broker_name,
    broker_order_id,
    symbol,
    timeframe,
    strategy_name,
    side,
    qty,
    price,
    status,
    created_at
FROM orders
ORDER BY id DESC
LIMIT 5;
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        rows = connection.execute(SELECT_ORDERS_SQL).fetchall()
    finally:
        connection.close()

    if not rows:
        print("No orders found.")
        return

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
