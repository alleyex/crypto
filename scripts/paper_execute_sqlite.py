import sqlite3
import uuid
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"
ORDER_QTY = 0.001


CREATE_ORDERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    client_order_id TEXT NOT NULL UNIQUE,
    risk_event_id INTEGER UNIQUE,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    price REAL NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


CREATE_FILLS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id INTEGER NOT NULL,
    symbol TEXT NOT NULL,
    side TEXT NOT NULL,
    qty REAL NOT NULL,
    price REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(order_id) REFERENCES orders(id)
);
"""


SELECT_LATEST_APPROVED_RISK_SQL = """
SELECT
    re.id,
    re.signal_id,
    re.symbol,
    re.timeframe,
    re.strategy_name,
    re.signal_type,
    re.decision
FROM risk_events re
ORDER BY re.id DESC
LIMIT 1;
"""


SELECT_LATEST_CLOSE_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
  AND timeframe = ?
ORDER BY open_time DESC
LIMIT 1;
"""


INSERT_ORDER_SQL = """
INSERT INTO orders (
    client_order_id,
    risk_event_id,
    symbol,
    timeframe,
    strategy_name,
    side,
    qty,
    price,
    status
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


INSERT_FILL_SQL = """
INSERT INTO fills (
    order_id,
    symbol,
    side,
    qty,
    price
) VALUES (?, ?, ?, ?, ?);
"""


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        connection.execute(CREATE_ORDERS_TABLE_SQL)
        connection.execute(CREATE_FILLS_TABLE_SQL)
        columns = {
            row[1] for row in connection.execute("PRAGMA table_info(orders);").fetchall()
        }
        if "risk_event_id" not in columns:
            connection.execute("ALTER TABLE orders ADD COLUMN risk_event_id INTEGER;")
            connection.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_orders_risk_event_id "
                "ON orders(risk_event_id);"
            )

        latest_risk = connection.execute(SELECT_LATEST_APPROVED_RISK_SQL).fetchone()
        if latest_risk is None:
            print("No risk event found.")
            return

        risk_event_id, _, symbol, timeframe, strategy_name, signal_type, decision = latest_risk

        if decision != "APPROVED":
            print(f"Latest risk event is not executable: {decision}")
            return

        if signal_type not in ("BUY", "SELL"):
            print(f"Signal type not executable: {signal_type}")
            return

        existing_order = connection.execute(
            "SELECT id FROM orders WHERE risk_event_id = ? LIMIT 1;",
            (risk_event_id,),
        ).fetchone()
        if existing_order is not None:
            print(f"Risk event already executed: {risk_event_id}")
            return

        latest_close_row = connection.execute(
            SELECT_LATEST_CLOSE_SQL,
            (symbol, timeframe),
        ).fetchone()
        if latest_close_row is None:
            print("No candle data found for execution.")
            return

        latest_close = float(latest_close_row[0])
        client_order_id = str(uuid.uuid4())

        cursor = connection.execute(
            INSERT_ORDER_SQL,
            (
                client_order_id,
                risk_event_id,
                symbol,
                timeframe,
                strategy_name,
                signal_type,
                ORDER_QTY,
                latest_close,
                "FILLED",
            ),
        )
        order_id = cursor.lastrowid

        connection.execute(
            INSERT_FILL_SQL,
            (
                order_id,
                symbol,
                signal_type,
                ORDER_QTY,
                latest_close,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    print(f"executed_signal={signal_type}")
    print(f"qty={ORDER_QTY}")
    print(f"price={latest_close}")
    print("order_status=FILLED")


if __name__ == "__main__":
    main()
