import sqlite3
import uuid
from typing import Dict, Optional, Union

from app.data.candles_service import get_latest_close


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


SELECT_LATEST_RISK_SQL = """
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


def ensure_tables(connection: sqlite3.Connection) -> None:
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
    connection.commit()


def execute_latest_risk(
    connection: sqlite3.Connection,
    order_qty: float = 0.001,
) -> Optional[Dict[str, Union[float, str, int]]]:
    latest_risk = connection.execute(SELECT_LATEST_RISK_SQL).fetchone()
    if latest_risk is None:
        return None

    risk_event_id, _, symbol, timeframe, strategy_name, signal_type, decision = latest_risk
    if decision != "APPROVED":
        return {"risk_event_id": risk_event_id, "decision": decision}
    if signal_type not in ("BUY", "SELL"):
        return {"risk_event_id": risk_event_id, "decision": "SKIPPED", "signal_type": signal_type}

    existing_order = connection.execute(
        "SELECT id FROM orders WHERE risk_event_id = ? LIMIT 1;",
        (risk_event_id,),
    ).fetchone()
    if existing_order is not None:
        return {"risk_event_id": risk_event_id, "decision": "SKIPPED", "reason": "Already executed"}

    latest_close = get_latest_close(connection, symbol=symbol, timeframe=timeframe)
    if latest_close is None:
        return {"risk_event_id": risk_event_id, "decision": "SKIPPED", "reason": "No candle data"}

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
            order_qty,
            latest_close,
            "FILLED",
        ),
    )
    order_id = cursor.lastrowid
    connection.execute(
        INSERT_FILL_SQL,
        (order_id, symbol, signal_type, order_qty, latest_close),
    )
    connection.commit()

    return {
        "risk_event_id": risk_event_id,
        "order_id": order_id,
        "symbol": symbol,
        "side": signal_type,
        "qty": order_qty,
        "price": latest_close,
        "status": "FILLED",
    }
