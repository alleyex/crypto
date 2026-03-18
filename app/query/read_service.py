import sqlite3
from typing import Any


def _fetch_all(connection: sqlite3.Connection, query: str, limit: int = 5) -> list[dict[str, Any]]:
    connection.row_factory = sqlite3.Row
    rows = connection.execute(query, (limit,)).fetchall()
    return [dict(row) for row in rows]


SELECT_CANDLES_SQL = """
SELECT
    id,
    symbol,
    timeframe,
    open_time,
    open,
    high,
    low,
    close,
    volume,
    close_time,
    created_at
FROM candles
ORDER BY open_time DESC
LIMIT ?;
"""


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
LIMIT ?;
"""


SELECT_RISK_EVENTS_SQL = """
SELECT
    id,
    signal_id,
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    decision,
    reason,
    created_at
FROM risk_events
ORDER BY id DESC
LIMIT ?;
"""


SELECT_ORDERS_SQL = """
SELECT
    id,
    client_order_id,
    risk_event_id,
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
LIMIT ?;
"""


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
LIMIT ?;
"""


SELECT_POSITIONS_SQL = """
SELECT
    symbol,
    qty,
    avg_price,
    realized_pnl,
    updated_at
FROM positions
ORDER BY symbol ASC
LIMIT ?;
"""


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
LIMIT ?;
"""


SELECT_AUDIT_EVENTS_SQL = """
SELECT
    id,
    event_type,
    status,
    source,
    message,
    payload_json,
    created_at
FROM audit_events
ORDER BY id DESC
LIMIT ?;
"""


def get_candles(connection: sqlite3.Connection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_CANDLES_SQL, limit)


def get_signals(connection: sqlite3.Connection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_SIGNALS_SQL, limit)


def get_risk_events(connection: sqlite3.Connection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_RISK_EVENTS_SQL, limit)


def get_orders(connection: sqlite3.Connection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_ORDERS_SQL, limit)


def get_fills(connection: sqlite3.Connection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_FILLS_SQL, limit)


def get_positions(connection: sqlite3.Connection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_POSITIONS_SQL, limit)


def get_pnl_snapshots(connection: sqlite3.Connection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_PNL_SQL, limit)


def get_audit_events(connection: sqlite3.Connection, limit: int = 20) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_AUDIT_EVENTS_SQL, limit)
