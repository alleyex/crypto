import json
from typing import Any

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts

# Re-exports so callers can continue importing from this module.
from app.query.activity_summary import get_strategy_activity_summary as get_strategy_activity_summary  # noqa: F401
from app.query.activity_summary import get_strategy_closed_trades as get_strategy_closed_trades  # noqa: F401
from app.query.activity_summary import _fills_by_order_id, _executed_orders  # noqa: F401
from app.query.execution_report import get_execution_report as get_execution_report  # noqa: F401
from app.query.job_queue_summary import get_job_queue_summary as get_job_queue_summary  # noqa: F401

def _fetch_all(connection: DBConnection, query: str, limit: int = 5) -> list[dict[str, Any]]:
    return fetch_all_as_dicts(connection, query, (limit,))

def _decode_json_field(value: Any) -> Any:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None

_CANDLES_COLUMNS = """
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
    quote_asset_volume,
    number_of_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume,
    created_at"""


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
LIMIT ?;
"""

SELECT_ALL_ORDERS_SQL = """
SELECT
    id,
    client_order_id,
    risk_event_id,
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
ORDER BY id DESC;
"""

SELECT_FILLS_SQL = """
SELECT
    id,
    order_id,
    symbol,
    side,
    qty,
    price,
    commission,
    commission_asset,
    quote_qty,
    transact_time,
    created_at
FROM fills
ORDER BY id DESC
LIMIT ?;
"""

SELECT_ALL_FILLS_SQL = """
SELECT
    id,
    order_id,
    symbol,
    side,
    qty,
    price,
    commission,
    commission_asset,
    quote_qty,
    transact_time,
    created_at
FROM fills
ORDER BY id DESC;
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

SELECT_JOB_QUEUE_SQL = """
SELECT
    id,
    job_type,
    status,
    payload_json,
    result_json,
    error_message,
    attempt_count,
    created_at,
    started_at,
    completed_at
FROM job_queue
ORDER BY id DESC
LIMIT ?;
"""

def _candle_query(table: str, symbol: str | None, timeframes: list | None, limit: int) -> tuple[str, tuple]:
    if symbol and timeframes:
        placeholders = ",".join("?" * len(timeframes))
        sql = f"SELECT{_CANDLES_COLUMNS}\nFROM {table}\nWHERE symbol = ? AND timeframe IN ({placeholders})\nORDER BY open_time DESC\nLIMIT ?;"
        return sql, (symbol, *timeframes, limit)
    if symbol:
        sql = f"SELECT{_CANDLES_COLUMNS}\nFROM {table}\nWHERE symbol = ?\nORDER BY open_time DESC\nLIMIT ?;"
        return sql, (symbol, limit)
    sql = f"SELECT{_CANDLES_COLUMNS}\nFROM {table}\nORDER BY open_time DESC\nLIMIT ?;"
    return sql, (limit,)

def get_candles(connection: DBConnection, limit: int = 5, symbol: str | None = None, timeframes: list | None = None) -> list[dict[str, Any]]:
    sql, params = _candle_query("candles", symbol, timeframes, limit)
    return fetch_all_as_dicts(connection, sql, params)

def get_futures_candles(connection: DBConnection, limit: int = 5, symbol: str | None = None, timeframes: list | None = None) -> list[dict[str, Any]]:
    sql, params = _candle_query("futures_candles", symbol, timeframes, limit)
    return fetch_all_as_dicts(connection, sql, params)

def get_signals(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_SIGNALS_SQL, limit)

def get_risk_events(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_RISK_EVENTS_SQL, limit)

def get_orders(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_ORDERS_SQL, limit)

def get_fills(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_FILLS_SQL, limit)

def get_all_orders(connection: DBConnection) -> list[dict[str, Any]]:
    return fetch_all_as_dicts(connection, SELECT_ALL_ORDERS_SQL)

def get_all_fills(connection: DBConnection) -> list[dict[str, Any]]:
    return fetch_all_as_dicts(connection, SELECT_ALL_FILLS_SQL)

def get_positions(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_POSITIONS_SQL, limit)

def get_pnl_snapshots(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_PNL_SQL, limit)

def get_audit_events(connection: DBConnection, limit: int = 20) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_AUDIT_EVENTS_SQL, limit)

def get_job_queue_jobs(connection: DBConnection, limit: int = 20) -> list[dict[str, Any]]:
    rows = _fetch_all(connection, SELECT_JOB_QUEUE_SQL, limit)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["payload"] = _decode_json_field(item.get("payload_json"))
        item["result"] = _decode_json_field(item.get("result_json"))
        normalized.append(item)
    return normalized
