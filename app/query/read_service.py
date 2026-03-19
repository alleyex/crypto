from typing import Any

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.strategy.registry import list_registered_strategies


def _fetch_all(connection: DBConnection, query: str, limit: int = 5) -> list[dict[str, Any]]:
    return fetch_all_as_dicts(connection, query, (limit,))


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


def get_candles(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_CANDLES_SQL, limit)


def get_signals(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_SIGNALS_SQL, limit)


def get_risk_events(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_RISK_EVENTS_SQL, limit)


def get_orders(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_ORDERS_SQL, limit)


def get_fills(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_FILLS_SQL, limit)


def get_positions(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_POSITIONS_SQL, limit)


def get_pnl_snapshots(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_PNL_SQL, limit)


def get_audit_events(connection: DBConnection, limit: int = 20) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_AUDIT_EVENTS_SQL, limit)


def get_strategy_activity_summary(
    connection: DBConnection,
    per_table_limit: int = 100,
) -> list[dict[str, Any]]:
    signals = get_signals(connection, limit=per_table_limit)
    risk_events = get_risk_events(connection, limit=per_table_limit)
    orders = get_orders(connection, limit=per_table_limit)
    fills = get_fills(connection, limit=per_table_limit)

    summaries: list[dict[str, Any]] = []
    for strategy_name in list_registered_strategies():
        latest_signal = next((item for item in signals if item["strategy_name"] == strategy_name), None)
        latest_risk = next((item for item in risk_events if item["strategy_name"] == strategy_name), None)
        latest_order = next((item for item in orders if item["strategy_name"] == strategy_name), None)
        strategy_orders = [item for item in orders if item["strategy_name"] == strategy_name]
        order_ids = {item["id"] for item in strategy_orders}
        latest_fill = next((item for item in fills if item["order_id"] in order_ids), None)
        filled_order_count = sum(1 for item in strategy_orders if item["status"] == "FILLED")

        summaries.append(
            {
                "strategy_name": strategy_name,
                "latest_signal": latest_signal,
                "latest_risk": latest_risk,
                "latest_order": latest_order,
                "latest_fill": latest_fill,
                "filled_order_count": filled_order_count,
                "has_activity": any(
                    item is not None for item in (latest_signal, latest_risk, latest_order, latest_fill)
                ),
            }
        )

    return summaries
