import uuid
from typing import Dict, List, Optional, Union

from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.core.migrations import run_migrations
from app.data.candles_service import get_latest_close
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl


CREATE_ORDERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
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
    id INTEGER PRIMARY KEY,
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


SELECT_RISK_BY_ID_SQL = """
SELECT
    re.id,
    re.signal_id,
    re.symbol,
    re.timeframe,
    re.strategy_name,
    re.signal_type,
    re.decision
FROM risk_events re
WHERE re.id = ?;
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


def ensure_tables(connection: DBConnection) -> None:
    run_migrations(connection)


def _select_pending_approved_risk_ids(
    connection: DBConnection,
    symbol_names: Optional[List[str]] = None,
) -> List[int]:
    query = """
    SELECT re.id
    FROM risk_events re
    LEFT JOIN orders o ON o.risk_event_id = re.id
    WHERE re.decision = 'APPROVED'
      AND o.id IS NULL
    """
    params: list[str] = []
    filtered_symbol_names = list(dict.fromkeys(symbol_names or []))
    if filtered_symbol_names:
        placeholders = ", ".join("?" for _ in filtered_symbol_names)
        query += f" AND re.symbol IN ({placeholders})"
        params.extend(filtered_symbol_names)
    query += " ORDER BY re.id ASC;"
    rows = connection.execute(query, tuple(params)).fetchall()
    return [int(row[0]) for row in rows]


def execute_risk_event_id(
    connection: DBConnection,
    risk_event_id: int,
    order_qty: float = 0.001,
) -> Optional[Dict[str, Union[float, str, int]]]:
    risk_event = connection.execute(SELECT_RISK_BY_ID_SQL, (risk_event_id,)).fetchone()
    if risk_event is None:
        return None

    risk_event_id, _, symbol, timeframe, strategy_name, signal_type, decision = risk_event
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
    order_id = insert_and_get_rowid(
        connection,
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
    insert_and_get_rowid(
        connection,
        INSERT_FILL_SQL,
        (order_id, symbol, signal_type, order_qty, latest_close),
    )
    # Keep persisted daily realized PnL in sync with newly written fills.
    rebuild_daily_realized_pnl(connection)
    connection.commit()
    insert_event(
        connection,
        event_type="order",
        status="filled",
        source="paper_broker",
        message=f"{signal_type} order filled for {symbol} at {latest_close}.",
        payload={
            "order_id": order_id,
            "risk_event_id": risk_event_id,
            "symbol": symbol,
            "side": signal_type,
            "qty": order_qty,
            "price": latest_close,
            "strategy_name": strategy_name,
        },
    )

    return {
        "risk_event_id": risk_event_id,
        "order_id": order_id,
        "symbol": symbol,
        "side": signal_type,
        "qty": order_qty,
        "price": latest_close,
        "status": "FILLED",
    }


def execute_latest_risk(
    connection: DBConnection,
    order_qty: float = 0.001,
) -> Optional[Dict[str, Union[float, str, int]]]:
    latest_risk = connection.execute(SELECT_LATEST_RISK_SQL).fetchone()
    if latest_risk is None:
        return None
    return execute_risk_event_id(connection, int(latest_risk[0]), order_qty=order_qty)


def execute_pending_approved_risks(
    connection: DBConnection,
    order_qty: float = 0.001,
    symbol_names: Optional[List[str]] = None,
) -> List[Dict[str, Union[float, str, int]]]:
    pending_rows = _select_pending_approved_risk_ids(connection, symbol_names=symbol_names)
    execution_results: List[Dict[str, Union[float, str, int]]] = []
    for risk_event_id in pending_rows:
        execution_result = execute_risk_event_id(connection, risk_event_id, order_qty=order_qty)
        if execution_result is not None:
            execution_results.append(execution_result)
    return execution_results


def execute_risk_event_ids(
    connection: DBConnection,
    risk_event_ids: List[int],
    order_qty: float = 0.001,
) -> List[Dict[str, Union[float, str, int]]]:
    execution_results: List[Dict[str, Union[float, str, int]]] = []
    for risk_event_id in list(dict.fromkeys(risk_event_ids)):
        execution_result = execute_risk_event_id(connection, int(risk_event_id), order_qty=order_qty)
        if execution_result is not None:
            execution_results.append(execution_result)
    return execution_results
