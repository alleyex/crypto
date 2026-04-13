import uuid
from typing import Union

from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.core.db import utc_now_iso
from app.core.migrations import run_migrations
from app.core.utils import dedup_ordered
from app.data.candles_service import get_latest_close
from app.execution.queries import (
    SELECT_LATEST_RISK_SQL,
    SELECT_RISK_BY_ID_SQL,
    select_pending_approved_risk_ids,
)
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl

CREATE_ORDERS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY,
    client_order_id TEXT NOT NULL UNIQUE,
    risk_event_id INTEGER UNIQUE,
    broker_name TEXT,
    broker_order_id TEXT,
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

INSERT_ORDER_SQL = """
INSERT INTO orders (
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

INSERT_FILL_SQL = """
INSERT INTO fills (
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""

def ensure_tables(connection: DBConnection) -> None:
    run_migrations(connection)

def execute_risk_event_id(
    connection: DBConnection,
    risk_event_id: int,
    order_qty: float = 0.001,
) -> dict[str, Union[float, str, int]] | None:
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
            "paper",
            None,
            symbol,
            timeframe,
            strategy_name,
            signal_type,
            order_qty,
            latest_close,
            "FILLED",
            utc_now_iso(),
        ),
    )
    insert_and_get_rowid(
        connection,
        INSERT_FILL_SQL,
        (order_id, symbol, signal_type, order_qty, latest_close, None, None, None, None, utc_now_iso()),
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
) -> dict[str, Union[float, str, int]] | None:
    latest_risk = connection.execute(SELECT_LATEST_RISK_SQL).fetchone()
    if latest_risk is None:
        return None
    return execute_risk_event_id(connection, int(latest_risk[0]), order_qty=order_qty)

def execute_pending_approved_risks(
    connection: DBConnection,
    order_qty: float = 0.001,
    symbol_names: list[str] | None = None,
) -> list[dict[str, Union[float, str, int]]]:
    pending_rows = select_pending_approved_risk_ids(connection, symbol_names=symbol_names)
    execution_results: list[dict[str, Union[float, str, int]]] = []
    for risk_event_id in pending_rows:
        execution_result = execute_risk_event_id(connection, risk_event_id, order_qty=order_qty)
        if execution_result is not None:
            execution_results.append(execution_result)
    return execution_results

def execute_risk_event_ids(
    connection: DBConnection,
    risk_event_ids: list[int],
    order_qty: float = 0.001,
) -> list[dict[str, Union[float, str, int]]]:
    execution_results: list[dict[str, Union[float, str, int]]] = []
    for risk_event_id in dedup_ordered(risk_event_ids):
        execution_result = execute_risk_event_id(connection, int(risk_event_id), order_qty=order_qty)
        if execution_result is not None:
            execution_results.append(execution_result)
    return execution_results
