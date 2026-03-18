import sqlite3
from datetime import datetime, timezone
from typing import Dict, Optional, Union

from app.audit.service import log_event
from app.core.settings import COOLDOWN_SECONDS
from app.core.settings import DEFAULT_ORDER_QTY
from app.core.settings import MAX_DAILY_LOSS
from app.core.settings import MAX_POSITION_QTY


CREATE_RISK_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS risk_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


SELECT_LATEST_SIGNAL_SQL = """
SELECT
    id,
    symbol,
    timeframe,
    strategy_name,
    signal_type
FROM signals
ORDER BY id DESC
LIMIT 1;
"""


SELECT_PREVIOUS_SIGNAL_SQL = """
SELECT signal_type
FROM signals
WHERE id < ?
ORDER BY id DESC
LIMIT 1;
"""


SELECT_POSITION_SQL = """
SELECT qty, realized_pnl
FROM positions
WHERE symbol = ?
LIMIT 1;
"""


SELECT_LATEST_FILL_SQL = """
SELECT created_at
FROM fills
WHERE symbol = ?
ORDER BY id DESC
LIMIT 1;
"""


INSERT_RISK_EVENT_SQL = """
INSERT INTO risk_events (
    signal_id,
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    decision,
    reason
) VALUES (?, ?, ?, ?, ?, ?, ?);
"""


def ensure_table(connection: sqlite3.Connection) -> None:
    connection.execute(CREATE_RISK_EVENTS_TABLE_SQL)
    columns = {
        row[1] for row in connection.execute("PRAGMA table_info(risk_events);").fetchall()
    }
    if "signal_id" not in columns:
        connection.execute("ALTER TABLE risk_events ADD COLUMN signal_id INTEGER;")
    connection.commit()


def _parse_created_at(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _fills_table_exists(connection: sqlite3.Connection) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'fills' LIMIT 1;"
    ).fetchone()
    return row is not None


def evaluate_latest_signal(
    connection: sqlite3.Connection,
    order_qty: float = DEFAULT_ORDER_QTY,
    max_position_qty: float = MAX_POSITION_QTY,
    cooldown_seconds: int = COOLDOWN_SECONDS,
    max_daily_loss: float = MAX_DAILY_LOSS,
) -> Optional[Dict[str, Union[int, str]]]:
    latest_signal = connection.execute(SELECT_LATEST_SIGNAL_SQL).fetchone()
    if latest_signal is None:
        return None

    signal_id, symbol, timeframe, strategy_name, signal_type = latest_signal

    if signal_type == "HOLD":
        decision = "REJECTED"
        reason = "Signal is HOLD."
    else:
        position_row = connection.execute(SELECT_POSITION_SQL, (symbol,)).fetchone()
        current_qty = float(position_row[0]) if position_row is not None else 0.0
        realized_pnl = float(position_row[1]) if position_row is not None else 0.0
        latest_fill = None
        if _fills_table_exists(connection):
            latest_fill = connection.execute(SELECT_LATEST_FILL_SQL, (symbol,)).fetchone()

        if realized_pnl <= -abs(max_daily_loss):
            decision = "REJECTED"
            reason = (
                f"Daily loss limit breached: realized_pnl={realized_pnl}, "
                f"limit=-{abs(max_daily_loss)}."
            )
        elif latest_fill is not None:
            last_fill_at = _parse_created_at(latest_fill[0])
            now = datetime.now(timezone.utc)
            cooldown_elapsed = (now - last_fill_at).total_seconds()
            if cooldown_elapsed < cooldown_seconds:
                decision = "REJECTED"
                reason = (
                    f"Cooldown active: last fill {int(cooldown_elapsed)} seconds ago, "
                    f"minimum {cooldown_seconds}."
                )
            elif signal_type == "BUY" and current_qty + order_qty > max_position_qty:
                decision = "REJECTED"
                reason = f"Max position exceeded: current={current_qty}, limit={max_position_qty}."
            elif signal_type == "BUY" and current_qty > 0:
                decision = "REJECTED"
                reason = "Existing long position already open."
            elif signal_type == "SELL" and current_qty <= 0:
                decision = "REJECTED"
                reason = "No position available to sell."
            else:
                previous_signal = connection.execute(
                    SELECT_PREVIOUS_SIGNAL_SQL,
                    (signal_id,),
                ).fetchone()
                if previous_signal and previous_signal[0] == signal_type:
                    decision = "REJECTED"
                    reason = "Duplicate signal type."
                else:
                    decision = "APPROVED"
                    reason = "Passed basic risk checks."
        elif signal_type == "BUY" and current_qty + order_qty > max_position_qty:
            decision = "REJECTED"
            reason = f"Max position exceeded: current={current_qty}, limit={max_position_qty}."
        elif signal_type == "BUY" and current_qty > 0:
            decision = "REJECTED"
            reason = "Existing long position already open."
        elif signal_type == "SELL" and current_qty <= 0:
            decision = "REJECTED"
            reason = "No position available to sell."
        else:
            previous_signal = connection.execute(
                SELECT_PREVIOUS_SIGNAL_SQL,
                (signal_id,),
            ).fetchone()
            if previous_signal and previous_signal[0] == signal_type:
                decision = "REJECTED"
                reason = "Duplicate signal type."
            else:
                decision = "APPROVED"
                reason = "Passed basic risk checks."

    cursor = connection.execute(
        INSERT_RISK_EVENT_SQL,
        (signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason),
    )
    connection.commit()
    log_event(
        event_type="risk_evaluation",
        status=str(decision).lower(),
        source="risk_service",
        message=reason,
        payload={
            "risk_event_id": cursor.lastrowid,
            "signal_id": signal_id,
            "symbol": symbol,
            "signal_type": signal_type,
            "decision": decision,
        },
    )

    return {
        "id": cursor.lastrowid,
        "signal_id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_name": strategy_name,
        "signal_type": signal_type,
        "decision": decision,
        "reason": reason,
    }
