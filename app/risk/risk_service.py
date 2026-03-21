from datetime import datetime, timezone
from typing import Dict, List, Optional, Union

from app.audit.service import insert_event
from app.audit.service import log_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.core.db import parse_db_timestamp
from app.core.db import table_exists
from app.core.migrations import run_migrations
from app.core.settings import COOLDOWN_SECONDS
from app.core.settings import DEFAULT_ORDER_QTY
from app.core.settings import MAX_DAILY_LOSS
from app.core.settings import MAX_POSITION_QTY
from app.portfolio.daily_pnl_service import get_daily_realized_pnl
from app.portfolio.portfolio_service import check_portfolio_limits
from app.risk.risk_config import get_risk_config
from app.system.kill_switch import enable_kill_switch
from app.system.kill_switch import kill_switch_enabled


CREATE_RISK_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS risk_events (
    id INTEGER PRIMARY KEY,
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


SELECT_SIGNAL_BY_ID_SQL = """
SELECT
    id,
    symbol,
    timeframe,
    strategy_name,
    signal_type
FROM signals
WHERE id = ?;
"""


SELECT_PREVIOUS_SIGNAL_SQL = """
SELECT signal_type
FROM signals
WHERE symbol = ?
  AND timeframe = ?
  AND strategy_name = ?
  AND id < ?
ORDER BY id DESC
LIMIT 1;
"""


SELECT_POSITION_SQL = """
SELECT qty, realized_pnl
FROM positions
WHERE symbol = ?
LIMIT 1;
"""


SELECT_PENDING_APPROVED_BUY_COUNT_SQL = """
SELECT COUNT(*)
FROM risk_events re
LEFT JOIN orders o ON o.risk_event_id = re.id
WHERE re.symbol = ?
  AND re.signal_type = 'BUY'
  AND re.decision = 'APPROVED'
  AND o.id IS NULL;
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


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def _get_pending_approved_buy_qty(
    connection: DBConnection,
    symbol: str,
    order_qty: float,
) -> float:
    """Return the virtual qty committed by APPROVED BUY risk events not yet executed.

    Queries risk_events LEFT JOIN orders; rows where orders.id IS NULL represent
    approvals that have not been fulfilled by an order yet.  Returns 0.0 if the
    orders table does not exist (pre-migration state).
    """
    if not table_exists(connection, "orders"):
        return 0.0
    row = connection.execute(SELECT_PENDING_APPROVED_BUY_COUNT_SQL, (symbol,)).fetchone()
    count = int(row[0]) if row else 0
    return count * order_qty


def _evaluate_signal_row(
    connection: DBConnection,
    signal_row,
    order_qty: float,
    max_position_qty: float,
    cooldown_seconds: int,
    max_daily_loss: float,
) -> Dict[str, Union[int, str]]:
    signal_id, symbol, timeframe, strategy_name, signal_type = signal_row

    daily_realized_pnl: Optional[float] = None
    total_realized_pnl: Optional[float] = None

    # 0. Kill switch — block all risk evaluation when the switch is active.
    if kill_switch_enabled():
        decision = "REJECTED"
        reason = "Kill switch is enabled."
    elif signal_type == "HOLD":
        # 1. HOLD signals are never actionable.
        decision = "REJECTED"
        reason = "Signal is HOLD."
    else:
        position_row = connection.execute(SELECT_POSITION_SQL, (symbol,)).fetchone()
        current_qty = float(position_row[0]) if position_row is not None else 0.0
        total_realized_pnl = float(position_row[1]) if position_row is not None else 0.0
        daily_realized_pnl = get_daily_realized_pnl(connection, symbol)

        # 2. Daily loss limit — auto-enable kill switch when breached.
        # Guard: max_daily_loss=0 means "no limit configured"; skip the check to
        # avoid triggering on any signal with non-positive daily PnL (0.0 <= -0.0).
        if max_daily_loss > 0 and daily_realized_pnl <= -abs(max_daily_loss):
            decision = "REJECTED"
            reason = (
                f"Daily loss limit breached: daily_realized_pnl={daily_realized_pnl}, "
                f"limit=-{abs(max_daily_loss)}."
            )
            breach_payload = {
                "symbol": symbol,
                "strategy_name": strategy_name,
                "daily_realized_pnl": daily_realized_pnl,
                "max_daily_loss": max_daily_loss,
                "limit": -abs(max_daily_loss),
            }
            insert_event(
                connection,
                event_type="daily_loss_breach",
                status="triggered",
                source="risk_service",
                message=reason,
                payload=breach_payload,
            )
            enable_kill_switch(
                reason=(
                    "Kill switch auto-enabled by risk service after daily loss limit breach. "
                    + reason
                ),
                source="risk_service",
                notify_message=(
                    "Crypto alert: kill switch auto-enabled after daily loss limit breach."
                ),
                payload_extra=breach_payload,
            )
        else:
            # 3. Cooldown — check time since last fill.
            latest_fill_at: Optional[datetime] = None
            if table_exists(connection, "fills"):
                fill_row = connection.execute(SELECT_LATEST_FILL_SQL, (symbol,)).fetchone()
                if fill_row is not None:
                    latest_fill_at = parse_db_timestamp(fill_row[0])

            pending_qty = _get_pending_approved_buy_qty(connection, symbol, order_qty)
            effective_buy_qty = current_qty + pending_qty

            if latest_fill_at is not None:
                cooldown_elapsed = (datetime.now(timezone.utc) - latest_fill_at).total_seconds()
                if cooldown_elapsed < cooldown_seconds:
                    decision = "REJECTED"
                    reason = (
                        f"Cooldown active: last fill {int(cooldown_elapsed)} seconds ago, "
                        f"minimum {cooldown_seconds}."
                    )
                else:
                    decision, reason = _apply_position_and_duplicate_rules(
                        connection, signal_id, symbol, timeframe, strategy_name,
                        signal_type, current_qty, effective_buy_qty, order_qty,
                        max_position_qty, pending_qty,
                    )
            else:
                decision, reason = _apply_position_and_duplicate_rules(
                    connection, signal_id, symbol, timeframe, strategy_name,
                    signal_type, current_qty, effective_buy_qty, order_qty,
                    max_position_qty, pending_qty,
                )

    risk_event_id = insert_and_get_rowid(
        connection,
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
            "risk_event_id": risk_event_id,
            "signal_id": signal_id,
            "symbol": symbol,
            "signal_type": signal_type,
            "decision": decision,
            "daily_realized_pnl": daily_realized_pnl if signal_type not in ("HOLD",) else None,
            "total_realized_pnl": total_realized_pnl if signal_type not in ("HOLD",) else None,
        },
    )

    return {
        "id": risk_event_id,
        "signal_id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_name": strategy_name,
        "signal_type": signal_type,
        "decision": decision,
        "reason": reason,
    }


def _apply_position_and_duplicate_rules(
    connection: DBConnection,
    signal_id: int,
    symbol: str,
    timeframe: str,
    strategy_name: str,
    signal_type: str,
    current_qty: float,
    effective_buy_qty: float,
    order_qty: float,
    max_position_qty: float,
    pending_qty: float,
) -> tuple:
    """Apply position-limit and duplicate-signal rules. Returns (decision, reason)."""
    if signal_type == "BUY" and effective_buy_qty + order_qty > max_position_qty:
        return "REJECTED", f"Max position exceeded: current={current_qty}, pending={pending_qty}, limit={max_position_qty}."
    if signal_type == "BUY" and effective_buy_qty > 0:
        return "REJECTED", f"Existing long position already open (pending_qty={pending_qty})."
    if signal_type == "SELL" and current_qty <= 0:
        return "REJECTED", "No position available to sell."
    previous_signal = connection.execute(
        SELECT_PREVIOUS_SIGNAL_SQL,
        (symbol, timeframe, strategy_name, signal_id),
    ).fetchone()
    if previous_signal and previous_signal[0] == signal_type:
        return "REJECTED", "Duplicate signal type."
    if signal_type == "BUY":
        approved, portfolio_reason = check_portfolio_limits(connection, strategy_name, symbol, order_qty)
        if not approved:
            return "REJECTED", portfolio_reason
    return "APPROVED", "Passed basic risk checks."


def evaluate_latest_signal(
    connection: DBConnection,
    order_qty: Optional[float] = None,
    max_position_qty: Optional[float] = None,
    cooldown_seconds: Optional[int] = None,
    max_daily_loss: Optional[float] = None,
) -> Optional[Dict[str, Union[int, str]]]:
    latest_signal = connection.execute(SELECT_LATEST_SIGNAL_SQL).fetchone()
    if latest_signal is None:
        return None

    strategy_name = latest_signal[3]
    cfg, _ = get_risk_config(connection, strategy_name)

    return _evaluate_signal_row(
        connection,
        latest_signal,
        order_qty=order_qty if order_qty is not None else cfg.order_qty,
        max_position_qty=max_position_qty if max_position_qty is not None else cfg.max_position_qty,
        cooldown_seconds=cooldown_seconds if cooldown_seconds is not None else cfg.cooldown_seconds,
        max_daily_loss=max_daily_loss if max_daily_loss is not None else cfg.max_daily_loss,
    )


def evaluate_signal_id(
    connection: DBConnection,
    signal_id: int,
    order_qty: Optional[float] = None,
    max_position_qty: Optional[float] = None,
    cooldown_seconds: Optional[int] = None,
    max_daily_loss: Optional[float] = None,
) -> Optional[Dict[str, Union[int, str]]]:
    signal_row = connection.execute(SELECT_SIGNAL_BY_ID_SQL, (signal_id,)).fetchone()
    if signal_row is None:
        return None

    # Load per-strategy config; falls back to global defaults if no DB row exists.
    strategy_name = signal_row[3]
    cfg, _ = get_risk_config(connection, strategy_name)

    return _evaluate_signal_row(
        connection,
        signal_row,
        order_qty=order_qty if order_qty is not None else cfg.order_qty,
        max_position_qty=max_position_qty if max_position_qty is not None else cfg.max_position_qty,
        cooldown_seconds=cooldown_seconds if cooldown_seconds is not None else cfg.cooldown_seconds,
        max_daily_loss=max_daily_loss if max_daily_loss is not None else cfg.max_daily_loss,
    )


def evaluate_signal_ids(
    connection: DBConnection,
    signal_ids: List[int],
    order_qty: Optional[float] = None,
    max_position_qty: Optional[float] = None,
    cooldown_seconds: Optional[int] = None,
    max_daily_loss: Optional[float] = None,
) -> List[Dict[str, Union[int, str]]]:
    """Evaluate a batch of signal IDs, loading per-strategy risk config for each."""
    results: List[Dict[str, Union[int, str]]] = []
    for sid in signal_ids:
        result = evaluate_signal_id(
            connection,
            sid,
            order_qty=order_qty,
            max_position_qty=max_position_qty,
            cooldown_seconds=cooldown_seconds,
            max_daily_loss=max_daily_loss,
        )
        if result is not None:
            results.append(result)
    return results
