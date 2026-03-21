from typing import Any, Dict, List, Optional

from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.data.candles_service import get_latest_close
from app.execution.adapter import get_execution_adapter
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions


_INSERT_RECONCILE_FILL_SQL = """
INSERT INTO fills (order_id, symbol, side, qty, price)
VALUES (?, ?, ?, ?, ?);
"""


def scan_orphan_orders(connection: DBConnection) -> List[Dict[str, Any]]:
    """Return orders that have no matching fill and are not in a terminal state.

    Returns an empty list if the orders or fills tables do not yet exist.
    """
    try:
        rows = connection.execute(
            """
            SELECT o.id, o.symbol, o.timeframe, o.side, o.qty, o.status, o.created_at
            FROM orders o
            LEFT JOIN fills f ON f.order_id = o.id
            WHERE f.id IS NULL
              AND o.status NOT IN ('CANCELLED', 'REJECTED', 'EXPIRED')
            ORDER BY o.id;
            """
        ).fetchall()
    except Exception:
        return []
    return [
        {
            "order_id": int(row[0]),
            "symbol": row[1],
            "timeframe": row[2],
            "side": row[3],
            "qty": row[4],
            "status": row[5],
            "created_at": row[6],
        }
        for row in rows
    ]


def reconcile_orphan_orders(
    connection: DBConnection,
    is_live: bool = False,
) -> List[Dict[str, Any]]:
    """Reconcile orphan orders by creating missing fills or flagging for manual review.

    For non-live backends (paper, simulated): synthesize a fill at the current
    latest close price so that positions and daily PnL can be rebuilt correctly.

    For live backends: emit a critical audit event and skip auto-fill — real
    money orders must be reconciled manually against the exchange.

    Returns a list of per-order reconciliation results.
    """
    orphans = scan_orphan_orders(connection)
    if not orphans:
        return []

    results: List[Dict[str, Any]] = []
    any_filled = False

    for orphan in orphans:
        order_id = orphan["order_id"]
        symbol = orphan["symbol"]
        timeframe = orphan["timeframe"]
        side = orphan["side"]
        qty = float(orphan["qty"])

        if is_live:
            # Live backend: never synthesize fills. Flag for manual review.
            insert_event(
                connection,
                event_type="orphan_order_live",
                status="critical",
                source="execution_job",
                message=(
                    f"Orphan order {order_id} ({symbol} {side}) on live backend "
                    "requires manual reconciliation against the exchange."
                ),
                payload=orphan,
            )
            results.append({
                "order_id": order_id,
                "action": "flagged_for_manual_review",
                "reason": "live_backend",
            })
            continue

        # Non-live backend: synthesize fill at current close price.
        price = get_latest_close(connection, symbol=symbol, timeframe=timeframe)
        if price is None:
            results.append({
                "order_id": order_id,
                "action": "skipped",
                "reason": "no_candle_data",
            })
            continue

        insert_and_get_rowid(
            connection,
            _INSERT_RECONCILE_FILL_SQL,
            (order_id, symbol, side, qty, price),
        )
        any_filled = True
        insert_event(
            connection,
            event_type="orphan_order_reconciled",
            status="reconciled",
            source="execution_job",
            message=f"Orphan order {order_id} ({symbol} {side} {qty}) reconciled with synthetic fill at {price}.",
            payload={**orphan, "fill_price": price},
        )
        results.append({
            "order_id": order_id,
            "action": "fill_synthesized",
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "fill_price": price,
        })

    if any_filled:
        rebuild_daily_realized_pnl(connection)
        connection.commit()

    return results


def run_execution_job(
    connection: DBConnection,
    risk_event_ids: Optional[list[int]] = None,
    symbol_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    execution_adapter = get_execution_adapter()
    execution_adapter.ensure_tables(connection)
    if risk_event_ids is not None:
        execution_results = execution_adapter.execute_risk_event_ids(connection, risk_event_ids)
        if execution_results:
            paper_execute_steps = [{"step": "paper_execute", **execution_result} for execution_result in execution_results]
        else:
            paper_execute_steps = [{"step": "paper_execute", "status": "skipped", "reason": "No risk events selected"}]
    else:
        execution_results = execution_adapter.execute_pending_approved_risks(connection, symbol_names=symbol_names)
        if execution_results:
            paper_execute_steps = [{"step": "paper_execute", **execution_result} for execution_result in execution_results]
        else:
            latest_execution_result = execution_adapter.execute_latest_risk(connection)
            if latest_execution_result is None:
                paper_execute_steps = [{"step": "paper_execute", "status": "skipped", "reason": "No risk event found"}]
            else:
                paper_execute_steps = [{"step": "paper_execute", **latest_execution_result}]

    updated_positions = update_positions(connection)
    ensure_pnl_table(connection)
    snapshot_count = update_pnl_snapshots(connection)

    is_live = execution_adapter.is_live
    reconcile_results = reconcile_orphan_orders(connection, is_live=is_live)

    orphan_step: Dict[str, Any] = {
        "step": "reconcile_orphan_orders",
        "reconciled_count": len(reconcile_results),
    }
    if reconcile_results:
        orphan_step["status"] = "warning" if is_live else "reconciled"
        orphan_step["results"] = reconcile_results
    else:
        orphan_step["status"] = "ok"

    return {
        "status": "ok",
        "steps": paper_execute_steps
        + [
            {"step": "update_positions", "updated_symbols": updated_positions},
            {"step": "update_pnl", "snapshot_count": snapshot_count},
            orphan_step,
        ],
    }
