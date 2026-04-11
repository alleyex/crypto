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

_INSERT_RECONCILE_FILL_FULL_SQL = """
INSERT INTO fills (order_id, symbol, side, qty, price, commission, commission_asset, quote_qty, transact_time)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def scan_orphan_orders(connection: DBConnection) -> List[Dict[str, Any]]:
    """Return orders that have no matching fill and are not in a terminal state.

    Returns an empty list if the orders or fills tables do not yet exist.
    """
    try:
        rows = connection.execute(
            """
            SELECT o.id, o.symbol, o.timeframe, o.side, o.qty, o.status, o.created_at,
                   o.broker_order_id, o.broker_name
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
            "broker_order_id": row[7],
            "broker_name": row[8],
        }
        for row in rows
    ]


def reconcile_orphan_orders(
    connection: DBConnection,
    is_live: bool = False,
    broker_client: Optional[Any] = None,
) -> List[Dict[str, Any]]:
    """Reconcile orphan orders by creating missing fills or flagging for manual review.

    For non-live backends (paper, simulated): synthesize a fill at the current
    latest close price so that positions and daily PnL can be rebuilt correctly.

    For live backends with broker_client: query the exchange for actual fill
    status. If FILLED, record real fill data and update order status.

    For live backends without broker_client: emit a critical audit event and
    skip auto-fill — real money orders must be reconciled manually.

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
        broker_order_id = orphan.get("broker_order_id")

        if is_live:
            # Live backend with broker_client and broker_order_id: query exchange.
            if broker_client is not None and broker_order_id:
                try:
                    fill_data = broker_client.query_order(symbol, str(broker_order_id))
                    exchange_status = str(fill_data.get("status", "UNKNOWN")).upper()
                    fill_price = float(fill_data.get("fill_price") or 0)
                    fill_qty = float(fill_data.get("fill_qty") or 0)
                    commission = fill_data.get("commission")
                    commission_asset = fill_data.get("commission_asset")
                    quote_qty = fill_data.get("quote_qty")
                    transact_time = fill_data.get("transact_time")

                    if exchange_status == "FILLED" and fill_price > 0 and fill_qty > 0:
                        # Update order status in DB.
                        connection.execute(
                            "UPDATE orders SET status = ? WHERE id = ?;",
                            (exchange_status, order_id),
                        )
                        insert_and_get_rowid(
                            connection,
                            _INSERT_RECONCILE_FILL_FULL_SQL,
                            (order_id, symbol, side, fill_qty, fill_price,
                             commission, commission_asset, quote_qty, transact_time),
                        )
                        any_filled = True
                        insert_event(
                            connection,
                            event_type="orphan_order_reconciled",
                            status="reconciled",
                            source="execution_job",
                            message=(
                                f"Orphan order {order_id} ({symbol} {side} {fill_qty}) "
                                f"reconciled via {broker_client.broker_name} at {fill_price}."
                            ),
                            payload={
                                **orphan,
                                "fill_price": fill_price,
                                "fill_qty": fill_qty,
                                "exchange_status": exchange_status,
                            },
                        )
                        results.append({
                            "order_id": order_id,
                            "action": "reconciled_from_exchange",
                            "symbol": symbol,
                            "side": side,
                            "qty": fill_qty,
                            "fill_price": fill_price,
                            "exchange_status": exchange_status,
                        })
                        continue

                    # Order not yet filled on exchange.
                    results.append({
                        "order_id": order_id,
                        "action": "skipped",
                        "reason": "not_filled_on_exchange",
                        "exchange_status": exchange_status,
                    })
                    continue
                except Exception as exc:
                    insert_event(
                        connection,
                        event_type="orphan_order_query_failed",
                        status="error",
                        source="execution_job",
                        message=(
                            f"Failed to query order {order_id} ({symbol} {side}) "
                            f"from exchange: {exc}"
                        ),
                        payload={**orphan, "error": str(exc)},
                    )
                    results.append({
                        "order_id": order_id,
                        "action": "query_failed",
                        "reason": str(exc),
                    })
                    continue

            # Live backend without broker_client: flag for manual review.
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
    broker_client = getattr(execution_adapter, "_broker", None)
    reconcile_results = reconcile_orphan_orders(connection, is_live=is_live, broker_client=broker_client)

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
