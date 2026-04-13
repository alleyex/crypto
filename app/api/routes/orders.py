from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.api.errors import http_bad_request

from app.audit.service import insert_event
from app.core.db import DBConnection

from app.execution.adapter import get_execution_adapter
from app.execution.exchange_trades import binance_futures_enabled
from app.pipeline.execution_job import reconcile_orphan_orders
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_orders
from pydantic import BaseModel
from app.api.deps import get_db

router = APIRouter()

class ReconcileOrdersRequest(BaseModel):
    audit_action: str | None = None
    audit_message: str | None = None

@router.post("/orders/reconcile")
def reconcile_orders(
    payload: ReconcileOrdersRequest | None = None,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    adapter = get_execution_adapter()
    broker_client = getattr(adapter, "_broker", None)
    orphan_results = reconcile_orphan_orders(
        connection, is_live=adapter.is_live, broker_client=broker_client
    )
    updated_symbols = update_positions(connection)
    snapshot_count = update_pnl_snapshots(connection)
    latest_orders = get_orders(connection, limit=5)
    insert_event(
        connection,
        event_type="execution_control",
        status="reconciled",
        source="execution_control",
        message=(
            payload.audit_message
            if payload is not None and payload.audit_message is not None
            else "Order reconciliation completed."
        ),
        payload={
            "action": (
                payload.audit_action
                if payload is not None and payload.audit_action is not None
                else "reconcile_orders"
            ),
            "orphan_reconcile_count": len(orphan_results),
            "updated_symbols": updated_symbols,
            "snapshot_count": snapshot_count,
            "latest_order_count": len(latest_orders),
        },
    )
    return {
        "status": "reconciled",
        "orphan_results": orphan_results,
        "updated_symbols": updated_symbols,
        "snapshot_count": snapshot_count,
        "orders": latest_orders,
    }

@router.post("/execution/sync-from-exchange")
def sync_fills_from_exchange(
    symbol: str = Query(default="SOLUSDT"),
    days: int = Query(default=7, ge=1, le=30),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Sync actual Binance trade fills into local DB.

    For each Binance trade, finds the matching local order by broker_order_id and
    upserts the fill with real price, qty, commission, and realized_pnl.
    Orders without a matching local record are counted but skipped.
    After sync, rebuilds positions and daily PnL from the corrected fills.
    Only available when CRYPTO_EXECUTION_BACKEND=binance and CRYPTO_BINANCE_FUTURES=true.
    """
    if not binance_futures_enabled():
        http_bad_request("Only available for live Binance Futures mode.")
    from app.execution.binance_broker import BinanceBrokerClient

    client = BinanceBrokerClient()
    cutoff_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000
    )
    raw_trades = client.get_user_trades(symbol.upper(), start_time=cutoff_ms, limit=1000)

    trades_by_order_id: dict[str, list[dict[str, Any]]] = {}
    for trade in raw_trades:
        oid = str(trade.get("orderId") or "")
        if oid:
            trades_by_order_id.setdefault(oid, []).append(trade)

    synced = 0
    skipped_no_local_order = 0

    for broker_order_id, order_trades in trades_by_order_id.items():
        order_row = connection.execute(
            "SELECT id, symbol, side FROM orders WHERE broker_order_id = ? LIMIT 1",
            (broker_order_id,),
        ).fetchone()
        if order_row is None:
            skipped_no_local_order += 1
            continue

        local_order_id = int(order_row[0])
        order_symbol = str(order_row[1])
        total_qty = sum(float(t.get("qty") or 0) for t in order_trades)
        total_commission = sum(float(t.get("commission") or 0) for t in order_trades)
        total_realized_pnl = sum(float(t.get("realizedPnl") or 0) for t in order_trades)
        avg_price = (
            sum(
                float(t.get("price") or 0) * float(t.get("qty") or 0)
                for t in order_trades
            )
            / total_qty
            if total_qty > 0
            else 0.0
        )
        side = "BUY" if bool(order_trades[0].get("buyer")) else "SELL"
        commission_asset = str(order_trades[0].get("commissionAsset") or "USDT")
        quote_qty = sum(float(t.get("quoteQty") or 0) for t in order_trades) or None
        transact_time = max(int(t.get("time") or 0) for t in order_trades) or None

        existing_fill = connection.execute(
            "SELECT id FROM fills WHERE order_id = ? LIMIT 1", (local_order_id,)
        ).fetchone()
        if existing_fill is not None:
            connection.execute(
                """UPDATE fills
                   SET qty=?, price=?, commission=?, commission_asset=?, quote_qty=?, transact_time=?
                   WHERE order_id=?""",
                (
                    total_qty,
                    avg_price,
                    total_commission,
                    commission_asset,
                    quote_qty,
                    transact_time,
                    local_order_id,
                ),
            )
        else:
            connection.execute(
                """INSERT INTO fills
                   (order_id, symbol, side, qty, price, commission, commission_asset, quote_qty, transact_time)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    local_order_id,
                    order_symbol,
                    side,
                    total_qty,
                    avg_price,
                    total_commission,
                    commission_asset,
                    quote_qty,
                    transact_time,
                ),
            )
        connection.execute(
            "UPDATE orders SET status='FILLED', qty=?, price=? WHERE id=? AND status != 'FILLED'",
            (total_qty, avg_price, local_order_id),
        )
        synced += 1

    update_positions(connection)
    rebuild_daily_realized_pnl(connection)
    connection.commit()
    return {
        "status": "ok",
        "symbol": symbol.upper(),
        "days": days,
        "binance_order_count": len(trades_by_order_id),
        "synced_count": synced,
        "skipped_no_local_order": skipped_no_local_order,
    }
