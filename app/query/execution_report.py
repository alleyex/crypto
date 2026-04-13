"""Execution report: fill aggregation and PnL breakdown."""
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import parse_db_timestamp
from app.core.db import utc_now
from app.query.activity_summary import (
    _commission_in_quote,
    _fills_by_order_id,
    _replay_closed_trades,
    _report_fill_sort_key,
)

def _normalize_binance_trade_fill(order_id: int, side: str, trade: dict[str, Any]) -> dict[str, Any]:
    qty = float(trade.get("qty") or trade.get("executedQty") or 0.0)
    price = float(trade.get("price") or 0.0)
    transact_time = int(trade.get("time") or trade.get("transactTime") or 0) or None
    if transact_time is None:
        created_at = datetime.now(timezone.utc).isoformat()
    else:
        created_at = datetime.fromtimestamp(transact_time / 1000, tz=timezone.utc).isoformat()
    quote_qty = trade.get("quoteQty")
    if quote_qty in (None, ""):
        quote_qty = qty * price
    else:
        quote_qty = float(quote_qty)
    return {
        "id": None,
        "order_id": order_id,
        "symbol": str(trade.get("symbol") or ""),
        "side": side,
        "qty": qty,
        "price": price,
        "commission": float(trade.get("commission") or 0.0),
        "commission_asset": str(trade.get("commissionAsset") or ""),
        "quote_qty": quote_qty,
        "transact_time": transact_time,
        "created_at": created_at,
    }

def _fetch_binance_report_fills(
    orders: list[dict[str, Any]],
    *,
    symbol: str,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    relevant_orders = [
        order for order in orders
        if order.get("symbol") == symbol
        and str(order.get("broker_name") or "").lower() == "binance"
        and order.get("broker_order_id") not in (None, "")
    ]
    if not relevant_orders:
        return []

    broker_order_map = {str(order["broker_order_id"]): order for order in relevant_orders}
    try:
        from app.execution.binance_broker import BinanceBrokerClient

        broker_trades = BinanceBrokerClient().get_user_trades(
            symbol,
            start_time=int(cutoff.timestamp() * 1000),
            limit=1000,
        )
    except Exception:
        return []

    normalized: list[dict[str, Any]] = []
    for trade in broker_trades:
        broker_order_id = str(trade.get("orderId") or "")
        order = broker_order_map.get(broker_order_id)
        if order is None:
            continue
        side = str(order.get("side") or "").upper()
        normalized.append(_normalize_binance_trade_fill(int(order["id"]), side, trade))
    normalized.sort(key=_report_fill_sort_key, reverse=True)
    return normalized

def _merge_report_fills(
    local_fills: list[dict[str, Any]],
    broker_fills: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not broker_fills:
        return local_fills

    broker_by_order_id = _fills_by_order_id(broker_fills)
    merged: list[dict[str, Any]] = []
    for order_id, fills in broker_by_order_id.items():
        merged.extend(fills)

    for fill in local_fills:
        order_id = fill.get("order_id")
        if order_id is None or int(order_id) not in broker_by_order_id:
            merged.append(fill)

    merged.sort(key=_report_fill_sort_key, reverse=True)
    return merged

def get_execution_report(
    connection: DBConnection,
    *,
    symbol: str = "BTCUSDT",
    strategy_name: str | None = None,
    days: int = 7,
    limit: int = 10,
) -> dict[str, Any]:
    from app.query.read_service import get_all_orders, get_all_fills, get_job_queue_jobs

    cutoff = utc_now().astimezone(timezone.utc)
    cutoff = cutoff.replace(microsecond=0)
    cutoff = cutoff - timedelta(days=days)

    orders = get_all_orders(connection)
    fills = get_all_fills(connection)
    positions = fetch_all_as_dicts(
        connection,
        "SELECT symbol, qty, avg_price, realized_pnl, updated_at FROM positions ORDER BY symbol ASC;",
    )
    queue_jobs = get_job_queue_jobs(connection, limit=200)

    fills_by_order_id = _fills_by_order_id(fills)
    strategy_filter = strategy_name.strip() if strategy_name else None

    filtered_orders = [
        item for item in orders
        if item.get("symbol") == symbol
        and (
            str(item.get("status") or "").upper() == "FILLED"
            or fills_by_order_id.get(int(item["id"]))
        )
        and (not strategy_filter or item.get("strategy_name") == strategy_filter)
    ]
    local_filtered_fills = [
        item for item in fills
        if item.get("symbol") == symbol
        and parse_db_timestamp(item["created_at"]) >= cutoff
        and (
            not strategy_filter
            or any(order.get("id") == item.get("order_id") and order.get("strategy_name") == strategy_filter for order in orders)
        )
    ]
    broker_filtered_fills = _fetch_binance_report_fills(filtered_orders, symbol=symbol, cutoff=cutoff)
    filtered_fills = _merge_report_fills(local_filtered_fills, broker_filtered_fills)
    reconciled_fills_by_order_id = _fills_by_order_id(filtered_fills)
    combined_fills_by_order_id = dict(fills_by_order_id)
    combined_fills_by_order_id.update(reconciled_fills_by_order_id)

    closed_trades, _ = _replay_closed_trades(
        filtered_orders,
        combined_fills_by_order_id,
        strategy_filter=strategy_filter,
        symbol_filter=symbol,
        cutoff=cutoff,
        include_hold_minutes=True,
    )
    gross_pnl = sum(float(item["realized_pnl"]) for item in closed_trades)
    closed_trade_count = len(closed_trades)
    wins = sum(1 for item in closed_trades if item["realized_pnl"] > 0)
    hold_values = [float(item["hold_minutes"]) for item in closed_trades if item.get("hold_minutes") is not None]
    best_trade = max((float(item["realized_pnl"]) for item in closed_trades), default=None)
    worst_trade = min((float(item["realized_pnl"]) for item in closed_trades), default=None)
    notional = sum(float(item["price"]) * float(item["qty"]) for item in filtered_fills)
    total_fees = sum(
        _commission_in_quote(fill, float(fill["price"]), float(fill["qty"]))
        for fill in filtered_fills
    )

    daily_rows: dict[str, dict[str, Any]] = {}
    for fill in filtered_fills:
        trade_date = str(fill["created_at"])[:10]
        row = daily_rows.setdefault(
            trade_date,
            {"trade_date": trade_date, "fills": 0, "notional": 0.0, "gross_pnl": 0.0, "fees": 0.0},
        )
        qty = float(fill["qty"])
        price = float(fill["price"])
        row["fills"] += 1
        row["notional"] += qty * price
        row["fees"] += _commission_in_quote(fill, price, qty)
    for trade in closed_trades:
        trade_date = str(trade["closed_at"])[:10]
        row = daily_rows.setdefault(
            trade_date,
            {"trade_date": trade_date, "fills": 0, "notional": 0.0, "gross_pnl": 0.0, "fees": 0.0},
        )
        row["gross_pnl"] += float(trade["realized_pnl"])

    daily = sorted(daily_rows.values(), key=lambda item: item["trade_date"], reverse=True)
    for row in daily:
        row["net_pnl"] = row["gross_pnl"] - row["fees"]

    failed_execution_jobs = [
        job for job in queue_jobs
        if job.get("job_type") == "execution"
        and job.get("status") == "failed"
        and parse_db_timestamp(job["created_at"]) >= cutoff
    ]
    current_position = next((item for item in positions if item.get("symbol") == symbol), None)

    return {
        "summary": {
            "symbol": symbol,
            "strategy_name": strategy_filter or "all",
            "days": days,
            "fills": len(filtered_fills),
            "closed_trades": closed_trade_count,
            "notional": notional,
            "gross_pnl": gross_pnl,
            "fees": total_fees,
            "net_pnl": gross_pnl - total_fees,
            "win_rate": (wins / closed_trade_count) if closed_trade_count else None,
            "avg_hold_minutes": (sum(hold_values) / len(hold_values)) if hold_values else None,
            "best_trade": best_trade,
            "worst_trade": worst_trade,
            "current_position": current_position,
        },
        "daily": daily[:limit],
        "recent_closed_trades": closed_trades[:limit],
        "recent_failed_execution_jobs": failed_execution_jobs[:limit],
        "recent_fills": sorted(filtered_fills, key=lambda item: str(item["created_at"]), reverse=True)[:limit],
    }
