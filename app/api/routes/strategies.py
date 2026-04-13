from typing import Any

from fastapi import APIRouter, Depends

from app.core.db import DBConnection
from app.execution.exchange_trades import (
    binance_futures_enabled,
    build_exchange_trade_snapshot,
    get_binance_futures_positions,
    normalize_binance_futures_position,
)
from app.query.read_service import get_strategy_activity_summary
from app.api.deps import get_db

router = APIRouter()

@router.get("/strategies/summary")
def strategy_summary(connection: DBConnection = Depends(get_db)) -> list[dict[str, Any]]:
    summaries = get_strategy_activity_summary(connection, include_live_book=True)
    if binance_futures_enabled():
        try:
            exchange_positions = {
                str(item["symbol"]).upper(): item
                for item in get_binance_futures_positions(include_flat=True)
            }
        except Exception:
            exchange_positions = {}
        for item in summaries:
            latest_signal = item.get("latest_signal") or {}
            latest_fill = item.get("latest_fill") or {}
            latest_order = item.get("latest_order") or {}
            symbol = (
                latest_signal.get("symbol")
                or item.get("open_position_symbol")
                or item.get("price_symbol")
                or latest_fill.get("symbol")
                or latest_order.get("symbol")
            )
            if not symbol:
                continue
            symbol_upper = str(symbol).upper()
            position = exchange_positions.get(symbol_upper)
            qty = float(position.get("qty") or 0.0) if position else 0.0
            item["net_position_qty"] = qty
            item["open_position_symbol"] = symbol_upper if qty != 0 else None
            item["open_entry_price"] = (
                float(position.get("avg_price") or 0.0) if qty != 0 else None
            )
            item["open_position_opened_at"] = (
                position.get("created_at") if position and qty != 0 else None
            )
            item["exchange_current_position"] = normalize_binance_futures_position(
                symbol_upper, position
            )
            try:
                snapshot = build_exchange_trade_snapshot(
                    symbol_upper,
                    strategy_name=str(item.get("strategy_name") or ""),
                    days=7,
                    limit=1000,
                )
                unrealized_pnl = (
                    float(position.get("unrealized_pnl") or 0.0) if position else 0.0
                )
                item.update({
                    "filled_order_count": len(snapshot["trades"]),
                    "filled_qty_total": snapshot["filled_qty_total"],
                    "gross_realized_pnl": snapshot["gross_pnl"],
                    "total_commission": snapshot["total_fees"],
                    "net_realized_pnl": snapshot["gross_pnl"] - snapshot["total_fees"],
                    "buy_fill_count": snapshot["buy_count"],
                    "sell_fill_count": snapshot["sell_count"],
                    "realized_trade_count": snapshot["realized_trade_count"],
                    "winning_trade_count": snapshot["win_trade_count"],
                    "losing_trade_count": snapshot["loss_trade_count"],
                    "breakeven_trade_count": (
                        snapshot["realized_trade_count"]
                        - snapshot["win_trade_count"]
                        - snapshot["loss_trade_count"]
                    ),
                    "unrealized_pnl": unrealized_pnl,
                    "exchange_pnl_source": "binance_user_trades",
                    "price_symbol": symbol_upper,
                    "latest_exchange_closed_trade": snapshot["latest_closed_trade"],
                })
                if snapshot["latest_closed_trade"] is not None:
                    item["latest_closed_trade"] = snapshot["latest_closed_trade"]
            except Exception:
                pass
    return summaries
