"""Exchange-enriched API routes.

These routes optionally augment local DB data with live Binance Futures data
when CRYPTO_EXECUTION_BACKEND=binance and CRYPTO_BINANCE_FUTURES=true.
"""
from typing import Any

from fastapi import APIRouter, Query

from app.api.errors import http_bad_request, http_bad_gateway
from app.core.db import get_connection
from app.execution.exchange_trades import (
    binance_futures_enabled,
    enrich_report_with_exchange_snapshot,
    get_binance_futures_positions,
    get_exchange_trades_for_window,
    normalize_binance_futures_position,
    resolve_report_window,
)
from app.query.read_service import get_execution_report, get_positions, get_strategy_activity_summary

router = APIRouter()

@router.get("/strategies/summary")
def strategy_summary() -> list[dict[str, Any]]:
    connection = get_connection()
    try:
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
                item["open_entry_price"] = float(position.get("avg_price") or 0.0) if qty != 0 else None
                item["open_position_opened_at"] = position.get("created_at") if position and qty != 0 else None
                item["exchange_current_position"] = normalize_binance_futures_position(symbol_upper, position)

                try:
                    snapshot = build_exchange_trade_snapshot(
                        symbol_upper,
                        strategy_name=str(item.get("strategy_name") or ""),
                        days=7,
                        limit=1000,
                    )
                    unrealized_pnl = float(position.get("unrealized_pnl") or 0.0) if position else 0.0
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
                        "breakeven_trade_count": snapshot["realized_trade_count"] - snapshot["win_trade_count"] - snapshot["loss_trade_count"],
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
    finally:
        connection.close()

@router.get("/positions")
def positions(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    if binance_futures_enabled():
        try:
            return get_binance_futures_positions(include_flat=False)[:limit]
        except Exception:
            connection = get_connection()
            try:
                return get_positions(connection, limit=limit)
            finally:
                connection.close()
    connection = get_connection()
    try:
        return get_positions(connection, limit=limit)
    finally:
        connection.close()

@router.get("/reports/testnet-execution")
def testnet_execution_report(
    symbol: str = Query(default="BTCUSDT"),
    strategy_name: str | None = Query(default=None),
    days: int = Query(default=7, ge=1, le=30),
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(default=10, ge=1, le=50),
) -> dict[str, Any]:
    try:
        resolved_days, _, _, window_meta = resolve_report_window(
            days=days,
            start_date=start_date,
            end_date=end_date,
        )
    except ValueError as exc:
        http_bad_request(str(exc))
    connection = get_connection()
    try:
        report = get_execution_report(
            connection,
            symbol=symbol,
            strategy_name=strategy_name,
            days=resolved_days,
            limit=limit,
        )
        report.setdefault("summary", {}).update(window_meta)
        report["summary"]["days"] = resolved_days
        enrich_report_with_exchange_snapshot(
            report, symbol, strategy_name, resolved_days, window_meta,
            start_date, end_date, limit,
        )
        return report
    finally:
        connection.close()

@router.get("/reports/exchange-trades")
def exchange_trades_report(
    symbol: str = Query(default="BTCUSDT"),
    days: int = Query(default=7, ge=1, le=30),
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    try:
        exchange = get_exchange_trades_for_window(
            symbol,
            days=days,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except ValueError as exc:
        http_bad_request(str(exc))
    except Exception as exc:
        http_bad_gateway(f"Unable to fetch Binance trade history: {exc}")
    return exchange
