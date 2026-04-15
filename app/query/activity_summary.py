"""Strategy activity summary and closed-trade replay logic."""
from datetime import datetime
from typing import Any

from app.core.db import DBConnection
from app.core.db import parse_db_timestamp
from app.core.db import utc_now
from app.core.settings import COMMISSION_RATE
from app.data.binance_client import fetch_book_ticker
from app.strategy.registry import list_registered_strategies

# ---------------------------------------------------------------------------
# Low-level helpers shared with execution_report
# ---------------------------------------------------------------------------

def _fills_by_order_id(fills: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for item in fills:
        order_id = item.get("order_id")
        if order_id is None:
            continue
        grouped.setdefault(int(order_id), []).append(item)
    return grouped

def _executed_orders(
    orders: list[dict[str, Any]],
    fills_by_order_id: dict[int, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    executed: list[dict[str, Any]] = []
    for item in orders:
        order_id = int(item["id"])
        if str(item.get("status") or "").upper() == "FILLED" or fills_by_order_id.get(order_id):
            executed.append(item)
    return executed

def _commission_in_quote(fill_record: dict[str, Any] | None, price: float, qty: float) -> float:
    if fill_record and fill_record.get("commission") is not None:
        commission = float(fill_record["commission"])
        asset = str(fill_record.get("commission_asset") or "")
        return commission if asset == "USDT" else commission * price
    return qty * price * COMMISSION_RATE

def _report_fill_sort_key(fill_record: dict[str, Any]) -> tuple[int, str]:
    transact_time = fill_record.get("transact_time")
    if transact_time not in (None, ""):
        return (int(transact_time), "")
    return (0, str(fill_record.get("created_at") or ""))

def _close_trade_status(realized_pnl: float) -> str:
    if realized_pnl > 0:
        return "win"
    if realized_pnl < 0:
        return "loss"
    return "breakeven"

def _replay_closed_trades(
    orders: list[dict[str, Any]],
    fills_by_order_id: dict[int, list[dict[str, Any]]],
    *,
    strategy_filter: str | None = None,
    symbol_filter: str | None = None,
    cutoff: datetime | None = None,
    include_hold_minutes: bool = False,
) -> tuple[list[dict[str, Any]], dict[tuple[str, str, str], dict[str, Any]]]:
    allowed_strategy_names = set(list_registered_strategies())
    filled_orders = list(reversed(_executed_orders(orders, fills_by_order_id)))
    positions_by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    last_entry_at_by_key: dict[tuple[str, str, str], datetime | None] = {}
    closed_trades: list[dict[str, Any]] = []

    for order in filled_orders:
        current_strategy_name = str(order["strategy_name"])
        if current_strategy_name not in allowed_strategy_names:
            continue
        if strategy_filter and current_strategy_name != strategy_filter:
            continue

        symbol = str(order["symbol"])
        if symbol_filter and symbol != symbol_filter:
            continue

        timeframe = str(order.get("timeframe") or "1m")
        key = (current_strategy_name, symbol, timeframe)
        position = positions_by_key.setdefault(key, {"qty": 0.0, "cost": 0.0, "opened_at": None})

        order_fills = fills_by_order_id.get(int(order["id"]), [])
        fill_record = order_fills[-1] if order_fills else None
        created_at_raw = fill_record["created_at"] if fill_record is not None else order["created_at"]
        created_at = parse_db_timestamp(created_at_raw)
        if cutoff is not None and created_at < cutoff:
            continue

        qty = float(order["qty"])
        price = float(order["price"])
        side = str(order["side"]).upper()
        current_qty = float(position["qty"])

        if side == "BUY":
            if current_qty < 0:
                close_qty = min(qty, abs(current_qty))
                average_entry_price = position["cost"] / abs(current_qty)
                realized_pnl = (average_entry_price - price) * close_qty
                hold_minutes = None
                if include_hold_minutes:
                    entry_at = last_entry_at_by_key.get(key)
                    if entry_at is not None:
                        hold_minutes = round((created_at - entry_at).total_seconds() / 60, 2)
                closed_trades.append(
                    {
                        "strategy_name": current_strategy_name,
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "qty": close_qty,
                        "entry_price": average_entry_price,
                        "exit_price": price,
                        "realized_pnl": realized_pnl,
                        "closed_at": created_at_raw,
                        "order_id": order["id"],
                        "status": _close_trade_status(realized_pnl),
                        **({"hold_minutes": hold_minutes} if include_hold_minutes else {}),
                    }
                )
                position["qty"] += close_qty
                position["cost"] -= close_qty * average_entry_price
                remaining_buy = qty - close_qty
                if remaining_buy > 1e-9:
                    position["qty"] += remaining_buy
                    position["cost"] += remaining_buy * price
                    position["opened_at"] = created_at_raw
                    last_entry_at_by_key[key] = created_at
                elif abs(position["qty"]) < 1e-9:
                    position["qty"] = 0.0
                    position["cost"] = 0.0
                    position["opened_at"] = None
                    last_entry_at_by_key[key] = None
                continue

            position["qty"] += qty
            position["cost"] += qty * price
            if qty > 0:
                if abs(current_qty) < 1e-9:
                    position["opened_at"] = created_at_raw
                last_entry_at_by_key[key] = created_at
            continue

        if side != "SELL":
            continue

        if current_qty > 0:
            close_qty = min(qty, current_qty)
            average_entry_price = position["cost"] / current_qty
            realized_pnl = (price - average_entry_price) * close_qty
            hold_minutes = None
            if include_hold_minutes:
                entry_at = last_entry_at_by_key.get(key)
                if entry_at is not None:
                    hold_minutes = round((created_at - entry_at).total_seconds() / 60, 2)
            closed_trades.append(
                {
                    "strategy_name": current_strategy_name,
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "qty": close_qty,
                    "entry_price": average_entry_price,
                    "exit_price": price,
                    "realized_pnl": realized_pnl,
                    "closed_at": created_at_raw,
                    "order_id": order["id"],
                    "status": _close_trade_status(realized_pnl),
                    **({"hold_minutes": hold_minutes} if include_hold_minutes else {}),
                }
            )
            position["qty"] -= close_qty
            position["cost"] -= close_qty * average_entry_price
            remaining_sell = qty - close_qty
            if remaining_sell > 1e-9:
                position["qty"] -= remaining_sell
                position["cost"] += remaining_sell * price
                position["opened_at"] = created_at_raw
                last_entry_at_by_key[key] = created_at
            elif abs(position["qty"]) < 1e-9:
                position["qty"] = 0.0
                position["cost"] = 0.0
                position["opened_at"] = None
                last_entry_at_by_key[key] = None
            continue

        position["qty"] -= qty
        position["cost"] += qty * price
        if qty > 0:
            if abs(current_qty) < 1e-9:
                position["opened_at"] = created_at_raw
            last_entry_at_by_key[key] = created_at

    closed_trades.sort(key=lambda item: str(item["closed_at"]), reverse=True)
    return closed_trades, positions_by_key

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_strategy_closed_trades(
    connection: DBConnection,
    limit: int = 20,
    per_table_limit: int = 200,
    strategy_name: str | None = None,
) -> list[dict[str, Any]]:
    from app.query.read_service import get_all_orders, get_all_fills
    orders = get_all_orders(connection)
    fills = get_all_fills(connection)
    fills_by_order_id = _fills_by_order_id(fills)
    strategy_filter = strategy_name.strip() if strategy_name else None
    closed_trades, _ = _replay_closed_trades(
        orders,
        fills_by_order_id,
        strategy_filter=strategy_filter,
    )
    return closed_trades[:limit]

def get_strategy_activity_summary(
    connection: DBConnection,
    per_table_limit: int = 100,
    include_live_book: bool = False,
) -> list[dict[str, Any]]:
    from app.query.read_service import (
        get_signals,
        get_risk_events,
        get_all_orders,
        get_all_fills,
    )
    strategy_names = list_registered_strategies()
    signals = get_signals(connection, limit=per_table_limit)
    risk_events = get_risk_events(connection, limit=per_table_limit)
    orders = get_all_orders(connection)
    fills = get_all_fills(connection)
    fills_by_order_id = _fills_by_order_id(fills)
    db_positions = {
        str(symbol): {
            "qty": float(qty),
            "avg_price": float(avg_price),
            "updated_at": updated_at,
        }
        for symbol, qty, avg_price, _realized_pnl, updated_at in connection.execute(
            """
            SELECT symbol, qty, avg_price, realized_pnl, updated_at
            FROM positions
            """
        ).fetchall()
    }
    closed_trades = get_strategy_closed_trades(
        connection,
        limit=max(len(strategy_names), per_table_limit),
        per_table_limit=per_table_limit,
    )
    latest_closed_trades: dict[str, Any] = {}
    for item in closed_trades:
        key = str(item["strategy_name"])
        if key not in latest_closed_trades:
            latest_closed_trades[key] = item

    summaries: list[dict[str, Any]] = []
    strategy_symbol_map: dict[str, str] = {}
    for strategy_name in strategy_names:
        latest_signal = next((item for item in signals if item["strategy_name"] == strategy_name), None)
        latest_risk = next((item for item in risk_events if item["strategy_name"] == strategy_name), None)
        latest_order = next((item for item in orders if item["strategy_name"] == strategy_name), None)
        strategy_orders = [item for item in orders if item["strategy_name"] == strategy_name]
        order_ids = {item["id"] for item in strategy_orders}
        latest_fill = next((item for item in fills if item["order_id"] in order_ids), None)
        latest_closed_trade = latest_closed_trades.get(strategy_name)
        latest_activity_candidates = [
            timestamp
            for timestamp in (
                latest_fill["created_at"] if latest_fill is not None else None,
                latest_order["created_at"] if latest_order is not None else None,
                latest_risk["created_at"] if latest_risk is not None else None,
                latest_signal["created_at"] if latest_signal is not None else None,
            )
            if timestamp is not None
        ]
        latest_activity_at = (
            max(parse_db_timestamp(timestamp) for timestamp in latest_activity_candidates).isoformat()
            if latest_activity_candidates
            else None
        )
        executed_orders = _executed_orders(strategy_orders, fills_by_order_id)
        filled_order_count = len(executed_orders)
        strategy_closed_trades, strategy_positions = _replay_closed_trades(
            strategy_orders,
            fills_by_order_id,
            strategy_filter=strategy_name,
        )

        gross_realized_pnl = sum(float(item["realized_pnl"]) for item in strategy_closed_trades)
        total_commission = 0.0
        filled_qty_total = 0.0
        buy_fill_count = 0
        sell_fill_count = 0
        realized_trade_count = len(strategy_closed_trades)
        winning_trade_count = sum(1 for item in strategy_closed_trades if float(item["realized_pnl"]) > 0)
        losing_trade_count = sum(1 for item in strategy_closed_trades if float(item["realized_pnl"]) < 0)
        breakeven_trade_count = realized_trade_count - winning_trade_count - losing_trade_count

        for order in executed_orders:
            symbol = order["symbol"]
            qty = float(order["qty"])
            price = float(order["price"])
            filled_qty_total += qty
            order_fills = fills_by_order_id.get(int(order["id"]), [])
            fill_record = order_fills[-1] if order_fills else None
            if fill_record and fill_record.get("commission") is not None:
                c = float(fill_record["commission"])
                asset = fill_record.get("commission_asset", "")
                total_commission += c * price if asset != "USDT" else c
            else:
                total_commission += qty * price * COMMISSION_RATE
            if order["side"] == "BUY":
                buy_fill_count += 1
            elif order["side"] == "SELL":
                sell_fill_count += 1
        net_position_qty = sum(item["qty"] for item in strategy_positions.values())
        open_entry_price = None
        open_position_symbol = None
        open_position_opened_at = None
        for (_, sym, _timeframe), pos in strategy_positions.items():
            if abs(pos["qty"]) > 1e-9:
                open_entry_price = pos["cost"] / abs(pos["qty"])
                open_position_symbol = sym
                open_position_opened_at = pos.get("opened_at")
                break

        # Prefer the current positions table over replayed fills. The replay is
        # useful for realized PnL/trade stats, but it can drift from the actual
        # live position state when fills are synced or positions are rebuilt.
        latest_symbol = (
            str(latest_signal["symbol"]) if latest_signal and latest_signal.get("symbol") else None
        )
        current_position_symbol = open_position_symbol or latest_symbol
        if current_position_symbol:
            db_position = db_positions.get(current_position_symbol)
            if db_position is not None:
                db_qty = float(db_position["qty"])
                net_position_qty = db_qty
                if abs(db_qty) > 1e-9:
                    open_position_symbol = current_position_symbol
                    open_entry_price = float(db_position["avg_price"]) if db_position["avg_price"] else None
                    open_position_opened_at = db_position.get("updated_at")
                else:
                    open_position_symbol = None
                    open_entry_price = None
                    open_position_opened_at = None

        current_price: float | None = None
        price_symbol: str | None = None
        sig_symbol = latest_signal["symbol"] if latest_signal else None
        sig_timeframe = latest_signal["timeframe"] if latest_signal else None
        if sig_symbol and sig_timeframe:
            strategy_symbol_map[strategy_name] = sig_symbol
            price_row = connection.execute(
                "SELECT close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY open_time DESC LIMIT 1",
                (sig_symbol, sig_timeframe),
            ).fetchone()
            if price_row:
                current_price = float(price_row[0])
                price_symbol = sig_symbol

        summaries.append(
            {
                "strategy_name": strategy_name,
                "latest_signal": latest_signal,
                "latest_risk": latest_risk,
                "latest_order": latest_order,
                "latest_fill": latest_fill,
                "latest_closed_trade": latest_closed_trade,
                "latest_activity_at": latest_activity_at,
                "latest_order_at": latest_order["created_at"] if latest_order is not None else None,
                "latest_fill_at": latest_fill["created_at"] if latest_fill is not None else None,
                "filled_order_count": filled_order_count,
                "filled_qty_total": filled_qty_total,
                "net_position_qty": net_position_qty,
                "open_entry_price": open_entry_price,
                "open_position_symbol": open_position_symbol,
                "open_position_opened_at": open_position_opened_at,
                "current_price": current_price,
                "price_symbol": price_symbol,
                "gross_realized_pnl": gross_realized_pnl,
                "total_commission": total_commission,
                "net_realized_pnl": gross_realized_pnl - total_commission,
                "buy_fill_count": buy_fill_count,
                "sell_fill_count": sell_fill_count,
                "realized_trade_count": realized_trade_count,
                "winning_trade_count": winning_trade_count,
                "losing_trade_count": losing_trade_count,
                "breakeven_trade_count": breakeven_trade_count,
                "has_activity": any(
                    item is not None for item in (latest_signal, latest_risk, latest_order, latest_fill)
                ),
            }
        )

    if include_live_book:
        book_tickers: dict[str, dict[str, Any]] = {}
        for symbol in sorted({value for value in strategy_symbol_map.values() if value}):
            try:
                book_tickers[symbol] = fetch_book_ticker(symbol=symbol)
            except Exception:
                continue

        for item in summaries:
            symbol = strategy_symbol_map.get(item["strategy_name"])
            book_ticker = book_tickers.get(symbol or "")
            if not book_ticker:
                continue
            bid_price = book_ticker.get("bid_price")
            ask_price = book_ticker.get("ask_price")
            item["book_ticker"] = book_ticker
            item["bid_price"] = bid_price
            item["ask_price"] = ask_price
            item["bid_qty"] = book_ticker.get("bid_qty")
            item["ask_qty"] = book_ticker.get("ask_qty")
            if bid_price is not None and ask_price is not None:
                item["current_price"] = (float(bid_price) + float(ask_price)) / 2

    return summaries
