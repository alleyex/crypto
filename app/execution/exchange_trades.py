"""Exchange trade helpers — Binance Futures position / trade utilities.

Extracted from app/api/main.py so that route modules can import them
without pulling in the entire FastAPI application.
"""
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.settings import BINANCE_FUTURES, EXECUTION_BACKEND
from app.execution.runtime import get_execution_backend_runtime_status

_BINANCE_FUTURES_MAX_WINDOW_MS = 7 * 24 * 3600 * 1000  # 7-day Binance limit

def binance_futures_enabled() -> bool:
    runtime = get_execution_backend_runtime_status()
    backend = str(runtime.get("backend") or EXECUTION_BACKEND).lower()
    return backend == "binance" and BINANCE_FUTURES

def get_binance_futures_positions(
    *, symbol: str | None = None, include_flat: bool = False
) -> list[dict[str, Any]]:
    if not binance_futures_enabled():
        return []
    from app.execution.binance_broker import BinanceBrokerClient

    client = BinanceBrokerClient()
    return client.get_positions(symbol=symbol, include_flat=include_flat)

def normalize_binance_futures_position(
    symbol: str, position: dict[str, Any] | None
) -> dict[str, Any]:
    if position:
        updated_at = position.get("updated_at")
        age_seconds = None
        if updated_at:
            age_seconds = int(
                (
                    datetime.now(timezone.utc)
                    - datetime.fromtimestamp(int(updated_at) / 1000, tz=timezone.utc)
                ).total_seconds()
            )
        return {
            "symbol": symbol.upper(),
            "qty": position.get("qty", 0.0),
            "avg_price": position.get("avg_price", 0.0),
            "realized_pnl": None,
            "unrealized_pnl": position.get("unrealized_pnl", 0.0),
            "updated_at": position.get("created_at"),
            "age_seconds": age_seconds,
            "source": "binance_futures",
        }
    return {
        "symbol": symbol.upper(),
        "qty": 0.0,
        "avg_price": 0.0,
        "realized_pnl": None,
        "unrealized_pnl": 0.0,
        "updated_at": None,
        "age_seconds": None,
        "source": "binance_futures",
    }

def get_binance_futures_position(symbol: str) -> dict[str, Any]:
    positions = get_binance_futures_positions(symbol=symbol, include_flat=True)
    return normalize_binance_futures_position(symbol, positions[0] if positions else None)

def normalize_exchange_trade(symbol: str, trade: dict[str, Any]) -> dict[str, Any]:
    price = float(trade.get("price") or 0.0)
    qty = float(trade.get("qty") or 0.0)
    quote_qty_value = trade.get("quoteQty")
    quote_qty = float(quote_qty_value) if quote_qty_value not in (None, "") else price * qty
    commission = float(trade.get("commission") or 0.0)
    realized_pnl = float(trade.get("realizedPnl") or trade.get("realized_pnl") or 0.0)
    transact_time = int(
        trade.get("time") or trade.get("transactTime") or trade.get("transact_time") or 0
    )
    raw_side = trade.get("side")
    if raw_side:
        side = str(raw_side).upper()
    else:
        side = "BUY" if bool(trade.get("buyer")) else "SELL"
    return {
        "trade_id": trade.get("id") or trade.get("trade_id"),
        "id": trade.get("id") or trade.get("trade_id"),
        "order_id": str(trade.get("orderId") or trade.get("order_id") or ""),
        "symbol": str(trade.get("symbol") or symbol.upper()),
        "side": side,
        "price": price,
        "qty": qty,
        "quote_qty": quote_qty,
        "commission": commission,
        "commission_asset": str(
            trade.get("commissionAsset") or trade.get("commission_asset") or ""
        ),
        "realized_pnl": realized_pnl,
        "role": "maker" if bool(trade.get("maker")) else str(trade.get("role") or "taker"),
        "transact_time": transact_time,
        "created_at": (
            datetime.fromtimestamp(transact_time / 1000, tz=timezone.utc).isoformat()
            if transact_time
            else None
        ),
    }

def normalize_exchange_trades(
    symbol: str, trades: list[dict[str, Any]], *, days: int, limit: int
) -> dict[str, Any]:
    normalized = [normalize_exchange_trade(symbol, trade) for trade in trades]
    normalized.sort(key=lambda item: int(item.get("transact_time") or 0), reverse=True)
    limited = normalized[:limit]
    return {
        "summary": {
            "symbol": symbol.upper(),
            "days": days,
            "fills": len(limited),
            "total_qty": sum(float(item["qty"]) for item in limited),
            "notional": sum(float(item["quote_qty"]) for item in limited),
            "fees": sum(
                float(item["commission"])
                for item in limited
                if str(item.get("commission_asset") or "") == "USDT"
            ),
            "realized_pnl": sum(float(item["realized_pnl"]) for item in limited),
            "source": "binance_user_trades",
        },
        "trades": limited,
    }

def get_exchange_trades(symbol: str, *, days: int, limit: int) -> dict[str, Any]:
    from app.execution.binance_broker import BinanceBrokerClient

    client = BinanceBrokerClient()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    trades = client.get_user_trades(
        symbol.upper(),
        start_time=int(cutoff.timestamp() * 1000),
        limit=max(limit, 1000),
    )
    return normalize_exchange_trades(symbol, trades, days=days, limit=limit)

def latest_exchange_closed_trade(
    trades: list[dict[str, Any]],
) -> dict[str, Any] | None:
    realized_trades = [
        trade for trade in trades if float(trade.get("realized_pnl") or 0.0) != 0.0
    ]
    if not realized_trades:
        return None
    latest = max(realized_trades, key=lambda item: int(item.get("transact_time") or 0))
    exit_fee = float(latest.get("commission") or 0.0)
    realized_pnl = float(latest.get("realized_pnl") or 0.0)
    return {
        "strategy_name": None,
        "symbol": latest.get("symbol"),
        "timeframe": None,
        "qty": float(latest.get("qty") or 0.0),
        "entry_price": None,
        "exit_price": float(latest.get("price") or 0.0),
        "realized_pnl": realized_pnl,
        "exit_fee": exit_fee,
        "exit_fee_asset": str(latest.get("commission_asset") or ""),
        "net_after_exit_fee": (
            realized_pnl - exit_fee
            if str(latest.get("commission_asset") or "").upper() == "USDT"
            else None
        ),
        "closed_at": latest.get("created_at"),
        "order_id": latest.get("order_id"),
        "status": (
            "win" if realized_pnl > 0 else "loss" if realized_pnl < 0 else "breakeven"
        ),
        "source": "binance_user_trades",
        "side": latest.get("side"),
        "trade_id": latest.get("trade_id"),
    }

def resolve_report_window(
    *,
    days: int,
    start_date: str | None,
    end_date: str | None,
) -> tuple[int, int | None, int | None, dict[str, Any]]:
    """Return (resolved_days, start_ms, end_ms, window_meta).

    Raises ValueError if end_date is before start_date.
    """
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = (
            datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            + timedelta(days=1)
        )
        if end_dt <= start_dt:
            raise ValueError("end_date must be on or after start_date")
        resolved_days = max(1, int((end_dt - start_dt).total_seconds() // 86400))
        return (
            resolved_days,
            int(start_dt.timestamp() * 1000),
            int(end_dt.timestamp() * 1000) - 1,
            {
                "start_date": start_date,
                "end_date": end_date,
                "window_label": f"{start_date} -> {end_date}",
            },
        )

    return (
        days,
        None,
        None,
        {
            "start_date": None,
            "end_date": None,
            "window_label": f"last {days} days",
        },
    )

def build_exchange_trade_snapshot(
    symbol: str,
    *,
    strategy_name: str | None = None,
    days: int,
    limit: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    from app.execution.binance_broker import BinanceBrokerClient

    resolved_days, start_time, end_time, window_meta = resolve_report_window(
        days=days,
        start_date=start_date,
        end_date=end_date,
    )
    client = BinanceBrokerClient()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if start_time is None:
        start_time = int(
            (datetime.now(timezone.utc) - timedelta(days=resolved_days)).timestamp() * 1000
        )
    if end_time is None:
        end_time = now_ms
    end_time = min(end_time, now_ms)

    seen: set[int] = set()
    recent_trades: list[dict[str, Any]] = []
    recent_realized_trades: list[dict[str, Any]] = []
    gross_pnl = 0.0
    total_fees = 0.0
    total_notional = 0.0
    total_qty = 0.0
    fills_count = 0
    buy_count = 0
    sell_count = 0
    realized_trade_count = 0
    win_trade_count = 0
    loss_trade_count = 0
    best_trade: float | None = None
    worst_trade: float | None = None
    latest_closed_trade_raw: dict[str, Any] | None = None
    daily_rows: dict[str, dict[str, Any]] = {}

    def _trim_recent(
        rows: list[dict[str, Any]], max_items: int
    ) -> list[dict[str, Any]]:
        if len(rows) <= max_items * 4:
            return rows
        return sorted(
            rows, key=lambda item: int(item.get("transact_time") or 0), reverse=True
        )[:max_items]

    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + _BINANCE_FUTURES_MAX_WINDOW_MS - 1, end_time)
        chunk = client.get_user_trades(
            symbol.upper(),
            start_time=chunk_start,
            end_time=chunk_end,
            limit=1000,
        )
        for raw_trade in chunk:
            tid = int(raw_trade.get("id") or raw_trade.get("tradeId") or 0)
            if tid and tid in seen:
                continue
            if tid:
                seen.add(tid)
            trade = normalize_exchange_trade(symbol, raw_trade)
            fills_count += 1
            total_qty += float(trade.get("qty") or 0.0)
            total_notional += float(trade.get("quote_qty") or 0.0)
            gross_pnl += float(trade.get("realized_pnl") or 0.0)
            if str(trade.get("commission_asset") or "").upper() == "USDT":
                total_fees += float(trade.get("commission") or 0.0)
            side = str(trade.get("side") or "").upper()
            if side == "BUY":
                buy_count += 1
            elif side == "SELL":
                sell_count += 1
            trade_date = str(trade.get("created_at") or "")[:10]
            if trade_date:
                row = daily_rows.setdefault(
                    trade_date,
                    {
                        "trade_date": trade_date,
                        "fills": 0,
                        "notional": 0.0,
                        "gross_pnl": 0.0,
                        "fees": 0.0,
                    },
                )
                row["fills"] += 1
                row["notional"] += float(trade.get("quote_qty") or 0.0)
                row["gross_pnl"] += float(trade.get("realized_pnl") or 0.0)
                if str(trade.get("commission_asset") or "").upper() == "USDT":
                    row["fees"] += float(trade.get("commission") or 0.0)
            recent_trades.append(trade)
            recent_trades = _trim_recent(recent_trades, limit)

            realized_value = float(trade.get("realized_pnl") or 0.0)
            if realized_value != 0.0:
                realized_trade_count += 1
                if realized_value > 0:
                    win_trade_count += 1
                elif realized_value < 0:
                    loss_trade_count += 1
                best_trade = realized_value if best_trade is None else max(best_trade, realized_value)
                worst_trade = (
                    realized_value if worst_trade is None else min(worst_trade, realized_value)
                )
                recent_realized_trades.append(trade)
                recent_realized_trades = _trim_recent(recent_realized_trades, limit)
                if latest_closed_trade_raw is None or int(
                    trade.get("transact_time") or 0
                ) > int(latest_closed_trade_raw.get("transact_time") or 0):
                    latest_closed_trade_raw = trade
        chunk_start = chunk_end + 1

    trades = sorted(
        recent_trades, key=lambda item: int(item.get("transact_time") or 0), reverse=True
    )[:limit]
    realized_trades = sorted(
        recent_realized_trades,
        key=lambda item: int(item.get("transact_time") or 0),
        reverse=True,
    )[:limit]
    exchange = {
        "summary": {
            "symbol": symbol.upper(),
            "days": resolved_days,
            "fills": fills_count,
            "total_qty": total_qty,
            "notional": total_notional,
            "fees": total_fees,
            "realized_pnl": gross_pnl,
            "source": "binance_user_trades",
            **window_meta,
            "start_time": start_time,
            "end_time": end_time,
        },
        "trades": trades,
    }
    closed_trade = latest_exchange_closed_trade(
        [latest_closed_trade_raw] if latest_closed_trade_raw is not None else []
    )
    if closed_trade is not None:
        closed_trade["strategy_name"] = strategy_name or closed_trade.get("strategy_name")
    recent_closed_trades = [
        {
            "strategy_name": strategy_name or "all",
            "symbol": item["symbol"],
            "timeframe": "1m",
            "qty": item["qty"],
            "entry_price": None,
            "exit_price": item["price"],
            "realized_pnl": item["realized_pnl"],
            "exit_fee": item["commission"],
            "exit_fee_asset": item["commission_asset"],
            "net_after_exit_fee": (
                float(item["realized_pnl"]) - float(item.get("commission") or 0.0)
                if str(item.get("commission_asset") or "").upper() == "USDT"
                else None
            ),
            "closed_at": item["created_at"],
            "hold_minutes": None,
            "order_id": item["order_id"],
            "status": (
                "win"
                if float(item["realized_pnl"]) > 0
                else "loss"
                if float(item["realized_pnl"]) < 0
                else "breakeven"
            ),
            "source": "binance_user_trades",
        }
        for item in realized_trades[:limit]
    ]
    daily = sorted(daily_rows.values(), key=lambda item: item["trade_date"], reverse=True)
    for row in daily:
        row["net_pnl"] = row["gross_pnl"] - row["fees"]
    return {
        "exchange": exchange,
        "trades": trades,
        "daily": daily,
        "gross_pnl": gross_pnl,
        "total_fees": total_fees,
        "realized_trade_count": realized_trade_count,
        "win_trade_count": win_trade_count,
        "loss_trade_count": loss_trade_count,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
        "latest_closed_trade": closed_trade,
        "recent_closed_trades": recent_closed_trades,
        "filled_qty_total": total_qty,
        "buy_count": buy_count,
        "sell_count": sell_count,
    }

def get_exchange_trades_for_window(
    symbol: str,
    *,
    days: int,
    limit: int,
    start_date: str | None,
    end_date: str | None,
) -> dict[str, Any]:
    from app.execution.binance_broker import BinanceBrokerClient

    resolved_days, start_time, end_time, window_meta = resolve_report_window(
        days=days,
        start_date=start_date,
        end_date=end_date,
    )
    client = BinanceBrokerClient()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    if start_time is None:
        start_time = int(
            (datetime.now(timezone.utc) - timedelta(days=resolved_days)).timestamp() * 1000
        )
    if end_time is None:
        end_time = now_ms
    end_time = min(end_time, now_ms)

    all_trades: list[dict[str, Any]] = []
    chunk_start = start_time
    while chunk_start < end_time:
        chunk_end = min(chunk_start + _BINANCE_FUTURES_MAX_WINDOW_MS - 1, end_time)
        chunk = client.get_user_trades(
            symbol.upper(),
            start_time=chunk_start,
            end_time=chunk_end,
            limit=1000,
        )
        all_trades.extend(chunk)
        chunk_start = chunk_end + 1

    seen: set[int] = set()
    deduped: list[dict[str, Any]] = []
    for t in all_trades:
        tid = int(t.get("id") or t.get("tradeId") or 0)
        if tid and tid in seen:
            continue
        seen.add(tid)
        deduped.append(t)

    normalized = normalize_exchange_trades(symbol, deduped, days=resolved_days, limit=limit)
    normalized["summary"].update(window_meta)
    normalized["summary"]["days"] = resolved_days
    normalized["summary"]["start_time"] = start_time
    normalized["summary"]["end_time"] = end_time
    return normalized


def enrich_report_with_exchange_snapshot(
    report: dict[str, Any],
    symbol: str,
    strategy_name: str | None,
    resolved_days: int,
    window_meta: dict[str, Any],
    start_date: str | None,
    end_date: str | None,
    limit: int,
) -> None:
    """Enrich a local execution report dict in-place with Binance snapshot data.

    Raises HTTPException(502) via http_bad_gateway if the Binance API call fails.
    No-op when Binance Futures is not enabled.
    """
    if not binance_futures_enabled():
        return
    from app.api.errors import http_bad_gateway
    try:
        snapshot = build_exchange_trade_snapshot(
            symbol,
            strategy_name=strategy_name or "ppo",
            days=resolved_days,
            start_date=start_date,
            end_date=end_date,
            limit=max(limit, 100),
        )
    except Exception as exc:
        http_bad_gateway(f"Binance API error: {exc}")
    exchange = snapshot["exchange"]
    report["summary"] = {
        **report.get("summary", {}),
        "symbol": symbol.upper(),
        "strategy_name": strategy_name or "all",
        "days": resolved_days,
        **window_meta,
        "fills": exchange["summary"]["fills"],
        "closed_trades": snapshot["realized_trade_count"],
        "notional": exchange["summary"]["notional"],
        "gross_pnl": snapshot["gross_pnl"],
        "fees": snapshot["total_fees"],
        "net_pnl": snapshot["gross_pnl"] - snapshot["total_fees"],
        "win_rate": (
            snapshot["win_trade_count"] / snapshot["realized_trade_count"]
            if snapshot["realized_trade_count"]
            else None
        ),
        "avg_hold_minutes": None,
        "best_trade": snapshot["best_trade"],
        "worst_trade": snapshot["worst_trade"],
        "current_position": get_binance_futures_position(symbol),
        "source": "binance_user_trades",
        "latest_exchange_closed_trade": snapshot["latest_closed_trade"],
    }
    report["daily"] = snapshot["daily"][:limit]
    report["recent_fills"] = snapshot["trades"][:limit]
    report["recent_closed_trades"] = snapshot["recent_closed_trades"][:limit]
    report["exchange_trades"] = exchange
