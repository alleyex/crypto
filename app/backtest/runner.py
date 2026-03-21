"""Backtesting engine.

Replays a list of OHLCV candles through the strategy + risk pipeline in an
isolated in-memory SQLite database and returns performance metrics.

Known constraints
-----------------
- Cooldown checking in risk_service uses ``datetime.now(utc)``.  Historical
  candle timestamps are in the past, so the elapsed-since-last-fill will always
  exceed any positive cooldown value.  Pass ``cooldown_seconds=0`` (default) for
  deterministic backtest semantics.
- ``max_daily_loss > 0`` can trigger ``enable_kill_switch()``, which writes to
  ``runtime/kill.switch`` on disk.  Leave ``max_daily_loss=0.0`` (default) to
  keep the backtest fully in-memory with no side effects.
"""

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.backtest.metrics import compute_metrics
from app.core.db import insert_and_get_rowid
from app.core.migrations import run_migrations
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl
from app.portfolio.positions_service import update_positions
from app.risk.risk_config import set_risk_config
from app.risk.risk_service import evaluate_signal_id
from app.strategy.registry import get_strategy


_INSERT_CANDLE_SQL = """
INSERT INTO candles (
    symbol, timeframe, open_time, open, high, low, close,
    volume, close_time, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timeframe, open_time) DO NOTHING;
"""

_INSERT_ORDER_SQL = """
INSERT INTO orders (
    client_order_id, risk_event_id, broker_name, broker_order_id,
    symbol, timeframe, strategy_name, side, qty, price, status, created_at
) VALUES (?, ?, 'backtest', ?, ?, ?, ?, ?, ?, ?, 'FILLED', ?);
"""

_INSERT_FILL_SQL = """
INSERT INTO fills (order_id, symbol, side, qty, price, created_at)
VALUES (?, ?, ?, ?, ?, ?);
"""

_SELECT_POSITION_SQL = """
SELECT qty, avg_price, realized_pnl FROM positions WHERE symbol = ? LIMIT 1;
"""


def _make_connection():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def _epoch_ms_to_iso(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _insert_candle(connection, candle: Dict, symbol: str, timeframe: str, iso_ts: str) -> None:
    open_time = int(candle["open_time"])
    close_time = int(candle.get("close_time", open_time + 59999))
    connection.execute(
        _INSERT_CANDLE_SQL,
        (
            symbol, timeframe,
            open_time,
            str(candle["open"]), str(candle["high"]),
            str(candle["low"]), str(candle["close"]),
            str(candle.get("volume", "0")),
            close_time,
            iso_ts,
        ),
    )


def _simulate_fill(
    connection,
    risk_event_id: int,
    symbol: str,
    timeframe: str,
    strategy_name: str,
    side: str,
    qty: float,
    price: float,
    iso_ts: str,
) -> None:
    client_order_id = f"bt-{risk_event_id}"
    broker_order_id = f"bt-{risk_event_id}"
    order_id = insert_and_get_rowid(
        connection,
        _INSERT_ORDER_SQL,
        (client_order_id, risk_event_id, broker_order_id,
         symbol, timeframe, strategy_name, side, qty, price, iso_ts),
    )
    connection.execute(
        _INSERT_FILL_SQL, (order_id, symbol, side, qty, price, iso_ts)
    )


def _get_position(connection, symbol: str):
    row = connection.execute(_SELECT_POSITION_SQL, (symbol,)).fetchone()
    if row is None:
        return 0.0, 0.0, 0.0
    return float(row[0]), float(row[1]), float(row[2])


def _record_equity(
    equity_curve: List[Dict],
    candle: Dict,
    connection,
    symbol: str,
    initial_capital: float,
    iso_ts: str,
) -> None:
    close = float(candle["close"])
    qty, avg_price, realized_pnl = _get_position(connection, symbol)
    unrealized_pnl = (close - avg_price) * qty if qty > 0 else 0.0
    equity_curve.append({
        "timestamp": iso_ts,
        "open_time": int(candle["open_time"]),
        "close": close,
        "equity": round(initial_capital + realized_pnl + unrealized_pnl, 8),
        "realized_pnl": round(realized_pnl, 8),
        "unrealized_pnl": round(unrealized_pnl, 8),
        "qty": qty,
    })


def run_backtest(
    symbol: str,
    strategy_name: str,
    candles: List[Dict],
    initial_capital: float = 10000.0,
    order_qty: float = 0.001,
    max_position_qty: float = 0.002,
    cooldown_seconds: int = 0,
    max_daily_loss: float = 0.0,
    timeframe: str = "1m",
    fill_on: str = "close",
) -> Dict[str, Any]:
    """Run a backtest and return a result dict.

    Parameters
    ----------
    symbol:           Trading pair, e.g. "BTCUSDT".
    strategy_name:    Key from STRATEGY_REGISTRY, e.g. "ma_cross".
    candles:          List of dicts with keys: open_time (epoch ms), open, high,
                      low, close, volume (optional), close_time (optional).
                      Must be provided in any order — sorted internally.
    initial_capital:  Reference capital for equity / return calculations.
    order_qty:        Quantity per order.
    max_position_qty: Maximum position size per symbol.
    cooldown_seconds: Minimum seconds between fills (0 = disabled).
    max_daily_loss:   Daily loss limit (0 = disabled, avoids kill-switch side-effects).
    timeframe:        Candle timeframe label stored in the DB.
    fill_on:          "close"     — fill at current candle close (default).
                      "next_open" — fill at next candle open (avoids bar-close
                                   lookahead bias; last signal may go unfilled).

    Returns
    -------
    Dict with keys: symbol, strategy_name, candle_count, trade_count,
    metrics, equity_curve, trades.
    """
    if not candles:
        return {
            "symbol": symbol,
            "strategy_name": strategy_name,
            "candle_count": 0,
            "trade_count": 0,
            "metrics": {},
            "equity_curve": [],
            "trades": [],
        }

    connection = _make_connection()
    strategy_fn = get_strategy(strategy_name)
    set_risk_config(
        connection,
        strategy_name=strategy_name,
        order_qty=order_qty,
        max_position_qty=max_position_qty,
        cooldown_seconds=cooldown_seconds,
        max_daily_loss=max_daily_loss,
    )

    sorted_candles = sorted(candles, key=lambda c: int(c["open_time"]))
    equity_curve: List[Dict] = []
    trades: List[Dict] = []
    pending_fill: Optional[Dict] = None  # used only when fill_on="next_open"

    for i, candle in enumerate(sorted_candles):
        iso_ts = _epoch_ms_to_iso(int(candle["open_time"]))

        # Execute pending fill from previous bar (next_open mode)
        if pending_fill is not None:
            fill_price = float(candle["open"])
            _simulate_fill(
                connection,
                risk_event_id=pending_fill["risk_event_id"],
                symbol=symbol,
                timeframe=timeframe,
                strategy_name=strategy_name,
                side=pending_fill["side"],
                qty=order_qty,
                price=fill_price,
                iso_ts=iso_ts,
            )
            update_positions(connection)
            rebuild_daily_realized_pnl(connection)
            connection.commit()
            trades.append({
                "candle_index": i,
                "timestamp": iso_ts,
                "side": pending_fill["side"],
                "qty": order_qty,
                "price": fill_price,
                "risk_event_id": pending_fill["risk_event_id"],
            })
            pending_fill = None

        _insert_candle(connection, candle, symbol, timeframe, iso_ts)
        connection.commit()

        signal_result = strategy_fn(connection, symbol)
        if signal_result is None or str(signal_result.get("signal_type", "HOLD")) == "HOLD":
            _record_equity(equity_curve, candle, connection, symbol, initial_capital, iso_ts)
            continue

        signal_id = int(signal_result["id"])
        risk_result = evaluate_signal_id(
            connection,
            signal_id,
            order_qty=order_qty,
            max_position_qty=max_position_qty,
            cooldown_seconds=cooldown_seconds,
            max_daily_loss=max_daily_loss,
        )
        if risk_result is None or risk_result.get("decision") != "APPROVED":
            _record_equity(equity_curve, candle, connection, symbol, initial_capital, iso_ts)
            continue

        risk_event_id = int(risk_result["id"])
        side = str(signal_result["signal_type"])  # "BUY" or "SELL"

        if fill_on == "next_open":
            pending_fill = {"risk_event_id": risk_event_id, "side": side}
        else:
            fill_price = float(candle["close"])
            _simulate_fill(
                connection,
                risk_event_id=risk_event_id,
                symbol=symbol,
                timeframe=timeframe,
                strategy_name=strategy_name,
                side=side,
                qty=order_qty,
                price=fill_price,
                iso_ts=iso_ts,
            )
            update_positions(connection)
            rebuild_daily_realized_pnl(connection)
            connection.commit()
            trades.append({
                "candle_index": i,
                "timestamp": iso_ts,
                "side": side,
                "qty": order_qty,
                "price": fill_price,
                "risk_event_id": risk_event_id,
            })

        _record_equity(equity_curve, candle, connection, symbol, initial_capital, iso_ts)

    connection.close()
    return {
        "symbol": symbol,
        "strategy_name": strategy_name,
        "candle_count": len(sorted_candles),
        "trade_count": len(trades),
        "metrics": compute_metrics(equity_curve, trades, initial_capital),
        "equity_curve": equity_curve,
        "trades": trades,
    }
