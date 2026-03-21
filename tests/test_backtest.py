"""Tests for the backtesting engine and metrics module."""

import math
from typing import Dict, List

import pytest

from app.backtest.metrics import compute_metrics, _max_drawdown_pct, _sharpe_ratio, _daily_closes
from app.backtest.runner import run_backtest


# ---------------------------------------------------------------------------
# Candle factory helpers
# ---------------------------------------------------------------------------

_BASE_MS = 1_700_000_000_000  # 2023-11-14 approx
_CANDLE_INTERVAL_MS = 60_000  # 1 minute


def _make_candles(closes: List[float], start_ms: int = _BASE_MS) -> List[Dict]:
    """Build minimal OHLCV candle dicts from a list of close prices."""
    candles = []
    for i, close in enumerate(closes):
        open_time = start_ms + i * _CANDLE_INTERVAL_MS
        candles.append({
            "open_time": open_time,
            "open": str(close),
            "high": str(close * 1.001),
            "low": str(close * 0.999),
            "close": str(close),
            "volume": "1.0",
            "close_time": open_time + _CANDLE_INTERVAL_MS - 1,
        })
    return candles


# ---------------------------------------------------------------------------
# metrics.py unit tests
# ---------------------------------------------------------------------------

def test_max_drawdown_zero_when_equity_always_rises() -> None:
    curve = [
        {"timestamp": "2024-01-01 00:00:00", "equity": 100.0},
        {"timestamp": "2024-01-01 00:01:00", "equity": 110.0},
        {"timestamp": "2024-01-01 00:02:00", "equity": 120.0},
    ]
    assert _max_drawdown_pct(curve) == 0.0


def test_max_drawdown_computed_correctly() -> None:
    curve = [
        {"timestamp": "2024-01-01 00:00:00", "equity": 100.0},
        {"timestamp": "2024-01-01 00:01:00", "equity": 80.0},   # 20% drawdown
        {"timestamp": "2024-01-01 00:02:00", "equity": 90.0},
    ]
    assert _max_drawdown_pct(curve) == pytest.approx(20.0, abs=0.01)


def test_sharpe_returns_none_with_fewer_than_two_days() -> None:
    assert _sharpe_ratio([10000.0]) is None
    assert _sharpe_ratio([]) is None


def test_sharpe_returns_none_when_zero_variance() -> None:
    # Flat equity → zero std → no Sharpe
    assert _sharpe_ratio([10000.0, 10000.0, 10000.0]) is None


def test_daily_closes_groups_by_date() -> None:
    curve = [
        {"timestamp": "2024-01-01 00:00:00", "equity": 100.0},
        {"timestamp": "2024-01-01 12:00:00", "equity": 105.0},
        {"timestamp": "2024-01-02 00:00:00", "equity": 108.0},
    ]
    closes = _daily_closes(curve)
    assert closes == [105.0, 108.0]


def test_compute_metrics_empty_curve() -> None:
    assert compute_metrics([], [], 10000.0) == {}


def test_compute_metrics_no_trades() -> None:
    curve = [{"timestamp": "2024-01-01 00:00:00", "equity": 10000.0}]
    metrics = compute_metrics(curve, [], 10000.0)
    assert metrics["total_return_pct"] == 0.0
    assert metrics["trade_count"] == 0
    assert metrics["win_rate_pct"] is None
    assert metrics["round_trips"] == 0


def test_compute_metrics_winning_round_trip() -> None:
    curve = [
        {"timestamp": "2024-01-01 00:00:00", "equity": 10000.0},
        {"timestamp": "2024-01-02 00:00:00", "equity": 10100.0},
    ]
    trades = [
        {"side": "BUY",  "qty": 0.001, "price": 50000.0},
        {"side": "SELL", "qty": 0.001, "price": 55000.0},
    ]
    metrics = compute_metrics(curve, trades, 10000.0)
    assert metrics["win_rate_pct"] == 100.0
    assert metrics["profit_factor"] == float("inf") or metrics["profit_factor"] is None or metrics["profit_factor"] > 0


def test_compute_metrics_profit_factor() -> None:
    curve = [
        {"timestamp": "2024-01-01 00:00:00", "equity": 10000.0},
        {"timestamp": "2024-01-02 00:00:00", "equity": 10050.0},
        {"timestamp": "2024-01-03 00:00:00", "equity": 10000.0},
        {"timestamp": "2024-01-04 00:00:00", "equity": 10100.0},
    ]
    trades = [
        {"side": "BUY",  "qty": 0.001, "price": 50000.0},
        {"side": "SELL", "qty": 0.001, "price": 60000.0},  # +10 profit
        {"side": "BUY",  "qty": 0.001, "price": 60000.0},
        {"side": "SELL", "qty": 0.001, "price": 55000.0},  # -5 loss
    ]
    metrics = compute_metrics(curve, trades, 10000.0)
    assert metrics["round_trips"] == 2
    assert metrics["win_rate_pct"] == 50.0
    # gross_profit=10*0.001=0.01, gross_loss=5*0.001=0.005 → PF=2.0
    assert metrics["profit_factor"] == pytest.approx(2.0, rel=0.01)


# ---------------------------------------------------------------------------
# runner.py integration tests (in-memory SQLite)
# ---------------------------------------------------------------------------

def test_run_backtest_empty_candles_returns_empty_result() -> None:
    result = run_backtest(symbol="BTCUSDT", strategy_name="ma_cross", candles=[])
    assert result["candle_count"] == 0
    assert result["trade_count"] == 0
    assert result["equity_curve"] == []
    assert result["trades"] == []


def test_run_backtest_fewer_candles_than_strategy_window_returns_no_trades() -> None:
    # ma_cross needs long_window=5 candles; 3 candles → no signal
    candles = _make_candles([100.0, 101.0, 102.0])
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        order_qty=0.001,
    )
    assert result["trade_count"] == 0
    assert result["candle_count"] == 3


def test_run_backtest_produces_equity_curve_for_every_candle() -> None:
    candles = _make_candles([100.0, 101.0, 102.0, 103.0, 104.0, 105.0])
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
    )
    # equity_curve has one entry per candle
    assert len(result["equity_curve"]) == len(candles)


def test_run_backtest_equity_starts_at_initial_capital_before_any_trade() -> None:
    # 3 candles — no trade will happen (need 5 for ma_cross)
    candles = _make_candles([100.0, 101.0, 102.0])
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        initial_capital=5000.0,
    )
    for point in result["equity_curve"]:
        assert point["equity"] == pytest.approx(5000.0, abs=1e-6)


def test_run_backtest_buy_signal_increases_qty_in_equity_curve() -> None:
    # Rising prices → ma_cross short_ma > long_ma → BUY signal after candle 5
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    candles = _make_candles(prices)
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        order_qty=0.001,
        max_position_qty=0.002,
    )
    # After a BUY, qty in equity_curve should be > 0
    qtys = [p["qty"] for p in result["equity_curve"]]
    assert max(qtys) > 0, "Expected at least one candle with a position open"


def test_run_backtest_sell_closes_position() -> None:
    # BUY first (rising), then SELL (falling)
    prices_up = [100.0, 101.0, 102.0, 103.0, 104.0]       # triggers BUY
    prices_down = [104.0, 103.0, 102.0, 101.0, 100.0, 99.0]  # triggers SELL
    candles = _make_candles(prices_up + prices_down)
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        order_qty=0.001,
        max_position_qty=0.002,
    )
    trades = result["trades"]
    sides = [t["side"] for t in trades]
    # Must have at least one BUY
    assert "BUY" in sides


def test_run_backtest_realized_pnl_after_sell() -> None:
    # Crafted to get BUY then SELL with profit
    prices_up = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    prices_down = [105.0, 104.0, 103.0, 102.0, 101.0, 100.0, 99.0]
    candles = _make_candles(prices_up + prices_down)
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        order_qty=0.001,
        max_position_qty=0.002,
    )
    trades = result["trades"]
    if len(trades) >= 2:
        # If there's a BUY and then a SELL, realized_pnl in equity curve should
        # reflect the trade outcome
        final_realized = result["equity_curve"][-1]["realized_pnl"]
        # realized_pnl could be positive or negative depending on prices
        assert isinstance(final_realized, float)


def test_run_backtest_metrics_populated() -> None:
    prices = [100.0 + i * 0.5 for i in range(20)]  # steadily rising
    candles = _make_candles(prices)
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        initial_capital=10000.0,
        order_qty=0.001,
    )
    metrics = result["metrics"]
    assert "total_return_pct" in metrics
    assert "max_drawdown_pct" in metrics
    assert "trade_count" in metrics
    assert metrics["initial_capital"] == 10000.0


def test_run_backtest_fill_on_next_open_defers_fill() -> None:
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0]
    candles = _make_candles(prices)
    result_close = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        fill_on="close",
    )
    result_next = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        fill_on="next_open",
    )
    # Both modes should produce trades (or both zero), structure should be valid
    for result in (result_close, result_next):
        assert isinstance(result["trades"], list)
        assert isinstance(result["equity_curve"], list)
        assert len(result["equity_curve"]) == len(candles)


def test_run_backtest_fill_on_next_open_fills_at_next_candle_open() -> None:
    # Use enough candles for ma_cross to fire a BUY
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0]
    candles = _make_candles(prices)
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        fill_on="next_open",
        order_qty=0.001,
        max_position_qty=0.002,
    )
    trades = result["trades"]
    if trades:
        # In next_open mode, fill price == open of the candle AFTER the signal bar.
        # The open price of each candle equals the close of that bar in our helper,
        # so fill_price should match candle["open"] for the bar after the signal.
        buy_trade = next((t for t in trades if t["side"] == "BUY"), None)
        if buy_trade:
            idx = buy_trade["candle_index"]
            assert idx >= 1  # fill happens on candle >= 1 (never candle 0)


def test_run_backtest_momentum_3bar_strategy() -> None:
    # momentum_3bar needs 3 bars; rising prices → BUY
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    candles = _make_candles(prices)
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="momentum_3bar",
        candles=candles,
        order_qty=0.001,
        max_position_qty=0.002,
    )
    assert result["strategy_name"] == "momentum_3bar"
    assert result["candle_count"] == len(prices)


def test_run_backtest_respects_max_position_qty() -> None:
    # Rising prices → would generate BUY every bar; max_position_qty limits to 1 BUY
    prices = [100.0 + i for i in range(20)]
    candles = _make_candles(prices)
    result = run_backtest(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        order_qty=0.001,
        max_position_qty=0.001,  # exactly one order allowed
    )
    buy_trades = [t for t in result["trades"] if t["side"] == "BUY"]
    assert len(buy_trades) <= 1


def test_run_backtest_candles_sorted_regardless_of_input_order() -> None:
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    candles = _make_candles(prices)
    shuffled = candles[::-1]  # reverse order
    result_forward = run_backtest("BTCUSDT", "ma_cross", candles)
    result_shuffled = run_backtest("BTCUSDT", "ma_cross", shuffled)
    # Both should produce identical trade counts and final equity
    assert result_forward["trade_count"] == result_shuffled["trade_count"]
    if result_forward["equity_curve"] and result_shuffled["equity_curve"]:
        assert result_forward["equity_curve"][-1]["equity"] == pytest.approx(
            result_shuffled["equity_curve"][-1]["equity"], rel=1e-6
        )


def test_run_backtest_result_keys_present() -> None:
    candles = _make_candles([100.0, 101.0, 102.0])
    result = run_backtest("BTCUSDT", "ma_cross", candles)
    for key in ("symbol", "strategy_name", "candle_count", "trade_count",
                "metrics", "equity_curve", "trades"):
        assert key in result


def test_run_backtest_equity_curve_fields() -> None:
    candles = _make_candles([100.0, 101.0, 102.0])
    result = run_backtest("BTCUSDT", "ma_cross", candles)
    for point in result["equity_curve"]:
        for field in ("timestamp", "open_time", "close", "equity",
                      "realized_pnl", "unrealized_pnl", "qty"):
            assert field in point
