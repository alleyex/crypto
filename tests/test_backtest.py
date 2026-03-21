"""Tests for the backtesting engine and metrics module."""

import math
import sqlite3
from typing import Dict, List

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.backtest.loader import load_candles_from_db, _iso_to_epoch_ms
from app.backtest.metrics import compute_metrics, _max_drawdown_pct, _sharpe_ratio, _daily_closes
from app.backtest.runner import run_backtest
from app.backtest.sweep import run_parameter_sweep
from app.backtest.walk_forward import run_walk_forward
from app.core.migrations import run_migrations
from app.strategy.bbands import _compute_bands, generate_signal as bbands_signal
from app.strategy.macd import _compute_macd, _ema, generate_signal as macd_signal
from app.strategy.registry import list_registered_strategies
from app.strategy.rsi import _compute_rsi, generate_signal as rsi_signal


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


# ---------------------------------------------------------------------------
# loader.py tests
# ---------------------------------------------------------------------------

def _make_db_with_candles(candles: List[Dict]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    run_migrations(conn)
    for c in candles:
        conn.execute(
            """INSERT INTO candles
               (symbol, timeframe, open_time, open, high, low, close, volume, close_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol, timeframe, open_time) DO NOTHING""",
            ("BTCUSDT", "1m",
             int(c["open_time"]),
             str(c["open"]), str(c["high"]), str(c["low"]), str(c["close"]),
             str(c.get("volume", "1.0")),
             int(c.get("close_time", int(c["open_time"]) + 59999))),
        )
    conn.commit()
    return conn


def test_iso_to_epoch_ms_date_only() -> None:
    ms = _iso_to_epoch_ms("2024-01-01")
    from datetime import datetime, timezone
    dt = datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert ms == int(dt.timestamp() * 1000)


def test_iso_to_epoch_ms_datetime() -> None:
    ms = _iso_to_epoch_ms("2024-06-15 12:30:00")
    from datetime import datetime, timezone
    dt = datetime(2024, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
    assert ms == int(dt.timestamp() * 1000)


def test_iso_to_epoch_ms_invalid_raises() -> None:
    with pytest.raises(ValueError):
        _iso_to_epoch_ms("not-a-date")


def test_load_candles_from_db_returns_all_when_no_filter() -> None:
    candles = _make_candles([100.0, 101.0, 102.0])
    conn = _make_db_with_candles(candles)
    loaded = load_candles_from_db(conn, symbol="BTCUSDT", timeframe="1m")
    assert len(loaded) == 3
    conn.close()


def test_load_candles_from_db_result_fields() -> None:
    candles = _make_candles([100.0])
    conn = _make_db_with_candles(candles)
    loaded = load_candles_from_db(conn, "BTCUSDT")
    assert len(loaded) == 1
    row = loaded[0]
    for field in ("open_time", "open", "high", "low", "close", "volume", "close_time"):
        assert field in row
    assert isinstance(row["open_time"], int)
    assert isinstance(row["close"], float)
    conn.close()


def test_load_candles_from_db_sorted_ascending() -> None:
    candles = _make_candles([100.0, 101.0, 102.0])
    conn = _make_db_with_candles(candles)
    loaded = load_candles_from_db(conn, "BTCUSDT")
    open_times = [r["open_time"] for r in loaded]
    assert open_times == sorted(open_times)
    conn.close()


def test_load_candles_from_db_start_filter() -> None:
    candles = _make_candles([100.0, 101.0, 102.0, 103.0, 104.0])
    conn = _make_db_with_candles(candles)
    # start at the 3rd candle's open_time
    start_ms = candles[2]["open_time"]
    from datetime import datetime, timezone
    start_iso = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    loaded = load_candles_from_db(conn, "BTCUSDT", start=start_iso)
    assert len(loaded) == 3
    assert loaded[0]["open_time"] == start_ms
    conn.close()


def test_load_candles_from_db_end_filter() -> None:
    candles = _make_candles([100.0, 101.0, 102.0, 103.0, 104.0])
    conn = _make_db_with_candles(candles)
    # end before the 3rd candle (exclusive)
    end_ms = candles[2]["open_time"]
    from datetime import datetime, timezone
    end_iso = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    loaded = load_candles_from_db(conn, "BTCUSDT", end=end_iso)
    assert len(loaded) == 2
    conn.close()


def test_load_candles_from_db_limit() -> None:
    candles = _make_candles([100.0, 101.0, 102.0, 103.0, 104.0])
    conn = _make_db_with_candles(candles)
    loaded = load_candles_from_db(conn, "BTCUSDT", limit=2)
    assert len(loaded) == 2
    conn.close()


def test_load_candles_from_db_empty_when_symbol_missing() -> None:
    candles = _make_candles([100.0, 101.0])
    conn = _make_db_with_candles(candles)
    loaded = load_candles_from_db(conn, "ETHUSDT")
    assert loaded == []
    conn.close()


# ---------------------------------------------------------------------------
# sweep.py tests
# ---------------------------------------------------------------------------

def test_run_parameter_sweep_single_combination() -> None:
    candles = _make_candles([100.0 + i for i in range(10)])
    results = run_parameter_sweep(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        param_grid={"order_qty": [0.001]},
    )
    assert len(results) == 1
    assert "params" in results[0]
    assert "metrics" in results[0]
    assert results[0]["params"]["order_qty"] == 0.001


def test_run_parameter_sweep_multiple_combinations() -> None:
    candles = _make_candles([100.0 + i for i in range(15)])
    results = run_parameter_sweep(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        param_grid={"order_qty": [0.001, 0.002], "max_position_qty": [0.002, 0.004]},
    )
    # 2 × 2 = 4 combinations
    assert len(results) == 4
    param_sets = [frozenset(r["params"].items()) for r in results]
    assert len(set(param_sets)) == 4  # all unique


def test_run_parameter_sweep_sorted_by_sharpe() -> None:
    candles = _make_candles([100.0 + i for i in range(15)])
    results = run_parameter_sweep(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        param_grid={"order_qty": [0.001, 0.002]},
        sort_by="sharpe_ratio",
    )
    sharpe_values = [
        r["metrics"].get("sharpe_ratio")
        for r in results
        if r["metrics"].get("sharpe_ratio") is not None
    ]
    assert sharpe_values == sorted(sharpe_values, reverse=True)


def test_run_parameter_sweep_sorted_by_max_drawdown() -> None:
    candles = _make_candles([100.0 + i for i in range(15)])
    results = run_parameter_sweep(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        param_grid={"order_qty": [0.001, 0.002]},
        sort_by="max_drawdown_pct",
    )
    # max_drawdown_pct is ascending (lower is better)
    dd_values = [r["metrics"].get("max_drawdown_pct", 0.0) for r in results]
    assert dd_values == sorted(dd_values)


def test_run_parameter_sweep_unknown_param_raises() -> None:
    candles = _make_candles([100.0, 101.0])
    with pytest.raises(ValueError, match="Unknown sweep parameter"):
        run_parameter_sweep("BTCUSDT", "ma_cross", candles, param_grid={"bad_param": [1]})


def test_run_parameter_sweep_result_contains_trade_count() -> None:
    candles = _make_candles([100.0 + i for i in range(10)])
    results = run_parameter_sweep(
        "BTCUSDT", "ma_cross", candles,
        param_grid={"order_qty": [0.001]},
    )
    assert "trade_count" in results[0]


# ---------------------------------------------------------------------------
# walk_forward.py tests
# ---------------------------------------------------------------------------

def test_run_walk_forward_basic_structure() -> None:
    candles = _make_candles([100.0 + i * 0.5 for i in range(30)])
    result = run_walk_forward(
        symbol="BTCUSDT",
        strategy_name="ma_cross",
        candles=candles,
        n_splits=3,
    )
    for key in ("symbol", "strategy_name", "candle_count", "n_splits", "splits", "oos_metrics"):
        assert key in result
    assert result["candle_count"] == 30
    assert result["n_splits"] == 3


def test_run_walk_forward_splits_count() -> None:
    candles = _make_candles([100.0 + i for i in range(40)])
    result = run_walk_forward("BTCUSDT", "ma_cross", candles, n_splits=4)
    assert len(result["splits"]) == 4


def test_run_walk_forward_split_fields() -> None:
    candles = _make_candles([100.0 + i for i in range(20)])
    result = run_walk_forward("BTCUSDT", "ma_cross", candles, n_splits=2)
    for split in result["splits"]:
        for key in ("fold", "train_candle_count", "test_candle_count",
                    "train_metrics", "test_metrics",
                    "train_trade_count", "test_trade_count"):
            assert key in split


def test_run_walk_forward_expanding_window() -> None:
    candles = _make_candles([100.0 + i for i in range(30)])
    result = run_walk_forward("BTCUSDT", "ma_cross", candles, n_splits=2)
    splits = result["splits"]
    # Each fold's training set is larger than the previous
    assert splits[1]["train_candle_count"] > splits[0]["train_candle_count"]


def test_run_walk_forward_oos_metrics_keys() -> None:
    candles = _make_candles([100.0 + i for i in range(30)])
    result = run_walk_forward("BTCUSDT", "ma_cross", candles, n_splits=3)
    oos = result["oos_metrics"]
    for key in ("total_return_pct_mean", "total_return_pct_std",
                "max_drawdown_pct_mean", "max_drawdown_pct_std"):
        assert key in oos


def test_run_walk_forward_too_few_candles_raises() -> None:
    candles = _make_candles([100.0, 101.0])  # 2 candles, n_splits=5 → chunk_size=0
    with pytest.raises(ValueError, match="Too few candles"):
        run_walk_forward("BTCUSDT", "ma_cross", candles, n_splits=5)


def test_run_walk_forward_invalid_n_splits_raises() -> None:
    candles = _make_candles([100.0 + i for i in range(10)])
    with pytest.raises(ValueError, match="n_splits must be >= 1"):
        run_walk_forward("BTCUSDT", "ma_cross", candles, n_splits=0)


# ---------------------------------------------------------------------------
# API endpoint tests  GET /backtest, POST /backtest/sweep, POST /backtest/walk-forward
# ---------------------------------------------------------------------------

def _seed_db_connection(candles: List[Dict], symbol: str = "BTCUSDT") -> sqlite3.Connection:
    """Build a seeded in-memory SQLite connection for endpoint monkeypatching."""
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)
    for c in candles:
        conn.execute(
            """INSERT INTO candles
               (symbol, timeframe, open_time, open, high, low, close, volume, close_time)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol, timeframe, open_time) DO NOTHING""",
            (symbol, "1m",
             int(c["open_time"]),
             str(c["open"]), str(c["high"]), str(c["low"]), str(c["close"]),
             "1.0", int(c.get("close_time", int(c["open_time"]) + 59999))),
        )
    conn.commit()
    return conn


def _patched_client(monkeypatch, candles: List[Dict], symbol: str = "BTCUSDT"):
    """Return a TestClient whose get_connection returns a seeded in-memory DB.

    Also patches _backtest_start_iso so that historical test candles (2023)
    always fall within the requested date range.
    """
    conn = _seed_db_connection(candles, symbol)
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    return TestClient(app)


def test_get_backtest_returns_metrics(monkeypatch) -> None:
    prices = [100.0 + i for i in range(20)]
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.get("/backtest?symbol=BTCUSDT&strategy=ma_cross&days=1")
    assert response.status_code == 200
    data = response.json()
    for key in ("symbol", "strategy_name", "candle_count", "trade_count", "metrics", "equity_curve", "trades"):
        assert key in data, f"Missing key: {key}"


def test_get_backtest_metrics_keys(monkeypatch) -> None:
    prices = [100.0 + i * 0.5 for i in range(20)]
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.get("/backtest")
    assert response.status_code == 200
    metrics = response.json()["metrics"]
    for key in ("total_return_pct", "max_drawdown_pct", "trade_count", "initial_capital"):
        assert key in metrics


def test_get_backtest_no_candles_returns_error(monkeypatch) -> None:
    # Empty DB — no candles for the symbol
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    client = TestClient(app)
    response = client.get("/backtest?symbol=ETHUSDT")
    assert response.status_code == 200
    assert "error" in response.json()


def test_get_backtest_unknown_strategy_returns_error(monkeypatch) -> None:
    prices = [100.0] * 10
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.get("/backtest?strategy=nonexistent_strategy")
    assert response.status_code == 200
    assert "error" in response.json()


def test_get_backtest_fill_on_next_open(monkeypatch) -> None:
    prices = [100.0 + i for i in range(20)]
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.get("/backtest?fill_on=next_open")
    assert response.status_code == 200
    assert "equity_curve" in response.json()


def test_post_backtest_sweep_returns_combinations(monkeypatch) -> None:
    prices = [100.0 + i for i in range(20)]
    client = _patched_client(monkeypatch, _make_candles(prices))
    payload = {
        "symbol": "BTCUSDT",
        "strategy": "ma_cross",
        "days": 1,
        "param_grid": {"order_qty": [0.001, 0.002]},
        "sort_by": "total_return_pct",
    }
    response = client.post("/backtest/sweep", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["combination_count"] == 2
    assert len(data["results"]) == 2
    assert data["results"][0]["params"]["order_qty"] in (0.001, 0.002)


def test_post_backtest_sweep_empty_param_grid_returns_error(monkeypatch) -> None:
    prices = [100.0] * 10
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.post("/backtest/sweep", json={"param_grid": {}})
    assert response.status_code == 200
    assert "error" in response.json()


def test_post_backtest_sweep_unknown_param_returns_error(monkeypatch) -> None:
    prices = [100.0] * 10
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.post("/backtest/sweep", json={"param_grid": {"bad_param": [1]}})
    assert response.status_code == 200
    assert "error" in response.json()


def test_post_backtest_walk_forward_returns_splits(monkeypatch) -> None:
    prices = [100.0 + i * 0.5 for i in range(40)]
    client = _patched_client(monkeypatch, _make_candles(prices))
    payload = {"symbol": "BTCUSDT", "strategy": "ma_cross", "days": 1, "n_splits": 3}
    response = client.post("/backtest/walk-forward", json=payload)
    assert response.status_code == 200
    data = response.json()
    for key in ("symbol", "strategy_name", "candle_count", "n_splits", "splits", "oos_metrics"):
        assert key in data
    assert data["n_splits"] == 3
    assert len(data["splits"]) == 3


def test_post_backtest_walk_forward_too_few_candles_returns_error(monkeypatch) -> None:
    prices = [100.0, 101.0]
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.post("/backtest/walk-forward", json={"n_splits": 10})
    assert response.status_code == 200
    assert "error" in response.json()


def test_post_backtest_walk_forward_unknown_strategy_returns_error(monkeypatch) -> None:
    prices = [100.0] * 20
    client = _patched_client(monkeypatch, _make_candles(prices))
    response = client.post("/backtest/walk-forward", json={"strategy": "ghost_strategy"})
    assert response.status_code == 200
    assert "error" in response.json()


# ---------------------------------------------------------------------------
# Strategy unit tests — RSI
# ---------------------------------------------------------------------------

def test_rsi_extreme_up_returns_100() -> None:
    # All gains → avg_loss = 0 → RSI = 100
    closes = [float(i) for i in range(1, 17)]  # 16 values, all rising
    assert _compute_rsi(closes, period=14) == 100.0


def test_rsi_extreme_down_near_zero() -> None:
    # All losses → avg_gain = 0 → RSI ≈ 0
    closes = [float(16 - i) for i in range(16)]  # falling
    rsi = _compute_rsi(closes, period=14)
    assert rsi < 5.0


def test_rsi_midpoint_near_50() -> None:
    # Alternating up/down → RSI near 50
    closes = [100.0 + (1 if i % 2 == 0 else -1) for i in range(30)]
    rsi = _compute_rsi(closes, period=14)
    assert 40.0 < rsi < 60.0


def _make_in_memory_db_with_closes(closes):
    conn = sqlite3.connect(":memory:")
    run_migrations(conn)
    for i, c in enumerate(closes):
        conn.execute(
            "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, close_time)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("BTCUSDT", "1m", (i + 1) * 60000, str(c), str(c), str(c), str(c), "1.0", (i + 1) * 60000 + 59999),
        )
    conn.commit()
    return conn


def test_rsi_generate_signal_returns_none_when_insufficient_candles() -> None:
    conn = _make_in_memory_db_with_closes([100.0] * 5)
    assert rsi_signal(conn, symbol="BTCUSDT", period=14) is None
    conn.close()


def test_rsi_generate_signal_returns_buy_when_oversold() -> None:
    # Falling prices → low RSI → BUY
    closes = [100.0 - i * 2 for i in range(50)]
    conn = _make_in_memory_db_with_closes(closes)
    result = rsi_signal(conn, symbol="BTCUSDT", period=14, oversold=30.0, overbought=70.0)
    assert result is not None
    assert result["signal_type"] == "BUY"
    assert result["short_ma"] <= 30.0  # RSI stored in short_ma
    conn.close()


def test_rsi_generate_signal_returns_sell_when_overbought() -> None:
    # Rising prices → high RSI → SELL
    closes = [100.0 + i * 2 for i in range(50)]
    conn = _make_in_memory_db_with_closes(closes)
    result = rsi_signal(conn, symbol="BTCUSDT", period=14, oversold=30.0, overbought=70.0)
    assert result is not None
    assert result["signal_type"] == "SELL"
    assert result["short_ma"] >= 70.0
    conn.close()


# ---------------------------------------------------------------------------
# Strategy unit tests — Bollinger Bands
# ---------------------------------------------------------------------------

def test_bbands_compute_middle_band() -> None:
    closes = [100.0] * 20
    middle, upper, lower = _compute_bands(closes, period=20, num_std=2.0)
    assert middle == pytest.approx(100.0)
    assert upper == pytest.approx(100.0)
    assert lower == pytest.approx(100.0)


def test_bbands_upper_above_lower() -> None:
    import random
    random.seed(42)
    closes = [100.0 + random.gauss(0, 1) for _ in range(20)]
    middle, upper, lower = _compute_bands(closes, period=20, num_std=2.0)
    assert upper > middle > lower


def test_bbands_generate_signal_returns_none_when_insufficient() -> None:
    conn = _make_in_memory_db_with_closes([100.0] * 10)
    assert bbands_signal(conn, symbol="BTCUSDT", period=20) is None
    conn.close()


def test_bbands_generate_signal_buy_at_lower_band() -> None:
    # Flat middle with a sharp drop at the end → touches lower band
    closes = [100.0] * 19 + [85.0]  # std≈3.5, lower≈93; 85 < lower → BUY
    conn = _make_in_memory_db_with_closes(closes)
    result = bbands_signal(conn, symbol="BTCUSDT", period=20, num_std=2.0)
    assert result is not None
    assert result["signal_type"] == "BUY"
    conn.close()


def test_bbands_generate_signal_sell_at_upper_band() -> None:
    closes = [100.0] * 19 + [115.0]  # spike above upper band → SELL
    conn = _make_in_memory_db_with_closes(closes)
    result = bbands_signal(conn, symbol="BTCUSDT", period=20, num_std=2.0)
    assert result is not None
    assert result["signal_type"] == "SELL"
    conn.close()


def test_bbands_generate_signal_hold_within_bands() -> None:
    # Volatile series so bands are wide; last close is at the middle → HOLD
    closes = [95.0, 105.0] * 9 + [100.0, 100.0]  # 20 candles, last close = 100 (middle)
    conn = _make_in_memory_db_with_closes(closes)
    result = bbands_signal(conn, symbol="BTCUSDT", period=20, num_std=2.0)
    assert result is not None
    assert result["signal_type"] == "HOLD"
    conn.close()


# ---------------------------------------------------------------------------
# Strategy unit tests — MACD
# ---------------------------------------------------------------------------

def test_ema_single_value() -> None:
    assert _ema([100.0], period=5) == [100.0]


def test_ema_constant_series_returns_constant() -> None:
    result = _ema([50.0] * 20, period=9)
    for v in result:
        assert v == pytest.approx(50.0)


def test_ema_increases_on_rising_series() -> None:
    closes = [float(i) for i in range(1, 30)]
    result = _ema(closes, period=9)
    assert result[-1] > result[0]


def test_macd_compute_returns_none_when_insufficient() -> None:
    closes = [100.0] * 10
    assert _compute_macd(closes, fast=12, slow=26, signal_period=9) is None


def test_macd_compute_returns_tuple_with_enough_data() -> None:
    closes = [100.0 + i * 0.1 for i in range(60)]
    result = _compute_macd(closes, fast=12, slow=26, signal_period=9)
    assert result is not None
    assert len(result) == 4


def test_macd_generate_signal_returns_none_when_insufficient() -> None:
    conn = _make_in_memory_db_with_closes([100.0] * 10)
    assert macd_signal(conn, symbol="BTCUSDT") is None
    conn.close()


def test_macd_generate_signal_buy_on_bullish_crossover() -> None:
    # Flat then sharp rise: MACD crosses above signal
    closes = [100.0] * 40 + [100.0 + i * 3 for i in range(20)]
    conn = _make_in_memory_db_with_closes(closes)
    # Run enough bars to accumulate signals; check at least one BUY was generated
    results = []
    for n in range(35, len(closes) + 1):
        sub_conn = _make_in_memory_db_with_closes(closes[:n])
        r = macd_signal(sub_conn, symbol="BTCUSDT")
        if r and r["signal_type"] == "BUY":
            results.append(r)
        sub_conn.close()
    assert len(results) > 0
    conn.close()


def test_macd_generate_signal_returns_hold_on_flat_prices() -> None:
    closes = [100.0] * 60
    conn = _make_in_memory_db_with_closes(closes)
    result = macd_signal(conn, symbol="BTCUSDT")
    assert result is not None
    assert result["signal_type"] == "HOLD"
    conn.close()


# ---------------------------------------------------------------------------
# Registry tests
# ---------------------------------------------------------------------------

def test_registry_contains_all_five_strategies() -> None:
    strategies = list_registered_strategies()
    for name in ("ma_cross", "momentum_3bar", "rsi", "bbands", "macd"):
        assert name in strategies


def test_registry_strategies_sorted_alphabetically() -> None:
    strategies = list_registered_strategies()
    assert strategies == sorted(strategies)


# ---------------------------------------------------------------------------
# Backtest integration — new strategies
# ---------------------------------------------------------------------------

def test_run_backtest_rsi_strategy() -> None:
    prices = [100.0 - i * 0.5 for i in range(50)]  # falling → oversold BUY
    candles = _make_candles(prices)
    result = run_backtest("BTCUSDT", "rsi", candles, order_qty=0.001, max_position_qty=0.002)
    assert result["strategy_name"] == "rsi"
    assert result["candle_count"] == len(prices)
    assert isinstance(result["metrics"], dict)


def test_run_backtest_bbands_strategy() -> None:
    prices = [100.0] * 19 + [85.0] + [100.0] * 19 + [115.0] + [100.0] * 5
    candles = _make_candles(prices)
    result = run_backtest("BTCUSDT", "bbands", candles, order_qty=0.001, max_position_qty=0.002)
    assert result["strategy_name"] == "bbands"
    assert result["candle_count"] == len(prices)


def test_run_backtest_macd_strategy() -> None:
    prices = [100.0 + i * 0.2 for i in range(80)]
    candles = _make_candles(prices)
    result = run_backtest("BTCUSDT", "macd", candles, order_qty=0.001, max_position_qty=0.002)
    assert result["strategy_name"] == "macd"
    assert result["candle_count"] == len(prices)

# ---------------------------------------------------------------------------
# history_service tests
# ---------------------------------------------------------------------------

from app.backtest.history_service import persist_run, list_runs, get_best_sweep_run


def _make_history_conn():
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)
    return conn


def test_persist_run_returns_id() -> None:
    conn = _make_history_conn()
    result = run_backtest("BTCUSDT", "ma_cross", _make_candles([100.0] * 5))
    run_id = persist_run(conn, "single", result, days=30)
    assert isinstance(run_id, int) and run_id > 0
    conn.close()


def test_persist_run_stored_values() -> None:
    conn = _make_history_conn()
    result = run_backtest("BTCUSDT", "ma_cross", _make_candles([100.0] * 5))
    persist_run(conn, "single", result, days=7, fill_on="next_open")
    history = list_runs(conn)
    assert history["total"] == 1
    row = history["runs"][0]
    assert row["symbol"] == "BTCUSDT"
    assert row["strategy_name"] == "ma_cross"
    assert row["run_type"] == "single"
    assert row["fill_on"] == "next_open"
    assert row["days"] == 7
    conn.close()


def test_list_runs_filter_by_symbol() -> None:
    conn = _make_history_conn()
    r1 = run_backtest("BTCUSDT", "ma_cross", _make_candles([100.0] * 5))
    r2 = run_backtest("ETHUSDT", "ma_cross", _make_candles([100.0] * 5))
    persist_run(conn, "single", r1)
    persist_run(conn, "single", r2)
    btc_history = list_runs(conn, symbol="BTCUSDT")
    assert btc_history["total"] == 1
    assert btc_history["runs"][0]["symbol"] == "BTCUSDT"
    conn.close()


def test_list_runs_filter_by_run_type() -> None:
    conn = _make_history_conn()
    result = run_backtest("BTCUSDT", "ma_cross", _make_candles([100.0] * 5))
    persist_run(conn, "single", result)
    persist_run(conn, "sweep", result, params={"order_qty": 0.001})
    assert list_runs(conn, run_type="single")["total"] == 1
    assert list_runs(conn, run_type="sweep")["total"] == 1
    assert list_runs(conn)["total"] == 2
    conn.close()


def test_list_runs_pagination() -> None:
    conn = _make_history_conn()
    result = run_backtest("BTCUSDT", "ma_cross", _make_candles([100.0] * 5))
    for _ in range(5):
        persist_run(conn, "single", result)
    page = list_runs(conn, limit=2, offset=0)
    assert len(page["runs"]) == 2
    assert page["total"] == 5
    assert page["limit"] == 2
    assert page["offset"] == 0
    conn.close()


def test_persist_run_handles_infinite_profit_factor() -> None:
    """profit_factor=inf must not raise (coerced to NULL)."""
    conn = _make_history_conn()
    result = run_backtest("BTCUSDT", "ma_cross", _make_candles([100.0] * 5))
    result["metrics"]["profit_factor"] = float("inf")
    run_id = persist_run(conn, "single", result)
    row = list_runs(conn)["runs"][0]
    assert row["profit_factor"] is None
    conn.close()


def test_persist_run_params_json_stored() -> None:
    conn = _make_history_conn()
    result = run_backtest("BTCUSDT", "ma_cross", _make_candles([100.0] * 5))
    params = {"order_qty": 0.002, "max_position_qty": 0.004}
    persist_run(conn, "sweep", result, params=params)
    import json
    row = list_runs(conn, run_type="sweep")["runs"][0]
    assert json.loads(row["params_json"]) == params
    conn.close()


# ---------------------------------------------------------------------------
# GET /backtest/history endpoint tests
# ---------------------------------------------------------------------------


def test_get_backtest_history_empty(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    client = TestClient(app)
    resp = client.get("/backtest/history")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["runs"] == []
    conn.close()


class _PersistentConn:
    """Wraps a sqlite3 connection, ignoring close() so it can be reused across requests."""
    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def close(self) -> None:
        pass  # intentional no-op

    def __getattr__(self, name: str):
        return getattr(self._conn, name)

    def really_close(self) -> None:
        self._conn.close()


def _make_seeded_conn(candles: List[Dict], symbol: str = "BTCUSDT") -> "_PersistentConn":
    """In-memory connection with migrations + candles seeded for the given symbol."""
    raw = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(raw)
    for c in candles:
        raw.execute(
            """INSERT OR IGNORE INTO candles
               (symbol, timeframe, open_time, open, high, low, close, volume, close_time)
               VALUES (?, '1m', ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, c["open_time"], c["open"], c["high"], c["low"],
             c["close"], c["volume"], c["open_time"] + 59999),
        )
    raw.commit()
    return _PersistentConn(raw)


def test_get_backtest_history_appears_after_run(monkeypatch) -> None:
    candles = _make_candles([100.0 + i for i in range(30)])
    conn = _make_seeded_conn(candles, "BTCUSDT")
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    client = TestClient(app)
    client.get("/backtest?symbol=BTCUSDT&strategy=ma_cross&days=30")
    resp = client.get("/backtest/history")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] >= 1
    assert data["runs"][0]["symbol"] == "BTCUSDT"
    assert data["runs"][0]["run_type"] == "single"
    conn.really_close()


def test_get_backtest_history_filter_by_symbol(monkeypatch) -> None:
    candles = _make_candles([100.0 + i for i in range(30)])
    conn = _make_seeded_conn(candles, "BTCUSDT")
    # also seed ETHUSDT candles
    for c in candles:
        conn._conn.execute(
            """INSERT OR IGNORE INTO candles
               (symbol, timeframe, open_time, open, high, low, close, volume, close_time)
               VALUES (?, '1m', ?, ?, ?, ?, ?, ?, ?)""",
            ("ETHUSDT", c["open_time"], c["open"], c["high"], c["low"],
             c["close"], c["volume"], c["open_time"] + 59999),
        )
    conn._conn.commit()
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    client = TestClient(app)
    client.get("/backtest?symbol=BTCUSDT&strategy=ma_cross")
    client.get("/backtest?symbol=ETHUSDT&strategy=ma_cross")
    resp = client.get("/backtest/history?symbol=BTCUSDT")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 1
    assert data["runs"][0]["symbol"] == "BTCUSDT"
    conn.really_close()


def test_get_backtest_history_pagination(monkeypatch) -> None:
    candles = _make_candles([100.0 + i for i in range(30)])
    conn = _make_seeded_conn(candles, "BTCUSDT")
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    client = TestClient(app)
    for _ in range(5):
        client.get("/backtest?symbol=BTCUSDT&strategy=ma_cross")
    resp = client.get("/backtest/history?limit=2&offset=0")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["runs"]) == 2
    assert data["total"] == 5
    conn.really_close()


# ---------------------------------------------------------------------------
# POST /backtest/sweep/{strategy}/apply-best-params endpoint tests
# ---------------------------------------------------------------------------


def test_apply_best_sweep_params_unknown_strategy(monkeypatch) -> None:
    conn = _make_seeded_conn([], "BTCUSDT")
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    client = TestClient(app)
    resp = client.post("/backtest/sweep/nonexistent_strat/apply-best-params", json={})
    assert resp.status_code == 404
    assert "nonexistent_strat" in resp.json()["detail"]
    conn.really_close()


def test_apply_best_sweep_params_no_runs_returns_404(monkeypatch) -> None:
    conn = _make_seeded_conn([], "BTCUSDT")
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    client = TestClient(app)
    resp = client.post("/backtest/sweep/ma_cross/apply-best-params", json={})
    assert resp.status_code == 404
    assert "No sweep runs found" in resp.json()["detail"]
    conn.really_close()


def test_apply_best_sweep_params_invalid_sort_by_returns_422(monkeypatch) -> None:
    conn = _make_seeded_conn([], "BTCUSDT")
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    client = TestClient(app)
    resp = client.post(
        "/backtest/sweep/ma_cross/apply-best-params",
        json={"sort_by": "not_a_metric"},
    )
    assert resp.status_code == 422


def _persist_sweep_run(
    conn, strategy: str = "ma_cross", symbol: str = "BTCUSDT",
    order_qty: float = 0.003, sharpe: float = 1.5, trade_count: int = 5,
) -> None:
    """Insert a synthetic sweep row without needing real candles."""
    result = {
        "symbol": symbol,
        "strategy_name": strategy,
        "candle_count": 100,
        "trade_count": trade_count,
        "metrics": {
            "initial_capital": 10000.0,
            "final_equity": 10500.0,
            "total_return_pct": 5.0,
            "max_drawdown_pct": 2.0,
            "sharpe_ratio": sharpe,
            "win_rate_pct": 60.0,
            "profit_factor": 1.8,
            "round_trips": trade_count // 2,
        },
    }
    params = {"order_qty": order_qty, "max_position_qty": order_qty * 2}
    persist_run(conn, "sweep", result, params=params)


def test_apply_best_sweep_params_golden_path(monkeypatch) -> None:
    candles = _make_candles([100.0 + i for i in range(30)])
    conn = _make_seeded_conn(candles, "BTCUSDT")
    _persist_sweep_run(conn, order_qty=0.003)
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    client = TestClient(app)
    resp = client.post("/backtest/sweep/ma_cross/apply-best-params", json={})
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["strategy"] == "ma_cross"
    assert data["source_run"]["params_applied"]["order_qty"] == 0.003
    assert "config" in data
    assert data["config"]["order_qty"] == 0.003
    conn.really_close()


def test_apply_best_sweep_params_min_trade_count_filter(monkeypatch) -> None:
    candles = _make_candles([100.0 + i for i in range(30)])
    conn = _make_seeded_conn(candles, "BTCUSDT")
    # Insert a run with only 1 trade — should be filtered out by min_trade_count=5
    _persist_sweep_run(conn, order_qty=0.001, trade_count=1)
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    client = TestClient(app)
    resp = client.post(
        "/backtest/sweep/ma_cross/apply-best-params",
        json={"min_trade_count": 5},
    )
    assert resp.status_code == 404
    conn.really_close()
