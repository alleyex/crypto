"""Walk-forward validation with an expanding training window."""

from typing import Any, Dict, List, Optional

from app.backtest.runner import run_backtest


def run_walk_forward(
    symbol: str,
    strategy_name: str,
    candles: List[Dict],
    n_splits: int = 5,
    initial_capital: float = 10000.0,
    order_qty: float = 0.001,
    max_position_qty: float = 0.002,
    cooldown_seconds: int = 0,
    max_daily_loss: float = 0.0,
    timeframe: str = "1m",
    fill_on: str = "close",
) -> Dict[str, Any]:
    """Run expanding-window walk-forward validation.

    Splits sorted candles into (n_splits + 1) equal chunks.  For fold i
    (0-indexed, i in 0..n_splits-1):

        train = candles[:  (i+1) * chunk_size ]
        test  = candles[ (i+1)*chunk_size : (i+2)*chunk_size ]

    The first chunk is always used only as an initial training period;
    testing starts from the second chunk onward.

    Parameters
    ----------
    n_splits:
        Number of test folds.  Total candles are divided into n_splits+1
        chunks.  Must be >= 1.

    Returns
    -------
    Dict with keys:
      - splits: list of per-fold result dicts (train_metrics, test_metrics,
                train_candle_count, test_candle_count, fold)
      - oos_metrics: aggregated out-of-sample statistics across all folds
                     (mean/std of total_return_pct, sharpe_ratio,
                      max_drawdown_pct)
      - candle_count: total candles used
      - n_splits: number of folds completed
    """
    if n_splits < 1:
        raise ValueError("n_splits must be >= 1")

    sorted_candles = sorted(candles, key=lambda c: int(c["open_time"]))
    total = len(sorted_candles)
    chunk_size = total // (n_splits + 1)

    if chunk_size < 1:
        raise ValueError(
            f"Too few candles ({total}) for n_splits={n_splits}. "
            f"Need at least {n_splits + 1} candles."
        )

    common_kwargs = dict(
        symbol=symbol,
        strategy_name=strategy_name,
        initial_capital=initial_capital,
        order_qty=order_qty,
        max_position_qty=max_position_qty,
        cooldown_seconds=cooldown_seconds,
        max_daily_loss=max_daily_loss,
        timeframe=timeframe,
        fill_on=fill_on,
    )

    splits = []
    for i in range(n_splits):
        train_end = (i + 1) * chunk_size
        test_end = min((i + 2) * chunk_size, total)
        train_candles = sorted_candles[:train_end]
        test_candles = sorted_candles[train_end:test_end]

        if not test_candles:
            break

        train_result = run_backtest(candles=train_candles, **common_kwargs)
        test_result = run_backtest(candles=test_candles, **common_kwargs)

        splits.append({
            "fold": i,
            "train_candle_count": len(train_candles),
            "test_candle_count": len(test_candles),
            "train_metrics": train_result["metrics"],
            "test_metrics": test_result["metrics"],
            "train_trade_count": train_result["trade_count"],
            "test_trade_count": test_result["trade_count"],
            "test_equity_curve": test_result.get("equity_curve", []),
        })

    oos_metrics = _aggregate_oos(splits)
    return {
        "symbol": symbol,
        "strategy_name": strategy_name,
        "candle_count": total,
        "n_splits": len(splits),
        "splits": splits,
        "oos_metrics": oos_metrics,
    }


def _aggregate_oos(splits: List[Dict]) -> Dict[str, Any]:
    """Compute mean and std of key metrics across out-of-sample test folds."""
    keys = ("total_return_pct", "sharpe_ratio", "max_drawdown_pct", "win_rate_pct")
    result: Dict[str, Any] = {}

    for key in keys:
        values = [
            s["test_metrics"][key]
            for s in splits
            if s["test_metrics"].get(key) is not None
        ]
        if not values:
            result[f"{key}_mean"] = None
            result[f"{key}_std"] = None
            continue
        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)
        import math
        result[f"{key}_mean"] = round(mean, 4)
        result[f"{key}_std"] = round(math.sqrt(variance), 4)

    return result
