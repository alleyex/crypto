"""Parameter sweep (grid search) over backtest parameters."""

import itertools
from typing import Any, Dict, List, Optional

from app.backtest.runner import run_backtest


_SWEEP_PARAMS = frozenset(
    {"order_qty", "max_position_qty", "cooldown_seconds", "max_daily_loss"}
)


def run_parameter_sweep(
    symbol: str,
    strategy_name: str,
    candles: List[Dict],
    param_grid: Dict[str, List[Any]],
    sort_by: str = "sharpe_ratio",
    initial_capital: float = 10000.0,
    timeframe: str = "1m",
    fill_on: str = "close",
) -> List[Dict[str, Any]]:
    """Run run_backtest() for every combination in param_grid.

    Parameters
    ----------
    symbol, strategy_name, candles:
        Passed directly to run_backtest().
    param_grid:
        Dict mapping parameter names to lists of values to try.
        Supported keys: order_qty, max_position_qty, cooldown_seconds,
        max_daily_loss.
        Example: {"order_qty": [0.001, 0.002], "max_position_qty": [0.002, 0.004]}
    sort_by:
        Metric key to sort results by (descending). Use "total_return_pct",
        "sharpe_ratio", "max_drawdown_pct" (ascending for drawdown), etc.
        Defaults to "sharpe_ratio".
    initial_capital, timeframe, fill_on:
        Forwarded to run_backtest() for every combination.

    Returns
    -------
    List of dicts, each containing "params" (the combination tried) and
    "metrics" (the backtest metrics), sorted best-first by sort_by.
    None metrics values are sorted last.
    """
    unknown = set(param_grid) - _SWEEP_PARAMS
    if unknown:
        raise ValueError(f"Unknown sweep parameter(s): {sorted(unknown)}")

    keys = sorted(param_grid)
    value_lists = [param_grid[k] for k in keys]
    combinations = list(itertools.product(*value_lists))

    results: List[Dict[str, Any]] = []
    for combo in combinations:
        params = dict(zip(keys, combo))
        bt = run_backtest(
            symbol=symbol,
            strategy_name=strategy_name,
            candles=candles,
            initial_capital=initial_capital,
            timeframe=timeframe,
            fill_on=fill_on,
            **params,
        )
        results.append({"params": params, "metrics": bt["metrics"], "trade_count": bt["trade_count"]})

    ascending = sort_by == "max_drawdown_pct"
    results.sort(
        key=lambda r: (
            r["metrics"].get(sort_by) is None,
            r["metrics"].get(sort_by, 0.0) if not ascending
            else -(r["metrics"].get(sort_by, 0.0) or 0.0),
        ),
    )
    return results
