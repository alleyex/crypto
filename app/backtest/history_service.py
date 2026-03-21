"""Backtest run persistence service."""
import json
import math
from typing import Any, Dict, List, Optional

from app.core.db import DBConnection, fetch_all_as_dicts, insert_and_get_rowid


_INSERT_SQL = """
INSERT INTO backtest_runs (
    run_type, symbol, strategy_name, timeframe, days, candle_count,
    trade_count, fill_on, initial_capital, final_equity,
    total_return_pct, max_drawdown_pct, sharpe_ratio, win_rate_pct,
    profit_factor, round_trips, params_json
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


def _safe_float(value: Any) -> Optional[float]:
    """Coerce to float, returning None for inf/nan (not storable as SQL REAL)."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(f):
        return f
    return None


def persist_run(
    connection: DBConnection,
    run_type: str,
    result: Dict[str, Any],
    days: Optional[int] = None,
    timeframe: str = "1m",
    fill_on: str = "close",
    params: Optional[Dict[str, Any]] = None,
) -> int:
    """Persist a single backtest result row and return its id."""
    metrics = result.get("metrics") or {}
    run_id = insert_and_get_rowid(
        connection,
        _INSERT_SQL,
        (
            run_type,
            result["symbol"],
            result["strategy_name"],
            timeframe,
            days,
            int(result["candle_count"]),
            int(result["trade_count"]),
            fill_on,
            _safe_float(metrics.get("initial_capital")),
            _safe_float(metrics.get("final_equity")),
            _safe_float(metrics.get("total_return_pct")),
            _safe_float(metrics.get("max_drawdown_pct")),
            _safe_float(metrics.get("sharpe_ratio")),
            _safe_float(metrics.get("win_rate_pct")),
            _safe_float(metrics.get("profit_factor")),
            metrics.get("round_trips"),
            json.dumps(params, sort_keys=True) if params is not None else None,
        ),
    )
    connection.commit()
    return run_id


def list_runs(
    connection: DBConnection,
    symbol: Optional[str] = None,
    strategy_name: Optional[str] = None,
    run_type: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    """Return paginated backtest history, newest first."""
    where_clauses: List[str] = []
    params: List[Any] = []

    if symbol:
        where_clauses.append("symbol = ?")
        params.append(symbol)
    if strategy_name:
        where_clauses.append("strategy_name = ?")
        params.append(strategy_name)
    if run_type:
        where_clauses.append("run_type = ?")
        params.append(run_type)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    count_row = connection.execute(
        f"SELECT COUNT(*) FROM backtest_runs {where_sql};", tuple(params)
    ).fetchone()
    total = int(count_row[0]) if count_row else 0

    rows = fetch_all_as_dicts(
        connection,
        f"""
        SELECT * FROM backtest_runs
        {where_sql}
        ORDER BY created_at DESC, id DESC
        LIMIT ? OFFSET ?;
        """,
        tuple(params) + (limit, offset),
    )
    return {"total": total, "limit": limit, "offset": offset, "runs": rows}
