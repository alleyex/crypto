"""Backtest run persistence service."""
import json
import math
from typing import Any, Dict, List, Optional

from app.core.db import DBConnection, fetch_all_as_dicts, insert_and_get_rowid

_VALID_SORT_KEYS = frozenset(
    {"sharpe_ratio", "total_return_pct", "max_drawdown_pct", "win_rate_pct", "profit_factor"}
)


_INSERT_SQL = """
INSERT INTO backtest_runs (
    run_type, symbol, strategy_name, timeframe, days, candle_count,
    trade_count, fill_on, initial_capital, final_equity,
    total_return_pct, max_drawdown_pct, sharpe_ratio, win_rate_pct,
    profit_factor, round_trips, params_json, experiment_name
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
    experiment_name: Optional[str] = None,
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
            experiment_name,
        ),
    )
    connection.commit()
    return run_id


def list_runs(
    connection: DBConnection,
    symbol: Optional[str] = None,
    strategy_name: Optional[str] = None,
    run_type: Optional[str] = None,
    experiment_name: Optional[str] = None,
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
    if experiment_name:
        where_clauses.append("experiment_name = ?")
        params.append(experiment_name)

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


def list_experiments(connection: DBConnection) -> List[str]:
    """Return sorted list of distinct experiment names (excluding NULL)."""
    rows = connection.execute(
        "SELECT DISTINCT experiment_name FROM backtest_runs"
        " WHERE experiment_name IS NOT NULL ORDER BY experiment_name ASC;"
    ).fetchall()
    return [str(r[0]) for r in rows]


def get_run(connection: DBConnection, run_id: int) -> Optional[Dict[str, Any]]:
    """Return a single run by id, or None if not found."""
    rows = fetch_all_as_dicts(
        connection, "SELECT * FROM backtest_runs WHERE id = ?;", (run_id,)
    )
    return rows[0] if rows else None


def update_run(
    connection: DBConnection,
    run_id: int,
    notes: Optional[str] = None,
    tags: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Update mutable fields (notes, tags) on an existing run. Returns updated row or None."""
    if get_run(connection, run_id) is None:
        return None
    if notes is not None:
        connection.execute(
            "UPDATE backtest_runs SET notes = ? WHERE id = ?;", (notes, run_id)
        )
    if tags is not None:
        connection.execute(
            "UPDATE backtest_runs SET tags_json = ? WHERE id = ?;",
            (json.dumps(tags, sort_keys=True), run_id),
        )
    connection.commit()
    return get_run(connection, run_id)


def compare_runs(
    connection: DBConnection, run_ids: List[int]
) -> Dict[str, Any]:
    """Return rows for the given IDs (in requested order) plus per-metric best run id."""
    if not run_ids:
        return {"runs": [], "best": {}}
    placeholders = ",".join("?" * len(run_ids))
    rows = fetch_all_as_dicts(
        connection,
        f"SELECT * FROM backtest_runs WHERE id IN ({placeholders});",
        tuple(run_ids),
    )
    id_to_row = {r["id"]: r for r in rows}
    ordered = [id_to_row[rid] for rid in run_ids if rid in id_to_row]

    best: Dict[str, Any] = {}
    for metric in _VALID_SORT_KEYS:
        ascending = metric == "max_drawdown_pct"
        valid = [(r["id"], r.get(metric)) for r in ordered if r.get(metric) is not None]
        if valid:
            best[metric] = min(valid, key=lambda x: x[1] if ascending else -x[1])[0]
    return {"runs": ordered, "best": best}


def leaderboard_runs(
    connection: DBConnection,
    strategy_name: Optional[str] = None,
    sort_by: str = "sharpe_ratio",
    limit: int = 10,
) -> List[Dict[str, Any]]:
    """Return top runs sorted by a metric column, best first."""
    if sort_by not in _VALID_SORT_KEYS:
        raise ValueError(
            f"sort_by must be one of {sorted(_VALID_SORT_KEYS)}, got {sort_by!r}."
        )
    where_clauses = [f"{sort_by} IS NOT NULL"]
    params: List[Any] = []
    if strategy_name:
        where_clauses.append("strategy_name = ?")
        params.append(strategy_name)

    ascending = sort_by == "max_drawdown_pct"
    order_dir = "ASC" if ascending else "DESC"

    return fetch_all_as_dicts(
        connection,
        f"SELECT * FROM backtest_runs WHERE {' AND '.join(where_clauses)}"
        f" ORDER BY {sort_by} {order_dir} LIMIT ?;",
        tuple(params) + (limit,),
    )


def promote_run(
    connection: DBConnection, run_id: int
) -> Optional[Dict[str, Any]]:
    """Mark run as champion for its strategy. Clears promoted_at from all other runs
    of the same strategy. Returns the updated row or None if run not found."""
    run = get_run(connection, run_id)
    if run is None:
        return None
    connection.execute(
        "UPDATE backtest_runs SET promoted_at = NULL WHERE strategy_name = ? AND id != ?;",
        (run["strategy_name"], run_id),
    )
    connection.execute(
        "UPDATE backtest_runs SET promoted_at = CURRENT_TIMESTAMP WHERE id = ?;",
        (run_id,),
    )
    connection.commit()
    return get_run(connection, run_id)


def get_champion_run(
    connection: DBConnection, strategy_name: str
) -> Optional[Dict[str, Any]]:
    """Return the current champion run for a strategy, or None."""
    rows = fetch_all_as_dicts(
        connection,
        "SELECT * FROM backtest_runs WHERE strategy_name = ? AND promoted_at IS NOT NULL"
        " ORDER BY promoted_at DESC LIMIT 1;",
        (strategy_name,),
    )
    return rows[0] if rows else None


def get_best_sweep_run(
    connection: DBConnection,
    strategy_name: str,
    symbol: Optional[str] = None,
    sort_by: str = "sharpe_ratio",
    min_trade_count: int = 1,
) -> Optional[Dict[str, Any]]:
    """Return the best persisted sweep run for a strategy, or None.

    Raises ValueError if sort_by is not a recognised metric column.
    The returned dict has two extra keys injected:
      - 'params':  parsed params_json dict
      - 'metrics': dict of all metric columns
    """
    if sort_by not in _VALID_SORT_KEYS:
        raise ValueError(
            f"sort_by must be one of {sorted(_VALID_SORT_KEYS)}, got {sort_by!r}."
        )

    clauses: List[str] = ["run_type = ?", "strategy_name = ?"]
    params: List[Any] = ["sweep", strategy_name]
    if symbol:
        clauses.append("symbol = ?")
        params.append(symbol)
    if min_trade_count > 0:
        clauses.append("trade_count >= ?")
        params.append(min_trade_count)

    rows = fetch_all_as_dicts(
        connection,
        "SELECT * FROM backtest_runs WHERE "
        + " AND ".join(clauses)
        + " ORDER BY created_at DESC, id DESC;",
        tuple(params),
    )
    if not rows:
        return None

    ascending = sort_by == "max_drawdown_pct"

    def _sort_key(row: Dict[str, Any]):
        v = row.get(sort_by)
        if v is None:
            return (1, 0.0)
        return (0, float(v) if not ascending else -float(v))

    rows.sort(key=_sort_key)
    best = dict(rows[0])
    raw_params = best.get("params_json")
    best["params"] = json.loads(raw_params) if raw_params else {}
    best["metrics"] = {
        k: best.get(k)
        for k in ("total_return_pct", "max_drawdown_pct", "sharpe_ratio", "win_rate_pct", "profit_factor")
    }
    return best
