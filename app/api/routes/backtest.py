from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.audit.service import insert_event
from app.backtest.history_service import (
    compare_runs as compare_backtest_runs,
    get_champion_run,
    get_equity_curve as get_backtest_equity_curve,
    get_run as get_backtest_run,
    get_walk_forward_group,
    leaderboard_runs as leaderboard_backtest_runs,
    list_experiments as list_backtest_experiments,
    list_runs as list_backtest_runs,
    list_walk_forward_groups,
    persist_run as persist_backtest_run,
    promote_run as promote_backtest_run,
    update_run as update_backtest_run,
)
from app.backtest.loader import load_candles_from_db
from app.backtest.runner import run_backtest
from app.core.db import DBConnection
from app.core.settings import DEFAULT_ORDER_QTY, DEFAULT_STRATEGY_NAME, MAX_POSITION_QTY
from app.data.symbols import DEFAULT_SYMBOL
from app.strategy.registry import list_registered_strategies
from app.api.errors import http_not_found, http_unprocessable
from app.api.deps import get_db

router = APIRouter()

def _backtest_start_iso(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")

class BacktestRunUpdateRequest(BaseModel):
    notes: str | None = None
    tags: dict[str, Any] | None = None

@router.get("/backtest")
def backtest(
    symbol: str = Query(default=DEFAULT_SYMBOL),
    strategy: str = Query(default=DEFAULT_STRATEGY_NAME),
    days: int = Query(default=30, ge=1, le=365),
    order_qty: float = Query(default=DEFAULT_ORDER_QTY, gt=0),
    max_position_qty: float = Query(default=MAX_POSITION_QTY, gt=0),
    fill_on: str = Query(default="close", pattern="^(close|next_open)$"),
    initial_capital: float = Query(default=10000.0, gt=0),
    experiment_name: str | None = Query(default=None),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Run a backtest over recent candles loaded from the DB."""
    if strategy not in list_registered_strategies():
        return {"error": f"Unknown strategy: {strategy!r}. Available: {list_registered_strategies()}"}
    candles = load_candles_from_db(
        connection,
        symbol=symbol,
        start=_backtest_start_iso(days),
    )
    if not candles:
        return {
            "error": f"No candles found for symbol={symbol!r} in the last {days} days.",
            "symbol": symbol,
            "strategy": strategy,
            "days": days,
        }
    result = run_backtest(
        symbol=symbol,
        strategy_name=strategy,
        candles=candles,
        initial_capital=initial_capital,
        order_qty=order_qty,
        max_position_qty=max_position_qty,
        fill_on=fill_on,
    )
    persist_backtest_run(
        connection, run_type="single", result=result, days=days,
        fill_on=fill_on, experiment_name=experiment_name,
        equity_curve=result.get("equity_curve"),
    )
    return result

@router.get("/backtest/history")
def backtest_history(
    symbol: str | None = Query(default=None),
    strategy: str | None = Query(default=None),
    run_type: str | None = Query(default=None, pattern="^(single|sweep|walk_forward)$"),
    experiment_name: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return paginated history of all persisted backtest runs."""
    return list_backtest_runs(
        connection,
        symbol=symbol,
        strategy_name=strategy,
        run_type=run_type,
        experiment_name=experiment_name,
        limit=limit,
        offset=offset,
    )

@router.get("/backtest/experiments")
def backtest_experiments(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    """Return sorted list of distinct experiment names."""
    return {"experiments": list_backtest_experiments(connection)}

@router.get("/backtest/compare")
def backtest_compare(
    ids: str = Query(description="Comma-separated run IDs, e.g. 1,2,3"),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return multiple runs side-by-side with per-metric best run id."""
    try:
        run_ids = [int(i.strip()) for i in ids.split(",") if i.strip()]
    except ValueError:
        http_unprocessable("ids must be comma-separated integers.")
    if not run_ids:
        http_unprocessable("At least one id is required.")
    return compare_backtest_runs(connection, run_ids)

@router.get("/backtest/leaderboard/{strategy_name}")
def backtest_leaderboard(
    strategy_name: str,
    sort_by: str = Query(default="sharpe_ratio"),
    limit: int = Query(default=10, ge=1, le=100),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return top runs for a strategy sorted by a metric column."""
    try:
        runs = leaderboard_backtest_runs(
            connection,
            strategy_name=strategy_name,
            sort_by=sort_by,
            limit=limit,
        )
    except ValueError as exc:
        http_unprocessable(str(exc))
    return {"strategy_name": strategy_name, "sort_by": sort_by, "runs": runs}

@router.get("/backtest/runs/{run_id}")
def backtest_get_run(
    run_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return a single backtest run by id."""
    run = get_backtest_run(connection, run_id)
    if run is None:
        http_not_found(f"Run {run_id} not found.")
    return run

@router.get("/backtest/runs/{run_id}/equity-curve")
def backtest_run_equity_curve(
    run_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return the stored equity curve for a run."""
    curve = get_backtest_equity_curve(connection, run_id)
    if curve is None:
        http_not_found(f"Run {run_id} not found.")
    return {"run_id": run_id, "equity_curve": curve}

@router.patch("/backtest/runs/{run_id}")
def backtest_update_run(
    run_id: int,
    body: BacktestRunUpdateRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Update mutable fields (notes, tags) on an existing run."""
    updated = update_backtest_run(connection, run_id, notes=body.notes, tags=body.tags)
    if updated is None:
        http_not_found(f"Run {run_id} not found.")
    return updated

@router.post("/backtest/runs/{run_id}/promote")
def backtest_promote_run(
    run_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Mark a run as champion for its strategy."""
    promoted = promote_backtest_run(connection, run_id)
    if promoted is None:
        http_not_found(f"Run {run_id} not found.")
    insert_event(
        connection,
        event_type="param_sync",
        status="ok",
        source="api",
        message=(
            f"Run {run_id} promoted as champion for strategy={promoted['strategy_name']!r}."
        ),
        payload={"run_id": run_id, "strategy_name": promoted["strategy_name"]},
    )
    return {"status": "ok", "run": promoted}

@router.get("/backtest/champion/{strategy_name}")
def backtest_champion(
    strategy_name: str,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return the current champion run for a strategy."""
    run = get_champion_run(connection, strategy_name)
    if run is None:
        http_not_found(f"No champion run found for strategy={strategy_name!r}.")
    return run

@router.get("/backtest/walk-forward/groups")
def backtest_wf_groups(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    """Return summary list of all persisted walk-forward groups, newest first."""
    return {"groups": list_walk_forward_groups(connection)}

@router.get("/backtest/walk-forward/groups/{wf_group_id}")
def backtest_wf_group(
    wf_group_id: str,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return all folds and aggregate stats for a walk-forward group."""
    group = get_walk_forward_group(connection, wf_group_id)
    if group is None:
        http_not_found(f"Walk-forward group {wf_group_id!r} not found.")
    return group

