"""Backtest sweep and walk-forward API routes."""

import uuid as _uuid
from typing import Any

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field

from app.api.deps import get_db
from app.api.errors import http_not_found, http_unprocessable
from app.audit.service import insert_event
from app.backtest.history_service import (
    get_best_sweep_run,
    persist_run as persist_backtest_run,
)
from app.backtest.loader import load_candles_from_db
from app.backtest.sweep import run_parameter_sweep
from app.backtest.walk_forward import run_walk_forward
from app.core.db import DBConnection
from app.core.settings import DEFAULT_ORDER_QTY, DEFAULT_STRATEGY_NAME, MAX_POSITION_QTY
from app.data.symbols import DEFAULT_SYMBOL
from app.risk.risk_config import set_risk_config
from app.api.routes.backtest import _backtest_start_iso
from app.strategy.registry import list_registered_strategies

router = APIRouter()


class BacktestSweepRequest(BaseModel):
    symbol: str = DEFAULT_SYMBOL
    strategy: str = DEFAULT_STRATEGY_NAME
    days: int = 30
    param_grid: dict[str, list[float]] = {}
    sort_by: str = "sharpe_ratio"
    fill_on: str = "close"
    initial_capital: float = 10000.0
    experiment_name: str | None = None


class ApplyBestSweepParamsRequest(BaseModel):
    symbol: str | None = None
    sort_by: str = "sharpe_ratio"
    min_trade_count: int = Field(default=1, ge=0)


class BacktestWalkForwardRequest(BaseModel):
    symbol: str = DEFAULT_SYMBOL
    strategy: str = DEFAULT_STRATEGY_NAME
    days: int = 30
    n_splits: int = 5
    order_qty: float = DEFAULT_ORDER_QTY
    max_position_qty: float = MAX_POSITION_QTY
    fill_on: str = "close"
    initial_capital: float = 10000.0
    experiment_name: str | None = None


@router.post("/backtest/sweep")
def backtest_sweep(
    req: BacktestSweepRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Run a parameter grid search over recent candles."""
    if req.strategy not in list_registered_strategies():
        return {"error": f"Unknown strategy: {req.strategy!r}. Available: {list_registered_strategies()}"}
    if not req.param_grid:
        return {"error": "param_grid must not be empty."}
    candles = load_candles_from_db(
        connection,
        symbol=req.symbol,
        start=_backtest_start_iso(req.days),
    )
    if not candles:
        return {"error": f"No candles found for symbol={req.symbol!r} in the last {req.days} days."}
    try:
        results = run_parameter_sweep(
            symbol=req.symbol,
            strategy_name=req.strategy,
            candles=candles,
            param_grid={k: list(v) for k, v in req.param_grid.items()},
            sort_by=req.sort_by,
            initial_capital=req.initial_capital,
            fill_on=req.fill_on,
        )
    except ValueError as exc:
        return {"error": str(exc)}
    for combo in results:
        persist_backtest_run(
            connection,
            run_type="sweep",
            result={
                "symbol": req.symbol,
                "strategy_name": req.strategy,
                "candle_count": len(candles),
                "trade_count": combo.get("trade_count", 0),
                "metrics": combo.get("metrics", {}),
            },
            days=req.days,
            fill_on=req.fill_on,
            params=combo.get("params"),
            experiment_name=req.experiment_name,
        )
    return {
        "symbol": req.symbol,
        "strategy": req.strategy,
        "days": req.days,
        "candle_count": len(candles),
        "combination_count": len(results),
        "sort_by": req.sort_by,
        "results": results,
    }


@router.post("/backtest/walk-forward")
def backtest_walk_forward(
    req: BacktestWalkForwardRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Run expanding-window walk-forward validation over recent candles."""
    if req.strategy not in list_registered_strategies():
        return {"error": f"Unknown strategy: {req.strategy!r}. Available: {list_registered_strategies()}"}
    candles = load_candles_from_db(
        connection,
        symbol=req.symbol,
        start=_backtest_start_iso(req.days),
    )
    if not candles:
        return {"error": f"No candles found for symbol={req.symbol!r} in the last {req.days} days."}
    try:
        wf_result = run_walk_forward(
            symbol=req.symbol,
            strategy_name=req.strategy,
            candles=candles,
            n_splits=req.n_splits,
            initial_capital=req.initial_capital,
            order_qty=req.order_qty,
            max_position_qty=req.max_position_qty,
            fill_on=req.fill_on,
        )
    except ValueError as exc:
        return {"error": str(exc)}
    wf_group_id = str(_uuid.uuid4())
    for split in wf_result.get("splits", []):
        persist_backtest_run(
            connection,
            run_type="walk_forward",
            result={
                "symbol": req.symbol,
                "strategy_name": req.strategy,
                "candle_count": split.get("test_candle_count", 0),
                "trade_count": split.get("test_trade_count", 0),
                "metrics": split.get("test_metrics", {}),
            },
            days=req.days,
            fill_on=req.fill_on,
            params={
                "fold": split.get("fold"),
                "train_candle_count": split.get("train_candle_count"),
                "test_candle_count": split.get("test_candle_count"),
            },
            experiment_name=req.experiment_name,
            wf_group_id=wf_group_id,
            fold_index=split.get("fold"),
            equity_curve=split.get("test_equity_curve"),
        )
    wf_result["wf_group_id"] = wf_group_id
    return wf_result


@router.post("/backtest/sweep/{strategy}/apply-best-params")
def apply_best_sweep_params(
    strategy: str,
    body: ApplyBestSweepParamsRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Apply the best persisted sweep run's params to risk_configs for *strategy*."""
    if strategy not in list_registered_strategies():
        http_not_found(f"Unknown strategy: {strategy!r}. Available: {list_registered_strategies()}")
    try:
        best = get_best_sweep_run(
            connection,
            strategy_name=strategy,
            symbol=body.symbol,
            sort_by=body.sort_by,
            min_trade_count=body.min_trade_count,
        )
    except ValueError as exc:
        http_unprocessable(str(exc))
    if best is None:
        http_not_found(
            f"No sweep runs found for strategy={strategy!r}"
            + (f", symbol={body.symbol!r}" if body.symbol else "")
            + f" with trade_count >= {body.min_trade_count}."
        )
    params = best["params"]
    cfg = set_risk_config(
        connection,
        strategy,
        order_qty=float(params["order_qty"]) if "order_qty" in params else None,
        max_position_qty=float(params["max_position_qty"]) if "max_position_qty" in params else None,
        cooldown_seconds=int(params["cooldown_seconds"]) if "cooldown_seconds" in params else None,
        max_daily_loss=float(params["max_daily_loss"]) if "max_daily_loss" in params else None,
    )
    result = {
        "status": "ok",
        "strategy": strategy,
        "source_run": {
            "id": best["id"],
            "symbol": best["symbol"],
            "created_at": best["created_at"],
            "sort_by": body.sort_by,
            "sort_value": best["metrics"].get(body.sort_by),
            "trade_count": best["trade_count"],
            "params_applied": params,
        },
        "config": cfg.to_dict(),
    }
    insert_event(
        connection,
        event_type="param_sync",
        status="ok",
        source="api",
        message=(
            f"Best sweep params applied to strategy={strategy!r} "
            f"from run_id={best['id']} (sort_by={body.sort_by}, "
            f"value={best['metrics'].get(body.sort_by)})."
        ),
        payload={
            "strategy": strategy,
            "source_run_id": best["id"],
            "sort_by": body.sort_by,
            "sort_value": best["metrics"].get(body.sort_by),
            "params_applied": params,
        },
    )
    return result
