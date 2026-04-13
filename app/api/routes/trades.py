from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.core.db import DBConnection
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.data.symbols import DEFAULT_SYMBOL
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions
from app.query.read_service import (
    get_fills,
    get_orders,
    get_pnl_snapshots,
    get_signals,
    get_strategy_closed_trades,
)
from app.strategy.registry import list_registered_strategies
from app.strategy.signal_service import insert_signal
from app.api.deps import get_db

router = APIRouter()

class TestSignalRequest(BaseModel):
    signal_type: Literal["BUY", "SELL", "HOLD"]
    symbol: str = DEFAULT_SYMBOL
    timeframe: str = "1m"
    strategy_name: str = "manual_test"

@router.get("/signals")
def signals(
    limit: int = Query(default=5, ge=1, le=100),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_signals(connection, limit=limit)

@router.post("/signals/test")
def create_test_signal(
    payload: TestSignalRequest,
    connection: DBConnection = Depends(get_db),
) -> dict:
    return insert_signal(
        connection,
        signal_type=payload.signal_type,
        symbol=payload.symbol,
        timeframe=payload.timeframe,
        strategy_name=payload.strategy_name,
    )

@router.get("/strategies")
def strategies() -> dict[str, Any]:
    return {
        "default_strategy": DEFAULT_STRATEGY_NAME,
        "strategies": list_registered_strategies(),
    }

@router.get("/strategies/closed-trades")
def strategy_closed_trades(
    limit: int = Query(default=20, ge=1, le=200),
    strategy_name: str | None = Query(default=None),
    connection: DBConnection = Depends(get_db),
) -> list[dict[str, Any]]:
    return get_strategy_closed_trades(connection, limit=limit, strategy_name=strategy_name)

@router.get("/orders")
def orders(
    limit: int = Query(default=5, ge=1, le=100),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_orders(connection, limit=limit)

@router.get("/fills")
def fills(
    limit: int = Query(default=5, ge=1, le=100),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_fills(connection, limit=limit)

@router.get("/pnl")
def pnl(
    limit: int = Query(default=5, ge=1, le=100),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_pnl_snapshots(connection, limit=limit)

@router.post("/pnl/update")
def update_pnl(connection: DBConnection = Depends(get_db)) -> dict[str, int]:
    snapshot_count = update_pnl_snapshots(connection)
    return {"snapshot_count": snapshot_count}

@router.post("/positions/rebuild")
def rebuild_positions(connection: DBConnection = Depends(get_db)) -> dict[str, int]:
    updated_symbols = update_positions(connection)
    return {"updated_symbols": updated_symbols}
