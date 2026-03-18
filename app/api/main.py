from typing import Literal

from fastapi import FastAPI, Query
from pydantic import BaseModel

from app.core.db import DB_FILE, get_connection
from app.pipeline.run_pipeline import run_pipeline_collect
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_candles
from app.query.read_service import get_fills
from app.query.read_service import get_orders
from app.query.read_service import get_pnl_snapshots
from app.query.read_service import get_positions
from app.query.read_service import get_risk_events
from app.query.read_service import get_signals
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.ma_cross import insert_signal


app = FastAPI(title="Crypto Trading MVP API")


class TestSignalRequest(BaseModel):
    signal_type: Literal["BUY", "SELL", "HOLD"]
    symbol: str = "BTCUSDT"
    timeframe: str = "1m"
    strategy_name: str = "manual_test"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "database": str(DB_FILE)}


@app.get("/candles")
def candles(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_candles(connection, limit=limit)
    finally:
        connection.close()


@app.get("/signals")
def signals(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_signals(connection, limit=limit)
    finally:
        connection.close()


@app.get("/risk-events")
def risk_events(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_risk_events(connection, limit=limit)
    finally:
        connection.close()


@app.get("/orders")
def orders(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_orders(connection, limit=limit)
    finally:
        connection.close()


@app.get("/fills")
def fills(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_fills(connection, limit=limit)
    finally:
        connection.close()


@app.get("/positions")
def positions(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_positions(connection, limit=limit)
    finally:
        connection.close()


@app.get("/pnl")
def pnl(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_pnl_snapshots(connection, limit=limit)
    finally:
        connection.close()


@app.post("/pipeline/run")
def run_pipeline_endpoint() -> dict:
    return run_pipeline_collect()


@app.post("/signals/test")
def create_test_signal(payload: TestSignalRequest) -> dict:
    connection = get_connection()
    try:
        ensure_signals_table(connection)
        return insert_signal(
            connection,
            signal_type=payload.signal_type,
            symbol=payload.symbol,
            timeframe=payload.timeframe,
            strategy_name=payload.strategy_name,
        )
    finally:
        connection.close()


@app.post("/positions/rebuild")
def rebuild_positions() -> dict[str, int]:
    connection = get_connection()
    try:
        ensure_positions_table(connection)
        updated_symbols = update_positions(connection)
        return {"updated_symbols": updated_symbols}
    finally:
        connection.close()


@app.post("/pnl/update")
def update_pnl() -> dict[str, int]:
    connection = get_connection()
    try:
        ensure_pnl_table(connection)
        snapshot_count = update_pnl_snapshots(connection)
        return {"snapshot_count": snapshot_count}
    finally:
        connection.close()
