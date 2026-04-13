from typing import Any

from fastapi import APIRouter, Depends, Query

from app.core.db import DBConnection
from app.data.fetch_history import read_fetch_history
from app.validation.candles_quality import run_candles_quality_check
from app.validation.soak_history import build_soak_history_summary
from app.validation.soak_history import read_soak_validation_history
from app.validation.soak_history import record_soak_validation_snapshot
from app.validation.soak_report import build_soak_validation_report
from app.api.deps import get_db

router = APIRouter()

@router.get("/validation/soak")
def soak_validation() -> dict[str, Any]:
    return build_soak_validation_report()

@router.post("/validation/soak/record")
def record_soak_validation() -> dict[str, Any]:
    return record_soak_validation_snapshot()

@router.get("/validation/soak/history")
def soak_validation_history(limit: int = Query(default=20, ge=1, le=200)) -> list[dict[str, Any]]:
    return read_soak_validation_history(limit=limit)

@router.get("/validation/soak/history/summary")
def soak_validation_history_summary() -> dict[str, Any]:
    return build_soak_history_summary()

@router.get("/market-data/fetch/history")
def market_data_fetch_history(limit: int = Query(default=20, ge=1, le=200)) -> list:
    return read_fetch_history(limit=limit)

@router.get("/validation/candles/quality")
def candles_quality(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    return run_candles_quality_check(connection)
