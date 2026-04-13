from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.api.errors import http_not_found
from pydantic import BaseModel

from app.backtest.loader import load_candles_from_db
from app.core.db import DBConnection

from app.features.compute import compute_features_for_candles, FEATURE_SET_VERSION
from app.features.store import (
    delete_features,
    get_features as get_feature_vectors,
    get_latest_feature_vector,
    materialize_features,
)
from app.api.deps import get_db

router = APIRouter()

def _start_iso(days: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")

class MaterializeFeaturesRequest(BaseModel):
    symbol: str
    timeframe: str = "1m"
    days: int | None = None  # if set, only use candles from the last N days

@router.post("/features/materialize")
def materialize_features_endpoint(
    body: MaterializeFeaturesRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Compute and persist v1 feature vectors from stored candles.

    Loads candles for symbol/timeframe (optionally filtered to the last ``days``
    days), computes the full feature set, and upserts every row into the
    ``feature_vectors`` table.  Safe to call repeatedly — idempotent.
    """
    start_iso = _start_iso(body.days) if body.days else None
    candles = load_candles_from_db(
        connection,
        symbol=body.symbol,
        timeframe=body.timeframe,
        start=start_iso,
    )
    if not candles:
        return {
            "status": "ok",
            "symbol": body.symbol,
            "timeframe": body.timeframe,
            "candle_count": 0,
            "vectors_upserted": 0,
            "feature_set": FEATURE_SET_VERSION,
        }
    count = materialize_features(
        connection,
        symbol=body.symbol,
        timeframe=body.timeframe,
        candles=candles,
    )
    return {
        "status": "ok",
        "symbol": body.symbol,
        "timeframe": body.timeframe,
        "candle_count": len(candles),
        "vectors_upserted": count,
        "feature_set": FEATURE_SET_VERSION,
    }

@router.get("/features/compute")
def compute_features_endpoint(
    symbol: str,
    timeframe: str = "1m",
    days: int | None = Query(default=None, ge=1),
    limit: int = Query(default=100, ge=1, le=5000),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Compute features on-the-fly from stored candles without persisting.

    Returns the last ``limit`` feature vectors (newest last).
    Use ``days`` to restrict the candle window.
    """
    start_iso = _start_iso(days) if days else None
    candles = load_candles_from_db(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        start=start_iso,
    )
    if not candles:
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "candle_count": 0,
            "feature_set": FEATURE_SET_VERSION,
            "vectors": [],
        }
    vectors = compute_features_for_candles(candles)
    trimmed = vectors[-limit:]
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "candle_count": len(candles),
        "feature_set": FEATURE_SET_VERSION,
        "vectors": trimmed,
    }

@router.get("/features/{symbol}/latest")
def get_latest_stored_feature(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return the most recent persisted feature vector for a symbol/timeframe.

    Returns 404 if no features have been materialized yet.
    """
    fv = get_latest_feature_vector(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
    )
    if fv is None:
        http_not_found(f"No stored features for symbol={symbol!r} timeframe={timeframe!r}.")
    return fv

@router.get("/features/{symbol}")
def get_stored_features(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    start_time: int | None = Query(default=None, description="epoch-ms lower bound"),
    end_time: int | None = Query(default=None, description="epoch-ms upper bound"),
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    ascending: bool = True,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return paginated persisted feature vectors for a symbol/timeframe."""
    return get_feature_vectors(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
        ascending=ascending,
    )

@router.delete("/features/{symbol}")
def delete_stored_features(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Delete all stored feature vectors for a symbol/timeframe/feature_set."""
    deleted = delete_features(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
    )
    return {
        "status": "ok",
        "symbol": symbol,
        "timeframe": timeframe,
        "feature_set": feature_set,
        "deleted": deleted,
    }
