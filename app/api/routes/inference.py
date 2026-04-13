from typing import Any

from fastapi import APIRouter, Depends, Query

from app.api.errors import http_not_found

from app.core.db import DBConnection

from app.features.compute import FEATURE_SET_VERSION
from app.inference.service import (
    get_inference_status,
    predict_batch as inference_predict_batch,
    predict_latest as inference_predict_latest,
)
from app.api.deps import get_db

router = APIRouter()

@router.get("/inference/status/{symbol}")
def inference_status_endpoint(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Check whether inference is ready for a symbol.

    Returns ``ready=true`` when a champion model has been promoted AND at
    least one feature vector has been materialised for the symbol.
    """
    return get_inference_status(connection, symbol=symbol, timeframe=timeframe, feature_set=feature_set)

@router.get("/inference/predict/{symbol}")
def inference_predict_endpoint(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    threshold: float = Query(default=0.5, ge=0.0, le=1.0),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return a directional prediction for the latest candle.

    Uses the champion model from the registry and the most recently
    materialised feature vector.  Returns 404 when no champion model or
    no feature vectors are available.
    """
    result = inference_predict_latest(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        threshold=threshold,
    )
    if result is None:
        http_not_found(
            f"Cannot produce a prediction for symbol={symbol!r}: "
            "no champion model or no materialised feature vectors. "
            "Run POST /registry/models/{id}/promote and "
            "POST /features/materialize first."
        )
    return result.to_dict()

@router.get("/inference/batch/{symbol}")
def inference_batch_endpoint(
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = FEATURE_SET_VERSION,
    threshold: float = Query(default=0.5, ge=0.0, le=1.0),
    start_time: int | None = Query(default=None, description="epoch-ms lower bound"),
    end_time: int | None = Query(default=None, description="epoch-ms upper bound"),
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return batch predictions over stored feature vectors.

    Requires a champion model and materialised feature vectors.
    Supports the same pagination and time-range filters as
    GET /features/{symbol}.
    """
    return inference_predict_batch(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        threshold=threshold,
        limit=limit,
        offset=offset,
        start_time=start_time,
        end_time=end_time,
    )
