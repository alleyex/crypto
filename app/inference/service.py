"""Inference service — serve champion model predictions.

Design
------
- Inference is always served from the *champion* model in the registry.
- Feature computation uses the feature store when vectors are already
  materialised; falls back to on-the-fly computation from raw candles
  if the store is empty.
- No model is cached in memory: every call reads the champion row from DB
  (connection is cheap in SQLite; this can be wrapped in a cache later).

Public API
----------
predict_latest(connection, symbol, timeframe, feature_set)
    → PredictionResult | None

predict_batch(connection, symbol, timeframe, feature_set, vectors)
    → List[PredictionResult]

get_inference_status(connection, symbol, timeframe, feature_set)
    → Dict with champion metadata + readiness flag
"""

from typing import Any, Dict, List, Optional

from app.registry.registry_service import get_champion
from app.features.store import get_features, get_latest_feature_vector
from app.training.dataset import FEATURE_NAMES, _safe_float
from app.training.trainer import predict_proba


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

class PredictionResult:
    __slots__ = (
        "symbol", "timeframe", "feature_set",
        "open_time", "close",
        "probability", "signal", "threshold",
        "model_id", "model_version", "model_type",
    )

    def __init__(
        self,
        symbol: str,
        timeframe: str,
        feature_set: str,
        open_time: Optional[int],
        close: Optional[float],
        probability: float,
        threshold: float,
        model_id: int,
        model_version: str,
        model_type: str,
    ) -> None:
        self.symbol = symbol
        self.timeframe = timeframe
        self.feature_set = feature_set
        self.open_time = open_time
        self.close = close
        self.probability = round(probability, 6)
        self.signal = "UP" if probability >= threshold else "DOWN"
        self.threshold = threshold
        self.model_id = model_id
        self.model_version = model_version
        self.model_type = model_type

    def to_dict(self) -> Dict[str, Any]:
        return {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "feature_set": self.feature_set,
            "open_time": self.open_time,
            "close": self.close,
            "probability": self.probability,
            "signal": self.signal,
            "threshold": self.threshold,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "model_type": self.model_type,
        }


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

def _extract_feature_row(fv: Dict[str, Any]) -> List[float]:
    """Extract ordered feature values from a feature-vector dict."""
    row = []
    for name in FEATURE_NAMES:
        val = _safe_float(fv.get(name), 0.0)
        if name == "rsi_14":
            val = val / 100.0
        row.append(val)
    return row


# ---------------------------------------------------------------------------
# Core inference helpers
# ---------------------------------------------------------------------------

def _run_prediction(
    fv: Dict[str, Any],
    weights: List[float],
    bias: float,
    symbol: str,
    timeframe: str,
    feature_set: str,
    threshold: float,
    model_id: int,
    model_version: str,
    model_type: str,
) -> PredictionResult:
    row = _extract_feature_row(fv)
    proba = predict_proba(weights, bias, [row])[0]
    return PredictionResult(
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        open_time=fv.get("open_time"),
        close=fv.get("close"),
        probability=proba,
        threshold=threshold,
        model_id=model_id,
        model_version=model_version,
        model_type=model_type,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def predict_latest(
    connection: Any,
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = "v1",
    threshold: float = 0.5,
) -> Optional[PredictionResult]:
    """Return a prediction for the most recent stored feature vector.

    Returns None if no champion model or no feature vectors are available.
    """
    champion = get_champion(connection, symbol, timeframe, feature_set)
    if champion is None:
        return None

    fv = get_latest_feature_vector(connection, symbol, timeframe, feature_set)
    if fv is None:
        return None

    model = champion["model"]
    return _run_prediction(
        fv=fv,
        weights=model["weights"],
        bias=model["bias"],
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        threshold=threshold,
        model_id=champion["id"],
        model_version=champion["version"],
        model_type=model.get("model_type", "unknown"),
    )


def predict_batch(
    connection: Any,
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = "v1",
    threshold: float = 0.5,
    limit: int = 500,
    offset: int = 0,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
) -> Dict[str, Any]:
    """Return predictions for stored feature vectors using the champion model.

    Parameters mirror ``get_features()`` pagination/filtering.
    Returns a dict with ``total``, ``predictions`` list, and ``model`` metadata.
    """
    champion = get_champion(connection, symbol, timeframe, feature_set)
    if champion is None:
        return {
            "symbol": symbol, "timeframe": timeframe, "feature_set": feature_set,
            "champion_available": False,
            "total": 0, "predictions": [], "model": None,
        }

    fv_result = get_features(
        connection,
        symbol=symbol,
        timeframe=timeframe,
        feature_set=feature_set,
        start_time=start_time,
        end_time=end_time,
        limit=limit,
        offset=offset,
        ascending=True,
    )

    model = champion["model"]
    weights = model["weights"]
    bias = model["bias"]
    model_id = champion["id"]
    model_version = champion["version"]
    model_type = model.get("model_type", "unknown")

    predictions = [
        _run_prediction(
            fv=fv,
            weights=weights,
            bias=bias,
            symbol=symbol,
            timeframe=timeframe,
            feature_set=feature_set,
            threshold=threshold,
            model_id=model_id,
            model_version=model_version,
            model_type=model_type,
        ).to_dict()
        for fv in fv_result["vectors"]
    ]

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "feature_set": feature_set,
        "champion_available": True,
        "total": fv_result["total"],
        "limit": fv_result["limit"],
        "offset": fv_result["offset"],
        "predictions": predictions,
        "model": {
            "id": model_id,
            "version": model_version,
            "model_type": model_type,
            "promoted_at": champion.get("promoted_at"),
        },
    }


def get_inference_status(
    connection: Any,
    symbol: str,
    timeframe: str = "1m",
    feature_set: str = "v1",
) -> Dict[str, Any]:
    """Return readiness status for inference on a symbol/timeframe."""
    champion = get_champion(connection, symbol, timeframe, feature_set)
    fv = get_latest_feature_vector(connection, symbol, timeframe, feature_set) if champion else None

    ready = champion is not None and fv is not None
    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "feature_set": feature_set,
        "ready": ready,
        "champion_model_id": champion["id"] if champion else None,
        "champion_version": champion["version"] if champion else None,
        "champion_promoted_at": champion.get("promoted_at") if champion else None,
        "latest_feature_open_time": fv.get("open_time") if fv else None,
    }
