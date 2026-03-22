"""Tests for the inference service: service layer and API endpoints."""

import math
import sqlite3
from typing import Any, Dict, List, Optional

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.migrations import run_migrations
from app.features.compute import MIN_CANDLES, compute_features_for_candles
from app.features.store import materialize_features
from app.inference.service import (
    PredictionResult,
    get_inference_status,
    predict_batch,
    predict_latest,
)
from app.registry.registry_service import promote_model, register_model
from app.training.dataset import FEATURE_NAMES


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_MS = 1_700_000_000_000
_INTERVAL_MS = 60_000

_DUMMY_MODEL: Dict[str, Any] = {
    "model_type": "logistic_regression_v1",
    "symbol": "BTCUSDT",
    "timeframe": "1m",
    "feature_set": "v1",
    "feature_names": FEATURE_NAMES,
    "weights": [0.0] * len(FEATURE_NAMES),
    "bias": 0.1,  # sigmoid(0.1) ≈ 0.525 → UP
    "n_features": len(FEATURE_NAMES),
    "n_train": 100,
    "n_epochs": 10,
    "learning_rate": 0.01,
    "l2_lambda": 1e-4,
    "final_train_loss": 0.693,
}

_DUMMY_METRICS: Dict[str, Any] = {
    "train": {"accuracy": 0.54, "n": 80},
    "test": {"accuracy": 0.51, "n": 20},
}


def _make_candles(closes: List[float]) -> List[Dict]:
    return [
        {
            "open_time": _BASE_MS + i * _INTERVAL_MS,
            "open": str(c), "high": str(c * 1.001), "low": str(c * 0.999),
            "close": str(c), "volume": "1.0",
            "close_time": _BASE_MS + i * _INTERVAL_MS + _INTERVAL_MS - 1,
        }
        for i, c in enumerate(closes)
    ]


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def _insert_candles(conn: sqlite3.Connection, candles: List[Dict]) -> None:
    for c in candles:
        conn.execute(
            "INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, close_time)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            ("BTCUSDT", "1m", int(c["open_time"]), float(c["open"]),
             float(c["high"]), float(c["low"]), float(c["close"]),
             float(c["volume"]), int(c["close_time"])),
        )
    conn.commit()


class _PersistentConn:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        pass

    def really_close(self):
        self._conn.close()


def _make_api_conn() -> _PersistentConn:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return _PersistentConn(conn)


def _seed_champion(conn) -> int:
    """Register and promote a dummy model. Returns model_id."""
    model_id = register_model(conn, "BTCUSDT", "1m", "v1",
                               model=_DUMMY_MODEL, metrics=_DUMMY_METRICS)
    promote_model(conn, model_id)
    return model_id


def _seed_features(conn, n: int = MIN_CANDLES + 20) -> None:
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)


# ---------------------------------------------------------------------------
# Group 1: PredictionResult
# ---------------------------------------------------------------------------

def test_prediction_result_signal_up():
    r = PredictionResult("BTCUSDT", "1m", "v1", 1000, 100.0,
                         probability=0.7, threshold=0.5,
                         model_id=1, model_version="v1", model_type="lr")
    assert r.signal == "UP"


def test_prediction_result_signal_down():
    r = PredictionResult("BTCUSDT", "1m", "v1", 1000, 100.0,
                         probability=0.3, threshold=0.5,
                         model_id=1, model_version="v1", model_type="lr")
    assert r.signal == "DOWN"


def test_prediction_result_exactly_at_threshold():
    r = PredictionResult("BTCUSDT", "1m", "v1", 1000, 100.0,
                         probability=0.5, threshold=0.5,
                         model_id=1, model_version="v1", model_type="lr")
    assert r.signal == "UP"


def test_prediction_result_to_dict_keys():
    r = PredictionResult("BTCUSDT", "1m", "v1", 1000, 99.5,
                         probability=0.6, threshold=0.5,
                         model_id=3, model_version="abc", model_type="lr")
    d = r.to_dict()
    expected = {
        "symbol", "timeframe", "feature_set", "open_time", "close",
        "probability", "signal", "threshold", "model_id", "model_version", "model_type",
    }
    assert expected == set(d.keys())


def test_prediction_result_probability_rounded():
    r = PredictionResult("BTCUSDT", "1m", "v1", 1000, None,
                         probability=0.123456789, threshold=0.5,
                         model_id=1, model_version="v", model_type="lr")
    assert len(str(r.probability).split(".")[-1]) <= 6


# ---------------------------------------------------------------------------
# Group 2: predict_latest (service layer)
# ---------------------------------------------------------------------------

def test_predict_latest_returns_none_without_champion():
    conn = _make_conn()
    _seed_features(conn)
    result = predict_latest(conn, "BTCUSDT", "1m", "v1")
    assert result is None


def test_predict_latest_returns_none_without_features():
    conn = _make_conn()
    _seed_champion(conn)
    result = predict_latest(conn, "BTCUSDT", "1m", "v1")
    assert result is None


def test_predict_latest_returns_result():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    result = predict_latest(conn, "BTCUSDT", "1m", "v1")
    assert result is not None
    assert isinstance(result, PredictionResult)


def test_predict_latest_signal_is_up_or_down():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    result = predict_latest(conn, "BTCUSDT", "1m", "v1")
    assert result.signal in ("UP", "DOWN")


def test_predict_latest_probability_in_range():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    result = predict_latest(conn, "BTCUSDT", "1m", "v1")
    assert 0.0 <= result.probability <= 1.0


def test_predict_latest_model_id_matches_champion():
    conn = _make_conn()
    model_id = _seed_champion(conn)
    _seed_features(conn)
    result = predict_latest(conn, "BTCUSDT", "1m", "v1")
    assert result.model_id == model_id


def test_predict_latest_custom_threshold_high():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    # threshold=0.99 → almost certainly DOWN
    result = predict_latest(conn, "BTCUSDT", "1m", "v1", threshold=0.99)
    assert result.signal == "DOWN"
    assert result.threshold == 0.99


def test_predict_latest_custom_threshold_low():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    # threshold=0.01 → almost certainly UP
    result = predict_latest(conn, "BTCUSDT", "1m", "v1", threshold=0.01)
    assert result.signal == "UP"


# ---------------------------------------------------------------------------
# Group 3: predict_batch (service layer)
# ---------------------------------------------------------------------------

def test_predict_batch_no_champion():
    conn = _make_conn()
    _seed_features(conn)
    result = predict_batch(conn, "BTCUSDT", "1m", "v1")
    assert result["champion_available"] is False
    assert result["predictions"] == []


def test_predict_batch_returns_predictions():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    result = predict_batch(conn, "BTCUSDT", "1m", "v1")
    assert result["champion_available"] is True
    assert len(result["predictions"]) > 0


def test_predict_batch_all_signals_valid():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    result = predict_batch(conn, "BTCUSDT", "1m", "v1")
    for p in result["predictions"]:
        assert p["signal"] in ("UP", "DOWN")
        assert 0.0 <= p["probability"] <= 1.0


def test_predict_batch_model_metadata_present():
    conn = _make_conn()
    model_id = _seed_champion(conn)
    _seed_features(conn)
    result = predict_batch(conn, "BTCUSDT", "1m", "v1")
    assert result["model"]["id"] == model_id
    assert result["model"]["model_type"] == "logistic_regression_v1"


def test_predict_batch_pagination():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn, n=MIN_CANDLES + 50)
    page1 = predict_batch(conn, "BTCUSDT", "1m", "v1", limit=10, offset=0)
    page2 = predict_batch(conn, "BTCUSDT", "1m", "v1", limit=10, offset=10)
    t1 = {p["open_time"] for p in page1["predictions"]}
    t2 = {p["open_time"] for p in page2["predictions"]}
    assert t1.isdisjoint(t2)


def test_predict_batch_time_filter():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn, n=MIN_CANDLES + 40)
    end_ts = _BASE_MS + 9 * _INTERVAL_MS
    result = predict_batch(conn, "BTCUSDT", "1m", "v1", end_time=end_ts)
    for p in result["predictions"]:
        assert p["open_time"] <= end_ts


# ---------------------------------------------------------------------------
# Group 4: get_inference_status
# ---------------------------------------------------------------------------

def test_inference_status_not_ready_no_champion():
    conn = _make_conn()
    _seed_features(conn)
    status = get_inference_status(conn, "BTCUSDT", "1m", "v1")
    assert status["ready"] is False
    assert status["champion_model_id"] is None


def test_inference_status_not_ready_no_features():
    conn = _make_conn()
    _seed_champion(conn)
    status = get_inference_status(conn, "BTCUSDT", "1m", "v1")
    assert status["ready"] is False
    assert status["champion_model_id"] is not None
    assert status["latest_feature_open_time"] is None


def test_inference_status_ready():
    conn = _make_conn()
    _seed_champion(conn)
    _seed_features(conn)
    status = get_inference_status(conn, "BTCUSDT", "1m", "v1")
    assert status["ready"] is True
    assert status["champion_model_id"] is not None
    assert status["latest_feature_open_time"] is not None


def test_inference_status_fields():
    conn = _make_conn()
    status = get_inference_status(conn, "BTCUSDT", "1m", "v1")
    expected = {
        "symbol", "timeframe", "feature_set", "ready",
        "champion_model_id", "champion_version",
        "champion_promoted_at", "latest_feature_open_time",
    }
    assert expected == set(status.keys())


# ---------------------------------------------------------------------------
# Group 5: API endpoints
# ---------------------------------------------------------------------------

def _inference_client(monkeypatch) -> tuple:
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    return TestClient(app), pconn


def _seed_api_champion(pconn: _PersistentConn) -> int:
    model_id = register_model(pconn, "BTCUSDT", "1m", "v1",
                               model=_DUMMY_MODEL, metrics=_DUMMY_METRICS)
    promote_model(pconn, model_id)
    return model_id


def _seed_api_features(pconn: _PersistentConn, n: int = MIN_CANDLES + 20) -> None:
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    candles = _make_candles(closes)
    _insert_candles(pconn._conn, candles)
    materialize_features(pconn, "BTCUSDT", "1m", candles)


def test_api_status_not_ready(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    resp = client.get("/inference/status/BTCUSDT?timeframe=1m")
    assert resp.status_code == 200
    assert resp.json()["ready"] is False


def test_api_status_ready(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_champion(pconn)
    _seed_api_features(pconn)
    resp = client.get("/inference/status/BTCUSDT?timeframe=1m")
    assert resp.status_code == 200
    assert resp.json()["ready"] is True


def test_api_predict_404_no_champion(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.get("/inference/predict/BTCUSDT?timeframe=1m")
    assert resp.status_code == 404


def test_api_predict_404_no_features(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_champion(pconn)
    resp = client.get("/inference/predict/BTCUSDT?timeframe=1m")
    assert resp.status_code == 404


def test_api_predict_returns_signal(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_champion(pconn)
    _seed_api_features(pconn)
    resp = client.get("/inference/predict/BTCUSDT?timeframe=1m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["signal"] in ("UP", "DOWN")
    assert 0.0 <= data["probability"] <= 1.0
    assert data["model_type"] == "logistic_regression_v1"


def test_api_predict_threshold_param(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_champion(pconn)
    _seed_api_features(pconn)
    resp = client.get("/inference/predict/BTCUSDT?timeframe=1m&threshold=0.99")
    assert resp.status_code == 200
    assert resp.json()["signal"] == "DOWN"
    assert resp.json()["threshold"] == 0.99


def test_api_batch_no_champion_returns_empty(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.get("/inference/batch/BTCUSDT?timeframe=1m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["champion_available"] is False
    assert data["predictions"] == []


def test_api_batch_returns_predictions(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_champion(pconn)
    _seed_api_features(pconn)
    resp = client.get("/inference/batch/BTCUSDT?timeframe=1m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["champion_available"] is True
    assert len(data["predictions"]) > 0
    assert data["model"]["model_type"] == "logistic_regression_v1"


def test_api_batch_limit_param(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_champion(pconn)
    _seed_api_features(pconn, n=MIN_CANDLES + 50)
    resp = client.get("/inference/batch/BTCUSDT?timeframe=1m&limit=5")
    assert resp.status_code == 200
    assert len(resp.json()["predictions"]) == 5


def test_api_batch_predictions_have_required_fields(monkeypatch):
    client, pconn = _inference_client(monkeypatch)
    _seed_api_champion(pconn)
    _seed_api_features(pconn)
    resp = client.get("/inference/batch/BTCUSDT?timeframe=1m&limit=1")
    assert resp.status_code == 200
    pred = resp.json()["predictions"][0]
    expected_keys = {
        "symbol", "timeframe", "feature_set", "open_time", "close",
        "probability", "signal", "threshold", "model_id", "model_version", "model_type",
    }
    assert expected_keys == set(pred.keys())


def test_api_predict_after_champion_change(monkeypatch):
    """Promote a second model and verify predictions now come from it."""
    client, pconn = _inference_client(monkeypatch)
    _seed_api_features(pconn)

    # First champion: bias=0.1 (UP leaning)
    m1 = register_model(pconn, "BTCUSDT", "1m", "v1", model=_DUMMY_MODEL)
    promote_model(pconn, m1)
    r1 = client.get("/inference/predict/BTCUSDT?timeframe=1m").json()

    # Second champion: bias=-5 (DOWN leaning)
    model2 = dict(_DUMMY_MODEL)
    model2 = {**_DUMMY_MODEL, "bias": -5.0}
    m2 = register_model(pconn, "BTCUSDT", "1m", "v1", model=model2)
    promote_model(pconn, m2)
    r2 = client.get("/inference/predict/BTCUSDT?timeframe=1m").json()

    assert r2["model_id"] == m2
    assert r2["signal"] == "DOWN"
    assert r1["model_id"] == m1
