"""Tests for training pipeline: dataset, trainer, job_service, and API."""

import math
import sqlite3
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.migrations import run_migrations
from app.features.compute import compute_features_for_candles, MIN_CANDLES
from app.features.store import materialize_features
from app.training.dataset import (
    FEATURE_NAMES,
    build_dataset,
    dataset_summary,
    train_test_split,
)
from app.training.trainer import (
    _sigmoid,
    evaluate,
    model_to_dict,
    predict,
    predict_proba,
    train,
)
from app.training.job_service import (
    create_job,
    get_job,
    list_jobs,
    update_job,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_MS = 1_700_000_000_000
_INTERVAL_MS = 60_000


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


def _seeded_vectors(n: int = MIN_CANDLES + 50) -> List[Dict]:
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    return compute_features_for_candles(_make_candles(closes))


def _small_xy(n: int = 50):
    X = [[float(j) / 10 for j in range(len(FEATURE_NAMES))] for _ in range(n)]
    y = [i % 2 for i in range(n)]
    times = [_BASE_MS + i * _INTERVAL_MS for i in range(n)]
    return X, y, times


# ---------------------------------------------------------------------------
# Group 1: sigmoid
# ---------------------------------------------------------------------------

def test_sigmoid_zero():
    assert _sigmoid(0.0) == pytest.approx(0.5)


def test_sigmoid_large_positive():
    assert _sigmoid(100.0) == pytest.approx(1.0, abs=1e-6)


def test_sigmoid_large_negative():
    assert _sigmoid(-100.0) == pytest.approx(0.0, abs=1e-6)


def test_sigmoid_symmetry():
    assert _sigmoid(2.0) + _sigmoid(-2.0) == pytest.approx(1.0, abs=1e-10)


# ---------------------------------------------------------------------------
# Group 2: dataset.build_dataset
# ---------------------------------------------------------------------------

def test_build_dataset_length():
    vectors = _seeded_vectors(MIN_CANDLES + 50)
    X, y, times = build_dataset(vectors)
    # Should be len(valid) - 1
    assert len(X) == len(y) == len(times)
    assert len(X) > 0


def test_build_dataset_labels_binary():
    vectors = _seeded_vectors()
    _, y, _ = build_dataset(vectors)
    assert all(lbl in (0, 1) for lbl in y)


def test_build_dataset_feature_width():
    vectors = _seeded_vectors()
    X, _, _ = build_dataset(vectors)
    assert all(len(row) == len(FEATURE_NAMES) for row in X)


def test_build_dataset_times_ascending():
    vectors = _seeded_vectors()
    _, _, times = build_dataset(vectors)
    assert times == sorted(times)


def test_build_dataset_empty_vectors():
    X, y, times = build_dataset([])
    assert X == [] and y == [] and times == []


def test_build_dataset_no_close_skipped():
    vectors = [{"open_time": 1000, "close": None}]
    X, y, times = build_dataset(vectors)
    assert X == []


def test_build_dataset_rsi_scaled():
    # rsi_14 should be /100 inside the row
    vectors = _seeded_vectors()
    X, _, _ = build_dataset(vectors)
    rsi_idx = FEATURE_NAMES.index("rsi_14")
    for row in X:
        # rsi was 0-100 range, after /100 should be 0-1
        assert 0.0 <= row[rsi_idx] <= 1.0


def test_dataset_summary_basic():
    y = [1, 1, 0, 1, 0]
    s = dataset_summary(y)
    assert s["n"] == 5
    assert s["n_up"] == 3
    assert s["n_down"] == 2
    assert s["pct_up"] == pytest.approx(0.6)


def test_dataset_summary_empty():
    s = dataset_summary([])
    assert s["n"] == 0
    assert s["pct_up"] is None


# ---------------------------------------------------------------------------
# Group 3: dataset.train_test_split
# ---------------------------------------------------------------------------

def test_train_test_split_sizes():
    X, y, times = _small_xy(100)
    X_tr, y_tr, t_tr, X_te, y_te, t_te = train_test_split(X, y, times, test_ratio=0.2)
    assert len(X_tr) == 80
    assert len(X_te) == 20


def test_train_test_split_no_shuffle():
    X, y, times = _small_xy(50)
    X_tr, y_tr, t_tr, X_te, y_te, t_te = train_test_split(X, y, times, test_ratio=0.2)
    # Train times should come before test times
    assert max(t_tr) < min(t_te)


def test_train_test_split_preserves_all():
    X, y, times = _small_xy(50)
    X_tr, y_tr, t_tr, X_te, y_te, t_te = train_test_split(X, y, times)
    assert len(X_tr) + len(X_te) == 50


# ---------------------------------------------------------------------------
# Group 4: trainer.train / predict / evaluate
# ---------------------------------------------------------------------------

def test_train_returns_weights():
    X, y, _ = _small_xy(60)
    result = train(X, y, n_features=len(FEATURE_NAMES))
    assert "weights" in result
    assert len(result["weights"]) == len(FEATURE_NAMES)
    assert "bias" in result


def test_train_loss_decreases():
    # Separable data: all up rows have positive feature, all down have negative
    n = 100
    X = [[1.0] + [0.0] * (len(FEATURE_NAMES) - 1)] * (n // 2) + \
        [[-1.0] + [0.0] * (len(FEATURE_NAMES) - 1)] * (n // 2)
    y = [1] * (n // 2) + [0] * (n // 2)
    result = train(X, y, n_features=len(FEATURE_NAMES), n_epochs=200, learning_rate=0.1)
    hist = result["train_loss_history"]
    assert hist[0] > hist[-1]


def test_train_empty_raises():
    with pytest.raises(ValueError):
        train([], [], n_features=len(FEATURE_NAMES))


def test_train_length_mismatch_raises():
    X, y, _ = _small_xy(10)
    with pytest.raises(ValueError):
        train(X, y[:-1], n_features=len(FEATURE_NAMES))


def test_predict_proba_range():
    X, y, _ = _small_xy(30)
    result = train(X, y, n_features=len(FEATURE_NAMES))
    probs = predict_proba(result["weights"], result["bias"], X)
    assert all(0.0 <= p <= 1.0 for p in probs)


def test_predict_binary_output():
    X, y, _ = _small_xy(30)
    result = train(X, y, n_features=len(FEATURE_NAMES))
    preds = predict(result["weights"], result["bias"], X)
    assert all(p in (0, 1) for p in preds)


def test_evaluate_perfect_predictions():
    y_true = [1, 0, 1, 1, 0]
    y_pred = [1, 0, 1, 1, 0]
    m = evaluate(y_true, y_pred)
    assert m["accuracy"] == 1.0


def test_evaluate_all_wrong():
    y_true = [1, 0, 1]
    y_pred = [0, 1, 0]
    m = evaluate(y_true, y_pred)
    assert m["accuracy"] == 0.0


def test_evaluate_empty():
    m = evaluate([], [])
    assert m["accuracy"] is None


def test_evaluate_keys():
    m = evaluate([1, 0, 1], [1, 1, 0])
    assert {"accuracy", "precision", "recall", "f1", "tp", "fp", "fn", "tn"}.issubset(m.keys())


def test_model_to_dict_keys():
    X, y, _ = _small_xy(30)
    result = train(X, y, n_features=len(FEATURE_NAMES))
    d = model_to_dict(result, FEATURE_NAMES, "BTCUSDT", "1m", "v1")
    assert d["model_type"] == "logistic_regression_v1"
    assert d["symbol"] == "BTCUSDT"
    assert len(d["weights"]) == len(FEATURE_NAMES)


def test_train_reproducible_with_seed():
    X, y, _ = _small_xy(60)
    r1 = train(X, y, n_features=len(FEATURE_NAMES), seed=99)
    r2 = train(X, y, n_features=len(FEATURE_NAMES), seed=99)
    assert r1["weights"] == r2["weights"]
    assert r1["bias"] == r2["bias"]


# ---------------------------------------------------------------------------
# Group 5: job_service
# ---------------------------------------------------------------------------

def test_create_job_returns_id():
    conn = _make_conn()
    job_id = create_job(conn, "BTCUSDT", "1m", "v1")
    assert isinstance(job_id, int)
    assert job_id > 0


def test_get_job_returns_pending():
    conn = _make_conn()
    job_id = create_job(conn, "BTCUSDT", "1m", "v1", params={"n_epochs": 50})
    job = get_job(conn, job_id)
    assert job is not None
    assert job["status"] == "pending"
    assert job["params"]["n_epochs"] == 50


def test_get_job_returns_none_for_missing():
    conn = _make_conn()
    assert get_job(conn, 9999) is None


def test_update_job_status_to_done():
    conn = _make_conn()
    job_id = create_job(conn, "BTCUSDT", "1m", "v1")
    update_job(conn, job_id, status="done", metrics={"test": {"accuracy": 0.55}})
    job = get_job(conn, job_id)
    assert job["status"] == "done"
    assert job["metrics"]["test"]["accuracy"] == 0.55


def test_update_job_status_to_failed():
    conn = _make_conn()
    job_id = create_job(conn, "BTCUSDT", "1m", "v1")
    update_job(conn, job_id, status="failed", error="something went wrong")
    job = get_job(conn, job_id)
    assert job["status"] == "failed"
    assert "something" in job["error"]


def test_list_jobs_pagination():
    conn = _make_conn()
    for _ in range(5):
        create_job(conn, "BTCUSDT", "1m", "v1")
    result = list_jobs(conn, limit=3, offset=0)
    assert result["total"] == 5
    assert len(result["jobs"]) == 3


def test_list_jobs_filter_by_symbol():
    conn = _make_conn()
    create_job(conn, "BTCUSDT", "1m", "v1")
    create_job(conn, "ETHUSDT", "1m", "v1")
    result = list_jobs(conn, symbol="BTCUSDT")
    assert result["total"] == 1
    assert result["jobs"][0]["symbol"] == "BTCUSDT"


def test_list_jobs_filter_by_status():
    conn = _make_conn()
    job_id = create_job(conn, "BTCUSDT", "1m", "v1")
    update_job(conn, job_id, status="done")
    create_job(conn, "BTCUSDT", "1m", "v1")  # stays pending
    done_result = list_jobs(conn, status="done")
    pending_result = list_jobs(conn, status="pending")
    assert done_result["total"] == 1
    assert pending_result["total"] == 1


def test_job_model_field_parsed():
    conn = _make_conn()
    job_id = create_job(conn, "BTCUSDT", "1m", "v1")
    model_data = {"weights": [0.1, 0.2], "bias": 0.5, "model_type": "logistic_regression_v1"}
    update_job(conn, job_id, status="done", model=model_data)
    job = get_job(conn, job_id)
    assert job["model"]["model_type"] == "logistic_regression_v1"
    assert job["model"]["bias"] == 0.5


# ---------------------------------------------------------------------------
# Group 6: API endpoints
# ---------------------------------------------------------------------------

def _training_client(monkeypatch, closes):
    pconn = _make_api_conn()
    candles = _make_candles(closes)
    _insert_candles(pconn._conn, candles)
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    return TestClient(app), pconn


def test_api_list_training_jobs_empty(monkeypatch):
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    client = TestClient(app)
    resp = client.get("/training/jobs")
    assert resp.status_code == 200
    data = resp.json()
    assert data["total"] == 0
    assert data["jobs"] == []


def test_api_get_training_job_not_found(monkeypatch):
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    client = TestClient(app)
    resp = client.get("/training/jobs/9999")
    assert resp.status_code == 404


def test_api_run_training_job_fails_without_features(monkeypatch):
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    client = TestClient(app)
    resp = client.post("/training/jobs", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    assert resp.status_code == 200
    job = resp.json()
    assert job["status"] == "failed"
    assert "Insufficient" in (job["error"] or "")


def test_api_run_training_job_succeeds(monkeypatch):
    n = MIN_CANDLES + 100
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    client, pconn = _training_client(monkeypatch, closes)

    # Materialise features first
    mat = client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    assert mat.status_code == 200

    resp = client.post(
        "/training/jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_epochs": 10},
    )
    assert resp.status_code == 200
    job = resp.json()
    assert job["status"] == "done"
    assert job["model"] is not None
    assert job["model"]["model_type"] == "logistic_regression_v1"
    assert job["metrics"]["test"]["accuracy"] is not None


def test_api_run_training_job_stored_in_list(monkeypatch):
    n = MIN_CANDLES + 100
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    client, pconn = _training_client(monkeypatch, closes)

    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    client.post("/training/jobs", json={"symbol": "BTCUSDT", "timeframe": "1m", "n_epochs": 5})

    list_resp = client.get("/training/jobs")
    assert list_resp.json()["total"] == 1


def test_api_get_training_job_by_id(monkeypatch):
    n = MIN_CANDLES + 100
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    client, pconn = _training_client(monkeypatch, closes)

    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    resp = client.post("/training/jobs", json={"symbol": "BTCUSDT", "timeframe": "1m", "n_epochs": 5})
    job_id = resp.json()["id"]

    get_resp = client.get(f"/training/jobs/{job_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == job_id


def test_api_training_job_dataset_stats_present(monkeypatch):
    n = MIN_CANDLES + 100
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    client, pconn = _training_client(monkeypatch, closes)

    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    resp = client.post("/training/jobs", json={"symbol": "BTCUSDT", "timeframe": "1m", "n_epochs": 5})
    job = resp.json()
    assert job["dataset"] is not None
    assert job["dataset"]["n_train"] > 0
    assert job["dataset"]["n_test"] > 0
    assert "feature_names" in job["dataset"]


def test_api_training_job_list_filter_by_status(monkeypatch):
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    client = TestClient(app)

    # Run a job that will fail (no features)
    client.post("/training/jobs", json={"symbol": "BTCUSDT", "timeframe": "1m"})

    failed = client.get("/training/jobs?status=failed")
    assert failed.json()["total"] == 1
    pending = client.get("/training/jobs?status=pending")
    assert pending.json()["total"] == 0
