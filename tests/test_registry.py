"""Tests for the model registry: service layer and API endpoints."""

import math
import sqlite3
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.migrations import run_migrations
from app.features.compute import MIN_CANDLES
from app.registry.registry_service import (
    VALID_STATUSES,
    archive_model,
    get_champion,
    get_model,
    list_models,
    list_versions,
    promote_model,
    register_model,
    update_notes,
)


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
    "feature_names": ["returns_1", "returns_5"],
    "weights": [0.1, -0.2],
    "bias": 0.05,
    "n_features": 2,
    "n_train": 100,
    "n_epochs": 10,
    "learning_rate": 0.01,
    "l2_lambda": 1e-4,
    "final_train_loss": 0.693,
}

_DUMMY_METRICS: Dict[str, Any] = {
    "train": {"accuracy": 0.54, "precision": 0.52, "recall": 0.60, "f1": 0.56, "n": 80},
    "test": {"accuracy": 0.51, "precision": 0.50, "recall": 0.55, "f1": 0.52, "n": 20},
    "final_train_loss": 0.693,
}


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def _register(conn, symbol="BTCUSDT", timeframe="1m", feature_set="v1",
               job_id=None, notes=None) -> int:
    return register_model(conn, symbol, timeframe, feature_set,
                          model=_DUMMY_MODEL, training_job_id=job_id,
                          metrics=_DUMMY_METRICS, notes=notes)


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


# ---------------------------------------------------------------------------
# Group 1: register_model
# ---------------------------------------------------------------------------

def test_register_returns_id():
    conn = _make_conn()
    model_id = _register(conn)
    assert isinstance(model_id, int) and model_id > 0


def test_register_status_is_candidate():
    conn = _make_conn()
    model_id = _register(conn)
    model = get_model(conn, model_id)
    assert model["status"] == "candidate"


def test_register_model_fields():
    conn = _make_conn()
    model_id = _register(conn, notes="test run")
    model = get_model(conn, model_id)
    assert model["symbol"] == "BTCUSDT"
    assert model["timeframe"] == "1m"
    assert model["notes"] == "test run"
    assert model["model"]["model_type"] == "logistic_regression_v1"
    assert model["metrics"]["test"]["accuracy"] == 0.51


def test_register_version_generated():
    conn = _make_conn()
    model_id = _register(conn)
    model = get_model(conn, model_id)
    assert model["version"] is not None
    assert len(model["version"]) > 0


def test_register_multiple_distinct_versions():
    conn = _make_conn()
    id1 = _register(conn)
    id2 = _register(conn)
    m1 = get_model(conn, id1)
    m2 = get_model(conn, id2)
    assert m1["version"] != m2["version"]


def test_get_model_not_found():
    conn = _make_conn()
    assert get_model(conn, 9999) is None


# ---------------------------------------------------------------------------
# Group 2: promote_model
# ---------------------------------------------------------------------------

def test_promote_sets_status_champion():
    conn = _make_conn()
    model_id = _register(conn)
    result = promote_model(conn, model_id)
    assert result["status"] == "champion"


def test_promote_sets_promoted_at():
    conn = _make_conn()
    model_id = _register(conn)
    result = promote_model(conn, model_id)
    assert result["promoted_at"] is not None


def test_promote_archives_previous_champion():
    conn = _make_conn()
    id1 = _register(conn)
    id2 = _register(conn)
    promote_model(conn, id1)
    promote_model(conn, id2)

    m1 = get_model(conn, id1)
    m2 = get_model(conn, id2)
    assert m1["status"] == "archived"
    assert m2["status"] == "champion"


def test_promote_only_one_champion_per_key():
    conn = _make_conn()
    ids = [_register(conn) for _ in range(4)]
    for mid in ids:
        promote_model(conn, mid)

    champions = [get_model(conn, mid) for mid in ids if get_model(conn, mid)["status"] == "champion"]
    assert len(champions) == 1
    assert champions[0]["id"] == ids[-1]


def test_promote_cross_symbol_independent():
    conn = _make_conn()
    btc_id = register_model(conn, "BTCUSDT", "1m", "v1", model=_DUMMY_MODEL)
    eth_id = register_model(conn, "ETHUSDT", "1m", "v1", model=_DUMMY_MODEL)
    promote_model(conn, btc_id)
    promote_model(conn, eth_id)

    btc = get_model(conn, btc_id)
    eth = get_model(conn, eth_id)
    assert btc["status"] == "champion"
    assert eth["status"] == "champion"


def test_promote_not_found_returns_none():
    conn = _make_conn()
    assert promote_model(conn, 9999) is None


# ---------------------------------------------------------------------------
# Group 3: rollback
# ---------------------------------------------------------------------------

def test_rollback_by_promoting_archived():
    conn = _make_conn()
    id_old = _register(conn)
    id_new = _register(conn)
    promote_model(conn, id_old)  # old is champion
    promote_model(conn, id_new)  # new becomes champion, old archived

    # Rollback: promote old again
    rollback = promote_model(conn, id_old)
    assert rollback["status"] == "champion"
    assert get_model(conn, id_new)["status"] == "archived"


# ---------------------------------------------------------------------------
# Group 4: archive_model
# ---------------------------------------------------------------------------

def test_archive_sets_status():
    conn = _make_conn()
    model_id = _register(conn)
    result = archive_model(conn, model_id)
    assert result["status"] == "archived"


def test_archive_champion_leaves_no_champion():
    conn = _make_conn()
    model_id = _register(conn)
    promote_model(conn, model_id)
    archive_model(conn, model_id)

    champion = get_champion(conn, "BTCUSDT", "1m", "v1")
    assert champion is None


def test_archive_not_found_returns_none():
    conn = _make_conn()
    assert archive_model(conn, 9999) is None


# ---------------------------------------------------------------------------
# Group 5: get_champion
# ---------------------------------------------------------------------------

def test_get_champion_returns_none_when_no_champion():
    conn = _make_conn()
    assert get_champion(conn, "BTCUSDT", "1m", "v1") is None


def test_get_champion_returns_promoted_model():
    conn = _make_conn()
    model_id = _register(conn)
    promote_model(conn, model_id)
    champion = get_champion(conn, "BTCUSDT", "1m", "v1")
    assert champion is not None
    assert champion["id"] == model_id
    assert champion["status"] == "champion"


# ---------------------------------------------------------------------------
# Group 6: list_models / list_versions
# ---------------------------------------------------------------------------

def test_list_models_total():
    conn = _make_conn()
    _register(conn)
    _register(conn)
    result = list_models(conn)
    assert result["total"] == 2


def test_list_models_filter_by_status():
    conn = _make_conn()
    id1 = _register(conn)
    _register(conn)
    promote_model(conn, id1)
    result = list_models(conn, status="champion")
    assert result["total"] == 1
    assert result["models"][0]["status"] == "champion"


def test_list_models_filter_by_symbol():
    conn = _make_conn()
    _register(conn, symbol="BTCUSDT")
    register_model(conn, "ETHUSDT", "1m", "v1", model=_DUMMY_MODEL)
    result = list_models(conn, symbol="BTCUSDT")
    assert result["total"] == 1


def test_list_models_pagination():
    conn = _make_conn()
    for _ in range(5):
        _register(conn)
    result = list_models(conn, limit=3, offset=0)
    assert result["total"] == 5
    assert len(result["models"]) == 3


def test_list_versions_returns_all():
    conn = _make_conn()
    _register(conn)
    _register(conn)
    register_model(conn, "ETHUSDT", "1m", "v1", model=_DUMMY_MODEL)
    versions = list_versions(conn, "BTCUSDT", "1m", "v1")
    assert len(versions) == 2


def test_update_notes():
    conn = _make_conn()
    model_id = _register(conn)
    result = update_notes(conn, model_id, "updated note")
    assert result["notes"] == "updated note"


def test_update_notes_not_found():
    conn = _make_conn()
    assert update_notes(conn, 9999, "x") is None


# ---------------------------------------------------------------------------
# Group 7: API endpoints
# ---------------------------------------------------------------------------

def _registry_client(monkeypatch) -> tuple:
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    return TestClient(app), pconn


def _seed_training_job(pconn: _PersistentConn) -> int:
    """Insert a fake 'done' training job and return its id."""
    from app.training.job_service import create_job, update_job
    import json
    job_id = create_job(pconn, "BTCUSDT", "1m", "v1")
    update_job(pconn, job_id, status="done",
               model=_DUMMY_MODEL, metrics=_DUMMY_METRICS)
    return job_id


def test_api_list_models_empty(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    resp = client.get("/registry/models")
    assert resp.status_code == 200
    assert resp.json()["total"] == 0


def test_api_register_model(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    job_id = _seed_training_job(pconn)
    resp = client.post("/registry/models", json={
        "symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": job_id,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "candidate"
    assert data["symbol"] == "BTCUSDT"
    assert data["model"]["model_type"] == "logistic_regression_v1"


def test_api_register_without_job_id_fails(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    resp = client.post("/registry/models", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    assert resp.status_code == 422


def test_api_register_nonexistent_job_returns_404(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    resp = client.post("/registry/models", json={
        "symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": 9999,
    })
    assert resp.status_code == 404


def test_api_get_model_not_found(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    resp = client.get("/registry/models/9999")
    assert resp.status_code == 404


def test_api_get_model_by_id(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    job_id = _seed_training_job(pconn)
    reg = client.post("/registry/models", json={
        "symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": job_id,
    })
    model_id = reg.json()["id"]
    resp = client.get(f"/registry/models/{model_id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == model_id


def test_api_promote_model(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    job_id = _seed_training_job(pconn)
    reg = client.post("/registry/models", json={
        "symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": job_id,
    })
    model_id = reg.json()["id"]
    resp = client.post(f"/registry/models/{model_id}/promote")
    assert resp.status_code == 200
    assert resp.json()["status"] == "champion"


def test_api_champion_endpoint(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    job_id = _seed_training_job(pconn)
    reg = client.post("/registry/models", json={
        "symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": job_id,
    })
    model_id = reg.json()["id"]
    client.post(f"/registry/models/{model_id}/promote")

    resp = client.get("/registry/champion/BTCUSDT?timeframe=1m")
    assert resp.status_code == 200
    assert resp.json()["status"] == "champion"
    assert resp.json()["id"] == model_id


def test_api_champion_404_when_none(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    resp = client.get("/registry/champion/BTCUSDT?timeframe=1m")
    assert resp.status_code == 404


def test_api_archive_model(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    job_id = _seed_training_job(pconn)
    reg = client.post("/registry/models", json={
        "symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": job_id,
    })
    model_id = reg.json()["id"]
    resp = client.post(f"/registry/models/{model_id}/archive")
    assert resp.status_code == 200
    assert resp.json()["status"] == "archived"


def test_api_rollback_via_promote(monkeypatch):
    client, pconn = _registry_client(monkeypatch)

    # Register two models
    j1 = _seed_training_job(pconn)
    j2 = _seed_training_job(pconn)
    r1 = client.post("/registry/models", json={"symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": j1})
    r2 = client.post("/registry/models", json={"symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": j2})
    id1, id2 = r1.json()["id"], r2.json()["id"]

    client.post(f"/registry/models/{id1}/promote")
    client.post(f"/registry/models/{id2}/promote")  # id2 is champion, id1 archived

    # Rollback: promote id1 again
    rollback = client.post(f"/registry/models/{id1}/promote")
    assert rollback.json()["status"] == "champion"

    # id2 should now be archived
    m2 = client.get(f"/registry/models/{id2}").json()
    assert m2["status"] == "archived"


def test_api_update_notes(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    job_id = _seed_training_job(pconn)
    reg = client.post("/registry/models", json={
        "symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": job_id,
    })
    model_id = reg.json()["id"]
    resp = client.patch(f"/registry/models/{model_id}", json={"notes": "baseline v1"})
    assert resp.status_code == 200
    assert resp.json()["notes"] == "baseline v1"


def test_api_list_versions(monkeypatch):
    client, pconn = _registry_client(monkeypatch)
    for _ in range(3):
        j = _seed_training_job(pconn)
        client.post("/registry/models", json={"symbol": "BTCUSDT", "timeframe": "1m", "training_job_id": j})

    resp = client.get("/registry/versions/BTCUSDT?timeframe=1m")
    assert resp.status_code == 200
    assert len(resp.json()) == 3
