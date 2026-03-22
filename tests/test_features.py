"""Tests for the feature store: compute, store, and API endpoints."""

import math
import sqlite3
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.migrations import run_migrations
from app.features.compute import (
    FEATURE_SET_VERSION,
    MIN_CANDLES,
    _bbands,
    _ema_series,
    _log_return,
    _macd,
    _rsi,
    _sma,
    _volatility,
    compute_feature_vector,
    compute_features_for_candles,
)
from app.features.store import (
    delete_features,
    get_features,
    get_latest_feature_vector,
    materialize_features,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_BASE_MS = 1_700_000_000_000
_INTERVAL_MS = 60_000


def _make_candles(closes: List[float], start_ms: int = _BASE_MS) -> List[Dict]:
    candles = []
    for i, close in enumerate(closes):
        open_time = start_ms + i * _INTERVAL_MS
        candles.append({
            "open_time": open_time,
            "open": str(close),
            "high": str(close * 1.001),
            "low": str(close * 0.999),
            "close": str(close),
            "volume": "1.0",
            "close_time": open_time + _INTERVAL_MS - 1,
        })
    return candles


def _make_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return conn


def _insert_candles(conn: sqlite3.Connection, candles: List[Dict]) -> None:
    for c in candles:
        conn.execute(
            """
            INSERT INTO candles (symbol, timeframe, open_time, open, high, low, close, volume, close_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "BTCUSDT", "1m",
                int(c["open_time"]),
                float(c["open"]), float(c["high"]), float(c["low"]),
                float(c["close"]), float(c["volume"]),
                int(c["close_time"]),
            ),
        )
    conn.commit()


# Persistent connection wrapper (same pattern as test_backtest.py)
class _PersistentConn:
    def __init__(self, conn: sqlite3.Connection):
        self._conn = conn
        self._conn.row_factory = sqlite3.Row

    def execute(self, sql, params=()):
        return self._conn.execute(sql, params)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        pass  # no-op — keep alive across TestClient requests

    def really_close(self):
        self._conn.close()


def _make_api_conn() -> _PersistentConn:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    run_migrations(conn)
    return _PersistentConn(conn)


# ---------------------------------------------------------------------------
# Group 1: Low-level indicator helpers
# ---------------------------------------------------------------------------

def test_sma_basic():
    assert _sma([1.0, 2.0, 3.0, 4.0, 5.0], 3) == pytest.approx(4.0)


def test_sma_insufficient_data_returns_none():
    assert _sma([1.0, 2.0], 5) is None


def test_sma_exact_window():
    assert _sma([10.0, 20.0, 30.0], 3) == pytest.approx(20.0)


def test_ema_series_single_value():
    result = _ema_series([100.0], 3)
    assert result == [100.0]


def test_ema_series_length_matches_input():
    values = [float(i) for i in range(1, 11)]
    result = _ema_series(values, 3)
    assert len(result) == 10


def test_ema_series_is_smoothed():
    # EMA should lag behind a suddenly large spike
    values = [100.0] * 10 + [200.0]
    result = _ema_series(values, 3)
    # Last value after spike should be between 100 and 200
    assert 100.0 < result[-1] < 200.0


def test_rsi_all_gains_returns_100():
    closes = [float(i) for i in range(1, 20)]  # strictly increasing
    rsi = _rsi(closes, 14)
    assert rsi == 100.0


def test_rsi_all_losses_returns_0():
    closes = [float(20 - i) for i in range(20)]  # strictly decreasing
    rsi = _rsi(closes, 14)
    assert rsi == pytest.approx(0.0, abs=1e-6)


def test_rsi_insufficient_data_returns_none():
    assert _rsi([1.0, 2.0, 3.0], 14) is None


def test_rsi_range():
    closes = [100.0 + math.sin(i * 0.3) * 5 for i in range(40)]
    rsi = _rsi(closes, 14)
    assert rsi is not None
    assert 0.0 <= rsi <= 100.0


def test_macd_returns_none_when_insufficient():
    closes = [float(i) for i in range(30)]  # need slow(26) + signal(9) = 35
    result = _macd(closes)
    assert result is None


def test_macd_returns_dict_with_expected_keys():
    closes = [100.0 + math.sin(i * 0.1) * 2 for i in range(60)]
    result = _macd(closes)
    assert result is not None
    assert "macd_line" in result
    assert "macd_signal" in result
    assert "macd_hist" in result


def test_macd_hist_equals_line_minus_signal():
    closes = [100.0 + i * 0.1 for i in range(60)]
    result = _macd(closes)
    assert result is not None
    assert result["macd_hist"] == pytest.approx(
        result["macd_line"] - result["macd_signal"], abs=1e-10
    )


def test_bbands_returns_none_when_insufficient():
    assert _bbands([1.0, 2.0, 3.0], 20) is None


def test_bbands_upper_gt_mid_gt_lower():
    closes = [100.0 + math.sin(i * 0.2) * 3 for i in range(25)]
    result = _bbands(closes, 20)
    assert result is not None
    assert result["bb_upper"] > result["bb_mid"] > result["bb_lower"]


def test_bbands_pct_b_at_mid_is_half():
    # If close == mid, pct_b should be 0.5
    closes = [100.0] * 25  # constant → std=0 → bands collapse, pct_b = 0.5
    result = _bbands(closes, 20)
    assert result is not None
    assert result["bb_pct_b"] == pytest.approx(0.5)


def test_log_return_basic():
    # ln(110/100) ≈ 0.09531
    closes = [100.0, 110.0]
    r = _log_return(closes, 1)
    assert r == pytest.approx(math.log(110.0 / 100.0))


def test_log_return_insufficient_data_returns_none():
    assert _log_return([100.0], 1) is None


def test_volatility_returns_none_when_insufficient():
    returns = [0.001] * 15
    assert _volatility(returns, 20) is None


def test_volatility_zero_for_constant_series():
    returns = [0.005] * 25
    vol = _volatility(returns, 20)
    assert vol == pytest.approx(0.0, abs=1e-10)


# ---------------------------------------------------------------------------
# Group 2: compute_feature_vector
# ---------------------------------------------------------------------------

def test_compute_feature_vector_has_all_keys():
    closes = [100.0 + i * 0.1 for i in range(MIN_CANDLES)]
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    expected_keys = {
        "open_time", "close", "feature_set",
        "returns_1", "returns_5", "returns_20",
        "ma_5", "ma_20", "ma_50", "ma_cross_5_20",
        "rsi_14",
        "macd_line", "macd_signal", "macd_hist",
        "bb_upper", "bb_mid", "bb_lower", "bb_pct_b",
        "volatility_20",
    }
    assert expected_keys == set(fv.keys())


def test_compute_feature_vector_feature_set_version():
    closes = [100.0] * MIN_CANDLES
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    assert fv["feature_set"] == FEATURE_SET_VERSION


def test_compute_feature_vector_open_time_preserved():
    closes = [100.0] * MIN_CANDLES
    ts = _BASE_MS + 999
    fv = compute_feature_vector(closes, open_time=ts)
    assert fv["open_time"] == ts


def test_compute_feature_vector_close_is_last():
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    assert fv["close"] == pytest.approx(closes[-1], rel=1e-5)


def test_compute_feature_vector_nones_when_insufficient():
    closes = [100.0, 101.0]  # way too few
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    # Many indicators should be None
    assert fv["ma_50"] is None
    assert fv["rsi_14"] is None
    assert fv["macd_line"] is None


def test_compute_feature_vector_ma_cross_bull():
    # Increasing prices → short MA > long MA → cross = 1.0
    closes = [100.0 + i * 0.5 for i in range(MIN_CANDLES)]
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    assert fv["ma_cross_5_20"] == 1.0


def test_compute_feature_vector_ma_cross_bear():
    # Decreasing prices → short MA < long MA → cross = -1.0
    closes = [200.0 - i * 0.5 for i in range(MIN_CANDLES)]
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    assert fv["ma_cross_5_20"] == -1.0


def test_compute_feature_vector_rsi_in_range():
    closes = [100.0 + math.sin(i * 0.3) * 5 for i in range(MIN_CANDLES)]
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    assert fv["rsi_14"] is not None
    assert 0.0 <= fv["rsi_14"] <= 100.0


def test_compute_feature_vector_bb_order():
    closes = [100.0 + math.sin(i * 0.2) * 3 for i in range(MIN_CANDLES)]
    fv = compute_feature_vector(closes, open_time=_BASE_MS)
    if fv["bb_upper"] is not None:
        assert fv["bb_upper"] >= fv["bb_mid"] >= fv["bb_lower"]


# ---------------------------------------------------------------------------
# Group 3: compute_features_for_candles
# ---------------------------------------------------------------------------

def test_compute_features_for_candles_length():
    closes = [100.0 + i * 0.1 for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    vectors = compute_features_for_candles(candles)
    assert len(vectors) == len(closes)


def test_compute_features_for_candles_sorted_by_open_time():
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    # Feed in reverse order — should still sort internally
    candles = list(reversed(_make_candles(closes)))
    vectors = compute_features_for_candles(candles)
    times = [v["open_time"] for v in vectors]
    assert times == sorted(times)


def test_compute_features_for_candles_early_vectors_have_nones():
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    vectors = compute_features_for_candles(candles)
    # Very first candle cannot have ma_50
    assert vectors[0]["ma_50"] is None


def test_compute_features_for_candles_last_vector_fully_populated():
    closes = [100.0 + math.sin(i * 0.2) * 5 for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    vectors = compute_features_for_candles(candles)
    last = vectors[-1]
    assert last["ma_5"] is not None
    assert last["ma_20"] is not None
    assert last["ma_50"] is not None
    assert last["rsi_14"] is not None
    assert last["macd_line"] is not None
    assert last["bb_upper"] is not None
    assert last["volatility_20"] is not None


# ---------------------------------------------------------------------------
# Group 4: feature store — materialize / get / delete
# ---------------------------------------------------------------------------

def test_materialize_features_returns_count():
    conn = _make_conn()
    closes = [100.0 + i * 0.1 for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    count = materialize_features(conn, "BTCUSDT", "1m", candles)
    assert count == len(candles)


def test_materialize_features_idempotent():
    conn = _make_conn()
    closes = [100.0 + i * 0.1 for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)
    count2 = materialize_features(conn, "BTCUSDT", "1m", candles)
    # Second run should upsert same rows, not error
    assert count2 == len(candles)
    result = get_features(conn, "BTCUSDT", "1m", limit=5000)
    assert result["total"] == len(candles)


def test_materialize_empty_candles_returns_zero():
    conn = _make_conn()
    count = materialize_features(conn, "BTCUSDT", "1m", [])
    assert count == 0


def test_get_features_pagination():
    conn = _make_conn()
    closes = [100.0 + i * 0.1 for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)

    page1 = get_features(conn, "BTCUSDT", "1m", limit=10, offset=0)
    page2 = get_features(conn, "BTCUSDT", "1m", limit=10, offset=10)

    assert page1["total"] == len(candles)
    assert len(page1["vectors"]) == 10
    assert len(page2["vectors"]) == 10
    # Pages should not overlap
    times1 = {v["open_time"] for v in page1["vectors"]}
    times2 = {v["open_time"] for v in page2["vectors"]}
    assert times1.isdisjoint(times2)


def test_get_features_ascending_order():
    conn = _make_conn()
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)
    result = get_features(conn, "BTCUSDT", "1m", ascending=True)
    times = [v["open_time"] for v in result["vectors"]]
    assert times == sorted(times)


def test_get_features_descending_order():
    conn = _make_conn()
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)
    result = get_features(conn, "BTCUSDT", "1m", ascending=False)
    times = [v["open_time"] for v in result["vectors"]]
    assert times == sorted(times, reverse=True)


def test_get_features_time_filter():
    conn = _make_conn()
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)

    # Only candles with open_time <= 10th candle
    end_ts = _BASE_MS + 9 * _INTERVAL_MS
    result = get_features(conn, "BTCUSDT", "1m", end_time=end_ts, limit=5000)
    assert result["total"] == 10
    for v in result["vectors"]:
        assert v["open_time"] <= end_ts


def test_get_features_empty_when_no_data():
    conn = _make_conn()
    result = get_features(conn, "BTCUSDT", "1m")
    assert result["total"] == 0
    assert result["vectors"] == []


def test_get_latest_feature_vector_returns_last():
    conn = _make_conn()
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)
    latest = get_latest_feature_vector(conn, "BTCUSDT", "1m")
    assert latest is not None
    expected_ts = _BASE_MS + (MIN_CANDLES - 1) * _INTERVAL_MS
    assert latest["open_time"] == expected_ts


def test_get_latest_feature_vector_none_when_empty():
    conn = _make_conn()
    result = get_latest_feature_vector(conn, "BTCUSDT", "1m")
    assert result is None


def test_delete_features_removes_rows():
    conn = _make_conn()
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)
    deleted = delete_features(conn, "BTCUSDT", "1m")
    assert deleted == len(candles)
    result = get_features(conn, "BTCUSDT", "1m")
    assert result["total"] == 0


def test_delete_features_only_affects_target_symbol():
    conn = _make_conn()
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)
    materialize_features(conn, "ETHUSDT", "1m", candles)
    delete_features(conn, "BTCUSDT", "1m")
    assert get_features(conn, "BTCUSDT", "1m")["total"] == 0
    assert get_features(conn, "ETHUSDT", "1m")["total"] == len(candles)


def test_features_vectors_have_feature_keys():
    conn = _make_conn()
    closes = [100.0 + math.sin(i * 0.2) * 3 for i in range(MIN_CANDLES)]
    candles = _make_candles(closes)
    materialize_features(conn, "BTCUSDT", "1m", candles)
    result = get_features(conn, "BTCUSDT", "1m", limit=1, ascending=False)
    assert len(result["vectors"]) == 1
    v = result["vectors"][0]
    assert "rsi_14" in v
    assert "macd_line" in v
    assert "bb_upper" in v


# ---------------------------------------------------------------------------
# Group 5: API endpoints
# ---------------------------------------------------------------------------

def _feature_client(monkeypatch, closes: List[float]) -> tuple:
    """Return (TestClient, pconn) with candles pre-inserted."""
    pconn = _make_api_conn()
    candles = _make_candles(closes)
    _insert_candles(pconn._conn, candles)
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    return TestClient(app), pconn


def test_api_compute_features_empty_db(monkeypatch):
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    client = TestClient(app)
    resp = client.get("/features/compute?symbol=BTCUSDT&timeframe=1m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["candle_count"] == 0
    assert data["vectors"] == []


def test_api_compute_features_returns_vectors(monkeypatch):
    closes = [100.0 + math.sin(i * 0.2) * 3 for i in range(MIN_CANDLES)]
    client, pconn = _feature_client(monkeypatch, closes)
    resp = client.get("/features/compute?symbol=BTCUSDT&timeframe=1m")
    assert resp.status_code == 200
    data = resp.json()
    assert data["candle_count"] == MIN_CANDLES
    assert len(data["vectors"]) == MIN_CANDLES
    assert data["feature_set"] == FEATURE_SET_VERSION


def test_api_compute_features_limit_param(monkeypatch):
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    client, pconn = _feature_client(monkeypatch, closes)
    resp = client.get("/features/compute?symbol=BTCUSDT&timeframe=1m&limit=5")
    assert resp.status_code == 200
    assert len(resp.json()["vectors"]) == 5


def test_api_materialize_and_get(monkeypatch):
    closes = [100.0 + math.sin(i * 0.2) * 3 for i in range(MIN_CANDLES)]
    client, pconn = _feature_client(monkeypatch, closes)

    mat = client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    assert mat.status_code == 200
    assert mat.json()["vectors_upserted"] == MIN_CANDLES

    get_resp = client.get("/features/BTCUSDT?timeframe=1m")
    assert get_resp.status_code == 200
    assert get_resp.json()["total"] == MIN_CANDLES


def test_api_materialize_idempotent(monkeypatch):
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    client, pconn = _feature_client(monkeypatch, closes)

    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    get_resp = client.get("/features/BTCUSDT?timeframe=1m")
    assert get_resp.json()["total"] == MIN_CANDLES


def test_api_get_latest_returns_404_when_empty(monkeypatch):
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    client = TestClient(app)
    resp = client.get("/features/BTCUSDT/latest?timeframe=1m")
    assert resp.status_code == 404


def test_api_get_latest_returns_most_recent(monkeypatch):
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    client, pconn = _feature_client(monkeypatch, closes)

    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    resp = client.get("/features/BTCUSDT/latest?timeframe=1m")
    assert resp.status_code == 200
    data = resp.json()
    expected_ts = _BASE_MS + (MIN_CANDLES - 1) * _INTERVAL_MS
    assert data["open_time"] == expected_ts


def test_api_delete_features(monkeypatch):
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    client, pconn = _feature_client(monkeypatch, closes)

    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    del_resp = client.delete("/features/BTCUSDT?timeframe=1m")
    assert del_resp.status_code == 200
    assert del_resp.json()["deleted"] == MIN_CANDLES

    get_resp = client.get("/features/BTCUSDT?timeframe=1m")
    assert get_resp.json()["total"] == 0


def test_api_get_features_pagination_params(monkeypatch):
    closes = [100.0 + i for i in range(MIN_CANDLES)]
    client, pconn = _feature_client(monkeypatch, closes)

    client.post("/features/materialize", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    resp = client.get("/features/BTCUSDT?timeframe=1m&limit=10&offset=5")
    data = resp.json()
    assert data["total"] == MIN_CANDLES
    assert len(data["vectors"]) == 10
    assert data["offset"] == 5
