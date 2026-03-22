"""Tests for the RL experiment: environment, agent, experiment runner, and API."""

import math
import sqlite3
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.migrations import run_migrations
from app.features.compute import MIN_CANDLES, compute_features_for_candles
from app.features.store import materialize_features
from app.rl.agent import ReinforceAgent
from app.rl.environment import TradingEnv, buy_and_hold_return, episode_metrics
from app.rl.experiment import (
    _extract_rows_and_closes,
    run_rl_experiment,
)
from app.training.dataset import FEATURE_NAMES


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


def _seeded_vectors(n: int = MIN_CANDLES + 80) -> List[Dict]:
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    return compute_features_for_candles(_make_candles(closes))


def _flat_rows(n: int = 30) -> List[List[float]]:
    return [[0.0] * len(FEATURE_NAMES)] * n


def _dummy_closes(n: int = 30, start: float = 100.0, step: float = 0.1) -> List[float]:
    return [start + i * step for i in range(n)]


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


# ---------------------------------------------------------------------------
# Group 1: TradingEnv
# ---------------------------------------------------------------------------

def test_env_reset_returns_first_obs():
    rows = _flat_rows(10)
    closes = _dummy_closes(10)
    env = TradingEnv(rows, closes)
    obs = env.reset()
    assert obs == rows[0]


def test_env_step_returns_next_obs():
    rows = _flat_rows(5)
    closes = _dummy_closes(5)
    env = TradingEnv(rows, closes)
    env.reset()
    next_obs, reward, done = env.step(0)
    assert next_obs == rows[1]
    assert not done


def test_env_done_on_last_step():
    rows = _flat_rows(2)
    closes = _dummy_closes(2)
    env = TradingEnv(rows, closes)
    env.reset()
    # First step: t=0→1, not done yet
    _, _, done = env.step(0)
    assert not done
    # Second step: t=1 (last), now done
    _, _, done = env.step(0)
    assert done


def test_env_hold_reward_is_zero():
    rows = _flat_rows(5)
    closes = [100.0, 110.0, 120.0, 130.0, 140.0]
    env = TradingEnv(rows, closes)
    env.reset()
    _, reward, _ = env.step(0)
    assert reward == 0.0


def test_env_long_reward_is_log_return():
    rows = _flat_rows(5)
    closes = [100.0, 110.0, 120.0, 130.0, 140.0]
    env = TradingEnv(rows, closes)
    env.reset()
    _, reward, _ = env.step(1)
    assert reward == pytest.approx(math.log(110.0 / 100.0))


def test_env_last_step_reward_zero_even_if_long():
    # The last candle (is_last=True) yields 0 reward even for LONG action.
    # With 3 rows, t=2 is the last index.
    rows = _flat_rows(3)
    closes = [100.0, 120.0, 200.0]
    env = TradingEnv(rows, closes)
    env.reset()
    env.step(0)       # t=0 → t=1
    env.step(0)       # t=1 → t=2, done=False (t=2 < n=3)
    _, reward, done = env.step(1)  # t=2 (last), action=LONG → reward=0, done=True
    assert reward == 0.0
    assert done


def test_env_raises_after_done():
    rows = _flat_rows(2)
    env = TradingEnv(rows, _dummy_closes(2))
    env.reset()
    env.step(0)  # t=0→1, not done
    env.step(0)  # t=1→2, done=True
    with pytest.raises(RuntimeError):
        env.step(0)  # episode is done — must raise


def test_env_mismatched_lengths_raises():
    with pytest.raises(ValueError):
        TradingEnv(_flat_rows(5), _dummy_closes(4))


def test_env_too_short_raises():
    with pytest.raises(ValueError):
        TradingEnv(_flat_rows(1), _dummy_closes(1))


def test_buy_and_hold_return():
    closes = [100.0, 200.0]
    assert buy_and_hold_return(closes) == pytest.approx(math.log(2.0))


def test_buy_and_hold_flat():
    closes = [100.0, 100.0, 100.0]
    assert buy_and_hold_return(closes) == pytest.approx(0.0)


def test_episode_metrics_basic():
    rewards = [0.01, 0.0, -0.005, 0.02]
    actions = [1, 0, 1, 1]
    m = episode_metrics(rewards, actions)
    assert m["n_trades"] == 3
    assert m["n_steps"] == 4
    assert isinstance(m["cumulative_return"], float)


def test_episode_metrics_empty():
    m = episode_metrics([], [])
    assert m["n_steps"] == 0
    assert m["cumulative_return"] == 0.0


# ---------------------------------------------------------------------------
# Group 2: ReinforceAgent
# ---------------------------------------------------------------------------

def test_agent_action_prob_range():
    agent = ReinforceAgent(len(FEATURE_NAMES))
    obs = [0.0] * len(FEATURE_NAMES)
    p = agent.action_prob(obs)
    assert 0.0 < p < 1.0


def test_agent_select_action_binary():
    agent = ReinforceAgent(len(FEATURE_NAMES))
    obs = [0.0] * len(FEATURE_NAMES)
    for _ in range(20):
        action, log_prob = agent.select_action(obs)
        assert action in (0, 1)
        assert log_prob < 0.0  # log of probability ≤ 1


def test_agent_greedy_action_binary():
    agent = ReinforceAgent(len(FEATURE_NAMES))
    obs = [0.0] * len(FEATURE_NAMES)
    action = agent.greedy_action(obs)
    assert action in (0, 1)


def test_agent_update_changes_weights():
    agent = ReinforceAgent(len(FEATURE_NAMES), seed=1)
    initial_weights = list(agent.weights)
    obs = [[0.1 * j for j in range(len(FEATURE_NAMES))]] * 5
    actions = [1, 0, 1, 0, 1]
    rewards = [0.01, 0.0, 0.02, 0.0, 0.01]
    agent.update(obs, actions, rewards)
    assert agent.weights != initial_weights


def test_agent_run_episode_length_matches_env():
    rows = _flat_rows(20)
    closes = _dummy_closes(20)
    env = TradingEnv(rows, closes)
    agent = ReinforceAgent(len(FEATURE_NAMES))
    ep = agent.run_episode(env, train=True)
    assert len(ep["rewards"]) == 20
    assert len(ep["actions"]) == 20


def test_agent_run_episode_no_train_no_loss():
    rows = _flat_rows(10)
    closes = _dummy_closes(10)
    env = TradingEnv(rows, closes)
    agent = ReinforceAgent(len(FEATURE_NAMES))
    ep = agent.run_episode(env, train=False)
    assert ep["loss"] is None


def test_agent_reproducible_with_seed():
    rows = _flat_rows(15)
    closes = _dummy_closes(15)

    a1 = ReinforceAgent(len(FEATURE_NAMES), seed=7)
    a2 = ReinforceAgent(len(FEATURE_NAMES), seed=7)
    env1 = TradingEnv(rows, closes)
    env2 = TradingEnv(rows, closes)
    ep1 = a1.run_episode(env1, train=True)
    ep2 = a2.run_episode(env2, train=True)
    assert ep1["actions"] == ep2["actions"]


def test_agent_to_dict_keys():
    agent = ReinforceAgent(len(FEATURE_NAMES))
    d = agent.to_dict()
    assert d["model_type"] == "reinforce_v1"
    assert len(d["weights"]) == len(FEATURE_NAMES)
    assert "bias" in d


# ---------------------------------------------------------------------------
# Group 3: experiment.run_rl_experiment
# ---------------------------------------------------------------------------

def test_experiment_too_few_vectors_raises():
    vectors = _seeded_vectors(5)
    with pytest.raises(ValueError):
        run_rl_experiment(vectors, n_episodes=2)


def test_experiment_returns_required_keys():
    vectors = _seeded_vectors()
    result = run_rl_experiment(vectors, n_episodes=5)
    expected = {"agent", "train", "test_rl", "test_bnh", "test_supervised", "verdict", "dataset"}
    assert expected == set(result.keys())


def test_experiment_verdict_is_string():
    vectors = _seeded_vectors()
    result = run_rl_experiment(vectors, n_episodes=5)
    assert isinstance(result["verdict"], str)
    assert result["verdict"] in (
        "rl_wins", "supervised_wins", "bnh_wins", "tie",
        "rl_beats_bnh", "bnh_beats_rl",
    )


def test_experiment_no_supervised_no_supervised_result():
    vectors = _seeded_vectors()
    result = run_rl_experiment(vectors, n_episodes=5,
                               supervised_weights=None, supervised_bias=None)
    assert result["test_supervised"] is None
    assert result["verdict"] in ("rl_beats_bnh", "bnh_beats_rl")


def test_experiment_with_supervised_baseline():
    vectors = _seeded_vectors()
    weights = [0.0] * len(FEATURE_NAMES)
    bias = 0.1
    result = run_rl_experiment(vectors, n_episodes=5,
                               supervised_weights=weights, supervised_bias=bias)
    assert result["test_supervised"] is not None
    assert result["test_supervised"]["strategy"] == "supervised_champion"


def test_experiment_dataset_sizes():
    vectors = _seeded_vectors(MIN_CANDLES + 100)
    result = run_rl_experiment(vectors, n_episodes=5, test_ratio=0.2)
    ds = result["dataset"]
    assert ds["n_train"] + ds["n_test"] == ds["n_total"]
    assert ds["n_test"] > 0 and ds["n_train"] > 0


def test_experiment_loss_history_length():
    vectors = _seeded_vectors()
    result = run_rl_experiment(vectors, n_episodes=10)
    assert len(result["train"]["loss_history"]) == 10


def test_experiment_agent_has_feature_names():
    vectors = _seeded_vectors()
    result = run_rl_experiment(vectors, n_episodes=3)
    assert result["agent"]["model_type"] == "reinforce_v1"
    assert len(result["agent"]["weights"]) == len(FEATURE_NAMES)


def test_experiment_reproducible():
    vectors = _seeded_vectors()
    r1 = run_rl_experiment(vectors, n_episodes=5, seed=99)
    r2 = run_rl_experiment(vectors, n_episodes=5, seed=99)
    assert r1["agent"]["weights"] == r2["agent"]["weights"]
    assert r1["verdict"] == r2["verdict"]


def test_extract_rows_and_closes_filters_none():
    vectors = [
        {"open_time": 1000, "close": None, "returns_1": 0.0},
        {"open_time": 2000, "close": 100.0, "returns_1": 0.01},
    ]
    rows, closes = _extract_rows_and_closes(vectors)
    assert len(rows) == 1
    assert closes[0] == 100.0


# ---------------------------------------------------------------------------
# Group 4: API endpoint
# ---------------------------------------------------------------------------

def _rl_client(monkeypatch) -> tuple:
    pconn = _make_api_conn()
    monkeypatch.setattr("app.api.main.get_connection", lambda: pconn)
    monkeypatch.setattr("app.api.main._backtest_start_iso", lambda days: "2020-01-01")
    return TestClient(app), pconn


def _seed_api_features(pconn: _PersistentConn, n: int = MIN_CANDLES + 80) -> None:
    closes = [100.0 + math.sin(i * 0.15) * 5 + i * 0.02 for i in range(n)]
    candles = _make_candles(closes)
    _insert_candles(pconn._conn, candles)
    materialize_features(pconn, "BTCUSDT", "1m", candles)


def test_api_rl_job_fails_without_features(monkeypatch):
    client, pconn = _rl_client(monkeypatch)
    resp = client.post("/training/rl-jobs", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "failed"
    assert "Insufficient" in (resp.json()["error"] or "")


def test_api_rl_job_succeeds(monkeypatch):
    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 5},
    )
    assert resp.status_code == 200
    job = resp.json()
    assert job["status"] == "done"
    assert job["model"]["model_type"] == "reinforce_v1"
    assert job["metrics"]["verdict"] is not None


def test_api_rl_job_metrics_structure(monkeypatch):
    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3},
    )
    m = resp.json()["metrics"]
    assert "test_rl" in m
    assert "test_bnh" in m
    assert "verdict" in m
    assert m["test_rl"]["n_steps"] > 0


def test_api_rl_job_stored_in_list(monkeypatch):
    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)
    client.post("/training/rl-jobs", json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3})
    list_resp = client.get("/training/jobs")
    assert list_resp.json()["total"] == 1


def test_api_rl_job_dataset_stats(monkeypatch):
    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3},
    )
    ds = resp.json()["dataset"]
    assert ds["n_train"] > 0
    assert ds["n_test"] > 0
    assert "feature_names" in ds


def test_api_rl_job_with_champion_supervised_comparison(monkeypatch):
    """When a champion model exists, test_supervised should be populated."""
    from app.registry.registry_service import promote_model, register_model
    from app.training.dataset import FEATURE_NAMES

    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)

    # Register and promote a dummy champion
    dummy_model = {
        "model_type": "logistic_regression_v1",
        "weights": [0.0] * len(FEATURE_NAMES),
        "bias": 0.1,
        "n_features": len(FEATURE_NAMES),
        "feature_names": FEATURE_NAMES,
        "n_train": 50, "n_epochs": 10,
        "learning_rate": 0.01, "l2_lambda": 1e-4, "final_train_loss": 0.693,
    }
    mid = register_model(pconn, "BTCUSDT", "1m", "v1", model=dummy_model)
    promote_model(pconn, mid)

    resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3, "use_champion": True},
    )
    assert resp.status_code == 200
    m = resp.json()["metrics"]
    assert m["test_supervised"] is not None
    assert m["test_supervised"]["strategy"] == "supervised_champion"


# ---------------------------------------------------------------------------
# Group 5: RL registry integration
# ---------------------------------------------------------------------------

def test_api_rl_job_registers_in_model_registry(monkeypatch):
    """Successful RL job should auto-register the agent in model_registry as candidate."""
    from app.registry.registry_service import get_model

    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3},
    )
    assert resp.status_code == 200
    job = resp.json()
    assert job["status"] == "done"
    assert job["registry_model_id"] is not None

    model = get_model(pconn, job["registry_model_id"])
    assert model is not None
    assert model["status"] == "candidate"
    assert model["model"]["model_type"] == "reinforce_v1"
    assert model["symbol"] == "BTCUSDT"


def test_api_rl_job_auto_promote(monkeypatch):
    """auto_promote=True should set RL agent as champion in model_registry."""
    from app.registry.registry_service import get_champion

    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3, "auto_promote": True},
    )
    assert resp.status_code == 200
    job = resp.json()
    assert job["registry_status"] == "champion"

    champion = get_champion(pconn, "BTCUSDT", "1m", "v1")
    assert champion is not None
    assert champion["id"] == job["registry_model_id"]
    assert champion["model"]["model_type"] == "reinforce_v1"


def test_api_rl_job_registry_status_candidate_by_default(monkeypatch):
    """Without auto_promote, RL agent should remain a candidate."""
    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)
    resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3},
    )
    assert resp.json()["registry_status"] == "candidate"


def test_api_rl_job_failed_job_has_no_registry_model(monkeypatch):
    """A failed RL job should not produce a registry entry."""
    client, pconn = _rl_client(monkeypatch)
    # No features seeded → job will fail
    resp = client.post("/training/rl-jobs", json={"symbol": "BTCUSDT", "timeframe": "1m"})
    assert resp.json()["status"] == "failed"
    assert resp.json()["registry_model_id"] is None
    assert resp.json()["registry_status"] is None


def test_api_rl_champion_served_by_inference(monkeypatch):
    """An RL agent promoted to champion should be served by the inference endpoint."""
    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)

    # Train + auto-promote RL agent
    rl_resp = client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 5, "auto_promote": True},
    )
    assert rl_resp.json()["registry_status"] == "champion"

    # Inference should use the RL champion
    inf_resp = client.get("/inference/predict/BTCUSDT")
    assert inf_resp.status_code == 200
    result = inf_resp.json()
    assert result["model_type"] == "reinforce_v1"
    assert result["signal"] in ("UP", "DOWN")
    assert 0.0 <= result["probability"] <= 1.0


def test_api_rl_champion_inference_status_ready(monkeypatch):
    """Inference status should be ready=True after RL champion is promoted."""
    client, pconn = _rl_client(monkeypatch)
    _seed_api_features(pconn)

    client.post(
        "/training/rl-jobs",
        json={"symbol": "BTCUSDT", "timeframe": "1m", "n_episodes": 3, "auto_promote": True},
    )

    status_resp = client.get("/inference/status/BTCUSDT")
    assert status_resp.status_code == 200
    assert status_resp.json()["ready"] is True
