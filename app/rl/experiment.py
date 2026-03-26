"""RL experiment runner.

Trains a REINFORCE agent and compares it against two baselines:
  1. Buy-and-hold (always LONG)
  2. Supervised logistic-regression model (if a champion exists in registry)

The result dict is stored in the training_jobs table with job_type='rl'
embedded in params_json, so no new DB table is needed.
"""

import math
from typing import Any, Dict, List, Optional, Tuple

from app.rl.environment import (
    TradingEnv,
    buy_and_hold_return_after_fees,
    episode_metrics,
)
from app.rl.agent import ReinforceAgent
from app.training.dataset import FEATURE_NAMES, _safe_float, train_test_split
from app.training.trainer import predict as supervised_predict


# ---------------------------------------------------------------------------
# Feature / close extraction from feature-vector dicts
# ---------------------------------------------------------------------------

def _extract_rows_and_closes(
    vectors: List[Dict[str, Any]],
) -> Tuple[List[List[float]], List[float]]:
    """Return (feature_rows, close_prices) for vectors that have a close."""
    rows, closes = [], []
    for fv in vectors:
        close = fv.get("close")
        if close is None:
            continue
        row = []
        for name in FEATURE_NAMES:
            val = _safe_float(fv.get(name), 0.0)
            if name == "rsi_14":
                val /= 100.0
            row.append(val)
        rows.append(row)
        closes.append(float(close))
    return rows, closes


# ---------------------------------------------------------------------------
# Evaluation helpers
# ---------------------------------------------------------------------------

def _evaluate_greedy(
    agent: ReinforceAgent,
    feature_rows: List[List[float]],
    closes: List[float],
    fee_rate: float,
) -> Dict[str, Any]:
    """Run greedy rollout (no training) and return metrics."""
    env = TradingEnv(feature_rows, closes, fee_rate=fee_rate)
    obs = env.reset()
    actions: List[int] = []
    rewards: List[float] = []

    while True:
        action = agent.greedy_action(obs)
        actions.append(action)
        next_obs, reward, done = env.step(action)
        rewards.append(reward)
        if done:
            break
        obs = next_obs

    return episode_metrics(rewards, actions)


def _evaluate_supervised(
    weights: List[float],
    bias: float,
    feature_rows: List[List[float]],
    closes: List[float],
    fee_rate: float,
    threshold: float = 0.5,
) -> Dict[str, Any]:
    """Simulate trading using supervised model signals."""
    preds = supervised_predict(weights, bias, feature_rows, threshold)
    rewards: List[float] = []
    actions: List[int] = []
    n = len(feature_rows)
    prev_action = 0
    for i in range(n):
        action = preds[i]
        actions.append(action)
        fee = fee_rate if action != prev_action else 0.0
        if action == 1 and i < n - 1 and closes[i] > 0:
            reward = math.log(closes[i + 1] / closes[i]) - fee
        else:
            reward = -fee
        rewards.append(reward)
        prev_action = action
    return episode_metrics(rewards, actions)


# ---------------------------------------------------------------------------
# Main experiment function
# ---------------------------------------------------------------------------

def run_rl_experiment(
    vectors: List[Dict[str, Any]],
    n_episodes: int = 200,
    learning_rate: float = 1e-3,
    gamma: float = 1.0,
    test_ratio: float = 0.2,
    seed: int = 42,
    fee_rate: float = 0.0,
    supervised_weights: Optional[List[float]] = None,
    supervised_bias: Optional[float] = None,
) -> Dict[str, Any]:
    """Train a REINFORCE agent and benchmark it against baselines.

    Parameters
    ----------
    vectors:
        Feature-vector dicts in chronological order.
    n_episodes:
        Number of training episodes (each = one pass over training data).
    supervised_weights / supervised_bias:
        Optional champion model weights for comparison.  If None the
        supervised baseline section is omitted.

    Returns
    -------
    Dict with keys:
      agent_dict        serialised final policy weights
      train_metrics     episode-level training stats (loss curve, last-episode)
      test_rl           greedy evaluation on test set
      test_bnh          buy-and-hold on test set
      test_supervised   supervised model on test set (or None)
      dataset           split sizes
    """
    if len(vectors) < 10:
        raise ValueError(f"Need at least 10 feature vectors, got {len(vectors)}.")
    if fee_rate < 0:
        raise ValueError("fee_rate must be non-negative.")

    all_rows, all_closes = _extract_rows_and_closes(vectors)
    if len(all_rows) < 10:
        raise ValueError(f"Only {len(all_rows)} valid rows after filtering None closes.")

    n = len(all_rows)
    split = max(1, int(n * (1.0 - test_ratio)))
    train_rows, train_closes = all_rows[:split], all_closes[:split]
    test_rows, test_closes = all_rows[split:], all_closes[split:]

    if len(train_rows) < 2 or len(test_rows) < 2:
        raise ValueError("Train or test split is too small (need >= 2 rows each).")

    agent = ReinforceAgent(
        n_features=len(FEATURE_NAMES),
        learning_rate=learning_rate,
        gamma=gamma,
        seed=seed,
    )

    train_env = TradingEnv(train_rows, train_closes, fee_rate=fee_rate)
    loss_history: List[float] = []

    for _ in range(n_episodes):
        ep = agent.run_episode(train_env, train=True)
        if ep["loss"] is not None:
            loss_history.append(round(ep["loss"], 6))

    # Evaluation
    test_rl = _evaluate_greedy(agent, test_rows, test_closes, fee_rate=fee_rate)
    test_bnh = {
        "cumulative_return": round(buy_and_hold_return_after_fees(test_closes, fee_rate=fee_rate), 6),
        "n_steps": len(test_closes),
        "n_trades": len(test_closes) - 1,
        "strategy": "buy_and_hold",
    }

    test_supervised: Optional[Dict[str, Any]] = None
    if supervised_weights is not None and supervised_bias is not None:
        test_supervised = _evaluate_supervised(
            supervised_weights, supervised_bias, test_rows, test_closes, fee_rate=fee_rate
        )
        test_supervised["strategy"] = "supervised_champion"

    # Summary comparison
    rl_ret = test_rl["cumulative_return"]
    bnh_ret = test_bnh["cumulative_return"]
    sup_ret = test_supervised["cumulative_return"] if test_supervised else None

    verdict: str
    if sup_ret is not None:
        if rl_ret > sup_ret and rl_ret > bnh_ret:
            verdict = "rl_wins"
        elif sup_ret > rl_ret and sup_ret > bnh_ret:
            verdict = "supervised_wins"
        elif bnh_ret >= rl_ret and bnh_ret >= sup_ret:
            verdict = "bnh_wins"
        else:
            verdict = "tie"
    else:
        verdict = "rl_beats_bnh" if rl_ret > bnh_ret else "bnh_beats_rl"

    return {
        "agent": agent.to_dict(),
        "train": {
            "n_episodes": n_episodes,
            "fee_rate": fee_rate,
            "loss_history": loss_history,
            "final_loss": loss_history[-1] if loss_history else None,
        },
        "test_rl": test_rl,
        "test_bnh": test_bnh,
        "test_supervised": test_supervised,
        "verdict": verdict,
        "dataset": {
            "n_total": n,
            "n_train": len(train_rows),
            "n_test": len(test_rows),
            "fee_rate": fee_rate,
            "feature_names": FEATURE_NAMES,
        },
    }
