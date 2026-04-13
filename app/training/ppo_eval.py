"""Walk-forward evaluation helpers for PPO models."""

from __future__ import annotations

import math
from typing import Any


def _run_episode(env, model=None) -> dict[str, Any]:
    obs, _ = env.reset()
    total_r = 0.0
    pos_counts: dict[int, int] = {-1: 0, 0: 0, 1: 0}
    n_trades = 0
    prev_pos = 0

    while True:
        if model is not None:
            action, _ = model.predict(obs, deterministic=True)
            action = int(action)
        else:
            action = 1  # buy-and-hold

        obs, r, done, trunc, info = env.step(action)
        total_r += r
        pos = info.get("position", 0)
        pos_counts[pos] = pos_counts.get(pos, 0) + 1
        if pos != prev_pos:
            n_trades += 1
        prev_pos = pos
        if done or trunc:
            break

    total_steps = sum(pos_counts.values())
    return {
        "log_ret":  total_r,
        "pct_ret":  math.exp(total_r) - 1,
        "n_trades": n_trades,
        "long_pct": pos_counts.get(1, 0) / max(total_steps, 1),
        "flat_pct": pos_counts.get(0, 0) / max(total_steps, 1),
    }


def walk_forward_eval(
    df,
    model,
    eval_start_idx: int,
    n_windows: int,
    ep_len: int,
    frame_stack: int = 1,
    decision_interval: int = 1,
    reward_horizon: int = 1,
) -> list[dict[str, Any]]:
    """Run n_windows walk-forward eval windows, comparing PPO vs buy-and-hold."""
    from app.rl.crypto_env import CryptoTradingEnv

    results = []
    idx = eval_start_idx
    available = len(df) - idx - 1
    actual_windows = min(n_windows, available // ep_len)

    for w in range(actual_windows):
        window_df = df.iloc[idx: idx + ep_len + 1].reset_index(drop=True)
        ppo_env = CryptoTradingEnv(
            window_df,
            episode_length=ep_len,
            deterministic=True,
            frame_stack=frame_stack,
            decision_interval=decision_interval,
            reward_horizon=reward_horizon,
        )
        bnh_env = CryptoTradingEnv(
            window_df,
            episode_length=ep_len,
            deterministic=True,
            frame_stack=frame_stack,
            decision_interval=decision_interval,
            reward_horizon=reward_horizon,
        )

        ppo_r = _run_episode(ppo_env, model)
        bnh_r = _run_episode(bnh_env, model=None)

        results.append({
            "window":    w + 1,
            "start_bar": idx,
            "ppo":       ppo_r,
            "bnh":       bnh_r,
            "beats_bnh": ppo_r["log_ret"] > bnh_r["log_ret"],
        })
        idx += ep_len

    return results
