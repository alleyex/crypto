"""PPO training entry point — callable from API.

Wraps the logic from scripts/train_ppo.py into a function that:
  1. Loads candles from DB and builds features
  2. Trains PPO (Stable Baselines3) with optional progress callbacks
  3. Runs walk-forward evaluation
  4. Saves candidate model to runtime/models/ppo_{symbol}_{tf}_candidate_{job_id}.zip
  5. Returns a structured result dict

Caller is responsible for running this in a background thread and persisting
progress/results to the training_jobs table.
"""

from __future__ import annotations

import math
import warnings
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR  = ROOT / "runtime" / "models"
TB_LOGS_DIR = ROOT / "runtime" / "tb_logs"

# Default PPO hyperparameters (mirrors scripts/train_ppo.py)
DEFAULT_PPO_KWARGS: Dict[str, Any] = dict(
    learning_rate=3e-4,
    n_steps=2048,
    batch_size=256,
    n_epochs=10,
    gamma=0.99,
    gae_lambda=0.95,
    clip_range=0.2,
    ent_coef=0.01,
    policy_kwargs={"net_arch": [128, 128]},
    verbose=0,
    seed=42,
)

TRAIN_FRAC   = 0.70
TRAIN_EP_LEN = 1440   # 1 day of 1m bars
EVAL_EP_LEN  = 2880   # 2 days of 1m bars


# ---------------------------------------------------------------------------
# SB3 progress callback
# ---------------------------------------------------------------------------

def _make_progress_callback(
    total_steps: int,
    on_progress: Optional[Callable[[int, int], None]],
):
    """Return a SB3 BaseCallback that fires on_progress(current, total)."""
    try:
        from stable_baselines3.common.callbacks import BaseCallback

        class _ProgressCallback(BaseCallback):
            def __init__(self) -> None:
                super().__init__(verbose=0)

            def _on_step(self) -> bool:
                if on_progress and self.n_calls % 10_000 == 0:
                    on_progress(self.num_timesteps, total_steps)
                return True

        return _ProgressCallback()
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# Evaluation helper (mirrors scripts/train_ppo.py)
# ---------------------------------------------------------------------------

def _run_episode(env, model=None) -> dict:
    obs, _ = env.reset()
    total_r = 0.0
    pos_counts: Dict[int, int] = {-1: 0, 0: 0, 1: 0}
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
        "log_ret":   total_r,
        "pct_ret":   math.exp(total_r) - 1,
        "n_trades":  n_trades,
        "long_pct":  pos_counts.get(1, 0) / max(total_steps, 1),
        "flat_pct":  pos_counts.get(0, 0) / max(total_steps, 1),
    }


def _walk_forward_eval(df: pd.DataFrame, model, eval_start_idx: int,
                        n_windows: int, ep_len: int) -> List[dict]:
    from app.rl.crypto_env import CryptoTradingEnv

    results = []
    idx = eval_start_idx
    available = len(df) - idx - 1
    actual_windows = min(n_windows, available // ep_len)

    for w in range(actual_windows):
        window_df = df.iloc[idx: idx + ep_len + 1].reset_index(drop=True)
        ppo_env = CryptoTradingEnv(window_df, episode_length=ep_len, deterministic=True)
        bnh_env = CryptoTradingEnv(window_df, episode_length=ep_len, deterministic=True)

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


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def run_ppo_training(
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    total_steps: int = 1_000_000,
    eval_windows: int = 8,
    fee_rate: float = 0.001,
    learning_rate: float = 3e-4,
    n_steps: int = 2048,
    batch_size: int = 256,
    n_epochs: int = 10,
    gamma: float = 0.99,
    seed: int = 42,
    job_id: Optional[int] = None,
    on_progress: Optional[Callable[[int, int], None]] = None,
) -> Dict[str, Any]:
    """Train PPO and return results dict.

    Parameters
    ----------
    on_progress : callable(current_step, total_steps)
        Called every ~10k steps during training.
    job_id : int, optional
        Used to name the candidate model file.

    Returns
    -------
    dict with keys:
        symbol, timeframe, total_steps, eval_windows,
        n_train, n_total, fee_rate,
        walk_forward: list of per-window results,
        verdict: "PASS" | "MARGINAL" | "FAIL",
        avg_ppo_pct, avg_bnh_pct, avg_edge, win_rate,
        model_path: str (candidate .zip path),
    """
    from stable_baselines3 import PPO
    from stable_baselines3.common.monitor import Monitor
    from app.core.db import get_connection
    from app.features.crypto_features import build_crypto_features, MIN_VALID_ROWS
    from app.rl.crypto_env import CryptoTradingEnv, FEE_PER_SIDE as _DEFAULT_FEE

    # --- Load candles ---
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT open_time, open, high, low, close, volume,
                   quote_asset_volume, number_of_trades,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM candles WHERE symbol=? AND timeframe=? ORDER BY open_time ASC
            """,
            (symbol, timeframe),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise RuntimeError(f"No candle data for {symbol}/{timeframe}")

    cols = ["open_time", "open", "high", "low", "close", "volume",
            "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume"]
    df = build_crypto_features(
        pd.DataFrame(rows, columns=cols)
    ).iloc[MIN_VALID_ROWS:].reset_index(drop=True)

    n_total   = len(df)
    train_end = int(n_total * TRAIN_FRAC)

    if train_end < TRAIN_EP_LEN:
        raise ValueError(
            f"Not enough training data: {train_end} rows (need {TRAIN_EP_LEN}). "
            "Run more candle history first."
        )

    train_df = df.iloc[:train_end].reset_index(drop=True)

    # --- Build env with custom fee_rate ---
    # Temporarily patch FEE_PER_SIDE in the env module
    import app.rl.crypto_env as _env_mod
    _original_fee = _env_mod.FEE_PER_SIDE
    _env_mod.FEE_PER_SIDE = fee_rate
    try:
        train_env = Monitor(CryptoTradingEnv(train_df, episode_length=TRAIN_EP_LEN, seed=seed))

        ppo_kwargs = {**DEFAULT_PPO_KWARGS,
                      "learning_rate": learning_rate,
                      "n_steps": n_steps,
                      "batch_size": batch_size,
                      "n_epochs": n_epochs,
                      "gamma": gamma,
                      "seed": seed}

        tb_log_name = f"ppo_{symbol}_{timeframe}" + (f"_job{job_id}" if job_id else "")
        TB_LOGS_DIR.mkdir(parents=True, exist_ok=True)
        model = PPO("MlpPolicy", train_env, tensorboard_log=str(TB_LOGS_DIR), **ppo_kwargs)

        cb = _make_progress_callback(total_steps, on_progress)
        model.learn(total_timesteps=total_steps, callback=cb, tb_log_name=tb_log_name)

        if on_progress:
            on_progress(total_steps, total_steps)

        # --- Walk-forward eval ---
        eval_results = _walk_forward_eval(
            df, model,
            eval_start_idx=train_end,
            n_windows=eval_windows,
            ep_len=EVAL_EP_LEN,
        )
    finally:
        _env_mod.FEE_PER_SIDE = _original_fee

    # --- Verdict ---
    ppo_rets = [r["ppo"]["log_ret"] for r in eval_results]
    bnh_rets = [r["bnh"]["log_ret"] for r in eval_results]
    wins = sum(1 for r in eval_results if r["beats_bnh"])
    win_rate = wins / len(eval_results) if eval_results else 0.0
    avg_ppo  = float(np.mean(ppo_rets)) if ppo_rets else 0.0
    avg_bnh  = float(np.mean(bnh_rets)) if bnh_rets else 0.0
    avg_edge = avg_ppo - avg_bnh

    if win_rate >= 0.75 and avg_edge > 0:
        verdict = "PASS"
    elif win_rate >= 0.5 and avg_edge > 0:
        verdict = "MARGINAL"
    else:
        verdict = "FAIL"

    # --- Save candidate model ---
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = f"_candidate_{job_id}" if job_id is not None else "_candidate"
    model_path = MODELS_DIR / f"ppo_{symbol}_{timeframe}{suffix}"
    model.save(str(model_path))

    return {
        "symbol":       symbol,
        "timeframe":    timeframe,
        "total_steps":  total_steps,
        "eval_windows": eval_windows,
        "n_total":      n_total,
        "n_train":      train_end,
        "fee_rate":     fee_rate,
        "walk_forward": eval_results,
        "verdict":      verdict,
        "win_rate":     round(win_rate, 4),
        "avg_ppo_pct":  round(math.exp(avg_ppo) - 1, 6),
        "avg_bnh_pct":  round(math.exp(avg_bnh) - 1, 6),
        "avg_edge":     round(avg_edge, 6),
        "model_path":   str(model_path) + ".zip",
    }


def deploy_candidate_model(symbol: str, timeframe: str, job_id: int) -> str:
    """Copy candidate model to active path and clear ppo_strategy model cache.

    Returns the active model path.
    """
    import shutil
    from app.strategy.ppo_strategy import _model_cache

    candidate = MODELS_DIR / f"ppo_{symbol}_{timeframe}_candidate_{job_id}.zip"
    if not candidate.exists():
        raise FileNotFoundError(f"Candidate model not found: {candidate}")

    active = MODELS_DIR / f"ppo_{symbol}_{timeframe}.zip"
    shutil.copy2(str(candidate), str(active))

    # Clear model cache so next inference reloads from disk
    cache_key = f"{symbol}_{timeframe}"
    _model_cache.pop(cache_key, None)

    return str(active)
