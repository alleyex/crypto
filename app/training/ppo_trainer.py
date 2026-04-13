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

import hashlib
import json
import math
import pickle
import subprocess
import sys
import warnings
from pathlib import Path
import re
from typing import Any, Callable

warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd

from app.training.ppo_eval import walk_forward_eval

ROOT = Path(__file__).resolve().parent.parent.parent
MODELS_DIR       = ROOT / "runtime" / "models"
TB_LOGS_DIR      = ROOT / "runtime" / "tb_logs"
FEATURE_CACHE_DIR = ROOT / "runtime" / "feature_cache"

# ---------------------------------------------------------------------------
# Feature-cache helpers
# ---------------------------------------------------------------------------

def _feature_cache_paths(symbol: str, timeframe: str) -> tuple[Path, Path]:
    key = f"features_{symbol}_{timeframe}"
    return (
        FEATURE_CACHE_DIR / f"{key}.pkl",
        FEATURE_CACHE_DIR / f"{key}.meta.json",
    )

def _load_feature_cache(
    symbol: str,
    timeframe: str,
    last_candle_ts: int,
    last_aggtrade_ts: int | None,
    feat_cols: list[str],
    last_premium_ts: int | None = None,
) -> pd.DataFrame | None:
    """Return cached feature DataFrame if it is still valid, else None."""
    data_path, meta_path = _feature_cache_paths(symbol, timeframe)
    if not meta_path.exists() or not data_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text())
    except Exception:
        return None

    cols_hash = hashlib.md5(",".join(feat_cols).encode()).hexdigest()
    if (
        meta.get("last_candle_ts") != last_candle_ts
        or meta.get("last_aggtrade_ts") != last_aggtrade_ts
        or meta.get("last_premium_ts") != last_premium_ts
        or meta.get("feat_cols_hash") != cols_hash
    ):
        return None

    try:
        return pickle.loads(data_path.read_bytes())
    except Exception:
        return None

def _save_feature_cache(
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    last_candle_ts: int,
    last_aggtrade_ts: int | None,
    feat_cols: list[str],
    last_premium_ts: int | None = None,
) -> None:
    """Persist feature DataFrame and its validation metadata to disk."""
    FEATURE_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    data_path, meta_path = _feature_cache_paths(symbol, timeframe)
    meta = {
        "last_candle_ts": last_candle_ts,
        "last_aggtrade_ts": last_aggtrade_ts,
        "last_premium_ts": last_premium_ts,
        "feat_cols_hash": hashlib.md5(",".join(feat_cols).encode()).hexdigest(),
    }
    data_path.write_bytes(pickle.dumps(df, protocol=4))
    meta_path.write_text(json.dumps(meta))

# Default PPO hyperparameters (mirrors scripts/train_ppo.py)
DEFAULT_PPO_KWARGS: dict[str, Any] = dict(
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

TB_PORT = 6006

def _ensure_tensorboard_running() -> None:
    """Start TensorBoard in the background if not already running.

    TensorBoard 2.x has a numpy 2.0 incompatibility (uses removed np.string_).
    We launch it via a wrapper script that monkey-patches numpy before import.
    """
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        if s.connect_ex(("localhost", TB_PORT)) == 0:
            return  # already running

    TB_LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Inline wrapper that patches numpy before tensorboard imports
    wrapper = (
        "import numpy as np; "
        "np.string_ = np.bytes_; "
        "np.unicode_ = np.str_; "
        f"import sys; sys.argv = ['tensorboard', '--logdir', {str(TB_LOGS_DIR)!r}, "
        f"'--port', '{TB_PORT}', '--host', '0.0.0.0']; "
        "from tensorboard.main import run_main; run_main()"
    )
    subprocess.Popen(
        [sys.executable, "-c", wrapper],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

TRAIN_FRAC   = 0.70
DEFAULT_TRAIN_EP_LEN = 1440   # 1 day of 1m bars
DEFAULT_EVAL_EP_LEN  = 2880   # 2 days of 1m bars

def resolve_episode_lengths(timeframe: str) -> tuple[int, int]:
    """Return fair episode lengths for the given timeframe.

    All timeframes use a fixed 2-day train / 4-day eval window expressed in
    bars, so slower timeframes (15m, 1h) are not penalised by an artificial
    bar-count minimum that was designed for 5m.

    Examples:
      1m  → train=1440 (1d), eval=2880 (2d)   [special-cased for legacy]
      5m  → train=576  (2d), eval=1152 (4d)
      15m → train=192  (2d), eval=384  (4d)
      1h  → train=48   (2d), eval=96   (4d)
    """
    normalized = str(timeframe or "1m").strip().lower()
    if normalized == "1m":
        return DEFAULT_TRAIN_EP_LEN, DEFAULT_EVAL_EP_LEN

    match = re.fullmatch(r"(\d+)(m|h|d)", normalized)
    if not match:
        return DEFAULT_TRAIN_EP_LEN, DEFAULT_EVAL_EP_LEN

    value = int(match.group(1))
    unit = match.group(2)
    minutes_per_bar = value
    if unit == "h":
        minutes_per_bar *= 60
    elif unit == "d":
        minutes_per_bar *= 1440

    if minutes_per_bar <= 0:
        return DEFAULT_TRAIN_EP_LEN, DEFAULT_EVAL_EP_LEN

    bars_per_day = max(1, 1440 // minutes_per_bar)
    train_ep_len = bars_per_day * 2   # 2 days in bars
    eval_ep_len  = bars_per_day * 4   # 4 days in bars
    return train_ep_len, eval_ep_len

# ---------------------------------------------------------------------------
# SB3 progress callback
# ---------------------------------------------------------------------------

def _make_progress_callback(
    total_steps: int,
    on_progress: Callable[[int, int], None] | None,
):
    """Return a SB3 BaseCallback that fires on_progress(current, total)."""
    try:
        from stable_baselines3.common.callbacks import BaseCallback

        class _ProgressCallback(BaseCallback):
            def __init__(self) -> None:
                super().__init__(verbose=0)

            def _on_step(self) -> bool:
                if on_progress and self.n_calls % 2_048 == 0:
                    on_progress(self.num_timesteps, total_steps)
                return True

        return _ProgressCallback()
    except ImportError:
        return None

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
    gae_lambda: float = 0.95,
    clip_range: float = 0.2,
    ent_coef: float = 0.01,
    seed: int = 42,
    frame_stack: int = 1,
    holding_bonus: float = 0.0,
    decision_interval: int = 1,
    reward_horizon: int = 1,
    train_frac: float = TRAIN_FRAC,
    job_id: int | None = None,
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[str, Any]:
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
    _ensure_tensorboard_running()

    from stable_baselines3 import PPO
    from stable_baselines3.common.monitor import Monitor
    from app.core.db import get_connection
    from app.features.crypto_features import build_crypto_features, MIN_VALID_ROWS, get_feature_columns
    from app.rl.crypto_env import CryptoTradingEnv, FEE_PER_SIDE as _DEFAULT_FEE

    feat_cols = get_feature_columns()

    # --- Probe latest timestamps to check feature cache ---
    conn = get_connection()
    try:
        _row = conn.execute(
            "SELECT MAX(open_time) FROM futures_candles WHERE symbol=? AND timeframe=?",
            (symbol, timeframe),
        ).fetchone()
        last_candle_ts: int = int(_row[0]) if _row and _row[0] else 0

        last_aggtrade_ts: int | None = None
        if timeframe == "1m":
            _at_row = conn.execute(
                "SELECT MAX(timestamp_ms) FROM futures_aggtrade_minutes WHERE symbol=?",
                (symbol,),
            ).fetchone()
            if _at_row and _at_row[0]:
                last_aggtrade_ts = int(_at_row[0])

        last_premium_ts: int | None = None
        try:
            _pm_row = conn.execute(
                "SELECT MAX(timestamp_ms) FROM futures_premium_metrics WHERE symbol=?",
                (symbol,),
            ).fetchone()
            if _pm_row and _pm_row[0]:
                last_premium_ts = int(_pm_row[0])
        except Exception:
            pass  # table may not exist
    finally:
        conn.close()

    if last_candle_ts == 0:
        raise RuntimeError(f"No candle data for {symbol}/{timeframe}")

    # --- Try cache first ---
    df = _load_feature_cache(symbol, timeframe, last_candle_ts, last_aggtrade_ts, feat_cols, last_premium_ts)

    if df is None:
        # Cache miss: full load + feature build
        conn = get_connection()
        try:
            rows = conn.execute(
                """
                SELECT open_time, open, high, low, close, volume,
                       quote_asset_volume, number_of_trades,
                       taker_buy_base_volume, taker_buy_quote_volume
                FROM futures_candles WHERE symbol=? AND timeframe=? ORDER BY open_time ASC
                """,
                (symbol, timeframe),
            ).fetchall()

            aggtrade_rows = []
            if timeframe == "1m" and rows:
                min_ts = int(rows[0][0])
                max_ts = int(rows[-1][0])
                aggtrade_rows = conn.execute(
                    """
                    SELECT timestamp_ms, trade_count,
                           qty_total, qty_taker_buy, qty_taker_sell,
                           quote_total, quote_taker_buy, quote_taker_sell,
                           vwap, coverage_ratio
                    FROM futures_aggtrade_minutes
                    WHERE symbol=? AND timestamp_ms BETWEEN ? AND ?
                    ORDER BY timestamp_ms ASC
                    """,
                    (symbol, min_ts, max_ts),
                ).fetchall()

            premium_rows = []
            if rows:
                min_ts = int(rows[0][0])
                max_ts = int(rows[-1][0])
                try:
                    premium_rows = conn.execute(
                        """
                        SELECT timestamp_ms, last_funding_rate, mark_index_basis_pct
                        FROM futures_premium_metrics
                        WHERE symbol=? AND timestamp_ms BETWEEN ? AND ?
                        ORDER BY timestamp_ms ASC
                        """,
                        (symbol, min_ts, max_ts),
                    ).fetchall()
                except Exception:
                    pass  # table may not exist
        finally:
            conn.close()

        cols = ["open_time", "open", "high", "low", "close", "volume",
                "quote_asset_volume", "number_of_trades",
                "taker_buy_base_volume", "taker_buy_quote_volume"]
        df_raw = pd.DataFrame(rows, columns=cols)
        numeric_cols = [c for c in cols if c != "open_time"]
        df_raw[numeric_cols] = df_raw[numeric_cols].astype(float)

        aggtrade_df = None
        if aggtrade_rows:
            at_cols = ["timestamp_ms", "trade_count",
                       "qty_total", "qty_taker_buy", "qty_taker_sell",
                       "quote_total", "quote_taker_buy", "quote_taker_sell",
                       "vwap", "coverage_ratio"]
            aggtrade_df = pd.DataFrame(aggtrade_rows, columns=at_cols)
            for c in at_cols:
                aggtrade_df[c] = pd.to_numeric(aggtrade_df[c], errors="coerce")

            # Trim candles to aggtrade coverage start so all aggtrade features
            # are real values (no neutral fill from missing data).
            at_start_ts = int(aggtrade_df["timestamp_ms"].iloc[0])
            df_raw = df_raw[df_raw["open_time"] >= at_start_ts].reset_index(drop=True)

        premium_df = None
        if premium_rows:
            pm_cols = ["timestamp_ms", "last_funding_rate", "mark_index_basis_pct"]
            premium_df = pd.DataFrame(premium_rows, columns=pm_cols)
            for c in pm_cols:
                premium_df[c] = pd.to_numeric(premium_df[c], errors="coerce")

        df = build_crypto_features(df_raw, aggtrade_df=aggtrade_df, premium_df=premium_df).iloc[MIN_VALID_ROWS:].reset_index(drop=True)

        # Persist for next training run
        _save_feature_cache(symbol, timeframe, df, last_candle_ts, last_aggtrade_ts, feat_cols, last_premium_ts)

    n_total   = len(df)
    train_ep_len, eval_ep_len = resolve_episode_lengths(timeframe)
    train_end = int(n_total * train_frac)

    if train_end < train_ep_len:
        raise ValueError(
            f"Not enough training data: {train_end} rows (need {train_ep_len}). "
            "Run more candle history first."
        )

    train_df = df.iloc[:train_end].reset_index(drop=True)

    # --- Build env with custom fee_rate ---
    # Temporarily patch FEE_PER_SIDE in the env module
    import app.rl.crypto_env as _env_mod
    _original_fee = _env_mod.FEE_PER_SIDE
    _env_mod.FEE_PER_SIDE = fee_rate
    try:
        train_env = Monitor(
            CryptoTradingEnv(
                train_df,
                episode_length=train_ep_len,
                seed=seed,
                frame_stack=frame_stack,
                holding_bonus_rate=holding_bonus,
                decision_interval=decision_interval,
                reward_horizon=reward_horizon,
            )
        )

        ppo_kwargs = {**DEFAULT_PPO_KWARGS,
                      "learning_rate": learning_rate,
                      "n_steps": n_steps,
                      "batch_size": batch_size,
                      "n_epochs": n_epochs,
                      "gamma": gamma,
                      "gae_lambda": gae_lambda,
                      "clip_range": clip_range,
                      "ent_coef": ent_coef,
                      "seed": seed}

        tb_log_name = f"ppo_{symbol}_{timeframe}" + (f"_job{job_id}" if job_id else "")
        try:
            from torch.utils.tensorboard import SummaryWriter as _SW  # noqa: F401
            tb_run_dir = TB_LOGS_DIR / tb_log_name
            tb_run_dir.mkdir(parents=True, exist_ok=True)
            _tb_log = str(TB_LOGS_DIR)
        except Exception:
            _tb_log = None  # tensorboard unavailable — skip logging
        model = PPO("MlpPolicy", train_env, tensorboard_log=_tb_log, **ppo_kwargs)

        cb = _make_progress_callback(total_steps, on_progress)
        model.learn(total_timesteps=total_steps, callback=cb, tb_log_name=tb_log_name)

        if on_progress:
            on_progress(total_steps, total_steps)

        # --- Walk-forward eval ---
        eval_results = walk_forward_eval(
            df, model,
            eval_start_idx=train_end,
            n_windows=eval_windows,
            ep_len=eval_ep_len,
            frame_stack=frame_stack,
            decision_interval=decision_interval,
            reward_horizon=reward_horizon,
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

    total_trades = sum(r["ppo"].get("n_trades", 0) for r in eval_results)
    avg_trades   = total_trades / len(eval_results) if eval_results else 0.0
    if win_rate >= 0.75 and avg_edge > 0 and avg_trades >= 1.0:
        verdict = "PASS"
    elif win_rate >= 0.5 and avg_edge > 0 and avg_trades >= 1.0:
        verdict = "MARGINAL"
    else:
        verdict = "FAIL"

    # --- Save candidate model + observation metadata ---
    from app.rl.crypto_env import N_RECENT_RETS, OBS_DIM
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    suffix = f"_candidate_{job_id}" if job_id is not None else "_candidate"
    model_path = MODELS_DIR / f"ppo_{symbol}_{timeframe}{suffix}"
    model.save(str(model_path))

    model_meta = {
        "obs_dim":       OBS_DIM * frame_stack,
        "n_feat":        len(feat_cols),
        "n_recent_rets": N_RECENT_RETS,
        "feat_cols":     feat_cols,
        "frame_stack":   frame_stack,
        "decision_interval": decision_interval,
        "reward_horizon": reward_horizon,
        "action_interval": decision_interval,
    }
    (model_path.parent / f"{model_path.name}.meta.json").write_text(json.dumps(model_meta))

    return {
        "symbol":       symbol,
        "timeframe":    timeframe,
        "total_steps":  total_steps,
        "eval_windows": eval_windows,
        "n_total":      n_total,
        "n_train":      train_end,
        "fee_rate":     fee_rate,
        "learning_rate": learning_rate,
        "n_steps":      n_steps,
        "batch_size":   batch_size,
        "n_epochs":     n_epochs,
        "gamma":        gamma,
        "gae_lambda":   gae_lambda,
        "clip_range":   clip_range,
        "ent_coef":        ent_coef,
        "frame_stack":     frame_stack,
        "holding_bonus":   holding_bonus,
        "decision_interval": decision_interval,
        "reward_horizon": reward_horizon,
        "action_interval": decision_interval,
        "train_frac":      train_frac,
        "train_ep_len":    train_ep_len,
        "eval_ep_len":  eval_ep_len,
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

    # Copy meta file if present
    candidate_meta = MODELS_DIR / f"ppo_{symbol}_{timeframe}_candidate_{job_id}.meta.json"
    if candidate_meta.exists():
        shutil.copy2(str(candidate_meta), str(MODELS_DIR / f"ppo_{symbol}_{timeframe}.meta.json"))

    # Clear model cache so next inference reloads from disk
    cache_key = f"{symbol}_{timeframe}"
    _model_cache.pop(cache_key, None)

    return str(active)
