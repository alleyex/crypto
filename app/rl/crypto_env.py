"""Gymnasium-compatible crypto trading environment — Phase 9.

Uses V2 features from crypto_features.py as the observation space.

Observation space  (Box float32, shape=(N_FEAT + 3,)):
  [0 : N_FEAT]  normalised V2 features  (currently 19)
  [N_FEAT]      current position  –1=short  0=flat  +1=long
  [N_FEAT+1]    unrealised log-return since entry, clipped ±0.10
  [N_FEAT+2]    bars held in current position / episode_length  [0, 1]

Action space  (Discrete 3):
  0  →  flat   (close open position)
  1  →  long
  2  →  short

Reward:
  step_pnl = position * log_ret_1_t
  fee      = |Δposition| * FEE_PER_SIDE   (charged once on change)
  reward   = step_pnl − fee

Episode:
  Runs for `episode_length` steps.
  Training: random start index each reset.
  Eval (deterministic=True): sequential start from 0.
"""

from __future__ import annotations

import math
import numpy as np
import pandas as pd
import gymnasium as gym
from gymnasium import spaces
from typing import Any, Dict, Optional, Tuple

from app.features.crypto_features import get_feature_columns

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FEE_PER_SIDE  = 0.0004   # Binance taker fee (0.04 %)
UPNL_CLIP     = 0.10     # clip unrealised PnL at ±10 %
DEFAULT_EP_LEN = 1440    # 1 day of 1-minute candles

_FEAT_COLS = get_feature_columns()
N_FEAT     = len(_FEAT_COLS)
OBS_DIM    = N_FEAT + 3   # features + position + upnl + bars_held_norm

# Map discrete action → signed position
_ACTION_TO_POS = {0: 0, 1: 1, 2: -1}


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

class CryptoTradingEnv(gym.Env):
    """Single-asset, long/flat/short trading environment.

    Parameters
    ----------
    df : DataFrame
        Output of ``build_crypto_features()``, sorted by open_time ascending.
        Must contain all columns in ``get_feature_columns()`` plus ``log_ret_1``.
    episode_length : int
        Number of bars per episode.
    deterministic : bool
        If True, each reset starts from the first valid index (for eval).
        If False, start index is random (for training).
    seed : int | None
        RNG seed.
    """

    metadata = {"render_modes": []}

    def __init__(
        self,
        df: pd.DataFrame,
        episode_length: int = DEFAULT_EP_LEN,
        deterministic: bool = False,
        seed: Optional[int] = None,
    ) -> None:
        super().__init__()

        # Validate required columns
        missing = [c for c in _FEAT_COLS + ["log_ret_1"] if c not in df.columns]
        if missing:
            raise ValueError(f"DataFrame missing columns: {missing}")

        self._df            = df.reset_index(drop=True)
        self._episode_length = episode_length
        self._deterministic = deterministic

        # Pre-extract numpy arrays for speed
        self._feat_arr  = df[_FEAT_COLS].to_numpy(dtype=np.float32)
        self._ret_arr   = df["log_ret_1"].to_numpy(dtype=np.float32)

        # Fill NaN → 0.0 (warm-up rows)
        np.nan_to_num(self._feat_arr, nan=0.0, copy=False)
        np.nan_to_num(self._ret_arr,  nan=0.0, copy=False)

        self._n_rows    = len(self._feat_arr)
        self._max_start = self._n_rows - self._episode_length - 1

        if self._max_start < 0:
            raise ValueError(
                f"Not enough rows ({self._n_rows}) for episode_length={episode_length}."
            )

        # Gymnasium spaces
        self.observation_space = spaces.Box(
            low  = np.full(OBS_DIM, -10.0, dtype=np.float32),
            high = np.full(OBS_DIM,  10.0, dtype=np.float32),
            dtype=np.float32,
        )
        self.action_space = spaces.Discrete(3)

        # Episode state (initialised in reset)
        self._start_idx: int = 0
        self._step_idx:  int = 0
        self._position:  int = 0     # -1 / 0 / +1
        self._entry_ret: float = 0.0  # cumulative log-ret since entry
        self._bars_held: int = 0

        # RNG
        self._rng = np.random.default_rng(seed)

    # ------------------------------------------------------------------
    # Gymnasium interface
    # ------------------------------------------------------------------

    def reset(
        self,
        *,
        seed: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Tuple[np.ndarray, Dict[str, Any]]:
        super().reset(seed=seed)
        if seed is not None:
            self._rng = np.random.default_rng(seed)

        if self._deterministic:
            self._start_idx = 0
        else:
            self._start_idx = int(self._rng.integers(0, self._max_start + 1))

        self._step_idx  = 0
        self._position  = 0
        self._entry_ret = 0.0
        self._bars_held = 0

        return self._obs(), {}

    def step(
        self, action: int
    ) -> Tuple[np.ndarray, float, bool, bool, Dict[str, Any]]:
        idx = self._start_idx + self._step_idx

        # --- Execute action ---
        new_position = _ACTION_TO_POS[int(action)]
        delta        = abs(new_position - self._position)
        fee          = delta * FEE_PER_SIDE

        if new_position != self._position:
            self._position  = new_position
            self._entry_ret = 0.0
            self._bars_held = 0

        # --- Step return ---
        log_ret = float(self._ret_arr[idx])
        step_pnl = self._position * log_ret
        reward   = step_pnl - fee

        # Update unrealised PnL and bars held
        self._entry_ret += log_ret if self._position != 0 else 0.0
        if self._position != 0:
            self._bars_held += 1
        else:
            self._bars_held = 0

        # Advance
        self._step_idx += 1
        done      = self._step_idx >= self._episode_length
        truncated = False

        obs  = self._obs() if not done else np.zeros(OBS_DIM, dtype=np.float32)
        info = {
            "step":      self._step_idx,
            "position":  self._position,
            "log_ret":   log_ret,
            "step_pnl":  step_pnl,
            "fee":       fee,
        }
        return obs, float(reward), done, truncated, info

    def render(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _obs(self) -> np.ndarray:
        idx    = self._start_idx + self._step_idx
        feats  = self._feat_arr[idx].copy()
        upnl   = float(np.clip(self._entry_ret, -UPNL_CLIP, UPNL_CLIP))
        bars_n = self._bars_held / self._episode_length
        state  = np.array(
            [float(self._position), upnl, bars_n], dtype=np.float32
        )
        return np.concatenate([feats, state])


# ---------------------------------------------------------------------------
# Factory: load from DB and build env
# ---------------------------------------------------------------------------

def make_env(
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    episode_length: int = DEFAULT_EP_LEN,
    deterministic: bool = False,
    seed: Optional[int] = None,
) -> CryptoTradingEnv:
    """Convenience factory: load candles from DB, build features, return env."""
    from app.core.db import get_connection
    from app.features.crypto_features import build_crypto_features, MIN_VALID_ROWS

    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT open_time, open, high, low, close, volume,
                   quote_asset_volume, number_of_trades,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM candles
            WHERE symbol = ? AND timeframe = ?
            ORDER BY open_time ASC
            """,
            (symbol, timeframe),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        raise RuntimeError(f"No candle data for {symbol}/{timeframe}")

    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume",
    ]
    df_raw = pd.DataFrame(rows, columns=cols)
    df     = build_crypto_features(df_raw).iloc[MIN_VALID_ROWS:].reset_index(drop=True)

    return CryptoTradingEnv(
        df,
        episode_length=episode_length,
        deterministic=deterministic,
        seed=seed,
    )
