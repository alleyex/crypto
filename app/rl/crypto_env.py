"""Gymnasium-compatible crypto trading environment — Phase 10.

Uses V2 features from crypto_features.py as the observation space.

Observation space  (Box float32, shape=(N_FEAT + 3 + 5,)):
  [0 : N_FEAT]      normalised V2 features  (currently 15)
  [N_FEAT]          current position  –1=short  0=flat  +1=long
  [N_FEAT+1]        unrealised log-return since entry, clipped ±0.10
  [N_FEAT+2]        bars held in current position / episode_length  [0, 1]
  [N_FEAT+3 : N_FEAT+8]  last 5 bar log-returns (oldest → newest)

Action space  (Discrete 3):
  0  →  flat   (close open position)
  1  →  long
  2  →  short

Reward:
  decision_interval = how often the agent may change position.
  reward_horizon    = how many future bars the reward looks ahead.

  On each decision bar:
    reward        = position * sum(log_ret[t..t+reward_horizon-1]) − fee
    fee           = |Δposition| * FEE_PER_SIDE   (charged once on change)
                    long→short or short→long costs 2× fee (close + open)
    holding_bonus = holding_bonus_rate * reward_horizon if position == 1 else 0

  On hold bars (between decision bars):
    position is carried forward and reward is 0.

Episode:
  Runs for `episode_length` steps (in bars).
  Effective decisions = episode_length // decision_interval.
  Training: random start index each reset.
  Eval (deterministic=True): sequential start from 0.
"""

from __future__ import annotations

import math
from collections import deque
import numpy as np
import pandas as pd
import gymnasium as gym
from gymnasium import spaces
from typing import Any

from app.features.crypto_features import get_feature_columns

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
FEE_PER_SIDE  = 0.0005   # Binance futures taker fee (0.05 %)
UPNL_CLIP     = 0.10     # clip unrealised PnL at ±10 %
DEFAULT_EP_LEN = 1440    # 1 day of 1-minute candles

_FEAT_COLS = get_feature_columns()
N_FEAT     = len(_FEAT_COLS)
N_RECENT_RETS = 5         # number of recent log-returns appended to obs
OBS_DIM    = N_FEAT + 3 + N_RECENT_RETS  # features + position + upnl + bars_held_norm + recent_rets

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
        seed: int | None = None,
        frame_stack: int = 1,
        holding_bonus_rate: float = 0.0,
        decision_interval: int = 1,
        reward_horizon: int | None = None,
        action_interval: int | None = None,
    ) -> None:
        super().__init__()

        # Validate required columns
        missing = [c for c in _FEAT_COLS + ["log_ret_1"] if c not in df.columns]
        if missing:
            raise ValueError(f"DataFrame missing columns: {missing}")

        self._df            = df.reset_index(drop=True)
        self._episode_length = episode_length
        self._deterministic = deterministic
        self._frame_stack   = frame_stack

        # Pre-extract numpy arrays for speed
        self._holding_bonus_rate = float(holding_bonus_rate)
        if action_interval is not None:
            # Legacy training jobs stored a single action_interval parameter
            # that controlled both decision cadence and reward aggregation.
            legacy_interval = max(1, int(action_interval))
            if decision_interval == 1:
                decision_interval = legacy_interval
            if reward_horizon is None:
                reward_horizon = legacy_interval
        self._decision_interval = max(1, int(decision_interval))
        self._reward_horizon    = max(1, int(reward_horizon or 1))
        self._feat_arr  = df[_FEAT_COLS].to_numpy(dtype=np.float32)
        self._ret_arr   = df["log_ret_1"].to_numpy(dtype=np.float32)

        # Fill NaN → 0.0 (warm-up rows)
        self._feat_arr = np.nan_to_num(self._feat_arr, nan=0.0)
        self._ret_arr  = np.nan_to_num(self._ret_arr,  nan=0.0)

        self._n_rows    = len(self._feat_arr)
        self._max_start = self._n_rows - self._episode_length - 1

        if self._max_start < 0:
            raise ValueError(
                f"Not enough rows ({self._n_rows}) for episode_length={episode_length}."
            )

        # Gymnasium spaces
        obs_dim_total = OBS_DIM * frame_stack
        self.observation_space = spaces.Box(
            low  = np.full(obs_dim_total, -10.0, dtype=np.float32),
            high = np.full(obs_dim_total,  10.0, dtype=np.float32),
            dtype=np.float32,
        )
        self.action_space = spaces.Discrete(3)

        # Frame buffer (pre-filled with zeros)
        self._frame_buffer: deque = deque(
            [np.zeros(OBS_DIM, dtype=np.float32)] * frame_stack,
            maxlen=frame_stack,
        )

        # Episode state (initialised in reset)
        self._start_idx: int = 0
        self._step_idx:  int = 0
        self._position:  int = 0     # -1 / 0 / +1
        self._entry_ret: float = 0.0  # cumulative log-ret since entry
        self._bars_held: int = 0
        self._bars_since_decision: int = 0
        self._recent_rets: deque = deque([0.0] * N_RECENT_RETS, maxlen=N_RECENT_RETS)

        # RNG
        self._rng = np.random.default_rng(seed)

    # ------------------------------------------------------------------
    # Gymnasium interface
    # ------------------------------------------------------------------

    def reset(
        self,
        *,
        seed: int | None = None,
        options: dict[str, Any] | None = None,
    ) -> tuple[np.ndarray, dict[str, Any]]:
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
        self._bars_since_decision = 0
        for i in range(N_RECENT_RETS):
            self._recent_rets[i] = 0.0

        # Refill frame buffer with zeros before pushing initial obs
        for i in range(self._frame_stack):
            self._frame_buffer[i] = np.zeros(OBS_DIM, dtype=np.float32)

        return self._obs(), {}

    def step(
        self, action: int
    ) -> tuple[np.ndarray, float, bool, bool, dict[str, Any]]:
        idx = self._start_idx + self._step_idx
        decision_bar = (self._bars_since_decision % self._decision_interval) == 0

        fee = 0.0
        if decision_bar:
            old_position = self._position
            new_position = _ACTION_TO_POS[int(action)]
            delta = abs(new_position - old_position)
            fee = delta * FEE_PER_SIDE

            if new_position != old_position:
                self._position = new_position
                self._entry_ret = 0.0
                self._bars_held = 0

        reward = 0.0
        horizon_pnl = 0.0
        if decision_bar:
            horizon_bars = min(self._reward_horizon, self._episode_length - self._step_idx)
            for offset in range(horizon_bars):
                horizon_idx = idx + offset
                if horizon_idx >= len(self._ret_arr):
                    break
                horizon_pnl += self._position * float(self._ret_arr[horizon_idx])
            holding_bonus = self._holding_bonus_rate * horizon_bars if self._position == 1 else 0.0
            reward = horizon_pnl - fee + holding_bonus

        current_log_ret = float(self._ret_arr[idx])
        self._entry_ret += current_log_ret if self._position != 0 else 0.0
        if self._position != 0:
            self._bars_held += 1
        else:
            self._bars_held = 0
        self._recent_rets.append(current_log_ret)
        self._step_idx += 1
        self._bars_since_decision = (self._bars_since_decision + 1) % self._decision_interval

        done      = self._step_idx >= self._episode_length
        truncated = False

        obs  = self._obs() if not done else np.zeros(OBS_DIM * self._frame_stack, dtype=np.float32)
        info = {
            "step":      self._step_idx,
            "position":  self._position,
            "log_ret":   current_log_ret,
            "step_pnl":  horizon_pnl if decision_bar else 0.0,
            "fee":       fee,
            "decision_bar": decision_bar,
        }
        return obs, float(reward), done, truncated, info

    def render(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _obs(self) -> np.ndarray:
        idx    = self._start_idx + self._step_idx
        feats  = self._feat_arr[idx]   # view — concatenate below creates a new array
        upnl   = float(np.clip(self._entry_ret, -UPNL_CLIP, UPNL_CLIP))
        bars_n = self._bars_held / self._episode_length
        state  = np.array(
            [float(self._position), upnl, bars_n], dtype=np.float32
        )
        recent = np.array(self._recent_rets, dtype=np.float32)
        raw_obs = np.concatenate([feats, state, recent])
        self._frame_buffer.append(raw_obs)
        return np.concatenate(list(self._frame_buffer))

# ---------------------------------------------------------------------------
# Factory: load from DB and build env
# ---------------------------------------------------------------------------

def make_env(
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    episode_length: int = DEFAULT_EP_LEN,
    deterministic: bool = False,
    seed: int | None = None,
    frame_stack: int = 1,
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
    numeric_cols = [col for col in cols if col != "open_time"]
    df_raw[numeric_cols] = df_raw[numeric_cols].astype(float)
    df     = build_crypto_features(df_raw).iloc[MIN_VALID_ROWS:].reset_index(drop=True)

    return CryptoTradingEnv(
        df,
        episode_length=episode_length,
        deterministic=deterministic,
        seed=seed,
        frame_stack=frame_stack,
    )
