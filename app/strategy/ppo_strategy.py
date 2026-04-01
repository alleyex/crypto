"""PPO-based trading strategy — inference only.

Loads the trained PPO model from runtime/models/, builds the latest
observation from recent candles + current position state, and returns
a BUY / SELL / HOLD signal compatible with the existing strategy pipeline.

Position state (bars_held, entry_price) is persisted to a small JSON file
in runtime/ because the DB only tracks qty, not entry bar or unrealised PnL.

The current PPO environment is long/flat only:
  action=0 -> flat
  action=1 -> long

Signals are derived from the desired position versus the current DB position:
  flat -> long  => BUY
  long -> flat  => SELL
  no change     => HOLD
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.features.crypto_features import (
    MIN_VALID_ROWS,
    build_crypto_features,
    get_feature_columns,
)
from app.rl.crypto_env import FEE_PER_SIDE, N_FEAT, UPNL_CLIP, _ACTION_TO_POS

RUNTIME_DIR  = Path(__file__).resolve().parent.parent.parent / "runtime"
MODELS_DIR   = RUNTIME_DIR / "models"
STATE_DIR    = RUNTIME_DIR / "ppo_state"

# Number of recent candles to load for feature computation
CANDLE_LOOKBACK = MIN_VALID_ROWS + 50   # 170 bars

_FEAT_COLS = get_feature_columns()

# Signals table insert SQL (matches existing schema in ma_cross.py)
_INSERT_SIGNAL_SQL = """
INSERT INTO signals (
    symbol, timeframe, strategy_name, signal_type,
    short_ma, long_ma
) VALUES (?, ?, ?, ?, ?, ?);
"""

_OPEN_LONG_THRESHOLD = 0.55
_CLOSE_LONG_THRESHOLD = 0.45


# ---------------------------------------------------------------------------
# State persistence
# ---------------------------------------------------------------------------

def _state_path(symbol: str, timeframe: str) -> Path:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    return STATE_DIR / f"{symbol.lower()}_{timeframe.lower()}.json"


def _load_state(symbol: str, timeframe: str) -> dict:
    p = _state_path(symbol, timeframe)
    if p.exists():
        try:
            return json.loads(p.read_text())
        except Exception:
            pass
    return {"position": 0, "entry_price": None, "bars_held": 0}


def _save_state(symbol: str, timeframe: str, state: dict) -> None:
    _state_path(symbol, timeframe).write_text(json.dumps(state))


# ---------------------------------------------------------------------------
# Model loader (cached per process, invalidated by model file mtime)
# ---------------------------------------------------------------------------

_model_cache: Dict[str, Any] = {}
_model_cache_mtime: Dict[str, float] = {}


def _load_model(symbol: str, timeframe: str = "1m"):
    key = f"{symbol}_{timeframe}"
    model_path = MODELS_DIR / f"ppo_{symbol}_{timeframe}.zip"
    if not model_path.exists():
        raise FileNotFoundError(
            f"PPO model not found: {model_path}\n"
            f"Run: python scripts/train_ppo.py --symbol {symbol} --timeframe {timeframe}"
        )

    model_mtime = model_path.stat().st_mtime
    cached_mtime = _model_cache_mtime.get(key)
    if key not in _model_cache or cached_mtime != model_mtime:
        from stable_baselines3 import PPO

        _model_cache[key] = PPO.load(str(model_path))
        _model_cache_mtime[key] = model_mtime
    return _model_cache[key]


# ---------------------------------------------------------------------------
# Observation builder
# ---------------------------------------------------------------------------

def _build_observation(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    state: dict,
    episode_length: int = 1440,
) -> Optional[np.ndarray]:
    """Return the current PPO observation, or None if insufficient data."""
    rows = connection.execute(
        """
        SELECT open_time, open, high, low, close, volume,
               quote_asset_volume, number_of_trades,
               taker_buy_base_volume, taker_buy_quote_volume
        FROM candles
        WHERE symbol = ? AND timeframe = ?
        ORDER BY open_time DESC
        LIMIT ?
        """,
        (symbol, timeframe, CANDLE_LOOKBACK),
    ).fetchall()

    if len(rows) < MIN_VALID_ROWS:
        return None

    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume",
    ]
    # Rows came in DESC order; reverse for chronological
    df = pd.DataFrame(reversed(rows), columns=cols)
    numeric_cols = [col for col in cols if col != "open_time"]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df = build_crypto_features(df)

    # Take the last row (most recent bar)
    latest = df[_FEAT_COLS].iloc[-1].to_numpy(dtype=np.float32)
    np.nan_to_num(latest, nan=0.0, copy=False)

    # Position state
    position   = int(state.get("position", 0))
    entry_price = state.get("entry_price")
    bars_held  = int(state.get("bars_held", 0))

    # Unrealised PnL
    current_close = float(df["close"].iloc[-1])
    if entry_price and entry_price > 0 and position != 0:
        upnl = float(np.clip(
            math.log(current_close / entry_price) * position,
            -UPNL_CLIP, UPNL_CLIP,
        ))
    else:
        upnl = 0.0

    bars_held_norm = min(bars_held / episode_length, 1.0)
    state_vec = np.array([float(position), upnl, bars_held_norm], dtype=np.float32)

    return np.concatenate([latest, state_vec])


# ---------------------------------------------------------------------------
# Current position from DB
# ---------------------------------------------------------------------------

def _get_db_position(connection: DBConnection, symbol: str) -> int:
    """Return +1 (long), -1 (short), or 0 (flat) from positions table."""
    try:
        row = connection.execute(
            "SELECT qty FROM positions WHERE symbol = ?", (symbol,)
        ).fetchone()
        if row is None:
            return 0
        qty = float(row[0])
        if qty > 0:
            return 1
        if qty < 0:
            return -1
        return 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Main generate_signal function
# ---------------------------------------------------------------------------

def generate_signal(
    connection: DBConnection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> Optional[Dict[str, Any]]:
    """Generate a PPO-based signal and persist it to the signals table.

    Returns the signal dict (same shape as other strategies), or None
    if the model / data is not ready.
    """
    import warnings
    warnings.filterwarnings("ignore")

    # Load model
    try:
        model = _load_model(symbol, timeframe)
    except FileNotFoundError as exc:
        return None  # model not trained yet; skip silently

    # Load + sync position state
    state = _load_state(symbol, timeframe)
    db_position = _get_db_position(connection, symbol)

    # If DB disagrees with cached state, trust DB and reset bars_held
    if db_position != state.get("position", 0):
        state["position"]    = db_position
        state["bars_held"]   = 0
        state["entry_price"] = None

    # Build observation
    obs = _build_observation(connection, symbol, timeframe, state)
    if obs is None:
        return None  # not enough candles

    import torch
    obs_tensor = model.policy.obs_to_tensor(obs)[0]
    with torch.no_grad():
        distribution = model.policy.get_distribution(obs_tensor)
        probs = distribution.distribution.probs.squeeze().cpu().numpy()

    action, _ = model.predict(obs, deterministic=True)
    action = int(action)
    target_position = _ACTION_TO_POS[action]
    current_position = state.get("position", 0)

    prob_flat = float(probs[0]) if len(probs) > 0 else 0.0
    prob_long = float(probs[1]) if len(probs) > 1 else 0.0

    signal_type = "HOLD"
    top_prob = prob_flat if target_position == 0 else prob_long

    if current_position == 0 and target_position == 1:
        if top_prob >= _OPEN_LONG_THRESHOLD:
            signal_type = "BUY"
            new_position = 1
        else:
            new_position = 0
    elif current_position == 1 and target_position == 0:
        if top_prob >= _CLOSE_LONG_THRESHOLD:
            signal_type = "SELL"
            new_position = 0
        else:
            new_position = 1
    else:
        new_position = current_position

    # Expose probabilities in legacy signal fields for existing UI/reporting.
    # "buy" means opening/maintaining long; "sell" means flattening the long.
    prob_hold = round(float(prob_flat if current_position == 0 else prob_long), 4)
    prob_buy = round(float(prob_long), 4)
    prob_sell = round(float(prob_flat if current_position == 1 else 0.0), 4)

    # Update state
    current_close_row = connection.execute(
        "SELECT close FROM candles WHERE symbol=? AND timeframe=? ORDER BY open_time DESC LIMIT 1",
        (symbol, timeframe),
    ).fetchone()
    current_close = float(current_close_row[0]) if current_close_row else None

    if new_position != state["position"]:
        # Position changed → record new entry price
        state["entry_price"] = current_close
        state["bars_held"]   = 0
    else:
        state["bars_held"] = state.get("bars_held", 0) + 1

    state["position"] = new_position
    _save_state(symbol, timeframe, state)

    # Persist signal to DB
    # short_ma = prob_buy, long_ma = prob_sell
    signal_id = insert_and_get_rowid(
        connection,
        _INSERT_SIGNAL_SQL,
        (symbol, timeframe, "ppo", signal_type, prob_buy, prob_sell),
    )

    return {
        "id":            signal_id,
        "symbol":        symbol,
        "timeframe":     timeframe,
        "strategy_name": "ppo",
        "signal_type":   signal_type,
        "action":        action,
        "position":      new_position,
        "confidence":    round(top_prob, 4),
        "prob_hold":     prob_hold,
        "prob_buy":      prob_buy,
        "prob_sell":     prob_sell,
    }
