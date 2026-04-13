"""PPO-based trading strategy — inference only.

Loads the trained PPO model from runtime/models/, builds the latest
observation from recent candles + current position state, and returns
a BUY / SELL / HOLD signal compatible with the existing strategy pipeline.

Position state (bars_held, entry_price) is persisted to a small JSON file
in runtime/ because the DB only tracks qty, not entry bar or unrealised PnL.

PPO environment supports long / flat / short (futures):
  action=0 -> flat
  action=1 -> long
  action=2 -> short

Signals are derived from the desired position versus the current DB position:
  * → more long  (flat→long, short→flat, short→long)  => BUY
  * → more short (long→flat, flat→short, long→short)  => SELL
  no change                                            => HOLD
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.features.crypto_features import (
    MIN_VALID_ROWS,
    build_crypto_features,
    get_feature_columns,
)
from app.risk.risk_config import get_risk_config
from app.rl.crypto_env import FEE_PER_SIDE, N_FEAT, UPNL_CLIP, _ACTION_TO_POS

RUNTIME_DIR  = Path(__file__).resolve().parent.parent.parent / "runtime"
MODELS_DIR   = RUNTIME_DIR / "models"
STATE_DIR    = RUNTIME_DIR / "ppo_state"

# Number of recent candles to load for feature computation.
# Must cover more than 1 full day (1440 bars) so that dist_prev_day_high/low
# features have access to at least one prior day of data.
CANDLE_LOOKBACK = 1600   # ~26 hours of 1m bars

_FEAT_COLS = get_feature_columns()

# Signals table insert SQL (matches the shared signal schema)
_INSERT_SIGNAL_SQL = """
INSERT INTO signals (
    symbol, timeframe, strategy_name, signal_type,
    short_ma, long_ma
) VALUES (?, ?, ?, ?, ?, ?);
"""

def _blocked_reason_for_decision(
    current_position: int,
    target_position: int,
) -> str | None:
    if target_position == current_position:
        return "same_position"
    return None

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

def get_runtime_target_position(symbol: str, timeframe: str = "1m") -> int | None:
    """Return the latest PPO target position from runtime state.

    This is used by downstream risk/execution in broker-backed futures mode so
    they can distinguish:
      long -> flat
      long -> short
      short -> flat
      short -> long
    even though the persisted signal remains BUY / SELL / HOLD.
    """
    try:
        state = _load_state(symbol, timeframe)
    except Exception:
        return None
    position = state.get("position")
    if position in (-1, 0, 1):
        return int(position)
    return None

# ---------------------------------------------------------------------------
# Model loader (cached per process, invalidated by model file mtime)
# ---------------------------------------------------------------------------

_model_cache: dict[str, Any] = {}
_model_cache_mtime: dict[str, float] = {}
_model_meta_cache: dict[str, dict[str, Any]] = {}

def _load_model_meta(symbol: str, timeframe: str) -> dict[str, Any]:
    """Load observation metadata saved alongside the model.

    Falls back to legacy defaults (no recent_rets) when no meta file exists,
    so old models continue to work without modification.
    """
    meta_path = MODELS_DIR / f"ppo_{symbol}_{timeframe}.meta.json"
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text())
        except Exception:
            pass
    # Legacy fallback: assume no recent log-return features
    return {
        "n_recent_rets": 0,
        "feat_cols":     _FEAT_COLS,
        "frame_stack":   1,
        "decision_interval": 1,
        "reward_horizon": 1,
        "action_interval": 1,
    }

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
        _model_meta_cache[key] = _load_model_meta(symbol, timeframe)
    return _model_cache[key]

def _get_model_meta(symbol: str, timeframe: str) -> dict[str, Any]:
    key = f"{symbol}_{timeframe}"
    if key not in _model_meta_cache:
        _model_meta_cache[key] = _load_model_meta(symbol, timeframe)
    return _model_meta_cache[key]

# ---------------------------------------------------------------------------
# Observation builder
# ---------------------------------------------------------------------------

def _build_observation(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    state: dict,
    episode_length: int = 1440,
    meta: dict[str, Any] | None = None,
) -> tuple[np.ndarray, float] | None:
    """Return (observation, current_close) using obs structure from model meta.

    The meta dict controls which feature columns and how many recent log-returns
    are appended, so this function automatically adapts to any trained model
    without code changes.  When meta is None it falls back to legacy behaviour
    (no recent_rets).
    """
    if meta is None:
        meta = {"n_recent_rets": 0, "feat_cols": _FEAT_COLS}

    feat_cols     = meta.get("feat_cols") or _FEAT_COLS
    n_recent_rets = int(meta.get("n_recent_rets") or 0)

    rows = connection.execute(
        """
        SELECT open_time, open, high, low, close, volume,
               quote_asset_volume, number_of_trades,
               taker_buy_base_volume, taker_buy_quote_volume
        FROM futures_candles
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

    # Load aggtrade microstructure data for the same window (1m only)
    min_ts = int(df["open_time"].min())
    max_ts = int(df["open_time"].max())

    aggtrade_df = None
    if timeframe == "1m":
        at_cols = ["timestamp_ms", "trade_count",
                   "qty_total", "qty_taker_buy", "qty_taker_sell",
                   "quote_total", "quote_taker_buy", "quote_taker_sell",
                   "vwap", "coverage_ratio"]
        try:
            at_rows = connection.execute(
                """
                SELECT timestamp_ms, trade_count,
                       qty_total, qty_taker_buy, qty_taker_sell,
                       quote_total, quote_taker_buy, quote_taker_sell,
                       vwap, coverage_ratio
                FROM futures_aggtrade_minutes
                WHERE symbol = ? AND timestamp_ms BETWEEN ? AND ?
                ORDER BY timestamp_ms ASC
                """,
                (symbol, min_ts, max_ts),
            ).fetchall()
            if at_rows:
                aggtrade_df = pd.DataFrame(at_rows, columns=at_cols)
                for c in at_cols:
                    aggtrade_df[c] = pd.to_numeric(aggtrade_df[c], errors="coerce")
        except Exception:
            pass  # aggtrade table might not exist; fall back to neutral defaults

    # Load futures premium data for the same window
    premium_df = None
    try:
        pm_rows = connection.execute(
            """
            SELECT timestamp_ms, last_funding_rate, mark_index_basis_pct
            FROM futures_premium_metrics
            WHERE symbol = ? AND timestamp_ms BETWEEN ? AND ?
            ORDER BY timestamp_ms ASC
            """,
            (symbol, min_ts, max_ts),
        ).fetchall()
        if pm_rows:
            pm_cols = ["timestamp_ms", "last_funding_rate", "mark_index_basis_pct"]
            premium_df = pd.DataFrame(pm_rows, columns=pm_cols)
            for c in pm_cols:
                premium_df[c] = pd.to_numeric(premium_df[c], errors="coerce")
    except Exception:
        pass  # table might not exist; fall back to neutral defaults (0.0)

    df = build_crypto_features(df, aggtrade_df=aggtrade_df, premium_df=premium_df)

    # Take the last row (most recent bar) using the model's feature columns
    latest = df[feat_cols].iloc[-1].to_numpy(dtype=np.float32)
    np.nan_to_num(latest, nan=0.0, copy=False)

    # Position state
    position    = int(state.get("position", 0))
    entry_price = state.get("entry_price")
    bars_held   = int(state.get("bars_held", 0))

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

    parts = [latest, state_vec]

    # Append recent log-returns if the model was trained with them
    if n_recent_rets > 0 and "log_ret_1" in df.columns:
        recent = df["log_ret_1"].values[-n_recent_rets:]
        if len(recent) < n_recent_rets:
            recent = np.pad(recent, (n_recent_rets - len(recent), 0))
        parts.append(recent.astype(np.float32))

    return np.concatenate(parts), current_close

# ---------------------------------------------------------------------------
# Current position from DB
# ---------------------------------------------------------------------------

def _get_db_position(connection: DBConnection, symbol: str) -> int:
    """Return +1 (long), -1 (short), or 0 (flat).

    In Binance Futures mode, queries the exchange directly so that the model
    always sees the real position rather than the local DB which may lag or
    compute incorrectly for SHORT positions.  Falls back to local DB on error.
    """
    try:
        from app.core.settings import BINANCE_FUTURES
        from app.execution.runtime import read_configured_execution_backend
        if BINANCE_FUTURES and read_configured_execution_backend() == "binance":
            from app.execution.binance_broker import BinanceBrokerClient
            positions = BinanceBrokerClient().get_positions(symbol=symbol, include_flat=True)
            if positions:
                qty = float(positions[0].get("qty") or 0.0)
            else:
                qty = 0.0
            if qty > 1e-9:
                return 1
            if qty < -1e-9:
                return -1
            return 0
    except Exception:
        pass
    # Fallback: local DB
    try:
        row = connection.execute(
            "SELECT qty FROM positions WHERE symbol = ?", (symbol,)
        ).fetchone()
        if row is None:
            return 0
        qty = float(row[0])
        if qty > 1e-9:
            return 1
        if qty < -1e-9:
            return -1
        return 0
    except Exception:
        return 0

def _get_db_entry_price(connection: DBConnection, symbol: str) -> float | None:
    try:
        row = connection.execute(
            "SELECT avg_price FROM positions WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        price = float(row[0])
        return price if price > 0 else None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Main generate_signal function
# ---------------------------------------------------------------------------

def generate_signal(
    connection: DBConnection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> dict[str, Any] | None:
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

    # If DB disagrees with cached state, trust DB and force immediate re-decision
    if db_position != state.get("position", 0):
        state["position"]          = db_position
        state["bars_held"]         = 0
        state["entry_price"]       = None
        state["bars_since_decision"] = 0  # force re-decision on next bar

    # Respect decision cadence stored in model metadata.
    meta = _get_model_meta(symbol, timeframe)
    action_interval = int(meta.get("decision_interval") or meta.get("action_interval") or 1)
    bsd            = int(state.get("bars_since_decision", 0))

    if action_interval > 1 and bsd % action_interval != 0:
        # Hold bar: keep current position, advance counters, skip model call
        if state.get("position", 0) != 0:
            state["bars_held"] = state.get("bars_held", 0) + 1
        else:
            state["bars_held"] = 0
        state["bars_since_decision"] = (bsd + 1) % action_interval
        _save_state(symbol, timeframe, state)
        return None  # executor sees no new signal → position unchanged

    # Decision bar: advance counter before returning so next call is a hold bar
    state["bars_since_decision"] = (bsd + 1) % action_interval

    # Build observation using the obs structure from the deployed model's meta
    obs_result = _build_observation(connection, symbol, timeframe, state, meta=meta)
    if obs_result is None:
        return None  # not enough candles
    obs, current_close = obs_result

    if state.get("entry_price") in (None, 0) and db_position != 0:
        db_entry_price = _get_db_entry_price(connection, symbol)
        if db_entry_price:
            state["entry_price"] = db_entry_price

    import torch
    obs_tensor = model.policy.obs_to_tensor(obs)[0]
    with torch.no_grad():
        distribution = model.policy.get_distribution(obs_tensor)
        probs = distribution.distribution.probs.squeeze().cpu().numpy()

    action, _ = model.predict(obs, deterministic=True)
    action = int(action)
    model_target_position = _ACTION_TO_POS[action]   # 0, +1, or -1
    current_position = state.get("position", 0)
    target_position = model_target_position

    prob_flat  = float(probs[0]) if len(probs) > 0 else 0.0
    prob_long  = float(probs[1]) if len(probs) > 1 else 0.0
    prob_short = float(probs[2]) if len(probs) > 2 else 0.0

    cfg, _ = get_risk_config(connection, "ppo")
    stop_loss_pct = float(cfg.stop_loss_pct or 0.0)
    risk_override_reason: str | None = None
    entry_price = state.get("entry_price")
    if (
        stop_loss_pct > 0
        and current_position != 0
        and entry_price is not None
        and float(entry_price) > 0
    ):
        stop_return = math.log(current_close / float(entry_price)) * float(current_position)
        if stop_return <= -abs(stop_loss_pct):
            target_position = 0
            risk_override_reason = "stop_loss"

    # Resolve signal type directly from the model's target position.
    # target > current → BUY (need to buy contracts to move in that direction)
    # target < current → SELL (need to sell contracts)
    signal_type  = "HOLD"
    new_position = current_position

    if target_position > current_position:
        signal_type = "BUY"
        new_position = target_position
    elif target_position < current_position:
        signal_type = "SELL"
        new_position = target_position

    # Confidence: probability of the resolved position's action
    if new_position == 1:
        top_prob = prob_long
    elif new_position == -1:
        top_prob = prob_short
    else:
        top_prob = prob_flat

    # Expose probabilities for UI / reporting
    prob_hold = round(prob_flat,  4)
    prob_buy  = round(prob_long,  4)
    prob_sell = round(prob_short, 4)
    blocked_reason = _blocked_reason_for_decision(
        current_position=current_position,
        target_position=target_position,
    )

    # Update state  (current_close already returned by _build_observation)
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

    insert_event(
        connection,
        event_type="ppo_inference",
        status=signal_type.lower(),
        source="strategy",
        message=(
            f"ppo inference for {symbol}/{timeframe}: current={current_position} "
            f"target={target_position} signal={signal_type}"
        ),
        payload={
            "signal_id": int(signal_id),
            "symbol": symbol,
            "timeframe": timeframe,
            "strategy_name": "ppo",
            "signal_type": signal_type,
            "action": action,
            "current_position": current_position,
            "model_target_position": model_target_position,
            "target_position": target_position,
            "new_position": new_position,
            "blocked_reason": blocked_reason,
            "risk_override_reason": risk_override_reason,
            "decision_interval": int(meta.get("decision_interval") or meta.get("action_interval") or 1),
            "reward_horizon": int(meta.get("reward_horizon") or meta.get("action_interval") or 1),
            "stop_loss_pct": stop_loss_pct,
            "confidence": round(top_prob, 4),
            "prob_hold": prob_hold,
            "prob_buy": prob_buy,
            "prob_sell": prob_sell,
        },
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
        "prob_short":    round(prob_short, 4),
        "current_position": current_position,
        "model_target_position": model_target_position,
        "target_position": target_position,
        "blocked_reason": blocked_reason,
        "risk_override_reason": risk_override_reason,
    }
