"""V3 Crypto Feature Engineering — Phase 4 + extended set + new indicators.

Computes features from OHLCV candle data using pandas.

Normalization strategy (see planning/features-roadmap-worksheet.md):
  Type ①  Bounded by definition (log_ret, ratios)  → clip only
  Type ②  Heavy-tailed (volume, trades)             → log1p → rolling_zscore(w=50)
  Type ③  Regime-dependent (atr, rv, hl_spread)    → rolling_zscore(w=50)
  Type ④  Extreme outlier (liquidity proxy)         → log1p → robust_zscore(w=100)

All rolling stats use shift(1) to prevent look-ahead bias.

Required DataFrame columns:
  open_time, open, high, low, close, volume,
  quote_asset_volume, number_of_trades,
  taker_buy_base_volume, taker_buy_quote_volume
"""

import numpy as np
import pandas as pd

FEATURE_SET = "v3"
MIN_VALID_ROWS = 120  # warm-up rows before features are reliable

# Rolling windows
_Z_WINDOW = 50
_ROBUST_Z_WINDOW = 100
_ATR_PERIOD = 14
_RV_PERIOD = 20
_RSI_PERIOD = 14
_SMA_SHORT = 20
_SMA_LONG = 60
_BB_PERIOD = 20
_ADX_PERIOD = 14
_VWAP_PERIOD = 20
_MACD_FAST = 12
_MACD_SLOW = 26
_MACD_SIGNAL = 9

# Clipping constants
_CLIP_Z = 4.0      # clip all z-scored features at ±4σ
_CLIP_RET1 = 0.20  # ±20% per bar
_CLIP_RET5 = 0.40
_CLIP_RET20 = 0.80


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------

def rolling_zscore(s: pd.Series, w: int) -> pd.Series:
    """Rolling z-score with look-ahead prevention.

    Mean and std are computed from the PREVIOUS w bars (shift(1)),
    so the current bar is never included in its own normalisation.
    """
    shifted = s.shift(1)
    mu = shifted.rolling(window=w, min_periods=w // 2).mean()
    sigma = shifted.rolling(window=w, min_periods=w // 2).std()
    return ((s - mu) / sigma.replace(0, np.nan)).clip(-_CLIP_Z, _CLIP_Z)


def robust_zscore(s: pd.Series, w: int) -> pd.Series:
    """Robust rolling z-score (median / IQR) with look-ahead prevention.

    Uses median and IQR of the previous w bars.
    More resistant to extreme outliers than standard z-score.
    """
    shifted = s.shift(1)
    roll = shifted.rolling(window=w, min_periods=w // 2)
    med = roll.median()
    iqr = roll.quantile(0.75) - roll.quantile(0.25)
    scale = (iqr / 1.35).replace(0, np.nan)  # 1.35 maps IQR → σ-equivalent
    return ((s - med) / scale).clip(-_CLIP_Z, _CLIP_Z)


# ---------------------------------------------------------------------------
# Low-level indicator helpers
# ---------------------------------------------------------------------------

def _true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["close"].shift(1)
    return pd.concat(
        [
            df["high"] - df["low"],
            (df["high"] - prev_close).abs(),
            (df["low"] - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_crypto_features(df: pd.DataFrame) -> pd.DataFrame:
    """Compute V1 feature set for a candle DataFrame.

    Parameters
    ----------
    df:
        DataFrame sorted by open_time ascending with required OHLCV columns.
        Will be sorted internally if not already.

    Returns
    -------
    DataFrame with original columns plus all feature columns.
    Rows with insufficient history will have NaN in feature columns.
    """
    df = df.copy().sort_values("open_time").reset_index(drop=True)

    # ── Time features (open_time is UTC milliseconds) ─────────────────────
    ts_sec = df["open_time"] / 1000
    hour = (ts_sec // 3600 % 24).astype(float)
    dow  = (ts_sec // 86400 % 7).astype(float)   # 0=Thu (unix epoch), wraps weekly
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["dow_sin"]  = np.sin(2 * np.pi * dow  / 7)
    df["dow_cos"]  = np.cos(2 * np.pi * dow  / 7)
    df["is_asia_session"] = ((hour >= 0) & (hour < 8)).astype(np.float32)
    df["is_us_session"]   = ((hour >= 13) & (hour < 22)).astype(np.float32)

    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)
    trades = df["number_of_trades"].astype(float)
    quote_vol = df["quote_asset_volume"].astype(float)
    taker_base = df["taker_buy_base_volume"].astype(float)

    safe_close = close.replace(0, np.nan)
    safe_vol = volume.replace(0, np.nan)
    safe_trades = trades.replace(0, np.nan)

    # ── Type ①: Log returns (bounded, no normalisation needed) ───────────
    df["log_ret_1"] = np.log(close / close.shift(1)).clip(-_CLIP_RET1, _CLIP_RET1)
    df["log_ret_5"] = np.log(close / close.shift(5)).clip(-_CLIP_RET5, _CLIP_RET5)
    df["log_ret_20"] = np.log(close / close.shift(20)).clip(-_CLIP_RET20, _CLIP_RET20)

    # ── Type ①: Taker flow ratios (bounded [0,1] and [-1,1]) ─────────────
    df["taker_ratio"] = (taker_base / safe_vol).clip(0.0, 1.0)
    df["flow_imbalance"] = (2.0 * df["taker_ratio"] - 1.0)  # [-1, 1]

    # ── Type ①: High-low spread normalised by close ───────────────────────
    df["hl_spread"] = ((high - low) / safe_close).clip(0.0, 0.5)

    # ── Type ③: ATR(14) / close → rolling z-score ────────────────────────
    tr = _true_range(df)
    atr_14 = tr.ewm(span=_ATR_PERIOD, min_periods=_ATR_PERIOD, adjust=False).mean()
    df["atr_14_norm"] = (atr_14 / safe_close).clip(0.0, 0.10)
    df["atr_14_norm_z"] = rolling_zscore(df["atr_14_norm"], w=_Z_WINDOW)

    # ── Type ③: Realised volatility (20-period std of log_ret_1) ─────────
    df["rv_20"] = df["log_ret_1"].rolling(window=_RV_PERIOD, min_periods=_RV_PERIOD // 2).std()
    df["rv_20_z"] = rolling_zscore(df["rv_20"], w=_Z_WINDOW)

    # ── Type ③: HL spread z-score ─────────────────────────────────────────
    df["hl_spread_z"] = rolling_zscore(df["hl_spread"], w=_Z_WINDOW)

    # ── Type ②: Volume → log1p → rolling z-score ─────────────────────────
    log_vol = np.log1p(volume)
    df["log_vol"] = log_vol
    df["log_vol_z"] = rolling_zscore(log_vol, w=_Z_WINDOW)

    # ── Type ②: Trades → log1p → rolling z-score ─────────────────────────
    log_trades = np.log1p(trades)
    df["log_trades"] = log_trades
    df["log_trades_z"] = rolling_zscore(log_trades, w=_Z_WINDOW)

    # ── Type ②: Avg quote per trade → log1p → rolling z-score ───────────
    log_avg_quote = np.log1p(quote_vol / safe_trades)
    df["avg_quote_per_trade_log"] = log_avg_quote
    df["avg_quote_per_trade_z"] = rolling_zscore(log_avg_quote, w=_Z_WINDOW)

    # ── Type ④: Liquidity proxy (quote_vol / hl_spread) → robust z-score ─
    safe_hl = df["hl_spread"].replace(0, np.nan)
    liq_proxy = np.log1p(quote_vol / safe_hl)
    df["liquidity_proxy_log"] = liq_proxy
    df["liquidity_proxy_z"] = robust_zscore(liq_proxy, w=_ROBUST_Z_WINDOW)

    # ── Extended returns ──────────────────────────────────────────────────
    df["log_ret_3"] = np.log(close / close.shift(3)).clip(-0.30, 0.30)
    df["log_ret_10"] = np.log(close / close.shift(10)).clip(-0.60, 0.60)

    # ── Trend: price distance from SMA ───────────────────────────────────
    sma20 = close.rolling(window=_SMA_SHORT, min_periods=_SMA_SHORT // 2).mean()
    sma60 = close.rolling(window=_SMA_LONG, min_periods=_SMA_LONG // 2).mean()
    df["dist_sma_20"] = ((close - sma20) / safe_close).clip(-0.10, 0.10)
    df["dist_sma_60"] = ((close - sma60) / safe_close).clip(-0.20, 0.20)

    # ── RSI(14) normalised to [-1, 1] ─────────────────────────────────────
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = (-delta).clip(lower=0)
    avg_gain = gain.ewm(span=_RSI_PERIOD, min_periods=_RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(span=_RSI_PERIOD, min_periods=_RSI_PERIOD, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    df["rsi_14"] = (rsi / 50.0 - 1.0).clip(-1.0, 1.0)  # normalise [0,100]→[-1,1]

    # ── K-bar pattern features (Type ①: bounded) ─────────────────────────
    candle_range = (high - low).replace(0, np.nan)
    body = (df["close"].astype(float) - df["open"].astype(float)).abs()
    df["body_ratio"] = (body / candle_range).clip(0.0, 1.0)
    df["close_location"] = ((close - low) / candle_range).clip(0.0, 1.0)
    upper_wick = high - df[["close", "open"]].astype(float).max(axis=1)
    lower_wick = df[["close", "open"]].astype(float).min(axis=1) - low
    df["upper_wick_ratio"] = (upper_wick / candle_range).clip(0.0, 1.0)
    df["lower_wick_ratio"] = (lower_wick / candle_range).clip(0.0, 1.0)

    # ── Bollinger Bands (Type ①: bounded) ────────────────────────────────
    bb_sma = close.rolling(window=_BB_PERIOD, min_periods=_BB_PERIOD // 2).mean()
    bb_std = close.rolling(window=_BB_PERIOD, min_periods=_BB_PERIOD // 2).std()
    bb_upper = bb_sma + 2 * bb_std
    bb_lower = bb_sma - 2 * bb_std
    bb_width = (bb_upper - bb_lower).replace(0, np.nan)
    df["bb_pct"] = ((close - bb_lower) / bb_width).clip(0.0, 1.0)
    df["bb_width_norm"] = (bb_width / safe_close).clip(0.0, 0.20)

    # ── VWAP distance (Type ①: bounded) ──────────────────────────────────
    # Rolling VWAP over _VWAP_PERIOD bars using typical price × volume
    typical = (high + low + close) / 3.0
    cum_tp_vol = (typical * volume).rolling(window=_VWAP_PERIOD, min_periods=_VWAP_PERIOD // 2).sum()
    cum_vol    = volume.rolling(window=_VWAP_PERIOD, min_periods=_VWAP_PERIOD // 2).sum()
    vwap = cum_tp_vol / cum_vol.replace(0, np.nan)
    df["dist_vwap_20"] = ((close - vwap) / safe_close).clip(-0.10, 0.10)

    # ── ADX(14) normalised to [0, 1] (Type ①: bounded) ───────────────────
    prev_high  = high.shift(1)
    prev_low   = low.shift(1)
    dm_plus  = (high - prev_high).clip(lower=0)
    dm_minus = (prev_low - low).clip(lower=0)
    # Zero out where the other direction is larger
    dm_plus  = dm_plus.where(dm_plus > dm_minus, 0.0)
    dm_minus = dm_minus.where(dm_minus > dm_plus, 0.0)
    atr_adx = tr.ewm(span=_ADX_PERIOD, min_periods=_ADX_PERIOD, adjust=False).mean()
    safe_atr = atr_adx.replace(0, np.nan)
    di_plus  = 100 * dm_plus.ewm(span=_ADX_PERIOD, min_periods=_ADX_PERIOD, adjust=False).mean() / safe_atr
    di_minus = 100 * dm_minus.ewm(span=_ADX_PERIOD, min_periods=_ADX_PERIOD, adjust=False).mean() / safe_atr
    dx = (100 * (di_plus - di_minus).abs() / (di_plus + di_minus).replace(0, np.nan))
    adx = dx.ewm(span=_ADX_PERIOD, min_periods=_ADX_PERIOD, adjust=False).mean()
    df["adx_14"] = (adx / 100.0).clip(0.0, 1.0)  # normalise [0,100]→[0,1]

    # ── Cumulative taker flow imbalance (Type ①: bounded) ─────────────────
    taker_sell_base = (volume - taker_base).clip(lower=0)
    net_flow = taker_base - taker_sell_base  # positive = buy pressure
    safe_roll_vol_5  = volume.rolling(window=5,  min_periods=3).sum().replace(0, np.nan)
    safe_roll_vol_20 = volume.rolling(window=20, min_periods=10).sum().replace(0, np.nan)
    df["flow_imbalance_5"]  = (net_flow.rolling(window=5,  min_periods=3).sum()  / safe_roll_vol_5).clip(-1.0, 1.0)
    df["flow_imbalance_20"] = (net_flow.rolling(window=20, min_periods=10).sum() / safe_roll_vol_20).clip(-1.0, 1.0)

    # ── Consecutive bar streak (Type ①: bounded) ──────────────────────────
    direction = np.sign(close - df["open"].astype(float))  # +1 / 0 / -1
    streak = pd.array([0.0] * len(df), dtype=float)
    streak_vals = [0.0]
    for i in range(1, len(direction)):
        d = float(direction.iloc[i])
        prev = streak_vals[-1]
        if d > 0:
            streak_vals.append(max(prev, 0) + 1)
        elif d < 0:
            streak_vals.append(min(prev, 0) - 1)
        else:
            streak_vals.append(0)
    df["streak"] = (pd.Series(streak_vals, index=df.index) / 10.0).clip(-1.0, 1.0)

    # ── MACD histogram normalised by ATR (Type ③: z-scored) ──────────────
    ema_fast   = close.ewm(span=_MACD_FAST,   min_periods=_MACD_FAST,   adjust=False).mean()
    ema_slow   = close.ewm(span=_MACD_SLOW,   min_periods=_MACD_SLOW,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    macd_sig   = macd_line.ewm(span=_MACD_SIGNAL, min_periods=_MACD_SIGNAL, adjust=False).mean()
    macd_hist  = (macd_line - macd_sig) / safe_close.replace(0, np.nan)
    df["macd_hist_norm"] = macd_hist.clip(-0.02, 0.02)

    return df


def get_feature_columns() -> list:
    """Return the ordered list of model-input feature columns (V3).

    Removed:
      - dist_sma_20: r=+0.92 with log_ret_10 (redundant)
      - body_ratio:  IC t=+0.23 (no predictive power)

    Ablation test (2026-03-29): removing hl_spread / log_vol_z / lower_wick_ratio /
    is_asia_session / is_us_session degraded performance (FAIL→MARGINAL),
    so all 25 V2 features are retained.

    V3 additions (2026-03-30): bb_pct, bb_width_norm, dist_vwap_20, adx_14,
    flow_imbalance_5, flow_imbalance_20, streak, macd_hist_norm
    """
    return [
        # Type ①: returns (bounded)
        "log_ret_1", "log_ret_3", "log_ret_5", "log_ret_10", "log_ret_20",
        # Type ①: order flow & spread
        "flow_imbalance",
        "hl_spread",
        # Type ①: trend (SMA distance — long window only)
        "dist_sma_60",
        # Type ①: momentum oscillator
        "rsi_14",
        # Type ①: K-bar patterns
        "close_location", "upper_wick_ratio", "lower_wick_ratio",
        # Type ③: volatility → z-scored
        "atr_14_norm_z", "rv_20_z", "hl_spread_z",
        # Type ②: volume → log1p → z-scored
        "log_vol_z", "log_trades_z", "avg_quote_per_trade_z",
        # Type ④: liquidity → log1p → robust z-scored
        "liquidity_proxy_z",
        # Time features
        "hour_sin", "hour_cos",
        "dow_sin",  "dow_cos",
        "is_asia_session", "is_us_session",
        # V3: Bollinger Bands
        "bb_pct", "bb_width_norm",
        # V3: VWAP distance
        "dist_vwap_20",
        # V3: ADX trend strength
        "adx_14",
        # V3: cumulative taker flow
        "flow_imbalance_5", "flow_imbalance_20",
        # V3: consecutive bar streak
        "streak",
        # V3: MACD histogram
        "macd_hist_norm",
    ]
