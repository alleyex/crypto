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

Optional order book columns via ``orderbook_df``:
  timestamp_ms, ob_imbalance, spread_pct, mid_price
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
# Feature group sub-functions (all modify df in-place)
# ---------------------------------------------------------------------------

def _add_time_features(df: pd.DataFrame) -> None:
    """Hour/day cyclical encoding and session flags."""
    ts_sec = df["open_time"] / 1000
    hour = (ts_sec // 3600 % 24).astype(float)
    dow  = (ts_sec // 86400 % 7).astype(float)   # 0=Thu (unix epoch), wraps weekly
    df["hour_sin"] = np.sin(2 * np.pi * hour / 24)
    df["hour_cos"] = np.cos(2 * np.pi * hour / 24)
    df["dow_sin"]  = np.sin(2 * np.pi * dow  / 7)
    df["dow_cos"]  = np.cos(2 * np.pi * dow  / 7)
    df["is_asia_session"] = ((hour >= 0) & (hour < 8)).astype(np.float32)
    df["is_us_session"]   = ((hour >= 13) & (hour < 22)).astype(np.float32)

def _add_price_volatility_features(
    df: pd.DataFrame,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    safe_close: pd.Series,
    safe_vol: pd.Series,
    trades: pd.Series,
    safe_trades: pd.Series,
    quote_vol: pd.Series,
    taker_base: pd.Series,
) -> "tuple[pd.Series, pd.Series]":
    """Log returns, taker ratios, spread, ATR, volatility, volume features.

    Returns (tr, atr_14) for use by the trend/momentum group.
    """
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

    return tr, atr_14

def _add_trend_momentum_features(
    df: pd.DataFrame,
    close: pd.Series,
    high: pd.Series,
    low: pd.Series,
    volume: pd.Series,
    safe_close: pd.Series,
    tr: pd.Series,
    atr_14: pd.Series,
) -> None:
    """SMA, RSI, k-bar patterns, BB, VWAP, ADX, MACD, V4/V5 indicators."""
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
    taker_base = df["taker_buy_base_volume"].astype(float)
    taker_sell_base = (volume - taker_base).clip(lower=0)
    net_flow = taker_base - taker_sell_base  # positive = buy pressure
    safe_roll_vol_5  = volume.rolling(window=5,  min_periods=3).sum().replace(0, np.nan)
    safe_roll_vol_20 = volume.rolling(window=20, min_periods=10).sum().replace(0, np.nan)
    df["flow_imbalance_5"]  = (net_flow.rolling(window=5,  min_periods=3).sum()  / safe_roll_vol_5).clip(-1.0, 1.0)
    df["flow_imbalance_20"] = (net_flow.rolling(window=20, min_periods=10).sum() / safe_roll_vol_20).clip(-1.0, 1.0)

    # ── Consecutive bar streak (Type ①: bounded) ──────────────────────────
    direction = np.sign(close - df["open"].astype(float))  # +1 / 0 / -1
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

    # ── V4: Price z-score (mean reversion signal) ─────────────────────────
    df["price_z_20"] = rolling_zscore(close, w=20)
    df["price_z_50"] = rolling_zscore(close, w=50)

    # ── V4: MA-crossover momentum ─────────────────────────────────────────
    sma10 = close.rolling(10, min_periods=5).mean()
    df["momentum_10_20"] = ((sma10 / sma20.replace(0, np.nan)) - 1.0).clip(-0.05, 0.05)
    df["momentum_20_60"] = ((sma20 / sma60.replace(0, np.nan)) - 1.0).clip(-0.10, 0.10)

    # ── V4: ATR ratio — volatility expansion/contraction ──────────────────
    atr_50 = tr.ewm(span=50, min_periods=25, adjust=False).mean()
    df["atr_ratio_14_50"] = (atr_14 / atr_50.replace(0, np.nan)).clip(0.2, 5.0)

    # ── V4: Volume ratio — volume surge indicator ─────────────────────────
    vol_mean_10 = volume.rolling(10, min_periods=5).mean()
    vol_mean_50 = volume.rolling(50, min_periods=25).mean()
    df["vol_ratio_10_50"] = (vol_mean_10 / vol_mean_50.replace(0, np.nan)).clip(0.1, 10.0)

    # ── V4: Breakout / distance from 20-bar range ─────────────────────────
    rolling_high_20 = high.shift(1).rolling(20, min_periods=10).max()
    rolling_low_20  = low.shift(1).rolling(20, min_periods=10).min()
    df["dist_to_high_20"] = (close / rolling_high_20.replace(0, np.nan) - 1.0).clip(-0.10, 0.10)
    df["dist_to_low_20"]  = (close / rolling_low_20.replace(0, np.nan)  - 1.0).clip(-0.10, 0.10)

    # ── V4: Trend strength — (close - sma20) / ATR ─────────────────────────
    df["trend_strength"] = ((close - sma20) / atr_14.replace(0, np.nan)).clip(-5.0, 5.0)

    # ── V4: Prev-day high/low distance (no look-ahead) ────────────────────
    # Compute each calendar day's H/L, shift by 1 day, then map back.
    # Every bar in day D gets the COMPLETE high/low of day D-1 — no intraday
    # future information leaks in.
    open_time_dt = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["_date"] = open_time_dt.dt.date
    daily_high = df.groupby("_date")["high"].max()
    daily_low  = df.groupby("_date")["low"].min()
    prev_day_high = df["_date"].map(daily_high.shift(1))
    prev_day_low  = df["_date"].map(daily_low.shift(1))
    df["dist_prev_day_high"] = (close / prev_day_high.replace(0, np.nan) - 1.0).clip(-0.10, 0.10)
    df["dist_prev_day_low"]  = (close / prev_day_low.replace(0, np.nan)  - 1.0).clip(-0.10, 0.10)
    df.drop(columns=["_date"], inplace=True)

    # ── V5: Stochastic Oscillator %K and %D ───────────────────────────────
    # %K = (close - lowest_low_14) / (highest_high_14 - lowest_low_14), [0,1]
    # %D = SMA3(%K) — signal line
    stoch_low  = low.shift(1).rolling(14, min_periods=7).min()
    stoch_high = high.shift(1).rolling(14, min_periods=7).max()
    stoch_range = (stoch_high - stoch_low).replace(0, np.nan)
    stoch_k = ((close - stoch_low) / stoch_range).clip(0.0, 1.0)
    df["stoch_k"] = stoch_k
    df["stoch_d"] = stoch_k.rolling(3, min_periods=2).mean()

    # ── V5: CCI — Commodity Channel Index ─────────────────────────────────
    # CCI = (typical_price - SMA20) / (0.015 × MeanAbsDev20), clipped [-3, 3]
    typical = (high + low + close) / 3.0
    cci_sma  = typical.rolling(20, min_periods=10).mean()
    cci_mad  = typical.rolling(20, min_periods=10).apply(
        lambda x: np.mean(np.abs(x - x.mean())), raw=True
    )
    df["cci_20"] = ((typical - cci_sma) / (0.015 * cci_mad.replace(0, np.nan))).clip(-3.0, 3.0)

    # ── V5: Volatility-normalised return ──────────────────────────────────
    # ret_z = log_ret_5 / rv_20 — how "significant" the recent move is
    safe_rv = df["rv_20"].replace(0, np.nan)
    df["ret_z_5"] = (np.log(close / close.shift(5)) / (safe_rv * np.sqrt(5))).clip(-3.0, 3.0)

    # ── V5: Multi-timeframe RSI ────────────────────────────────────────────
    # rsi_56  ≈ 1h RSI  (14 × 4 bars/h)
    # rsi_224 ≈ 4h RSI  (14 × 16 bars/4h)
    def _rsi_series(s: pd.Series, period: int) -> pd.Series:
        delta    = s.diff()
        gain     = delta.clip(lower=0)
        loss     = (-delta).clip(lower=0)
        avg_gain = gain.ewm(span=period, min_periods=period, adjust=False).mean()
        avg_loss = loss.ewm(span=period, min_periods=period, adjust=False).mean()
        rs       = avg_gain / avg_loss.replace(0, np.nan)
        return (100.0 - 100.0 / (1.0 + rs)) / 50.0 - 1.0  # normalised to [-1,1]

    df["rsi_56"]  = _rsi_series(close, 56).clip(-1.0, 1.0)
    df["rsi_224"] = _rsi_series(close, 224).clip(-1.0, 1.0)

    # ── V5: Intraday open distance ─────────────────────────────────────────
    # Distance from today's first bar open — captures intraday bias.
    # No look-ahead: we use the open price of the first bar of each UTC day.
    open_time_dt2 = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df["_date2"] = open_time_dt2.dt.date
    day_open = df.groupby("_date2")["open"].first()
    df["dist_day_open"] = (close / df["_date2"].map(day_open).replace(0, np.nan) - 1.0).clip(-0.10, 0.10)
    df.drop(columns=["_date2"], inplace=True)

    # ── V5: Bollinger Band squeeze ─────────────────────────────────────────
    # Ratio of current BB width to its 50-bar rolling mean.
    # < 1 = compression (squeeze); > 1 = expansion.
    bb_width_ma = df["bb_width_norm"].rolling(50, min_periods=25).mean().replace(0, np.nan)
    df["bb_squeeze"] = (df["bb_width_norm"] / bb_width_ma).clip(0.2, 3.0)

def _add_funding_features(df: pd.DataFrame, funding_df: "pd.DataFrame | None") -> None:
    """Funding rate features — requires funding_df with funding_time_ms and funding_rate."""
    if funding_df is None or len(funding_df) == 0:
        return
    fr = funding_df[["funding_time_ms", "funding_rate"]].copy()
    fr = fr.sort_values("funding_time_ms").reset_index(drop=True)
    fr["funding_rate"] = fr["funding_rate"].astype(float)

    # Merge: for each candle, take the most recent funding rate (as-of join)
    candle_ms = df["open_time"].values.astype(np.int64)
    fund_ms   = fr["funding_time_ms"].values.astype(np.int64)
    fund_rate = fr["funding_rate"].values

    # searchsorted gives index of first fund_ms > candle_ms
    idx = np.searchsorted(fund_ms, candle_ms, side="right") - 1
    idx = np.clip(idx, 0, len(fund_rate) - 1)
    fr_values = np.where(idx >= 0, fund_rate[idx], np.nan)

    df["funding_rate"] = fr_values  # raw rate (e.g. 0.0001 = 0.01%)

    # z-score over 20 settlements (~7 days)
    fr_series = pd.Series(df["funding_rate"])
    fr_shifted = fr_series.shift(1)
    fr_mu  = fr_shifted.rolling(20, min_periods=10).mean()
    fr_std = fr_shifted.rolling(20, min_periods=10).std().replace(0, np.nan)
    df["funding_rate_z20"] = ((fr_series - fr_mu) / fr_std).clip(-4.0, 4.0)

    # Cyclical encoding: hours until next 8h settlement (0 to 8)
    hour_utc = ((df["open_time"] / 1000) // 3600 % 24).astype(float)
    hours_to_next = (8.0 - (hour_utc % 8.0)) % 8.0
    df["funding_cos"] = np.cos(2 * np.pi * hours_to_next / 8.0)
    df["funding_sin"] = np.sin(2 * np.pi * hours_to_next / 8.0)

def _add_aggtrade_features(
    df: pd.DataFrame,
    aggtrade_df: "pd.DataFrame | None",
    close: pd.Series,
    safe_close: pd.Series,
) -> None:
    """AggTrade microstructure features.

    Neutral defaults (taker_buy_ratio=0.5, quote_flow_imb=0.0, vwap_dev=0.0)
    are used when aggtrade_df is None or has no valid rows.
    """
    _at_taker = pd.Series(0.5, index=df.index, dtype=np.float32)
    _at_qflow = pd.Series(0.0, index=df.index, dtype=np.float32)
    _at_vwap  = pd.Series(0.0, index=df.index, dtype=np.float32)

    if aggtrade_df is not None and len(aggtrade_df) > 0:
        at = aggtrade_df.copy()
        at["coverage_ratio"] = pd.to_numeric(at["coverage_ratio"], errors="coerce").fillna(0.0)
        at["trade_count"]    = pd.to_numeric(at["trade_count"],    errors="coerce").fillna(0)
        at = at[(at["coverage_ratio"] >= 0.3) & (at["trade_count"] >= 30)]

        if len(at) > 0:
            at = at.sort_values("timestamp_ms").reset_index(drop=True)
            at_ms        = at["timestamp_ms"].values.astype(np.int64)
            at_qty       = at["qty_total"].astype(float).values
            at_buy_qty   = at["qty_taker_buy"].astype(float).values
            at_quote     = at["quote_total"].astype(float).values
            at_buy_quote = at["quote_taker_buy"].astype(float).values
            at_sel_quote = at["quote_taker_sell"].astype(float).values
            at_vwap_arr  = at["vwap"].astype(float).values

            candle_ms = df["open_time"].values.astype(np.int64)
            idx = np.searchsorted(at_ms, candle_ms, side="right") - 1
            valid = idx >= 0

            safe_qty   = np.where(at_qty   > 0, at_qty,   np.nan)
            safe_quote = np.where(at_quote > 0, at_quote, np.nan)

            taker_raw = at_buy_qty / safe_qty                                # [0, 1]
            qflow_raw = (at_buy_quote - at_sel_quote) / safe_quote           # [-1, 1]

            taker_vals = np.full(len(df), 0.5)
            qflow_vals = np.full(len(df), 0.0)
            vwap_vals  = np.full(len(df), np.nan)

            taker_vals[valid] = taker_raw[idx[valid]]
            qflow_vals[valid] = qflow_raw[idx[valid]]
            vwap_vals[valid]  = at_vwap_arr[idx[valid]]

            _at_taker = pd.Series(taker_vals, index=df.index, dtype=np.float32)
            _at_qflow = pd.Series(qflow_vals, index=df.index, dtype=np.float32)
            vwap_s    = pd.Series(vwap_vals,  index=df.index, dtype=np.float32)
            _at_vwap  = ((close - vwap_s) / safe_close).clip(-0.01, 0.01).fillna(0.0).astype(np.float32)

    df["taker_buy_ratio"]   = _at_taker.clip(0.0, 1.0)
    df["quote_flow_imb"]    = _at_qflow.clip(-1.0, 1.0)
    df["taker_buy_ratio_5"] = df["taker_buy_ratio"].rolling(5, min_periods=3).mean().clip(0.0, 1.0).fillna(0.5)
    df["vwap_dev"]          = _at_vwap

def _add_premium_features(df: pd.DataFrame, premium_df: "pd.DataFrame | None") -> None:
    """Futures premium features (funding_rate_z20, basis_z).

    Defaults to 0.0 (neutral) when premium_df is None.
    """
    df["funding_rate_z20"] = pd.Series(0.0, index=df.index, dtype=np.float32)
    df["basis_z"]          = pd.Series(0.0, index=df.index, dtype=np.float32)

    if premium_df is None or len(premium_df) == 0:
        return

    pm = premium_df[["timestamp_ms", "last_funding_rate", "mark_index_basis_pct"]].copy()
    pm = pm.sort_values("timestamp_ms").reset_index(drop=True)
    pm["last_funding_rate"]    = pm["last_funding_rate"].astype(float)
    pm["mark_index_basis_pct"] = pm["mark_index_basis_pct"].astype(float)

    # As-of join: for each candle, take the most-recent premium row
    pm_ms       = pm["timestamp_ms"].values.astype(np.int64)
    pm_fr       = pm["last_funding_rate"].values
    pm_basis    = pm["mark_index_basis_pct"].values
    candle_ms_p = df["open_time"].values.astype(np.int64)

    idx_p = np.searchsorted(pm_ms, candle_ms_p, side="right") - 1
    idx_p = np.clip(idx_p, 0, len(pm_fr) - 1)
    valid_p = idx_p >= 0

    fr_vals    = np.where(valid_p, pm_fr[idx_p],    np.nan)
    basis_vals = np.where(valid_p, pm_basis[idx_p], np.nan)

    fr_series    = pd.Series(fr_vals,    index=df.index)
    basis_series = pd.Series(basis_vals, index=df.index)

    # funding_rate_z20: z-score over 480-bar window (~8 h of 1m data)
    df["funding_rate_z20"] = rolling_zscore(fr_series,    w=480).fillna(0.0).astype(np.float32)
    # basis_z: z-score over 50-bar window
    df["basis_z"]          = rolling_zscore(basis_series, w=50).fillna(0.0).astype(np.float32)

def _add_orderbook_features(
    df: pd.DataFrame,
    orderbook_df: "pd.DataFrame | None",
    close: pd.Series,
    safe_close: pd.Series,
) -> None:
    """Order book snapshot features (ob_imbalance, spread_bps, mid_dev_from_close)."""
    if orderbook_df is None or len(orderbook_df) == 0:
        return

    ob_cols = ["timestamp_ms", "ob_imbalance", "spread_pct", "mid_price"]
    ob = orderbook_df[ob_cols].copy().sort_values("timestamp_ms").reset_index(drop=True)
    ob["timestamp_ms"] = ob["timestamp_ms"].astype(np.int64)
    ob["ob_imbalance"] = ob["ob_imbalance"].astype(float)
    ob["spread_pct"] = ob["spread_pct"].astype(float)
    ob["mid_price"] = ob["mid_price"].astype(float)

    candle_ms = df["open_time"].values.astype(np.int64)
    ob_ms = ob["timestamp_ms"].values.astype(np.int64)
    idx = np.searchsorted(ob_ms, candle_ms, side="right") - 1

    valid = idx >= 0
    imbalance = np.full(len(df), np.nan, dtype=float)
    spread_pct = np.full(len(df), np.nan, dtype=float)
    mid_price = np.full(len(df), np.nan, dtype=float)

    imbalance[valid] = ob["ob_imbalance"].values[idx[valid]]
    spread_pct[valid] = ob["spread_pct"].values[idx[valid]]
    mid_price[valid] = ob["mid_price"].values[idx[valid]]

    df["ob_imbalance"] = pd.Series(imbalance, index=df.index).clip(-1.0, 1.0)
    df["spread_bps"] = pd.Series(spread_pct, index=df.index).mul(10_000.0).clip(0.0, 50.0)
    df["mid_dev_from_close"] = (
        (pd.Series(mid_price, index=df.index) - close) / safe_close
    ).clip(-0.01, 0.01)
    df["ob_imbalance_5_mean"] = (
        df["ob_imbalance"].rolling(window=5, min_periods=1).mean().clip(-1.0, 1.0)
    )

# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_crypto_features(
    df: pd.DataFrame,
    funding_df: "pd.DataFrame | None" = None,
    orderbook_df: "pd.DataFrame | None" = None,
    aggtrade_df: "pd.DataFrame | None" = None,
    premium_df: "pd.DataFrame | None" = None,
) -> pd.DataFrame:
    """Compute feature set for a candle DataFrame.

    Parameters
    ----------
    df:
        DataFrame sorted by open_time ascending with required OHLCV columns.
        Will be sorted internally if not already.
    funding_df:
        Optional funding rate DataFrame with columns:
          funding_time_ms (int), funding_rate (float), mark_price (float).
        When provided, funding rate features are computed and merged.
        Each funding rate applies forward until the next settlement (every 8h).
    orderbook_df:
        Optional order book DataFrame with columns:
          timestamp_ms (int), ob_imbalance (float),
          spread_pct (float), mid_price (float).
        When provided, snapshots are merged as-of using the most recent
        snapshot at or before each candle open_time.
    aggtrade_df:
        Optional aggTrade minute DataFrame from futures_aggtrade_minutes with
        columns: timestamp_ms, qty_total, qty_taker_buy, qty_taker_sell,
        quote_total, quote_taker_buy, quote_taker_sell, vwap, trade_count,
        coverage_ratio.
        Rows with coverage_ratio < 0.3 or trade_count < 30 are discarded.
        When None, aggtrade feature columns are filled with neutral constants.
    premium_df:
        Optional per-minute futures premium DataFrame from futures_premium_metrics
        with columns: timestamp_ms (int), last_funding_rate (float),
        mark_index_basis_pct (float).
        When provided, computes funding_rate_z20 (z-score of per-minute funding
        rate over 480-bar window) and basis_z (z-score of mark/index basis over
        50 bars).  When None, both columns default to 0.0 (neutral).

    Returns
    -------
    DataFrame with original columns plus all feature columns.
    Rows with insufficient history will have NaN in feature columns.
    """
    df = df.copy().sort_values("open_time").reset_index(drop=True)

    close      = df["close"].astype(float)
    high       = df["high"].astype(float)
    low        = df["low"].astype(float)
    volume     = df["volume"].astype(float)
    trades     = df["number_of_trades"].astype(float)
    quote_vol  = df["quote_asset_volume"].astype(float)
    taker_base = df["taker_buy_base_volume"].astype(float)
    safe_close = close.replace(0, np.nan)
    safe_vol   = volume.replace(0, np.nan)
    safe_trades = trades.replace(0, np.nan)

    _add_time_features(df)
    tr, atr_14 = _add_price_volatility_features(
        df, close, high, low, volume, safe_close, safe_vol,
        trades, safe_trades, quote_vol, taker_base,
    )
    _add_trend_momentum_features(df, close, high, low, volume, safe_close, tr, atr_14)
    _add_funding_features(df, funding_df)
    _add_aggtrade_features(df, aggtrade_df, close, safe_close)
    _add_premium_features(df, premium_df)
    _add_orderbook_features(df, orderbook_df, close, safe_close)

    return df

def get_feature_columns() -> list:
    """Return the ordered list of model-input feature columns (V7 — basis_z added).

    V5 methodology (2026-03-30):
      LGBM greedy forward selection over 44 candidates on BTCUSDT 15m.
      Walk-forward: train=10k, test=2k, 10 folds.
      Only 7 features improved accuracy (baseline 0.500 → 0.5256).
      All other features produced negative or negligible Δ when added.

      Removed from V3: everything except the 7 below. Adding more features
      introduces noise that hurts LGBM (and by proxy, PPO observation quality).

    V7 (2026-04-05):
      Added basis_z: pearson=+0.115, spearman=+0.102 vs 5-bar fwd return.
      Clear monotone quintile pattern on SOLUSDT 1m (42-day window).
      funding_rate_z20 excluded: 90% zeros, near-zero correlation.
    """
    return [
        "rsi_14",             # Δ+0.0100 — strongest single signal
        "log_ret_3",          # Δ+0.0011 — 3-bar momentum
        "stoch_k",            # Δ+0.0030 — Stochastic %K (orthogonal to RSI)
        "bb_pct",             # Δ+0.0009 — Bollinger band position
        "price_z_20",         # Δ+0.0028 — 20-bar price z-score (mean reversion)
        "rsi_56",             # Δ+0.0004 — 1h-equivalent RSI (multi-timeframe)
        "close_location",     # Δ+0.0012 — close position within bar range
        "log_ret_1",          # Δ+0.0015 — 1-bar return
        "flow_imbalance_20",  # Δ+0.0016 — 20-bar taker flow imbalance
        "upper_wick_ratio",   # Δ+0.0010 — upper wick selling pressure
        "dist_prev_day_low",  # Δ+0.0020 — distance from prev-day low
        # V6: futures microstructure (neutral=0.5/0.0 when no aggtrade data)
        "taker_buy_ratio",    # qty-weighted taker buy pressure [0,1]
        "quote_flow_imb",     # dollar-weighted buy/sell imbalance [-1,1]
        "taker_buy_ratio_5",  # 5-bar smoothed taker ratio [0,1]
        "vwap_dev",           # futures VWAP deviation from close [-0.01,0.01]
        # V7: futures basis (neutral=0.0 when no premium data)
        "basis_z",            # mark/index basis z-score (pearson=+0.115 vs fwd_ret_5)
    ]
