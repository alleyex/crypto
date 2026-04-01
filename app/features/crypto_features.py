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

def build_crypto_features(
    df: pd.DataFrame,
    funding_df: "pd.DataFrame | None" = None,
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

    # ── Funding Rate features (optional) ──────────────────────────────────
    # Requires funding_df with columns: funding_time_ms, funding_rate.
    # Funding settles every 8 hours (00:00 / 08:00 / 16:00 UTC).
    # Each rate applies forward until the next settlement.
    if funding_df is not None and len(funding_df) > 0:
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

    return df


def get_feature_columns() -> list:
    """Return the ordered list of model-input feature columns (V5 — lean set).

    V5 methodology (2026-03-30):
      LGBM greedy forward selection over 44 candidates on BTCUSDT 15m.
      Walk-forward: train=10k, test=2k, 10 folds.
      Only 7 features improved accuracy (baseline 0.500 → 0.5256).
      All other features produced negative or negligible Δ when added.

      Removed from V3: everything except the 7 below. Adding more features
      introduces noise that hurts LGBM (and by proxy, PPO observation quality).
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
    ]
