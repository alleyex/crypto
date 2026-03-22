"""Pure feature computation functions — no DB dependencies.

Feature set ``v1`` produces a dict with these keys for each candle
(None when insufficient history):

  Prices / returns
  ----------------
  open_time       int   epoch-ms of the candle
  close           float raw close price
  returns_1       float 1-period log-return
  returns_5       float 5-period log-return
  returns_20      float 20-period log-return

  Moving averages
  ---------------
  ma_5            float simple moving average, period 5
  ma_20           float simple moving average, period 20
  ma_50           float simple moving average, period 50
  ma_cross_5_20   float 1.0 / 0.0 / -1.0  (ma_5 vs ma_20)

  RSI
  ---
  rsi_14          float RSI(14) [0–100]

  MACD
  ----
  macd_line       float EMA(12) - EMA(26)
  macd_signal     float EMA(9) of macd_line
  macd_hist       float macd_line - macd_signal

  Bollinger Bands (period=20, num_std=2)
  --------------------------------------
  bb_upper        float upper band
  bb_mid          float middle band (SMA-20)
  bb_lower        float lower band
  bb_pct_b        float %B  =  (close - lower) / (upper - lower)

  Volatility
  ----------
  volatility_20   float rolling std of returns_1 over 20 periods
"""

import math
from typing import Any, Dict, List, Optional

FEATURE_SET_VERSION = "v1"

# Minimum candles needed to compute the full feature vector.
MIN_CANDLES = 60  # covers MA-50 + a few warmup bars


# ---------------------------------------------------------------------------
# Low-level indicator helpers
# ---------------------------------------------------------------------------

def _sma(values: List[float], period: int) -> Optional[float]:
    if len(values) < period:
        return None
    window = values[-period:]
    return sum(window) / period


def _ema_series(values: List[float], period: int) -> List[float]:
    """Full EMA series using multiplier k = 2/(period+1)."""
    if not values:
        return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1.0 - k))
    return result


def _rsi(closes: List[float], period: int = 14) -> Optional[float]:
    """Wilder-smoothed RSI; returns None if insufficient data."""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0.0) for d in deltas]
    losses = [abs(min(d, 0.0)) for d in deltas]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period

    if avg_loss == 0.0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))


def _macd(
    closes: List[float],
    fast: int = 12,
    slow: int = 26,
    signal_period: int = 9,
) -> Optional[Dict[str, float]]:
    """Returns dict with macd_line, macd_signal, macd_hist or None."""
    if len(closes) < slow + signal_period:
        return None
    ema_fast = _ema_series(closes, fast)
    ema_slow = _ema_series(closes, slow)
    macd_series = [f - s for f, s in zip(ema_fast, ema_slow)]
    valid_macd = macd_series[slow - 1:]
    if len(valid_macd) < signal_period:
        return None
    signal_series = _ema_series(valid_macd, signal_period)
    line = valid_macd[-1]
    sig = signal_series[-1]
    return {"macd_line": line, "macd_signal": sig, "macd_hist": line - sig}


def _bbands(
    closes: List[float],
    period: int = 20,
    num_std: float = 2.0,
) -> Optional[Dict[str, float]]:
    """Returns dict with bb_upper, bb_mid, bb_lower, bb_pct_b or None."""
    if len(closes) < period:
        return None
    window = closes[-period:]
    mid = sum(window) / period
    variance = sum((c - mid) ** 2 for c in window) / period
    std = math.sqrt(variance)
    upper = mid + num_std * std
    lower = mid - num_std * std
    close = closes[-1]
    band_width = upper - lower
    pct_b = (close - lower) / band_width if band_width != 0 else 0.5
    return {"bb_upper": upper, "bb_mid": mid, "bb_lower": lower, "bb_pct_b": pct_b}


def _log_return(closes: List[float], period: int) -> Optional[float]:
    if len(closes) < period + 1:
        return None
    prev = closes[-(period + 1)]
    curr = closes[-1]
    if prev <= 0:
        return None
    return math.log(curr / prev)


def _volatility(returns: List[Optional[float]], period: int = 20) -> Optional[float]:
    valid = [r for r in returns[-period:] if r is not None]
    if len(valid) < period:
        return None
    mean = sum(valid) / len(valid)
    variance = sum((r - mean) ** 2 for r in valid) / len(valid)
    return math.sqrt(variance)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def compute_feature_vector(
    closes: List[float],
    open_time: int,
) -> Dict[str, Any]:
    """Compute the v1 feature vector for the *last* candle in ``closes``.

    Parameters
    ----------
    closes:
        List of close prices in chronological order (oldest first).
        Must contain at least 2 values; ideally >= MIN_CANDLES.
    open_time:
        Epoch-ms timestamp of the last candle.

    Returns
    -------
    Dict with all v1 feature keys.  Keys that cannot be computed (due to
    insufficient history) are set to None.
    """
    n = len(closes)
    close = closes[-1] if n >= 1 else None

    # --- returns ---
    ret1 = _log_return(closes, 1)
    ret5 = _log_return(closes, 5)
    ret20 = _log_return(closes, 20)

    # --- moving averages ---
    ma5 = _sma(closes, 5)
    ma20 = _sma(closes, 20)
    ma50 = _sma(closes, 50)

    if ma5 is not None and ma20 is not None:
        ma_cross = 1.0 if ma5 > ma20 else (-1.0 if ma5 < ma20 else 0.0)
    else:
        ma_cross = None

    # --- RSI ---
    rsi14 = _rsi(closes, 14)

    # --- MACD ---
    macd_result = _macd(closes)
    macd_line = macd_result["macd_line"] if macd_result else None
    macd_signal = macd_result["macd_signal"] if macd_result else None
    macd_hist = macd_result["macd_hist"] if macd_result else None

    # --- Bollinger Bands ---
    bb = _bbands(closes, 20)
    bb_upper = bb["bb_upper"] if bb else None
    bb_mid = bb["bb_mid"] if bb else None
    bb_lower = bb["bb_lower"] if bb else None
    bb_pct_b = bb["bb_pct_b"] if bb else None

    # --- volatility ---
    # Compute rolling 1-period returns for each position first
    returns_series: List[Optional[float]] = []
    for i in range(1, n):
        prev = closes[i - 1]
        curr = closes[i]
        if prev > 0:
            returns_series.append(math.log(curr / prev))
        else:
            returns_series.append(None)
    vol20 = _volatility(returns_series, 20)

    def _r(v: Optional[float], decimals: int = 8) -> Optional[float]:
        return round(v, decimals) if v is not None else None

    return {
        "open_time": open_time,
        "close": _r(close, 6),
        "returns_1": _r(ret1),
        "returns_5": _r(ret5),
        "returns_20": _r(ret20),
        "ma_5": _r(ma5, 6),
        "ma_20": _r(ma20, 6),
        "ma_50": _r(ma50, 6),
        "ma_cross_5_20": ma_cross,
        "rsi_14": _r(rsi14, 4),
        "macd_line": _r(macd_line),
        "macd_signal": _r(macd_signal),
        "macd_hist": _r(macd_hist),
        "bb_upper": _r(bb_upper, 6),
        "bb_mid": _r(bb_mid, 6),
        "bb_lower": _r(bb_lower, 6),
        "bb_pct_b": _r(bb_pct_b, 6),
        "volatility_20": _r(vol20),
        "feature_set": FEATURE_SET_VERSION,
    }


def compute_features_for_candles(
    candles: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Compute feature vectors for every candle in the list.

    Parameters
    ----------
    candles:
        List of candle dicts with at least ``open_time`` and ``close`` keys.
        Will be sorted by ``open_time`` ascending internally.

    Returns
    -------
    List of feature-vector dicts (same length as candles), one per candle.
    Candles with insufficient history will have most fields as None.
    """
    sorted_candles = sorted(candles, key=lambda c: int(c["open_time"]))
    closes = [float(c["close"]) for c in sorted_candles]
    result = []
    for i, candle in enumerate(sorted_candles):
        fv = compute_feature_vector(
            closes=closes[: i + 1],
            open_time=int(candle["open_time"]),
        )
        result.append(fv)
    return result
