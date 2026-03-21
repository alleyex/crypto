"""MACD (Moving Average Convergence Divergence) crossover strategy.

Generates:
  BUY  when the MACD line crosses above the signal line
       (histogram transitions from negative to positive)
  SELL when the MACD line crosses below the signal line
       (histogram transitions from positive to negative)
  HOLD otherwise (no crossover on the most recent bar)

Signal record fields:
  short_ma → MACD line value (EMA_fast - EMA_slow)
  long_ma  → signal line value (EMA of MACD)
"""

from typing import Dict, List, Optional, Union

from app.core.db import DBConnection
from app.strategy.ma_cross import insert_signal


_SELECT_CLOSES_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
  AND timeframe = ?
ORDER BY open_time DESC
LIMIT ?;
"""

STRATEGY_NAME = "macd"
DEFAULT_FAST = 12
DEFAULT_SLOW = 26
DEFAULT_SIGNAL = 9


def _ema(values: List[float], period: int) -> List[float]:
    """Compute EMA series using the standard multiplier k = 2/(period+1)."""
    if not values:
        return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1.0 - k))
    return result


def _compute_macd(
    closes: List[float],
    fast: int,
    slow: int,
    signal_period: int,
) -> Optional[tuple]:
    """Return (macd_line, signal_line, prev_macd, prev_signal) or None."""
    if len(closes) < slow + signal_period:
        return None

    ema_fast = _ema(closes, fast)
    ema_slow = _ema(closes, slow)
    macd_series = [f - s for f, s in zip(ema_fast, ema_slow)]

    # Only compute signal line over the MACD values that exist after slow warmup
    valid_macd = macd_series[slow - 1:]
    if len(valid_macd) < signal_period:
        return None

    signal_series = _ema(valid_macd, signal_period)

    macd_now = valid_macd[-1]
    signal_now = signal_series[-1]
    macd_prev = valid_macd[-2] if len(valid_macd) >= 2 else macd_now
    signal_prev = signal_series[-2] if len(signal_series) >= 2 else signal_now

    return macd_now, signal_now, macd_prev, signal_prev


def generate_signal(
    connection: DBConnection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    fast: int = DEFAULT_FAST,
    slow: int = DEFAULT_SLOW,
    signal_period: int = DEFAULT_SIGNAL,
    strategy_name: str = STRATEGY_NAME,
) -> Optional[Dict[str, Union[float, str]]]:
    # Fetch enough candles for EMA warmup; 3× slow gives stable values
    fetch_limit = slow * 3 + signal_period
    rows = connection.execute(_SELECT_CLOSES_SQL, (symbol, timeframe, fetch_limit)).fetchall()
    min_required = slow + signal_period
    if len(rows) < min_required:
        return None

    closes = list(reversed([float(r[0]) for r in rows]))
    result = _compute_macd(closes, fast, slow, signal_period)
    if result is None:
        return None

    macd_now, signal_now, macd_prev, signal_prev = result
    hist_now = macd_now - signal_now
    hist_prev = macd_prev - signal_prev

    if hist_prev <= 0 and hist_now > 0:
        signal = "BUY"
    elif hist_prev >= 0 and hist_now < 0:
        signal = "SELL"
    else:
        signal = "HOLD"

    return insert_signal(
        connection,
        signal_type=signal,
        symbol=symbol,
        timeframe=timeframe,
        strategy_name=strategy_name,
        short_ma=round(macd_now, 8),
        long_ma=round(signal_now, 8),
    )
