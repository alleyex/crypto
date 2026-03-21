"""Bollinger Bands mean-reversion strategy.

Generates:
  BUY  when close touches or breaks below the lower band
  SELL when close touches or breaks above the upper band
  HOLD when close is between the bands

Signal record fields:
  short_ma → current close price
  long_ma  → middle band (SMA)
"""

import math
from typing import Dict, Optional, Union

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

STRATEGY_NAME = "bbands"
DEFAULT_PERIOD = 20
DEFAULT_NUM_STD = 2.0


def _compute_bands(closes: list, period: int, num_std: float):
    """Return (middle, upper, lower) Bollinger Bands for the last `period` closes."""
    window = closes[-period:]
    middle = sum(window) / period
    variance = sum((c - middle) ** 2 for c in window) / period
    std = math.sqrt(variance)
    return middle, middle + num_std * std, middle - num_std * std


def generate_signal(
    connection: DBConnection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    period: int = DEFAULT_PERIOD,
    num_std: float = DEFAULT_NUM_STD,
    strategy_name: str = STRATEGY_NAME,
) -> Optional[Dict[str, Union[float, str]]]:
    rows = connection.execute(_SELECT_CLOSES_SQL, (symbol, timeframe, period)).fetchall()
    if len(rows) < period:
        return None

    closes = list(reversed([float(r[0]) for r in rows]))
    current_close = closes[-1]
    middle, upper, lower = _compute_bands(closes, period, num_std)

    if current_close <= lower:
        signal = "BUY"
    elif current_close >= upper:
        signal = "SELL"
    else:
        signal = "HOLD"

    return insert_signal(
        connection,
        signal_type=signal,
        symbol=symbol,
        timeframe=timeframe,
        strategy_name=strategy_name,
        short_ma=round(current_close, 6),
        long_ma=round(middle, 6),
    )
