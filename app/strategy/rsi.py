"""RSI (Relative Strength Index) strategy.

Generates:
  BUY  when RSI crosses below the oversold threshold (default 30)
  SELL when RSI crosses above the overbought threshold (default 70)
  HOLD otherwise

Signal record fields:
  short_ma → current RSI value
  long_ma  → 0.0 (unused; kept for schema compatibility)
"""

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

STRATEGY_NAME = "rsi"
DEFAULT_PERIOD = 14
DEFAULT_OVERSOLD = 30.0
DEFAULT_OVERBOUGHT = 70.0


def _compute_rsi(closes: list, period: int) -> float:
    """Compute RSI using Wilder's smoothing method.

    Requires len(closes) >= period + 1.
    """
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
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def generate_signal(
    connection: DBConnection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    period: int = DEFAULT_PERIOD,
    oversold: float = DEFAULT_OVERSOLD,
    overbought: float = DEFAULT_OVERBOUGHT,
    strategy_name: str = STRATEGY_NAME,
) -> Optional[Dict[str, Union[float, str]]]:
    # Fetch enough candles for a stable Wilder average (3× period)
    min_required = period + 1
    fetch_limit = period * 3
    rows = connection.execute(_SELECT_CLOSES_SQL, (symbol, timeframe, fetch_limit)).fetchall()
    if len(rows) < min_required:
        return None

    closes = list(reversed([float(r[0]) for r in rows]))
    rsi = _compute_rsi(closes, period)

    if rsi <= oversold:
        signal = "BUY"
    elif rsi >= overbought:
        signal = "SELL"
    else:
        signal = "HOLD"

    return insert_signal(
        connection,
        signal_type=signal,
        symbol=symbol,
        timeframe=timeframe,
        strategy_name=strategy_name,
        short_ma=round(rsi, 4),
        long_ma=0.0,
    )
