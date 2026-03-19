from typing import Dict, Optional, Union

from app.core.db import DBConnection
from app.strategy.ma_cross import insert_signal


SELECT_CLOSES_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
  AND timeframe = ?
ORDER BY open_time DESC
LIMIT ?;
"""


def generate_signal(
    connection: DBConnection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    lookback_bars: int = 3,
    strategy_name: str = "momentum_3bar",
) -> Optional[Dict[str, Union[float, str]]]:
    sample_size = lookback_bars + 1
    rows = connection.execute(
        SELECT_CLOSES_SQL,
        (symbol, timeframe, sample_size),
    ).fetchall()
    if len(rows) < sample_size:
        return None

    closes_desc = [float(row[0]) for row in rows]
    closes = list(reversed(closes_desc))
    latest_close = closes[-1]
    anchor_close = closes[0]

    if latest_close > anchor_close:
        signal = "BUY"
    elif latest_close < anchor_close:
        signal = "SELL"
    else:
        signal = "HOLD"

    return insert_signal(
        connection,
        signal_type=signal,
        symbol=symbol,
        timeframe=timeframe,
        strategy_name=strategy_name,
        short_ma=latest_close,
        long_ma=anchor_close,
    )
