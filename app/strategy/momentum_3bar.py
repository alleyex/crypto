from typing import Dict, Optional, Union

from app.core.db import DBConnection
from app.core.db import table_exists
from app.strategy.ma_cross import insert_signal


SELECT_CLOSES_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
  AND timeframe = ?
ORDER BY open_time DESC
LIMIT ?;
"""


SELECT_POSITION_QTY_SQL = """
SELECT qty
FROM positions
WHERE symbol = ?
LIMIT 1;
"""


SELECT_PREVIOUS_SIGNAL_SQL = """
SELECT signal_type
FROM signals
WHERE symbol = ?
  AND timeframe = ?
  AND strategy_name = ?
ORDER BY id DESC
LIMIT 1;
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

    # Avoid emitting repeated actionable signals that the current position state
    # would immediately reject. This keeps the strategy signal stream aligned with
    # the single-position risk model.
    if signal != "HOLD" and table_exists(connection, "positions"):
        position_row = connection.execute(SELECT_POSITION_QTY_SQL, (symbol,)).fetchone()
        current_qty = float(position_row[0]) if position_row is not None else 0.0
        if signal == "BUY" and current_qty > 0:
            signal = "HOLD"
        elif signal == "SELL" and current_qty <= 0:
            signal = "HOLD"
    if signal != "HOLD":
        previous_signal_row = connection.execute(
            SELECT_PREVIOUS_SIGNAL_SQL,
            (symbol, timeframe, strategy_name),
        ).fetchone()
        previous_signal = str(previous_signal_row[0]) if previous_signal_row is not None else None
        if previous_signal == signal:
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
