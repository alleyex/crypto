import sqlite3
from typing import Dict, Optional, Union


CREATE_SIGNALS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    short_ma REAL NOT NULL,
    long_ma REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


SELECT_CLOSES_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
  AND timeframe = ?
ORDER BY open_time DESC
LIMIT ?;
"""


INSERT_SIGNAL_SQL = """
INSERT INTO signals (
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    short_ma,
    long_ma
) VALUES (?, ?, ?, ?, ?, ?);
"""


def ensure_table(connection: sqlite3.Connection) -> None:
    connection.execute(CREATE_SIGNALS_TABLE_SQL)
    connection.commit()


def insert_signal(
    connection: sqlite3.Connection,
    signal_type: str,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    strategy_name: str = "manual_test",
    short_ma: float = 0.0,
    long_ma: float = 0.0,
) -> Dict[str, Union[int, float, str]]:
    cursor = connection.execute(
        INSERT_SIGNAL_SQL,
        (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma),
    )
    connection.commit()
    return {
        "id": cursor.lastrowid,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_name": strategy_name,
        "signal_type": signal_type,
        "short_ma": short_ma,
        "long_ma": long_ma,
    }


def average(values: list[float]) -> float:
    return sum(values) / len(values)


def generate_signal(
    connection: sqlite3.Connection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    short_window: int = 3,
    long_window: int = 5,
    strategy_name: str = "ma_cross",
) -> Optional[Dict[str, Union[float, str]]]:
    rows = connection.execute(
        SELECT_CLOSES_SQL,
        (symbol, timeframe, long_window),
    ).fetchall()
    if len(rows) < long_window:
        return None

    closes_desc = [float(row[0]) for row in rows]
    closes = list(reversed(closes_desc))
    short_ma = average(closes[-short_window:])
    long_ma = average(closes[-long_window:])

    if short_ma > long_ma:
        signal = "BUY"
    elif short_ma < long_ma:
        signal = "SELL"
    else:
        signal = "HOLD"

    return insert_signal(
        connection,
        signal_type=signal,
        symbol=symbol,
        timeframe=timeframe,
        strategy_name=strategy_name,
        short_ma=short_ma,
        long_ma=long_ma,
    )
