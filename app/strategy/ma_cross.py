from typing import Dict, Optional, Union

from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.core.migrations import run_migrations

CREATE_SIGNALS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY,
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


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def insert_signal(
    connection: DBConnection,
    signal_type: str,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    strategy_name: str = "manual_test",
    short_ma: float = 0.0,
    long_ma: float = 0.0,
) -> Dict[str, Union[int, float, str]]:
    signal_id = insert_and_get_rowid(
        connection,
        INSERT_SIGNAL_SQL,
        (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma),
    )
    connection.commit()
    if signal_type in ("BUY", "SELL"):
        insert_event(
            connection,
            event_type="signal",
            status=signal_type.lower(),
            source="strategy",
            message=f"{strategy_name} generated {signal_type} signal for {symbol}.",
            payload={
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_name": strategy_name,
                "signal_type": signal_type,
                "short_ma": round(short_ma, 6),
                "long_ma": round(long_ma, 6),
            },
        )
    return {
        "id": signal_id,
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
    connection: DBConnection,
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
