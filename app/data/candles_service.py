import sqlite3
from typing import Optional

from app.system.heartbeat import upsert_heartbeat


CREATE_CANDLES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS candles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open_time INTEGER NOT NULL,
    open TEXT NOT NULL,
    high TEXT NOT NULL,
    low TEXT NOT NULL,
    close TEXT NOT NULL,
    volume TEXT NOT NULL,
    close_time INTEGER NOT NULL,
    quote_asset_volume TEXT,
    number_of_trades INTEGER,
    taker_buy_base_volume TEXT,
    taker_buy_quote_volume TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timeframe, open_time)
);
"""


INSERT_CANDLE_SQL = """
INSERT OR IGNORE INTO candles (
    symbol,
    timeframe,
    open_time,
    open,
    high,
    low,
    close,
    volume,
    close_time,
    quote_asset_volume,
    number_of_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
"""


SELECT_LATEST_CLOSE_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
  AND timeframe = ?
ORDER BY open_time DESC
LIMIT 1;
"""


def ensure_table(connection: sqlite3.Connection) -> None:
    connection.execute(CREATE_CANDLES_TABLE_SQL)
    connection.commit()


def save_klines(
    connection: sqlite3.Connection,
    klines: list[list],
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> int:
    rows = [
        (
            symbol,
            timeframe,
            item[0],
            item[1],
            item[2],
            item[3],
            item[4],
            item[5],
            item[6],
            item[7],
            item[8],
            item[9],
            item[10],
        )
        for item in klines
    ]
    connection.executemany(INSERT_CANDLE_SQL, rows)
    connection.commit()
    upsert_heartbeat(
        connection,
        component="market_data",
        status="ok",
        message="Market data saved.",
        payload={
            "symbol": symbol,
            "timeframe": timeframe,
            "saved_klines": len(rows),
        },
    )
    return len(rows)


def get_latest_close(
    connection: sqlite3.Connection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> Optional[float]:
    row = connection.execute(SELECT_LATEST_CLOSE_SQL, (symbol, timeframe)).fetchone()
    if row is None:
        return None
    return float(row[0])
