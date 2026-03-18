import sqlite3
from pathlib import Path

import requests


URL = "https://api.binance.com/api/v3/klines"
PARAMS = {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "limit": 5,
}

DB_DIR = Path("storage")
DB_FILE = DB_DIR / "market_data.db"


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


def fetch_klines() -> list[list]:
    response = requests.get(URL, params=PARAMS, timeout=10)
    response.raise_for_status()
    return response.json()


def init_db(connection: sqlite3.Connection) -> None:
    connection.execute(CREATE_CANDLES_TABLE_SQL)
    connection.commit()


def save_klines(connection: sqlite3.Connection, klines: list[list]) -> None:
    rows = []

    for item in klines:
        rows.append(
            (
                "BTCUSDT",
                "1m",
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
        )

    connection.executemany(INSERT_CANDLE_SQL, rows)
    connection.commit()


def main() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)

    klines = fetch_klines()

    connection = sqlite3.connect(DB_FILE)
    try:
        init_db(connection)
        save_klines(connection, klines)
    finally:
        connection.close()

    print(f"Saved {len(klines)} klines to {DB_FILE}")


if __name__ == "__main__":
    main()
