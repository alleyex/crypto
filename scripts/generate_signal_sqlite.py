import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"
SHORT_WINDOW = 3
LONG_WINDOW = 5

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


def average(values: list[float]) -> float:
    return sum(values) / len(values)


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        connection.execute(CREATE_SIGNALS_TABLE_SQL)
        rows = connection.execute(
            SELECT_CLOSES_SQL,
            ("BTCUSDT", "1m", LONG_WINDOW),
        ).fetchall()

        if len(rows) < LONG_WINDOW:
            print("Not enough candle data to generate a signal.")
            return

        closes_desc = [float(row[0]) for row in rows]
        closes = list(reversed(closes_desc))

        short_ma = average(closes[-SHORT_WINDOW:])
        long_ma = average(closes[-LONG_WINDOW:])

        if short_ma > long_ma:
            signal = "BUY"
        elif short_ma < long_ma:
            signal = "SELL"
        else:
            signal = "HOLD"

        connection.execute(
            INSERT_SIGNAL_SQL,
            ("BTCUSDT", "1m", "ma_cross", signal, short_ma, long_ma),
        )
        connection.commit()
    finally:
        connection.close()

    print(f"short_ma={short_ma:.2f}")
    print(f"long_ma={long_ma:.2f}")
    print(f"signal={signal}")


if __name__ == "__main__":
    main()
