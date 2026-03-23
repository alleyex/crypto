from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.db import DBConnection
from app.core.migrations import run_migrations
from app.core.settings import CANDLE_STALENESS_MULTIPLIER
from app.system.heartbeat import upsert_heartbeat

TIMEFRAME_INTERVAL_MS: Dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


def candle_staleness_threshold_seconds(timeframe: str, multiplier: int = CANDLE_STALENESS_MULTIPLIER) -> int:
    """Return the staleness threshold in seconds for a given timeframe.

    threshold = interval_seconds × multiplier
    e.g. 1m with multiplier=3 → 180s, 1h → 10800s.
    Falls back to 1m interval for unknown timeframes.
    """
    interval_ms = TIMEFRAME_INTERVAL_MS.get(timeframe, 60_000)
    return round(interval_ms / 1000 * multiplier)


CREATE_CANDLES_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS candles (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    open_time INTEGER NOT NULL,
    open NUMERIC(20,8) NOT NULL,
    high NUMERIC(20,8) NOT NULL,
    low NUMERIC(20,8) NOT NULL,
    close NUMERIC(20,8) NOT NULL,
    volume NUMERIC(20,8) NOT NULL,
    close_time INTEGER NOT NULL,
    quote_asset_volume NUMERIC(20,8),
    number_of_trades INTEGER,
    taker_buy_base_volume NUMERIC(20,8),
    taker_buy_quote_volume NUMERIC(20,8),
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, timeframe, open_time)
);
"""


INSERT_CANDLE_SQL = """
INSERT INTO candles (
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
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timeframe, open_time) DO NOTHING;
"""


SELECT_LATEST_CLOSE_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
  AND timeframe = ?
ORDER BY open_time DESC
LIMIT 1;
"""


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def save_klines(
    connection: DBConnection,
    klines: list[list],
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> int:
    rows = [
        (
            symbol,
            timeframe,
            int(item[0]),
            float(item[1]),
            float(item[2]),
            float(item[3]),
            float(item[4]),
            float(item[5]),
            int(item[6]),
            float(item[7]) if item[7] is not None else None,
            int(item[8]) if item[8] is not None else None,
            float(item[9]) if item[9] is not None else None,
            float(item[10]) if item[10] is not None else None,
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


def get_candles_status(connection: DBConnection) -> List[Dict[str, Any]]:
    """Return per-(symbol, timeframe) candle statistics including gap estimates."""
    rows = connection.execute(
        """
        SELECT symbol, timeframe,
               COUNT(*)       AS count,
               MIN(open_time) AS earliest_ms,
               MAX(open_time) AS latest_ms
        FROM candles
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe;
        """
    ).fetchall()

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    result = []
    for row in rows:
        symbol, timeframe, count, earliest_ms, latest_ms = (
            row[0], row[1], int(row[2]), int(row[3]), int(row[4])
        )
        interval_ms = TIMEFRAME_INTERVAL_MS.get(timeframe, 60_000)
        expected_span_ms = (count - 1) * interval_ms
        actual_span_ms = latest_ms - earliest_ms
        gap_count = max(0, round((actual_span_ms - expected_span_ms) / interval_ms))
        stale_seconds = round((now_ms - latest_ms) / 1000)
        threshold_seconds = candle_staleness_threshold_seconds(timeframe)

        latest_iso = datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        earliest_iso = datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        result.append({
            "symbol": symbol,
            "timeframe": timeframe,
            "count": count,
            "earliest": earliest_iso,
            "latest": latest_iso,
            "stale_seconds": stale_seconds,
            "staleness_threshold_seconds": threshold_seconds,
            "is_stale": stale_seconds > threshold_seconds,
            "has_gaps": gap_count > 0,
            "gap_count_estimate": gap_count,
        })
    return result


def get_latest_close(
    connection: DBConnection,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> Optional[float]:
    row = connection.execute(SELECT_LATEST_CLOSE_SQL, (symbol, timeframe)).fetchone()
    if row is None:
        return None
    return float(row[0])
