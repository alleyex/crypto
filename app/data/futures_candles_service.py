from datetime import datetime, timezone
from typing import Any

from app.core.db import DBConnection
from app.data.candles_service import TIMEFRAME_INTERVAL_MS
from app.data.candles_service import candle_staleness_threshold_seconds
from app.system.heartbeat import upsert_heartbeat

INSERT_FUTURES_CANDLE_SQL = """
INSERT INTO futures_candles (
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

SELECT_LATEST_FUTURES_OPEN_TIME_SQL = """
SELECT MAX(open_time)
FROM futures_candles
WHERE symbol = ? AND timeframe = ?;
"""

def save_klines(
    connection: DBConnection,
    klines: list[list],
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> int:
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
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
        if item is not None and len(item) >= 11 and int(item[6]) < now_ms
    ]
    connection.executemany(INSERT_FUTURES_CANDLE_SQL, rows)
    connection.commit()
    upsert_heartbeat(
        connection,
        component="futures_market_data",
        status="ok",
        message="Futures market data saved.",
        payload={"symbol": symbol, "timeframe": timeframe, "saved_klines": len(rows)},
    )
    return len(rows)

def get_status(connection: DBConnection) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT symbol, timeframe,
               COUNT(*)        AS count,
               MIN(open_time)  AS earliest_ms,
               MAX(open_time)  AS latest_open_ms,
               MAX(close_time) AS latest_close_ms
        FROM futures_candles
        GROUP BY symbol, timeframe
        ORDER BY symbol, timeframe;
        """
    ).fetchall()

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    result = []
    for row in rows:
        symbol, timeframe, count, earliest_ms, latest_open_ms, latest_close_ms = (
            row[0], row[1], int(row[2]), int(row[3]), int(row[4]), int(row[5])
        )
        interval_ms = TIMEFRAME_INTERVAL_MS.get(timeframe, 60_000)
        expected_span_ms = (count - 1) * interval_ms
        actual_span_ms = latest_open_ms - earliest_ms
        gap_count = max(0, round((actual_span_ms - expected_span_ms) / interval_ms))
        stale_seconds = max(0, round((now_ms - latest_close_ms) / 1000))
        threshold_seconds = candle_staleness_threshold_seconds(timeframe)
        latest_iso = datetime.fromtimestamp(latest_close_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        earliest_iso = datetime.fromtimestamp(earliest_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        expected_count = round(actual_span_ms / interval_ms) + 1 if actual_span_ms > 0 else count
        coverage_pct = round(count / expected_count * 100, 1) if expected_count > 0 else 100.0
        result.append(
            {
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
                "coverage_pct": coverage_pct,
            }
        )
    return result

def get_latest_open_time(connection: DBConnection, symbol: str, timeframe: str) -> int | None:
    row = connection.execute(SELECT_LATEST_FUTURES_OPEN_TIME_SQL, (symbol, timeframe)).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])

def delete_candles(
    connection: DBConnection,
    *,
    symbols: list[str] | None = None,
    timeframes: list[str] | None = None,
) -> int:
    clauses: list[str] = []
    params: list[Any] = []
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        clauses.append(f"symbol IN ({placeholders})")
        params.extend(symbols)
    if timeframes:
        placeholders = ",".join("?" for _ in timeframes)
        clauses.append(f"timeframe IN ({placeholders})")
        params.extend(timeframes)
    if not clauses:
        raise ValueError("At least one symbol or timeframe filter is required.")

    where_sql = " AND ".join(clauses)
    before_row = connection.execute(
        f"SELECT COUNT(*) FROM futures_candles WHERE {where_sql};",
        tuple(params),
    ).fetchone()
    before_count = int(before_row[0]) if before_row and before_row[0] is not None else 0
    connection.execute(
        f"DELETE FROM futures_candles WHERE {where_sql};",
        tuple(params),
    )
    connection.commit()
    upsert_heartbeat(
        connection,
        component="futures_market_data",
        status="ok",
        message="Futures market data cleared.",
        payload={
            "symbols": symbols or [],
            "timeframes": timeframes or [],
            "deleted_rows": before_count,
        },
    )
    return before_count
