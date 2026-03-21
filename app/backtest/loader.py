"""Load historical candles from the DB for use with run_backtest()."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.db import DBConnection


_SELECT_CANDLES_SQL = """
SELECT open_time, open, high, low, close, volume, close_time
FROM candles
WHERE symbol = ?
  AND timeframe = ?
{where_extra}
ORDER BY open_time ASC
{limit_clause};
"""


def _iso_to_epoch_ms(iso: str) -> int:
    """Parse an ISO date/datetime string (UTC) to epoch milliseconds."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(iso, fmt).replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date string: {iso!r}")


def load_candles_from_db(
    connection: DBConnection,
    symbol: str,
    timeframe: str = "1m",
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Query candles from the DB and return them as a list of dicts.

    Parameters
    ----------
    connection: Active DB connection (SQLite or PostgreSQL).
    symbol:     Trading pair, e.g. "BTCUSDT".
    timeframe:  Candle timeframe stored in the DB, e.g. "1m".
    start:      Optional ISO date/datetime string (UTC) for the earliest
                candle to include, e.g. "2024-01-01" or "2024-01-01 08:00:00".
    end:        Optional ISO date/datetime string (UTC) for the latest candle
                to include (exclusive upper bound on open_time).
    limit:      Optional maximum number of candles to return (most recent when
                no start/end given).

    Returns
    -------
    List of dicts with keys: open_time, open, high, low, close, volume,
    close_time.  The list is sorted by open_time ascending.
    """
    conditions = []
    params: list = [symbol, timeframe]

    if start is not None:
        conditions.append("AND open_time >= ?")
        params.append(_iso_to_epoch_ms(start))
    if end is not None:
        conditions.append("AND open_time < ?")
        params.append(_iso_to_epoch_ms(end))

    where_extra = "\n  ".join(conditions)
    limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""

    query = _SELECT_CANDLES_SQL.format(
        where_extra=where_extra,
        limit_clause=limit_clause,
    )

    rows = connection.execute(query, params).fetchall()
    return [
        {
            "open_time": int(row[0]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "close_time": int(row[6]),
        }
        for row in rows
    ]
