import time
from typing import Any, Dict, List

from app.core.db import DBConnection
from app.data.candles_service import TIMEFRAME_INTERVAL_MS

PRICE_SPIKE_THRESHOLD = 0.05  # 5%


def _check_duplicates(connection: DBConnection) -> Dict[str, Any]:
    rows = connection.execute("""
        SELECT symbol, timeframe, open_time, COUNT(*) AS cnt
        FROM candles
        GROUP BY symbol, timeframe, open_time
        HAVING cnt > 1
        LIMIT 20
    """).fetchall()
    return {
        "count": len(rows),
        "examples": [
            {"symbol": r[0], "timeframe": r[1], "open_time": r[2], "count": r[3]}
            for r in rows
        ],
    }


def _check_integrity(connection: DBConnection) -> Dict[str, Any]:
    rows = connection.execute("""
        SELECT symbol, timeframe, open_time, open, high, low, close, volume
        FROM candles
        WHERE high < open OR high < close OR high < low
           OR low > open  OR low > close
           OR open <= 0 OR close <= 0 OR high <= 0 OR low <= 0
           OR volume < 0
        LIMIT 20
    """).fetchall()
    return {
        "count": len(rows),
        "examples": [
            {
                "symbol": r[0], "timeframe": r[1], "open_time": r[2],
                "open": float(r[3]), "high": float(r[4]),
                "low": float(r[5]), "close": float(r[6]), "volume": float(r[7]),
            }
            for r in rows
        ],
    }


def _check_gaps(connection: DBConnection) -> Dict[str, Any]:
    pairs = connection.execute(
        "SELECT DISTINCT symbol, timeframe FROM candles ORDER BY symbol, timeframe"
    ).fetchall()

    details: List[Dict[str, Any]] = []
    total_gaps = 0

    for symbol, timeframe in pairs:
        interval_ms = TIMEFRAME_INTERVAL_MS.get(timeframe, 60_000)
        times = [
            r[0]
            for r in connection.execute(
                "SELECT open_time FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY open_time ASC",
                (symbol, timeframe),
            ).fetchall()
        ]

        gap_count = 0
        missing_count = 0
        for i in range(1, len(times)):
            diff = times[i] - times[i - 1]
            if diff > interval_ms * 1.5:
                gap_count += 1
                missing_count += round(diff / interval_ms) - 1

        if gap_count > 0:
            details.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "gap_count": gap_count,
                "missing_candles": missing_count,
                "total_candles": len(times),
            })
            total_gaps += gap_count

    return {
        "total_gaps": total_gaps,
        "affected_pairs": len(details),
        "details": details,
    }


def _check_price_spikes(connection: DBConnection, threshold: float = PRICE_SPIKE_THRESHOLD) -> Dict[str, Any]:
    pairs = connection.execute(
        "SELECT DISTINCT symbol, timeframe FROM candles ORDER BY symbol, timeframe"
    ).fetchall()

    details: List[Dict[str, Any]] = []
    total_spikes = 0

    for symbol, timeframe in pairs:
        rows = connection.execute(
            "SELECT open_time, close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY open_time ASC",
            (symbol, timeframe),
        ).fetchall()

        spikes = []
        for i in range(1, len(rows)):
            prev_close = float(rows[i - 1][1])
            curr_close = float(rows[i][1])
            if prev_close > 0:
                change = abs(curr_close - prev_close) / prev_close
                if change > threshold:
                    spikes.append({
                        "open_time": rows[i][0],
                        "prev_close": prev_close,
                        "close": curr_close,
                        "change_pct": round(change * 100, 2),
                    })

        if spikes:
            details.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "spike_count": len(spikes),
                "examples": spikes[:3],
            })
            total_spikes += len(spikes)

    return {
        "threshold_pct": round(threshold * 100, 1),
        "total_spikes": total_spikes,
        "affected_pairs": len(details),
        "details": details,
    }


def run_candles_quality_check(connection: DBConnection) -> Dict[str, Any]:
    started_at = time.time()

    duplicates = _check_duplicates(connection)
    integrity = _check_integrity(connection)
    gaps = _check_gaps(connection)
    spikes = _check_price_spikes(connection)

    errors = []
    warnings = []

    if duplicates["count"] > 0:
        errors.append(f"{duplicates['count']} duplicate row(s)")
    if integrity["count"] > 0:
        errors.append(f"{integrity['count']} OHLCV integrity violation(s)")
    if gaps["total_gaps"] > 0:
        warnings.append(f"{gaps['total_gaps']} gap(s) across {gaps['affected_pairs']} symbol+timeframe pair(s)")
    if spikes["total_spikes"] > 0:
        warnings.append(f"{spikes['total_spikes']} price spike(s) (>{spikes['threshold_pct']}%)")

    status = "error" if errors else ("warning" if warnings else "ok")

    return {
        "status": status,
        "errors": errors,
        "warnings": warnings,
        "duplicates": duplicates,
        "integrity": integrity,
        "gaps": gaps,
        "price_spikes": spikes,
        "duration_seconds": round(time.time() - started_at, 3),
    }
