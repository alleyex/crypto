"""Futures open interest metrics collection service."""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import requests

from app.core.db import DBConnection
from app.core.settings import FUTURES_OPEN_INTEREST_SYMBOLS

_OI_URL = "https://fapi.binance.com/fapi/v1/openInterest"
_OI_TESTNET_URL = "https://testnet.binancefuture.com/fapi/v1/openInterest"
_TIMEOUT = 8
_RETRIES = 2
_BACKOFF = 1.0

FUTURES_OPEN_INTEREST_PAUSED_FILE = Path("runtime/futures_open_interest.paused")


def configured_futures_open_interest_symbols() -> list[str]:
    return list(FUTURES_OPEN_INTEREST_SYMBOLS)


def is_futures_open_interest_collection_enabled() -> bool:
    return not FUTURES_OPEN_INTEREST_PAUSED_FILE.exists()


def enable_futures_open_interest_collection() -> None:
    if FUTURES_OPEN_INTEREST_PAUSED_FILE.exists():
        FUTURES_OPEN_INTEREST_PAUSED_FILE.unlink()


def disable_futures_open_interest_collection() -> None:
    FUTURES_OPEN_INTEREST_PAUSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    FUTURES_OPEN_INTEREST_PAUSED_FILE.write_text("paused\n", encoding="utf-8")


def _is_testnet() -> bool:
    return os.getenv("CRYPTO_BINANCE_TESTNET", "").strip().lower() in ("1", "true", "yes", "on")


def _oi_url() -> str:
    return _OI_TESTNET_URL if _is_testnet() else _OI_URL


def _minute_ms(now_ms: Optional[int] = None) -> int:
    now_ms = int(now_ms or datetime.now(timezone.utc).timestamp() * 1000)
    return (now_ms // 60_000) * 60_000


def fetch_futures_open_interest(symbol: str) -> dict[str, Any]:
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))
    params = {"symbol": symbol}
    last_exc: Exception = RuntimeError("fetch_futures_open_interest: no attempts made")
    for attempt in range(_RETRIES + 1):
        try:
            resp = requests.get(_oi_url(), params=params, timeout=timeout)
            resp.raise_for_status()
            row = resp.json() or {}
            open_interest = float(row["openInterest"]) if row.get("openInterest") not in (None, "") else None
            return {
                "symbol": str(row.get("symbol") or symbol).upper(),
                "timestamp_ms": _minute_ms(),
                "open_interest": round(open_interest, 8) if open_interest is not None else None,
                "source": "rest",
            }
        except Exception as exc:
            last_exc = exc
            if attempt < _RETRIES:
                time.sleep(_BACKOFF * (2 ** attempt))
                continue
            raise last_exc


def _compute_open_interest_value(connection: DBConnection, symbol: str, timestamp_ms: int, open_interest: Optional[float]) -> Optional[float]:
    if open_interest is None:
        return None
    row = connection.execute(
        """
        SELECT mark_price
        FROM futures_premium_metrics
        WHERE symbol = ? AND timestamp_ms <= ?
        ORDER BY timestamp_ms DESC
        LIMIT 1
        """,
        (symbol, timestamp_ms),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    return round(float(open_interest) * float(row[0]), 8)


def _compute_oi_change(connection: DBConnection, symbol: str, timestamp_ms: int, open_interest: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    if open_interest is None:
        return None, None
    row = connection.execute(
        """
        SELECT open_interest
        FROM futures_open_interest_metrics
        WHERE symbol = ? AND timestamp_ms < ?
        ORDER BY timestamp_ms DESC
        LIMIT 1
        """,
        (symbol, timestamp_ms),
    ).fetchone()
    if row is None or row[0] is None:
        return None, None
    prev_oi = float(row[0])
    change = float(open_interest) - prev_oi
    change_pct = (change / prev_oi) if prev_oi != 0 else None
    return round(change, 8), (round(change_pct, 10) if change_pct is not None else None)


_UPSERT_SQL = """
INSERT INTO futures_open_interest_metrics
    (symbol, timestamp_ms, open_interest, open_interest_value, oi_change_1m, oi_change_pct_1m, source)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    open_interest = excluded.open_interest,
    open_interest_value = excluded.open_interest_value,
    oi_change_1m = excluded.oi_change_1m,
    oi_change_pct_1m = excluded.oi_change_pct_1m,
    source = excluded.source;
"""


def save_futures_open_interest(connection: DBConnection, row: Dict[str, Any]) -> bool:
    record = dict(row)
    record["open_interest_value"] = _compute_open_interest_value(
        connection,
        str(record["symbol"]),
        int(record["timestamp_ms"]),
        float(record["open_interest"]) if record.get("open_interest") is not None else None,
    )
    oi_change_1m, oi_change_pct_1m = _compute_oi_change(
        connection,
        str(record["symbol"]),
        int(record["timestamp_ms"]),
        float(record["open_interest"]) if record.get("open_interest") is not None else None,
    )
    record["oi_change_1m"] = oi_change_1m
    record["oi_change_pct_1m"] = oi_change_pct_1m
    connection.execute(
        _UPSERT_SQL,
        (
            record["symbol"],
            record["timestamp_ms"],
            record.get("open_interest"),
            record.get("open_interest_value"),
            record.get("oi_change_1m"),
            record.get("oi_change_pct_1m"),
            record.get("source", "rest"),
        ),
    )
    connection.commit()
    return True


def collect_futures_open_interest_metrics(connection: DBConnection, symbol_names: Optional[list[str]] = None) -> dict[str, Any]:
    symbols = list(symbol_names or configured_futures_open_interest_symbols())
    saved = 0
    errors: list[dict[str, str]] = []
    for symbol in symbols:
        try:
            row = fetch_futures_open_interest(symbol)
            save_futures_open_interest(connection, row)
            saved += 1
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)})
    return {
        "saved": saved,
        "errors": errors,
        "source_counts": {"rest": saved},
        "symbols": symbols,
    }


def get_futures_open_interest_stats(connection: DBConnection) -> List[Dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT symbol,
               COUNT(*) AS total,
               MIN(timestamp_ms) AS earliest_ms,
               MAX(timestamp_ms) AS latest_ms,
               MAX(source) AS latest_source
        FROM futures_open_interest_metrics
        GROUP BY symbol
        ORDER BY symbol;
        """
    ).fetchall()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    result: list[dict[str, Any]] = []
    for row in rows:
        symbol, total, earliest_ms, latest_ms, latest_source = row
        total = int(total)
        earliest_ms = int(earliest_ms)
        latest_ms = int(latest_ms)
        span_ms = latest_ms - earliest_ms
        expected = max(1, round(span_ms / 60_000) + 1)
        coverage_pct = round(total / expected * 100, 1)
        stale_seconds = round((now_ms - latest_ms) / 1000)
        result.append(
            {
                "symbol": str(symbol),
                "total": total,
                "coverage_pct": coverage_pct,
                "latest": datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "stale_seconds": stale_seconds,
                "is_stale": stale_seconds > 180,
                "latest_source": str(latest_source or "unknown"),
            }
        )
    return result
