"""Futures premium metrics collection service.

Collects mark price, index price, settle price, and funding data from
Binance Futures premium index endpoint and stores one row per symbol per minute.
"""

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
from app.core.settings import FUTURES_PREMIUM_SYMBOLS

_PREMIUM_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"
_PREMIUM_TESTNET_URL = "https://testnet.binancefuture.com/fapi/v1/premiumIndex"
_TIMEOUT = 8
_RETRIES = 2
_BACKOFF = 1.0

FUTURES_PREMIUM_PAUSED_FILE = Path("runtime/futures_premium.paused")


def configured_futures_premium_symbols() -> list[str]:
    return list(FUTURES_PREMIUM_SYMBOLS)


def is_futures_premium_collection_enabled() -> bool:
    return not FUTURES_PREMIUM_PAUSED_FILE.exists()


def enable_futures_premium_collection() -> None:
    if FUTURES_PREMIUM_PAUSED_FILE.exists():
        FUTURES_PREMIUM_PAUSED_FILE.unlink()


def disable_futures_premium_collection() -> None:
    FUTURES_PREMIUM_PAUSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    FUTURES_PREMIUM_PAUSED_FILE.write_text("paused\n", encoding="utf-8")


def _is_testnet() -> bool:
    return os.getenv("CRYPTO_BINANCE_TESTNET", "").strip().lower() in ("1", "true", "yes", "on")


def _premium_url() -> str:
    return _PREMIUM_TESTNET_URL if _is_testnet() else _PREMIUM_URL


def _minute_ms(now_ms: Optional[int] = None) -> int:
    now_ms = int(now_ms or datetime.now(timezone.utc).timestamp() * 1000)
    return (now_ms // 60_000) * 60_000


def fetch_futures_premium_metrics(symbols: Optional[list[str]] = None) -> list[dict[str, Any]]:
    symbol_filter = {s.upper() for s in (symbols or configured_futures_premium_symbols())}
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))
    last_exc: Exception = RuntimeError("fetch_futures_premium_metrics: no attempts made")
    data: list[dict[str, Any]] = []
    for attempt in range(_RETRIES + 1):
        try:
            resp = requests.get(_premium_url(), timeout=timeout)
            resp.raise_for_status()
            payload = resp.json()
            rows = payload if isinstance(payload, list) else [payload]
            data = [row for row in rows if str(row.get("symbol") or "").upper() in symbol_filter]
            break
        except Exception as exc:
            last_exc = exc
            if attempt < _RETRIES:
                time.sleep(_BACKOFF * (2 ** attempt))
                continue
            raise last_exc
    minute_ms = _minute_ms()
    result: list[dict[str, Any]] = []
    for row in data:
        symbol = str(row.get("symbol") or "").upper()
        mark_price = float(row["markPrice"]) if row.get("markPrice") not in (None, "") else None
        index_price = float(row["indexPrice"]) if row.get("indexPrice") not in (None, "") else None
        estimated_settle_price = float(row["estimatedSettlePrice"]) if row.get("estimatedSettlePrice") not in (None, "") else None
        last_funding_rate = float(row["lastFundingRate"]) if row.get("lastFundingRate") not in (None, "") else None
        next_funding_time_ms = int(row["nextFundingTime"]) if row.get("nextFundingTime") not in (None, "") else None
        basis_pct = None
        spread_bps = None
        if mark_price and index_price and index_price > 0:
            basis_pct = (mark_price / index_price) - 1.0
            spread_bps = basis_pct * 10_000.0
        result.append(
            {
                "symbol": symbol,
                "timestamp_ms": minute_ms,
                "mark_price": round(mark_price, 8) if mark_price is not None else None,
                "index_price": round(index_price, 8) if index_price is not None else None,
                "estimated_settle_price": round(estimated_settle_price, 8) if estimated_settle_price is not None else None,
                "last_funding_rate": round(last_funding_rate, 10) if last_funding_rate is not None else None,
                "next_funding_time_ms": next_funding_time_ms,
                "mark_index_basis_pct": round(basis_pct, 10) if basis_pct is not None else None,
                "mark_index_spread_bps": round(spread_bps, 6) if spread_bps is not None else None,
                "source": "rest",
            }
        )
    return result


_UPSERT_SQL = """
INSERT INTO futures_premium_metrics
    (symbol, timestamp_ms, mark_price, index_price, estimated_settle_price,
     last_funding_rate, next_funding_time_ms, mark_index_basis_pct,
     mark_index_spread_bps, source)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    mark_price = excluded.mark_price,
    index_price = excluded.index_price,
    estimated_settle_price = excluded.estimated_settle_price,
    last_funding_rate = excluded.last_funding_rate,
    next_funding_time_ms = excluded.next_funding_time_ms,
    mark_index_basis_pct = excluded.mark_index_basis_pct,
    mark_index_spread_bps = excluded.mark_index_spread_bps,
    source = excluded.source;
"""


def save_futures_premium_metrics(connection: DBConnection, rows: list[dict[str, Any]]) -> int:
    saved = 0
    for row in rows:
        connection.execute(
            _UPSERT_SQL,
            (
                row["symbol"],
                row["timestamp_ms"],
                row.get("mark_price"),
                row.get("index_price"),
                row.get("estimated_settle_price"),
                row.get("last_funding_rate"),
                row.get("next_funding_time_ms"),
                row.get("mark_index_basis_pct"),
                row.get("mark_index_spread_bps"),
                row.get("source", "rest"),
            ),
        )
        saved += 1
    connection.commit()
    return saved


def collect_futures_premium_metrics(connection: DBConnection, symbol_names: Optional[list[str]] = None) -> dict[str, Any]:
    symbols = list(symbol_names or configured_futures_premium_symbols())
    rows = fetch_futures_premium_metrics(symbols)
    saved = save_futures_premium_metrics(connection, rows)
    return {
        "saved": saved,
        "errors": [],
        "source_counts": {"rest": saved},
        "symbols": symbols,
    }


def get_futures_premium_stats(connection: DBConnection) -> List[Dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT symbol,
               COUNT(*) AS total,
               MIN(timestamp_ms) AS earliest_ms,
               MAX(timestamp_ms) AS latest_ms,
               MAX(source) AS latest_source
        FROM futures_premium_metrics
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
