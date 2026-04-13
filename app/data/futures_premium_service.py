"""Futures premium metrics collection service.

Collects mark price, index price, settle price, and funding data from
Binance Futures premium index endpoint and stores one row per symbol per minute.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.db import DBConnection
from app.core.settings import FUTURES_PREMIUM_SYMBOLS
from app.data.binance_config import futures_rest_base_url
from app.data.collection_state import CollectionState
from app.data.retry_helpers import fetch_with_retry

_MARK_KLINES_URL = "https://fapi.binance.com/fapi/v1/markPriceKlines"
_INDEX_KLINES_URL = "https://fapi.binance.com/fapi/v1/indexPriceKlines"
_TIMEOUT = 8
_RETRIES = 2
_BACKOFF = 1.0
_KLINES_LIMIT = 1500

FUTURES_PREMIUM_PAUSED_FILE = Path("runtime/futures_premium.paused")
_state = CollectionState(FUTURES_PREMIUM_PAUSED_FILE)

def configured_futures_premium_symbols() -> list[str]:
    return list(FUTURES_PREMIUM_SYMBOLS)

def is_futures_premium_collection_enabled() -> bool:
    return _state.is_enabled()

def enable_futures_premium_collection() -> None:
    _state.enable()

def disable_futures_premium_collection() -> None:
    _state.disable()

def _premium_url() -> str:
    return f"{futures_rest_base_url()}/fapi/v1/premiumIndex"

def _minute_ms(now_ms: int | None = None) -> int:
    now_ms = int(now_ms or datetime.now(timezone.utc).timestamp() * 1000)
    return (now_ms // 60_000) * 60_000

def _previous_closed_minute_ms(now_ms: int | None = None) -> int:
    return _minute_ms(now_ms) - 60_000

def _pair_for_symbol(symbol: str) -> str:
    normalized = symbol.upper()
    if normalized == "1000PEPEUSDT":
        return "PEPEUSDT"
    return normalized

def _fetch_klines(url: str, params: dict[str, Any], timeout: int) -> list[list[Any]]:
    rows: list[list[Any]] = []
    cursor = int(params["startTime"])
    end_time = int(params["endTime"])
    while cursor <= end_time:
        local_params = dict(params)
        local_params["startTime"] = cursor
        batch = fetch_with_retry(url, params=local_params, timeout=timeout, retries=_RETRIES, backoff=_BACKOFF).json() or []
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < _KLINES_LIMIT:
            break
        cursor = int(batch[-1][0]) + 60_000
    return rows

def _fetch_mark_rows(symbol: str, start_ms: int, end_ms: int, timeout: int) -> dict[int, float]:
    params = {
        "symbol": symbol.upper(),
        "interval": "1m",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": _KLINES_LIMIT,
    }
    rows = _fetch_klines(_MARK_KLINES_URL, params, timeout)
    return {int(row[0]): float(row[4]) for row in rows}

def _fetch_index_rows(symbol: str, start_ms: int, end_ms: int, timeout: int) -> dict[int, float]:
    params = {
        "pair": _pair_for_symbol(symbol),
        "interval": "1m",
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": _KLINES_LIMIT,
    }
    rows = _fetch_klines(_INDEX_KLINES_URL, params, timeout)
    return {int(row[0]): float(row[4]) for row in rows}

def fetch_futures_premium_metrics(
    symbols: list[str] | None = None,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    symbol_filter = {s.upper() for s in (symbols or configured_futures_premium_symbols())}
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))
    payload = fetch_with_retry(_premium_url(), timeout=timeout, retries=_RETRIES, backoff=_BACKOFF).json()
    rows = payload if isinstance(payload, list) else [payload]
    data = [row for row in rows if str(row.get("symbol") or "").upper() in symbol_filter]
    minute_ms = _minute_ms(now_ms)
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

def fetch_futures_premium_history(
    symbol: str,
    start_ms: int,
    end_ms: int,
    timeout: int | None = None,
) -> list[dict[str, Any]]:
    if start_ms > end_ms:
        return []
    timeout = int(timeout or os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))
    mark_rows = _fetch_mark_rows(symbol, start_ms, end_ms, timeout)
    index_rows = _fetch_index_rows(symbol, start_ms, end_ms, timeout)
    result: list[dict[str, Any]] = []
    for timestamp_ms in sorted(set(mark_rows) & set(index_rows)):
        mark_price = mark_rows[timestamp_ms]
        index_price = index_rows[timestamp_ms]
        basis_pct = None
        spread_bps = None
        if index_price and index_price > 0:
            basis_pct = (mark_price / index_price) - 1.0
            spread_bps = basis_pct * 10_000.0
        result.append(
            {
                "symbol": symbol.upper(),
                "timestamp_ms": timestamp_ms,
                "mark_price": round(mark_price, 8),
                "index_price": round(index_price, 8),
                "estimated_settle_price": None,
                "last_funding_rate": None,
                "next_funding_time_ms": None,
                "mark_index_basis_pct": round(basis_pct, 10) if basis_pct is not None else None,
                "mark_index_spread_bps": round(spread_bps, 6) if spread_bps is not None else None,
                "source": "archive",
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

def _latest_symbol_timestamps(connection: DBConnection, symbols: list[str]) -> dict[str, int]:
    if not symbols:
        return {}
    placeholders = ",".join("?" for _ in symbols)
    rows = connection.execute(
        f"""
        SELECT symbol, MAX(timestamp_ms) AS latest_ms
        FROM futures_premium_metrics
        WHERE symbol IN ({placeholders})
        GROUP BY symbol
        """,
        tuple(symbols),
    ).fetchall()
    return {str(symbol): int(latest_ms) for symbol, latest_ms in rows if latest_ms is not None}

def backfill_missing_futures_premium_metrics(
    connection: DBConnection,
    symbols: list[str],
    now_ms: int | None = None,
) -> int:
    latest_by_symbol = _latest_symbol_timestamps(connection, symbols)
    previous_closed_minute_ms = _previous_closed_minute_ms(now_ms)
    saved = 0
    for symbol in symbols:
        latest_ms = latest_by_symbol.get(symbol.upper())
        if latest_ms is None:
            continue
        start_ms = latest_ms + 60_000
        if start_ms > previous_closed_minute_ms:
            continue
        rows = fetch_futures_premium_history(symbol, start_ms, previous_closed_minute_ms)
        saved += save_futures_premium_metrics(connection, rows)
    return saved

def collect_futures_premium_metrics(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    symbols = list(symbol_names or configured_futures_premium_symbols())
    archive_saved = backfill_missing_futures_premium_metrics(connection, symbols, now_ms=now_ms)
    rows = fetch_futures_premium_metrics(symbols, now_ms=now_ms)
    rest_saved = save_futures_premium_metrics(connection, rows)
    return {
        "saved": archive_saved + rest_saved,
        "errors": [],
        "source_counts": {"archive": archive_saved, "rest": rest_saved},
        "symbols": symbols,
    }

def get_futures_premium_stats(connection: DBConnection) -> list[dict[str, Any]]:
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
