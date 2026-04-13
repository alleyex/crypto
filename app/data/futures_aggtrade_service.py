"""Futures aggregated trade minute collection service.

Primary transport:
  Binance Futures WebSocket aggTrade stream

Fallback transport:
  Binance Futures REST /fapi/v1/aggTrades

The service maintains a per-symbol in-memory bucket for the current minute and
persists minute aggregates for the configured symbols.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.db import DBConnection
from app.core.settings import FUTURES_AGGTRADE_SYMBOLS
from app.core.settings import WS_RESTART_SECONDS as _WS_RESTART_SECONDS
from app.data.binance_config import futures_rest_base_url
from app.data.binance_config import futures_ws_base_url
from app.data.collection_state import CollectionState
from app.data.futures_collector_base import _BaseFuturesWSCollector
from app.data.retry_helpers import fetch_with_retry

_AGGTRADE_LIMIT = 1000
_TIMEOUT = 8
_RETRIES = 2
_BACKOFF = 1.0
_WS_STALE_SECONDS = 75

FUTURES_AGGTRADE_PAUSED_FILE = Path("runtime/futures_aggtrade.paused")
_state = CollectionState(FUTURES_AGGTRADE_PAUSED_FILE)

def configured_futures_aggtrade_symbols() -> list[str]:
    return list(FUTURES_AGGTRADE_SYMBOLS)

def is_futures_aggtrade_collection_enabled() -> bool:
    return _state.is_enabled()

def enable_futures_aggtrade_collection() -> None:
    _state.enable()

def disable_futures_aggtrade_collection() -> None:
    _state.disable()

def _aggtrades_url() -> str:
    return f"{futures_rest_base_url()}/fapi/v1/aggTrades"

def _ws_url(symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@aggTrade" for symbol in symbols)
    return f"{futures_ws_base_url()}/stream?streams={streams}"

def _new_bucket(symbol: str, minute_ms: int, event_ms: int, aggtrade_id: int, price: float, qty: float, buyer_is_maker: bool) -> dict[str, Any]:
    quote_qty = price * qty
    taker_buy_qty = 0.0 if buyer_is_maker else qty
    taker_sell_qty = qty if buyer_is_maker else 0.0
    taker_buy_quote = 0.0 if buyer_is_maker else quote_qty
    taker_sell_quote = quote_qty if buyer_is_maker else 0.0
    taker_buy_count = 0 if buyer_is_maker else 1
    taker_sell_count = 1 if buyer_is_maker else 0
    return {
        "symbol": symbol,
        "timestamp_ms": minute_ms,
        "trade_count": 1,
        "taker_buy_count": taker_buy_count,
        "taker_sell_count": taker_sell_count,
        "qty_total": qty,
        "qty_taker_buy": taker_buy_qty,
        "qty_taker_sell": taker_sell_qty,
        "quote_total": quote_qty,
        "quote_taker_buy": taker_buy_quote,
        "quote_taker_sell": taker_sell_quote,
        "price_open": price,
        "price_high": price,
        "price_low": price,
        "price_close": price,
        "price_qty_sum": price * qty,
        "first_trade_id": aggtrade_id,
        "last_trade_id": aggtrade_id,
        "first_event_ms": event_ms,
        "last_event_ms": event_ms,
        "active_second_marks": {event_ms // 1000},
    }

def _update_bucket(bucket: dict[str, Any], event_ms: int, aggtrade_id: int, price: float, qty: float, buyer_is_maker: bool) -> None:
    quote_qty = price * qty
    bucket["trade_count"] += 1
    bucket["qty_total"] += qty
    bucket["quote_total"] += quote_qty
    bucket["price_qty_sum"] += price * qty
    bucket["price_high"] = max(float(bucket["price_high"]), price)
    bucket["price_low"] = min(float(bucket["price_low"]), price)
    bucket["price_close"] = price
    if aggtrade_id < int(bucket["first_trade_id"]):
        bucket["first_trade_id"] = aggtrade_id
    if aggtrade_id > int(bucket["last_trade_id"]):
        bucket["last_trade_id"] = aggtrade_id
    if buyer_is_maker:
        bucket["taker_sell_count"] += 1
        bucket["qty_taker_sell"] += qty
        bucket["quote_taker_sell"] += quote_qty
    else:
        bucket["taker_buy_count"] += 1
        bucket["qty_taker_buy"] += qty
        bucket["quote_taker_buy"] += quote_qty
    bucket["last_event_ms"] = event_ms
    bucket.setdefault("active_second_marks", set()).add(event_ms // 1000)

def _snapshot_from_bucket(bucket: dict[str, Any], *, source: str) -> dict[str, Any]:
    trade_count = int(bucket["trade_count"])
    qty_total = float(bucket["qty_total"])
    quote_total = float(bucket["quote_total"])
    active_seconds = len(bucket.get("active_second_marks", set()))
    coverage_ratio = round(min(1.0, max(0.0, active_seconds / 60.0)), 6) if active_seconds > 0 else 0.0
    vwap = quote_total / qty_total if qty_total > 0 else None
    avg_trade_size = qty_total / trade_count if trade_count > 0 else None
    return {
        "symbol": bucket["symbol"],
        "timestamp_ms": int(bucket["timestamp_ms"]),
        "trade_count": trade_count,
        "taker_buy_count": int(bucket["taker_buy_count"]),
        "taker_sell_count": int(bucket["taker_sell_count"]),
        "qty_total": round(qty_total, 8),
        "qty_taker_buy": round(float(bucket["qty_taker_buy"]), 8),
        "qty_taker_sell": round(float(bucket["qty_taker_sell"]), 8),
        "quote_total": round(quote_total, 8),
        "quote_taker_buy": round(float(bucket["quote_taker_buy"]), 8),
        "quote_taker_sell": round(float(bucket["quote_taker_sell"]), 8),
        "price_open": round(float(bucket["price_open"]), 8),
        "price_high": round(float(bucket["price_high"]), 8),
        "price_low": round(float(bucket["price_low"]), 8),
        "price_close": round(float(bucket["price_close"]), 8),
        "vwap": round(vwap, 8) if vwap is not None else None,
        "avg_trade_size": round(avg_trade_size, 8) if avg_trade_size is not None else None,
        "first_trade_id": int(bucket["first_trade_id"]),
        "last_trade_id": int(bucket["last_trade_id"]),
        "first_event_ms": int(bucket["first_event_ms"]),
        "last_event_ms": int(bucket["last_event_ms"]),
        "active_seconds": active_seconds,
        "coverage_ratio": coverage_ratio,
        "source": source,
    }

def _snapshot_from_rest_rows(symbol: str, rows: list[dict[str, Any]], minute_ms: int) -> dict[str, Any] | None:
    filtered = [row for row in rows if int(row.get("T") or 0) // 60_000 * 60_000 == minute_ms]
    if not filtered:
        return None
    first = filtered[0]
    price = float(first["p"])
    qty = float(first["q"])
    event_ms = int(first["T"])
    aggtrade_id = int(first["a"])
    bucket = _new_bucket(symbol, minute_ms, event_ms, aggtrade_id, price, qty, bool(first.get("m")))
    for row in filtered[1:]:
        _update_bucket(
            bucket,
            int(row["T"]),
            int(row["a"]),
            float(row["p"]),
            float(row["q"]),
            bool(row.get("m")),
        )
    return _snapshot_from_bucket(bucket, source="rest")

class _FuturesAggTradeCollector(_BaseFuturesWSCollector):
    _thread_name = "futures-aggtrade-ws"

    def __init__(self) -> None:
        super().__init__()
        self._buckets: dict[str, dict[str, Any]] = {}

    def _get_ws_url(self, symbols: tuple[str, ...]) -> str:
        return _ws_url(list(symbols))

    def _handle_message(self, message: str, symbols: tuple[str, ...]) -> None:
        payload = json.loads(message)
        data = payload.get("data", payload)
        symbol = str(data.get("s") or "").upper()
        if not symbol:
            return
        event_ms = int(data.get("T") or data.get("E") or int(datetime.now(timezone.utc).timestamp() * 1000))
        minute_ms = (event_ms // 60_000) * 60_000
        aggtrade_id = int(data.get("a") or 0)
        price = float(data.get("p") or 0.0)
        qty = float(data.get("q") or 0.0)
        buyer_is_maker = bool(data.get("m"))
        if price <= 0 or qty <= 0:
            return
        with self._lock:
            prev = self._buckets.get(symbol)
            if prev is None or int(prev["timestamp_ms"]) != minute_ms:
                self._buckets[symbol] = _new_bucket(symbol, minute_ms, event_ms, aggtrade_id, price, qty, buyer_is_maker)
            else:
                _update_bucket(prev, event_ms, aggtrade_id, price, qty, buyer_is_maker)

    def get_minute_snapshot(self, symbol: str, now_ms: int | None = None) -> dict[str, Any] | None:
        now_ms = int(now_ms or datetime.now(timezone.utc).timestamp() * 1000)
        minute_ms = (now_ms // 60_000) * 60_000
        with self._lock:
            bucket = self._buckets.get(symbol.upper())
            if bucket is None:
                return None
            if int(bucket.get("timestamp_ms", 0)) != minute_ms:
                return None
            age_seconds = max(0, (now_ms - int(bucket.get("last_event_ms", 0))) / 1000)
            if age_seconds > _WS_STALE_SECONDS:
                return None
            return _snapshot_from_bucket(bucket, source="ws")

    def runtime_status(self) -> dict[str, Any]:
        with self._lock:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            symbol_runtime: dict[str, dict[str, Any]] = {}
            for symbol, bucket in self._buckets.items():
                last_event_ms = int(bucket.get("last_event_ms", 0) or 0)
                symbol_runtime[symbol] = {
                    "current_minute": int(bucket.get("timestamp_ms", 0) or 0),
                    "trade_count": int(bucket.get("trade_count", 0) or 0),
                    "active_seconds": len(bucket.get("active_second_marks", set())),
                    "first_event_ms": int(bucket.get("first_event_ms", 0) or 0),
                    "last_event_ms": last_event_ms,
                    "event_age_seconds": max(0, round((now_ms - last_event_ms) / 1000)) if last_event_ms else None,
                }
            return {
                "symbols": list(self._symbols),
                "ws_available": self._ws_available,
                "last_error": self._last_error,
                "cached_symbols": sorted(self._buckets.keys()),
                "symbol_runtime": symbol_runtime,
            }

_COLLECTOR = _FuturesAggTradeCollector()

def reset_futures_aggtrade_runtime(symbols: list[str] | None = None) -> dict[str, Any]:
    global _COLLECTOR
    _COLLECTOR = _FuturesAggTradeCollector()
    if symbols:
        _COLLECTOR.ensure_started(list(symbols))
    return _COLLECTOR.runtime_status()

def fetch_futures_aggtrade_snapshot(symbol: str, minute_ms: int | None = None) -> dict[str, Any] | None:
    minute_ms = int(minute_ms or (datetime.now(timezone.utc).timestamp() * 1000 // 60_000) * 60_000)
    params = {"symbol": symbol, "startTime": minute_ms, "endTime": minute_ms + 59_999, "limit": _AGGTRADE_LIMIT}
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))
    cursor = minute_ms
    rows: list[dict[str, Any]] = []
    while cursor <= minute_ms + 59_999:
        params["startTime"] = cursor
        attempt_rows = fetch_with_retry(_aggtrades_url(), params=params, timeout=timeout, retries=_RETRIES, backoff=_BACKOFF).json() or []
        if not attempt_rows:
            break
        rows.extend(attempt_rows)
        last_trade_ms = int(attempt_rows[-1].get("T") or cursor)
        if len(attempt_rows) < _AGGTRADE_LIMIT:
            break
        cursor = last_trade_ms + 1
    return _snapshot_from_rest_rows(symbol, rows, minute_ms)

_UPSERT_SQL = """
INSERT INTO futures_aggtrade_minutes
    (symbol, timestamp_ms, trade_count, taker_buy_count, taker_sell_count,
     qty_total, qty_taker_buy, qty_taker_sell,
     quote_total, quote_taker_buy, quote_taker_sell,
     price_open, price_high, price_low, price_close,
     vwap, avg_trade_size, first_trade_id, last_trade_id,
     first_event_ms, last_event_ms, active_seconds, coverage_ratio, source)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    trade_count = excluded.trade_count,
    taker_buy_count = excluded.taker_buy_count,
    taker_sell_count = excluded.taker_sell_count,
    qty_total = excluded.qty_total,
    qty_taker_buy = excluded.qty_taker_buy,
    qty_taker_sell = excluded.qty_taker_sell,
    quote_total = excluded.quote_total,
    quote_taker_buy = excluded.quote_taker_buy,
    quote_taker_sell = excluded.quote_taker_sell,
    price_open = excluded.price_open,
    price_high = excluded.price_high,
    price_low = excluded.price_low,
    price_close = excluded.price_close,
    vwap = excluded.vwap,
    avg_trade_size = excluded.avg_trade_size,
    first_trade_id = excluded.first_trade_id,
    last_trade_id = excluded.last_trade_id,
    first_event_ms = excluded.first_event_ms,
    last_event_ms = excluded.last_event_ms,
    active_seconds = excluded.active_seconds,
    coverage_ratio = excluded.coverage_ratio,
    source = excluded.source;
"""

def save_futures_aggtrade_snapshot(connection: DBConnection, snapshot: dict[str, Any]) -> bool:
    connection.execute(
        _UPSERT_SQL,
        (
            snapshot["symbol"],
            snapshot["timestamp_ms"],
            snapshot["trade_count"],
            snapshot["taker_buy_count"],
            snapshot["taker_sell_count"],
            snapshot["qty_total"],
            snapshot["qty_taker_buy"],
            snapshot["qty_taker_sell"],
            snapshot["quote_total"],
            snapshot["quote_taker_buy"],
            snapshot["quote_taker_sell"],
            snapshot["price_open"],
            snapshot["price_high"],
            snapshot["price_low"],
            snapshot["price_close"],
            snapshot.get("vwap"),
            snapshot.get("avg_trade_size"),
            snapshot["first_trade_id"],
            snapshot["last_trade_id"],
            snapshot["first_event_ms"],
            snapshot["last_event_ms"],
            snapshot.get("active_seconds", 0),
            snapshot.get("coverage_ratio", 0.0),
            snapshot.get("source", "rest"),
        ),
    )
    connection.commit()
    return True

def collect_futures_aggtrade_minutes(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> dict[str, Any]:
    symbols = list(symbol_names or configured_futures_aggtrade_symbols())
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    _COLLECTOR.ensure_started(symbols)

    saved = 0
    errors: list[dict[str, str]] = []
    source_counts = {"ws": 0, "rest": 0}

    for symbol in symbols:
        try:
            snapshot = _COLLECTOR.get_minute_snapshot(symbol, now_ms=now_ms)
            if snapshot is None:
                snapshot = fetch_futures_aggtrade_snapshot(symbol, minute_ms=(now_ms // 60_000) * 60_000)
            if snapshot is None:
                continue
            save_futures_aggtrade_snapshot(connection, snapshot)
            source = str(snapshot.get("source", "rest"))
            source_counts[source] = source_counts.get(source, 0) + 1
            saved += 1
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)})

    return {
        "saved": saved,
        "errors": errors,
        "source_counts": source_counts,
        "collector": _COLLECTOR.runtime_status(),
    }

def get_futures_aggtrade_stats(
    connection: DBConnection,
    runtime: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    runtime = runtime if runtime is not None else _COLLECTOR.runtime_status().get("symbol_runtime", {})
    rows = connection.execute(
        """
        SELECT symbol,
               COUNT(*) AS total,
               MIN(timestamp_ms) AS earliest_ms,
               MAX(timestamp_ms) AS latest_ms,
               MAX(source) AS latest_source
        FROM futures_aggtrade_minutes
        GROUP BY symbol
        ORDER BY symbol;
        """
    ).fetchall()

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    result = []
    for row in rows:
        symbol, total, earliest_ms, latest_ms, latest_source = row
        symbol = str(symbol)
        total = int(total)
        earliest_ms = int(earliest_ms)
        latest_ms = int(latest_ms)
        symbol_runtime = runtime.get(symbol, {})
        span_ms = latest_ms - earliest_ms
        expected = max(1, round(span_ms / 60_000) + 1)
        coverage_pct = round(total / expected * 100, 1)
        stale_seconds = round((now_ms - latest_ms) / 1000)
        last_event_ms = symbol_runtime.get("last_event_ms")
        event_age_seconds = None
        if last_event_ms:
            event_age_seconds = max(0, round((now_ms - int(last_event_ms)) / 1000))
        result.append(
            {
                "symbol": symbol,
                "total": total,
                "coverage_pct": coverage_pct,
                "latest": datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "stale_seconds": stale_seconds,
                "is_stale": stale_seconds > 180,
                "latest_source": str(latest_source or "unknown"),
                "last_snapshot_at": datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "last_event_at": (
                    datetime.fromtimestamp(int(last_event_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    if last_event_ms else None
                ),
                "event_age_seconds": event_age_seconds,
                "current_minute_trade_count": int(symbol_runtime.get("trade_count", 0) or 0),
                "current_minute_active_seconds": int(symbol_runtime.get("active_seconds", 0) or 0),
            }
        )
    return result

def get_recent_futures_aggtrade_minutes(
    connection: DBConnection,
    symbol: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT symbol,
               timestamp_ms,
               trade_count,
               taker_buy_count,
               taker_sell_count,
               qty_total,
               qty_taker_buy,
               qty_taker_sell,
               quote_total,
               quote_taker_buy,
               quote_taker_sell,
               price_open,
               price_high,
               price_low,
               price_close,
               vwap,
               avg_trade_size,
               active_seconds,
               coverage_ratio,
               source
        FROM futures_aggtrade_minutes
        WHERE symbol = ?
        ORDER BY timestamp_ms DESC
        LIMIT ?
        """,
        (str(symbol).upper(), int(limit)),
    ).fetchall()
    result: list[dict[str, Any]] = []
    for row in rows:
        result.append(
            {
                "symbol": str(row[0]),
                "timestamp": datetime.fromtimestamp(int(row[1]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "trade_count": int(row[2] or 0),
                "taker_buy_count": int(row[3] or 0),
                "taker_sell_count": int(row[4] or 0),
                "qty_total": float(row[5] or 0.0),
                "qty_taker_buy": float(row[6] or 0.0),
                "qty_taker_sell": float(row[7] or 0.0),
                "quote_total": float(row[8] or 0.0),
                "quote_taker_buy": float(row[9] or 0.0),
                "quote_taker_sell": float(row[10] or 0.0),
                "price_open": float(row[11]) if row[11] is not None else None,
                "price_high": float(row[12]) if row[12] is not None else None,
                "price_low": float(row[13]) if row[13] is not None else None,
                "price_close": float(row[14]) if row[14] is not None else None,
                "vwap": float(row[15]) if row[15] is not None else None,
                "avg_trade_size": float(row[16]) if row[16] is not None else None,
                "active_seconds": int(row[17] or 0),
                "coverage_ratio": float(row[18] or 0.0),
                "source": str(row[19] or "unknown"),
            }
        )
    return result
