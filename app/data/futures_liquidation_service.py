"""Futures liquidation minute collection service."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.db import DBConnection
from app.core.settings import FUTURES_LIQUIDATION_SYMBOLS
from app.core.settings import WS_RESTART_SECONDS as _WS_RESTART_SECONDS
from app.data.binance_config import futures_ws_base_url
from app.data.collection_state import CollectionState
from app.data.futures_collector_base import _BaseFuturesWSCollector

_WS_STALE_SECONDS = 75

FUTURES_LIQUIDATION_PAUSED_FILE = Path("runtime/futures_liquidation.paused")
_state = CollectionState(FUTURES_LIQUIDATION_PAUSED_FILE)

def configured_futures_liquidation_symbols() -> list[str]:
    return list(FUTURES_LIQUIDATION_SYMBOLS)

def is_futures_liquidation_collection_enabled() -> bool:
    return _state.is_enabled()

def enable_futures_liquidation_collection() -> None:
    _state.enable()

def disable_futures_liquidation_collection() -> None:
    _state.disable()

def _ws_url() -> str:
    return f"{futures_ws_base_url()}/ws/!forceOrder@arr"

def _new_bucket(symbol: str, minute_ms: int, event_ms: int, side: str, qty: float, price: float) -> dict[str, Any]:
    quote = qty * price
    buy_count = 1 if side == "BUY" else 0
    sell_count = 1 if side == "SELL" else 0
    return {
        "symbol": symbol,
        "timestamp_ms": minute_ms,
        "event_count": 1,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "qty_total": qty,
        "qty_buy": qty if side == "BUY" else 0.0,
        "qty_sell": qty if side == "SELL" else 0.0,
        "quote_total": quote,
        "quote_buy": quote if side == "BUY" else 0.0,
        "quote_sell": quote if side == "SELL" else 0.0,
        "price_qty_sum": price * qty,
        "max_quote": quote,
        "first_event_ms": event_ms,
        "last_event_ms": event_ms,
        "active_second_marks": {event_ms // 1000},
    }

def _update_bucket(bucket: dict[str, Any], event_ms: int, side: str, qty: float, price: float) -> None:
    quote = qty * price
    bucket["event_count"] += 1
    bucket["qty_total"] += qty
    bucket["quote_total"] += quote
    bucket["price_qty_sum"] += price * qty
    bucket["max_quote"] = max(float(bucket["max_quote"]), quote)
    if side == "BUY":
        bucket["buy_count"] += 1
        bucket["qty_buy"] += qty
        bucket["quote_buy"] += quote
    else:
        bucket["sell_count"] += 1
        bucket["qty_sell"] += qty
        bucket["quote_sell"] += quote
    bucket["last_event_ms"] = event_ms
    bucket.setdefault("active_second_marks", set()).add(event_ms // 1000)

def _snapshot_from_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    qty_total = float(bucket["qty_total"])
    active_seconds = len(bucket.get("active_second_marks", set()))
    coverage_ratio = round(min(1.0, max(0.0, active_seconds / 60.0)), 6) if active_seconds > 0 else 0.0
    avg_price = (float(bucket["price_qty_sum"]) / qty_total) if qty_total > 0 else None
    return {
        "symbol": bucket["symbol"],
        "timestamp_ms": int(bucket["timestamp_ms"]),
        "event_count": int(bucket["event_count"]),
        "buy_count": int(bucket["buy_count"]),
        "sell_count": int(bucket["sell_count"]),
        "qty_total": round(qty_total, 8),
        "qty_buy": round(float(bucket["qty_buy"]), 8),
        "qty_sell": round(float(bucket["qty_sell"]), 8),
        "quote_total": round(float(bucket["quote_total"]), 8),
        "quote_buy": round(float(bucket["quote_buy"]), 8),
        "quote_sell": round(float(bucket["quote_sell"]), 8),
        "avg_price": round(avg_price, 8) if avg_price is not None else None,
        "max_quote": round(float(bucket["max_quote"]), 8),
        "first_event_ms": int(bucket["first_event_ms"]),
        "last_event_ms": int(bucket["last_event_ms"]),
        "active_seconds": active_seconds,
        "coverage_ratio": coverage_ratio,
        "source": "ws",
    }

class _FuturesLiquidationCollector(_BaseFuturesWSCollector):
    _thread_name = "futures-liquidation-ws"

    def __init__(self) -> None:
        super().__init__()
        self._buckets: dict[str, dict[str, Any]] = {}

    def _get_ws_url(self, symbols: tuple[str, ...]) -> str:
        return _ws_url()

    def _handle_message(self, message: str, symbols: tuple[str, ...]) -> None:
        symbol_filter = set(symbols)
        payload = json.loads(message)
        rows = payload.get("data", payload)
        events = rows if isinstance(rows, list) else [rows]
        for event in events:
            order = event.get("o") or {}
            symbol = str(order.get("s") or "").upper()
            if not symbol or symbol not in symbol_filter:
                continue
            event_ms = int(event.get("E") or order.get("T") or int(datetime.now(timezone.utc).timestamp() * 1000))
            minute_ms = (event_ms // 60_000) * 60_000
            side = str(order.get("S") or "").upper()
            qty = float(order.get("q") or 0.0)
            price = float(order.get("p") or order.get("ap") or 0.0)
            if side not in {"BUY", "SELL"} or qty <= 0 or price <= 0:
                continue
            with self._lock:
                prev = self._buckets.get(symbol)
                if prev is None or int(prev["timestamp_ms"]) != minute_ms:
                    self._buckets[symbol] = _new_bucket(symbol, minute_ms, event_ms, side, qty, price)
                else:
                    _update_bucket(prev, event_ms, side, qty, price)

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
            return _snapshot_from_bucket(bucket)

    def runtime_status(self) -> dict[str, Any]:
        with self._lock:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            symbol_runtime: dict[str, dict[str, Any]] = {}
            for symbol, bucket in self._buckets.items():
                last_event_ms = int(bucket.get("last_event_ms", 0) or 0)
                symbol_runtime[symbol] = {
                    "current_minute": int(bucket.get("timestamp_ms", 0) or 0),
                    "event_count": int(bucket.get("event_count", 0) or 0),
                    "active_seconds": len(bucket.get("active_second_marks", set())),
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

_COLLECTOR = _FuturesLiquidationCollector()

def reset_futures_liquidation_runtime(symbols: list[str] | None = None) -> dict[str, Any]:
    global _COLLECTOR
    _COLLECTOR = _FuturesLiquidationCollector()
    if symbols:
        _COLLECTOR.ensure_started(list(symbols))
    return _COLLECTOR.runtime_status()

_UPSERT_SQL = """
INSERT INTO futures_liquidation_minutes
    (symbol, timestamp_ms, event_count, buy_count, sell_count,
     qty_total, qty_buy, qty_sell, quote_total, quote_buy, quote_sell,
     avg_price, max_quote, first_event_ms, last_event_ms, active_seconds,
     coverage_ratio, source)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    event_count = excluded.event_count,
    buy_count = excluded.buy_count,
    sell_count = excluded.sell_count,
    qty_total = excluded.qty_total,
    qty_buy = excluded.qty_buy,
    qty_sell = excluded.qty_sell,
    quote_total = excluded.quote_total,
    quote_buy = excluded.quote_buy,
    quote_sell = excluded.quote_sell,
    avg_price = excluded.avg_price,
    max_quote = excluded.max_quote,
    first_event_ms = excluded.first_event_ms,
    last_event_ms = excluded.last_event_ms,
    active_seconds = excluded.active_seconds,
    coverage_ratio = excluded.coverage_ratio,
    source = excluded.source;
"""

def save_futures_liquidation_snapshot(connection: DBConnection, snapshot: dict[str, Any]) -> bool:
    connection.execute(
        _UPSERT_SQL,
        (
            snapshot["symbol"],
            snapshot["timestamp_ms"],
            snapshot["event_count"],
            snapshot["buy_count"],
            snapshot["sell_count"],
            snapshot["qty_total"],
            snapshot["qty_buy"],
            snapshot["qty_sell"],
            snapshot["quote_total"],
            snapshot["quote_buy"],
            snapshot["quote_sell"],
            snapshot.get("avg_price"),
            snapshot.get("max_quote"),
            snapshot["first_event_ms"],
            snapshot["last_event_ms"],
            snapshot.get("active_seconds", 0),
            snapshot.get("coverage_ratio", 0.0),
            snapshot.get("source", "ws"),
        ),
    )
    connection.commit()
    return True

def collect_futures_liquidation_minutes(connection: DBConnection, symbol_names: list[str] | None = None) -> dict[str, Any]:
    symbols = list(symbol_names or configured_futures_liquidation_symbols())
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    _COLLECTOR.ensure_started(symbols)

    saved = 0
    errors: list[dict[str, str]] = []
    source_counts = {"ws": 0}
    for symbol in symbols:
        try:
            snapshot = _COLLECTOR.get_minute_snapshot(symbol, now_ms=now_ms)
            if snapshot is None:
                continue
            save_futures_liquidation_snapshot(connection, snapshot)
            source_counts["ws"] += 1
            saved += 1
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)})
    return {
        "saved": saved,
        "errors": errors,
        "source_counts": source_counts,
        "collector": _COLLECTOR.runtime_status(),
    }

def get_futures_liquidation_stats(connection: DBConnection, runtime: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    runtime = runtime if runtime is not None else _COLLECTOR.runtime_status().get("symbol_runtime", {})
    rows = connection.execute(
        """
        SELECT symbol,
               COUNT(*) AS total,
               MIN(timestamp_ms) AS earliest_ms,
               MAX(timestamp_ms) AS latest_ms,
               MAX(source) AS latest_source
        FROM futures_liquidation_minutes
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
        symbol_runtime = runtime.get(str(symbol), {})
        last_event_ms = symbol_runtime.get("last_event_ms")
        event_age_seconds = None
        if last_event_ms:
            event_age_seconds = max(0, round((now_ms - int(last_event_ms)) / 1000))
        result.append(
            {
                "symbol": str(symbol),
                "total": total,
                "coverage_pct": coverage_pct,
                "latest": datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "stale_seconds": stale_seconds,
                "is_stale": stale_seconds > 180,
                "latest_source": str(latest_source or "unknown"),
                "last_event_at": (
                    datetime.fromtimestamp(int(last_event_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    if last_event_ms else None
                ),
                "event_age_seconds": event_age_seconds,
                "current_minute_event_count": int(symbol_runtime.get("event_count", 0) or 0),
                "current_minute_active_seconds": int(symbol_runtime.get("active_seconds", 0) or 0),
            }
        )
    return result
