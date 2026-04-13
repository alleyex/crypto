"""Futures order book snapshot collection service.

Primary transport:
  Binance Futures WebSocket depth stream

Fallback transport:
  Binance Futures REST /fapi/v1/depth

The service keeps an in-memory per-symbol cache of the latest WS state.
Scheduler jobs persist one snapshot per minute for each configured symbol,
falling back to REST when the WS cache is stale or unavailable.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.db import DBConnection
from app.core.settings import FUTURES_ORDERBOOK_SYMBOLS
from app.core.settings import WS_RESTART_SECONDS as _WS_RESTART_SECONDS
from app.data.binance_config import futures_rest_base_url
from app.data.binance_config import futures_ws_base_url
from app.data.collection_state import CollectionState
from app.data.futures_collector_base import _BaseFuturesWSCollector
from app.data.retry_helpers import fetch_with_retry

_DEPTH_LIMIT = 10
_TIMEOUT = 8
_RETRIES = 2
_BACKOFF = 1.0
_WS_STALE_SECONDS = 75

FUTURES_ORDERBOOK_PAUSED_FILE = Path("runtime/futures_orderbook.paused")
_state = CollectionState(FUTURES_ORDERBOOK_PAUSED_FILE)

def configured_futures_orderbook_symbols() -> list[str]:
    return list(FUTURES_ORDERBOOK_SYMBOLS)

def is_futures_orderbook_collection_enabled() -> bool:
    return _state.is_enabled()

def enable_futures_orderbook_collection() -> None:
    _state.enable()

def disable_futures_orderbook_collection() -> None:
    _state.disable()

def _depth_url() -> str:
    return f"{futures_rest_base_url()}/fapi/v1/depth"

def _ws_url(symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@depth10@100ms" for symbol in symbols)
    return f"{futures_ws_base_url()}/stream?streams={streams}"

def _compute_snapshot_metrics(bids: list[list[float]], asks: list[list[float]]) -> tuple[float, float | None, float | None]:
    bid_vol = sum(q for _, q in bids) if bids else 0.0
    ask_vol = sum(q for _, q in asks) if asks else 0.0
    total_vol = bid_vol + ask_vol
    imbalance = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0.0

    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    if best_bid and best_ask:
        mid_price = (best_bid + best_ask) / 2.0
        spread_pct = (best_ask - best_bid) / mid_price
    else:
        mid_price = None
        spread_pct = None

    return imbalance, spread_pct, mid_price

def _build_snapshot_from_depth(
    symbol: str,
    bids_raw: list[list[Any]],
    asks_raw: list[list[Any]],
    timestamp_ms: int,
    *,
    source: str,
    sample_count: int = 0,
    first_event_ms: int | None = None,
    last_event_ms: int | None = None,
) -> dict[str, Any]:
    bids = [[float(p), float(q)] for p, q in bids_raw]
    asks = [[float(p), float(q)] for p, q in asks_raw]
    imbalance, spread_pct, mid_price = _compute_snapshot_metrics(bids, asks)
    minute_ms = (timestamp_ms // 60_000) * 60_000
    return {
        "symbol": symbol,
        "timestamp_ms": minute_ms,
        "bids": bids,
        "asks": asks,
        "ob_imbalance": round(imbalance, 6),
        "ob_imbalance_mean": round(imbalance, 6),
        "ob_imbalance_std": 0.0,
        "ob_imbalance_min": round(imbalance, 6),
        "ob_imbalance_max": round(imbalance, 6),
        "spread_pct": round(spread_pct, 8) if spread_pct is not None else None,
        "spread_pct_mean": round(spread_pct, 8) if spread_pct is not None else None,
        "spread_pct_max": round(spread_pct, 8) if spread_pct is not None else None,
        "spread_bps": round(spread_pct * 10_000.0, 4) if spread_pct is not None else None,
        "spread_bps_mean": round(spread_pct * 10_000.0, 4) if spread_pct is not None else None,
        "spread_bps_max": round(spread_pct * 10_000.0, 4) if spread_pct is not None else None,
        "mid_price": round(mid_price, 2) if mid_price is not None else None,
        "mid_price_mean": round(mid_price, 2) if mid_price is not None else None,
        "mid_price_min": round(mid_price, 2) if mid_price is not None else None,
        "mid_price_max": round(mid_price, 2) if mid_price is not None else None,
        "mid_price_ret_1m": None,
        "source": source,
        "sample_count": int(sample_count),
        "active_seconds": 1 if sample_count > 0 else 0,
        "coverage_ratio": round((1 / 60.0), 6) if sample_count > 0 else 0.0,
        "first_event_ms": int(first_event_ms or timestamp_ms),
        "last_event_ms": int(last_event_ms or timestamp_ms),
    }

def _new_bucket(
    symbol: str,
    minute_ms: int,
    bids: list[list[float]],
    asks: list[list[float]],
    imbalance: float,
    spread_pct: float | None,
    mid_price: float | None,
    event_ms: int,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "timestamp_ms": minute_ms,
        "bids": bids,
        "asks": asks,
        "ob_imbalance_last": imbalance,
        "ob_imbalance_sum": imbalance,
        "ob_imbalance_sumsq": imbalance * imbalance,
        "ob_imbalance_min": imbalance,
        "ob_imbalance_max": imbalance,
        "spread_pct_last": spread_pct,
        "spread_pct_sum": float(spread_pct or 0.0),
        "spread_pct_count": 1 if spread_pct is not None else 0,
        "spread_pct_max": spread_pct,
        "mid_price_last": mid_price,
        "mid_price_sum": float(mid_price or 0.0),
        "mid_price_count": 1 if mid_price is not None else 0,
        "mid_price_min": mid_price,
        "mid_price_max": mid_price,
        "sample_count": 1,
        "active_second_marks": {event_ms // 1000},
        "first_event_ms": event_ms,
        "last_event_ms": event_ms,
    }

def _update_bucket(
    bucket: dict[str, Any],
    bids: list[list[float]],
    asks: list[list[float]],
    imbalance: float,
    spread_pct: float | None,
    mid_price: float | None,
    event_ms: int,
) -> None:
    bucket["bids"] = bids
    bucket["asks"] = asks
    bucket["ob_imbalance_last"] = imbalance
    bucket["ob_imbalance_sum"] += imbalance
    bucket["ob_imbalance_sumsq"] += imbalance * imbalance
    bucket["ob_imbalance_min"] = min(float(bucket["ob_imbalance_min"]), imbalance)
    bucket["ob_imbalance_max"] = max(float(bucket["ob_imbalance_max"]), imbalance)
    if spread_pct is not None:
        bucket["spread_pct_last"] = spread_pct
        bucket["spread_pct_sum"] += spread_pct
        bucket["spread_pct_count"] += 1
        current_max = bucket["spread_pct_max"]
        bucket["spread_pct_max"] = spread_pct if current_max is None else max(float(current_max), spread_pct)
    if mid_price is not None:
        bucket["mid_price_last"] = mid_price
        bucket["mid_price_sum"] += mid_price
        bucket["mid_price_count"] += 1
        current_min = bucket["mid_price_min"]
        current_max = bucket["mid_price_max"]
        bucket["mid_price_min"] = mid_price if current_min is None else min(float(current_min), mid_price)
        bucket["mid_price_max"] = mid_price if current_max is None else max(float(current_max), mid_price)
    bucket["sample_count"] += 1
    bucket.setdefault("active_second_marks", set()).add(event_ms // 1000)
    bucket["last_event_ms"] = event_ms

def _snapshot_from_bucket(bucket: dict[str, Any], *, source: str) -> dict[str, Any]:
    n = int(bucket["sample_count"])
    imbalance_mean = float(bucket["ob_imbalance_sum"]) / n
    variance = max(0.0, float(bucket["ob_imbalance_sumsq"]) / n - imbalance_mean * imbalance_mean)
    spread_count = int(bucket["spread_pct_count"])
    mid_count = int(bucket["mid_price_count"])
    spread_mean = float(bucket["spread_pct_sum"]) / spread_count if spread_count > 0 else None
    mid_mean = float(bucket["mid_price_sum"]) / mid_count if mid_count > 0 else None
    spread_last = bucket["spread_pct_last"]
    mid_last = bucket["mid_price_last"]
    spread_bps_last = float(spread_last) * 10_000.0 if spread_last is not None else None
    spread_bps_mean = spread_mean * 10_000.0 if spread_mean is not None else None
    spread_bps_max = float(bucket["spread_pct_max"]) * 10_000.0 if bucket["spread_pct_max"] is not None else None
    active_seconds = len(bucket.get("active_second_marks", set()))
    coverage_ratio = 0.0
    first_event_ms = int(bucket["first_event_ms"])
    last_event_ms = int(bucket["last_event_ms"])
    if active_seconds > 0:
        coverage_ratio = min(1.0, max(0.0, active_seconds / 60.0))
    return {
        "symbol": bucket["symbol"],
        "timestamp_ms": int(bucket["timestamp_ms"]),
        "bids": bucket["bids"],
        "asks": bucket["asks"],
        "ob_imbalance": round(float(bucket["ob_imbalance_last"]), 6),
        "ob_imbalance_mean": round(imbalance_mean, 6),
        "ob_imbalance_std": round(variance ** 0.5, 6),
        "ob_imbalance_min": round(float(bucket["ob_imbalance_min"]), 6),
        "ob_imbalance_max": round(float(bucket["ob_imbalance_max"]), 6),
        "spread_pct": round(float(spread_last), 8) if spread_last is not None else None,
        "spread_pct_mean": round(spread_mean, 8) if spread_mean is not None else None,
        "spread_pct_max": round(float(bucket["spread_pct_max"]), 8) if bucket["spread_pct_max"] is not None else None,
        "spread_bps": round(spread_bps_last, 4) if spread_bps_last is not None else None,
        "spread_bps_mean": round(spread_bps_mean, 4) if spread_bps_mean is not None else None,
        "spread_bps_max": round(spread_bps_max, 4) if spread_bps_max is not None else None,
        "mid_price": round(float(mid_last), 2) if mid_last is not None else None,
        "mid_price_mean": round(mid_mean, 2) if mid_mean is not None else None,
        "mid_price_min": round(float(bucket["mid_price_min"]), 2) if bucket["mid_price_min"] is not None else None,
        "mid_price_max": round(float(bucket["mid_price_max"]), 2) if bucket["mid_price_max"] is not None else None,
        "mid_price_ret_1m": None,
        "source": source,
        "sample_count": n,
        "active_seconds": active_seconds,
        "coverage_ratio": round(coverage_ratio, 6),
        "first_event_ms": first_event_ms,
        "last_event_ms": last_event_ms,
    }

class _FuturesOrderBookCollector(_BaseFuturesWSCollector):
    _thread_name = "futures-orderbook-ws"

    def __init__(self) -> None:
        super().__init__()
        self._snapshots: dict[str, dict[str, Any]] = {}

    def _get_ws_url(self, symbols: tuple[str, ...]) -> str:
        return _ws_url(list(symbols))

    def _handle_message(self, message: str, symbols: tuple[str, ...]) -> None:
        payload = json.loads(message)
        data = payload.get("data", payload)
        symbol = str(data.get("s") or "").upper()
        bids = data.get("b") or []
        asks = data.get("a") or []
        if not symbol or not bids or not asks:
            return
        event_ms = int(data.get("E") or int(datetime.now(timezone.utc).timestamp() * 1000))
        bids_f = [[float(p), float(q)] for p, q in bids]
        asks_f = [[float(p), float(q)] for p, q in asks]
        imbalance, spread_pct, mid_price = _compute_snapshot_metrics(bids_f, asks_f)
        with self._lock:
            prev = self._snapshots.get(symbol)
            curr_minute = (event_ms // 60_000) * 60_000
            if prev is None or int(prev["timestamp_ms"]) != curr_minute:
                self._snapshots[symbol] = _new_bucket(
                    symbol,
                    curr_minute,
                    bids_f,
                    asks_f,
                    imbalance,
                    spread_pct,
                    mid_price,
                    event_ms,
                )
            else:
                _update_bucket(prev, bids_f, asks_f, imbalance, spread_pct, mid_price, event_ms)

    def get_minute_snapshot(self, symbol: str, now_ms: int | None = None) -> dict[str, Any] | None:
        now_ms = int(now_ms or datetime.now(timezone.utc).timestamp() * 1000)
        minute_ms = (now_ms // 60_000) * 60_000
        with self._lock:
            snapshot = self._snapshots.get(symbol.upper())
            if snapshot is None:
                return None
            if int(snapshot.get("timestamp_ms", 0)) != minute_ms:
                return None
            age_seconds = max(0, (now_ms - int(snapshot.get("last_event_ms", 0))) / 1000)
            if age_seconds > _WS_STALE_SECONDS:
                return None
            return _snapshot_from_bucket(snapshot, source="ws")

    def runtime_status(self) -> dict[str, Any]:
        with self._lock:
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            symbol_runtime: dict[str, dict[str, Any]] = {}
            for symbol, snapshot in self._snapshots.items():
                last_event_ms = int(snapshot.get("last_event_ms", 0) or 0)
                symbol_runtime[symbol] = {
                    "current_minute": int(snapshot.get("timestamp_ms", 0) or 0),
                    "sample_count": int(snapshot.get("sample_count", 0) or 0),
                    "first_event_ms": int(snapshot.get("first_event_ms", 0) or 0),
                    "last_event_ms": last_event_ms,
                    "event_age_seconds": max(0, round((now_ms - last_event_ms) / 1000)) if last_event_ms else None,
                }
            return {
                "symbols": list(self._symbols),
                "ws_available": self._ws_available,
                "last_error": self._last_error,
                "cached_symbols": sorted(self._snapshots.keys()),
                "symbol_runtime": symbol_runtime,
            }

_COLLECTOR = _FuturesOrderBookCollector()

def reset_futures_orderbook_runtime(symbols: list[str] | None = None) -> dict[str, Any]:
    global _COLLECTOR
    _COLLECTOR = _FuturesOrderBookCollector()
    if symbols:
        _COLLECTOR.ensure_started(list(symbols))
    return _COLLECTOR.runtime_status()

def fetch_futures_orderbook_snapshot(symbol: str) -> dict[str, Any]:
    params = {"symbol": symbol, "limit": _DEPTH_LIMIT}
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))

    data = fetch_with_retry(_depth_url(), params=params, timeout=timeout, retries=_RETRIES, backoff=_BACKOFF).json()
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    return _build_snapshot_from_depth(
        symbol,
        data.get("bids", []),
        data.get("asks", []),
        now_ms,
        source="rest",
        sample_count=1,
        first_event_ms=now_ms,
        last_event_ms=now_ms,
    )

_UPSERT_SQL = """
INSERT INTO futures_order_book_snapshots
    (symbol, timestamp_ms, bids_json, asks_json, ob_imbalance,
     ob_imbalance_mean, ob_imbalance_std, ob_imbalance_min, ob_imbalance_max,
     spread_pct, spread_pct_mean, spread_pct_max,
     spread_bps, spread_bps_mean, spread_bps_max,
     mid_price, mid_price_mean, mid_price_min, mid_price_max, mid_price_ret_1m,
     source, sample_count, active_seconds, coverage_ratio, first_event_ms, last_event_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    bids_json = excluded.bids_json,
    asks_json = excluded.asks_json,
    ob_imbalance = excluded.ob_imbalance,
    ob_imbalance_mean = excluded.ob_imbalance_mean,
    ob_imbalance_std = excluded.ob_imbalance_std,
    ob_imbalance_min = excluded.ob_imbalance_min,
    ob_imbalance_max = excluded.ob_imbalance_max,
    spread_pct = excluded.spread_pct,
    spread_pct_mean = excluded.spread_pct_mean,
    spread_pct_max = excluded.spread_pct_max,
    spread_bps = excluded.spread_bps,
    spread_bps_mean = excluded.spread_bps_mean,
    spread_bps_max = excluded.spread_bps_max,
    mid_price = excluded.mid_price,
    mid_price_mean = excluded.mid_price_mean,
    mid_price_min = excluded.mid_price_min,
    mid_price_max = excluded.mid_price_max,
    mid_price_ret_1m = excluded.mid_price_ret_1m,
    source = excluded.source,
    sample_count = excluded.sample_count,
    active_seconds = excluded.active_seconds,
    coverage_ratio = excluded.coverage_ratio,
    first_event_ms = excluded.first_event_ms,
    last_event_ms = excluded.last_event_ms;
"""

def _compute_mid_price_ret_1m(connection: DBConnection, symbol: str, timestamp_ms: int, mid_price: float | None) -> float | None:
    if mid_price is None or mid_price <= 0:
        return None
    row = connection.execute(
        """
        SELECT mid_price
        FROM futures_order_book_snapshots
        WHERE symbol = ? AND timestamp_ms < ?
        ORDER BY timestamp_ms DESC
        LIMIT 1
        """,
        (symbol, timestamp_ms),
    ).fetchone()
    if row is None or row[0] is None:
        return None
    prev_mid = float(row[0])
    if prev_mid <= 0:
        return None
    return round((mid_price / prev_mid) - 1.0, 8)

def save_futures_orderbook_snapshot(connection: DBConnection, snapshot: dict[str, Any]) -> bool:
    snapshot = dict(snapshot)
    if snapshot.get("spread_bps") is None and snapshot.get("spread_pct") is not None:
        snapshot["spread_bps"] = round(float(snapshot["spread_pct"]) * 10_000.0, 4)
    if snapshot.get("spread_bps_mean") is None and snapshot.get("spread_pct_mean") is not None:
        snapshot["spread_bps_mean"] = round(float(snapshot["spread_pct_mean"]) * 10_000.0, 4)
    if snapshot.get("spread_bps_max") is None and snapshot.get("spread_pct_max") is not None:
        snapshot["spread_bps_max"] = round(float(snapshot["spread_pct_max"]) * 10_000.0, 4)
    if snapshot.get("coverage_ratio") is None:
        active_seconds = snapshot.get("active_seconds")
        if active_seconds is not None:
            snapshot["coverage_ratio"] = round(min(1.0, max(0.0, int(active_seconds) / 60.0)), 6)
        else:
            first_event_ms = snapshot.get("first_event_ms")
            last_event_ms = snapshot.get("last_event_ms")
            if first_event_ms is not None and last_event_ms is not None and int(last_event_ms) >= int(first_event_ms):
                snapshot["coverage_ratio"] = round(
                    min(1.0, max(0.0, (int(last_event_ms) - int(first_event_ms)) / 60_000.0)),
                    6,
                )
            else:
                snapshot["coverage_ratio"] = 0.0
    if snapshot.get("active_seconds") is None:
        first_event_ms = snapshot.get("first_event_ms")
        last_event_ms = snapshot.get("last_event_ms")
        if snapshot.get("coverage_ratio") is not None:
            snapshot["active_seconds"] = max(0, min(60, int(round(float(snapshot["coverage_ratio"]) * 60))))
        elif first_event_ms is not None and last_event_ms is not None and int(last_event_ms) >= int(first_event_ms):
            snapshot["active_seconds"] = max(0, min(60, int(round((int(last_event_ms) - int(first_event_ms)) / 1000.0))))
        else:
            snapshot["active_seconds"] = 0
    snapshot["mid_price_ret_1m"] = _compute_mid_price_ret_1m(
        connection,
        str(snapshot["symbol"]),
        int(snapshot["timestamp_ms"]),
        float(snapshot["mid_price"]) if snapshot.get("mid_price") is not None else None,
    )
    connection.execute(
        _UPSERT_SQL,
        (
            snapshot["symbol"],
            snapshot["timestamp_ms"],
            json.dumps(snapshot["bids"]),
            json.dumps(snapshot["asks"]),
            snapshot["ob_imbalance"],
            snapshot.get("ob_imbalance_mean"),
            snapshot.get("ob_imbalance_std"),
            snapshot.get("ob_imbalance_min"),
            snapshot.get("ob_imbalance_max"),
            snapshot.get("spread_pct"),
            snapshot.get("spread_pct_mean"),
            snapshot.get("spread_pct_max"),
            snapshot.get("spread_bps"),
            snapshot.get("spread_bps_mean"),
            snapshot.get("spread_bps_max"),
            snapshot.get("mid_price"),
            snapshot.get("mid_price_mean"),
            snapshot.get("mid_price_min"),
            snapshot.get("mid_price_max"),
            snapshot.get("mid_price_ret_1m"),
            snapshot.get("source", "rest"),
            int(snapshot.get("sample_count", 0)),
            int(snapshot.get("active_seconds", 0)),
            snapshot.get("coverage_ratio"),
            snapshot.get("first_event_ms"),
            snapshot.get("last_event_ms"),
        ),
    )
    connection.commit()
    return True

def collect_futures_orderbook_snapshots(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> dict[str, Any]:
    symbols = list(symbol_names or configured_futures_orderbook_symbols())
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    _COLLECTOR.ensure_started(symbols)

    saved = 0
    errors: list[dict[str, str]] = []
    source_counts = {"ws": 0, "rest": 0}

    for symbol in symbols:
        try:
            snapshot = _COLLECTOR.get_minute_snapshot(symbol, now_ms=now_ms)
            if snapshot is None:
                snapshot = fetch_futures_orderbook_snapshot(symbol)
            save_futures_orderbook_snapshot(connection, snapshot)
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

def get_futures_orderbook_stats(
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
        FROM futures_order_book_snapshots
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
                    if last_event_ms
                    else None
                ),
                "event_age_seconds": event_age_seconds,
                "current_minute_sample_count": int(symbol_runtime.get("sample_count", 0) or 0),
            }
        )
    return result

def get_recent_futures_orderbook_snapshots(
    connection: DBConnection,
    symbol: str,
    limit: int = 5,
) -> list[dict[str, Any]]:
    rows = connection.execute(
        """
        SELECT symbol,
               timestamp_ms,
               source,
               sample_count,
               active_seconds,
               coverage_ratio,
               ob_imbalance,
               ob_imbalance_mean,
               ob_imbalance_std,
               spread_bps,
               spread_bps_mean,
               spread_bps_max,
               mid_price,
               mid_price_mean,
               mid_price_ret_1m,
               first_event_ms,
               last_event_ms,
               bids_json,
               asks_json
        FROM futures_order_book_snapshots
        WHERE symbol = ?
        ORDER BY timestamp_ms DESC
        LIMIT ?
        """,
        (str(symbol).upper(), int(limit)),
    ).fetchall()

    result: list[dict[str, Any]] = []
    for row in rows:
        bids = json.loads(row[17]) if row[17] else []
        asks = json.loads(row[18]) if row[18] else []
        best_bid = float(bids[0][0]) if bids else None
        best_ask = float(asks[0][0]) if asks else None
        result.append(
            {
                "symbol": str(row[0]),
                "timestamp": datetime.fromtimestamp(int(row[1]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "source": str(row[2] or "unknown"),
                "sample_count": int(row[3] or 0),
                "active_seconds": int(row[4] or 0),
                "coverage_ratio": float(row[5] or 0.0),
                "ob_imbalance": float(row[6] or 0.0),
                "ob_imbalance_mean": float(row[7] or 0.0),
                "ob_imbalance_std": float(row[8] or 0.0),
                "spread_bps": float(row[9]) if row[9] is not None else None,
                "spread_bps_mean": float(row[10]) if row[10] is not None else None,
                "spread_bps_max": float(row[11]) if row[11] is not None else None,
                "mid_price": float(row[12]) if row[12] is not None else None,
                "mid_price_mean": float(row[13]) if row[13] is not None else None,
                "mid_price_ret_1m": float(row[14]) if row[14] is not None else None,
                "first_event_at": (
                    datetime.fromtimestamp(int(row[15]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    if row[15] is not None else None
                ),
                "last_event_at": (
                    datetime.fromtimestamp(int(row[16]) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                    if row[16] is not None else None
                ),
                "best_bid": best_bid,
                "best_ask": best_ask,
            }
        )
    return result
