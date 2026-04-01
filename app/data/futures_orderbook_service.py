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
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import requests

from app.core.db import DBConnection
from app.core.settings import FUTURES_ORDERBOOK_SYMBOLS

_FUTURES_DEPTH_URL = "https://fapi.binance.com/fapi/v1/depth"
_FUTURES_TESTNET_DEPTH_URL = "https://testnet.binancefuture.com/fapi/v1/depth"
_FUTURES_WS_URL = "wss://fstream.binance.com/stream?streams={streams}"
_FUTURES_TESTNET_WS_URL = "wss://stream.binancefuture.com/stream?streams={streams}"
_DEPTH_LIMIT = 10
_TIMEOUT = 8
_RETRIES = 2
_BACKOFF = 1.0
_WS_STALE_SECONDS = 75
_WS_RESTART_SECONDS = 15

FUTURES_ORDERBOOK_PAUSED_FILE = Path("runtime/futures_orderbook.paused")


def configured_futures_orderbook_symbols() -> list[str]:
    return list(FUTURES_ORDERBOOK_SYMBOLS)


def is_futures_orderbook_collection_enabled() -> bool:
    return not FUTURES_ORDERBOOK_PAUSED_FILE.exists()


def enable_futures_orderbook_collection() -> None:
    if FUTURES_ORDERBOOK_PAUSED_FILE.exists():
        FUTURES_ORDERBOOK_PAUSED_FILE.unlink()


def disable_futures_orderbook_collection() -> None:
    FUTURES_ORDERBOOK_PAUSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    FUTURES_ORDERBOOK_PAUSED_FILE.write_text("paused\n", encoding="utf-8")


def _is_testnet() -> bool:
    return os.getenv("CRYPTO_BINANCE_TESTNET", "").strip().lower() in ("1", "true", "yes", "on")


def _depth_url() -> str:
    return _FUTURES_TESTNET_DEPTH_URL if _is_testnet() else _FUTURES_DEPTH_URL


def _ws_url(symbols: list[str]) -> str:
    streams = "/".join(f"{symbol.lower()}@depth10@100ms" for symbol in symbols)
    base = _FUTURES_TESTNET_WS_URL if _is_testnet() else _FUTURES_WS_URL
    return base.format(streams=streams)


def _compute_snapshot_metrics(bids: list[list[float]], asks: list[list[float]]) -> tuple[float, Optional[float], Optional[float]]:
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
    last_event_ms: Optional[int] = None,
) -> Dict[str, Any]:
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
        "spread_pct": round(spread_pct, 8) if spread_pct is not None else None,
        "mid_price": round(mid_price, 2) if mid_price is not None else None,
        "source": source,
        "sample_count": int(sample_count),
        "last_event_ms": int(last_event_ms or timestamp_ms),
    }


class _FuturesOrderBookCollector:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: Optional[threading.Thread] = None
        self._symbols: tuple[str, ...] = ()
        self._snapshots: dict[str, Dict[str, Any]] = {}
        self._last_error: Optional[str] = None
        self._ws_available = True

    def ensure_started(self, symbols: list[str]) -> bool:
        symbols_key = tuple(sorted(set(symbols)))
        with self._lock:
            if self._thread is not None and self._thread.is_alive() and self._symbols == symbols_key:
                return self._ws_available
            self._symbols = symbols_key
            self._thread = threading.Thread(
                target=self._run_forever,
                args=(list(symbols_key),),
                name="futures-orderbook-ws",
                daemon=True,
            )
            self._thread.start()
            return self._ws_available

    def _run_forever(self, symbols: list[str]) -> None:
        try:
            from websocket import WebSocketApp
        except Exception as exc:
            with self._lock:
                self._last_error = f"websocket unavailable: {exc}"
                self._ws_available = False
            return

        def on_message(_: Any, message: str) -> None:
            try:
                payload = json.loads(message)
                data = payload.get("data", payload)
                symbol = str(data.get("s") or "").upper()
                bids = data.get("b") or []
                asks = data.get("a") or []
                if not symbol or not bids or not asks:
                    return
                event_ms = int(data.get("E") or int(datetime.now(timezone.utc).timestamp() * 1000))
                with self._lock:
                    prev = self._snapshots.get(symbol)
                    prev_minute = prev["timestamp_ms"] if prev else None
                    curr_minute = (event_ms // 60_000) * 60_000
                    sample_count = 1
                    if prev is not None and prev_minute == curr_minute:
                        sample_count = int(prev.get("sample_count", 0)) + 1
                    self._snapshots[symbol] = _build_snapshot_from_depth(
                        symbol,
                        bids,
                        asks,
                        event_ms,
                        source="ws",
                        sample_count=sample_count,
                        last_event_ms=event_ms,
                    )
            except Exception as exc:
                with self._lock:
                    self._last_error = f"on_message error: {exc}"

        def on_error(_: Any, error: Any) -> None:
            with self._lock:
                self._last_error = str(error)

        while True:
            try:
                ws = WebSocketApp(_ws_url(symbols), on_message=on_message, on_error=on_error)
                with self._lock:
                    self._ws_available = True
                    self._last_error = None
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                with self._lock:
                    self._last_error = f"ws loop error: {exc}"
                    self._ws_available = False
            time.sleep(_WS_RESTART_SECONDS)

    def get_minute_snapshot(self, symbol: str, now_ms: Optional[int] = None) -> Optional[Dict[str, Any]]:
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
            return dict(snapshot)

    def runtime_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "symbols": list(self._symbols),
                "ws_available": self._ws_available,
                "last_error": self._last_error,
                "cached_symbols": sorted(self._snapshots.keys()),
            }


_COLLECTOR = _FuturesOrderBookCollector()


def fetch_futures_orderbook_snapshot(symbol: str) -> Dict[str, Any]:
    params = {"symbol": symbol, "limit": _DEPTH_LIMIT}
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))

    last_exc: Exception = RuntimeError("fetch_futures_orderbook_snapshot: no attempts made")
    for attempt in range(_RETRIES + 1):
        try:
            resp = requests.get(_depth_url(), params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
            return _build_snapshot_from_depth(
                symbol,
                data.get("bids", []),
                data.get("asks", []),
                now_ms,
                source="rest",
                sample_count=0,
                last_event_ms=now_ms,
            )
        except Exception as exc:
            last_exc = exc
            if attempt < _RETRIES:
                time.sleep(_BACKOFF * (2 ** attempt))
                continue
            raise last_exc


_UPSERT_SQL = """
INSERT INTO futures_order_book_snapshots
    (symbol, timestamp_ms, bids_json, asks_json, ob_imbalance, spread_pct, mid_price,
     source, sample_count, last_event_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    bids_json = excluded.bids_json,
    asks_json = excluded.asks_json,
    ob_imbalance = excluded.ob_imbalance,
    spread_pct = excluded.spread_pct,
    mid_price = excluded.mid_price,
    source = excluded.source,
    sample_count = excluded.sample_count,
    last_event_ms = excluded.last_event_ms;
"""


def save_futures_orderbook_snapshot(connection: DBConnection, snapshot: Dict[str, Any]) -> bool:
    connection.execute(
        _UPSERT_SQL,
        (
            snapshot["symbol"],
            snapshot["timestamp_ms"],
            json.dumps(snapshot["bids"]),
            json.dumps(snapshot["asks"]),
            snapshot["ob_imbalance"],
            snapshot.get("spread_pct"),
            snapshot.get("mid_price"),
            snapshot.get("source", "rest"),
            int(snapshot.get("sample_count", 0)),
            snapshot.get("last_event_ms"),
        ),
    )
    connection.commit()
    return True


def collect_futures_orderbook_snapshots(
    connection: DBConnection,
    symbol_names: Optional[list[str]] = None,
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


def get_futures_orderbook_stats(connection: DBConnection) -> List[Dict[str, Any]]:
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
        span_ms = latest_ms - earliest_ms
        expected = max(1, round(span_ms / 60_000) + 1)
        coverage_pct = round(total / expected * 100, 1)
        stale_seconds = round((now_ms - latest_ms) / 1000)
        result.append(
            {
                "symbol": symbol,
                "total": total,
                "coverage_pct": coverage_pct,
                "latest": datetime.fromtimestamp(latest_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                "stale_seconds": stale_seconds,
                "is_stale": stale_seconds > 180,
                "latest_source": str(latest_source or "unknown"),
            }
        )
    return result
