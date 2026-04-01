"""Order book snapshot collection service.

Fetches top-10 bid/ask depth from Binance REST every minute and stores
pre-computed imbalance metrics for use as ML features.

Endpoint used:
  GET https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=10
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from app.core.db import DBConnection

_DEPTH_URL = "https://api.binance.com/api/v3/depth"
_TESTNET_DEPTH_URL = "https://testnet.binance.vision/api/v3/depth"
_DEPTH_LIMIT = 10
_TIMEOUT = 8
_RETRIES = 2
_BACKOFF = 1.0

ORDERBOOK_PAUSED_FILE = Path("runtime/orderbook.paused")


# ---------------------------------------------------------------------------
# Control helpers
# Collection is ON by default; only a pause file disables it.
# This means collection resumes automatically after every restart.
# ---------------------------------------------------------------------------

def is_orderbook_collection_enabled() -> bool:
    return not ORDERBOOK_PAUSED_FILE.exists()


def enable_orderbook_collection() -> None:
    """Resume collection by removing the pause flag."""
    if ORDERBOOK_PAUSED_FILE.exists():
        ORDERBOOK_PAUSED_FILE.unlink()


def disable_orderbook_collection() -> None:
    """Pause collection by writing the pause flag."""
    ORDERBOOK_PAUSED_FILE.parent.mkdir(parents=True, exist_ok=True)
    ORDERBOOK_PAUSED_FILE.write_text("paused\n", encoding="utf-8")


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _depth_url() -> str:
    if os.getenv("CRYPTO_BINANCE_TESTNET", "").strip().lower() in ("1", "true", "yes", "on"):
        return _TESTNET_DEPTH_URL
    return _DEPTH_URL


def fetch_orderbook_snapshot(symbol: str) -> Dict[str, Any]:
    """Fetch top-10 order book from Binance and compute metrics.

    Returns dict with keys:
        symbol, timestamp_ms, bids, asks,
        ob_imbalance, spread_pct, mid_price
    """
    params = {"symbol": symbol, "limit": _DEPTH_LIMIT}
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_TIMEOUT)))

    last_exc: Exception = RuntimeError("fetch_orderbook_snapshot: no attempts made")
    for attempt in range(_RETRIES + 1):
        try:
            resp = requests.get(_depth_url(), params=params, timeout=timeout)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as exc:
            last_exc = exc
            if attempt < _RETRIES:
                time.sleep(_BACKOFF * (2 ** attempt))
                continue
            raise last_exc

    bids: List[List[float]] = [[float(p), float(q)] for p, q in data.get("bids", [])]
    asks: List[List[float]] = [[float(p), float(q)] for p, q in data.get("asks", [])]

    bid_vol = sum(q for _, q in bids) if bids else 0.0
    ask_vol = sum(q for _, q in asks) if asks else 0.0
    total_vol = bid_vol + ask_vol
    ob_imbalance = (bid_vol - ask_vol) / total_vol if total_vol > 0 else 0.0

    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None
    if best_bid and best_ask:
        mid_price = (best_bid + best_ask) / 2.0
        spread_pct = (best_ask - best_bid) / mid_price
    else:
        mid_price = None
        spread_pct = None

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # Align to the start of the current minute
    timestamp_ms = (now_ms // 60_000) * 60_000

    return {
        "symbol":       symbol,
        "timestamp_ms": timestamp_ms,
        "bids":         bids,
        "asks":         asks,
        "ob_imbalance": round(ob_imbalance, 6),
        "spread_pct":   round(spread_pct, 8) if spread_pct is not None else None,
        "mid_price":    round(mid_price, 2) if mid_price is not None else None,
    }


# ---------------------------------------------------------------------------
# Persist
# ---------------------------------------------------------------------------

_INSERT_SQL = """
INSERT INTO order_book_snapshots
    (symbol, timestamp_ms, bids_json, asks_json, ob_imbalance, spread_pct, mid_price)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(symbol, timestamp_ms) DO NOTHING;
"""


def save_orderbook_snapshot(connection: DBConnection, snapshot: Dict[str, Any]) -> bool:
    """Insert snapshot into DB. Returns True if a new row was inserted."""
    connection.execute(
        _INSERT_SQL,
        (
            snapshot["symbol"],
            snapshot["timestamp_ms"],
            json.dumps(snapshot["bids"]),
            json.dumps(snapshot["asks"]),
            snapshot["ob_imbalance"],
            snapshot.get("spread_pct"),
            snapshot.get("mid_price"),
        ),
    )
    connection.commit()
    return True


# ---------------------------------------------------------------------------
# Stats for UI
# ---------------------------------------------------------------------------

def get_orderbook_stats(connection: DBConnection) -> List[Dict[str, Any]]:
    """Return per-symbol collection statistics."""
    rows = connection.execute(
        """
        SELECT symbol,
               COUNT(*)       AS total,
               MIN(timestamp_ms) AS earliest_ms,
               MAX(timestamp_ms) AS latest_ms
        FROM order_book_snapshots
        GROUP BY symbol
        ORDER BY symbol;
        """
    ).fetchall()

    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    result = []
    for row in rows:
        symbol, total, earliest_ms, latest_ms = (
            str(row[0]), int(row[1]), int(row[2]), int(row[3])
        )
        span_ms = latest_ms - earliest_ms
        # expected = one snapshot per minute over the span
        expected = max(1, round(span_ms / 60_000) + 1)
        coverage_pct = round(total / expected * 100, 1)
        stale_seconds = round((now_ms - latest_ms) / 1000)

        latest_iso = datetime.fromtimestamp(
            latest_ms / 1000, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M:%S")

        result.append({
            "symbol":        symbol,
            "total":         total,
            "coverage_pct":  coverage_pct,
            "latest":        latest_iso,
            "stale_seconds": stale_seconds,
            "is_stale":      stale_seconds > 180,  # >3 min = stale for 1m collection
        })
    return result


def get_recent_snapshots(
    connection: DBConnection,
    symbol: str,
    limit: int = 5,
) -> List[Dict[str, Any]]:
    """Return the most recent N snapshots for a symbol."""
    rows = connection.execute(
        """
        SELECT timestamp_ms, ob_imbalance, spread_pct, mid_price
        FROM order_book_snapshots
        WHERE symbol = ?
        ORDER BY timestamp_ms DESC
        LIMIT ?
        """,
        (symbol, limit),
    ).fetchall()

    result = []
    for row in rows:
        ts_ms, imb, spread, mid = row[0], row[1], row[2], row[3]
        result.append({
            "timestamp": datetime.fromtimestamp(
                ts_ms / 1000, tz=timezone.utc
            ).strftime("%H:%M"),
            "ob_imbalance": round(float(imb), 4) if imb is not None else None,
            "spread_pct":   round(float(spread) * 100, 4) if spread is not None else None,
            "mid_price":    float(mid) if mid is not None else None,
        })
    return result
