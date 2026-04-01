from __future__ import annotations

import sqlite3
from datetime import datetime
from datetime import timezone

from app.core.migrations import run_migrations
from app.data import futures_orderbook_service as svc


def test_collect_futures_orderbook_snapshots_uses_rest_fallback(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)

    now_ms = 1_800_000

    monkeypatch.setattr(
        svc,
        "_COLLECTOR",
        type(
            "CollectorStub",
            (),
            {
                "ensure_started": lambda self, symbols: False,
                "get_minute_snapshot": lambda self, symbol, now_ms=None: None,
                "runtime_status": lambda self: {"ws_available": False},
            },
        )(),
    )
    monkeypatch.setattr(
        svc,
        "fetch_futures_orderbook_snapshot",
        lambda symbol: {
            "symbol": symbol,
            "timestamp_ms": now_ms,
            "bids": [[100.0, 2.0]],
            "asks": [[100.1, 1.0]],
            "ob_imbalance": 0.333333,
            "spread_pct": 0.001,
            "mid_price": 100.05,
            "source": "rest",
            "sample_count": 0,
            "last_event_ms": now_ms,
        },
    )

    result = svc.collect_futures_orderbook_snapshots(conn, ["BTCUSDT"])

    row = conn.execute(
        """
        SELECT symbol, source, sample_count
        FROM futures_order_book_snapshots
        WHERE symbol = 'BTCUSDT'
        """
    ).fetchone()
    assert result["saved"] == 1
    assert result["source_counts"]["rest"] == 1
    assert row == ("BTCUSDT", "rest", 0)


def test_collect_futures_orderbook_snapshots_prefers_fresh_ws_cache(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)

    now_ms = int(datetime(2026, 4, 1, 1, 40, tzinfo=timezone.utc).timestamp() * 1000)
    minute_ms = (now_ms // 60_000) * 60_000
    ws_snapshot = {
        "symbol": "ETHUSDT",
        "timestamp_ms": minute_ms,
        "bids": [[2000.0, 4.0]],
        "asks": [[2000.2, 3.0]],
        "ob_imbalance": 0.142857,
        "spread_pct": 0.0001,
        "mid_price": 2000.1,
        "source": "ws",
        "sample_count": 7,
        "last_event_ms": now_ms,
    }

    class CollectorStub:
        def ensure_started(self, symbols: list[str]) -> bool:
            return True

        def get_minute_snapshot(self, symbol: str, now_ms: int | None = None):
            return dict(ws_snapshot)

        def runtime_status(self):
            return {"ws_available": True}

    monkeypatch.setattr(svc, "_COLLECTOR", CollectorStub())

    def _unexpected_fetch(symbol: str):
        raise AssertionError("REST fallback should not be used when WS cache is fresh")

    monkeypatch.setattr(svc, "fetch_futures_orderbook_snapshot", _unexpected_fetch)

    result = svc.collect_futures_orderbook_snapshots(conn, ["ETHUSDT"])

    row = conn.execute(
        """
        SELECT symbol, source, sample_count, last_event_ms
        FROM futures_order_book_snapshots
        WHERE symbol = 'ETHUSDT'
        """
    ).fetchone()
    assert result["saved"] == 1
    assert result["source_counts"]["ws"] == 1
    assert row == ("ETHUSDT", "ws", 7, now_ms)
