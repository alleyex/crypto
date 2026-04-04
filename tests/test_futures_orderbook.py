from __future__ import annotations

import sqlite3
from datetime import datetime
from datetime import timezone

from app.core.migrations import run_migrations
from app.data import futures_orderbook_service as svc
from app.pipeline.futures_orderbook_job import run_futures_orderbook_job


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
            "ob_imbalance_mean": 0.333333,
            "ob_imbalance_std": 0.0,
            "ob_imbalance_min": 0.333333,
            "ob_imbalance_max": 0.333333,
            "spread_pct": 0.001,
            "spread_pct_mean": 0.001,
            "spread_pct_max": 0.001,
            "mid_price": 100.05,
            "mid_price_mean": 100.05,
            "mid_price_min": 100.05,
            "mid_price_max": 100.05,
            "source": "rest",
            "sample_count": 1,
            "first_event_ms": now_ms,
            "last_event_ms": now_ms,
        },
    )

    result = svc.collect_futures_orderbook_snapshots(conn, ["BTCUSDT"])

    row = conn.execute(
        """
        SELECT symbol, source, sample_count, ob_imbalance_mean, spread_pct_mean, spread_bps_mean,
               mid_price_mean, mid_price_ret_1m, coverage_ratio
        FROM futures_order_book_snapshots
        WHERE symbol = 'BTCUSDT'
        """
    ).fetchone()
    assert result["saved"] == 1
    assert result["source_counts"]["rest"] == 1
    assert row == ("BTCUSDT", "rest", 1, 0.333333, 0.001, 10.0, 100.05, None, 0.0)


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
        "ob_imbalance_mean": 0.25,
        "ob_imbalance_std": 0.11,
        "ob_imbalance_min": 0.142857,
        "ob_imbalance_max": 0.4,
        "spread_pct": 0.0001,
        "spread_pct_mean": 0.00012,
        "spread_pct_max": 0.0002,
        "mid_price": 2000.1,
        "mid_price_mean": 2000.3,
        "mid_price_min": 1999.9,
        "mid_price_max": 2000.6,
        "source": "ws",
        "sample_count": 7,
        "first_event_ms": now_ms - 5000,
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
        SELECT symbol, source, sample_count, first_event_ms, last_event_ms,
               ob_imbalance_mean, spread_pct_mean, spread_bps_mean, mid_price_mean, coverage_ratio
        FROM futures_order_book_snapshots
        WHERE symbol = 'ETHUSDT'
        """
    ).fetchone()
    assert result["saved"] == 1
    assert result["source_counts"]["ws"] == 1
    assert row == ("ETHUSDT", "ws", 7, now_ms - 5000, now_ms, 0.25, 0.00012, 1.2, 2000.3, 0.083333)


def test_snapshot_from_bucket_computes_minute_aggregates() -> None:
    bucket = svc._new_bucket(
        "BTCUSDT",
        1_800_000,
        [[100.0, 2.0]],
        [[100.1, 1.0]],
        0.3,
        0.001,
        100.05,
        1_801_000,
    )
    svc._update_bucket(bucket, [[100.0, 2.0]], [[100.2, 1.0]], -0.1, 0.002, 100.10, 1_802_000)
    snapshot = svc._snapshot_from_bucket(bucket, source="ws")

    assert snapshot["sample_count"] == 2
    assert snapshot["ob_imbalance"] == -0.1
    assert snapshot["ob_imbalance_mean"] == 0.1
    assert snapshot["ob_imbalance_min"] == -0.1
    assert snapshot["ob_imbalance_max"] == 0.3
    assert snapshot["spread_pct_mean"] == 0.0015
    assert snapshot["spread_pct_max"] == 0.002
    assert snapshot["spread_bps"] == 20.0
    assert snapshot["spread_bps_mean"] == 15.0
    assert snapshot["spread_bps_max"] == 20.0
    assert snapshot["mid_price_mean"] == 100.07
    assert snapshot["mid_price_min"] == 100.05
    assert snapshot["mid_price_max"] == 100.1
    assert snapshot["coverage_ratio"] == 0.033333


def test_save_futures_orderbook_snapshot_computes_mid_price_ret_1m() -> None:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)

    first = {
        "symbol": "BTCUSDT",
        "timestamp_ms": 60_000,
        "bids": [[100.0, 1.0]],
        "asks": [[100.1, 1.0]],
        "ob_imbalance": 0.0,
        "ob_imbalance_mean": 0.0,
        "ob_imbalance_std": 0.0,
        "ob_imbalance_min": 0.0,
        "ob_imbalance_max": 0.0,
        "spread_pct": 0.001,
        "spread_pct_mean": 0.001,
        "spread_pct_max": 0.001,
        "spread_bps": 10.0,
        "spread_bps_mean": 10.0,
        "spread_bps_max": 10.0,
        "mid_price": 100.0,
        "mid_price_mean": 100.0,
        "mid_price_min": 100.0,
        "mid_price_max": 100.0,
        "source": "rest",
        "sample_count": 1,
        "coverage_ratio": 0.0,
        "first_event_ms": 60_000,
        "last_event_ms": 60_000,
    }
    second = dict(first)
    second["timestamp_ms"] = 120_000
    second["mid_price"] = 101.0
    second["mid_price_mean"] = 101.0
    second["mid_price_min"] = 101.0
    second["mid_price_max"] = 101.0

    svc.save_futures_orderbook_snapshot(conn, first)
    svc.save_futures_orderbook_snapshot(conn, second)

    row = conn.execute(
        """
        SELECT mid_price_ret_1m
        FROM futures_order_book_snapshots
        WHERE symbol='BTCUSDT' AND timestamp_ms=120000
        """
    ).fetchone()
    assert row == (0.01,)


def test_get_futures_orderbook_stats_exposes_runtime_fields(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)

    svc.save_futures_orderbook_snapshot(
        conn,
        {
            "symbol": "BTCUSDT",
            "timestamp_ms": 120_000,
            "bids": [[100.0, 1.0]],
            "asks": [[100.1, 1.0]],
            "ob_imbalance": 0.2,
            "ob_imbalance_mean": 0.15,
            "ob_imbalance_std": 0.05,
            "ob_imbalance_min": 0.1,
            "ob_imbalance_max": 0.2,
            "spread_pct": 0.001,
            "spread_pct_mean": 0.001,
            "spread_pct_max": 0.001,
            "spread_bps": 10.0,
            "spread_bps_mean": 10.0,
            "spread_bps_max": 10.0,
            "mid_price": 100.0,
            "mid_price_mean": 100.0,
            "mid_price_min": 100.0,
            "mid_price_max": 100.0,
            "source": "ws",
            "sample_count": 3,
            "coverage_ratio": 0.5,
            "first_event_ms": 100_000,
            "last_event_ms": 110_000,
        },
    )

    class CollectorStub:
        def runtime_status(self):
            return {
                "symbol_runtime": {
                    "BTCUSDT": {
                        "current_minute": 120_000,
                        "sample_count": 7,
                        "first_event_ms": 121_000,
                        "last_event_ms": 125_000,
                        "event_age_seconds": 12,
                    }
                }
            }

    monkeypatch.setattr(svc, "_COLLECTOR", CollectorStub())

    stats = svc.get_futures_orderbook_stats(conn)
    assert len(stats) == 1
    row = stats[0]
    assert row["symbol"] == "BTCUSDT"
    assert row["total"] == 1
    assert row["coverage_pct"] == 100.0
    assert row["latest_source"] == "ws"
    assert row["last_snapshot_at"] == "1970-01-01 00:02:00"
    assert row["last_event_at"] == "1970-01-01 00:02:05"
    assert isinstance(row["event_age_seconds"], int)
    assert row["event_age_seconds"] >= 0
    assert row["current_minute_sample_count"] == 7


def test_run_futures_orderbook_job_persists_collector_runtime(monkeypatch) -> None:
    conn = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(conn)

    monkeypatch.setattr("app.pipeline.futures_orderbook_job.is_futures_orderbook_collection_enabled", lambda: True)
    monkeypatch.setattr("app.pipeline.futures_orderbook_job.configured_futures_orderbook_symbols", lambda: ["BTCUSDT"])
    monkeypatch.setattr(
        "app.pipeline.futures_orderbook_job.collect_futures_orderbook_snapshots",
        lambda connection, symbols: {
            "saved": 1,
            "errors": [],
            "source_counts": {"ws": 1},
            "collector": {
                "symbol_runtime": {
                    "BTCUSDT": {
                        "sample_count": 9,
                        "last_event_ms": 123456789,
                        "event_age_seconds": 4,
                    }
                }
            },
        },
    )

    result = run_futures_orderbook_job(conn)
    assert result["status"] == "ok"

    row = conn.execute(
        """
        SELECT status, message, payload_json
        FROM runtime_heartbeats
        WHERE component = 'futures_orderbook_collector'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == "ok"
    assert "1/1" in row[1]
    assert "\"symbol_runtime\"" in row[2]
