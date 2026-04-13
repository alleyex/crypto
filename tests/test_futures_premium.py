from app.core.migrations import run_migrations
from app.data.futures_premium_service import collect_futures_premium_metrics
from conftest import make_connection


def _make_connection():
    conn = make_connection()
    run_migrations(conn)
    return conn


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self._payload


def test_collect_futures_premium_metrics_backfills_missing_minutes(monkeypatch) -> None:
    connection = _make_connection()
    try:
        connection.execute(
            """
            INSERT INTO futures_premium_metrics
                (symbol, timestamp_ms, mark_price, index_price, mark_index_basis_pct,
                 mark_index_spread_bps, source)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            ("BTCUSDT", 60_000, 101.0, 100.0, 0.01, 100.0, "rest"),
        )
        connection.commit()

        def fake_get(url, params=None, timeout=None):
            if "premiumIndex" in url:
                return _FakeResponse(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "markPrice": "104.0",
                            "indexPrice": "100.0",
                            "estimatedSettlePrice": "104.0",
                            "lastFundingRate": "0.0001",
                            "nextFundingTime": "0",
                        }
                    ]
                )
            start_time = int(params["startTime"])
            if "markPriceKlines" in url:
                return _FakeResponse(
                    [
                        [start_time, "0", "0", "0", "102.0"],
                        [start_time + 60_000, "0", "0", "0", "103.0"],
                    ]
                )
            if "indexPriceKlines" in url:
                return _FakeResponse(
                    [
                        [start_time, "0", "0", "0", "100.0"],
                        [start_time + 60_000, "0", "0", "0", "100.0"],
                    ]
                )
            raise AssertionError(f"unexpected url {url}")

        monkeypatch.setattr("app.data.retry_helpers.requests.get", fake_get)

        result = collect_futures_premium_metrics(connection, ["BTCUSDT"], now_ms=240_000)

        assert result["saved"] == 3
        assert result["source_counts"] == {"archive": 2, "rest": 1}

        rows = connection.execute(
            """
            SELECT timestamp_ms, source
            FROM futures_premium_metrics
            WHERE symbol = ?
            ORDER BY timestamp_ms
            """,
            ("BTCUSDT",),
        ).fetchall()
        assert rows == [
            (60_000, "rest"),
            (120_000, "archive"),
            (180_000, "archive"),
            (240_000, "rest"),
        ]
    finally:
        connection.close()


def test_collect_futures_premium_metrics_skips_backfill_when_no_gap(monkeypatch) -> None:
    connection = _make_connection()
    try:
        def fake_get(url, params=None, timeout=None):
            if "premiumIndex" in url:
                return _FakeResponse(
                    [
                        {
                            "symbol": "BTCUSDT",
                            "markPrice": "101.0",
                            "indexPrice": "100.0",
                            "estimatedSettlePrice": "101.0",
                            "lastFundingRate": "0.0001",
                            "nextFundingTime": "0",
                        }
                    ]
                )
            raise AssertionError("historical klines should not be fetched")

        monkeypatch.setattr("app.data.retry_helpers.requests.get", fake_get)

        result = collect_futures_premium_metrics(connection, ["BTCUSDT"], now_ms=60_000)

        assert result["saved"] == 1
        assert result["source_counts"] == {"archive": 0, "rest": 1}
    finally:
        connection.close()
