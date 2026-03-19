import json
import sqlite3
import urllib.error
from io import StringIO
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import contextlib

from fastapi.testclient import TestClient

from app.api.main import app
from app.core import db as db_module
from app.core.db import get_database_info
from app.core.db import fetch_all_as_dicts
from app.core.db import get_backend_name
from app.core.db import get_table_columns
from app.core.db import get_connection
from app.core.db import insert_and_get_rowid
from app.core.db import list_tables
from app.core.db import parse_db_timestamp
from app.core.db import _rewrite_query_params
from app.core.db import table_exists
from app.core.migrations import _auto_id_column_sql
from app.core.migrations import POSTGRES_MIGRATION_LOCK_ID
from app.core.migrations import run_migrations
from app.core.postgres_smoke import run_postgres_migration_smoke
from app.core.postgres_smoke import run_postgres_smoke
from app.query.read_service import get_orders as query_get_orders
from scripts.run_postgres_compose_validation import build_override_compose
from scripts.run_postgres_compose_validation import attach_metadata
from scripts.run_postgres_compose_validation import assert_pipeline_validation_success
from scripts.run_postgres_compose_validation import make_env
from scripts.run_postgres_compose_validation import request_json_with_retry
from scripts.run_postgres_compose_validation import run_validation_mode
from scripts.run_postgres_compose_validation import wait_for_api
from scripts.write_postgres_validation_artifact import build_artifact_manifest
from scripts.write_postgres_validation_artifact import build_summary_markdown
from scripts.write_postgres_validation_artifact import get_validation_layer
from scripts.write_postgres_validation_artifact import get_validation_verdict
from scripts.write_postgres_validation_artifact import write_optional_output
from scripts.write_postgres_validation_artifact import write_validation_artifacts
from scripts.artifact_utils import build_file_entry
from scripts.artifact_utils import build_manifest_files
from scripts.write_test_artifact import build_test_artifact_manifest
from scripts.write_test_artifact import build_test_summary
from scripts.write_test_artifact import get_outcome
from scripts.write_test_artifact import read_junit_counts
from scripts.write_test_artifact import write_test_artifact
from app.data.binance_client import fetch_klines
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import save_klines
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.strategy_job import run_strategy_job
from app.pipeline.strategy_job import run_strategy_jobs
from app.pipeline.run_pipeline import run_pipeline_collect
from app.portfolio.daily_pnl_service import get_daily_realized_pnl
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_fills
from app.query.read_service import get_audit_events
from app.query.read_service import get_orders
from app.query.read_service import get_pnl_snapshots
from app.query.read_service import get_positions
from app.query.read_service import get_risk_events
from app.query.read_service import get_signals
from app.query.read_service import get_strategy_activity_summary
from app.query.read_service import get_strategy_closed_trades
from app.risk.risk_service import ensure_table as ensure_risk_table
from app.risk.risk_service import evaluate_latest_signal
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.ma_cross import insert_signal
from app.strategy.ma_cross import generate_signal
from app.strategy.momentum_3bar import generate_signal as generate_momentum_3bar_signal
from app.strategy.registry import generate_registered_signal
from app.strategy.registry import get_strategy
from app.strategy.registry import list_registered_strategies
from app.system.kill_switch import disable_kill_switch
from app.system.kill_switch import enable_kill_switch
from app.scheduler.runner import run_scheduler
from app.scheduler.control import read_effective_active_strategies
from app.system.heartbeat import get_heartbeats
from app.system.heartbeat import upsert_heartbeat
from app.validation.soak_history import read_soak_validation_history
from app.validation.soak_history import record_soak_validation_snapshot
from app.validation.soak_report import build_soak_validation_report


def make_connection() -> sqlite3.Connection:
    return sqlite3.connect(":memory:")


def make_kline(open_time: int, close: float) -> list:
    return [
        open_time,
        str(close - 1),
        str(close + 1),
        str(close - 2),
        str(close),
        "100",
        open_time + 59_999,
        "1000",
        10,
        "50",
        "500",
    ]


def seed_candles(connection: sqlite3.Connection, closes: list[float]) -> None:
    ensure_candles_table(connection)
    klines = [make_kline((index + 1) * 60_000, close) for index, close in enumerate(closes)]
    save_klines(connection, klines)


def insert_fill(
    connection: sqlite3.Connection,
    order_id: int,
    symbol: str,
    side: str,
    qty: float,
    price: float,
    created_at: str,
) -> None:
    connection.execute(
        """
        INSERT INTO fills (order_id, symbol, side, qty, price, created_at)
        VALUES (?, ?, ?, ?, ?, ?);
        """,
        (order_id, symbol, side, qty, price, created_at),
    )


def test_generate_signal_creates_buy_signal_from_moving_average_cross() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10, 11, 12, 13, 14])
        ensure_signals_table(connection)

        result = generate_signal(connection)

        assert result is not None
        assert result["signal_type"] == "BUY"
        signals = get_signals(connection, limit=1)
        assert signals[0]["strategy_name"] == "ma_cross"
    finally:
        connection.close()


def test_get_strategy_activity_summary_groups_latest_records_by_strategy() -> None:
    connection = make_connection()
    try:
        ensure_candles_table(connection)
        ensure_signals_table(connection)
        ensure_risk_table(connection)
        ensure_execution_tables(connection)

        save_klines(connection, [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])])
        generate_signal(connection, strategy_name="ma_cross")
        evaluate_latest_signal(connection)
        execution_result = execute_latest_risk(connection)
        assert execution_result is not None
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ma-cross-sell-1", 999, "BTCUSDT", "1m", "ma_cross", "SELL", 0.001, 12.0, "FILLED"),
        )
        ma_cross_sell_order_id = int(
            connection.execute(
                "SELECT id FROM orders WHERE client_order_id = 'ma-cross-sell-1';"
            ).fetchone()[0]
        )
        insert_fill(connection, ma_cross_sell_order_id, "BTCUSDT", "SELL", 0.001, 12.0, "2026-03-19 10:05:00")

        save_klines(connection, [make_kline((index + 10) * 60_000, close) for index, close in enumerate([20, 21, 22, 24])])
        generate_momentum_3bar_signal(connection)
        evaluate_latest_signal(connection, cooldown_seconds=0)

        summary = get_strategy_activity_summary(connection)

        by_name = {item["strategy_name"]: item for item in summary}
        assert by_name["ma_cross"]["latest_signal"] is not None
        assert by_name["ma_cross"]["latest_risk"] is not None
        assert by_name["ma_cross"]["latest_order"] is not None
        assert by_name["ma_cross"]["latest_fill"] is not None
        assert by_name["ma_cross"]["latest_closed_trade"] is not None
        assert by_name["ma_cross"]["latest_closed_trade"]["symbol"] == "BTCUSDT"
        assert by_name["ma_cross"]["latest_closed_trade"]["status"] == "loss"
        assert by_name["ma_cross"]["latest_closed_trade"]["closed_at"] == "2026-03-19 10:05:00"
        assert by_name["ma_cross"]["latest_closed_trade"]["realized_pnl"] == -0.002
        assert by_name["ma_cross"]["latest_activity_at"] == by_name["ma_cross"]["latest_fill_at"]
        assert by_name["ma_cross"]["latest_order_at"] is not None
        assert by_name["ma_cross"]["latest_fill_at"] == "2026-03-19 10:05:00"
        assert by_name["ma_cross"]["filled_order_count"] == 2
        assert by_name["ma_cross"]["filled_qty_total"] == 0.002
        assert by_name["ma_cross"]["gross_realized_pnl"] == -0.002
        assert by_name["ma_cross"]["buy_fill_count"] == 1
        assert by_name["ma_cross"]["sell_fill_count"] == 1
        assert by_name["ma_cross"]["realized_trade_count"] == 1
        assert by_name["ma_cross"]["winning_trade_count"] == 0
        assert by_name["ma_cross"]["losing_trade_count"] == 1
        assert by_name["ma_cross"]["breakeven_trade_count"] == 0
        assert by_name["ma_cross"]["net_position_qty"] == 0.0
        assert by_name["momentum_3bar"]["latest_signal"] is not None
        assert by_name["momentum_3bar"]["latest_risk"] is not None
        assert by_name["momentum_3bar"]["latest_fill"] is None
        assert by_name["momentum_3bar"]["latest_closed_trade"] is None
        assert by_name["momentum_3bar"]["latest_activity_at"] is not None
        assert by_name["momentum_3bar"]["latest_order_at"] is None
        assert by_name["momentum_3bar"]["latest_fill_at"] is None
        assert by_name["momentum_3bar"]["filled_order_count"] == 0
        assert by_name["momentum_3bar"]["filled_qty_total"] == 0.0
        assert by_name["momentum_3bar"]["buy_fill_count"] == 0
        assert by_name["momentum_3bar"]["sell_fill_count"] == 0
    finally:
        connection.close()


def test_get_strategy_closed_trades_returns_recent_realized_trades() -> None:
    connection = make_connection()
    try:
        ensure_execution_tables(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-1", 1, "BTCUSDT", "1m", "ma_cross", "BUY", 1.0, 100.0, "FILLED", "2026-03-19 10:00:00"),
        )
        buy_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-1';").fetchone()[0])
        insert_fill(connection, buy_order_id, "BTCUSDT", "BUY", 1.0, 100.0, "2026-03-19 10:00:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-1", 2, "BTCUSDT", "1m", "ma_cross", "SELL", 1.0, 110.0, "FILLED", "2026-03-19 10:05:00"),
        )
        sell_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'sell-1';").fetchone()[0])
        insert_fill(connection, sell_order_id, "BTCUSDT", "SELL", 1.0, 110.0, "2026-03-19 10:05:00")

        closed_trades = get_strategy_closed_trades(connection)

        assert len(closed_trades) == 1
        assert closed_trades[0]["strategy_name"] == "ma_cross"
        assert closed_trades[0]["realized_pnl"] == 10.0
        assert closed_trades[0]["status"] == "win"
    finally:
        connection.close()


def test_get_strategy_closed_trades_filters_by_strategy() -> None:
    connection = make_connection()
    try:
        ensure_execution_tables(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-ma", 1, "BTCUSDT", "1m", "ma_cross", "BUY", 1.0, 100.0, "FILLED", "2026-03-19 10:00:00"),
        )
        buy_ma_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-ma';").fetchone()[0])
        insert_fill(connection, buy_ma_order_id, "BTCUSDT", "BUY", 1.0, 100.0, "2026-03-19 10:00:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-ma", 2, "BTCUSDT", "1m", "ma_cross", "SELL", 1.0, 110.0, "FILLED", "2026-03-19 10:05:00"),
        )
        sell_ma_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'sell-ma';").fetchone()[0])
        insert_fill(connection, sell_ma_order_id, "BTCUSDT", "SELL", 1.0, 110.0, "2026-03-19 10:05:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-mom", 3, "ETHUSDT", "1m", "momentum_3bar", "BUY", 1.0, 200.0, "FILLED", "2026-03-19 10:10:00"),
        )
        buy_mom_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-mom';").fetchone()[0])
        insert_fill(connection, buy_mom_order_id, "ETHUSDT", "BUY", 1.0, 200.0, "2026-03-19 10:10:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-mom", 4, "ETHUSDT", "1m", "momentum_3bar", "SELL", 1.0, 190.0, "FILLED", "2026-03-19 10:15:00"),
        )
        sell_mom_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'sell-mom';").fetchone()[0])
        insert_fill(connection, sell_mom_order_id, "ETHUSDT", "SELL", 1.0, 190.0, "2026-03-19 10:15:00")

        closed_trades = get_strategy_closed_trades(connection, strategy_name="momentum_3bar")

        assert len(closed_trades) == 1
        assert closed_trades[0]["strategy_name"] == "momentum_3bar"
        assert closed_trades[0]["symbol"] == "ETHUSDT"
        assert closed_trades[0]["status"] == "loss"
    finally:
        connection.close()


def test_strategy_registry_exposes_ma_cross() -> None:
    names = list_registered_strategies()

    assert "ma_cross" in names
    assert "momentum_3bar" in names
    assert get_strategy("ma_cross") is not None
    assert get_strategy("momentum_3bar") is not None


def test_generate_registered_signal_runs_ma_cross_strategy(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr(
            "app.strategy.registry.STRATEGY_REGISTRY",
            {"ma_cross": lambda conn: {"strategy_name": "ma_cross", "signal_type": "BUY", "short_ma": 3.0, "long_ma": 2.0}},
        )

        result = generate_registered_signal(connection, strategy_name="ma_cross")

        assert result == {
            "strategy_name": "ma_cross",
            "signal_type": "BUY",
            "short_ma": 3.0,
            "long_ma": 2.0,
        }
    finally:
        connection.close()


def test_momentum_3bar_generates_buy_signal() -> None:
    connection = make_connection()
    try:
        ensure_candles_table(connection)
        base_open_time = 1_710_000_000_000
        klines = []
        for index, close in enumerate((100.0, 101.0, 102.0, 104.0)):
            open_time = base_open_time + (index * 60_000)
            klines.append(
                [
                    open_time,
                    str(close - 1.0),
                    str(close + 1.0),
                    str(close - 2.0),
                    str(close),
                    "100",
                    open_time + 59_000,
                    "1000",
                    10,
                    "50",
                    "500",
                    "0",
                ]
            )

        save_klines(connection, klines)

        result = generate_momentum_3bar_signal(connection)

        assert result is not None
        assert result["strategy_name"] == "momentum_3bar"
        assert result["signal_type"] == "BUY"
        assert result["short_ma"] == 104.0
        assert result["long_ma"] == 100.0
    finally:
        connection.close()


def test_run_postgres_smoke_requires_database_url() -> None:
    try:
        run_postgres_smoke("")
        assert False, "Expected RuntimeError for missing database URL."
    except RuntimeError as exc:
        assert "CRYPTO_DATABASE_URL" in str(exc)


def test_run_postgres_smoke_executes_basic_postgres_flow(monkeypatch) -> None:
    executed: list[tuple[str, object]] = []

    class DummyCursor:
        def execute(self, query: str, params=None) -> None:
            executed.append((query.strip(), params))

        def fetchone(self):
            query = executed[-1][0]
            if query == "SELECT current_database(), current_user;":
                return ("crypto", "crypto")
            if query == "SELECT COUNT(*), MIN(note), MAX(note) FROM crypto_postgres_smoke;":
                return (1, "smoke", "smoke")
            raise AssertionError(f"Unexpected fetch for query: {query}")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyConnection:
        def cursor(self):
            return DummyCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyPsycopg:
        def connect(self, database_url: str):
            assert database_url == "postgresql://crypto:crypto@127.0.0.1:5432/crypto"
            return DummyConnection()

    monkeypatch.setattr("app.core.postgres_smoke._load_psycopg", lambda: DummyPsycopg())

    result = run_postgres_smoke("postgresql://crypto:crypto@127.0.0.1:5432/crypto")

    assert result == {
        "ok": True,
        "database": "crypto",
        "user": "crypto",
        "temp_row_count": 1,
        "temp_first_note": "smoke",
        "temp_last_note": "smoke",
    }
    assert any("ON CONFLICT (id) DO NOTHING" in query for query, _ in executed)


def test_run_postgres_smoke_retries_until_connection_succeeds(monkeypatch) -> None:
    attempts: list[str] = []
    sleep_calls: list[float] = []
    executed: list[str] = []

    class DummyCursor:
        def execute(self, query: str, params=None) -> None:
            executed.append(" ".join(query.split()))

        def fetchone(self):
            if executed[-1] == "SELECT current_database(), current_user;":
                return ("crypto", "crypto")
            return (1, "smoke", "smoke")

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyConnection:
        def cursor(self):
            return DummyCursor()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyPsycopg:
        class OperationalError(RuntimeError):
            pass

        def connect(self, database_url: str):
            attempts.append(database_url)
            if len(attempts) < 3:
                raise self.OperationalError("database system is starting up")
            return DummyConnection()

    monkeypatch.setattr("app.core.postgres_smoke._load_psycopg", lambda: DummyPsycopg())
    monkeypatch.setattr("app.core.postgres_smoke.time.sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setenv("CRYPTO_POSTGRES_CONNECT_RETRIES", "3")
    monkeypatch.setenv("CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS", "0.25")

    result = run_postgres_smoke("postgresql://crypto:crypto@127.0.0.1:5432/crypto")

    assert result["ok"] is True
    assert len(attempts) == 3
    assert sleep_calls == [0.25, 0.25]


def test_run_postgres_migration_smoke_runs_migrations_and_checks_tables(monkeypatch) -> None:
    executed: list[tuple[str, object]] = []
    run_calls: list[str] = []

    class DummyRawConnection:
        def cursor(self):
            executed_ref = executed

            class CursorContext:
                description = None
                _rows = []

                def execute(self, query: str, params=None):
                    normalized = " ".join(query.split())
                    executed_ref.append((normalized, params))
                    if "FROM pg_catalog.pg_tables" in normalized and "schemaname = 'public'" in normalized:
                        self.description = [("tablename",)]
                        self._rows = [
                            ("audit_events",),
                            ("candles",),
                            ("daily_realized_pnl",),
                            ("fills",),
                            ("orders",),
                            ("pnl_snapshots",),
                            ("positions",),
                            ("risk_events",),
                            ("runtime_heartbeats",),
                            ("schema_migrations",),
                            ("signals",),
                        ]
                    else:
                        self.description = None
                        self._rows = []

                def fetchone(self):
                    return self._rows[0] if self._rows else None

                def fetchall(self):
                    return list(self._rows)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            return CursorContext()

        def commit(self):
            return None

        def close(self):
            return None

    class DummyPsycopg:
        def connect(self, database_url: str):
            assert database_url == "postgresql://crypto:crypto@127.0.0.1:5432/crypto"
            return DummyRawConnection()

    monkeypatch.setattr("app.core.postgres_smoke._load_psycopg", lambda: DummyPsycopg())
    monkeypatch.setattr(
        "app.core.postgres_smoke.run_migrations",
        lambda connection: run_calls.append(connection.__class__.__name__) or ["001_create_candles_table"],
    )

    result = run_postgres_migration_smoke("postgresql://crypto:crypto@127.0.0.1:5432/crypto")

    assert result["ok"] is True
    assert result["applied_migrations"] == ["001_create_candles_table"]
    assert result["all_expected_tables_present"] is True
    assert run_calls == ["PostgresConnectionAdapter"]


def test_run_postgres_migration_smoke_retries_until_connection_succeeds(monkeypatch) -> None:
    attempts: list[str] = []
    sleep_calls: list[float] = []

    class DummyRawConnection:
        def cursor(self):
            class CursorContext:
                description = [("tablename",)]
                _rows = [("schema_migrations",)]

                def execute(self, query: str, params=None):
                    return None

                def fetchone(self):
                    return self._rows[0]

                def fetchall(self):
                    return list(self._rows)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            return CursorContext()

        def commit(self):
            return None

        def close(self):
            return None

    class DummyPsycopg:
        class OperationalError(RuntimeError):
            pass

        def connect(self, database_url: str):
            attempts.append(database_url)
            if len(attempts) < 2:
                raise self.OperationalError("server closed the connection unexpectedly")
            return DummyRawConnection()

    monkeypatch.setattr("app.core.postgres_smoke._load_psycopg", lambda: DummyPsycopg())
    monkeypatch.setattr("app.core.postgres_smoke.time.sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setattr("app.core.postgres_smoke.run_migrations", lambda connection: ["001_create_candles_table"])
    monkeypatch.setenv("CRYPTO_POSTGRES_CONNECT_RETRIES", "2")
    monkeypatch.setenv("CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS", "0.5")

    result = run_postgres_migration_smoke("postgresql://crypto:crypto@127.0.0.1:5432/crypto")

    assert result["ok"] is True
    assert len(attempts) == 2
    assert sleep_calls == [0.5]


def test_build_override_compose_uses_isolated_mounts_and_api_port(tmp_path: Path) -> None:
    rendered = build_override_compose(api_port=8012, work_dir=tmp_path)

    assert 'ports: []' in rendered
    assert '- "8012:8000"' in rendered
    assert f"- {tmp_path / 'storage'}:/app/storage" in rendered
    assert f"- {tmp_path / 'logs'}:/app/logs" in rendered
    assert f"- {tmp_path / 'runtime'}:/app/runtime" in rendered


def test_wait_for_api_retries_on_connection_reset(monkeypatch) -> None:
    calls: list[str] = []
    sleep_calls: list[int] = []
    time_values = iter([0, 0, 0, 0, 0, 0])

    def fake_request_json(method: str, url: str):
        calls.append(url)
        if len(calls) < 3:
            raise ConnectionResetError(104, "Connection reset by peer")
        return {"status": "ok"}

    monkeypatch.setattr("scripts.run_postgres_compose_validation.request_json", fake_request_json)
    monkeypatch.setattr("scripts.run_postgres_compose_validation.time.sleep", lambda seconds: sleep_calls.append(seconds))
    monkeypatch.setattr("scripts.run_postgres_compose_validation.time.time", lambda: next(time_values))

    result = wait_for_api("http://127.0.0.1:8012", timeout_seconds=5)

    assert result == {"status": "ok"}
    assert len(calls) == 3
    assert sleep_calls == [1, 1]


def test_request_json_with_retry_retries_transient_http_500(monkeypatch) -> None:
    calls: list[str] = []
    sleep_calls: list[float] = []

    def fake_request_json(method: str, url: str):
        calls.append(url)
        if len(calls) < 3:
            raise urllib.error.HTTPError(url, 500, "Internal Server Error", hdrs=None, fp=None)
        return {"ok": True}

    monkeypatch.setattr("scripts.run_postgres_compose_validation.request_json", fake_request_json)
    monkeypatch.setattr("scripts.run_postgres_compose_validation.time.sleep", lambda seconds: sleep_calls.append(seconds))

    result = request_json_with_retry("POST", "http://127.0.0.1:8012/pipeline/run", attempts=3, delay_seconds=0.5)

    assert result == {"ok": True}
    assert len(calls) == 3
    assert sleep_calls == [0.5, 0.5]


def test_assert_pipeline_validation_success_rejects_failed_pipeline() -> None:
    try:
        assert_pipeline_validation_success(
            {
                "steps": [
                    {"step": "save_klines", "status": "failed", "error": "Binance API unavailable"},
                ]
            }
        )
    except RuntimeError as exc:
        assert "Pipeline validation failed" in str(exc)
    else:
        raise AssertionError("Expected pipeline validation failure to raise.")


def test_run_migrations_uses_postgres_advisory_lock(monkeypatch) -> None:
    executed: list[tuple[str, tuple]] = []

    class FakeCursor:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchall(self):
            return list(self._rows)

    class FakeConnection:
        def execute(self, query: str, params: tuple = ()):
            executed.append((" ".join(query.split()), params))
            if "SELECT version FROM schema_migrations" in query:
                return FakeCursor([])
            return FakeCursor([])

        def commit(self) -> None:
            executed.append(("COMMIT", ()))

        def rollback(self) -> None:
            executed.append(("ROLLBACK", ()))

    connection = FakeConnection()
    monkeypatch.setattr("app.core.migrations.get_backend_name", lambda _connection: "postgres")
    monkeypatch.setattr(
        "app.core.migrations.MIGRATIONS",
        [("001_test_migration", lambda _connection: executed.append(("MIGRATION", ())))],
    )

    run_migrations(connection)

    assert executed[0] == ("SELECT pg_advisory_lock(?);", (POSTGRES_MIGRATION_LOCK_ID,))
    assert ("MIGRATION", ()) in executed
    assert (
        "INSERT INTO schema_migrations (version) VALUES (?) ON CONFLICT (version) DO NOTHING;",
        ("001_test_migration",),
    ) in executed
    assert executed[-1] == ("SELECT pg_advisory_unlock(?);", (POSTGRES_MIGRATION_LOCK_ID,))


def test_run_migrations_preserves_original_error_when_unlock_fails(monkeypatch) -> None:
    class FakeCursor:
        def __init__(self, rows):
            self._rows = list(rows)

        def fetchall(self):
            return list(self._rows)

    class FakeConnection:
        def __init__(self):
            self.rolled_back = False

        def execute(self, query: str, params: tuple = ()):
            normalized = " ".join(query.split())
            if normalized == "SELECT pg_advisory_unlock(?);":
                raise RuntimeError("unlock failed")
            if "SELECT version FROM schema_migrations" in normalized:
                return FakeCursor([])
            return FakeCursor([])

        def commit(self) -> None:
            pass

        def rollback(self) -> None:
            self.rolled_back = True

    connection = FakeConnection()
    monkeypatch.setattr("app.core.migrations.get_backend_name", lambda _connection: "postgres")
    monkeypatch.setattr(
        "app.core.migrations.MIGRATIONS",
        [("001_broken", lambda _connection: (_ for _ in ()).throw(RuntimeError("migration failed")))],
    )

    try:
        run_migrations(connection)
    except RuntimeError as exc:
        assert str(exc) == "migration failed"
    else:
        raise AssertionError("Expected migration failure to raise.")

    assert connection.rolled_back is True


def test_make_env_defaults_postgres_runtime_settings(monkeypatch) -> None:
    monkeypatch.delenv("CRYPTO_POSTGRES_CONNECT_RETRIES", raising=False)
    monkeypatch.delenv("CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS", raising=False)

    env = make_env(
        project_name="crypto_pg_validation",
        database_url="postgresql://crypto:crypto@postgres:5432/crypto",
    )

    assert env["COMPOSE_PROJECT_NAME"] == "crypto_pg_validation"
    assert env["CRYPTO_DB_BACKEND"] == "postgres"
    assert env["CRYPTO_DATABASE_URL"] == "postgresql://crypto:crypto@postgres:5432/crypto"
    assert env["CRYPTO_USE_FAKE_KLINES"] == "1"
    assert env["CRYPTO_FAKE_KLINE_CLOSES"] == "10,11,12,13,14"
    assert env["CRYPTO_POSTGRES_CONNECT_RETRIES"] == "15"
    assert env["CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS"] == "1"


def test_fetch_klines_returns_fake_data_when_enabled(monkeypatch) -> None:
    monkeypatch.setenv("CRYPTO_USE_FAKE_KLINES", "1")
    monkeypatch.setenv("CRYPTO_FAKE_KLINE_CLOSES", "21,22,23,24,25")
    monkeypatch.setattr("app.data.binance_client.time.time", lambda: 300.0)

    klines = fetch_klines(limit=3)

    assert len(klines) == 3
    assert [kline[4] for kline in klines] == ["23.0", "24.0", "25.0"]
    assert klines[0][0] < klines[-1][0]


def test_build_summary_markdown_renders_key_runtime_fields() -> None:
    markdown = build_summary_markdown(
        {
            "mode": "compose-soak-readability",
            "ok": True,
            "event_name": "schedule",
            "run_id": "12345",
            "generated_at": "2026-03-18T15:05:16+00:00",
            "base_url": "http://127.0.0.1:8012",
            "health": {"status": "ok", "database": "postgresql://crypto:crypto@postgres:5432/crypto"},
            "pipeline": {"steps": [{"step": "save_klines"}, {"step": "update_pnl"}]},
            "orders": [{"id": 1}],
            "audit_events": [{"id": 1}, {"id": 2}],
            "scheduler_logs": ["scheduler-1  | [2026-03-18T15:05:15] soak_snapshot status=ok"],
            "soak_validation": {"status": "ok"},
            "soak_history": [
                {"recorded_at": "2026-03-18T15:05:15+00:00", "status": "ok"},
                {"recorded_at": "2026-03-18T15:04:15+00:00", "status": "ok"},
            ],
        }
    )

    assert "# PostgreSQL Compose Validation" in markdown
    assert "- mode: `compose-soak-readability`" in markdown
    assert "- validation_layer: `readability`" in markdown
    assert "- verdict: `readability-check`" in markdown
    assert "- event_name: `schedule`" in markdown
    assert "- run_id: `12345`" in markdown
    assert "- generated_at: `2026-03-18T15:05:16+00:00`" in markdown
    assert "- health_status: `ok`" in markdown
    assert "- pipeline_step_count: `2`" in markdown
    assert "- order_count: `1`" in markdown
    assert "- soak_status: `ok`" in markdown
    assert "- soak_history_count: `2`" in markdown
    assert "- soak_history_latest_at: `2026-03-18T15:05:15+00:00`" in markdown
    assert "soak_snapshot status=ok" in markdown


def test_write_optional_output_writes_requested_file(tmp_path: Path) -> None:
    output_path = tmp_path / "reports" / "summary.md"

    write_optional_output(str(output_path), "hello\n")

    assert output_path.read_text(encoding="utf-8") == "hello\n"


def test_build_file_entry_includes_relative_path_and_checksum(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact"
    artifact_root.mkdir(parents=True, exist_ok=True)
    path = artifact_root / "summary.md"
    path.write_text("summary\n", encoding="utf-8")

    entry = build_file_entry(path, artifact_root, "Human-readable summary.")

    assert entry["path"] == "summary.md"
    assert entry["purpose"] == "Human-readable summary."
    assert entry["size_bytes"] == str(len("summary\n".encode("utf-8")))
    assert len(entry["sha256"]) == 64


def test_build_manifest_files_skips_missing_paths(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifact"
    artifact_root.mkdir(parents=True, exist_ok=True)
    (artifact_root / "summary.md").write_text("summary\n", encoding="utf-8")

    files = build_manifest_files(
        artifact_root=artifact_root,
        file_purposes={
            "summary.md": "Human-readable summary.",
            "missing.log": "Should be skipped.",
        },
    )

    assert [item["path"] for item in files] == ["summary.md"]


def test_read_junit_counts_reads_pytest_xml(tmp_path: Path) -> None:
    junit_path = tmp_path / "junit.xml"
    junit_path.write_text(
        '<testsuite tests="7" failures="1" errors="2" skipped="3"></testsuite>\n',
        encoding="utf-8",
    )

    counts = read_junit_counts(junit_path)

    assert counts == {"tests": 7, "failures": 1, "errors": 2, "skipped": 3}


def test_build_test_summary_renders_test_metadata() -> None:
    summary = build_test_summary(
        {"tests": 7, "failures": 0, "errors": 0, "skipped": 1},
        event_name="pull_request",
        run_id="321",
        generated_at="2026-03-19T01:02:03+00:00",
    )

    assert "# Test Results" in summary
    assert "- outcome: `passed`" in summary
    assert "- event_name: `pull_request`" in summary
    assert "- run_id: `321`" in summary
    assert "- generated_at: `2026-03-19T01:02:03+00:00`" in summary
    assert "- validation_layer: `test`" in summary
    assert "- verdict: `test-check`" in summary
    assert "- tests: `7`" in summary
    assert "- skipped: `1`" in summary


def test_get_outcome_maps_junit_counts() -> None:
    assert get_outcome({"tests": 1, "failures": 0, "errors": 0, "skipped": 0}) == "passed"
    assert get_outcome({"tests": 1, "failures": 1, "errors": 0, "skipped": 0}) == "failed"


def test_build_test_artifact_manifest_includes_summary_junit_and_manifest(tmp_path: Path) -> None:
    artifact_dir = tmp_path / "artifacts"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    summary_path = artifact_dir / "summary.md"
    junit_path = artifact_dir / "junit.xml"
    manifest_path = artifact_dir / "manifest.json"
    summary_path.write_text("summary\n", encoding="utf-8")
    junit_path.write_text(
        '<testsuite tests="7" failures="0" errors="0" skipped="1"></testsuite>\n',
        encoding="utf-8",
    )
    manifest_path.write_text("{}\n", encoding="utf-8")

    manifest = build_test_artifact_manifest(
        artifact_dir=artifact_dir,
        junit_xml_path=junit_path,
        summary_path=summary_path,
        event_name="push",
        run_id="555",
        generated_at="2026-03-19T01:02:03+00:00",
    )

    manifest["files"].append(
        {
            "path": "manifest.json",
            "purpose": "Artifact manifest for test results.",
            "size_bytes": str(len(manifest_path.read_bytes())),
            "sha256": "placeholder",
        }
    )

    assert manifest["artifact_kind"] == "test-results-artifact"
    assert manifest["validation_layer"] == "test"
    assert manifest["verdict"] == "test-check"
    assert manifest["outcome"] == "passed"
    assert manifest["event_name"] == "push"
    assert manifest["run_id"] == "555"
    assert [item["path"] for item in manifest["files"]] == [
        "summary.md",
        "junit.xml",
        "manifest.json",
    ]


def test_write_test_artifact_writes_summary_manifest_and_step_summary(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "artifacts"
    junit_path = artifact_dir / "junit.xml"
    step_summary_path = tmp_path / "step-summary.md"
    artifact_dir.mkdir(parents=True, exist_ok=True)
    junit_path.write_text(
        '<testsuite tests="9" failures="0" errors="0" skipped="2"></testsuite>\n',
        encoding="utf-8",
    )
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(step_summary_path))

    summary_path, manifest_path = write_test_artifact(
        artifact_dir=artifact_dir,
        junit_xml_path=junit_path,
        event_name="workflow_dispatch",
        run_id="777",
        write_step_summary=True,
    )

    assert summary_path.exists()
    assert manifest_path.exists()
    summary_text = summary_path.read_text(encoding="utf-8")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert "- outcome: `passed`" in summary_text
    assert "- event_name: `workflow_dispatch`" in summary_text
    assert "- run_id: `777`" in summary_text
    assert step_summary_path.read_text(encoding="utf-8") == summary_text
    assert manifest["artifact_kind"] == "test-results-artifact"
    assert manifest["validation_layer"] == "test"
    assert manifest["verdict"] == "test-check"
    assert manifest["outcome"] == "passed"
    assert [item["path"] for item in manifest["files"]] == [
        "summary.md",
        "junit.xml",
        "manifest.json",
    ]


def test_attach_metadata_uses_github_env_when_present(monkeypatch) -> None:
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_RUN_ID", "98765")

    enriched = attach_metadata({"mode": "smoke", "ok": True})

    assert enriched["validation_layer"] == "smoke"
    assert enriched["verdict"] == "quick-check"
    assert enriched["event_name"] == "pull_request"
    assert enriched["run_id"] == "98765"
    assert enriched["mode"] == "smoke"
    assert enriched["ok"] is True
    assert "generated_at" in enriched


def test_attach_metadata_defaults_to_local_when_github_env_missing(monkeypatch) -> None:
    monkeypatch.delenv("GITHUB_EVENT_NAME", raising=False)
    monkeypatch.delenv("GITHUB_RUN_ID", raising=False)

    enriched = attach_metadata({"mode": "smoke", "ok": True})

    assert enriched["validation_layer"] == "smoke"
    assert enriched["verdict"] == "quick-check"
    assert enriched["event_name"] == "local"
    assert enriched["run_id"] == "local"
    assert "generated_at" in enriched


def test_build_artifact_manifest_for_smoke(tmp_path: Path) -> None:
    artifact_root = tmp_path / "smoke-artifact"
    artifact_root.mkdir(parents=True, exist_ok=True)
    (artifact_root / "summary.md").write_text("summary\n", encoding="utf-8")
    (artifact_root / "result.json").write_text("{}\n", encoding="utf-8")
    (artifact_root / "raw.log").write_text("raw\n", encoding="utf-8")

    manifest = build_artifact_manifest(
        result={
            "mode": "smoke",
            "event_name": "pull_request",
            "run_id": "111",
            "generated_at": "2026-03-18T15:05:16+00:00",
        },
        artifact_root=artifact_root,
        file_purposes={
            "summary.md": "Human-readable validation summary.",
            "result.json": "Full structured validation result.",
            "raw.log": "Raw stdout from the validation script.",
        },
    )

    assert manifest["mode"] == "smoke"
    assert manifest["artifact_kind"] == "postgres-validation-artifact"
    assert manifest["validation_layer"] == "smoke"
    assert manifest["verdict"] == "quick-check"
    assert manifest["event_name"] == "pull_request"
    assert [item["path"] for item in manifest["files"]] == ["summary.md", "result.json", "raw.log"]
    assert all("size_bytes" in item for item in manifest["files"])
    assert all("sha256" in item for item in manifest["files"])


def test_build_artifact_manifest_for_compose_with_service_logs(tmp_path: Path) -> None:
    artifact_root = tmp_path / "compose-artifact"
    (artifact_root / "services").mkdir(parents=True, exist_ok=True)
    for relative_path, content in {
        "summary.md": "summary\n",
        "result.json": "{}\n",
        "raw.log": "raw\n",
        "docker.log": "docker\n",
        "services/api.log": "api\n",
        "services/scheduler.log": "scheduler\n",
        "services/postgres.log": "postgres\n",
    }.items():
        path = artifact_root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    manifest = build_artifact_manifest(
        result={
            "mode": "compose-soak-readability",
            "event_name": "schedule",
            "run_id": "222",
            "generated_at": "2026-03-18T15:05:16+00:00",
        },
        artifact_root=artifact_root,
        file_purposes={
            "summary.md": "Human-readable validation summary.",
            "result.json": "Full structured validation result.",
            "raw.log": "Raw stdout from the validation script.",
            "docker.log": "Combined Docker Compose logs for postgres, api, and scheduler.",
            "services/api.log": "Docker Compose logs for the api service.",
            "services/scheduler.log": "Docker Compose logs for the scheduler service.",
            "services/postgres.log": "Docker Compose logs for the postgres service.",
        },
    )

    assert manifest["mode"] == "compose-soak-readability"
    assert manifest["artifact_kind"] == "postgres-validation-artifact"
    assert manifest["validation_layer"] == "readability"
    assert manifest["verdict"] == "readability-check"
    assert manifest["event_name"] == "schedule"
    assert [item["path"] for item in manifest["files"]] == [
        "summary.md",
        "result.json",
        "raw.log",
        "docker.log",
        "services/api.log",
        "services/scheduler.log",
        "services/postgres.log",
    ]
    assert all("size_bytes" in item for item in manifest["files"])
    assert all("sha256" in item for item in manifest["files"])


def test_write_validation_artifacts_writes_compose_outputs_and_step_summary(tmp_path: Path, monkeypatch) -> None:
    artifact_dir = tmp_path / "artifact"
    step_summary_path = tmp_path / "step-summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(step_summary_path))

    result_json = write_validation_artifacts(
        result={
            "mode": "compose-runtime",
            "ok": True,
            "event_name": "push",
            "run_id": "444",
            "generated_at": "2026-03-19T01:02:03+00:00",
            "health": {"status": "ok", "database": "postgresql://crypto:crypto@postgres:5432/crypto"},
            "pipeline": {"steps": [{"step": "save_klines"}]},
            "orders": [{"id": 1}],
            "audit_events": [{"id": 1}],
            "scheduler_logs": ["scheduler-1 | tick"],
            "docker_logs": "docker logs\n",
            "api_logs": "api logs\n",
            "scheduler_logs_full": "scheduler logs\n",
            "postgres_logs": "postgres logs\n",
        },
        json_output=str(artifact_dir / "result.json"),
        summary_file=str(artifact_dir / "summary.md"),
        raw_log_output=str(artifact_dir / "raw.log"),
        docker_logs_output=str(artifact_dir / "docker.log"),
        docker_logs_dir=str(artifact_dir / "services"),
        manifest_output=str(artifact_dir / "manifest.json"),
        write_step_summary=True,
    )

    assert json.loads(result_json)["mode"] == "compose-runtime"
    assert (artifact_dir / "result.json").exists()
    assert (artifact_dir / "summary.md").exists()
    assert (artifact_dir / "raw.log").exists()
    assert (artifact_dir / "docker.log").read_text(encoding="utf-8") == "docker logs\n"
    assert (artifact_dir / "services" / "api.log").read_text(encoding="utf-8") == "api logs\n"
    assert (artifact_dir / "services" / "scheduler.log").read_text(encoding="utf-8") == "scheduler logs\n"
    assert (artifact_dir / "services" / "postgres.log").read_text(encoding="utf-8") == "postgres logs\n"
    manifest = json.loads((artifact_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["artifact_kind"] == "postgres-validation-artifact"
    assert manifest["validation_layer"] == "runtime"
    assert manifest["verdict"] == "runtime-check"
    assert [item["path"] for item in manifest["files"]] == [
        "summary.md",
        "result.json",
        "raw.log",
        "docker.log",
        "services/api.log",
        "services/scheduler.log",
        "services/postgres.log",
    ]
    assert step_summary_path.read_text(encoding="utf-8") == (artifact_dir / "summary.md").read_text(encoding="utf-8")


def test_get_validation_layer_maps_modes() -> None:
    assert get_validation_layer("smoke") == "smoke"
    assert get_validation_layer("compose-runtime") == "runtime"
    assert get_validation_layer("compose-soak-readability") == "readability"
    assert get_validation_layer("unknown") == "unknown"


def test_get_validation_verdict_maps_modes() -> None:
    assert get_validation_verdict("smoke") == "quick-check"
    assert get_validation_verdict("compose-runtime") == "runtime-check"
    assert get_validation_verdict("compose-soak-readability") == "readability-check"
    assert get_validation_verdict("unknown") == "unknown-check"


def test_run_validation_mode_dispatches_smoke(monkeypatch) -> None:
    smoke_calls: list[str] = []
    migration_calls: list[str] = []

    monkeypatch.setattr(
        "scripts.run_postgres_compose_validation.run_postgres_smoke",
        lambda database_url: smoke_calls.append(database_url) or {"ok": True},
    )
    monkeypatch.setattr(
        "scripts.run_postgres_compose_validation.run_postgres_migration_smoke",
        lambda database_url: migration_calls.append(database_url) or {"ok": True},
    )

    class Args:
        mode = "smoke"
        database_url = "postgresql://crypto:crypto@postgres:5432/crypto"
        api_port = 8012
        project_name = "crypto_pg_validation"
        startup_timeout = 90.0
        keep_up = False

    result = run_validation_mode(Args())

    assert result["mode"] == "smoke"
    assert smoke_calls == ["postgresql://crypto:crypto@postgres:5432/crypto"]
    assert migration_calls == ["postgresql://crypto:crypto@postgres:5432/crypto"]


def test_run_validation_mode_dispatches_compose_soak(monkeypatch) -> None:
    calls: list[tuple[str, object]] = []
    compose_result = {
        "mode": "compose-runtime",
        "ok": True,
        "base_url": "http://127.0.0.1:8012",
        "health": {"status": "ok", "database": "postgresql://crypto:crypto@postgres:5432/crypto"},
        "pipeline": {"steps": []},
        "orders": [],
        "audit_events": [],
        "scheduler_logs": [],
        "docker_logs": "api-1 | up\nscheduler-1 | up\n",
        "api_logs": "api-1 | ready\n",
        "scheduler_logs_full": "scheduler-1 | tick\n",
        "postgres_logs": "postgres-1 | healthy\n",
    }

    monkeypatch.setattr(
        "scripts.run_postgres_compose_validation.validate_compose_runtime",
        lambda **kwargs: calls.append(("compose", kwargs)) or dict(compose_result),
    )
    monkeypatch.setattr(
        "scripts.run_postgres_compose_validation.request_json",
        lambda method, url: calls.append((method, url))
        or ({"status": "ok"} if url.endswith("/validation/soak") else []),
    )

    class Args:
        mode = "compose-soak-readability"
        database_url = "postgresql://crypto:crypto@postgres:5432/crypto"
        api_port = 8012
        project_name = "crypto_pg_validation"
        startup_timeout = 90.0
        keep_up = False

    result = run_validation_mode(Args())

    assert result["mode"] == "compose-soak-readability"
    assert result["soak_validation"] == {"status": "ok"}
    assert result["soak_history"] == []
    assert result["docker_logs"] == "api-1 | up\nscheduler-1 | up\n"
    assert result["api_logs"] == "api-1 | ready\n"
    assert result["scheduler_logs_full"] == "scheduler-1 | tick\n"
    assert result["postgres_logs"] == "postgres-1 | healthy\n"
    assert any(call[0] == "GET" and str(call[1]).endswith("/validation/soak") for call in calls)
    assert any(call[0] == "GET" and str(call[1]).endswith("/validation/soak/history") for call in calls)


def test_auto_id_column_sql_supports_sqlite_and_postgres() -> None:
    assert _auto_id_column_sql("sqlite") == "id INTEGER PRIMARY KEY"
    assert _auto_id_column_sql("postgres") == "id BIGSERIAL PRIMARY KEY"


def test_get_backend_name_detects_postgres_adapter() -> None:
    class DummyRawConnection:
        def cursor(self):
            raise AssertionError("cursor should not be used")

        def commit(self):
            return None

        def close(self):
            return None

    connection = db_module.PostgresConnectionAdapter(DummyRawConnection())

    assert get_backend_name(connection) == "postgres"


def test_db_helpers_support_postgres_introspection_queries() -> None:
    executed: list[tuple[str, object]] = []

    class DummyConnection:
        def execute(self, query: str, params=None):
            executed.append((" ".join(query.split()), params))

            class DummyCursor:
                def __init__(self, rows):
                    self._rows = rows
                    self.description = [("name",)] if rows and len(rows[0]) == 1 else None

                def fetchall(self):
                    return self._rows

                def fetchone(self):
                    return self._rows[0] if self._rows else None

            normalized = " ".join(query.split())
            if "FROM pg_catalog.pg_tables" in normalized:
                return DummyCursor([("audit_events",), ("candles",)])
            if "FROM information_schema.tables" in normalized:
                return DummyCursor([("candles",)])
            if "FROM information_schema.columns" in normalized:
                return DummyCursor([("id",), ("symbol",)])
            raise AssertionError(f"Unexpected query: {normalized}")

    connection = DummyConnection()

    assert list_tables(connection, backend="postgres") == ["audit_events", "candles"]
    assert table_exists(connection, "candles", backend="postgres") is True
    assert get_table_columns(connection, "candles", backend="postgres") == {"id", "symbol"}
    assert any("%s" in query for query, _ in executed)


def test_rewrite_query_params_converts_sqlite_placeholders() -> None:
    query = "INSERT INTO demo (name, note) VALUES (?, '? literal stays', ?);"
    assert _rewrite_query_params(query) == "INSERT INTO demo (name, note) VALUES (%s, '? literal stays', %s);"


def test_parse_db_timestamp_supports_sqlite_and_postgres_formats() -> None:
    sqlite_parsed = parse_db_timestamp("2026-03-18 10:00:00")
    postgres_parsed = parse_db_timestamp("2026-03-18 10:00:00.622394+00:00")
    postgres_short_offset = parse_db_timestamp("2026-03-18 10:00:00.622394+00")

    assert sqlite_parsed.isoformat() == "2026-03-18T10:00:00+00:00"
    assert postgres_parsed.isoformat() == "2026-03-18T10:00:00.622394+00:00"
    assert postgres_short_offset.isoformat() == "2026-03-18T10:00:00.622394+00:00"


def test_get_connection_supports_postgres_backend(monkeypatch) -> None:
    class DummyRawConnection:
        def cursor(self):
            raise AssertionError("cursor should not be used in this test")

        def commit(self):
            return None

        def close(self):
            return None

    class DummyPsycopg:
        def connect(self, database_url: str):
            assert database_url == "postgresql://crypto:crypto@127.0.0.1:5432/crypto"
            return DummyRawConnection()

    monkeypatch.setattr(db_module, "DB_BACKEND", "postgres")
    monkeypatch.setattr(db_module, "DATABASE_URL", "postgresql://crypto:crypto@127.0.0.1:5432/crypto")
    monkeypatch.setattr(db_module, "_load_psycopg", lambda: DummyPsycopg())

    connection = get_connection()

    assert connection.__class__.__name__ == "PostgresConnectionAdapter"


def test_get_connection_retries_postgres_until_ready(monkeypatch) -> None:
    attempts: list[str] = []
    sleep_calls: list[float] = []

    class DummyRawConnection:
        def cursor(self):
            raise AssertionError("cursor should not be used in this test")

        def commit(self):
            return None

        def close(self):
            return None

    class DummyPsycopg:
        def connect(self, database_url: str):
            attempts.append(database_url)
            if len(attempts) < 3:
                raise RuntimeError("database system is starting up")
            return DummyRawConnection()

    monkeypatch.setattr(db_module, "DB_BACKEND", "postgres")
    monkeypatch.setattr(db_module, "DATABASE_URL", "postgresql://crypto:crypto@127.0.0.1:5432/crypto")
    monkeypatch.setattr(db_module, "POSTGRES_CONNECT_RETRIES", 3)
    monkeypatch.setattr(db_module, "POSTGRES_CONNECT_RETRY_DELAY_SECONDS", 0.25)
    monkeypatch.setattr(db_module, "_load_psycopg", lambda: DummyPsycopg())
    monkeypatch.setattr(db_module.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    connection = get_connection()

    assert connection.__class__.__name__ == "PostgresConnectionAdapter"
    assert len(attempts) == 3
    assert sleep_calls == [0.25, 0.25]


def test_insert_and_get_rowid_uses_returning_for_postgres() -> None:
    executed: list[tuple[str, object]] = []

    class DummyCursor:
        def __init__(self, rows, description):
            self._rows = rows
            self.description = description

        def fetchone(self):
            return self._rows[0] if self._rows else None

        def fetchall(self):
            return list(self._rows)

    class DummyRawConnection:
        def cursor(self):
            executed_ref = executed

            class CursorContext:
                description = None
                _rows = []

                def execute(self, query: str, params=None):
                    executed_ref.append((query, params))
                    if "RETURNING id" in query:
                        self.description = [("id",)]
                        self._rows = [(7,)]
                    else:
                        self.description = None
                        self._rows = []

                def fetchone(self):
                    return self._rows[0] if self._rows else None

                def fetchall(self):
                    return list(self._rows)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            return CursorContext()

        def commit(self):
            return None

        def close(self):
            return None

    connection = db_module.PostgresConnectionAdapter(DummyRawConnection())

    row_id = insert_and_get_rowid(
        connection,
        "INSERT INTO audit_events (event_type, status, source, message, payload_json) VALUES (?, ?, ?, ?, ?);",
        ("manual_action", "completed", "test", "hello", None),
    )

    assert row_id == 7
    assert executed[0][0].endswith("RETURNING id;")
    assert executed[0][1] == ("manual_action", "completed", "test", "hello", None)


def test_fetch_all_as_dicts_maps_cursor_description_to_dicts() -> None:
    class DummyCursor:
        description = [("id",), ("name",)]

        def fetchall(self):
            return [(1, "alpha"), (2, "beta")]

    class DummyConnection:
        def execute(self, query: str, params=None):
            assert query == "SELECT id, name FROM sample LIMIT %s;"
            assert params == (2,)
            return DummyCursor()

    result = fetch_all_as_dicts(DummyConnection(), "SELECT id, name FROM sample LIMIT %s;", (2,))

    assert result == [{"id": 1, "name": "alpha"}, {"id": 2, "name": "beta"}]


def test_query_read_service_supports_postgres_limit_queries() -> None:
    executed: list[tuple[str, object]] = []

    class DummyRawConnection:
        def cursor(self):
            executed_ref = executed

            class CursorContext:
                description = None
                _rows = []

                def execute(self, query: str, params=None):
                    executed_ref.append((query, params))
                    self.description = [
                        ("id",),
                        ("client_order_id",),
                        ("risk_event_id",),
                        ("symbol",),
                        ("timeframe",),
                        ("strategy_name",),
                        ("side",),
                        ("qty",),
                        ("price",),
                        ("status",),
                        ("created_at",),
                    ]
                    self._rows = [
                        (1, "order-1", 11, "BTCUSDT", "1m", "ma_cross", "BUY", 0.001, 100.0, "FILLED", "2026-03-18 10:00:00")
                    ]

                def fetchone(self):
                    return self._rows[0] if self._rows else None

                def fetchall(self):
                    return list(self._rows)

                def __enter__(self):
                    return self

                def __exit__(self, exc_type, exc, tb):
                    return False

            return CursorContext()

        def commit(self):
            return None

        def close(self):
            return None

    connection = db_module.PostgresConnectionAdapter(DummyRawConnection())

    rows = query_get_orders(connection, limit=1)

    assert rows == [
        {
            "id": 1,
            "client_order_id": "order-1",
            "risk_event_id": 11,
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "strategy_name": "ma_cross",
            "side": "BUY",
            "qty": 0.001,
            "price": 100.0,
            "status": "FILLED",
            "created_at": "2026-03-18 10:00:00",
        }
    ]
    assert executed[0][1] == (1,)
    assert "LIMIT %s" in executed[0][0]


def test_save_klines_does_not_duplicate_existing_candles() -> None:
    connection = make_connection()
    try:
        ensure_candles_table(connection)
        kline = make_kline(60_000, 10)

        save_klines(connection, [kline])
        save_klines(connection, [kline])

        row = connection.execute("SELECT COUNT(*) FROM candles;").fetchone()
        assert row is not None
        assert row[0] == 1
    finally:
        connection.close()


def test_evaluate_latest_signal_rejects_duplicate_signal_type() -> None:
    connection = make_connection()
    try:
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)

        first_signal = insert_signal(connection, "BUY", strategy_name="manual_test")
        first_risk = evaluate_latest_signal(connection)
        second_signal = insert_signal(connection, "BUY", strategy_name="manual_test")
        second_risk = evaluate_latest_signal(connection)

        assert first_signal["id"] != second_signal["id"]
        assert first_risk is not None
        assert first_risk["decision"] == "APPROVED"
        assert second_risk is not None
        assert second_risk["decision"] == "REJECTED"
        assert second_risk["reason"] == "Duplicate signal type."
    finally:
        connection.close()


def test_execute_latest_risk_only_creates_one_order_per_risk_event() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10, 11, 12, 13, 14])
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)
        ensure_execution_tables(connection)

        insert_signal(connection, "BUY", strategy_name="manual_test")
        risk_result = evaluate_latest_signal(connection)
        first_execution = execute_latest_risk(connection, order_qty=0.25)
        second_execution = execute_latest_risk(connection, order_qty=0.25)

        assert risk_result is not None
        assert first_execution is not None
        assert first_execution["status"] == "FILLED"
        assert second_execution == {
            "risk_event_id": risk_result["id"],
            "decision": "SKIPPED",
            "reason": "Already executed",
        }
        assert len(get_orders(connection, limit=5)) == 1
        assert len(get_fills(connection, limit=5)) == 1
    finally:
        connection.close()


def test_evaluate_latest_signal_rejects_buy_when_max_position_would_be_exceeded() -> None:
    connection = make_connection()
    try:
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)
        connection.execute(
            """
            INSERT INTO positions (symbol, qty, avg_price, realized_pnl)
            VALUES (?, ?, ?, ?);
            """,
            ("BTCUSDT", 0.0015, 100.0, 0.0),
        )
        connection.commit()

        insert_signal(connection, "BUY", strategy_name="manual_test")
        risk_result = evaluate_latest_signal(
            connection,
            order_qty=0.001,
            max_position_qty=0.002,
            cooldown_seconds=0,
        )

        assert risk_result is not None
        assert risk_result["decision"] == "REJECTED"
        assert "Max position exceeded" in risk_result["reason"]
    finally:
        connection.close()


def test_evaluate_latest_signal_rejects_when_cooldown_is_active() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10, 11, 12, 13, 14])
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)
        ensure_execution_tables(connection)

        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("recent-order", 1, "BTCUSDT", "1m", "manual_test", "BUY", 0.001, 100.0, "FILLED"),
        )
        order_id = connection.execute(
            "SELECT id FROM orders WHERE client_order_id = 'recent-order';"
        ).fetchone()[0]
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price) VALUES (?, ?, ?, ?, ?);",
            (order_id, "BTCUSDT", "BUY", 0.001, 100.0),
        )
        connection.commit()

        insert_signal(connection, "SELL", strategy_name="manual_test")
        risk_result = evaluate_latest_signal(connection, cooldown_seconds=300)

        assert risk_result is not None
        assert risk_result["decision"] == "REJECTED"
        assert "Cooldown active" in risk_result["reason"]
    finally:
        connection.close()


def test_evaluate_latest_signal_rejects_when_daily_loss_limit_is_breached() -> None:
    connection = make_connection()
    try:
        today = datetime.now(timezone.utc).date().isoformat()
        ensure_execution_tables(connection)
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-daily-1", 1, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED", f"{today} 10:00:00"),
        )
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-daily-1", 2, "BTCUSDT", "1m", "manual_test", "SELL", 1.0, 25.0, "FILLED", f"{today} 10:05:00"),
        )
        insert_fill(connection, 1, "BTCUSDT", "BUY", 1.0, 100.0, f"{today} 10:00:00")
        insert_fill(connection, 2, "BTCUSDT", "SELL", 1.0, 25.0, f"{today} 10:05:00")
        connection.commit()

        insert_signal(connection, "BUY", strategy_name="manual_test")
        risk_result = evaluate_latest_signal(
            connection,
            cooldown_seconds=0,
            max_daily_loss=50.0,
        )

        assert risk_result is not None
        assert risk_result["decision"] == "REJECTED"
        assert "Daily loss limit breached" in risk_result["reason"]
        assert "daily_realized_pnl=-75.0" in risk_result["reason"]
    finally:
        connection.close()


def test_evaluate_latest_signal_auto_enables_kill_switch_when_daily_loss_limit_is_breached(
    monkeypatch,
) -> None:
    connection = make_connection()
    kill_switch_calls = []
    monkeypatch.setattr(
        "app.risk.risk_service.enable_kill_switch",
        lambda reason, source, notify_message: kill_switch_calls.append(
            {
                "reason": reason,
                "source": source,
                "notify_message": notify_message,
            }
        )
        or "runtime/kill.switch",
    )
    try:
        today = datetime.now(timezone.utc).date().isoformat()
        ensure_execution_tables(connection)
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-daily-2", 1, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED", f"{today} 11:00:00"),
        )
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-daily-2", 2, "BTCUSDT", "1m", "manual_test", "SELL", 1.0, 25.0, "FILLED", f"{today} 11:05:00"),
        )
        insert_fill(connection, 1, "BTCUSDT", "BUY", 1.0, 100.0, f"{today} 11:00:00")
        insert_fill(connection, 2, "BTCUSDT", "SELL", 1.0, 25.0, f"{today} 11:05:00")
        connection.commit()

        insert_signal(connection, "BUY", strategy_name="manual_test")
        risk_result = evaluate_latest_signal(
            connection,
            cooldown_seconds=0,
            max_daily_loss=50.0,
        )

        assert risk_result is not None
        assert risk_result["decision"] == "REJECTED"
        assert len(kill_switch_calls) == 1
        assert kill_switch_calls[0]["source"] == "risk_service"
        assert "Daily loss limit breached" in kill_switch_calls[0]["reason"]
        assert "auto-enabled" in kill_switch_calls[0]["notify_message"]
    finally:
        connection.close()


def test_daily_realized_pnl_ledger_ignores_previous_day_losses() -> None:
    connection = make_connection()
    try:
        previous_day = (datetime.now(timezone.utc).date() - timedelta(days=1)).isoformat()
        ensure_execution_tables(connection)
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-prev-day", 1, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED", f"{previous_day} 10:00:00"),
        )
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-prev-day", 2, "BTCUSDT", "1m", "manual_test", "SELL", 1.0, 25.0, "FILLED", f"{previous_day} 10:05:00"),
        )
        insert_fill(connection, 1, "BTCUSDT", "BUY", 1.0, 100.0, f"{previous_day} 10:00:00")
        insert_fill(connection, 2, "BTCUSDT", "SELL", 1.0, 25.0, f"{previous_day} 10:05:00")
        connection.commit()

        assert get_daily_realized_pnl(connection, "BTCUSDT", pnl_date=previous_day) == -75.0

        insert_signal(connection, "BUY", strategy_name="manual_test")
        risk_result = evaluate_latest_signal(
            connection,
            cooldown_seconds=0,
            max_daily_loss=50.0,
        )

        assert risk_result is not None
        assert risk_result["decision"] == "APPROVED"
    finally:
        connection.close()


def test_update_positions_and_pnl_snapshots_track_realized_and_unrealized_pnl() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [100, 101, 102, 103, 110])
        ensure_execution_tables(connection)
        ensure_positions_table(connection)
        ensure_pnl_table(connection)

        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-1", 1, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED"),
        )
        order_id = connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-1';").fetchone()[0]
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price) VALUES (?, ?, ?, ?, ?);",
            (order_id, "BTCUSDT", "BUY", 1.0, 100.0),
        )

        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-2", 2, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 120.0, "FILLED"),
        )
        order_id = connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-2';").fetchone()[0]
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price) VALUES (?, ?, ?, ?, ?);",
            (order_id, "BTCUSDT", "BUY", 1.0, 120.0),
        )

        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-1", 3, "BTCUSDT", "1m", "manual_test", "SELL", 0.5, 130.0, "FILLED"),
        )
        order_id = connection.execute("SELECT id FROM orders WHERE client_order_id = 'sell-1';").fetchone()[0]
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price) VALUES (?, ?, ?, ?, ?);",
            (order_id, "BTCUSDT", "SELL", 0.5, 130.0),
        )
        connection.commit()

        updated_symbols = update_positions(connection)
        snapshot_count = update_pnl_snapshots(connection)

        assert updated_symbols == 1
        assert snapshot_count == 1
        position = get_positions(connection, limit=1)[0]
        assert position["qty"] == 1.5
        assert position["avg_price"] == 110.0
        assert position["realized_pnl"] == 10.0

        pnl_snapshot = get_pnl_snapshots(connection, limit=1)[0]
        assert pnl_snapshot["market_price"] == 110.0
        assert pnl_snapshot["unrealized_pnl"] == 0.0
    finally:
        connection.close()


def test_run_pipeline_collect_runs_end_to_end(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "market_data.db"

    def fake_connection() -> sqlite3.Connection:
        return sqlite3.connect(db_path)

    monkeypatch.setattr("app.pipeline.run_pipeline.DB_FILE", db_path)
    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", fake_connection)
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda: [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])],
    )

    result = run_pipeline_collect()

    step_names = [step["step"] for step in result["steps"]]
    assert step_names == [
        "save_klines",
        "generate_signal",
        "evaluate_risk",
        "paper_execute",
        "update_positions",
        "update_pnl",
    ]

    connection = sqlite3.connect(db_path)
    try:
        risk_event = get_risk_events(connection, limit=1)[0]
        order = get_orders(connection, limit=1)[0]
        position = get_positions(connection, limit=1)[0]

        assert risk_event["decision"] == "APPROVED"
        assert order["status"] == "FILLED"
        assert position["qty"] == 0.001
    finally:
        connection.close()


def test_run_pipeline_collect_uses_selected_strategy(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "market_data_strategy.db"

    def fake_connection() -> sqlite3.Connection:
        return sqlite3.connect(db_path)

    monkeypatch.setattr("app.pipeline.run_pipeline.DB_FILE", db_path)
    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", fake_connection)
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda: [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])],
    )

    result = run_pipeline_collect(strategy_name="momentum_3bar")

    assert result["strategy_name"] == "momentum_3bar"
    assert result["steps"][1]["strategy_name"] == "momentum_3bar"


def test_run_pipeline_collect_is_blocked_when_kill_switch_is_enabled(monkeypatch, tmp_path) -> None:
    kill_switch_path = tmp_path / "kill.switch"
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: True)
    monkeypatch.setattr(
        "app.pipeline.run_pipeline.get_kill_switch_status",
        lambda: {"enabled": True, "kill_switch_file": str(kill_switch_path)},
    )

    result = run_pipeline_collect()

    assert result["steps"] == [
        {
            "step": "kill_switch",
            "status": "blocked",
            "enabled": True,
            "kill_switch_file": str(kill_switch_path),
            "reason": "Kill switch is enabled.",
        }
    ]


def test_run_pipeline_collect_returns_failed_result_when_fetch_klines_errors(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "pipeline-failure.db"

    monkeypatch.setattr("app.pipeline.run_pipeline.DB_FILE", db_path)
    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.audit.service.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda: (_ for _ in ()).throw(RuntimeError("Binance API unavailable")),
    )

    result = run_pipeline_collect()

    assert result["steps"] == [
        {
            "step": "save_klines",
            "status": "failed",
            "error": "Binance API unavailable",
            "error_type": "RuntimeError",
        }
    ]

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
        events = get_audit_events(connection, limit=5)
    finally:
        connection.close()

    assert any(item["component"] == "pipeline" and item["status"] == "failed" for item in heartbeats)
    assert any(
        item["event_type"] == "pipeline_run" and item["status"] == "failed" and "Binance API unavailable" in item["message"]
        for item in events
    )


def test_run_pipeline_collect_returns_failed_result_when_initial_migration_errors(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "pipeline-initial-failure.db"

    monkeypatch.setattr("app.pipeline.run_pipeline.get_database_label", lambda: "sqlite:///pipeline-initial-failure.db")
    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.audit.service.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.pipeline.run_pipeline.run_migrations",
        lambda _connection: (_ for _ in ()).throw(RuntimeError("migration bootstrap failed")),
    )

    result = run_pipeline_collect()

    assert result["database"] == "sqlite:///pipeline-initial-failure.db"
    assert result["steps"] == [
        {
            "step": "run_migrations",
            "status": "failed",
            "error": "migration bootstrap failed",
            "error_type": "RuntimeError",
        }
    ]


def test_health_endpoint_reports_ok_with_recent_pipeline_activity(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "health.db"
    log_path = tmp_path / "scheduler.log"
    log_path.write_text("[2026-03-18T10:00:00] run=1 signal=BUY risk=APPROVED execution=FILLED BUY\n", encoding="utf-8")
    fixed_now = datetime(2026, 3, 18, 10, 0, 0, tzinfo=timezone.utc)
    latest_open_time = int(fixed_now.timestamp() * 1000) - 60_000

    connection = sqlite3.connect(db_path)
    try:
        ensure_candles_table(connection)
        save_klines(
            connection,
            [
                make_kline(latest_open_time - 240_000, 10),
                make_kline(latest_open_time - 180_000, 11),
                make_kline(latest_open_time - 120_000, 12),
                make_kline(latest_open_time - 60_000, 13),
                make_kline(latest_open_time, 14),
            ],
        )
        ensure_signals_table(connection)
        ensure_risk_table(connection)
        ensure_execution_tables(connection)
        ensure_positions_table(connection)
        insert_signal(connection, "BUY", strategy_name="manual_test")
        evaluate_latest_signal(connection, cooldown_seconds=0)
        execute_latest_risk(connection)
        update_positions(connection)
    finally:
        connection.close()

    monkeypatch.setattr("app.api.main.DB_FILE", db_path)
    monkeypatch.setattr("app.api.main.LOG_FILE", log_path)
    monkeypatch.setattr("app.api.main.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.api.main._utc_now", lambda: fixed_now)
    monkeypatch.setattr(
        "app.api.main.get_stop_status",
        lambda: {"stopped": False, "stop_file": str(tmp_path / "scheduler.stop")},
    )
    monkeypatch.setattr(
        "app.api.main.read_scheduler_log",
        lambda lines=1: log_path.read_text(encoding="utf-8").splitlines()[-lines:],
    )
    called = []
    monkeypatch.setattr("app.api.main.maybe_send_health_alert", lambda report: called.append(report) or {"sent": False})
    monkeypatch.setattr(
        "app.api.main.get_kill_switch_status",
        lambda: {"enabled": False, "kill_switch_file": str(tmp_path / "kill.switch")},
    )

    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["database"]["status"] == "ok"
    assert payload["checks"]["candles"]["status"] == "ok"
    assert payload["checks"]["pipeline"]["status"] == "ok"
    assert payload["checks"]["scheduler"]["status"] == "ok"
    assert payload["checks"]["kill_switch"]["status"] == "ok"
    assert payload["config"]["max_daily_loss"] == 50.0
    assert len(called) == 1


def test_health_endpoint_reports_degraded_when_scheduler_stopped_and_no_candles(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "health-degraded.db"
    connection = sqlite3.connect(db_path)
    try:
        ensure_candles_table(connection)
        ensure_signals_table(connection)
        ensure_risk_table(connection)
        ensure_execution_tables(connection)
        ensure_positions_table(connection)
    finally:
        connection.close()

    monkeypatch.setattr("app.api.main.DB_FILE", db_path)
    monkeypatch.setattr("app.api.main.LOG_FILE", Path(tmp_path / "missing.log"))
    monkeypatch.setattr("app.api.main.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.api.main.get_stop_status",
        lambda: {"stopped": True, "stop_file": str(tmp_path / "scheduler.stop")},
    )
    monkeypatch.setattr("app.api.main.read_scheduler_log", lambda lines=1: [])
    called = []
    monkeypatch.setattr("app.api.main.maybe_send_health_alert", lambda report: called.append(report) or {"sent": False})
    monkeypatch.setattr(
        "app.api.main.get_kill_switch_status",
        lambda: {"enabled": True, "kill_switch_file": str(tmp_path / "kill.switch")},
    )

    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["checks"]["candles"]["status"] == "degraded"
    assert payload["checks"]["pipeline"]["status"] == "degraded"
    assert payload["checks"]["scheduler"]["status"] == "degraded"
    assert payload["checks"]["kill_switch"]["status"] == "degraded"
    assert len(called) == 1


def test_maybe_send_health_alert_deduplicates_same_degraded_state(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "health_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.health.HEALTH_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.health.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.health import maybe_send_health_alert

    report = {
        "status": "degraded",
        "checks": {
            "scheduler": {"status": "degraded"},
            "kill_switch": {"status": "ok"},
        },
    }

    first = maybe_send_health_alert(report)
    second = maybe_send_health_alert(report)

    assert first["sent"] is True
    assert second == {"sent": False, "reason": "Health alert already sent for current state."}
    assert len(sent_messages) == 1


def test_maybe_send_health_alert_clears_state_when_health_returns_ok(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "health_alert_state.json"
    monkeypatch.setattr("app.alerting.health.HEALTH_ALERT_STATE_FILE", state_file)

    from app.alerting.health import maybe_send_health_alert

    state_file.write_text('{"fingerprint":"x","status":"degraded"}', encoding="utf-8")
    result = maybe_send_health_alert({"status": "ok", "checks": {}})

    assert result == {"sent": False, "reason": "Health status is ok."}
    assert not state_file.exists()


def test_maybe_send_health_alert_ignores_volatile_heartbeat_fields(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "health_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.health.HEALTH_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.health.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.health import maybe_send_health_alert

    first_report = {
        "status": "degraded",
        "checks": {
            "kill_switch": {"status": "degraded", "enabled": True, "reason": "Kill switch is enabled."},
            "heartbeats": {
                "status": "ok",
                "components": [
                    {
                        "component": "alerting",
                        "status": "ok",
                        "message": "Telegram alert delivered.",
                        "last_seen_at": "2026-03-18 22:07:00",
                    }
                ],
            },
        },
    }
    second_report = {
        "status": "degraded",
        "checks": {
            "kill_switch": {"status": "degraded", "enabled": True, "reason": "Kill switch is enabled."},
            "heartbeats": {
                "status": "ok",
                "components": [
                    {
                        "component": "alerting",
                        "status": "ok",
                        "message": "Telegram alert delivered.",
                        "last_seen_at": "2026-03-18 22:08:00",
                    }
                ],
            },
        },
    }

    first = maybe_send_health_alert(first_report)
    second = maybe_send_health_alert(second_report)

    assert first["sent"] is True
    assert second == {"sent": False, "reason": "Health alert already sent for current state."}
    assert len(sent_messages) == 1


def test_admin_page_is_served() -> None:
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Admin Console" in response.text
    assert "/pipeline/run" in response.text
    assert "/strategies" in response.text
    assert "/strategies/summary" in response.text
    assert "/strategies/closed-trades" in response.text
    assert "/audit-events?limit=20" in response.text
    assert "/alerts/status" in response.text
    assert "/alerts/test" in response.text
    assert "/validation/soak" in response.text
    assert "/validation/soak/history" in response.text
    assert "/validation/soak/record" in response.text
    assert "Alert Delivery" in response.text
    assert "Runtime Heartbeats" in response.text
    assert "Pause Auto Refresh" in response.text
    assert "Auto refresh every 10 seconds." in response.text
    assert 'id="heartbeats-json"' in response.text
    assert 'id="market-data-status"' in response.text
    assert 'id="data-worker-status"' in response.text
    assert 'id="strategy-worker-status"' in response.text
    assert 'id="execution-worker-status"' in response.text
    assert 'id="alerting-runtime-status"' in response.text
    assert 'id="logs-mode-select"' in response.text
    assert 'id="pipeline-strategy-select"' in response.text
    assert 'id="scheduler-strategy-select"' in response.text
    assert 'id="scheduler-disabled-strategy-select"' in response.text
    assert 'id="scheduler-priority-controls"' in response.text
    assert 'id="scheduler-disabled-note-controls"' in response.text
    assert 'id="scheduler-effective-limit-input"' in response.text
    assert 'id="strategy-summary-board"' in response.text
    assert 'data-strategy-name="' in response.text
    assert 'data-promote-strategy="' in response.text
    assert 'data-disable-strategy="' in response.text
    assert 'data-enable-strategy="' in response.text
    assert 'id="strategy-closed-trades-board"' in response.text
    assert 'id="selected-strategy-board"' in response.text
    assert 'id="strategy-sort-select"' in response.text
    assert '<option value="latest_activity_at">latest activity</option>' in response.text
    assert '<option value="latest_closed_pnl">latest closed pnl</option>' in response.text
    assert '<option value="realized_trade_count">realized trades</option>' in response.text
    assert 'id="strategy-filter-select"' in response.text
    assert '<option value="fresh">fresh</option>' in response.text
    assert '<option value="stale">stale</option>' in response.text
    assert '<option value="idle">idle</option>' in response.text
    assert 'id="closed-trades-strategy-select"' in response.text
    assert 'id="closed-trades-reset-button"' in response.text
    assert 'id="scheduler-detail"' in response.text
    assert "effective order:" in response.text
    assert "limit:" in response.text
    assert "excluded by limit:" in response.text
    assert "disabled notes:" in response.text
    assert "warning: no enabled active strategies" in response.text
    assert "/scheduler/strategy" in response.text
    assert 'id="issue-strip"' in response.text
    assert 'id="pipeline-status"' in response.text
    assert "momentum_3bar" in response.text
    assert "Last Pipeline" in response.text
    assert "Send Test Alert" in response.text
    assert "Soak Validation" in response.text
    assert "Record Snapshot" in response.text
    assert "Latest Closed Symbol" in response.text
    assert "Latest Closed Status" in response.text
    assert "Latest Closed At" in response.text
    assert "Latest Closed PnL" in response.text
    assert "Disabled Reason" in response.text
    assert "LIMITED" in response.text
    assert "Promote" in response.text
    assert "Disable" in response.text
    assert "Enable" in response.text
    assert "Selected Strategy Details" in response.text
    assert "Select a strategy card to inspect a single strategy." in response.text
    assert "Closed Trades" in response.text
    assert "Recent Closed Trades" in response.text
    assert "Win Rate" in response.text
    assert "Last Closed Result" in response.text
    assert "Latest Activity" in response.text
    assert "Latest Order At" in response.text
    assert "Latest Fill At" in response.text
    assert "strategy-card clickable" in response.text
    assert "STRATEGY_STALE_AFTER_MINUTES" in response.text
    assert "FRESH" in response.text
    assert "STALE" in response.text
    assert "IDLE" in response.text


def test_root_redirects_to_admin() -> None:
    client = TestClient(app)

    response = client.get("/", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/admin"


def test_pipeline_run_endpoint_accepts_strategy_name(monkeypatch) -> None:
    client = TestClient(app)
    called: list[str] = []

    monkeypatch.setattr(
        "app.api.main.run_pipeline_collect",
        lambda strategy_name="ma_cross": called.append(strategy_name) or {
            "status": "completed",
            "strategy_name": strategy_name,
            "steps": [],
        },
    )

    response = client.post("/pipeline/run", json={"strategy_name": "momentum_3bar"})

    assert response.status_code == 200
    assert response.json()["strategy_name"] == "momentum_3bar"
    assert called == ["momentum_3bar"]


def test_strategies_endpoint_lists_registered_strategies() -> None:
    client = TestClient(app)

    response = client.get("/strategies")

    assert response.status_code == 200
    payload = response.json()
    assert payload["default_strategy"] == "ma_cross"
    assert "ma_cross" in payload["strategies"]
    assert "momentum_3bar" in payload["strategies"]


def test_strategy_summary_endpoint_returns_grouped_activity(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr("app.api.main.get_connection", lambda: object())
    monkeypatch.setattr(
        "app.api.main.get_strategy_activity_summary",
        lambda connection: [
            {
                "strategy_name": "ma_cross",
                "latest_signal": {"signal_type": "BUY"},
                "latest_risk": {"decision": "APPROVED"},
                "latest_order": {"status": "FILLED"},
                "latest_fill": {"side": "BUY"},
                "filled_order_count": 1,
                "filled_qty_total": 0.5,
                "net_position_qty": 0.25,
                "gross_realized_pnl": 12.5,
                "buy_fill_count": 2,
                "sell_fill_count": 1,
                "realized_trade_count": 1,
                "winning_trade_count": 1,
                "losing_trade_count": 0,
                "breakeven_trade_count": 0,
                "has_activity": True,
            },
            {
                "strategy_name": "momentum_3bar",
                "latest_signal": None,
                "latest_risk": None,
                "latest_order": None,
                "latest_fill": None,
                "filled_order_count": 0,
                "filled_qty_total": 0.0,
                "net_position_qty": 0.0,
                "gross_realized_pnl": 0.0,
                "buy_fill_count": 0,
                "sell_fill_count": 0,
                "realized_trade_count": 0,
                "winning_trade_count": 0,
                "losing_trade_count": 0,
                "breakeven_trade_count": 0,
                "has_activity": False,
            },
        ],
    )

    class DummyConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr("app.api.main.get_connection", lambda: DummyConnection())

    response = client.get("/strategies/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["strategy_name"] == "ma_cross"
    assert payload[0]["latest_risk"]["decision"] == "APPROVED"
    assert payload[0]["latest_fill"]["side"] == "BUY"
    assert payload[0]["filled_order_count"] == 1
    assert payload[0]["gross_realized_pnl"] == 12.5
    assert payload[0]["winning_trade_count"] == 1
    assert payload[1]["strategy_name"] == "momentum_3bar"


def test_strategy_closed_trades_endpoint_returns_recent_trades(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, object] = {}

    class DummyConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr("app.api.main.get_connection", lambda: DummyConnection())
    def fake_get_strategy_closed_trades(connection, limit=20, strategy_name=None):
        captured["limit"] = limit
        captured["strategy_name"] = strategy_name
        return [
            {
                "strategy_name": "ma_cross",
                "symbol": "BTCUSDT",
                "qty": 1.0,
                "entry_price": 100.0,
                "exit_price": 110.0,
                "realized_pnl": 10.0,
                "closed_at": "2026-03-19 10:05:00",
                "order_id": 2,
                "status": "win",
            }
        ]
    monkeypatch.setattr("app.api.main.get_strategy_closed_trades", fake_get_strategy_closed_trades)

    response = client.get("/strategies/closed-trades?limit=10&strategy_name=ma_cross")

    assert response.status_code == 200
    assert captured == {"limit": 10, "strategy_name": "ma_cross"}
    payload = response.json()
    assert payload[0]["strategy_name"] == "ma_cross"
    assert payload[0]["realized_pnl"] == 10.0


def test_scheduler_strategy_endpoints_round_trip(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "app.api.main.get_strategy_status",
        lambda: {
            "strategy_name": "ma_cross",
            "strategy_names": ["ma_cross", "momentum_3bar"],
            "disabled_strategy_names": ["momentum_3bar"],
            "effective_strategy_names": ["ma_cross"],
            "effective_strategy_limit": 1,
            "strategy_priorities": {"ma_cross": 0, "momentum_3bar": 10},
            "disabled_strategy_notes": {"momentum_3bar": "cooldown investigation"},
            "default_strategy": "ma_cross",
            "strategy_file": "runtime/scheduler.strategy",
            "disabled_strategy_file": "runtime/scheduler.strategy.disabled",
            "priority_file": "runtime/scheduler.strategy.priority.json",
            "disabled_reason_file": "runtime/scheduler.strategy.disabled.reason.json",
            "effective_limit_file": "runtime/scheduler.strategy.limit",
            "available_strategies": ["ma_cross", "momentum_3bar"],
        },
    )
    def fake_set_active_strategies(strategy_names):
        captured["active"] = strategy_names
        return {
            "strategy_name": strategy_names[0],
            "strategy_names": strategy_names,
            "strategy_file": "runtime/scheduler.strategy",
        }

    def fake_set_disabled_strategies(strategy_names):
        captured["disabled"] = strategy_names
        return {
            "disabled_strategy_names": strategy_names,
            "disabled_strategy_file": "runtime/scheduler.strategy.disabled",
        }

    def fake_set_strategy_priorities(strategy_priorities):
        captured["priorities"] = strategy_priorities
        return {
            "strategy_priorities": strategy_priorities,
            "priority_file": "runtime/scheduler.strategy.priority.json",
        }

    def fake_set_disabled_strategy_notes(strategy_notes):
        captured["notes"] = strategy_notes
        return {
            "disabled_strategy_notes": strategy_notes,
            "disabled_reason_file": "runtime/scheduler.strategy.disabled.reason.json",
        }

    def fake_set_effective_strategy_limit(limit):
        captured["limit"] = limit
        return {
            "effective_strategy_limit": limit,
            "effective_limit_file": "runtime/scheduler.strategy.limit",
        }

    monkeypatch.setattr("app.api.main.set_active_strategies", fake_set_active_strategies)
    monkeypatch.setattr("app.api.main.set_disabled_strategies", fake_set_disabled_strategies)
    monkeypatch.setattr("app.api.main.set_strategy_priorities", fake_set_strategy_priorities)
    monkeypatch.setattr("app.api.main.set_disabled_strategy_notes", fake_set_disabled_strategy_notes)
    monkeypatch.setattr("app.api.main.set_effective_strategy_limit", fake_set_effective_strategy_limit)

    status_response = client.get("/scheduler/strategy")
    update_response = client.post(
        "/scheduler/strategy",
        json={
            "strategy_names": ["ma_cross", "momentum_3bar"],
            "disabled_strategy_names": ["momentum_3bar"],
            "strategy_priorities": {"ma_cross": 0, "momentum_3bar": 10},
            "disabled_strategy_notes": {"momentum_3bar": "cooldown investigation"},
            "effective_strategy_limit": 1,
        },
    )

    assert status_response.status_code == 200
    assert status_response.json()["strategy_name"] == "ma_cross"
    assert status_response.json()["strategy_names"] == ["ma_cross", "momentum_3bar"]
    assert status_response.json()["disabled_strategy_names"] == ["momentum_3bar"]
    assert status_response.json()["effective_strategy_names"] == ["ma_cross"]
    assert status_response.json()["effective_strategy_limit"] == 1
    assert update_response.status_code == 200
    assert update_response.json()["strategy_names"] == ["ma_cross", "momentum_3bar"]
    assert update_response.json()["disabled_strategy_names"] == ["momentum_3bar"]
    assert update_response.json()["strategy_priorities"] == {"ma_cross": 0, "momentum_3bar": 10}
    assert update_response.json()["disabled_strategy_notes"] == {"momentum_3bar": "cooldown investigation"}
    assert update_response.json()["effective_strategy_limit"] == 1
    assert captured == {
        "active": ["ma_cross", "momentum_3bar"],
        "disabled": ["momentum_3bar"],
        "priorities": {"ma_cross": 0, "momentum_3bar": 10},
        "notes": {"momentum_3bar": "cooldown investigation"},
        "limit": 1,
    }


def test_favicon_returns_no_content() -> None:
    client = TestClient(app)

    response = client.get("/favicon.ico")

    assert response.status_code == 204


def test_alerts_status_reports_configuration(monkeypatch) -> None:
    monkeypatch.setattr("app.api.main.telegram_configured", lambda: True)
    client = TestClient(app)

    response = client.get("/alerts/status")

    assert response.status_code == 200
    assert response.json() == {"telegram_configured": True}


def test_alerts_test_endpoint_returns_sender_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.api.main.send_telegram_message",
        lambda text: {"sent": True, "response": {"ok": True, "text": text}},
    )
    client = TestClient(app)

    response = client.post("/alerts/test", json={"message": "hello"})

    assert response.status_code == 200
    assert response.json()["sent"] is True
    assert response.json()["response"]["text"] == "hello"


def test_send_telegram_message_returns_not_configured_when_env_missing(monkeypatch) -> None:
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_CHAT_ID", "")
    audit_calls = []
    monkeypatch.setattr("app.alerting.telegram.log_event", lambda **kwargs: audit_calls.append(kwargs))

    from app.alerting.telegram import send_telegram_message

    result = send_telegram_message("hello")

    assert result == {"sent": False, "reason": "Telegram is not configured."}
    assert audit_calls == [
        {
            "event_type": "alert_delivery",
            "status": "skipped",
            "source": "telegram",
            "message": "Telegram delivery skipped because configuration is missing.",
            "payload": {
                "text": "hello",
                "sent": False,
                "reason": "Telegram is not configured.",
            },
        }
    ]


def test_send_telegram_message_returns_failure_instead_of_raising(monkeypatch) -> None:
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_CHAT_ID", "chat")
    audit_calls = []
    monkeypatch.setattr("app.alerting.telegram.log_event", lambda **kwargs: audit_calls.append(kwargs))

    import requests

    def raise_timeout(*args, **kwargs):
        raise requests.ConnectTimeout("timed out")

    monkeypatch.setattr("app.alerting.telegram.requests.post", raise_timeout)

    from app.alerting.telegram import send_telegram_message

    result = send_telegram_message("hello")

    assert result["sent"] is False
    assert "Telegram send failed" in result["reason"]
    assert audit_calls == [
        {
            "event_type": "alert_delivery",
            "status": "failed",
            "source": "telegram",
            "message": "Telegram alert delivery failed.",
            "payload": {
                "text": "hello",
                "sent": False,
                "reason": result["reason"],
            },
        }
    ]


def test_send_telegram_message_logs_successful_delivery(monkeypatch) -> None:
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_CHAT_ID", "chat")
    audit_calls = []
    monkeypatch.setattr("app.alerting.telegram.log_event", lambda **kwargs: audit_calls.append(kwargs))

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"ok": True, "result": {"message_id": 1}}

    monkeypatch.setattr("app.alerting.telegram.requests.post", lambda *args, **kwargs: DummyResponse())

    from app.alerting.telegram import send_telegram_message

    result = send_telegram_message("hello")

    assert result == {"sent": True, "response": {"ok": True, "result": {"message_id": 1}}}
    assert audit_calls == [
        {
            "event_type": "alert_delivery",
            "status": "sent",
            "source": "telegram",
            "message": "Telegram alert delivered.",
            "payload": {
                "text": "hello",
                "telegram_ok": True,
                "chat_id": "chat",
            },
        }
    ]


def test_send_telegram_message_records_alerting_heartbeat(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "alerting-heartbeat.db"
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_CHAT_ID", "chat")
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.alerting.telegram.log_event", lambda **kwargs: None)

    class DummyResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"ok": True}

    monkeypatch.setattr("app.alerting.telegram.requests.post", lambda *args, **kwargs: DummyResponse())

    from app.alerting.telegram import send_telegram_message

    result = send_telegram_message("hello")

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()

    assert result["sent"] is True
    assert any(item["component"] == "alerting" and item["status"] == "ok" for item in heartbeats)


def test_health_reports_database_info(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "db-info.db"
    connection = sqlite3.connect(db_path)
    try:
        ensure_candles_table(connection)
        upsert_heartbeat(connection, "scheduler", "ok", "Scheduler loop completed.")
    finally:
        connection.close()

    monkeypatch.setattr("app.api.main.DB_FILE", db_path)
    monkeypatch.setattr(
        "app.api.main.get_database_info",
        lambda: {"backend": "sqlite", "sqlite_path": str(db_path)},
    )
    monkeypatch.setattr("app.api.main.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.api.main.get_stop_status",
        lambda: {"stopped": False, "stop_file": str(tmp_path / "scheduler.stop")},
    )
    monkeypatch.setattr(
        "app.api.main.get_kill_switch_status",
        lambda: {"enabled": False, "kill_switch_file": str(tmp_path / "kill.switch")},
    )
    monkeypatch.setattr("app.api.main.read_scheduler_log", lambda lines=1: [])

    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["database_info"]["backend"] == "sqlite"
    assert payload["database_info"]["sqlite_path"] == str(db_path)
    assert payload["checks"]["heartbeats"]["status"] == "ok"


def test_health_report_uses_postgres_database_url_when_backend_is_postgres(monkeypatch) -> None:
    class DummyConnection:
        def execute(self, query: str, params=None):
            normalized = " ".join(query.split())

            class DummyCursor:
                def __init__(self, rows):
                    self._rows = rows
                    self.description = [("name",)] if rows else None

                def fetchone(self):
                    return self._rows[0] if self._rows else None

                def fetchall(self):
                    return list(self._rows)

            if "FROM pg_catalog.pg_tables" in normalized:
                return DummyCursor([("candles",), ("runtime_heartbeats",)])
            if "FROM candles" in normalized:
                return DummyCursor([( "BTCUSDT", "1m", 0, 0)])
            if "FROM information_schema.tables" in normalized:
                return DummyCursor([])
            if "FROM runtime_heartbeats" in normalized:
                cursor = DummyCursor([])
                cursor.description = [
                    ("component",),
                    ("status",),
                    ("message",),
                    ("payload_json",),
                    ("last_seen_at",),
                ]
                return cursor
            raise AssertionError(f"Unexpected query: {normalized}")

        def close(self):
            return None

    monkeypatch.setattr("app.api.main.get_connection", lambda: DummyConnection())
    monkeypatch.setattr(
        "app.api.main.get_database_info",
        lambda: {
            "backend": "postgres",
            "database_url": "postgresql://crypto:crypto@127.0.0.1:5432/crypto",
        },
    )
    monkeypatch.setattr("app.api.main._utc_now", lambda: datetime.fromtimestamp(0, tz=timezone.utc))
    monkeypatch.setattr(
        "app.api.main.get_stop_status",
        lambda: {"stopped": False, "stop_file": "runtime/scheduler.stop"},
    )
    monkeypatch.setattr("app.api.main.read_scheduler_log", lambda lines=1: [])
    monkeypatch.setattr(
        "app.api.main.get_kill_switch_status",
        lambda: {"enabled": False, "kill_switch_file": "runtime/kill.switch"},
    )

    payload = app.openapi()  # keep app imported/initialized
    del payload
    report = __import__("app.api.main", fromlist=["build_health_report"]).build_health_report()

    assert report["database"] == "postgresql://crypto:crypto@127.0.0.1:5432/crypto"
    assert report["database_info"]["backend"] == "postgres"


def test_run_pipeline_collect_uses_postgres_database_label(monkeypatch) -> None:
    monkeypatch.setattr("app.pipeline.run_pipeline.get_database_label", lambda: "postgresql://crypto:crypto@127.0.0.1:5432/crypto")
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: True)
    monkeypatch.setattr(
        "app.pipeline.run_pipeline.get_kill_switch_status",
        lambda: {"enabled": True, "kill_switch_file": "runtime/kill.switch"},
    )

    result = run_pipeline_collect()

    assert result["database"] == "postgresql://crypto:crypto@127.0.0.1:5432/crypto"


def test_kill_switch_api_can_enable_and_disable(monkeypatch, tmp_path) -> None:
    kill_switch_path = tmp_path / "kill.switch"
    monkeypatch.setattr("app.system.kill_switch.KILL_SWITCH_FILE", kill_switch_path)
    monkeypatch.setattr("app.api.main.KILL_SWITCH_FILE", kill_switch_path, raising=False)

    client = TestClient(app)

    response = client.post("/kill-switch/enable")
    assert response.status_code == 200
    assert response.json()["enabled"] is True

    response = client.get("/kill-switch/status")
    assert response.status_code == 200
    assert response.json()["enabled"] is True

    response = client.post("/kill-switch/disable")
    assert response.status_code == 200
    assert response.json()["enabled"] is False


def test_enable_kill_switch_marks_repeat_enable_without_duplicate_alert(monkeypatch, tmp_path) -> None:
    kill_switch_path = tmp_path / "kill.switch"
    monkeypatch.setattr("app.system.kill_switch.KILL_SWITCH_FILE", kill_switch_path)
    sent_messages = []
    audit_calls = []
    monkeypatch.setattr("app.system.kill_switch.send_telegram_message", lambda text: sent_messages.append(text))
    monkeypatch.setattr("app.system.kill_switch.log_event", lambda **kwargs: audit_calls.append(kwargs))

    from app.system.kill_switch import enable_kill_switch

    enable_kill_switch(reason="First enable.", source="test", notify_message="first")
    enable_kill_switch(reason="Second enable.", source="test", notify_message="second")

    assert kill_switch_path.exists()
    assert sent_messages == ["first"]
    assert audit_calls[0]["status"] == "enabled"
    assert audit_calls[1]["status"] == "already_enabled"


def test_audit_events_endpoint_returns_logged_events(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "audit.db"

    monkeypatch.setattr("app.audit.service.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.api.main.get_connection", lambda: sqlite3.connect(db_path))

    from app.audit.service import ensure_table as ensure_audit_table
    from app.audit.service import insert_event

    connection = sqlite3.connect(db_path)
    try:
        ensure_audit_table(connection)
        insert_event(
            connection,
            event_type="manual_action",
            status="completed",
            source="test",
            message="Manual action recorded.",
            payload={"action": "demo"},
        )
    finally:
        connection.close()

    client = TestClient(app)
    response = client.get("/audit-events?limit=5")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["event_type"] == "manual_action"
    assert payload[0]["status"] == "completed"


def test_run_pipeline_collect_writes_audit_events(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "pipeline-audit.db"

    monkeypatch.setattr("app.pipeline.run_pipeline.DB_FILE", db_path)
    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.audit.service.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda: [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])],
    )
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)

    run_pipeline_collect()

    connection = sqlite3.connect(db_path)
    try:
        events = get_audit_events(connection, limit=10)
    finally:
        connection.close()

    event_types = [event["event_type"] for event in events]
    assert "pipeline_run" in event_types
    assert "risk_evaluation" in event_types

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "pipeline" and item["status"] == "completed" for item in heartbeats)


def test_pipeline_job_modules_run_in_sequence(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr(
            "app.pipeline.market_data_job.fetch_klines",
            lambda: [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])],
        )
        monkeypatch.setattr(
            "app.pipeline.strategy_job.generate_registered_signal",
            lambda conn, strategy_name="ma_cross": {
                "id": 1,
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "strategy_name": strategy_name,
                "signal_type": "BUY",
                "short_ma": 13.0,
                "long_ma": 12.0,
            },
        )
        monkeypatch.setattr(
            "app.pipeline.strategy_job.evaluate_latest_signal",
            lambda conn: {
                "id": 1,
                "signal_id": 1,
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "strategy_name": "ma_cross",
                "signal_type": "BUY",
                "decision": "APPROVED",
                "reason": "Passed basic risk checks.",
            },
        )

        market_result = run_market_data_job(connection)
        strategy_result = run_strategy_job(connection)
        execution_result = run_execution_job(connection)

        assert market_result == {"step": "save_klines", "saved_klines": 5}
        assert [step["step"] for step in strategy_result["steps"]] == ["generate_signal", "evaluate_risk"]
        assert [step["step"] for step in execution_result["steps"]] == ["paper_execute", "update_positions", "update_pnl"]
    finally:
        connection.close()


def test_run_strategy_job_uses_registry_strategy_name(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr("app.pipeline.strategy_job.ensure_signals_table", lambda conn: None)
        monkeypatch.setattr("app.pipeline.strategy_job.ensure_positions_table", lambda conn: None)
        monkeypatch.setattr("app.pipeline.strategy_job.ensure_risk_table", lambda conn: None)
        monkeypatch.setattr(
            "app.pipeline.strategy_job.generate_registered_signal",
            lambda conn, strategy_name="ma_cross": {
                "id": 11,
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "strategy_name": strategy_name,
                "signal_type": "BUY",
                "short_ma": 4.0,
                "long_ma": 3.0,
            },
        )
        monkeypatch.setattr(
            "app.pipeline.strategy_job.evaluate_latest_signal",
            lambda conn: {
                "id": 22,
                "signal_id": 11,
                "symbol": "BTCUSDT",
                "timeframe": "1m",
                "strategy_name": "ma_cross",
                "signal_type": "BUY",
                "decision": "APPROVED",
                "reason": "Passed basic risk checks.",
            },
        )

        result = run_strategy_job(connection, strategy_name="ma_cross")

        assert result["status"] == "ok"
        assert result["steps"][0]["strategy_name"] == "ma_cross"
        assert result["steps"][1]["strategy_name"] == "ma_cross"
    finally:
        connection.close()


def test_run_strategy_jobs_runs_multiple_registered_strategies(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr(
            "app.pipeline.strategy_job.run_strategy_job",
            lambda conn, strategy_name="ma_cross": {
                "status": "ok",
                "steps": [
                    {"step": "generate_signal", "strategy_name": strategy_name, "signal_type": "BUY"},
                    {"step": "evaluate_risk", "strategy_name": strategy_name, "decision": "APPROVED"},
                ],
            },
        )

        result = run_strategy_jobs(connection, ["ma_cross", "momentum_3bar"])

        assert result["status"] == "ok"
        assert result["strategy_names"] == ["ma_cross", "momentum_3bar"]
        assert [step["strategy_name"] for step in result["steps"] if step["step"] == "generate_signal"] == [
            "ma_cross",
            "momentum_3bar",
        ]
    finally:
        connection.close()


def test_job_scripts_call_backend_aware_job_modules(monkeypatch) -> None:
    outputs: list[str] = []

    class DummyConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr("scripts.run_market_data_job.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("scripts.run_market_data_job.run_migrations", lambda connection: None)
    monkeypatch.setattr("scripts.run_market_data_job.run_market_data_job", lambda connection: {"step": "save_klines", "saved_klines": 5})

    monkeypatch.setattr("scripts.run_strategy_job.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("scripts.run_strategy_job.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "scripts.run_strategy_job.parse_args",
        lambda: SimpleNamespace(strategy="momentum_3bar"),
    )
    monkeypatch.setattr(
        "scripts.run_strategy_job.run_strategy_job",
        lambda connection, strategy_name="ma_cross": {
            "status": "ok",
            "steps": [{"step": "generate_signal", "strategy_name": strategy_name}],
        },
    )

    monkeypatch.setattr("scripts.run_execution_job.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("scripts.run_execution_job.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "scripts.run_execution_job.run_execution_job",
        lambda connection: {"status": "ok", "steps": [{"step": "paper_execute"}]},
    )

    from scripts.run_market_data_job import main as market_main
    from scripts.run_strategy_job import main as strategy_main
    from scripts.run_execution_job import main as execution_main

    for entrypoint in (market_main, strategy_main, execution_main):
        buffer = StringIO()
        with contextlib.redirect_stdout(buffer):
            entrypoint()
        outputs.append(buffer.getvalue())

    assert '"saved_klines": 5' in outputs[0]
    assert '"strategy_name": "momentum_3bar"' in outputs[1]
    assert '"paper_execute"' in outputs[2]


def test_run_scheduler_records_soak_snapshot(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "scheduler.log"
    db_path = tmp_path / "scheduler-heartbeat.db"
    recorded = []
    monkeypatch.setattr("app.scheduler.runner.LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.scheduler.runner.run_pipeline_collect",
        lambda strategy_name="ma_cross": {
            "steps": [
                {"step": "generate_signal", "signal_type": "BUY"},
                {"step": "evaluate_risk", "decision": "APPROVED"},
                {"step": "paper_execute", "status": "FILLED", "side": "BUY"},
            ]
        },
    )
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1)

    assert recorded == [{"status": "ok"}]
    log_text = log_path.read_text(encoding="utf-8")
    assert "run=1 mode=pipeline strategy=ma_cross signal=BUY risk=APPROVED execution=FILLED BUY" in log_text
    assert "soak_snapshot status=ok" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "scheduler" and item["status"] == "ok" for item in heartbeats)


def test_run_scheduler_supports_strategy_only_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "strategy-worker.log"
    db_path = tmp_path / "scheduler-strategy-heartbeat.db"
    recorded = []

    monkeypatch.setattr("app.scheduler.runner.STRATEGY_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ma_cross", "momentum_3bar"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.scheduler.runner.run_strategy_jobs",
        lambda connection, strategy_names=None: {
            "status": "ok",
            "strategy_names": strategy_names or ["ma_cross"],
            "steps": [
                {"step": "generate_signal", "signal_type": "BUY", "strategy_name": "ma_cross"},
                {"step": "evaluate_risk", "decision": "APPROVED", "strategy_name": "ma_cross"},
                {"step": "generate_signal", "signal_type": "SELL", "strategy_name": "momentum_3bar"},
                {"step": "evaluate_risk", "decision": "REJECTED", "strategy_name": "momentum_3bar"},
            ],
        },
    )
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="strategy-only", strategy_name="momentum_3bar")

    assert recorded == [{"status": "ok"}]
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=strategy-only" in log_text
    assert "strategies=ma_cross,momentum_3bar" in log_text
    assert "signal=SELL" in log_text
    assert "risk=REJECTED" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "strategy_worker" and item["status"] == "ok" for item in heartbeats)


def test_read_effective_active_strategies_respects_priority_and_disabled(monkeypatch, tmp_path) -> None:
    strategy_file = tmp_path / "scheduler.strategy"
    disabled_file = tmp_path / "scheduler.strategy.disabled"
    priority_file = tmp_path / "scheduler.strategy.priority.json"
    limit_file = tmp_path / "scheduler.strategy.limit"

    strategy_file.write_text("momentum_3bar\nma_cross\n", encoding="utf-8")
    disabled_file.write_text("", encoding="utf-8")
    priority_file.write_text(json.dumps({"ma_cross": 1, "momentum_3bar": 5}), encoding="utf-8")
    limit_file.write_text("1\n", encoding="utf-8")

    monkeypatch.setattr("app.scheduler.control.STRATEGY_FILE", strategy_file)
    monkeypatch.setattr("app.scheduler.control.DISABLED_STRATEGY_FILE", disabled_file)
    monkeypatch.setattr("app.scheduler.control.PRIORITY_FILE", priority_file)
    monkeypatch.setattr("app.scheduler.control.EFFECTIVE_LIMIT_FILE", limit_file)

    assert read_effective_active_strategies() == ["ma_cross"]


def test_run_scheduler_skips_when_no_enabled_active_strategies(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "strategy-worker.log"
    db_path = tmp_path / "scheduler-empty-strategy-heartbeat.db"
    recorded = []

    monkeypatch.setattr("app.scheduler.runner.STRATEGY_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: [])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="strategy-only")

    assert recorded == [{"status": "ok"}]
    log_text = log_path.read_text(encoding="utf-8")
    assert "skipped=no-enabled-active-strategies" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(
        item["component"] == "strategy_worker"
        and item["status"] == "ok"
        and "no enabled active strategies" in item["message"]
        for item in heartbeats
    )


def test_read_scheduler_log_aggregates_split_worker_logs(monkeypatch, tmp_path) -> None:
    pipeline_log = tmp_path / "scheduler.log"
    data_log = tmp_path / "data-worker.log"
    strategy_log = tmp_path / "strategy-worker.log"

    pipeline_log.write_text("[2026-03-19T10:00:01] run=1 mode=pipeline signal=BUY\n", encoding="utf-8")
    data_log.write_text("[2026-03-19T10:00:02] run=1 mode=market-data-only signal=n/a\n", encoding="utf-8")
    strategy_log.write_text("[2026-03-19T10:00:03] run=1 mode=strategy-only signal=BUY\n", encoding="utf-8")

    monkeypatch.setattr(
        "app.scheduler.control.get_scheduler_log_files",
        lambda: {
            "pipeline": pipeline_log,
            "market-data-only": data_log,
            "strategy-only": strategy_log,
            "execution-only": tmp_path / "execution-worker.log",
        },
    )

    from app.scheduler.control import read_scheduler_log

    lines = read_scheduler_log(lines=2, mode="all")

    assert lines == [
        "[2026-03-19T10:00:02] run=1 mode=market-data-only signal=n/a",
        "[2026-03-19T10:00:03] run=1 mode=strategy-only signal=BUY",
    ]


def test_build_soak_validation_report_summarizes_runtime_state(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "soak.db"
    log_path = tmp_path / "scheduler.log"
    log_path.write_text(
        "\n".join(
            [
                "[2026-03-18T10:00:00] run=1 signal=BUY risk=APPROVED execution=FILLED BUY",
                "[2026-03-18T10:01:00] run=2 signal=SELL risk=REJECTED execution=REJECTED",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    connection = sqlite3.connect(db_path)
    try:
        ensure_candles_table(connection)
        ensure_signals_table(connection)
        ensure_risk_table(connection)
        ensure_execution_tables(connection)
        ensure_positions_table(connection)
        ensure_pnl_table(connection)
        seed_candles(connection, [10, 11, 12, 13, 14])
        insert_signal(connection, "BUY", strategy_name="manual_test")
        evaluate_latest_signal(connection, cooldown_seconds=0)
        execute_latest_risk(connection)
        update_positions(connection)
        update_pnl_snapshots(connection)
        upsert_heartbeat(connection, "scheduler", "ok", "Scheduler loop completed.")
    finally:
        connection.close()

    monkeypatch.setattr("app.validation.soak_report.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.validation.soak_report.read_scheduler_log",
        lambda lines=200: log_path.read_text(encoding="utf-8").splitlines()[-lines:],
    )

    report = build_soak_validation_report()

    assert report["status"] == "ok"
    assert report["scheduler"]["line_count"] == 2
    assert report["scheduler"]["recent_error_count"] == 0
    assert report["table_counts"]["candles"] == 5
    assert report["table_counts"]["signals"] == 1
    assert report["table_counts"]["orders"] == 1
    assert report["positions"]["open_symbols"] == 1
    assert any(item["component"] == "scheduler" for item in report["heartbeats"])


def test_build_soak_validation_report_marks_missing_runtime_activity_as_degraded(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "empty-soak.db"
    connection = sqlite3.connect(db_path)
    try:
        ensure_candles_table(connection)
        ensure_signals_table(connection)
    finally:
        connection.close()

    monkeypatch.setattr("app.validation.soak_report.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.validation.soak_report.read_scheduler_log", lambda lines=200: [])

    report = build_soak_validation_report()

    assert report["status"] == "degraded"
    assert "Scheduler log is empty." in report["issues"]
    assert "No candles stored." in report["issues"]
    assert "No signals generated." in report["issues"]


def test_build_soak_validation_report_marks_scheduler_stop_flag_as_degraded(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stopped-soak.db"
    log_path = tmp_path / "scheduler.log"
    log_path.write_text(
        "[2026-03-18T16:08:53] scheduler stopped by flag: runtime/scheduler.stop\n",
        encoding="utf-8",
    )

    connection = sqlite3.connect(db_path)
    try:
        ensure_candles_table(connection)
        ensure_signals_table(connection)
        seed_candles(connection, [10, 11, 12, 13, 14])
        insert_signal(connection, "BUY", strategy_name="manual_test")
    finally:
        connection.close()

    monkeypatch.setattr("app.validation.soak_report.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.validation.soak_report.read_scheduler_log",
        lambda lines=200: log_path.read_text(encoding="utf-8").splitlines()[-lines:],
    )

    report = build_soak_validation_report()

    assert report["status"] == "degraded"
    assert report["scheduler"]["stopped_by_flag"] is True
    assert "Scheduler is stopped by flag." in report["issues"]


def test_build_soak_validation_report_marks_stale_activity_as_degraded(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "stale-soak.db"
    log_path = tmp_path / "scheduler.log"
    log_path.write_text(
        "[2026-03-18T16:08:53] run=1 signal=BUY risk=APPROVED execution=FILLED BUY\n",
        encoding="utf-8",
    )

    connection = sqlite3.connect(db_path)
    try:
        ensure_candles_table(connection)
        ensure_signals_table(connection)
        ensure_risk_table(connection)
        ensure_execution_tables(connection)
        ensure_positions_table(connection)
        ensure_pnl_table(connection)
        seed_candles(connection, [10, 11, 12, 13, 14])
        connection.execute(
            """
            INSERT INTO signals (
                symbol, timeframe, strategy_name, signal_type, short_ma, long_ma, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            ("BTCUSDT", "1m", "manual_test", "BUY", 10.0, 9.0, "2026-03-18 00:00:00"),
        )
        connection.execute(
            """
            INSERT INTO risk_events (
                signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (1, "BTCUSDT", "1m", "manual_test", "BUY", "APPROVED", "ok", "2026-03-18 00:00:00"),
        )
        connection.execute(
            """
            INSERT INTO pnl_snapshots (
                symbol, qty, avg_price, market_price, unrealized_pnl, created_at
            ) VALUES (?, ?, ?, ?, ?, ?);
            """,
            ("BTCUSDT", 0.0, 0.0, 0.0, 0.0, "2026-03-18 00:00:00"),
        )
        connection.commit()
    finally:
        connection.close()

    class FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 3, 18, 1, 0, 0, tzinfo=timezone.utc)

    monkeypatch.setattr("app.validation.soak_report.datetime", FrozenDateTime)
    monkeypatch.setattr("app.validation.soak_report.SOAK_ACTIVITY_STALENESS_SECONDS", 60)
    monkeypatch.setattr("app.validation.soak_report.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.validation.soak_report.read_scheduler_log",
        lambda lines=200: log_path.read_text(encoding="utf-8").splitlines()[-lines:],
    )

    report = build_soak_validation_report()

    assert report["status"] == "degraded"
    assert report["latest_activity"]["signals"]["age_seconds"] == 3600
    assert any("signals activity is stale" in issue for issue in report["issues"])


def test_record_soak_validation_snapshot_persists_history(monkeypatch, tmp_path) -> None:
    history_file = tmp_path / "soak_history.jsonl"
    monkeypatch.setattr(
        "app.validation.soak_history.SOAK_HISTORY_FILE",
        history_file,
    )
    monkeypatch.setattr(
        "app.validation.soak_history.build_soak_validation_report",
        lambda: {"status": "ok", "checked_at": "2026-03-18T10:00:00+00:00", "issues": []},
    )

    snapshot = record_soak_validation_snapshot()
    history = read_soak_validation_history(limit=5)

    assert snapshot["status"] == "ok"
    assert history_file.exists()
    assert len(history) == 1
    assert history[0]["checked_at"] == "2026-03-18T10:00:00+00:00"


def test_soak_validation_endpoints_return_report_and_history(monkeypatch, tmp_path) -> None:
    history_file = tmp_path / "soak_history.jsonl"
    monkeypatch.setattr("app.validation.soak_history.SOAK_HISTORY_FILE", history_file)
    monkeypatch.setattr(
        "app.api.main.build_soak_validation_report",
        lambda: {"status": "ok", "checked_at": "2026-03-18T10:00:00+00:00", "issues": []},
    )
    monkeypatch.setattr(
        "app.validation.soak_history.build_soak_validation_report",
        lambda: {"status": "ok", "checked_at": "2026-03-18T10:00:00+00:00", "issues": []},
    )

    client = TestClient(app)

    report_response = client.get("/validation/soak")
    assert report_response.status_code == 200
    assert report_response.json()["status"] == "ok"

    record_response = client.post("/validation/soak/record")
    assert record_response.status_code == 200
    assert record_response.json()["checked_at"] == "2026-03-18T10:00:00+00:00"

    history_response = client.get("/validation/soak/history?limit=5")
    assert history_response.status_code == 200
    history = history_response.json()
    assert len(history) == 1
    assert history[0]["status"] == "ok"
