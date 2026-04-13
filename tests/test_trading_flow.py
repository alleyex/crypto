import json
import pytest
import sqlite3
import urllib.error
import numpy as np
from io import StringIO
from decimal import Decimal
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import contextlib
import os

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
from app.core.job_queue import complete_job
from app.core.job_queue import enqueue_pipeline_jobs
from app.core.job_queue import enqueue_job
from app.core.job_queue import lease_job_by_id
from app.core.job_queue import fail_batch_jobs
from app.core.job_queue import fail_job
from app.core.job_queue import get_job
from app.core.job_queue import lease_next_job
from app.core.job_queue import list_jobs
from app.core.job_queue import reclaim_stale_leased_jobs
from app.core.job_queue import retry_job
from app.core.job_queue import run_pipeline_batch
from app.core.job_queue import run_next_pipeline_batch
from app.core.job_runner import run_next_queued_job
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
from scripts.run_postgres_compose_validation import validate_compose_runtime
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
from app.data import fetch_history as fetch_history_module
from app.data.candles_service import save_klines
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_pending_approved_risks
from app.execution.paper_broker import execute_latest_risk
from app.execution.adapter import get_execution_backend_status
from app.execution.adapter import NoopExecutionAdapter
from app.execution.adapter import SimulatedLiveExecutionAdapter
from app.execution.adapter import get_execution_adapter_name
from app.execution.binance_broker import BinanceAPIError
from app.execution.live_broker import SimulatedBrokerClient
from app.execution import live_broker
from app.execution.runtime import get_execution_backend_runtime_status
from app.execution.runtime import set_execution_backend
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.strategy_job import run_strategy_job
from app.pipeline.strategy_job import run_strategy_jobs
from app.pipeline.run_pipeline import print_pipeline_result
from app.pipeline.run_pipeline import run_pipeline_collect
from app.portfolio.daily_pnl_service import get_daily_realized_pnl
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_fills
from app.query.read_service import get_audit_events
from app.query.read_service import get_job_queue_jobs
from app.query.read_service import get_job_queue_summary
from app.query.read_service import get_orders
from app.query.read_service import get_pnl_snapshots
from app.query.read_service import get_positions
from app.query.read_service import get_risk_events
from app.query.read_service import get_signals
from app.query.read_service import get_execution_report
from app.query.read_service import get_strategy_activity_summary
from app.query.read_service import get_strategy_closed_trades
from app.risk.risk_service import evaluate_signal_id
from app.risk.risk_service import evaluate_latest_signal
from app.strategy.signal_service import insert_signal
from app.strategy.ppo_strategy import _build_observation
from app.strategy import ppo_strategy
from app.strategy.registry import generate_registered_signal
from app.strategy.registry import get_strategy
from app.strategy.registry import list_registered_strategies
from app.system.kill_switch import disable_kill_switch
from app.system.kill_switch import enable_kill_switch
from app.scheduler.runner import run_scheduler
from app.scheduler.control import read_effective_active_strategies
from app.system.heartbeat import get_heartbeats
from app.system.heartbeat import upsert_heartbeat
from app.validation.soak_history import build_soak_history_summary
from app.validation.soak_history import read_soak_validation_history
from app.validation.soak_history import record_soak_validation_snapshot
from app.validation.soak_report import build_soak_validation_report
from app.validation.soak_report import _signal_quality_check
from conftest import make_connection, make_kline


def seed_candles(connection: sqlite3.Connection, closes: list[float]) -> None:
    run_migrations(connection)
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



def test_ppo_build_observation_handles_decimal_candle_rows() -> None:
    class _Cursor:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class _Connection:
        def __init__(self, rows):
            self._rows = rows

        def execute(self, *_args, **_kwargs):
            return _Cursor(self._rows)

    rows = []
    base_open_time = 60_000
    for index in range(170):
        close = Decimal("100") + Decimal(index) / Decimal("10")
        rows.append(
            (
                base_open_time * (index + 1),
                close - Decimal("0.5"),
                close + Decimal("1.0"),
                close - Decimal("1.0"),
                close,
                Decimal("100"),
                Decimal("1000"),
                Decimal("10"),
                Decimal("50"),
                Decimal("500"),
            )
        )

    result = _build_observation(
        _Connection(list(reversed(rows))),
        symbol="BTCUSDT",
        timeframe="5m",
        state={"position": 0, "entry_price": None, "bars_held": 0},
    )

    assert result is not None
    obs, current_close = result
    assert obs.dtype == "float32"
    assert isinstance(current_close, float)


def test_ppo_generate_signal_supports_two_action_long_flat_model(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, 100 + index) for index in range(170)],
        )

        class _FakeDistribution:
            def __init__(self):
                self.distribution = SimpleNamespace(
                    probs=SimpleNamespace(
                        squeeze=lambda: SimpleNamespace(
                            cpu=lambda: SimpleNamespace(
                                numpy=lambda: np.array([0.2, 0.8], dtype=float)
                            )
                        )
                    )
                )

        class _FakePolicy:
            @staticmethod
            def obs_to_tensor(obs):
                return (obs, None)

            @staticmethod
            def get_distribution(_obs_tensor):
                return _FakeDistribution()

        class _FakeModel:
            policy = _FakePolicy()

            @staticmethod
            def predict(_obs, deterministic=True):
                return 1, None

        monkeypatch.setattr(ppo_strategy, "_load_model", lambda *_args, **_kwargs: _FakeModel())
        monkeypatch.setattr(
            ppo_strategy,
            "_load_state",
            lambda *_args, **_kwargs: {"position": 0, "entry_price": None, "bars_held": 0},
        )
        monkeypatch.setattr(ppo_strategy, "_save_state", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(
            ppo_strategy, "_build_observation",
            lambda *_args, **_kwargs: (np.zeros(20, dtype=float), 100.0),
        )
        captured: list[dict] = []
        monkeypatch.setattr(
            ppo_strategy,
            "insert_event",
            lambda conn, event_type, status, source, message, payload=None: captured.append(
                {"event_type": event_type, "status": status, "source": source, "payload": payload}
            ) or 1,
        )

        result = ppo_strategy.generate_signal(connection, symbol="BTCUSDT", timeframe="1m")

        assert result is not None
        assert result["signal_type"] == "BUY"
        assert result["position"] == 1
        assert result["prob_buy"] == 0.8
        assert result["prob_sell"] == 0.0
        assert result["current_position"] == 0
        assert result["target_position"] == 1
        assert result["blocked_reason"] is None
        assert captured[0]["event_type"] == "ppo_inference"
        assert captured[0]["payload"]["current_position"] == 0
        assert captured[0]["payload"]["target_position"] == 1
        assert captured[0]["payload"]["blocked_reason"] is None
    finally:
        connection.close()


def test_ppo_generate_signal_logs_hold_reason_when_target_matches_current_position(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, 100 + index) for index in range(170)],
        )

        class _FakeDistribution:
            def __init__(self):
                self.distribution = SimpleNamespace(
                    probs=SimpleNamespace(
                        squeeze=lambda: SimpleNamespace(
                            cpu=lambda: SimpleNamespace(
                                numpy=lambda: np.array([0.01, 0.98, 0.01], dtype=float)
                            )
                        )
                    )
                )

        class _FakePolicy:
            @staticmethod
            def obs_to_tensor(obs):
                return (obs, None)

            @staticmethod
            def get_distribution(_obs_tensor):
                return _FakeDistribution()

        class _FakeModel:
            policy = _FakePolicy()

            @staticmethod
            def predict(_obs, deterministic=True):
                return 1, None

        monkeypatch.setattr(ppo_strategy, "_load_model", lambda *_args, **_kwargs: _FakeModel())
        monkeypatch.setattr(
            ppo_strategy,
            "_load_state",
            lambda *_args, **_kwargs: {"position": 1, "entry_price": 100.0, "bars_held": 3},
        )
        monkeypatch.setattr(ppo_strategy, "_get_db_position", lambda *_args, **_kwargs: 1)
        monkeypatch.setattr(ppo_strategy, "_save_state", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(
            ppo_strategy, "_build_observation",
            lambda *_args, **_kwargs: (np.zeros(24, dtype=float), 101.0),
        )
        captured: list[dict] = []
        monkeypatch.setattr(
            ppo_strategy,
            "insert_event",
            lambda conn, event_type, status, source, message, payload=None: captured.append(
                {"event_type": event_type, "status": status, "source": source, "payload": payload}
            ) or 1,
        )

        result = ppo_strategy.generate_signal(connection, symbol="BTCUSDT", timeframe="1m")

        assert result is not None
        assert result["signal_type"] == "HOLD"
        assert result["current_position"] == 1
        assert result["target_position"] == 1
        assert result["blocked_reason"] == "same_position"
        assert captured[0]["event_type"] == "ppo_inference"
        assert captured[0]["status"] == "hold"
        assert captured[0]["payload"]["blocked_reason"] == "same_position"
    finally:
        connection.close()


def test_ppo_generate_signal_applies_stop_loss_override(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, 100 + index) for index in range(170)],
        )

        class _FakeDistribution:
            def __init__(self):
                self.distribution = SimpleNamespace(
                    probs=SimpleNamespace(
                        squeeze=lambda: SimpleNamespace(
                            cpu=lambda: SimpleNamespace(
                                numpy=lambda: np.array([0.01, 0.98, 0.01], dtype=float)
                            )
                        )
                    )
                )

        class _FakePolicy:
            @staticmethod
            def obs_to_tensor(obs):
                return (obs, None)

            @staticmethod
            def get_distribution(_obs_tensor):
                return _FakeDistribution()

        class _FakeModel:
            policy = _FakePolicy()

            @staticmethod
            def predict(_obs, deterministic=True):
                return 1, None

        monkeypatch.setattr(ppo_strategy, "_load_model", lambda *_args, **_kwargs: _FakeModel())
        monkeypatch.setattr(
            ppo_strategy,
            "_load_state",
            lambda *_args, **_kwargs: {"position": 1, "entry_price": 100.0, "bars_held": 3},
        )
        monkeypatch.setattr(ppo_strategy, "_get_db_position", lambda *_args, **_kwargs: 1)
        monkeypatch.setattr(ppo_strategy, "_save_state", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(
            ppo_strategy, "_build_observation",
            lambda *_args, **_kwargs: (np.zeros(24, dtype=float), 98.5),
        )
        monkeypatch.setattr(
            ppo_strategy,
            "get_risk_config",
            lambda *_args, **_kwargs: (SimpleNamespace(stop_loss_pct=0.01), False),
        )
        captured: list[dict] = []
        monkeypatch.setattr(
            ppo_strategy,
            "insert_event",
            lambda conn, event_type, status, source, message, payload=None: captured.append(
                {"event_type": event_type, "status": status, "source": source, "payload": payload}
            ) or 1,
        )

        result = ppo_strategy.generate_signal(connection, symbol="BTCUSDT", timeframe="1m")

        assert result is not None
        assert result["signal_type"] == "SELL"
        assert result["current_position"] == 1
        assert result["model_target_position"] == 1
        assert result["target_position"] == 0
        assert result["risk_override_reason"] == "stop_loss"
        assert captured[0]["payload"]["risk_override_reason"] == "stop_loss"
        assert captured[0]["payload"]["stop_loss_pct"] == 0.01
    finally:
        connection.close()


def test_get_strategy_activity_summary_groups_latest_records_by_strategy() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
        ensure_execution_tables(connection)

        save_klines(connection, [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])])
        insert_signal(connection, "BUY", strategy_name="ppo")
        evaluate_latest_signal(connection)
        execution_result = execute_latest_risk(connection)
        assert execution_result is not None
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ma-cross-sell-1", 999, "BTCUSDT", "1m", "ppo", "SELL", 0.001, 12.0, "FILLED"),
        )
        ppo_sell_order_id = int(
            connection.execute(
                "SELECT id FROM orders WHERE client_order_id = 'ma-cross-sell-1';"
            ).fetchone()[0]
        )
        insert_fill(connection, ppo_sell_order_id, "BTCUSDT", "SELL", 0.001, 12.0, "2026-03-19 10:05:00")

        save_klines(connection, [make_kline((index + 10) * 60_000, close) for index, close in enumerate([20, 21, 22, 24])])
        insert_signal(connection, "BUY", strategy_name="ppo")
        evaluate_latest_signal(connection, cooldown_seconds=0)

        summary = get_strategy_activity_summary(connection)

        by_name = {item["strategy_name"]: item for item in summary}
        assert by_name["ppo"]["latest_signal"] is not None
        assert by_name["ppo"]["latest_risk"] is not None
        assert by_name["ppo"]["latest_order"] is not None
        assert by_name["ppo"]["latest_fill"] is not None
        assert by_name["ppo"]["latest_closed_trade"] is not None
        assert by_name["ppo"]["latest_closed_trade"]["symbol"] == "BTCUSDT"
        assert by_name["ppo"]["latest_closed_trade"]["status"] == "loss"
        assert by_name["ppo"]["latest_closed_trade"]["closed_at"] == "2026-03-19 10:05:00"
        assert by_name["ppo"]["latest_closed_trade"]["realized_pnl"] == -0.002
        assert by_name["ppo"]["latest_activity_at"] >= by_name["ppo"]["latest_fill_at"]
        assert by_name["ppo"]["latest_order_at"] is not None
        assert by_name["ppo"]["latest_fill_at"] == "2026-03-19 10:05:00"
        assert by_name["ppo"]["filled_order_count"] == 2
        assert by_name["ppo"]["filled_qty_total"] == 0.002
        assert by_name["ppo"]["gross_realized_pnl"] == -0.002
        assert by_name["ppo"]["buy_fill_count"] == 1
        assert by_name["ppo"]["sell_fill_count"] == 1
        assert by_name["ppo"]["realized_trade_count"] == 1
        assert by_name["ppo"]["winning_trade_count"] == 0
        assert by_name["ppo"]["losing_trade_count"] == 1
        assert by_name["ppo"]["breakeven_trade_count"] == 0
        assert by_name["ppo"]["net_position_qty"] == 0.0
    finally:
        connection.close()


def test_get_strategy_activity_summary_treats_filled_orders_with_new_status_as_executed() -> None:
    connection = make_connection()
    try:
        ensure_execution_tables(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ppo-buy-new", 1, "SOLUSDT", "1m", "ppo", "BUY", 1.0, 78.87, "NEW", "2026-04-07 14:35:06"),
        )
        order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'ppo-buy-new';").fetchone()[0])
        insert_fill(connection, order_id, "SOLUSDT", "BUY", 1.0, 78.87, "2026-04-07 14:35:07")

        summary = get_strategy_activity_summary(connection)

        by_name = {item["strategy_name"]: item for item in summary}
        assert by_name["ppo"]["latest_fill"] is not None
        assert by_name["ppo"]["filled_order_count"] == 1
        assert by_name["ppo"]["filled_qty_total"] == 1.0
        assert by_name["ppo"]["buy_fill_count"] == 1
        assert by_name["ppo"]["sell_fill_count"] == 0
        assert by_name["ppo"]["net_position_qty"] == 1.0
        assert by_name["ppo"]["open_position_symbol"] == "SOLUSDT"
        assert by_name["ppo"]["open_entry_price"] == 78.87
        assert by_name["ppo"]["open_position_opened_at"] == "2026-04-07 14:35:07"
    finally:
        connection.close()


def test_get_strategy_activity_summary_enriches_bid_ask_when_requested(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        save_klines(connection, [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])])
        insert_signal(connection, "BUY", strategy_name="ppo")

        monkeypatch.setattr(
            "app.query.activity_summary.fetch_book_ticker",
            lambda symbol: {
                "symbol": symbol,
                "bid_price": 70850.1,
                "bid_qty": 1.2,
                "ask_price": 70850.9,
                "ask_qty": 0.8,
            },
        )

        summary = get_strategy_activity_summary(connection, include_live_book=True)

        by_name = {item["strategy_name"]: item for item in summary}
        assert by_name["ppo"]["bid_price"] == pytest.approx(70850.1)
        assert by_name["ppo"]["ask_price"] == pytest.approx(70850.9)
        assert by_name["ppo"]["current_price"] == pytest.approx(70850.5)
    finally:
        connection.close()


def test_get_execution_report_summarizes_trades_and_failed_jobs() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, broker_name, broker_order_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES
                ('buy-1', 1, 'binance', 'ext-1', 'BTCUSDT', '1m', 'ppo', 'BUY', 0.001, 70000.0, 'FILLED', '2026-03-25 00:00:00'),
                ('sell-1', 2, 'binance', 'ext-2', 'BTCUSDT', '1m', 'ppo', 'SELL', 0.001, 70100.0, 'FILLED', '2026-03-25 00:05:00');
            """
        )
        connection.execute(
            """
            INSERT INTO fills (
                order_id, symbol, side, qty, price, commission, commission_asset, quote_qty, transact_time, created_at
            ) VALUES
                (1, 'BTCUSDT', 'BUY', 0.001, 70000.0, 0.028, 'USDT', 70.0, 1774500000000, '2026-03-25 00:00:00'),
                (2, 'BTCUSDT', 'SELL', 0.001, 70100.0, 0.02804, 'USDT', 70.1, 1774500300000, '2026-03-25 00:05:00');
            """
        )
        connection.execute(
            """
            INSERT INTO positions (symbol, qty, avg_price, realized_pnl, updated_at)
            VALUES ('BTCUSDT', 0.0, 0.0, 0.1, '2026-03-25 00:05:00');
            """
        )
        connection.execute(
            """
            INSERT INTO job_queue (
                job_type, status, payload_json, result_json, error_message, attempt_count, created_at, completed_at
            ) VALUES (
                'execution',
                'failed',
                '{"symbol_names":["BTCUSDT"]}',
                '{"error_detail":{"status_code":400,"binance_code":-2010,"binance_msg":"insufficient balance"}}',
                'execution failed',
                1,
                '2026-03-25 00:06:00',
                '2026-03-25 00:06:01'
            );
            """
        )
        connection.commit()

        report = get_execution_report(connection, symbol="BTCUSDT", strategy_name="ppo", days=30, limit=10)

        assert report["summary"]["fills"] == 2
        assert report["summary"]["closed_trades"] == 1
        assert report["summary"]["gross_pnl"] == pytest.approx(0.1)
        assert report["summary"]["fees"] == pytest.approx(0.05604)
        assert report["summary"]["net_pnl"] == pytest.approx(0.04396)
        assert len(report["recent_failed_execution_jobs"]) == 1
        assert report["recent_failed_execution_jobs"][0]["result"]["error_detail"]["binance_code"] == -2010
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
            ("buy-1", 1, "BTCUSDT", "1m", "ppo", "BUY", 1.0, 100.0, "FILLED", "2026-03-19 10:00:00"),
        )
        buy_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-1';").fetchone()[0])
        insert_fill(connection, buy_order_id, "BTCUSDT", "BUY", 1.0, 100.0, "2026-03-19 10:00:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-1", 2, "BTCUSDT", "1m", "ppo", "SELL", 1.0, 110.0, "FILLED", "2026-03-19 10:05:00"),
        )
        sell_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'sell-1';").fetchone()[0])
        insert_fill(connection, sell_order_id, "BTCUSDT", "SELL", 1.0, 110.0, "2026-03-19 10:05:00")

        closed_trades = get_strategy_closed_trades(connection)

        assert len(closed_trades) == 1
        assert closed_trades[0]["strategy_name"] == "ppo"
        assert closed_trades[0]["realized_pnl"] == 10.0
        assert closed_trades[0]["status"] == "win"
    finally:
        connection.close()


def test_get_strategy_closed_trades_uses_fill_backed_orders_even_when_status_is_new() -> None:
    connection = make_connection()
    try:
        ensure_execution_tables(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ppo-buy-new", 1, "SOLUSDT", "1m", "ppo", "BUY", 1.0, 78.87, "NEW", "2026-04-07 14:35:06"),
        )
        buy_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'ppo-buy-new';").fetchone()[0])
        insert_fill(connection, buy_order_id, "SOLUSDT", "BUY", 1.0, 78.87, "2026-04-07 14:35:07")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ppo-sell-new", 2, "SOLUSDT", "1m", "ppo", "SELL", 1.0, 79.28, "NEW", "2026-04-07 15:05:06"),
        )
        sell_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'ppo-sell-new';").fetchone()[0])
        insert_fill(connection, sell_order_id, "SOLUSDT", "SELL", 1.0, 79.28, "2026-04-07 15:05:07")

        closed_trades = get_strategy_closed_trades(connection, strategy_name="ppo")

        assert len(closed_trades) == 1
        assert closed_trades[0]["strategy_name"] == "ppo"
        assert closed_trades[0]["symbol"] == "SOLUSDT"
        assert closed_trades[0]["entry_price"] == 78.87
        assert closed_trades[0]["exit_price"] == 79.28
        assert closed_trades[0]["realized_pnl"] == pytest.approx(0.41)
    finally:
        connection.close()


def test_get_strategy_closed_trades_supports_short_round_trip() -> None:
    connection = make_connection()
    try:
        ensure_execution_tables(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ppo-short-open", 1, "SOLUSDT", "1m", "ppo", "SELL", 1.0, 82.27, "FILLED", "2026-04-09 02:30:00"),
        )
        short_open_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'ppo-short-open';").fetchone()[0])
        insert_fill(connection, short_open_order_id, "SOLUSDT", "SELL", 1.0, 82.27, "2026-04-09 02:30:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ppo-short-close", 2, "SOLUSDT", "1m", "ppo", "BUY", 1.0, 83.00681, "FILLED", "2026-04-09 02:34:44"),
        )
        short_close_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'ppo-short-close';").fetchone()[0])
        insert_fill(connection, short_close_order_id, "SOLUSDT", "BUY", 1.0, 83.00681, "2026-04-09 02:34:44")

        closed_trades = get_strategy_closed_trades(connection, strategy_name="ppo")

        assert len(closed_trades) == 1
        assert closed_trades[0]["symbol"] == "SOLUSDT"
        assert closed_trades[0]["entry_price"] == pytest.approx(82.27)
        assert closed_trades[0]["exit_price"] == pytest.approx(83.00681)
        assert closed_trades[0]["realized_pnl"] == pytest.approx(-0.73681)
        assert closed_trades[0]["status"] == "loss"
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
            ("buy-ma", 1, "BTCUSDT", "1m", "other_strategy", "BUY", 1.0, 100.0, "FILLED", "2026-03-19 10:00:00"),
        )
        buy_ma_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-ma';").fetchone()[0])
        insert_fill(connection, buy_ma_order_id, "BTCUSDT", "BUY", 1.0, 100.0, "2026-03-19 10:00:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-ma", 2, "BTCUSDT", "1m", "other_strategy", "SELL", 1.0, 110.0, "FILLED", "2026-03-19 10:05:00"),
        )
        sell_ma_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'sell-ma';").fetchone()[0])
        insert_fill(connection, sell_ma_order_id, "BTCUSDT", "SELL", 1.0, 110.0, "2026-03-19 10:05:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-mom", 3, "ETHUSDT", "1m", "ppo", "BUY", 1.0, 200.0, "FILLED", "2026-03-19 10:10:00"),
        )
        buy_mom_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'buy-mom';").fetchone()[0])
        insert_fill(connection, buy_mom_order_id, "ETHUSDT", "BUY", 1.0, 200.0, "2026-03-19 10:10:00")
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-mom", 4, "ETHUSDT", "1m", "ppo", "SELL", 1.0, 190.0, "FILLED", "2026-03-19 10:15:00"),
        )
        sell_mom_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'sell-mom';").fetchone()[0])
        insert_fill(connection, sell_mom_order_id, "ETHUSDT", "SELL", 1.0, 190.0, "2026-03-19 10:15:00")

        closed_trades = get_strategy_closed_trades(connection, strategy_name="ppo")

        assert len(closed_trades) == 1
        assert closed_trades[0]["strategy_name"] == "ppo"
        assert closed_trades[0]["symbol"] == "ETHUSDT"
        assert closed_trades[0]["status"] == "loss"
    finally:
        connection.close()


def test_get_strategy_activity_summary_supports_open_short_positions() -> None:
    connection = make_connection()
    try:
        ensure_execution_tables(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("ppo-short-open", 1, "SOLUSDT", "1m", "ppo", "SELL", 1.0, 82.27, "FILLED", "2026-04-09 04:28:02"),
        )
        short_open_order_id = int(connection.execute("SELECT id FROM orders WHERE client_order_id = 'ppo-short-open';").fetchone()[0])
        insert_fill(connection, short_open_order_id, "SOLUSDT", "SELL", 1.0, 82.27, "2026-04-09 04:28:02")

        summary = get_strategy_activity_summary(connection)

        by_name = {item["strategy_name"]: item for item in summary}
        assert by_name["ppo"]["filled_order_count"] == 1
        assert by_name["ppo"]["sell_fill_count"] == 1
        assert by_name["ppo"]["buy_fill_count"] == 0
        assert by_name["ppo"]["net_position_qty"] == pytest.approx(-1.0)
        assert by_name["ppo"]["open_position_symbol"] == "SOLUSDT"
        assert by_name["ppo"]["open_entry_price"] == pytest.approx(82.27)
        assert by_name["ppo"]["open_position_opened_at"] == "2026-04-09 04:28:02"
    finally:
        connection.close()


def test_strategy_registry_exposes_ppo() -> None:
    names = list_registered_strategies()

    assert "ppo" in names
    assert get_strategy("ppo") is not None


def test_generate_registered_signal_runs_ppo_strategy(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr(
            "app.strategy.registry.STRATEGY_REGISTRY",
            {
                "ppo": lambda conn, symbol="BTCUSDT", timeframe="1m": {
                    "strategy_name": "ppo",
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "signal_type": "BUY",
                    "short_ma": 3.0,
                    "long_ma": 2.0,
                }
            },
        )

        result = generate_registered_signal(connection, strategy_name="ppo")

        assert result == {
            "strategy_name": "ppo",
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "signal_type": "BUY",
            "short_ma": 3.0,
            "long_ma": 2.0,
        }
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


def test_assert_pipeline_validation_success_accepts_nested_queue_batch_steps() -> None:
    assert_pipeline_validation_success(
        {
            "status": "completed",
            "orchestration": "queue_batch",
            "result": {
                "status": "ok",
                "steps": [
                    {"step": "generate_signal", "signal_type": "BUY"},
                    {"step": "paper_execute", "decision": "REJECTED"},
                ],
            },
        }
    )


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


def test_add_fills_commission_uses_backend_aware_column_lookup(monkeypatch) -> None:
    executed: list[tuple[str, tuple]] = []

    class FakeConnection:
        def execute(self, query: str, params: tuple = ()):
            executed.append((" ".join(query.split()), params))
            return None

    monkeypatch.setattr("app.core.migrations.table_exists", lambda _connection, table_name: table_name == "fills")
    monkeypatch.setattr(
        "app.core.migrations.get_table_columns",
        lambda _connection, _table_name: {"id", "order_id", "symbol", "side", "qty", "price", "created_at"},
    )

    from app.core.migrations import _add_fills_commission

    _add_fills_commission(FakeConnection())

    assert all("PRAGMA table_info" not in query for query, _params in executed)
    assert ("ALTER TABLE fills ADD COLUMN commission REAL DEFAULT NULL;", ()) in executed
    assert ("ALTER TABLE fills ADD COLUMN commission_asset TEXT DEFAULT NULL;", ()) in executed
    assert ("ALTER TABLE fills ADD COLUMN quote_qty REAL DEFAULT NULL;", ()) in executed
    assert ("ALTER TABLE fills ADD COLUMN transact_time INTEGER DEFAULT NULL;", ()) in executed


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


# ---- fetch_klines retry / backoff ----

def test_fetch_klines_retries_on_connection_error(monkeypatch) -> None:
    import requests as req
    calls = {"n": 0}

    def fake_get(*_a, **_kw):
        calls["n"] += 1
        if calls["n"] < 3:
            raise req.exceptions.ConnectionError("network down")
        resp = req.Response()
        resp.status_code = 200
        resp._content = b"[]"
        return resp

    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_COUNT", "3")
    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setattr("app.data.binance_client.requests.get", fake_get)

    result = fetch_klines(symbol="BTCUSDT", interval="1m", limit=1)

    assert result == []
    assert calls["n"] == 3


def test_fetch_klines_retries_on_429(monkeypatch) -> None:
    import requests as req
    calls = {"n": 0}

    def fake_get(*_a, **_kw):
        calls["n"] += 1
        resp = req.Response()
        if calls["n"] < 2:
            resp.status_code = 429
            resp._content = b'{"msg":"rate limit"}'
            resp.raise_for_status()
        resp.status_code = 200
        resp._content = b"[]"
        return resp

    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_COUNT", "3")
    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setattr("app.data.binance_client.requests.get", fake_get)

    result = fetch_klines(symbol="BTCUSDT", interval="1m", limit=1)

    assert result == []
    assert calls["n"] == 2


def test_fetch_klines_raises_after_max_retries(monkeypatch) -> None:
    import requests as req

    def fake_get(*_a, **_kw):
        raise req.exceptions.ConnectionError("always down")

    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_COUNT", "2")
    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setattr("app.data.binance_client.requests.get", fake_get)

    with pytest.raises(req.exceptions.ConnectionError):
        fetch_klines(symbol="BTCUSDT", interval="1m", limit=1)


def test_fetch_klines_does_not_retry_on_400(monkeypatch) -> None:
    import requests as req
    calls = {"n": 0}

    def fake_get(*_a, **_kw):
        calls["n"] += 1
        resp = req.Response()
        resp.status_code = 400
        resp._content = b'{"msg":"bad symbol"}'
        resp.raise_for_status()
        return resp

    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_COUNT", "3")
    monkeypatch.setenv("CRYPTO_BINANCE_RETRY_BACKOFF_SECONDS", "0")
    monkeypatch.setattr("app.data.binance_client.requests.get", fake_get)

    with pytest.raises(req.exceptions.HTTPError):
        fetch_klines(symbol="INVALID", interval="1m", limit=1)

    assert calls["n"] == 1  # no retry on 4xx


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

    def fake_validate_compose_runtime(**kwargs):
        calls.append(("compose", kwargs))
        result = {
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
        if kwargs.get("include_soak"):
            result["mode"] = "compose-soak-readability"
            result["soak_validation"] = {"status": "ok"}
            result["soak_history"] = []
        return result

    monkeypatch.setattr(
        "scripts.run_postgres_compose_validation.validate_compose_runtime",
        fake_validate_compose_runtime,
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
    compose_call = next(c for c in calls if c[0] == "compose")
    assert compose_call[1].get("include_soak") is True


def test_record_fetch_ignores_permission_error(monkeypatch, tmp_path: Path) -> None:
    runtime_dir = tmp_path / "runtime"
    history_file = runtime_dir / "market_fetch_history.jsonl"
    monkeypatch.setattr(fetch_history_module, "RUNTIME_DIR", runtime_dir)
    monkeypatch.setattr(fetch_history_module, "FETCH_HISTORY_FILE", history_file)

    def fake_open(*args, **kwargs):
        raise PermissionError("read-only runtime")

    monkeypatch.setattr("builtins.open", fake_open)

    fetch_history_module.record_fetch(
        {
            "saved_klines": 1,
            "symbol_names": ["BTCUSDT"],
            "timeframes": ["1m"],
            "symbol_results": [{"symbol": "BTCUSDT", "timeframe": "1m", "saved": 1}],
        }
    )

    assert not history_file.exists()


def test_validate_compose_runtime_sets_world_writable_bind_mounts(monkeypatch, tmp_path: Path) -> None:
    work_dir = tmp_path / "pg-validate"
    work_dir.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "scripts.run_postgres_compose_validation.tempfile.mkdtemp",
        lambda prefix: str(work_dir),
    )
    monkeypatch.setattr(
        "scripts.run_postgres_compose_validation.run_command",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("stop after setup")),
    )
    monkeypatch.setattr("scripts.run_postgres_compose_validation.shutil.rmtree", lambda *args, **kwargs: None)

    with pytest.raises(RuntimeError, match="stop after setup"):
        validate_compose_runtime(
            api_port=8012,
            project_name="crypto_pg_validation",
            database_url="postgresql://crypto:crypto@postgres:5432/crypto",
            startup_timeout=1.0,
            keep_up=False,
        )

    for name in ("storage", "logs", "runtime"):
        mode = os.stat(work_dir / name).st_mode & 0o777
        assert mode == 0o777


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


def test_migration_normalizes_legacy_utc_timestamp_strings() -> None:
    from app.core.migrations import _normalize_legacy_utc_timestamp_strings

    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO positions (symbol, qty, avg_price, realized_pnl, updated_at)
            VALUES (?, ?, ?, ?, ?);
            """,
            ("SOLUSDT", 1.0, 82.21, 0.0, "2026-04-09 06:05:48"),
        )
        connection.execute(
            """
            INSERT INTO runtime_heartbeats (component, status, message, payload_json, last_seen_at)
            VALUES (?, ?, ?, ?, ?);
            """,
            ("scheduler", "ok", "legacy timestamp", None, "2026-04-09 06:05:48"),
        )
        connection.commit()

        _normalize_legacy_utc_timestamp_strings(connection)
        connection.commit()

        position_updated_at = connection.execute(
            "SELECT updated_at FROM positions WHERE symbol = ?;",
            ("SOLUSDT",),
        ).fetchone()[0]
        heartbeat_last_seen_at = connection.execute(
            "SELECT last_seen_at FROM runtime_heartbeats WHERE component = ?;",
            ("scheduler",),
        ).fetchone()[0]

        assert position_updated_at == "2026-04-09T06:05:48+00:00"
        assert heartbeat_last_seen_at == "2026-04-09T06:05:48+00:00"
    finally:
        connection.close()


def test_offline_normalize_legacy_utc_timestamp_strings_updates_large_tables() -> None:
    from app.core.migrations import normalize_legacy_utc_timestamp_strings_offline

    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO audit_events (event_type, status, source, message, payload_json, created_at)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            ("test", "ok", "unit", "legacy timestamp", None, "2026-04-09 06:05:48"),
        )
        connection.execute(
            """
            INSERT INTO signals (
                symbol, timeframe, strategy_name, signal_type, short_ma, long_ma, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            ("SOLUSDT", "1m", "ppo", "BUY", 1.0, 2.0, "2026-04-09 06:05:48"),
        )
        connection.execute(
            "UPDATE schema_migrations SET applied_at = ? WHERE version = ?;",
            ("2026-04-09 06:05:48", "001_create_candles_table"),
        )
        connection.commit()

        result = normalize_legacy_utc_timestamp_strings_offline(
            connection,
            batch_size=1,
            table_names={"audit_events", "signals", "schema_migrations"},
        )

        audit_created_at = connection.execute(
            "SELECT created_at FROM audit_events LIMIT 1;"
        ).fetchone()[0]
        signal_created_at = connection.execute(
            "SELECT created_at FROM signals LIMIT 1;"
        ).fetchone()[0]
        schema_applied_at = connection.execute(
            "SELECT applied_at FROM schema_migrations WHERE version = ?;",
            ("001_create_candles_table",),
        ).fetchone()[0]

        assert result["audit_events.created_at"] == 1
        assert result["signals.created_at"] == 1
        assert result["schema_migrations.applied_at"] >= 1
        assert audit_created_at == "2026-04-09T06:05:48+00:00"
        assert signal_created_at == "2026-04-09T06:05:48+00:00"
        assert schema_applied_at == "2026-04-09T06:05:48+00:00"
    finally:
        connection.close()


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
                        (1, "order-1", 11, "BTCUSDT", "1m", "ppo", "BUY", 0.001, 100.0, "FILLED", "2026-03-18 10:00:00")
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
            "strategy_name": "ppo",
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
        run_migrations(connection)
        kline = make_kline(60_000, 10)

        save_klines(connection, [kline])
        save_klines(connection, [kline])

        row = connection.execute("SELECT COUNT(*) FROM candles;").fetchone()
        assert row is not None
        assert row[0] == 1
    finally:
        connection.close()


def test_save_klines_skips_unclosed_latest_candle(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr(
            "app.data.candles_service.datetime",
            type(
                "_FrozenDateTime",
                (),
                {
                    "now": staticmethod(lambda tz=None: datetime.fromtimestamp(90, tz=timezone.utc)),
                },
            ),
        )
        closed = make_kline(0, 10.0)
        open_kline = make_kline(60_000, 11.0)  # close_time=119999, still open at now_ms=90000

        saved = save_klines(connection, [closed, open_kline])

        rows = connection.execute("SELECT open_time, close FROM candles ORDER BY open_time ASC;").fetchall()
        assert saved == 1
        assert len(rows) == 1
        assert rows[0][0] == 0
    finally:
        connection.close()


def test_evaluate_signal_id_rejects_when_kill_switch_enabled(monkeypatch) -> None:
    monkeypatch.setattr("app.risk.risk_service.kill_switch_enabled", lambda: True)
    connection = make_connection()
    try:
        run_migrations(connection)
        seed_candles(connection, [100.0] * 5)
        connection.execute(
            "INSERT INTO signals (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma) VALUES (?, ?, ?, ?, ?, ?)",
            ("BTCUSDT", "1m", "ppo", "BUY", 1.1, 1.0),
        )
        connection.commit()
        from app.risk.risk_service import evaluate_latest_signal
        result = evaluate_latest_signal(connection)
        assert result is not None
        assert result["decision"] == "REJECTED"
        assert "Kill switch" in result["reason"]
    finally:
        connection.close()


def test_evaluate_signal_ids_batch_evaluates_all(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        seed_candles(connection, [100.0] * 5)
        for signal_type in ("BUY", "SELL"):
            connection.execute(
                "INSERT INTO signals (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma) VALUES (?, ?, ?, ?, ?, ?)",
                ("BTCUSDT", "1m", "ppo", signal_type, 1.1, 1.0),
            )
        connection.commit()
        from app.risk.risk_service import evaluate_signal_ids
        rows = connection.execute("SELECT id FROM signals ORDER BY id").fetchall()
        signal_ids = [int(r[0]) for r in rows]
        results = evaluate_signal_ids(connection, signal_ids)
        assert len(results) == 2
        assert all("decision" in r for r in results)
    finally:
        connection.close()


def test_evaluate_latest_signal_rejects_duplicate_signal_type() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)

        first_signal = insert_signal(connection, "BUY", strategy_name="manual_test")
        first_risk = evaluate_latest_signal(connection, max_position_qty=0.002)
        second_signal = insert_signal(connection, "BUY", strategy_name="manual_test")
        second_risk = evaluate_latest_signal(connection, max_position_qty=0.002)

        assert first_signal["id"] != second_signal["id"]
        assert first_risk is not None
        assert first_risk["decision"] == "APPROVED"
        assert second_risk is not None
        assert second_risk["decision"] == "REJECTED"
        # Second BUY is blocked by the pending approved BUY from the first signal,
        # which is more accurate than "duplicate signal type" (caught earlier now).
        assert "pending_qty" in second_risk["reason"] or second_risk["reason"] == "Duplicate signal type."
    finally:
        connection.close()


def test_evaluate_signal_id_rejects_second_strategy_buy_when_first_is_pending() -> None:
    """Second strategy BUY on same symbol must be REJECTED when first is APPROVED but not yet executed."""
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)

        # Two strategies both signal BUY for BTCUSDT in the same pipeline cycle.
        # Neither has executed yet (no order row exists).
        ma_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="ppo")
        momentum_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="ppo")

        ma_risk = evaluate_signal_id(connection, int(ma_signal["id"]), cooldown_seconds=0, max_position_qty=0.002)
        momentum_risk = evaluate_signal_id(connection, int(momentum_signal["id"]), cooldown_seconds=0, max_position_qty=0.002)

        assert ma_risk is not None
        assert ma_risk["decision"] == "APPROVED"
        assert momentum_risk is not None
        assert momentum_risk["decision"] == "REJECTED"
        assert "pending_qty" in momentum_risk["reason"]
    finally:
        connection.close()


def test_evaluate_signal_id_allows_second_strategy_buy_after_first_is_executed() -> None:
    """Once the first strategy's order is placed, a second BUY from another strategy is blocked by position."""
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)

        # Simulate first strategy approved and order placed (risk_event_id linked to an order).
        ma_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="ppo")
        ma_risk = evaluate_signal_id(connection, int(ma_signal["id"]), cooldown_seconds=0, max_position_qty=0.002)
        assert ma_risk is not None and ma_risk["decision"] == "APPROVED"

        # Insert order fulfilling the first risk event so pending_qty drops to 0.
        connection.execute(
            "INSERT INTO orders"
            " (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status, risk_event_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            ("coid-ma", "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, "FILLED", int(ma_risk["id"])),
        )
        # Update position to reflect executed BUY.
        connection.execute(
            "INSERT OR REPLACE INTO positions (symbol, qty, avg_price, realized_pnl) VALUES (?, ?, ?, ?);",
            ("BTCUSDT", 0.001, 50000.0, 0.0),
        )
        connection.commit()

        momentum_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="ppo")
        momentum_risk = evaluate_signal_id(connection, int(momentum_signal["id"]), cooldown_seconds=0, max_position_qty=0.002)

        assert momentum_risk is not None
        assert momentum_risk["decision"] == "REJECTED"
        # Now rejected due to actual position, not pending.
        assert "Existing long position" in momentum_risk["reason"]
    finally:
        connection.close()


def test_evaluate_signal_id_allows_same_signal_type_for_different_symbols() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)

        btc_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        eth_signal = insert_signal(connection, "BUY", symbol="ETHUSDT", strategy_name="manual_test")
        btc_risk = evaluate_signal_id(connection, int(btc_signal["id"]), cooldown_seconds=0)
        eth_risk = evaluate_signal_id(connection, int(eth_signal["id"]), cooldown_seconds=0)

        assert btc_risk is not None
        assert eth_risk is not None
        assert btc_risk["decision"] == "APPROVED"
        assert eth_risk["decision"] == "APPROVED"
    finally:
        connection.close()


def test_execute_latest_risk_only_creates_one_order_per_risk_event() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10, 11, 12, 13, 14])
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
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


def test_execute_pending_approved_risks_executes_multiple_symbols() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
        ensure_execution_tables(connection)

        save_klines(connection, [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])])
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, close) for index, close in enumerate([20, 21, 22, 23, 24])],
            symbol="ETHUSDT",
        )

        btc_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        eth_signal = insert_signal(connection, "BUY", symbol="ETHUSDT", strategy_name="manual_test")
        btc_risk = evaluate_signal_id(connection, int(btc_signal["id"]), cooldown_seconds=0)
        eth_risk = evaluate_signal_id(connection, int(eth_signal["id"]), cooldown_seconds=0)

        execution_results = execute_pending_approved_risks(connection, order_qty=0.25)

        assert btc_risk is not None
        assert eth_risk is not None
        assert [result["symbol"] for result in execution_results] == ["BTCUSDT", "ETHUSDT"]
        assert [result["risk_event_id"] for result in execution_results] == [btc_risk["id"], eth_risk["id"]]
        assert len(get_orders(connection, limit=5)) == 2
        assert len(get_fills(connection, limit=5)) == 2
    finally:
        connection.close()


def test_evaluate_latest_signal_rejects_buy_when_max_position_would_be_exceeded() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
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
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
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

        insert_signal(connection, "BUY", strategy_name="manual_test")
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
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
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
        rebuild_daily_realized_pnl(connection)

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
        lambda reason, source, notify_message, **kwargs: kill_switch_calls.append(
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
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
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
        rebuild_daily_realized_pnl(connection)

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
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
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
        rebuild_daily_realized_pnl(connection)

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


def test_futures_target_position_buy_does_not_treat_pending_buys_as_existing_long(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="SOLUSDT", strategy_name="ppo")
        for risk_event_id in (101, 102):
            connection.execute(
                """
                INSERT INTO risk_events (
                    id, signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    risk_event_id,
                    risk_event_id,
                    "SOLUSDT",
                    "1m",
                    "ppo",
                    "BUY",
                    "APPROVED",
                    "Passed basic risk checks.",
                    "2026-04-09T06:00:00+00:00",
                ),
            )
        connection.commit()

        monkeypatch.setattr("app.risk.risk_service._binance_futures_position_mode_enabled", lambda: True)
        monkeypatch.setattr("app.risk.risk_service._get_strategy_target_position", lambda *args, **kwargs: 1)
        monkeypatch.setattr("app.risk.risk_service._get_exchange_position_qty", lambda symbol: -1.0)
        monkeypatch.setattr("app.risk.risk_service.check_portfolio_limits", lambda *args, **kwargs: (True, ""))

        result = evaluate_signal_id(
            connection,
            int(signal["id"]),
            order_qty=1.0,
            max_position_qty=2.0,
            cooldown_seconds=0,
        )

        assert result is not None
        assert result["decision"] == "APPROVED"
        assert result["reason"] == "Passed basic risk checks."
    finally:
        connection.close()


def test_execute_latest_risk_refreshes_persisted_daily_realized_pnl() -> None:
    connection = make_connection()
    try:
        today = datetime.now(timezone.utc).date().isoformat()
        ensure_execution_tables(connection)
        run_migrations(connection)
        seed_candles(connection, [100, 101, 102, 103, 110])

        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("buy-ledger-refresh", 100, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED", f"{today} 10:00:00"),
        )
        buy_order_id = int(
            connection.execute(
                "SELECT id FROM orders WHERE client_order_id = 'buy-ledger-refresh';"
            ).fetchone()[0]
        )
        insert_fill(connection, buy_order_id, "BTCUSDT", "BUY", 1.0, 100.0, f"{today} 10:00:00")
        connection.commit()

        connection.execute(
            """
            INSERT INTO risk_events (
                signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (999, "BTCUSDT", "1m", "manual_test", "SELL", "APPROVED", "manual test approval"),
        )
        connection.commit()

        execution_result = execute_latest_risk(connection, order_qty=1.0)

        assert execution_result is not None
        assert execution_result["status"] == "FILLED"
        row = connection.execute(
            """
            SELECT realized_pnl
            FROM daily_realized_pnl
            WHERE symbol = ? AND pnl_date = ?;
            """,
            ("BTCUSDT", today),
        ).fetchone()
        assert row is not None
        assert float(row[0]) == 10.0
    finally:
        connection.close()


def test_update_positions_and_pnl_snapshots_track_realized_and_unrealized_pnl() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [100, 101, 102, 103, 110])
        ensure_execution_tables(connection)
        run_migrations(connection)
        run_migrations(connection)

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

    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", fake_connection)
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda symbol="BTCUSDT", interval="1m", limit=5: [
            make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])
        ],
    )
    from app.strategy.signal_service import insert_signal as _insert_signal
    monkeypatch.setattr(
        "app.pipeline.strategy_job.generate_registered_signal",
        lambda conn, strategy_name="ppo", symbol="BTCUSDT", timeframe="1m": _insert_signal(
            conn, "BUY", symbol=symbol, timeframe=timeframe, strategy_name=strategy_name,
            short_ma=12.0, long_ma=10.0,
        ),
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
        "reconcile_orphan_orders",
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


def test_run_pipeline_collect_records_multi_symbol_summary_in_heartbeat_and_audit(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "market_data_multi_symbol_pipeline.db"

    def fake_connection() -> sqlite3.Connection:
        return sqlite3.connect(db_path)

    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", fake_connection)
    monkeypatch.setattr("app.audit.service.get_connection", fake_connection)
    monkeypatch.setattr("app.system.heartbeat.get_connection", fake_connection)
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda symbol="BTCUSDT", interval="1m", limit=5: [
            make_kline((index + 1) * 60_000, close)
            for index, close in enumerate(
                [10, 11, 12, 13, 14] if symbol == "BTCUSDT" else [20, 21, 22, 23, 24]
            )
        ],
    )
    from app.strategy.signal_service import insert_signal as _insert_signal
    monkeypatch.setattr(
        "app.pipeline.strategy_job.generate_registered_signal",
        lambda conn, strategy_name="ppo", symbol="BTCUSDT", timeframe="1m": _insert_signal(
            conn, "BUY", symbol=symbol, timeframe=timeframe, strategy_name=strategy_name,
            short_ma=12.0, long_ma=10.0,
        ),
    )

    result = run_pipeline_collect()

    assert result["steps"][0]["symbol_results"] == [
        {"symbol": "BTCUSDT", "timeframe": "1m", "saved_klines": 5, "mode": "seed"},
        {"symbol": "ETHUSDT", "timeframe": "1m", "saved_klines": 5, "mode": "seed"},
    ]

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
        events = get_audit_events(connection, limit=10)
    finally:
        connection.close()

    pipeline_heartbeat = next(item for item in heartbeats if item["component"] == "pipeline" and item["status"] == "completed")
    heartbeat_payload = json.loads(pipeline_heartbeat["payload_json"])
    assert heartbeat_payload["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert heartbeat_payload["strategy_names"] == ["ppo"]
    assert heartbeat_payload["execution_backend"] == "paper"
    assert heartbeat_payload["execution_backend_status"]["backend"] == "paper"
    assert heartbeat_payload["execution_backend_status"]["dry_run"] is False
    assert heartbeat_payload["execution_backend_status"]["can_execute_orders"] is True
    assert heartbeat_payload["generated_signal_count"] == 2
    assert heartbeat_payload["approved_risk_count"] == 2
    assert heartbeat_payload["filled_execution_count"] == 2

    pipeline_event = next(item for item in events if item["event_type"] == "pipeline_run" and item["status"] == "completed")
    event_payload = json.loads(pipeline_event["payload_json"])
    assert event_payload["summary"]["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert event_payload["summary"]["execution_backend"] == "paper"
    assert event_payload["summary"]["execution_backend_status"]["backend"] == "paper"
    assert event_payload["summary"]["generated_signal_count"] == 2
    assert event_payload["summary"]["approved_risk_count"] == 2
    assert event_payload["summary"]["filled_execution_count"] == 2


def test_run_pipeline_collect_uses_selected_strategy(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "market_data_strategy.db"
    captured: dict[str, object] = {}

    def fake_connection() -> sqlite3.Connection:
        return sqlite3.connect(db_path)

    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", fake_connection)
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda symbol="BTCUSDT", interval="1m", limit=5: [
            make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])
        ],
    )
    monkeypatch.setattr("app.pipeline.run_pipeline.run_migrations", lambda connection: None)

    def fake_run_job(connection, job_type, payload=None):
        payload = payload or {}
        if job_type == "market_data":
            return {
                "step": "save_klines",
                "saved_klines": 5,
                "symbol_names": payload.get("symbol_names") or ["BTCUSDT"],
                "timeframes": ["1m"],
                "symbol_results": [{"symbol": "BTCUSDT", "timeframe": "1m", "saved_klines": 5, "mode": "seed"}],
            }
        if job_type == "strategy":
            captured["strategy_name"] = payload.get("strategy_name")
            return {
                "status": "ok",
                "signal_ids": [1],
                "steps": [
                    {
                        "step": "generate_signal",
                        "id": 1,
                        "symbol": "BTCUSDT",
                        "timeframe": "1m",
                        "strategy_name": payload.get("strategy_name"),
                        "signal_type": "BUY",
                        "short_ma": 13.0,
                        "long_ma": 12.0,
                    }
                ],
            }
        if job_type == "risk":
            return {
                "status": "ok",
                "risk_event_ids": [1],
                "steps": [
                    {
                        "step": "evaluate_risk",
                        "id": 1,
                        "signal_id": 1,
                        "symbol": "BTCUSDT",
                        "timeframe": "1m",
                        "strategy_name": payload.get("strategy_name", "ppo"),
                        "signal_type": "BUY",
                        "decision": "APPROVED",
                        "reason": "Passed basic risk checks.",
                    }
                ],
            }
        if job_type == "execution":
            return {
                "status": "ok",
                "steps": [
                    {
                        "step": "paper_execute",
                        "status": "FILLED",
                        "symbol": "BTCUSDT",
                        "side": "BUY",
                        "qty": 0.001,
                        "price": 14.0,
                    }
                ],
            }
        raise AssertionError(f"unexpected job_type: {job_type}")

    monkeypatch.setattr("app.pipeline.run_pipeline.run_job", fake_run_job)

    result = run_pipeline_collect(strategy_name="ppo")

    assert captured["strategy_name"] == "ppo"
    assert result["strategy_name"] == "ppo"
    assert result["steps"][1]["strategy_name"] == "ppo"


def test_run_pipeline_collect_uses_selected_symbols(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "market_data_symbols.db"
    captured: dict[str, object] = {}

    def fake_connection() -> sqlite3.Connection:
        return sqlite3.connect(db_path)

    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", fake_connection)
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.pipeline.run_pipeline.run_migrations", lambda connection: None)

    def fake_run_job(connection, job_type, payload=None):
        payload = payload or {}
        if job_type == "market_data":
            captured["market_data_symbols"] = list(payload.get("symbol_names") or [])
            symbol_names = payload.get("symbol_names") or []
            return {
                "step": "save_klines",
                "saved_klines": 10,
                "symbol_names": symbol_names,
                "symbol_results": [{"symbol": symbol, "saved_klines": 5} for symbol in symbol_names],
            }
        if job_type == "strategy":
            captured["strategy_symbols"] = list(payload.get("symbol_names") or [])
            strategy_name = str(payload.get("strategy_name") or "ppo")
            symbol_names = payload.get("symbol_names") or []
            return {
                "status": "ok",
                "steps": [
                    {
                        "step": "generate_signal",
                        "strategy_name": strategy_name,
                        "symbol": symbol_names[0],
                        "signal_type": "BUY",
                        "short_ma": 2.0,
                        "long_ma": 1.0,
                    },
                ],
                "signal_ids": [7],
            }
        if job_type == "risk":
            captured["risk_signal_ids"] = list(payload.get("signal_ids") or [])
            return {
                "status": "ok",
                "steps": [
                    {
                        "step": "evaluate_risk",
                        "id": 11,
                        "signal_id": 7,
                        "strategy_name": "ppo",
                        "symbol": (payload.get("symbol_names") or ["ETHUSDT"])[0],
                        "decision": "APPROVED",
                        "reason": "Passed basic risk checks.",
                    },
                ],
                "risk_event_ids": [11],
            }
        if job_type == "execution":
            captured["execution_risk_event_ids"] = list(payload.get("risk_event_ids") or [])
            return {
                "status": "ok",
                "steps": [{"step": "paper_execute", "risk_event_id": 11, "symbol": "ETHUSDT", "status": "FILLED", "side": "BUY"}],
            }
        raise AssertionError(f"Unexpected job_type: {job_type}")

    monkeypatch.setattr("app.pipeline.run_pipeline.run_job", fake_run_job)

    result = run_pipeline_collect(strategy_name="ppo", symbol_names=["ETHUSDT"])

    assert result["requested_symbol_names"] == ["ETHUSDT"]
    assert captured["market_data_symbols"] == ["ETHUSDT"]
    assert captured["strategy_symbols"] == ["ETHUSDT"]
    assert captured["risk_signal_ids"] == [7]
    assert captured["execution_risk_event_ids"] == [11]


def test_print_pipeline_result_includes_symbol_and_strategy_scope() -> None:
    buffer = StringIO()
    result = {
        "steps": [
            {
                "step": "save_klines",
                "saved_klines": 10,
                "symbol_results": [
                    {"symbol": "BTCUSDT", "saved_klines": 5},
                    {"symbol": "ETHUSDT", "saved_klines": 5},
                ],
            },
            {
                "step": "generate_signal",
                "strategy_name": "ppo",
                "symbol": "BTCUSDT",
                "signal_type": "BUY",
                "short_ma": 13.0,
                "long_ma": 12.0,
            },
            {
                "step": "evaluate_risk",
                "strategy_name": "ppo",
                "symbol": "BTCUSDT",
                "decision": "APPROVED",
                "reason": "Passed basic risk checks.",
            },
            {
                "step": "paper_execute",
                "strategy_name": "ppo",
                "symbol": "BTCUSDT",
                "status": "FILLED",
                "side": "BUY",
                "qty": 0.25,
                "price": 14.0,
            },
        ]
    }

    with contextlib.redirect_stdout(buffer):
        print_pipeline_result(result)

    output = buffer.getvalue()
    assert "[symbol=BTCUSDT] saved_klines=5" in output
    assert "[symbol=ETHUSDT] saved_klines=5" in output
    assert "[strategy=ppo symbol=BTCUSDT] signal=BUY" in output
    assert "[strategy=ppo symbol=BTCUSDT] decision=APPROVED" in output
    assert "[strategy=ppo symbol=BTCUSDT] order_status=FILLED" in output


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

    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.audit.service.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda symbol="BTCUSDT", interval="1m", limit=5: (_ for _ in ()).throw(RuntimeError("Binance API unavailable")),
    )

    result = run_pipeline_collect()

    # Error is now nested in symbol_results instead of a top-level failed step
    assert result["steps"][0]["step"] == "save_klines"
    assert result["steps"][0]["symbol_results"][0]["error"] == "Binance API unavailable"
    assert result["steps"][0]["symbol_results"][0]["saved_klines"] == 0

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
        events = get_audit_events(connection, limit=5)
    finally:
        connection.close()

    assert any(item["component"] == "pipeline" for item in heartbeats)
    assert any(
        item["event_type"] == "pipeline_run"
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


def test_metrics_endpoint_returns_all_sections() -> None:
    client = TestClient(app)
    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "period_hours" in data
    assert "signals" in data
    assert "risk" in data
    assert "execution" in data
    assert "pnl" in data
    assert "queue" in data
    assert data["period_hours"] == 24


def test_metrics_endpoint_accepts_period_hours_param() -> None:
    client = TestClient(app)
    response = client.get("/metrics?period_hours=48")
    assert response.status_code == 200
    assert response.json()["period_hours"] == 48


def test_metrics_service_risk_summary_counts_correctly() -> None:
    from app.metrics.metrics_service import build_metrics
    from app.core.migrations import run_migrations

    connection = make_connection()
    try:
        run_migrations(connection)
        # Insert 3 risk events: 2 REJECTED, 1 APPROVED
        for decision, reason in [
            ("REJECTED", "Cooldown active"),
            ("REJECTED", "Cooldown active"),
            ("APPROVED", "Passed basic risk checks."),
        ]:
            connection.execute(
                "INSERT INTO risk_events (signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason)"
                " VALUES (?, ?, ?, ?, ?, ?, ?);",
                (1, "BTCUSDT", "1m", "ppo", "BUY", decision, reason),
            )
        connection.commit()

        result = build_metrics(connection, period_hours=24)

        assert result["risk"]["total"] == 3
        assert result["risk"]["approved"] == 1
        assert result["risk"]["rejected"] == 2
        assert result["risk"]["reject_rate"] == round(2 / 3, 4)
        assert result["risk"]["top_rejection_reasons"][0]["reason"] == "Cooldown active"
        assert result["risk"]["top_rejection_reasons"][0]["count"] == 2
    finally:
        connection.close()


def test_metrics_service_returns_zeros_on_empty_db() -> None:
    from app.metrics.metrics_service import build_metrics
    from app.core.migrations import run_migrations

    connection = make_connection()
    try:
        run_migrations(connection)
        result = build_metrics(connection, period_hours=24)

        assert result["signals"]["total"] == 0
        assert result["risk"]["total"] == 0
        assert result["execution"]["fills"] == 0
        assert result["pnl"]["today"] is None
        assert result["queue"]["completed"] == 0
    finally:
        connection.close()


def test_metrics_service_queue_summary_handles_backend_neutral_timestamps() -> None:
    from app.metrics.metrics_service import build_metrics
    from app.core.migrations import run_migrations

    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO job_queue (job_type, payload_json, status, created_at, started_at, completed_at)
            VALUES (?, ?, 'completed', ?, ?, ?);
            """,
            (
                "pipeline",
                "{}",
                "2099-04-01 10:00:00",
                "2099-04-01 10:00:05",
                "2099-04-01 10:00:08",
            ),
        )
        connection.commit()

        result = build_metrics(connection, period_hours=24)

        assert result["queue"]["completed"] == 1
        assert result["queue"]["failed"] == 0
        assert result["queue"]["avg_job_duration_seconds"] == 3.0
    finally:
        connection.close()


# ---------------------------------------------------------------------------
# Risk config tests
# ---------------------------------------------------------------------------


def test_get_risk_config_returns_global_defaults_when_no_row() -> None:
    from app.core.migrations import run_migrations
    from app.risk.risk_config import get_risk_config
    from app.core.settings import DEFAULT_ORDER_QTY, MAX_POSITION_QTY, COOLDOWN_SECONDS, STOP_LOSS_PCT, MAX_DAILY_LOSS

    connection = make_connection()
    try:
        run_migrations(connection)
        cfg, is_default = get_risk_config(connection, "ppo")
        assert is_default is True
        assert cfg.order_qty == DEFAULT_ORDER_QTY
        assert cfg.max_position_qty == MAX_POSITION_QTY
        assert cfg.cooldown_seconds == COOLDOWN_SECONDS
        assert cfg.stop_loss_pct == STOP_LOSS_PCT
        assert cfg.max_daily_loss == MAX_DAILY_LOSS
    finally:
        connection.close()


def test_set_and_get_risk_config_per_strategy() -> None:
    from app.core.migrations import run_migrations
    from app.risk.risk_config import get_risk_config, set_risk_config

    connection = make_connection()
    try:
        run_migrations(connection)
        set_risk_config(connection, "ppo", order_qty=0.005, cooldown_seconds=600)
        cfg, is_default = get_risk_config(connection, "ppo")
        assert is_default is False
        assert cfg.order_qty == 0.005
        assert cfg.cooldown_seconds == 600
        # Unset fields fall back to global defaults
        from app.core.settings import MAX_POSITION_QTY, STOP_LOSS_PCT, MAX_DAILY_LOSS
        assert cfg.max_position_qty == MAX_POSITION_QTY
        assert cfg.stop_loss_pct == STOP_LOSS_PCT
        assert cfg.max_daily_loss == MAX_DAILY_LOSS
    finally:
        connection.close()


def test_delete_risk_config_reverts_to_defaults() -> None:
    from app.core.migrations import run_migrations
    from app.risk.risk_config import get_risk_config, set_risk_config, delete_risk_config

    connection = make_connection()
    try:
        run_migrations(connection)
        set_risk_config(connection, "ppo", order_qty=0.005)
        _, is_default_before = get_risk_config(connection, "ppo")
        assert is_default_before is False

        deleted = delete_risk_config(connection, "ppo")
        assert deleted is True

        _, is_default_after = get_risk_config(connection, "ppo")
        assert is_default_after is True
    finally:
        connection.close()


def test_evaluate_signal_id_uses_per_strategy_risk_config() -> None:
    """A per-strategy max_position_qty=0 should reject every BUY signal."""
    from app.core.migrations import run_migrations
    from app.risk.risk_config import set_risk_config
    from app.risk.risk_service import evaluate_signal_id
    from app.strategy.signal_service import insert_signal

    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        seed_candles(connection, [50000.0] * 5)

        # Insert a BUY signal directly
        signal_id = insert_and_get_rowid(
            connection,
            "INSERT INTO signals (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma) VALUES (?,?,?,?,?,?);",
            ("BTCUSDT", "1m", "ppo", "BUY", 1.0, 0.9),
        )
        connection.commit()

        # Set per-strategy config with max_position_qty=0 — every BUY should be rejected
        set_risk_config(connection, "ppo", max_position_qty=0.0, order_qty=0.001)

        result = evaluate_signal_id(connection, signal_id)
        assert result is not None
        assert result["decision"] == "REJECTED"
        assert "Max position" in result["reason"]
    finally:
        connection.close()


def test_risk_config_api_list_and_crud() -> None:
    client = TestClient(app)
    strategy = "_test_crud_strategy_"

    # Ensure clean state
    client.delete(f"/risk-config/{strategy}")

    # List — structure check
    resp = client.get("/risk-config")
    assert resp.status_code == 200
    data = resp.json()
    assert "global_defaults" in data
    assert "overrides" in data
    assert isinstance(data["overrides"], list)

    # Get — returns defaults when no override
    resp = client.get(f"/risk-config/{strategy}")
    assert resp.status_code == 200
    cfg = resp.json()
    assert cfg["is_default"] is True
    assert cfg["strategy_name"] == strategy

    # Post — set override
    resp = client.post(f"/risk-config/{strategy}", json={"order_qty": 0.005, "cooldown_seconds": 600, "stop_loss_pct": 0.01})
    assert resp.status_code == 200
    saved = resp.json()
    assert saved["status"] == "ok"
    assert saved["config"]["order_qty"] == 0.005
    assert saved["config"]["cooldown_seconds"] == 600
    assert saved["config"]["stop_loss_pct"] == 0.01

    # Get — now returns non-default
    resp = client.get(f"/risk-config/{strategy}")
    assert resp.status_code == 200
    assert resp.json()["is_default"] is False
    assert resp.json()["order_qty"] == 0.005
    assert resp.json()["stop_loss_pct"] == 0.01

    # Delete — revert to defaults
    resp = client.delete(f"/risk-config/{strategy}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True

    # Get — back to defaults
    resp = client.get(f"/risk-config/{strategy}")
    assert resp.status_code == 200
    assert resp.json()["is_default"] is True


# ---------------------------------------------------------------------------
# Portfolio service tests
# ---------------------------------------------------------------------------


def test_get_portfolio_config_returns_defaults_when_no_row() -> None:
    from app.core.migrations import run_migrations
    from app.portfolio.portfolio_service import get_portfolio_config, DEFAULT_TOTAL_CAPITAL

    connection = make_connection()
    try:
        run_migrations(connection)
        cfg = get_portfolio_config(connection)
        assert cfg.total_capital == DEFAULT_TOTAL_CAPITAL
        assert cfg.enforcement_active is False
    finally:
        connection.close()


def test_set_and_get_portfolio_config() -> None:
    from app.core.migrations import run_migrations
    from app.portfolio.portfolio_service import get_portfolio_config, set_portfolio_config

    connection = make_connection()
    try:
        run_migrations(connection)
        set_portfolio_config(connection, total_capital=10000.0, max_strategy_allocation_pct=0.4)
        cfg = get_portfolio_config(connection)
        assert cfg.total_capital == 10000.0
        assert cfg.max_strategy_allocation_pct == 0.4
        assert cfg.enforcement_active is True
        assert cfg.max_strategy_notional == 4000.0
    finally:
        connection.close()


def test_get_portfolio_summary_empty_db() -> None:
    from app.core.migrations import run_migrations
    from app.portfolio.portfolio_service import get_portfolio_summary

    connection = make_connection()
    try:
        run_migrations(connection)
        summary = get_portfolio_summary(connection)
        assert summary["open_position_count"] == 0
        assert summary["total_open_notional"] == 0.0
        assert summary["within_limits"] is True
        assert summary["violations"] == []
    finally:
        connection.close()


def test_check_portfolio_limits_no_enforcement_when_capital_zero() -> None:
    """When total_capital=0, limits are never enforced."""
    from app.core.migrations import run_migrations
    from app.portfolio.portfolio_service import check_portfolio_limits

    connection = make_connection()
    try:
        run_migrations(connection)
        approved, reason = check_portfolio_limits(connection, "ppo", "BTCUSDT", 1.0)
        assert approved is True
        assert reason == ""
    finally:
        connection.close()


def test_check_portfolio_limits_rejects_when_total_exposure_exceeded() -> None:
    from app.core.migrations import run_migrations
    from app.portfolio.portfolio_service import set_portfolio_config, check_portfolio_limits

    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)

        # Seed BTCUSDT candles at price 9.0 so latest price = 9.0
        seed_candles(connection, [9.0] * 5)

        # Set total_capital=10, max_total_exposure_pct=0.8 → limit = 8.0 USDT
        set_portfolio_config(connection, total_capital=10.0, max_total_exposure_pct=0.8)

        # Open position: 1.0 BTC at 9.0 → current notional = 9.0 (already over limit)
        connection.execute(
            "INSERT INTO positions (symbol, qty, avg_price, realized_pnl) VALUES (?,?,?,?);",
            ("BTCUSDT", 1.0, 9.0, 0.0),
        )
        connection.commit()

        # Any additional BUY should be rejected: 9.0 + 0.001*9.0 > 8.0
        approved, reason = check_portfolio_limits(connection, "ppo", "BTCUSDT", 0.001)
        assert approved is False
        assert "Portfolio total exposure limit" in reason
    finally:
        connection.close()


def test_check_portfolio_limits_includes_pending_approved_buys_in_total_exposure() -> None:
    """Pending approved BUYs (no order yet) must be counted against portfolio limits
    to prevent concurrent risk evaluations from both passing before either executes."""
    from app.core.migrations import run_migrations
    from app.portfolio.portfolio_service import set_portfolio_config, check_portfolio_limits

    connection = make_connection()
    try:
        run_migrations(connection)

        # BTCUSDT price = 5.0; total_capital=10, max_exposure=0.8 → limit=8.0
        seed_candles(connection, [5.0] * 5)
        set_portfolio_config(connection, total_capital=10.0, max_total_exposure_pct=0.8)

        # Insert an APPROVED BUY risk event with no corresponding order (pending execution).
        # order_qty=1.0, price=5.0 → pending notional = 5.0
        connection.execute(
            "INSERT INTO risk_events (signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason)"
            " VALUES (?,?,?,?,?,?,?);",
            (1, "BTCUSDT", "1m", "ppo", "BUY", "APPROVED", "test"),
        )
        connection.commit()

        # Now try another BUY of 1.0 @ 5.0 = 5.0 notional.
        # pending(5.0) + proposed(5.0) = 10.0 > limit(8.0) → must be rejected.
        approved, reason = check_portfolio_limits(connection, "ppo", "BTCUSDT", 1.0)
        assert approved is False
        assert "Portfolio total exposure limit" in reason
    finally:
        connection.close()


def test_check_portfolio_limits_pending_clears_after_order_placed() -> None:
    """Once the pending approved buy has a corresponding order, it is no longer
    counted as pending and the next BUY may be approved again."""
    from app.core.migrations import run_migrations
    from app.portfolio.portfolio_service import set_portfolio_config, check_portfolio_limits

    connection = make_connection()
    try:
        run_migrations(connection)

        # BTCUSDT price = 5.0; total_capital=20, limit=16.0
        seed_candles(connection, [5.0] * 5)
        set_portfolio_config(connection, total_capital=20.0, max_total_exposure_pct=0.8)

        # Insert APPROVED BUY risk event
        risk_event_id = insert_and_get_rowid(
            connection,
            "INSERT INTO risk_events (signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason)"
            " VALUES (?,?,?,?,?,?,?);",
            (1, "BTCUSDT", "1m", "ppo", "BUY", "APPROVED", "test"),
        )
        connection.commit()

        # Before order is placed: pending notional = 5.0 (1 BTC @ 5.0)
        # proposed = 1.0 @ 5.0 = 5.0; total = 10.0 ≤ 16.0 → approved
        approved, _ = check_portfolio_limits(connection, "ppo", "BTCUSDT", 1.0)
        assert approved is True

        # Place the order linking to the risk event
        connection.execute(
            "INSERT INTO orders (client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status)"
            " VALUES (?,?,?,?,?,?,?,?,?);",
            ("o1", risk_event_id, "BTCUSDT", "1m", "ppo", "BUY", 1.0, 5.0, "FILLED"),
        )
        connection.commit()

        # Now risk event has an order — no longer pending; same check should still pass
        approved2, _ = check_portfolio_limits(connection, "ppo", "BTCUSDT", 1.0)
        assert approved2 is True
    finally:
        connection.close()


def test_position_open_close_emits_audit_events() -> None:
    from app.core.migrations import run_migrations
    from app.portfolio.positions_service import update_positions
    from app.data.candles_service import save_klines

    connection = make_connection()
    try:
        run_migrations(connection)
        seed_candles(connection, [50000.0] * 5)

        # Simulate a BUY fill
        connection.execute(
            "INSERT INTO orders (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status)"
            " VALUES (?,?,?,?,?,?,?,?);",
            ("o1", "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, "FILLED"),
        )
        order_id = connection.execute("SELECT last_insert_rowid();").fetchone()[0]
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price) VALUES (?,?,?,?,?);",
            (order_id, "BTCUSDT", "BUY", 0.001, 50000.0),
        )
        connection.commit()

        update_positions(connection)

        events = connection.execute(
            "SELECT event_type, status, message FROM audit_events WHERE event_type='position' ORDER BY id;"
        ).fetchall()
        assert len(events) == 1
        assert events[0][1] == "opened"
        assert "BTCUSDT" in events[0][2]

        # Simulate a SELL fill
        connection.execute(
            "INSERT INTO orders (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status)"
            " VALUES (?,?,?,?,?,?,?,?);",
            ("o2", "BTCUSDT", "1m", "ppo", "SELL", 0.001, 51000.0, "FILLED"),
        )
        order_id2 = connection.execute("SELECT last_insert_rowid();").fetchone()[0]
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price) VALUES (?,?,?,?,?);",
            (order_id2, "BTCUSDT", "SELL", 0.001, 51000.0),
        )
        connection.commit()

        update_positions(connection)

        events = connection.execute(
            "SELECT event_type, status, message FROM audit_events WHERE event_type='position' ORDER BY id;"
        ).fetchall()
        assert len(events) == 2
        assert events[1][1] == "closed"
        assert "BTCUSDT" in events[1][2]
    finally:
        connection.close()


def test_daily_loss_breach_emits_audit_event(monkeypatch) -> None:
    from app.core.migrations import run_migrations
    from app.risk.risk_service import evaluate_signal_id

    connection = make_connection()
    try:
        run_migrations(connection)
        seed_candles(connection, [50000.0] * 5)

        monkeypatch.setattr("app.risk.risk_service.enable_kill_switch", lambda **kwargs: None)
        monkeypatch.setattr("app.risk.risk_service.kill_switch_enabled", lambda: False)

        # Insert a BUY fill then a SELL fill at a lower price to create negative daily PnL.
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price, created_at) VALUES (0,'BTCUSDT','BUY',0.001,50000.0,CURRENT_TIMESTAMP);"
        )
        connection.execute(
            "INSERT INTO fills (order_id, symbol, side, qty, price, created_at) VALUES (0,'BTCUSDT','SELL',0.001,30000.0,CURRENT_TIMESTAMP);"
        )
        connection.commit()
        rebuild_daily_realized_pnl(connection)

        signal_id = insert_and_get_rowid(
            connection,
            "INSERT INTO signals (symbol,timeframe,strategy_name,signal_type,short_ma,long_ma) VALUES (?,?,?,?,?,?);",
            ("BTCUSDT", "1m", "ppo", "BUY", 200.0, 100.0),
        )
        connection.commit()

        # daily_pnl = (30000 - 50000) * 0.001 = -20; limit=10 → breach triggered
        evaluate_signal_id(connection, signal_id, max_daily_loss=10.0)

        events = connection.execute(
            "SELECT event_type, status FROM audit_events WHERE event_type='daily_loss_breach';"
        ).fetchall()
        assert len(events) == 1
        assert events[0][1] == "triggered"
    finally:
        connection.close()


def test_daily_loss_zero_limit_does_not_trigger(monkeypatch) -> None:
    """max_daily_loss=0 means no limit configured — must not auto-enable kill switch."""
    from app.core.migrations import run_migrations
    from app.risk.risk_service import evaluate_signal_id

    connection = make_connection()
    try:
        run_migrations(connection)
        seed_candles(connection, [50000.0] * 5)

        monkeypatch.setattr("app.risk.risk_service.kill_switch_enabled", lambda: False)
        kill_switch_calls = []
        monkeypatch.setattr(
            "app.risk.risk_service.enable_kill_switch",
            lambda **kwargs: kill_switch_calls.append(kwargs),
        )

        signal_id = insert_and_get_rowid(
            connection,
            "INSERT INTO signals (symbol,timeframe,strategy_name,signal_type,short_ma,long_ma) VALUES (?,?,?,?,?,?);",
            ("BTCUSDT", "1m", "ppo", "BUY", 200.0, 100.0),
        )
        connection.commit()

        # max_daily_loss=0 → guard skips the check; kill switch must NOT be called
        evaluate_signal_id(connection, signal_id, max_daily_loss=0.0)

        assert kill_switch_calls == [], "kill switch must not fire when max_daily_loss=0"
        events = connection.execute(
            "SELECT event_type FROM audit_events WHERE event_type='daily_loss_breach';"
        ).fetchall()
        assert events == []
    finally:
        connection.close()


def test_kill_switch_enable_payload_includes_extra_fields(tmp_path, monkeypatch) -> None:
    from app.system.kill_switch import enable_kill_switch, disable_kill_switch

    monkeypatch.setattr("app.system.kill_switch.RUNTIME_DIR", tmp_path)
    monkeypatch.setattr("app.system.kill_switch.KILL_SWITCH_FILE", tmp_path / "kill.switch")
    monkeypatch.setattr("app.system.kill_switch.send_telegram_message", lambda msg: None)

    captured = {}

    def fake_log_event(event_type, status, source, message, payload=None):
        captured["payload"] = payload

    monkeypatch.setattr("app.system.kill_switch.log_event", fake_log_event)

    enable_kill_switch(
        reason="test breach",
        source="test",
        notify_message=None,
        payload_extra={"daily_realized_pnl": -75.0, "limit": -50.0},
    )

    assert captured["payload"]["daily_realized_pnl"] == -75.0
    assert captured["payload"]["limit"] == -50.0
    assert "kill_switch_file" in captured["payload"]


def test_portfolio_api_endpoints() -> None:
    client = TestClient(app)

    # GET /portfolio — works on empty DB
    resp = client.get("/portfolio")
    assert resp.status_code == 200
    data = resp.json()
    assert "open_positions" in data
    assert "per_strategy" in data
    assert "within_limits" in data
    assert isinstance(data["open_position_count"], int)

    # GET /portfolio/config — returns config with expected fields
    resp = client.get("/portfolio/config")
    assert resp.status_code == 200
    cfg = resp.json()
    assert "total_capital" in cfg
    assert "enforcement_active" in cfg

    # POST /portfolio/config — update
    resp = client.post("/portfolio/config", json={"total_capital": 5000.0})
    assert resp.status_code == 200
    assert resp.json()["config"]["total_capital"] == 5000.0
    assert resp.json()["config"]["enforcement_active"] is True

    # GET /portfolio/config — reflects new value
    resp = client.get("/portfolio/config")
    assert resp.status_code == 200
    assert resp.json()["total_capital"] == 5000.0


# ---------------------------------------------------------------------------
# Signal quality check tests
# ---------------------------------------------------------------------------

def _make_signal_quality_db(tmp_path, signals=None, risk_events=None, fills=None):
    db_path = tmp_path / "sq.db"
    conn = sqlite3.connect(db_path)
    run_migrations(conn)
    if signals:
        for sig in signals:
            conn.execute(
                "INSERT INTO signals (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma)"
                " VALUES (?, ?, ?, ?, 0, 0)",
                (sig.get("symbol", "BTCUSDT"), "1m", sig.get("strategy", "ppo"), sig["type"]),
            )
    if risk_events:
        for re in risk_events:
            conn.execute(
                "INSERT INTO risk_events (signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason)"
                " VALUES (NULL, 'BTCUSDT', '1m', ?, ?, ?, ?)",
                (re.get("strategy", "ppo"), re["signal_type"], re["decision"], re.get("reason", "ok")),
            )
    if fills:
        conn.execute(
            "INSERT INTO orders (client_order_id, risk_event_id, broker_name, symbol, timeframe,"
            " strategy_name, side, qty, price, status)"
            " VALUES ('c1', NULL, 'paper', 'BTCUSDT', '1m', 'ppo', 'BUY', 0.001, 100.0, 'FILLED')"
        )
        order_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        for _ in range(fills):
            conn.execute(
                "INSERT INTO fills (order_id, symbol, side, qty, price) VALUES (?, 'BTCUSDT', 'BUY', 0.001, 100.0)",
                (order_id,),
            )
    conn.commit()
    conn.close()
    return db_path


def test_signal_quality_check_empty_db_returns_zeros() -> None:
    conn = sqlite3.connect(":memory:")
    run_migrations(conn)
    result = _signal_quality_check(conn)
    assert result["total_signals"] == 0
    assert result["buy_count"] == 0
    assert result["approval_rate"] is None
    assert result["execution_rate"] is None
    conn.close()


def test_signal_quality_check_buy_sell_hold_counts(tmp_path) -> None:
    db_path = _make_signal_quality_db(
        tmp_path,
        signals=[{"type": "BUY"}, {"type": "BUY"}, {"type": "SELL"}, {"type": "HOLD"}],
    )
    conn = sqlite3.connect(db_path)
    result = _signal_quality_check(conn)
    conn.close()
    assert result["total_signals"] == 4
    assert result["buy_count"] == 2
    assert result["sell_count"] == 1
    assert result["hold_count"] == 1
    assert result["buy_rate"] == 0.5
    assert result["sell_rate"] == 0.25
    assert result["actionable_rate"] == 0.75


def test_signal_quality_check_approval_rate(tmp_path) -> None:
    db_path = _make_signal_quality_db(
        tmp_path,
        signals=[{"type": "BUY"}, {"type": "BUY"}],
        risk_events=[
            {"signal_type": "BUY", "decision": "APPROVED"},
            {"signal_type": "BUY", "decision": "REJECTED", "reason": "some reason"},
        ],
    )
    conn = sqlite3.connect(db_path)
    result = _signal_quality_check(conn)
    conn.close()
    assert result["approval_rate"] == 0.5


def test_signal_quality_check_duplicate_rejection_rate(tmp_path) -> None:
    db_path = _make_signal_quality_db(
        tmp_path,
        signals=[{"type": "BUY"}, {"type": "BUY"}],
        risk_events=[
            {"signal_type": "BUY", "decision": "APPROVED"},
            {"signal_type": "BUY", "decision": "REJECTED", "reason": "Duplicate signal type."},
        ],
    )
    conn = sqlite3.connect(db_path)
    result = _signal_quality_check(conn)
    conn.close()
    assert result["duplicate_rejection_rate"] == 0.5


def test_signal_quality_check_execution_rate(tmp_path) -> None:
    db_path = _make_signal_quality_db(
        tmp_path,
        signals=[{"type": "BUY"}],
        risk_events=[
            {"signal_type": "BUY", "decision": "APPROVED"},
            {"signal_type": "BUY", "decision": "APPROVED"},
        ],
        fills=1,
    )
    conn = sqlite3.connect(db_path)
    result = _signal_quality_check(conn)
    conn.close()
    assert result["execution_rate"] == 0.5  # 1 fill / 2 approved


def test_signal_quality_check_by_strategy_breakdown(tmp_path) -> None:
    db_path = _make_signal_quality_db(
        tmp_path,
        signals=[
            {"type": "BUY", "strategy": "ppo"},
            {"type": "SELL", "strategy": "ppo"},
            {"type": "BUY", "strategy": "ppo"},
        ],
    )
    conn = sqlite3.connect(db_path)
    result = _signal_quality_check(conn)
    conn.close()
    assert "ppo" in result["by_strategy"]
    assert result["by_strategy"]["ppo"]["signals"] == 3
    assert result["by_strategy"]["ppo"]["buy"] == 2
    assert result["by_strategy"]["ppo"]["sell"] == 1


def test_signal_quality_check_all_hold_gives_zero_actionable_rate(tmp_path) -> None:
    db_path = _make_signal_quality_db(
        tmp_path,
        signals=[{"type": "HOLD"}, {"type": "HOLD"}],
    )
    conn = sqlite3.connect(db_path)
    result = _signal_quality_check(conn)
    conn.close()
    assert result["actionable_rate"] == 0.0
    assert result["buy_count"] == 0


def test_build_soak_report_includes_signal_quality_key(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "soak2.db"
    conn = sqlite3.connect(db_path)
    run_migrations(conn)
    seed_candles(conn, [10.0, 11.0, 12.0])
    conn.close()
    monkeypatch.setattr("app.validation.soak_report.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.validation.soak_report.read_scheduler_log", lambda lines=200: ["dummy log line"])
    monkeypatch.setattr("app.validation.soak_report.get_heartbeats", lambda c: [])
    monkeypatch.setattr("app.validation.soak_report.build_soak_history_summary", lambda: {})
    report = build_soak_validation_report()
    assert "signal_quality" in report
    sq = report["signal_quality"]
    for key in ("total_signals", "buy_count", "sell_count", "hold_count",
                "actionable_rate", "approval_rate", "execution_rate",
                "duplicate_rejection_rate", "by_strategy"):
        assert key in sq


def test_build_soak_report_flags_low_actionable_rate_as_issue(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "soak3.db"
    conn = sqlite3.connect(db_path)
    run_migrations(conn)
    seed_candles(conn, [10.0, 11.0])
    # All HOLD signals → actionable_rate = 0 < 5%
    for _ in range(10):
        conn.execute(
            "INSERT INTO signals (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma)"
            " VALUES ('BTCUSDT', '1m', 'ppo', 'HOLD', 0, 0)"
        )
    conn.commit()
    conn.close()
    monkeypatch.setattr("app.validation.soak_report.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.validation.soak_report.read_scheduler_log", lambda lines=200: ["ok"])
    monkeypatch.setattr("app.validation.soak_report.get_heartbeats", lambda c: [])
    monkeypatch.setattr("app.validation.soak_report.build_soak_history_summary", lambda: {})
    report = build_soak_validation_report()
    assert any("actionable rate" in issue.lower() for issue in report["issues"])


# ---------------------------------------------------------------------------
# Audit log integration tests
# ---------------------------------------------------------------------------


class _SharedConn:
    """Wraps a sqlite3 connection, exposing close() as no-op for TestClient reuse."""
    def __init__(self, conn):
        self._conn = conn

    def close(self):
        pass

    def really_close(self):
        self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def _make_audit_conn():
    raw = sqlite3.connect(":memory:", check_same_thread=False)
    run_migrations(raw)
    return _SharedConn(raw)


def _audit_rows(conn, event_type=None):
    q = "SELECT event_type, status, source, message, payload_json FROM audit_events"
    if event_type:
        rows = conn._conn.execute(q + " WHERE event_type = ?", (event_type,)).fetchall()
    else:
        rows = conn._conn.execute(q).fetchall()
    return rows


def _patch_conn(monkeypatch, conn):
    """Patch both the API and audit service get_connection to use shared conn."""
    monkeypatch.setattr("app.api.main.get_connection", lambda: conn)
    monkeypatch.setattr("app.api.deps.get_connection", lambda: conn)
    monkeypatch.setattr("app.audit.service.get_connection", lambda: conn)


def test_audit_log_portfolio_config_update(monkeypatch) -> None:
    conn = _make_audit_conn()
    _patch_conn(monkeypatch, conn)
    client = TestClient(app)
    resp = client.post("/portfolio/config", json={"total_capital": 10000.0})
    assert resp.status_code == 200
    rows = _audit_rows(conn, "portfolio_config")
    assert len(rows) == 1
    assert rows[0][1] == "ok"
    assert rows[0][2] == "api"
    conn.really_close()


def test_audit_log_risk_config_update(monkeypatch) -> None:
    conn = _make_audit_conn()
    _patch_conn(monkeypatch, conn)
    client = TestClient(app)
    resp = client.post("/risk-config/ppo", json={"order_qty": 0.002})
    assert resp.status_code == 200
    rows = _audit_rows(conn, "risk_config")
    assert len(rows) == 1
    assert rows[0][1] == "ok"
    assert "ppo" in rows[0][3]
    conn.really_close()


def test_audit_log_risk_config_delete(monkeypatch) -> None:
    conn = _make_audit_conn()
    _patch_conn(monkeypatch, conn)
    client = TestClient(app)
    # First create an override, then delete it
    client.post("/risk-config/ppo", json={"order_qty": 0.002})
    resp = client.delete("/risk-config/ppo")
    assert resp.status_code == 200
    rows = _audit_rows(conn, "risk_config")
    assert len(rows) == 2  # one for update, one for delete
    messages = [r[3] for r in rows]
    assert any("deleted" in m for m in messages)
    conn.really_close()


def test_audit_log_param_sync(monkeypatch) -> None:
    import json as _json
    conn = _make_audit_conn()
    # Seed a sweep run directly
    params = {"order_qty": 0.003, "max_position_qty": 0.006}
    conn._conn.execute(
        """INSERT INTO backtest_runs
           (run_type, symbol, strategy_name, timeframe, candle_count, trade_count,
            fill_on, sharpe_ratio, params_json)
           VALUES ('sweep', 'BTCUSDT', 'ppo', '1m', 100, 5, 'close', 1.5, ?)""",
        (_json.dumps(params),),
    )
    conn._conn.commit()
    _patch_conn(monkeypatch, conn)
    client = TestClient(app)
    resp = client.post("/backtest/sweep/ppo/apply-best-params", json={})
    assert resp.status_code == 200
    rows = _audit_rows(conn, "param_sync")
    assert len(rows) == 1
    assert rows[0][1] == "ok"
    assert "ppo" in rows[0][3]
    payload = _json.loads(rows[0][4])
    assert payload["params_applied"]["order_qty"] == 0.003
    conn.really_close()


# ---------------------------------------------------------------------------
# Market Data Layer — get_candles_status + POST /market-data/fetch
# ---------------------------------------------------------------------------

def test_get_candles_status_empty() -> None:
    from app.data.candles_service import get_candles_status
    connection = make_connection()
    try:
        run_migrations(connection)
        result = get_candles_status(connection)
        assert result == []
    finally:
        connection.close()


def test_get_candles_status_single_symbol() -> None:
    from app.data.candles_service import get_candles_status
    connection = make_connection()
    try:
        seed_candles(connection, [100.0, 101.0, 102.0, 103.0, 104.0])
        result = get_candles_status(connection)
        assert len(result) == 1
        row = result[0]
        assert row["symbol"] == "BTCUSDT"
        assert row["timeframe"] == "1m"
        assert row["count"] == 5
        assert isinstance(row["stale_seconds"], int)
        assert isinstance(row["has_gaps"], bool)
        assert isinstance(row["gap_count_estimate"], int)
        assert isinstance(row["staleness_threshold_seconds"], int)
        assert isinstance(row["is_stale"], bool)
        # 1m × 3 = 180s threshold
        assert row["staleness_threshold_seconds"] == 180
    finally:
        connection.close()


def test_candle_staleness_threshold_seconds_is_timeframe_aware() -> None:
    from app.data.candles_service import candle_staleness_threshold_seconds
    assert candle_staleness_threshold_seconds("1m", multiplier=3) == 180
    assert candle_staleness_threshold_seconds("5m", multiplier=3) == 900
    assert candle_staleness_threshold_seconds("1h", multiplier=3) == 10_800
    assert candle_staleness_threshold_seconds("4h", multiplier=3) == 43_200
    assert candle_staleness_threshold_seconds("1d", multiplier=3) == 259_200
    # unknown timeframe falls back to 1m
    assert candle_staleness_threshold_seconds("99x", multiplier=3) == 180


def test_get_candles_status_no_gaps_when_consecutive() -> None:
    from app.data.candles_service import get_candles_status
    connection = make_connection()
    try:
        # Consecutive 1m candles — no gaps expected
        seed_candles(connection, [100.0] * 10)
        result = get_candles_status(connection)
        assert result[0]["has_gaps"] is False
        assert result[0]["gap_count_estimate"] == 0
    finally:
        connection.close()


def test_get_candles_status_detects_gaps() -> None:
    from app.data.candles_service import get_candles_status
    connection = make_connection()
    try:
        run_migrations(connection)
        # Insert candles with a 5-minute gap (skip 4 minutes)
        klines = [
            make_kline(60_000, 100.0),
            make_kline(120_000, 101.0),
            make_kline(420_000, 102.0),  # jump: skipped 3 minutes
        ]
        save_klines(connection, klines)
        result = get_candles_status(connection)
        assert result[0]["has_gaps"] is True
        assert result[0]["gap_count_estimate"] >= 1
    finally:
        connection.close()


def _make_market_api_conn():
    import sqlite3 as _sqlite3
    from app.core.migrations import run_migrations as _rm

    class _Conn:
        def __init__(self, c):
            self._c = c
        def execute(self, s, p=()):
            return self._c.execute(s, p)
        def executemany(self, s, p):
            return self._c.executemany(s, p)
        def commit(self):
            self._c.commit()
        def rollback(self):
            self._c.rollback()
        def close(self):
            pass
        def really_close(self):
            self._c.close()

    conn = _sqlite3.connect(":memory:", check_same_thread=False)
    conn.row_factory = _sqlite3.Row
    _rm(conn)
    return _Conn(conn)


def test_candles_status_endpoint(monkeypatch) -> None:
    pconn = _make_market_api_conn()
    monkeypatch.setattr("app.api.deps.get_connection", lambda: pconn)
    client = TestClient(app)
    resp = client.get("/candles/status")
    assert resp.status_code == 200
    assert isinstance(resp.json(), list)
    pconn.really_close()


def test_candles_status_endpoint_returns_stats_after_seed(monkeypatch) -> None:
    pconn = _make_market_api_conn()
    monkeypatch.setattr("app.api.deps.get_connection", lambda: pconn)
    run_migrations(pconn._c)
    klines = [make_kline((i + 1) * 60_000, 100.0 + i) for i in range(5)]
    save_klines(pconn._c, klines)
    client = TestClient(app)
    resp = client.get("/candles/status")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 1
    assert data[0]["symbol"] == "BTCUSDT"
    assert data[0]["count"] == 5
    assert data[0]["has_gaps"] is False
    pconn.really_close()


def test_market_data_fetch_endpoint(monkeypatch) -> None:
    pconn = _make_market_api_conn()
    monkeypatch.setattr("app.api.deps.get_connection", lambda: pconn)
    monkeypatch.setenv("CRYPTO_USE_FAKE_KLINES", "1")
    client = TestClient(app)
    resp = client.post("/market-data/fetch", json={"symbols": ["BTCUSDT"], "limit": 10})
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["saved_klines"] >= 0
    assert "symbol_results" in data
    pconn.really_close()


def test_market_data_fetch_endpoint_no_symbols_uses_active(monkeypatch) -> None:
    pconn = _make_market_api_conn()
    monkeypatch.setattr("app.api.deps.get_connection", lambda: pconn)
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda symbol, limit=100, interval="1m": [make_kline(60_000, 100.0)],
    )
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT"])
    client = TestClient(app)
    resp = client.post("/market-data/fetch")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    pconn.really_close()


def test_market_data_fetch_endpoint_respects_limit(monkeypatch) -> None:
    pconn = _make_market_api_conn()
    monkeypatch.setattr("app.api.deps.get_connection", lambda: pconn)
    captured: dict = {}
    def _fake_fetch(symbol, limit=100, interval="1m"):
        captured["limit"] = limit
        return [make_kline(60_000, 100.0)]
    monkeypatch.setattr("app.pipeline.market_data_job.fetch_klines", _fake_fetch)
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT"])
    client = TestClient(app)
    client.post("/market-data/fetch", json={"limit": 500})
    assert captured["limit"] == 500
    pconn.really_close()


def test_futures_collectors_config_endpoint_reports_enabled_flags(monkeypatch) -> None:
    monkeypatch.setattr("app.data.futures_orderbook_service.is_futures_orderbook_collection_enabled", lambda: True)
    monkeypatch.setattr("app.data.futures_aggtrade_service.is_futures_aggtrade_collection_enabled", lambda: False)
    monkeypatch.setattr("app.data.futures_premium_service.is_futures_premium_collection_enabled", lambda: True)
    monkeypatch.setattr("app.data.futures_open_interest_service.is_futures_open_interest_collection_enabled", lambda: False)
    monkeypatch.setattr("app.data.futures_liquidation_service.is_futures_liquidation_collection_enabled", lambda: True)
    client = TestClient(app)
    resp = client.get("/collectors/futures/config")
    assert resp.status_code == 200
    data = resp.json()
    assert data["enabled_collectors"] == [
        "futures_orderbook",
        "futures_premium",
        "futures_liquidation",
    ]


def test_futures_collectors_config_endpoint_applies_selection(monkeypatch) -> None:
    calls: list[tuple[str, str]] = []
    monkeypatch.setattr("app.data.futures_orderbook_service.enable_futures_orderbook_collection", lambda: calls.append(("futures_orderbook", "enable")))
    monkeypatch.setattr("app.data.futures_orderbook_service.disable_futures_orderbook_collection", lambda: calls.append(("futures_orderbook", "disable")))
    monkeypatch.setattr("app.data.futures_aggtrade_service.enable_futures_aggtrade_collection", lambda: calls.append(("futures_aggtrade", "enable")))
    monkeypatch.setattr("app.data.futures_aggtrade_service.disable_futures_aggtrade_collection", lambda: calls.append(("futures_aggtrade", "disable")))
    monkeypatch.setattr("app.data.futures_premium_service.enable_futures_premium_collection", lambda: calls.append(("futures_premium", "enable")))
    monkeypatch.setattr("app.data.futures_premium_service.disable_futures_premium_collection", lambda: calls.append(("futures_premium", "disable")))
    monkeypatch.setattr("app.data.futures_open_interest_service.enable_futures_open_interest_collection", lambda: calls.append(("futures_open_interest", "enable")))
    monkeypatch.setattr("app.data.futures_open_interest_service.disable_futures_open_interest_collection", lambda: calls.append(("futures_open_interest", "disable")))
    monkeypatch.setattr("app.data.futures_liquidation_service.enable_futures_liquidation_collection", lambda: calls.append(("futures_liquidation", "enable")))
    monkeypatch.setattr("app.data.futures_liquidation_service.disable_futures_liquidation_collection", lambda: calls.append(("futures_liquidation", "disable")))

    client = TestClient(app)
    resp = client.post(
        "/collectors/futures/config",
        json={"enabled_collectors": ["futures_orderbook", "futures_premium"]},
    )
    assert resp.status_code == 200
    assert resp.json()["enabled_collectors"] == ["futures_orderbook", "futures_premium"]
    assert calls == [
        ("futures_orderbook", "enable"),
        ("futures_aggtrade", "disable"),
        ("futures_premium", "enable"),
        ("futures_open_interest", "disable"),
        ("futures_liquidation", "disable"),
    ]
