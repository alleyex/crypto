import contextlib
import json
import sqlite3
from datetime import datetime, timezone
from io import StringIO
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.db import get_connection
from app.core.job_queue import (
    enqueue_job,
    enqueue_pipeline_jobs,
    run_pipeline_batch,
)
from app.core.job_runner import run_next_queued_job
from app.core.migrations import run_migrations
from app.data.candles_service import save_klines
from app.execution.adapter import get_execution_backend_status
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
from app.execution.runtime import get_execution_backend_runtime_status
from app.execution.runtime import set_execution_backend
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.run_pipeline import run_pipeline_collect
from app.pipeline.strategy_job import run_strategy_job
from app.pipeline.strategy_job import run_strategy_jobs
from app.portfolio.daily_pnl_service import get_daily_realized_pnl
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions
from app.risk.risk_service import evaluate_latest_signal
from app.risk.risk_service import evaluate_signal_id
from app.scheduler.control import read_effective_active_strategies
from app.scheduler.runner import run_scheduler
from app.strategy.registry import generate_registered_signal
from app.strategy.signal_service import insert_signal
from app.system.heartbeat import get_heartbeats
from app.system.heartbeat import upsert_heartbeat
from app.system.kill_switch import enable_kill_switch
from app.validation.soak_history import build_soak_history_summary
from app.validation.soak_history import read_soak_validation_history
from app.validation.soak_history import record_soak_validation_snapshot
from app.validation.soak_report import build_soak_validation_report
from conftest import make_connection, make_kline


def seed_candles(connection: sqlite3.Connection, closes: list[float]) -> None:
    run_migrations(connection)
    klines = [make_kline((index + 1) * 60_000, close) for index, close in enumerate(closes)]
    save_klines(connection, klines)

def test_run_strategy_job_uses_registry_strategy_name(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT"])
        monkeypatch.setattr(
            "app.pipeline.strategy_job.generate_registered_signal",
            lambda conn, strategy_name="ppo", symbol="BTCUSDT", timeframe="1m": {
                "id": 11,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_name": strategy_name,
                "signal_type": "BUY",
                "short_ma": 4.0,
                "long_ma": 3.0,
            },
        )

        result = run_strategy_job(connection, strategy_name="ppo")

        assert result["status"] == "ok"
        assert result["steps"][0]["strategy_name"] == "ppo"
        assert result["steps"][0]["symbol"] == "BTCUSDT"
        assert result["signal_ids"] == [11]
        assert all(step["step"] == "generate_signal" for step in result["steps"])
    finally:
        connection.close()


def test_run_strategy_jobs_runs_multiple_registered_strategies(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr(
            "app.pipeline.strategy_job.run_strategy_job",
            lambda conn, strategy_name="ppo", symbol_names=None, timeframe_names=None: {
                "status": "ok",
                "signal_ids": [1],
                "steps": [
                    {"step": "generate_signal", "strategy_name": strategy_name, "signal_type": "BUY"},
                ],
            },
        )

        result = run_strategy_jobs(connection, ["ppo"])

        assert result["status"] == "ok"
        assert result["strategy_names"] == ["ppo"]
        assert result["signal_ids"] == [1]
        assert [step["strategy_name"] for step in result["steps"] if step["step"] == "generate_signal"] == [
            "ppo",
        ]
    finally:
        connection.close()


def test_run_strategy_jobs_continues_after_one_strategy_crashes(monkeypatch) -> None:
    """A crashing strategy must not prevent subsequent strategies from running."""
    connection = make_connection()
    try:

        def fake_run_strategy_job(conn, strategy_name="ppo", symbol_names=None, timeframe_names=None):
            if strategy_name == "bad_strategy":
                raise RuntimeError("simulated strategy crash")
            return {
                "status": "ok",
                "steps": [
                    {"step": "generate_signal", "strategy_name": strategy_name, "signal_type": "BUY"},
                ],
            }

        monkeypatch.setattr("app.pipeline.strategy_job.run_strategy_job", fake_run_strategy_job)

        result = run_strategy_jobs(connection, ["bad_strategy", "ppo"])

        assert result["status"] == "partial_error"
        assert result["strategy_names"] == ["bad_strategy", "ppo"]
        assert result["signal_ids"] == []
        error_result = next(r for r in result["results"] if r["strategy_name"] == "bad_strategy")
        assert error_result["status"] == "error"
        assert "simulated strategy crash" in error_result["error"]
        ok_result = next(r for r in result["results"] if r.get("status") == "ok")
        assert ok_result is not None
    finally:
        connection.close()


def test_run_strategy_jobs_all_errors_returns_partial_error(monkeypatch) -> None:
    """When every strategy crashes the aggregate status is partial_error."""
    connection = make_connection()
    try:
        call_count = {"n": 0}

        def always_crash(conn, strategy_name="ppo", symbol_names=None, timeframe_names=None):
            call_count["n"] += 1
            raise ValueError(f"crash in {strategy_name}")

        monkeypatch.setattr("app.pipeline.strategy_job.run_strategy_job", always_crash)

        result = run_strategy_jobs(connection, ["ppo"])

        assert result["status"] == "partial_error"
        assert call_count["n"] == 1
        assert all(r["status"] == "error" for r in result["results"])
        assert {r["error_type"] for r in result["results"]} == {"ValueError"}
    finally:
        connection.close()


def test_run_strategy_job_supports_multiple_symbols(monkeypatch) -> None:
    connection = make_connection()
    try:
        monkeypatch.setattr(
            "app.pipeline.strategy_job.generate_registered_signal",
            lambda conn, strategy_name="ppo", symbol="BTCUSDT", timeframe="1m": {
                "id": 11 if symbol == "BTCUSDT" else 12,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_name": strategy_name,
                "signal_type": "BUY",
                "short_ma": 4.0,
                "long_ma": 3.0,
            },
        )

        result = run_strategy_job(connection, strategy_name="ppo", symbol_names=["BTCUSDT", "ETHUSDT"])

        assert result["status"] == "ok"
        assert [step["step"] for step in result["steps"]] == ["generate_signal", "generate_signal"]
        assert [step["symbol"] for step in result["steps"]] == ["BTCUSDT", "ETHUSDT"]
        assert result["signal_ids"] == [11, 12]
    finally:
        connection.close()


def test_evaluate_signal_id_uses_specific_signal(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO signals (id, symbol, timeframe, strategy_name, signal_type, short_ma, long_ma, created_at)
            VALUES
                (1, 'BTCUSDT', '1m', 'ppo', 'BUY', 11.0, 10.0, '2026-03-19 10:00:00'),
                (2, 'ETHUSDT', '1m', 'ppo', 'SELL', 9.0, 10.0, '2026-03-19 10:01:00');
            """
        )
        connection.commit()
        monkeypatch.setattr("app.risk.risk_service.get_daily_realized_pnl", lambda conn, symbol: 0.0)

        result = evaluate_signal_id(connection, 1, cooldown_seconds=0)

        assert result is not None
        assert result["signal_id"] == 1
        assert result["symbol"] == "BTCUSDT"
        assert result["decision"] == "APPROVED"
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
        lambda: SimpleNamespace(strategy="ppo"),
    )
    monkeypatch.setattr(
        "scripts.run_strategy_job.run_strategy_job",
        lambda connection, strategy_name="ppo": {
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
    assert '"strategy_name": "ppo"' in outputs[1]
    assert '"paper_execute"' in outputs[2]


def test_check_binance_backend_script_prints_connectivity_result(monkeypatch) -> None:
    import scripts.check_binance_backend as script

    class FakeClient:
        def check_account_connectivity(self):
            return {"status": "ok", "broker": "binance", "balance_count": 2}

    monkeypatch.setattr("scripts.check_binance_backend.BinanceBrokerClient", FakeClient)

    buffer = StringIO()
    with contextlib.redirect_stdout(buffer):
        script.main()

    assert json.loads(buffer.getvalue()) == {
        "status": "ok",
        "broker": "binance",
        "balance_count": 2,
    }


def test_check_binance_order_script_prints_validation_result(monkeypatch) -> None:
    import scripts.check_binance_order as script

    class FakeClient:
        def check_order_request(self, symbol, side, qty):
            return {
                "status": "ok",
                "broker": "binance",
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "validated": True,
            }

    monkeypatch.setattr("scripts.check_binance_order.BinanceBrokerClient", FakeClient)
    monkeypatch.setattr("scripts.check_binance_order.parse_args", lambda: SimpleNamespace(symbol="BTCUSDT", side="BUY", qty=0.001))

    buffer = StringIO()
    with contextlib.redirect_stdout(buffer):
        script.main()

    assert json.loads(buffer.getvalue()) == {
        "status": "ok",
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "qty": 0.001,
        "validated": True,
    }




def test_run_scheduler_records_soak_snapshot(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "scheduler.log"
    db_path = tmp_path / "scheduler-heartbeat.db"
    recorded = []
    monkeypatch.setattr("app.scheduler.runner.LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.DEFAULT_PIPELINE_ORCHESTRATION", "direct")
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr(
        "app.scheduler.runner.run_pipeline_collect",
        lambda strategy_name="ppo", symbol_names=None: {
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
    assert "run=1 mode=pipeline strategy=ppo symbols=BTCUSDT,ETHUSDT signal=BUY risk=APPROVED execution=FILLED BUY" in log_text
    assert "soak_snapshot status=ok" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "scheduler" and item["status"] == "ok" for item in heartbeats)


def test_run_scheduler_uses_queue_batch_default_for_pipeline_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "scheduler.log"
    db_path = tmp_path / "scheduler-pipeline-queue-batch.db"
    recorded = []

    monkeypatch.setattr("app.scheduler.runner.LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.DEFAULT_PIPELINE_ORCHESTRATION", "queue_batch")
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.scheduler.runner.enqueue_and_run_pipeline_batch",
        lambda connection, **kwargs: {
            "status": "completed",
            "batch_id": "batch-123",
            "enqueued_jobs": [{"batch_id": "batch-123", "job_id": 101, "job_type": "market_data"}],
            "jobs": [{"id": 101, "job_type": "market_data"}],
            "result": {"steps": [{"step": "save_klines", "saved_klines": 5}]},
        },
    )
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="pipeline")

    assert recorded == [{"status": "ok"}]
    log_text = log_path.read_text(encoding="utf-8")
    assert "queued=market_data=queued#101" in log_text
    assert "drained=market_data=drained#101" in log_text


def test_run_scheduler_supports_strategy_only_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "strategy-worker.log"
    db_path = tmp_path / "scheduler-strategy-heartbeat.db"
    recorded = []

    monkeypatch.setattr("app.scheduler.runner.STRATEGY_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo", "ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.scheduler.runner.run_strategy_jobs",
        lambda connection, strategy_names=None, symbol_names=None: {
            "status": "ok",
            "strategy_names": strategy_names or ["ppo"],
            "steps": [
                {"step": "generate_signal", "signal_type": "BUY", "strategy_name": "ppo"},
                {"step": "generate_signal", "signal_type": "SELL", "strategy_name": "ppo"},
            ],
        },
    )
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="strategy-only", strategy_name="ppo")

    assert recorded == [{"status": "ok"}]
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=strategy-only" in log_text
    assert "strategies=ppo,ppo" in log_text
    assert "symbols=BTCUSDT,ETHUSDT" in log_text
    assert "signal=ppo=BUY;ppo=SELL" in log_text
    assert "risk=n/a" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "strategy_worker" and item["status"] == "ok" for item in heartbeats)


def test_run_scheduler_supports_queue_dispatch_for_strategy_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "strategy-worker.log"
    db_path = tmp_path / "scheduler-strategy-queue.db"
    recorded = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr("app.scheduler.runner.STRATEGY_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo", "ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)

    def fake_enqueue_job(connection, job_type, payload=None):
        captured["job_type"] = job_type
        captured["payload"] = payload
        return 77

    monkeypatch.setattr("app.scheduler.runner.enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="strategy-only", orchestration="queue_dispatch")

    assert recorded == [{"status": "ok"}]
    assert captured["job_type"] == "strategy"
    assert captured["payload"] == {
        "execution_backend": "paper",
        "execution_backend_status": {
            "backend": "paper",
            "description": "Paper broker execution backend.",
            "dry_run": False,
            "can_execute_orders": True,
            "is_live": False,
            "placeholder": False,
            "status": "ok",
        },
        "strategy_name": "ppo",
        "strategy_names": ["ppo"],
        "symbol_names": ["BTCUSDT", "ETHUSDT"],
    }
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=strategy-only" in log_text
    assert "queued=strategy=queued#77" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(
        item["component"] == "strategy_worker"
        and item["status"] == "ok"
        and json.loads(item["payload_json"] or "{}").get("queue_dispatch") is True
        for item in heartbeats
    )


def test_run_scheduler_supports_queue_dispatch_for_pipeline_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "scheduler.log"
    db_path = tmp_path / "scheduler-pipeline-queue.db"
    recorded = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr("app.scheduler.runner.LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo", "ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)

    def fake_enqueue_pipeline_jobs(connection, **kwargs):
        captured.update(kwargs)
        return [
            {"batch_id": "batch-123", "job_id": 101, "job_type": "market_data"},
            {"batch_id": "batch-123", "job_id": 102, "job_type": "strategy"},
            {"batch_id": "batch-123", "job_id": 103, "job_type": "risk"},
            {"batch_id": "batch-123", "job_id": 104, "job_type": "execution"},
        ]

    monkeypatch.setattr("app.scheduler.runner.enqueue_pipeline_jobs", fake_enqueue_pipeline_jobs)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="pipeline", orchestration="queue_dispatch")

    assert recorded == [{"status": "ok"}]
    assert captured == {
        "payload": {"orchestration": "queue_dispatch", "source": "scheduler_pipeline"},
        "strategy_name": "ppo",
        "strategy_names": ["ppo", "ppo"],
        "symbol_names": ["BTCUSDT", "ETHUSDT"],
    }
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=pipeline" in log_text
    assert "queued=market_data=queued#101;strategy=queued#102;risk=queued#103;execution=queued#104" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(
        item["component"] == "scheduler"
        and item["status"] == "ok"
        and json.loads(item["payload_json"] or "{}").get("queue_dispatch") is True
        for item in heartbeats
    )


def test_run_scheduler_uses_default_pipeline_orchestration_setting(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "scheduler.log"
    db_path = tmp_path / "scheduler-pipeline-default-queue.db"
    recorded = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr("app.scheduler.runner.LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.DEFAULT_PIPELINE_ORCHESTRATION", "queue_dispatch")
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)

    def fake_enqueue_pipeline_jobs(connection, **kwargs):
        captured["kwargs"] = kwargs
        return [{"batch_id": "batch-123", "job_id": 101, "job_type": "market_data"}]

    monkeypatch.setattr("app.scheduler.runner.enqueue_pipeline_jobs", fake_enqueue_pipeline_jobs)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="pipeline")

    assert recorded == [{"status": "ok"}]
    assert captured["kwargs"] == {
        "payload": {"orchestration": "queue_dispatch", "source": "scheduler_pipeline"},
        "strategy_name": "ppo",
        "strategy_names": ["ppo"],
        "symbol_names": ["BTCUSDT"],
    }
    log_text = log_path.read_text(encoding="utf-8")
    assert "queued=market_data=queued#101" in log_text


def test_run_scheduler_supports_queue_drain_for_strategy_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "strategy-worker.log"
    db_path = tmp_path / "scheduler-strategy-drain.db"
    recorded = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr("app.scheduler.runner.STRATEGY_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo", "ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)

    def fake_run_next_queued_job(connection, job_type=None):
        captured["job_type"] = job_type
        return {
            "status": "completed",
            "job": {"id": 88, "job_type": job_type},
            "result": {
                "status": "ok",
                "steps": [
                    {"step": "generate_signal", "signal_type": "BUY", "strategy_name": "ppo", "symbol": "BTCUSDT"},
                ],
            },
        }

    monkeypatch.setattr("app.scheduler.runner.run_next_queued_job", fake_run_next_queued_job)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="strategy-only", orchestration="queue_drain")

    assert recorded == [{"status": "ok"}]
    assert captured["job_type"] == "strategy"
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=strategy-only" in log_text
    assert "drained=strategy=drained#88" in log_text
    assert "signal=BTCUSDT=BUY" in log_text
    assert "risk=n/a" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(
        item["component"] == "strategy_worker"
        and item["status"] == "ok"
        and json.loads(item["payload_json"] or "{}").get("queue_drain") is True
        for item in heartbeats
    )


def test_run_scheduler_supports_risk_only_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "risk-worker.log"
    db_path = tmp_path / "scheduler-risk-heartbeat.db"
    recorded = []

    monkeypatch.setattr("app.scheduler.runner.RISK_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.scheduler.runner.run_risk_job",
        lambda connection: {
            "status": "ok",
            "risk_event_ids": [11, 12],
            "steps": [
                {"step": "evaluate_risk", "decision": "APPROVED", "strategy_name": "ppo", "symbol": "BTCUSDT"},
                {"step": "evaluate_risk", "decision": "REJECTED", "strategy_name": "ppo", "symbol": "ETHUSDT"},
            ],
        },
    )
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="risk-only")

    assert recorded == [{"status": "ok"}]
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=risk-only" in log_text
    assert "symbols=BTCUSDT,ETHUSDT" in log_text
    assert "signal=n/a" in log_text
    assert "risk=BTCUSDT=APPROVED;ETHUSDT=REJECTED" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "risk_worker" and item["status"] == "ok" for item in heartbeats)


def test_run_scheduler_supports_queue_dispatch_for_risk_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "risk-worker.log"
    db_path = tmp_path / "scheduler-risk-queue.db"
    recorded = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr("app.scheduler.runner.RISK_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)

    def fake_enqueue_job(connection, job_type, payload=None):
        captured["job_type"] = job_type
        captured["payload"] = payload
        return 91

    monkeypatch.setattr("app.scheduler.runner.enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="risk-only", orchestration="queue_dispatch")

    assert recorded == [{"status": "ok"}]
    assert captured["job_type"] == "risk"
    assert captured["payload"]["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=risk-only" in log_text
    assert "queued=risk=queued#91" in log_text


def test_run_scheduler_supports_queue_drain_for_risk_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "risk-worker.log"
    db_path = tmp_path / "scheduler-risk-drain.db"
    recorded = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr("app.scheduler.runner.RISK_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)

    def fake_run_next_queued_job(connection, job_type=None):
        captured["job_type"] = job_type
        return {
            "status": "completed",
            "job": {"id": 92, "job_type": job_type},
            "result": {
                "status": "ok",
                "steps": [
                    {"step": "evaluate_risk", "decision": "APPROVED", "strategy_name": "ppo", "symbol": "BTCUSDT"},
                ],
            },
        }

    monkeypatch.setattr("app.scheduler.runner.run_next_queued_job", fake_run_next_queued_job)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="risk-only", orchestration="queue_drain")

    assert recorded == [{"status": "ok"}]
    assert captured["job_type"] == "risk"
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=risk-only" in log_text
    assert "drained=risk=drained#92" in log_text
    assert "risk=BTCUSDT=APPROVED" in log_text


def test_run_scheduler_supports_queue_drain_for_pipeline_mode(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "scheduler.log"
    db_path = tmp_path / "scheduler-pipeline-drain.db"
    recorded = []
    captured: dict[str, Any] = {}

    monkeypatch.setattr("app.scheduler.runner.LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_effective_active_strategies", lambda: ["ppo", "ppo"])
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)

    def fake_run_pipeline_batch(connection):
        captured["called"] = True
        return {
            "status": "completed",
            "batch_id": "batch-123",
            "remaining_job_types": [],
            "job": {"id": 204, "job_type": "execution"},
            "jobs": [
                {"id": 201, "job_type": "market_data"},
                {"id": 202, "job_type": "strategy"},
                {"id": 203, "job_type": "risk"},
                {"id": 204, "job_type": "execution"},
            ],
            "result": {
                "status": "ok",
                "steps": [
                    {"step": "generate_signal", "signal_type": "BUY", "strategy_name": "ppo", "symbol": "BTCUSDT"},
                    {"step": "evaluate_risk", "decision": "APPROVED", "strategy_name": "ppo", "symbol": "BTCUSDT"},
                ],
            },
        }

    monkeypatch.setattr("app.scheduler.runner.run_pipeline_batch", fake_run_pipeline_batch)
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="pipeline", orchestration="queue_drain")

    assert recorded == [{"status": "ok"}]
    assert captured == {"called": True}
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=pipeline" in log_text
    assert "drained=market_data=drained#201;strategy=drained#202;risk=drained#203;execution=drained#204" in log_text
    assert "signal=BTCUSDT=BUY" in log_text
    assert "risk=BTCUSDT=APPROVED" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(
        item["component"] == "scheduler"
        and item["status"] == "ok"
        and json.loads(item["payload_json"] or "{}").get("queue_drain") is True
        for item in heartbeats
    )


def test_run_scheduler_rejects_unsupported_mode() -> None:
    try:
        run_scheduler(iterations=1, mode="invalid-mode")
    except ValueError as exc:
        assert "Unsupported scheduler mode" in str(exc)
    else:
        raise AssertionError("Expected ValueError for unsupported scheduler mode.")


def test_run_scheduler_supports_execution_only_mode_with_symbols(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "execution-worker.log"
    db_path = tmp_path / "scheduler-execution-heartbeat.db"
    recorded = []

    monkeypatch.setattr("app.scheduler.runner.EXECUTION_WORKER_LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    monkeypatch.setattr("app.scheduler.runner.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.runner.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.scheduler.runner.run_execution_job",
        lambda connection, symbol_names=None: {
            "status": "ok",
            "steps": [
                {"step": "paper_execute", "symbol": "BTCUSDT", "status": "FILLED", "side": "BUY"},
                {"step": "paper_execute", "symbol": "ETHUSDT", "status": "FILLED", "side": "SELL"},
            ],
        },
    )
    monkeypatch.setattr(
        "app.validation.soak_history.record_soak_validation_snapshot",
        lambda: recorded.append({"status": "ok"}) or {"status": "ok"},
    )

    run_scheduler(interval_seconds=0, iterations=1, mode="execution-only")

    assert recorded == [{"status": "ok"}]
    log_text = log_path.read_text(encoding="utf-8")
    assert "mode=execution-only" in log_text
    assert "symbols=BTCUSDT,ETHUSDT" in log_text
    assert "execution=BTCUSDT=FILLED BUY;ETHUSDT=FILLED SELL" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "execution_worker" and item["status"] == "ok" for item in heartbeats)


def test_read_effective_active_strategies_respects_priority_and_disabled(monkeypatch, tmp_path) -> None:
    strategy_file = tmp_path / "scheduler.strategy"
    disabled_file = tmp_path / "scheduler.strategy.disabled"
    priority_file = tmp_path / "scheduler.strategy.priority.json"
    limit_file = tmp_path / "scheduler.strategy.limit"

    strategy_file.write_text("ppo\nppo\n", encoding="utf-8")
    disabled_file.write_text("", encoding="utf-8")
    priority_file.write_text(json.dumps({"ppo": 5}), encoding="utf-8")
    limit_file.write_text("1\n", encoding="utf-8")

    monkeypatch.setattr("app.scheduler.control.STRATEGY_FILE", strategy_file)
    monkeypatch.setattr("app.scheduler.control.DISABLED_STRATEGY_FILE", disabled_file)
    monkeypatch.setattr("app.scheduler.control.PRIORITY_FILE", priority_file)
    monkeypatch.setattr("app.scheduler.control.EFFECTIVE_LIMIT_FILE", limit_file)

    assert read_effective_active_strategies() == ["ppo"]


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
    risk_log = tmp_path / "risk-worker.log"

    pipeline_log.write_text("[2026-03-19T10:00:01] run=1 mode=pipeline signal=BUY\n", encoding="utf-8")
    data_log.write_text("[2026-03-19T10:00:02] run=1 mode=market-data-only signal=n/a\n", encoding="utf-8")
    strategy_log.write_text("[2026-03-19T10:00:03] run=1 mode=strategy-only signal=BUY\n", encoding="utf-8")
    risk_log.write_text("[2026-03-19T10:00:04] run=1 mode=risk-only risk=APPROVED\n", encoding="utf-8")

    monkeypatch.setattr(
        "app.scheduler.control.get_scheduler_log_files",
        lambda: {
            "pipeline": pipeline_log,
            "market-data-only": data_log,
            "strategy-only": strategy_log,
            "risk-only": risk_log,
            "execution-only": tmp_path / "execution-worker.log",
        },
    )

    from app.scheduler.control import read_scheduler_log

    lines = read_scheduler_log(lines=2, mode="all")

    assert lines == [
        "[2026-03-19T10:00:03] run=1 mode=strategy-only signal=BUY",
        "[2026-03-19T10:00:04] run=1 mode=risk-only risk=APPROVED",
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
        run_migrations(connection)
        ensure_execution_tables(connection)
        run_migrations(connection)
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
        run_migrations(connection)
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
        run_migrations(connection)
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
        run_migrations(connection)
        ensure_execution_tables(connection)
        run_migrations(connection)
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

    monkeypatch.setattr("app.validation.soak_report.utc_now", lambda: FrozenDateTime(2026, 3, 18, 1, 0, 0, tzinfo=timezone.utc))
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


def test_build_soak_validation_report_normalizes_datetime_heartbeats(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "heartbeat-soak.db"
    log_path = tmp_path / "scheduler.log"
    log_path.write_text(
        "[2026-03-18T16:08:53] run=1 signal=BUY risk=APPROVED execution=FILLED BUY\n",
        encoding="utf-8",
    )

    connection = sqlite3.connect(db_path)
    try:
        run_migrations(connection)
        seed_candles(connection, [10, 11, 12, 13, 14])
        insert_signal(connection, "BUY", strategy_name="manual_test")
    finally:
        connection.close()

    monkeypatch.setattr("app.validation.soak_report.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.validation.soak_report.read_scheduler_log",
        lambda lines=200: log_path.read_text(encoding="utf-8").splitlines()[-lines:],
    )
    monkeypatch.setattr(
        "app.validation.soak_report.get_heartbeats",
        lambda connection: [
            {
                "component": "scheduler",
                "status": "ok",
                "message": "Scheduler loop completed.",
                "payload_json": "{}",
                "last_seen_at": datetime(2026, 3, 18, 16, 8, 53, tzinfo=timezone.utc),
            }
        ],
    )

    report = build_soak_validation_report()
    snapshot = json.dumps(report, sort_keys=True)

    assert report["heartbeats"][0]["last_seen_at"] == "2026-03-18T16:08:53+00:00"
    assert "2026-03-18T16:08:53+00:00" in snapshot


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


def test_build_soak_history_summary_reports_progress(monkeypatch, tmp_path) -> None:
    history_file = tmp_path / "soak_history.jsonl"
    monkeypatch.setattr("app.validation.soak_history.SOAK_HISTORY_FILE", history_file)
    history_file.write_text(
        "\n".join(
            [
                json.dumps({"status": "ok", "checked_at": "2026-03-18T08:00:00+00:00"}),
                json.dumps({"status": "degraded", "checked_at": "2026-03-19T08:00:00+00:00"}),
                json.dumps({"status": "ok", "checked_at": "2026-03-20T20:00:00+00:00"}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    summary = build_soak_history_summary()

    assert summary["status"] == "ok"
    assert summary["record_count"] == 3
    assert summary["distinct_utc_dates"] == 3
    assert summary["ok_count"] == 2
    assert summary["degraded_count"] == 1
    assert summary["error_count"] == 0
    assert summary["continuous_span_hours"] == 60.0
    assert summary["remaining_span_hours"] == 108.0
    assert summary["meets_weekly_target"] is False
    # New metrics
    assert summary["accumulated_ok_hours"] == round(2 * 60 / 3600, 2)
    assert summary["longest_ok_streak_hours"] == round(1 * 60 / 3600, 2)
    assert summary["ok_rate"] == round(2 / 3, 4)
    assert summary["meets_accumulated_target"] is False
    assert "remaining_accumulated_hours" in summary


def test_soak_validation_endpoints_return_report_and_history(monkeypatch, tmp_path) -> None:
    history_file = tmp_path / "soak_history.jsonl"
    monkeypatch.setattr("app.validation.soak_history.SOAK_HISTORY_FILE", history_file)
    monkeypatch.setattr(
        "app.api.routes.validation.build_soak_validation_report",
        lambda: {"status": "ok", "checked_at": "2026-03-18T10:00:00+00:00", "issues": []},
    )
    monkeypatch.setattr(
        "app.validation.soak_history.build_soak_validation_report",
        lambda: {"status": "ok", "checked_at": "2026-03-18T10:00:00+00:00", "issues": []},
    )
    monkeypatch.setattr(
        "app.api.routes.validation.build_soak_history_summary",
        lambda: {"status": "ok", "continuous_span_hours": 60.0},
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

    summary_response = client.get("/validation/soak/history/summary")
    assert summary_response.status_code == 200
    assert summary_response.json()["continuous_span_hours"] == 60.0


def test_execution_backend_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/execution/backend")

    assert response.status_code == 200
    body = response.json()
    assert body["backend"] == "paper"
    assert body["status"] == "ok"
    assert body["default_backend"] == "paper"
    assert body["available_backends"] == ["paper", "noop", "simulated_live", "binance"]
    assert body["execution_backend_file"].endswith("execution.backend")


def test_execution_backend_update_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        "app.api.routes.execution.set_execution_backend",
        lambda backend, **kwargs: captured.update({"backend": backend, "audit_action": kwargs.get("audit_action"), "audit_message": kwargs.get("audit_message")}) or {"backend": backend, "execution_backend_file": "runtime/execution.backend"},
    )
    monkeypatch.setattr(
        "app.api.routes.execution.get_execution_backend_status",
        lambda: {
            "backend": "noop",
            "description": "No-op execution backend for dry-run validation.",
            "dry_run": True,
            "can_execute_orders": False,
            "placeholder": False,
            "status": "ok",
        },
    )
    monkeypatch.setattr(
        "app.api.routes.execution.get_execution_backend_runtime_status",
        lambda: {
            "backend": "noop",
            "default_backend": "paper",
            "available_backends": ["paper", "noop", "simulated_live", "binance"],
            "execution_backend_file": "runtime/execution.backend",
        },
    )

    response = client.post("/execution/backend", json={"backend": "noop"})

    assert response.status_code == 200
    assert captured["backend"] == "noop"
    assert captured["audit_action"] == "set_execution_backend:noop"
    assert response.json()["backend"] == "noop"
    assert response.json()["dry_run"] is True


def test_execution_backend_check_skips_for_non_binance_backend(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        "app.api.routes.execution.get_execution_backend_status",
        lambda: {
            "backend": "paper",
            "description": "Paper broker execution backend.",
            "dry_run": False,
            "can_execute_orders": True,
            "is_live": False,
            "placeholder": False,
            "status": "ok",
        },
    )

    response = client.get("/execution/backend/check")

    assert response.status_code == 200
    assert response.json() == {
        "status": "skipped",
        "backend": "paper",
        "reason": "Remote connectivity checks are only implemented for the binance backend.",
    }


def test_execution_backend_check_returns_binance_account_status(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        "app.api.routes.execution.get_execution_backend_status",
        lambda: {
            "backend": "binance",
            "description": "Live execution backend using Binance Spot API.",
            "dry_run": False,
            "can_execute_orders": True,
            "is_live": True,
            "placeholder": False,
            "status": "ok",
        },
    )

    class FakeClient:
        def check_account_connectivity(self):
            return {
                "status": "ok",
                "broker": "binance",
                "account_type": "SPOT",
                "can_trade": True,
                "can_deposit": True,
                "can_withdraw": True,
                "balance_count": 3,
            }

    monkeypatch.setattr("app.execution.binance_broker.BinanceBrokerClient", FakeClient)

    response = client.get("/execution/backend/check")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["broker"] == "binance"
    assert response.json()["balance_count"] == 3


def test_scheduler_stop_endpoint_accepts_custom_audit_metadata(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        "app.api.routes.scheduler.set_stop_flag",
        lambda **kwargs: captured.update(kwargs) or "runtime/scheduler.stop",
    )

    response = client.post(
        "/scheduler/stop",
        json={
            "audit_action": "broker_protection:pause_scheduler",
            "audit_message": "Scheduler paused from broker protection recommendation.",
        },
    )

    assert response.status_code == 200
    assert captured["audit_action"] == "broker_protection:pause_scheduler"
    assert captured["audit_message"] == "Scheduler paused from broker protection recommendation."


def test_kill_switch_enable_endpoint_accepts_custom_source(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, str] = {}

    monkeypatch.setattr(
        "app.api.routes.scheduler.enable_kill_switch",
        lambda **kwargs: captured.update(kwargs) or "runtime/kill.switch",
    )

    response = client.post(
        "/kill-switch/enable",
        json={
            "reason": "Kill switch enabled from broker protection recommendation.",
            "source": "broker_protection",
            "notify_message": "Crypto alert: kill switch enabled from broker protection recommendation.",
        },
    )

    assert response.status_code == 200
    assert captured["source"] == "broker_protection"
    assert captured["reason"] == "Kill switch enabled from broker protection recommendation."
    assert captured["reason"] == "Kill switch enabled from broker protection recommendation."


# ---------------------------------------------------------------------------
# /metrics endpoint
# ---------------------------------------------------------------------------

