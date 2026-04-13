import json
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.db import get_connection
from app.core.job_queue import (
    complete_job,
    enqueue_job,
    enqueue_pipeline_jobs,
    fail_job,
    lease_next_job,
    retry_job,
)
from app.core.migrations import run_migrations
from app.data.candles_service import save_klines
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
from app.execution.paper_broker import execute_pending_approved_risks
from app.execution.adapter import get_execution_backend_status
from app.execution.adapter import get_execution_adapter_name
from app.pipeline.execution_job import run_execution_job
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_job_queue_summary
from app.risk.risk_service import evaluate_latest_signal
from app.risk.risk_service import evaluate_signal_id
from app.strategy.signal_service import insert_signal
from app.system.heartbeat import upsert_heartbeat
from conftest import make_connection, make_kline


def seed_candles(connection: sqlite3.Connection, closes: list[float]) -> None:
    run_migrations(connection)
    klines = [make_kline((index + 1) * 60_000, close) for index, close in enumerate(closes)]
    save_klines(connection, klines)

def test_health_endpoint_reports_ok_with_recent_pipeline_activity(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "health.db"
    log_path = tmp_path / "scheduler.log"
    log_path.write_text("[2026-03-18T10:00:00] run=1 signal=BUY risk=APPROVED execution=FILLED BUY\n", encoding="utf-8")
    fixed_now = datetime(2026, 3, 18, 10, 0, 0, tzinfo=timezone.utc)
    latest_open_time = int(fixed_now.timestamp() * 1000) - 60_000

    connection = sqlite3.connect(db_path)
    try:
        run_migrations(connection)
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
        run_migrations(connection)
        ensure_execution_tables(connection)
        run_migrations(connection)
        insert_signal(connection, "BUY", strategy_name="manual_test")
        evaluate_latest_signal(connection, cooldown_seconds=0)
        execute_latest_risk(connection)
        update_positions(connection)
        upsert_heartbeat(
            connection,
            component="pipeline",
            status="completed",
            message="Pipeline run completed.",
            payload={
                "step_count": 6,
                "strategy_name": "ppo",
                "strategy_names": ["ppo"],
                "symbol_names": ["BTCUSDT", "ETHUSDT"],
                "generated_signal_count": 2,
                "approved_risk_count": 2,
                "rejected_risk_count": 0,
                "filled_execution_count": 2,
            },
        )
    finally:
        connection.close()

    monkeypatch.setattr("app.health.checks.LOG_FILE", log_path)
    monkeypatch.setattr("app.health.checks.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.health.checks.utc_now", lambda: fixed_now)
    monkeypatch.setattr(
        "app.health.checks.get_stop_status",
        lambda: {"stopped": False, "stop_file": str(tmp_path / "scheduler.stop")},
    )
    monkeypatch.setattr(
        "app.health.checks.read_scheduler_log",
        lambda lines=1: log_path.read_text(encoding="utf-8").splitlines()[-lines:],
    )
    called = []
    monkeypatch.setattr(
        "app.api.main.maybe_send_broker_alert",
        lambda report: called.append(("broker", report)) or {"sent": False},
    )
    monkeypatch.setattr(
        "app.api.main.maybe_send_execution_alert",
        lambda report: called.append(("execution", report)) or {"sent": False},
    )
    monkeypatch.setattr("app.api.main.maybe_send_health_alert", lambda report: called.append(report) or {"sent": False})
    monkeypatch.setattr(
        "app.api.main.maybe_send_queue_alert",
        lambda report: called.append(("queue", report)) or {"sent": False},
    )
    monkeypatch.setattr(
        "app.api.main.maybe_send_worker_alert",
        lambda report: called.append(("worker", report)) or {"sent": False},
    )
    monkeypatch.setattr(
        "app.health.checks.get_kill_switch_status",
        lambda: {"enabled": False, "kill_switch_file": str(tmp_path / "kill.switch")},
    )

    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["checks"]["database"]["status"] == "ok"
    assert payload["checks"]["candles"]["status"] == "ok"
    assert payload["checks"]["execution_backend"]["backend"] == "paper"
    assert payload["checks"]["execution_backend"]["can_execute_orders"] is True
    assert payload["checks"]["execution_backend"]["dry_run"] is False
    assert payload["checks"]["pipeline"]["status"] == "ok"
    assert payload["checks"]["pipeline"]["latest_run"]["strategy_names"] == ["ppo"]
    assert payload["checks"]["pipeline"]["latest_run"]["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert payload["checks"]["pipeline"]["latest_run"]["execution_backend"] == "paper"
    assert payload["checks"]["pipeline"]["latest_run"]["execution_backend_status"]["backend"] == "paper"
    assert payload["checks"]["pipeline"]["latest_run"]["generated_signal_count"] == 2
    assert payload["checks"]["pipeline"]["latest_run"]["filled_execution_count"] == 2
    assert payload["checks"]["scheduler"]["status"] == "ok"
    assert payload["checks"]["kill_switch"]["status"] == "ok"
    assert payload["config"]["max_daily_loss"] == 50.0
    assert len(called) == 5


def test_health_endpoint_reports_degraded_when_scheduler_stopped_and_no_candles(monkeypatch, tmp_path) -> None:
    db_path = tmp_path / "health-degraded.db"
    connection = sqlite3.connect(db_path)
    try:
        run_migrations(connection)
        ensure_execution_tables(connection)
        run_migrations(connection)
    finally:
        connection.close()

    monkeypatch.setattr("app.api.main._health_cache", {})
    monkeypatch.setattr("app.api.main._health_cache_ts", 0.0)
    monkeypatch.setattr("app.health.checks.LOG_FILE", Path(tmp_path / "missing.log"))
    monkeypatch.setattr("app.health.checks.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.health.checks.get_stop_status",
        lambda: {"stopped": True, "stop_file": str(tmp_path / "scheduler.stop")},
    )
    monkeypatch.setattr("app.health.checks.read_scheduler_log", lambda lines=1: [])
    called = []
    monkeypatch.setattr(
        "app.api.main.maybe_send_broker_alert",
        lambda report: called.append(("broker", report)) or {"sent": False},
    )
    monkeypatch.setattr(
        "app.api.main.maybe_send_execution_alert",
        lambda report: called.append(("execution", report)) or {"sent": False},
    )
    monkeypatch.setattr("app.api.main.maybe_send_health_alert", lambda report: called.append(report) or {"sent": False})
    monkeypatch.setattr(
        "app.api.main.maybe_send_queue_alert",
        lambda report: called.append(("queue", report)) or {"sent": False},
    )
    monkeypatch.setattr(
        "app.api.main.maybe_send_worker_alert",
        lambda report: called.append(("worker", report)) or {"sent": False},
    )
    monkeypatch.setattr(
        "app.health.checks.get_kill_switch_status",
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
    assert len(called) == 5


def test_maybe_send_health_alert_deduplicates_same_degraded_state(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "health_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.health.HEALTH_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
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
        "app.alerting.base.send_telegram_message",
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


def test_maybe_send_health_alert_handles_multiple_failed_heartbeat_components(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "health_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.health.HEALTH_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.health import maybe_send_health_alert

    report = {
        "status": "degraded",
        "checks": {
            "heartbeats": {
                "status": "degraded",
                "components": [
                    {"component": "scheduler", "status": "failed", "message": "late"},
                    {"component": "alerting", "status": "stopped", "message": "idle"},
                ],
            },
        },
    }

    result = maybe_send_health_alert(report)

    assert result["sent"] is True
    assert len(sent_messages) == 1


def test_maybe_send_queue_alert_deduplicates_same_failed_job(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "queue_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.queue.QUEUE_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.queue import maybe_send_queue_alert

    report = {
        "status": "degraded",
        "checks": {
            "queue": {
                "status": "degraded",
                "counts": {"failed": 1, "queued": 0, "leased": 0, "completed": 2, "total": 3},
                "latest_failed_job": {
                    "id": 9,
                    "job_type": "strategy",
                    "attempt_count": 3,
                    "error_message": "strategy failed",
                },
            }
        },
    }

    first = maybe_send_queue_alert(report)
    second = maybe_send_queue_alert(report)

    assert first["sent"] is True
    assert second == {"sent": False, "reason": "Queue alert already sent for current failed state."}
    assert len(sent_messages) == 1


def test_maybe_send_queue_alert_clears_state_when_queue_recovers(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "queue_alert_state.json"
    monkeypatch.setattr("app.alerting.queue.QUEUE_ALERT_STATE_FILE", state_file)

    from app.alerting.queue import maybe_send_queue_alert

    state_file.write_text('{"fingerprint":"x","failed_count":1}', encoding="utf-8")
    result = maybe_send_queue_alert({"status": "ok", "checks": {"queue": {"status": "ok", "counts": {"failed": 0}}}})

    assert result == {"sent": False, "reason": "Queue has no failed jobs."}
    assert not state_file.exists()


def test_maybe_send_worker_alert_deduplicates_same_stale_worker_state(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "worker_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.worker.WORKER_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.worker import maybe_send_worker_alert

    report = {
        "status": "degraded",
        "checks": {
            "heartbeats": {
                "status": "degraded",
                "components": [
                    {
                        "component": "strategy_worker",
                        "status": "ok",
                        "stale": True,
                        "age_seconds": 400,
                    }
                ],
            }
        },
    }

    first = maybe_send_worker_alert(report)
    second = maybe_send_worker_alert(report)

    assert first["sent"] is True
    assert second == {"sent": False, "reason": "Worker alert already sent for current stale state."}
    assert len(sent_messages) == 1


def test_maybe_send_worker_alert_clears_state_when_workers_recover(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "worker_alert_state.json"
    monkeypatch.setattr("app.alerting.worker.WORKER_ALERT_STATE_FILE", state_file)

    from app.alerting.worker import maybe_send_worker_alert

    state_file.write_text('{"fingerprint":"x","worker_count":1}', encoding="utf-8")
    result = maybe_send_worker_alert({"status": "ok", "checks": {"heartbeats": {"status": "ok", "components": []}}})

    assert result == {"sent": False, "reason": "No stale worker heartbeats."}
    assert not state_file.exists()


def test_maybe_send_execution_alert_deduplicates_same_failed_execution_job(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "execution_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.execution.EXECUTION_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.execution import maybe_send_execution_alert

    report = {
        "status": "degraded",
        "checks": {
            "queue": {
                "status": "degraded",
                "latest_failed_job": {
                    "id": 17,
                    "job_type": "execution",
                    "attempt_count": 2,
                    "error_message": "execution worker unavailable",
                },
            }
        },
    }

    first = maybe_send_execution_alert(report)
    second = maybe_send_execution_alert(report)

    assert first["sent"] is True
    assert second == {"sent": False, "reason": "Execution alert already sent for current failed job."}
    assert len(sent_messages) == 1


def test_maybe_send_execution_alert_clears_state_when_execution_recovers(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "execution_alert_state.json"
    monkeypatch.setattr("app.alerting.execution.EXECUTION_ALERT_STATE_FILE", state_file)

    from app.alerting.execution import maybe_send_execution_alert

    state_file.write_text('{"fingerprint":"x","job_id":17}', encoding="utf-8")
    result = maybe_send_execution_alert({"status": "ok", "checks": {"queue": {"status": "ok", "latest_failed_job": None}}})

    assert result == {"sent": False, "reason": "No failed execution queue job."}
    assert not state_file.exists()


def test_broker_protection_check_degrades_when_backend_cannot_execute_approved_orders() -> None:
    result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
        make_connection(),
        {"backend": "noop", "can_execute_orders": False, "dry_run": True, "placeholder": False},
        {"latest_run": {"approved_risk_count": 2}},
    )

    assert result["status"] == "degraded"
    assert result["backend"] == "noop"
    assert result["approved_risk_count"] == 2
    assert result["severity"] == "critical"
    assert result["reason_code"] == "backend_cannot_execute"
    assert result["recommended_action"] == "switch_to_paper_backend"
    assert result["reason"] == "Execution backend cannot execute approved orders."


def test_broker_protection_check_degrades_on_stale_non_terminal_order(monkeypatch) -> None:
    monkeypatch.setattr("app.health.broker_check.ORDER_STALENESS_SECONDS", 300)
    result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
        make_connection(),
        {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
        {"latest_order": {"status": "PENDING", "age_seconds": 420}},
    )

    assert result["status"] == "degraded"
    assert result["severity"] == "medium"
    assert result["reason_code"] == "stale_order_pending"
    assert result["recommended_action"] == "inspect_and_reconcile_orders"
    assert result["reason"] == "Latest order is stale and still not terminal."


def test_broker_protection_check_ignores_cooldown_only_rejected_risk_streak(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr("app.health.broker_check.RISK_REJECTION_STREAK_THRESHOLD", 3)
        for index in range(3):
            connection.execute(
                """
                INSERT INTO risk_events (
                    signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    index + 1,
                    "BTCUSDT",
                    "1m",
                    "ppo",
                    "BUY",
                    "REJECTED",
                    "Cooldown active: last fill 10 seconds ago, minimum 300.",
                    f"2026-03-20 10:0{index}:00",
                ),
            )
        connection.commit()

        result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
            connection,
            {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
            {"latest_risk": {"decision": "REJECTED", "reason": "Cooldown active: last fill 10 seconds ago, minimum 300."}},
        )

        assert result["status"] == "ok"
        assert "rejected_risk_streak" not in result
        assert result["expected_rejected_risk_streak"] == 3
        assert result["expected_latest_rejection_reason"] == "Cooldown active: last fill 10 seconds ago, minimum 300."
        assert result["reason_code"] is None
        assert result["recommended_action"] is None
    finally:
        connection.close()


def test_broker_protection_check_degrades_on_non_cooldown_rejected_risk_streak(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr("app.health.broker_check.RISK_REJECTION_STREAK_THRESHOLD", 3)
        connection.execute(
            """
            INSERT INTO positions (symbol, qty, avg_price, realized_pnl, updated_at)
            VALUES ('BTCUSDT', 0.001, 101000.0, -4.2, '2026-03-20 10:09:00');
            """
        )
        for index in range(3):
            connection.execute(
                """
                INSERT INTO risk_events (
                    signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    index + 1,
                    "BTCUSDT",
                    "1m",
                    "ppo",
                    "BUY",
                    "REJECTED",
                    "Existing long position already open (pending_qty=0.0).",
                    f"2026-03-20 10:0{index}:00",
                ),
            )
        connection.commit()

        result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
            connection,
            {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
            {
                "latest_risk": {
                    "symbol": "BTCUSDT",
                    "strategy_name": "ppo",
                    "decision": "REJECTED",
                    "reason": "Existing long position already open (pending_qty=0.0).",
                }
            },
        )

        assert result["status"] == "degraded"
        assert result["reason"] == "Recent risk evaluations are repeatedly rejected."
        assert result["reason_code"] == "risk_reject_streak"
        assert result["severity"] == "medium"
        assert result["recommended_action"] == "inspect_risk_rules"
        assert result["rejected_risk_streak"] == 3
        assert result["anomalous_rejected_risk_streak"] == 3
        assert result["current_position"]["symbol"] == "BTCUSDT"
        assert result["current_position"]["qty"] == 0.001
        assert len(result["recent_rejection_reasons"]) == 3
        assert result["recent_rejection_reasons"][0]["reason"] == "Existing long position already open (pending_qty=0.0)."
    finally:
        connection.close()


def test_broker_protection_check_ignores_hold_only_rejected_risk_streak(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr("app.health.broker_check.RISK_REJECTION_STREAK_THRESHOLD", 3)
        for index in range(3):
            connection.execute(
                """
                INSERT INTO risk_events (
                    signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    index + 1,
                    "BTCUSDT",
                    "1m",
                    "ppo",
                    "HOLD",
                    "REJECTED",
                    "Signal is HOLD.",
                    f"2026-03-20 10:0{index}:00",
                ),
            )
        connection.commit()

        result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
            connection,
            {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
            {"latest_risk": {"decision": "REJECTED", "reason": "Signal is HOLD."}},
        )

        assert result["status"] == "ok"
        assert "rejected_risk_streak" not in result
        assert result["expected_rejected_risk_streak"] == 3
        assert result["expected_latest_rejection_reason"] == "Signal is HOLD."
        assert result["reason_code"] is None
        assert result["recommended_action"] is None
    finally:
        connection.close()


def test_broker_protection_check_ignores_duplicate_only_rejected_risk_streak(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr("app.health.broker_check.RISK_REJECTION_STREAK_THRESHOLD", 3)
        for index in range(3):
            connection.execute(
                """
                INSERT INTO risk_events (
                    signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    index + 1,
                    "BTCUSDT",
                    "1m",
                    "ppo",
                    "BUY",
                    "REJECTED",
                    "Duplicate signal type.",
                    f"2026-03-20 10:0{index}:00",
                ),
            )
        connection.commit()

        result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
            connection,
            {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
            {"latest_risk": {"decision": "REJECTED", "reason": "Duplicate signal type."}},
        )

        assert result["status"] == "ok"
        assert "rejected_risk_streak" not in result
        assert result["expected_rejected_risk_streak"] == 3
        assert result["expected_latest_rejection_reason"] == "Duplicate signal type."
        assert result["reason_code"] is None
        assert result["recommended_action"] is None
    finally:
        connection.close()


def test_broker_protection_check_ignores_no_position_sell_rejected_risk_streak(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr("app.health.broker_check.RISK_REJECTION_STREAK_THRESHOLD", 3)
        for index in range(3):
            connection.execute(
                """
                INSERT INTO risk_events (
                    signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    index + 1,
                    "BTCUSDT",
                    "1m",
                    "ppo",
                    "SELL",
                    "REJECTED",
                    "No position available to sell.",
                    f"2026-03-20 10:0{index}:00",
                ),
            )
        connection.commit()

        result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
            connection,
            {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
            {"latest_risk": {"decision": "REJECTED", "reason": "No position available to sell."}},
        )

        assert result["status"] == "ok"
        assert "rejected_risk_streak" not in result
        assert result["expected_rejected_risk_streak"] == 3
        assert result["expected_latest_rejection_reason"] == "No position available to sell."
        assert result["reason_code"] is None
        assert result["recommended_action"] is None
    finally:
        connection.close()


def test_broker_protection_check_ignores_position_matches_target_rejected_risk_streak(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr("app.health.broker_check.RISK_REJECTION_STREAK_THRESHOLD", 3)
        for index in range(3):
            connection.execute(
                """
                INSERT INTO risk_events (
                    signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                """,
                (
                    index + 1,
                    "SOLUSDT",
                    "1m",
                    "ppo",
                    "BUY",
                    "REJECTED",
                    "Position already matches target.",
                    f"2026-03-20 10:0{index}:00",
                ),
            )
        connection.commit()

        result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
            connection,
            {"backend": "binance", "can_execute_orders": True, "dry_run": False, "placeholder": False},
            {"latest_risk": {"decision": "REJECTED", "reason": "Position already matches target."}},
        )

        assert result["status"] == "ok"
        assert "rejected_risk_streak" not in result
        assert result["expected_rejected_risk_streak"] == 3
        assert result["expected_latest_rejection_reason"] == "Position already matches target."
        assert result["reason_code"] is None
        assert result["recommended_action"] is None
    finally:
        connection.close()


def test_maybe_send_broker_alert_deduplicates_same_protected_state(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "broker_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.broker.BROKER_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.broker import maybe_send_broker_alert

    report = {
        "checks": {
            "broker_protection": {
                "status": "degraded",
                "backend": "noop",
                "reason": "Execution backend cannot execute approved orders.",
                "reason_code": "backend_cannot_execute",
                "severity": "critical",
                "recommended_action": "switch_to_paper_backend",
                "approved_risk_count": 2,
            }
        }
    }

    first = maybe_send_broker_alert(report)
    second = maybe_send_broker_alert(report)

    assert first["sent"] is True
    assert second == {"sent": False, "reason": "Broker alert already sent for current protected state."}
    assert len(sent_messages) == 1


def test_maybe_send_broker_alert_clears_state_when_protection_recovers(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "broker_alert_state.json"
    monkeypatch.setattr("app.alerting.broker.BROKER_ALERT_STATE_FILE", state_file)

    from app.alerting.broker import maybe_send_broker_alert

    state_file.write_text('{"fingerprint":"x","backend":"noop"}', encoding="utf-8")
    result = maybe_send_broker_alert({"checks": {"broker_protection": {"status": "ok"}}})

    assert result == {"sent": False, "reason": "Broker protection status is ok."}
    assert not state_file.exists()


def test_alert_state_write_stamps_written_at(tmp_path) -> None:
    """write_alert_state() always writes a written_at ISO timestamp."""
    from app.alerting.state import write_alert_state, read_alert_state
    import json

    state_file = tmp_path / "test_state.json"
    write_alert_state(state_file, {"fingerprint": "abc"})

    raw = json.loads(state_file.read_text())
    assert "written_at" in raw
    assert "fingerprint" in raw


def test_alert_state_read_returns_none_when_expired(tmp_path) -> None:
    """read_alert_state() returns None when the state is older than ttl_seconds."""
    from app.alerting.state import write_alert_state, read_alert_state
    from datetime import datetime, timezone, timedelta
    import json

    state_file = tmp_path / "test_state.json"
    # Write a state with written_at 2 hours ago
    old_time = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    state_file.write_text(
        json.dumps({"fingerprint": "abc", "written_at": old_time}),
        encoding="utf-8",
    )

    # TTL of 1 hour → state is expired
    result = read_alert_state(state_file, ttl_seconds=3600)
    assert result is None


def test_alert_state_read_returns_state_when_not_expired(tmp_path) -> None:
    """read_alert_state() returns the state when it is within ttl_seconds."""
    from app.alerting.state import write_alert_state, read_alert_state

    state_file = tmp_path / "test_state.json"
    write_alert_state(state_file, {"fingerprint": "abc"})

    # TTL of 24 hours — just-written state should not be expired
    result = read_alert_state(state_file, ttl_seconds=86400)
    assert result is not None
    assert result["fingerprint"] == "abc"


def test_alert_state_read_never_expires_when_ttl_zero(tmp_path) -> None:
    """read_alert_state() with ttl_seconds=0 never expires the state."""
    from app.alerting.state import read_alert_state
    from datetime import datetime, timezone, timedelta
    import json

    state_file = tmp_path / "test_state.json"
    # Write a state with written_at 30 days ago
    old_time = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    state_file.write_text(
        json.dumps({"fingerprint": "abc", "written_at": old_time}),
        encoding="utf-8",
    )

    result = read_alert_state(state_file, ttl_seconds=0)
    assert result is not None
    assert result["fingerprint"] == "abc"


def test_alert_state_build_fingerprint_handles_datetime() -> None:
    from app.alerting.state import build_fingerprint

    payload = {
        "status": "degraded",
        "latest_failed_job": {
            "completed_at": datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc),
        },
    }

    fingerprint = build_fingerprint(payload)

    assert isinstance(fingerprint, str)
    assert len(fingerprint) == 64


def test_broker_alert_refires_after_ttl_expires(monkeypatch, tmp_path) -> None:
    """Same alert condition re-fires when the saved state TTL has elapsed."""
    from datetime import datetime, timezone, timedelta
    from app.alerting.state import write_alert_state
    import app.alerting.broker as broker_mod

    state_file = tmp_path / "broker_alert_state.json"
    sent_messages: list[str] = []

    monkeypatch.setattr("app.alerting.broker.BROKER_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )
    # Override TTL to 1 second so we can test expiry without sleeping
    monkeypatch.setattr("app.alerting.base.ALERT_REFIRE_SECONDS", 1)

    from app.alerting.broker import maybe_send_broker_alert

    report = {
        "checks": {
            "broker_protection": {
                "status": "degraded",
                "backend": "noop",
                "reason": "Execution backend cannot execute approved orders.",
                "reason_code": "backend_cannot_execute",
                "severity": "critical",
                "recommended_action": "switch_to_paper_backend",
                "approved_risk_count": 1,
            }
        }
    }

    # First call — alert fires, state written
    first = maybe_send_broker_alert(report)
    assert first["sent"] is True

    # Second call immediately — deduplicated (state not yet expired)
    second = maybe_send_broker_alert(report)
    assert second["sent"] is False

    # Simulate expired state by back-dating written_at
    old_time = (datetime.now(timezone.utc) - timedelta(seconds=2)).isoformat()
    import json
    current = json.loads(state_file.read_text())
    current["written_at"] = old_time
    state_file.write_text(json.dumps(current), encoding="utf-8")

    # Third call — TTL elapsed, alert re-fires
    third = maybe_send_broker_alert(report)
    assert third["sent"] is True
    assert len(sent_messages) == 2


def test_pipeline_check_includes_latest_fill_and_unfilled_order_count() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        from app.execution.paper_broker import execute_pending_approved_risks as paper_exec
        paper_exec(connection, symbol_names=["BTCUSDT"])

        _pipeline_check = __import__("app.health.checks", fromlist=["pipeline_check"]).pipeline_check
        result = _pipeline_check(connection)

        assert result["status"] == "ok"
        assert "latest_order" in result
        assert result["latest_order"]["symbol"] == "BTCUSDT"
        assert result["latest_order"]["qty"] == 0.001
        assert result["latest_order"]["status"] == "FILLED"
        assert "latest_fill" in result
        assert result["latest_fill"]["symbol"] == "BTCUSDT"
        assert result["latest_fill"]["side"] == "BUY"
        assert result["latest_fill"]["price"] == 14.0
        assert result["unfilled_order_count"] == 0
    finally:
        connection.close()


def test_pipeline_check_counts_unfilled_orders() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO orders (client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status)
            VALUES ('test-uuid-1', 1, 'BTCUSDT', '1m', 'manual_test', 'BUY', 0.001, 50000.0, 'FILLED');
            """
        )
        connection.commit()

        _pipeline_check = __import__("app.health.checks", fromlist=["pipeline_check"]).pipeline_check
        result = _pipeline_check(connection)

        assert result["unfilled_order_count"] == 1
    finally:
        connection.close()


def test_broker_protection_check_degrades_on_unfilled_orders() -> None:
    result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
        make_connection(),
        {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
        {"unfilled_order_count": 2},
    )

    assert result["status"] == "degraded"
    assert result["severity"] == "high"
    assert result["reason_code"] == "unfilled_orders_detected"
    assert result["recommended_action"] == "inspect_and_reconcile_orders"
    assert result["unfilled_order_count"] == 2
    assert "2 order(s)" in result["reason"]


def test_broker_protection_check_ok_when_no_unfilled_orders() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
            connection,
            {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
            {"unfilled_order_count": 0},
        )
        assert result["status"] == "ok"
        assert result.get("reason_code") is None
    finally:
        connection.close()


def test_maybe_send_broker_alert_includes_unfilled_orders_in_message(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "broker_alert_state.json"
    sent_messages: list[str] = []
    monkeypatch.setattr("app.alerting.broker.BROKER_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.broker import maybe_send_broker_alert

    report = {
        "checks": {
            "broker_protection": {
                "status": "degraded",
                "backend": "paper",
                "reason": "2 order(s) have no corresponding fill.",
                "reason_code": "unfilled_orders_detected",
                "severity": "high",
                "recommended_action": "inspect_and_reconcile_orders",
                "unfilled_order_count": 2,
            }
        }
    }

    result = maybe_send_broker_alert(report)

    assert result["sent"] is True
    assert len(sent_messages) == 1
    assert "unfilled_orders=2" in sent_messages[0]


def test_broker_protection_check_propagates_latest_fill() -> None:
    result = __import__("app.health.checks", fromlist=["broker_protection_check"]).broker_protection_check(
        make_connection(),
        {"backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
        {
            "unfilled_order_count": 1,
            "latest_fill": {"symbol": "BTCUSDT", "side": "BUY", "qty": 0.001, "price": 50000.0, "created_at": "2026-01-01 10:00:00", "age_seconds": 30},
        },
    )
    assert result.get("latest_fill", {}).get("price") == 50000.0


def test_maybe_send_broker_alert_includes_latest_fill_price(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "broker_alert_state.json"
    sent_messages: list[str] = []
    monkeypatch.setattr("app.alerting.broker.BROKER_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.broker import maybe_send_broker_alert

    report = {
        "checks": {
            "broker_protection": {
                "status": "degraded",
                "backend": "paper",
                "reason": "1 order(s) have no corresponding fill.",
                "reason_code": "unfilled_orders_detected",
                "severity": "high",
                "recommended_action": "inspect_and_reconcile_orders",
                "unfilled_order_count": 1,
                "latest_fill": {"symbol": "BTCUSDT", "side": "BUY", "qty": 0.001, "price": 48500.5},
            }
        }
    }

    result = maybe_send_broker_alert(report)

    assert result["sent"] is True
    assert "unfilled_orders=1" in sent_messages[0]
    assert "latest_fill_price=48500.5" in sent_messages[0]


def test_maybe_send_broker_alert_prefers_anomalous_streak_label(monkeypatch, tmp_path) -> None:
    state_file = tmp_path / "broker_alert_state.json"
    sent_messages: list[str] = []
    monkeypatch.setattr("app.alerting.broker.BROKER_ALERT_STATE_FILE", state_file)
    monkeypatch.setattr(
        "app.alerting.base.send_telegram_message",
        lambda text: sent_messages.append(text) or {"sent": True},
    )

    from app.alerting.broker import maybe_send_broker_alert

    report = {
        "checks": {
            "broker_protection": {
                "status": "degraded",
                "backend": "paper",
                "reason": "Recent risk evaluations are repeatedly rejected.",
                "reason_code": "risk_reject_streak",
                "severity": "medium",
                "recommended_action": "inspect_risk_rules",
                "anomalous_rejected_risk_streak": 3,
                "latest_rejection_reason": "Existing long position already open (pending_qty=0.0).",
            }
        }
    }

    result = maybe_send_broker_alert(report)

    assert result["sent"] is True
    assert "anomalous_rejected_risk_streak=3" in sent_messages[0]
    assert ", rejected_risk_streak=3" not in sent_messages[0]


def test_insert_signal_writes_audit_event_for_buy_sell() -> None:
    from app.strategy.signal_service import insert_signal
    import app.strategy.signal_service as signal_service_mod

    connection = make_connection()
    run_migrations(connection)
    captured: list[dict] = []

    original = signal_service_mod.insert_event
    signal_service_mod.insert_event = lambda conn, event_type, status, source, message, payload=None: captured.append(
        {"event_type": event_type, "status": status, "source": source, "payload": payload}
    ) or 1
    try:
        insert_signal(connection, signal_type="BUY", symbol="BTCUSDT", timeframe="1m", strategy_name="test_strategy", short_ma=100.1, long_ma=99.9)
        insert_signal(connection, signal_type="HOLD", symbol="BTCUSDT", timeframe="1m", strategy_name="test_strategy", short_ma=100.0, long_ma=100.0)
        insert_signal(connection, signal_type="SELL", symbol="BTCUSDT", timeframe="1m", strategy_name="test_strategy", short_ma=99.8, long_ma=100.0)
    finally:
        signal_service_mod.insert_event = original
        connection.close()

    assert len(captured) == 2, "Only BUY and SELL should produce audit events"
    assert captured[0]["event_type"] == "signal"
    assert captured[0]["status"] == "buy"
    assert captured[0]["source"] == "strategy"
    assert captured[0]["payload"]["signal_type"] == "BUY"
    assert captured[1]["status"] == "sell"


def test_execute_risk_event_id_writes_audit_event_on_fill() -> None:
    import app.execution.paper_broker as paper_broker_mod

    connection = make_connection()
    run_migrations(connection)
    captured: list[dict] = []

    original = paper_broker_mod.insert_event
    paper_broker_mod.insert_event = lambda conn, event_type, status, source, message, payload=None: captured.append(
        {"event_type": event_type, "status": status, "source": source, "payload": payload}
    ) or 1
    try:
        seed_candles(connection, [50000.0] * 5)
        connection.execute(
            "INSERT INTO signals (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma) VALUES (?, ?, ?, ?, ?, ?)",
            ("BTCUSDT", "1m", "ppo", "BUY", 1.0, 0.9),
        )
        connection.execute(
            "INSERT INTO risk_events (signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (1, "BTCUSDT", "1m", "ppo", "BUY", "APPROVED", "ok"),
        )
        connection.commit()
        from app.execution.paper_broker import execute_risk_event_id
        execute_risk_event_id(connection, 1, order_qty=0.001)
    finally:
        paper_broker_mod.insert_event = original
        connection.close()

    assert len(captured) == 1
    assert captured[0]["event_type"] == "order"
    assert captured[0]["status"] == "filled"
    assert captured[0]["source"] == "paper_broker"
    assert captured[0]["payload"]["symbol"] == "BTCUSDT"
    assert captured[0]["payload"]["price"] == 50000.0


def test_execution_job_reconciles_orphan_order_and_emits_audit_event() -> None:
    """reconcile_orphan_orders synthesizes a fill for a paper-mode orphan and emits an audit event."""
    import app.pipeline.execution_job as execution_job_mod

    connection = make_connection()
    run_migrations(connection)
    seed_candles(connection, [50000.0] * 5)
    captured: list[dict] = []

    original = execution_job_mod.insert_event
    execution_job_mod.insert_event = lambda conn, event_type, status, source, message, payload=None: captured.append(
        {"event_type": event_type, "status": status, "source": source, "payload": payload}
    ) or 1
    try:
        connection.execute(
            "INSERT INTO orders (client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("cid-1", None, "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, "NEW"),
        )
        connection.commit()
        from app.pipeline.execution_job import run_execution_job
        run_execution_job(connection)
    finally:
        execution_job_mod.insert_event = original
        connection.close()

    reconciled_events = [e for e in captured if e["event_type"] == "orphan_order_reconciled"]
    assert len(reconciled_events) == 1
    assert reconciled_events[0]["status"] == "reconciled"
    assert reconciled_events[0]["source"] == "execution_job"
    assert reconciled_events[0]["payload"]["fill_price"] == 50000.0


def test_admin_page_is_served() -> None:
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Admin Console" in response.text
    assert "What This System Does" in response.text
    assert "Capability Overview" in response.text
    assert "Market Data" in response.text
    assert "Strategy" in response.text
    assert "Risk" in response.text
    assert "Execution" in response.text
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
    assert 'id="pipeline-symbol-pills"' in response.text
    assert 'id="data-worker-status"' in response.text
    assert 'id="strategy-worker-status"' in response.text
    assert 'id="risk-worker-status"' in response.text
    assert 'id="execution-worker-status"' in response.text
    assert 'id="alerting-runtime-status"' in response.text
    assert 'id="queue-status"' in response.text
    assert 'id="execution-backend-status"' in response.text
    assert 'id="execution-backend-detail"' in response.text
    assert 'id="execution-backend-select"' in response.text
    assert 'data-action="execution-backend-save"' in response.text
    assert 'id="queue-json"' in response.text
    assert 'id="queue-message"' in response.text
    assert 'id="queue-board"' in response.text
    assert 'id="queue-filter-select"' in response.text
    assert 'data-action="queue-recover-pipeline"' in response.text
    assert 'data-action="queue-clear-pipeline"' in response.text
    assert 'data-action="queue-enqueue-strategy"' in response.text
    assert 'data-action="queue-drain-strategy"' in response.text
    assert 'data-action="queue-drain-risk"' in response.text
    assert 'data-action="queue-drain-execution"' in response.text
    assert 'data-action="queue-retry-strategy"' in response.text
    assert 'data-action="queue-retry-risk"' in response.text
    assert 'data-action="queue-retry-execution"' in response.text
    assert "avg attempts=" in response.text
    assert "fail%=" in response.text
    assert "retries=" in response.text
    assert "failure streak=" in response.text
    assert "recent failed=" in response.text
    assert "recent retries=" in response.text
    assert "latest_failed=#" in response.text
    assert "latest_retry=#" in response.text
    assert "trend=" in response.text
    assert "batch=" in response.text
    assert "Incomplete batch:" in response.text
    assert "Completed batch:" in response.text
    assert "Queue Debug" in response.text
    assert "Latest failed:" in response.text
    assert "Latest retry:" in response.text
    assert "Enqueue Strategy Job" in response.text
    assert "Recover Stale Pipeline Batch" in response.text
    assert "Clear Stale Pipeline Batch" in response.text
    assert "Drain Strategy Job" in response.text
    assert "Drain Execution Job" in response.text
    assert "Retry Failed Strategy Job" in response.text
    assert "Retry Failed Execution Job" in response.text
    assert '<option value="failed">failed only</option>' in response.text
    assert '<option value="queued">queued only</option>' in response.text
    assert '<option value="market_data">market_data</option>' in response.text
    assert '<option value="strategy">strategy</option>' in response.text
    assert '<option value="execution">execution</option>' in response.text
    assert 'id="logs-mode-select"' in response.text
    assert 'id="pipeline-strategy-select"' in response.text
    assert 'id="scheduler-strategy-pills"' in response.text
    assert 'id="scheduler-disabled-strategy-pills"' in response.text
    assert 'id="scheduler-symbol-pills"' in response.text
    assert 'id="scheduler-priority-controls"' in response.text
    assert 'id="scheduler-disabled-note-controls"' in response.text
    assert 'id="scheduler-effective-limit-input"' in response.text
    assert "symbols:" in response.text
    assert 'data-action="scheduler-preset-top1"' in response.text
    assert 'data-action="scheduler-preset-top2"' in response.text
    assert 'data-action="scheduler-preset-all"' in response.text
    assert 'data-action="scheduler-priority-sequential"' in response.text
    assert 'Sequential' in response.text
    assert 'data-action="scheduler-priority-reverse"' in response.text
    assert 'Reverse' in response.text
    assert 'data-action="scheduler-priority-active-first"' in response.text
    assert 'Active first' in response.text
    assert 'data-action="scheduler-reset-priorities"' in response.text
    assert 'Reset' in response.text
    assert 'data-action="scheduler-clear-notes"' in response.text
    assert 'Clear Notes' in response.text
    assert 'id="scheduler-preset-detail"' in response.text
    assert "Limit presets change how many enabled strategies run." in response.text
    assert "Priority presets reorder the scheduler execution sequence." in response.text
    assert 'id="strategy-summary-board"' in response.text
    assert 'data-strategy-name="' in response.text
    assert 'data-promote-strategy="' in response.text
    assert 'data-demote-strategy="' in response.text
    assert 'Demote' in response.text
    assert 'data-disable-strategy="' in response.text
    assert 'data-enable-strategy="' in response.text
    assert 'id="strategy-closed-trades-board"' in response.text
    assert 'id="selected-strategy-board"' in response.text
    assert 'id="strategy-closed-trades-board"' in response.text
    assert 'id="closed-trades-strategy-select"' in response.text
    assert 'id="closed-trades-reset-button"' in response.text
    assert 'id="scheduler-control-board"' in response.text
    assert 'id="scheduler-preset-quick-actions"' in response.text
    assert 'id="scheduler-control-filter-select"' in response.text
    assert 'id="scheduler-control-reset-button"' in response.text
    assert '<option value="priority">priority</option>' in response.text
    assert '<option value="limit">limit</option>' in response.text
    assert '<option value="enable_disable">enable/disable</option>' in response.text
    assert "Reset" in response.text
    assert "Control Activity" in response.text
    assert "Recent scheduler and execution backend operations extracted from structured audit actions." in response.text
    assert "LATEST" in response.text
    assert "Copy Action" in response.text
    assert 'data-copy-scheduler-action="' in response.text
    assert "Replay Preset" in response.text
    assert 'data-replay-scheduler-preset="' in response.text
    assert "Recent presets loading..." in response.text
    assert 'id="scheduler-detail"' in response.text
    assert "effective order:" in response.text
    assert "limit:" in response.text
    assert "excluded by limit:" in response.text
    assert "disabled notes:" in response.text
    assert "warning: no enabled active strategies" in response.text
    assert "/scheduler/strategy" in response.text
    assert 'id="issue-strip"' in response.text
    assert 'id="pipeline-status"' in response.text
    assert 'id="pipeline-symbols"' in response.text
    assert 'id="pipeline-counts"' in response.text
    assert "ppo" in response.text
    assert "Last Pipeline" in response.text
    assert "Send Test Alert" in response.text
    assert "Soak Validation" in response.text
    assert "Record Snapshot" in response.text
    assert "top-1" in response.text
    assert "top-2" in response.text
    assert "all" in response.text
    assert "Latest Closed Symbol" in response.text
    assert "Latest Closed PnL" in response.text
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
    assert "Latest Fill At" in response.text
    assert "strategy-card clickable" in response.text
    assert "STRATEGY_STALE_AFTER_MINUTES" in response.text
    assert "FRESH" in response.text
    assert "STALE" in response.text
    assert "IDLE" in response.text


def test_queue_summary_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        "app.api.routes.queue.get_job_queue_summary",
        lambda connection: {
            "counts": {"queued": 2, "leased": 1, "completed": 4, "failed": 1, "total": 8},
            "metrics": {
                "success_ratio": 0.5,
                "failure_ratio": 0.125,
                "avg_attempt_count": 1.38,
                "max_attempt_count": 3,
                "retry_job_count": 2,
                "failure_streak": 1,
                "recent_failure_count": 1,
                "recent_retry_count": 1,
            },
            "job_type_counts": {
                "market_data": {
                    "queued": 0,
                    "leased": 0,
                    "completed": 1,
                    "failed": 0,
                    "total": 1,
                    "success_ratio": 1.0,
                    "failure_ratio": 0.0,
                    "avg_attempt_count": 1.0,
                    "max_attempt_count": 1,
                    "latest_failed_job": None,
                    "latest_retry_job": None,
                    "recent_terminal_statuses": ["C"],
                    "recent_terminal_trend": "C",
                },
                "strategy": {
                    "queued": 2,
                    "leased": 1,
                    "completed": 2,
                    "failed": 1,
                    "total": 6,
                    "success_ratio": 0.3333,
                    "failure_ratio": 0.1667,
                    "avg_attempt_count": 1.67,
                    "max_attempt_count": 3,
                    "latest_failed_job": {"id": 9, "job_type": "strategy", "status": "failed", "attempt_count": 3},
                    "latest_retry_job": {"id": 8, "job_type": "strategy", "status": "completed", "attempt_count": 2},
                    "recent_terminal_statuses": ["F", "C"],
                    "recent_terminal_trend": "FC",
                },
                "execution": {
                    "queued": 0,
                    "leased": 0,
                    "completed": 1,
                    "failed": 0,
                    "total": 1,
                    "success_ratio": 1.0,
                    "failure_ratio": 0.0,
                    "avg_attempt_count": 1.0,
                    "max_attempt_count": 1,
                    "latest_failed_job": None,
                    "latest_retry_job": None,
                    "recent_terminal_statuses": ["C"],
                    "recent_terminal_trend": "C",
                },
                "risk": {
                    "queued": 1,
                    "leased": 0,
                    "completed": 0,
                    "failed": 0,
                    "total": 1,
                    "success_ratio": 0.0,
                    "failure_ratio": 0.0,
                    "avg_attempt_count": 1.0,
                    "max_attempt_count": 1,
                    "latest_failed_job": None,
                    "latest_retry_job": None,
                    "recent_terminal_statuses": [],
                    "recent_terminal_trend": None,
                },
            },
            "recent_batches": [
                {
                    "batch_id": "batch-1234",
                    "job_types": ["market_data", "strategy", "risk", "execution"],
                    "statuses": {
                        "market_data": "completed",
                        "strategy": "queued",
                        "risk": "queued",
                        "execution": "queued",
                    },
                    "strategy_names": ["ppo", "ppo"],
                    "symbol_names": ["BTCUSDT", "ETHUSDT"],
                    "execution_backend": "paper",
                    "source": "api_pipeline",
                    "orchestration": "queue_batch",
                }
            ],
            "latest_incomplete_batch": {
                "batch_id": "batch-1234",
                "job_types": ["market_data", "strategy", "risk", "execution"],
                "statuses": {
                    "market_data": "completed",
                    "strategy": "queued",
                    "risk": "queued",
                    "execution": "queued",
                },
                "strategy_names": ["ppo", "ppo"],
                "symbol_names": ["BTCUSDT", "ETHUSDT"],
                "execution_backend": "paper",
                "source": "api_pipeline",
                "orchestration": "queue_batch",
            },
            "latest_completed_batch": {
                "batch_id": "batch-0001",
                "job_types": ["market_data", "strategy", "risk", "execution"],
                "statuses": {
                    "market_data": "completed",
                    "strategy": "completed",
                    "risk": "completed",
                    "execution": "completed",
                },
                "strategy_names": ["ppo"],
                "symbol_names": ["BTCUSDT"],
                "execution_backend": "paper",
                "source": "scheduler_pipeline",
                "orchestration": "queue_dispatch",
            },
            "failed_jobs": [{"id": 9, "job_type": "strategy", "status": "failed"}],
            "retry_jobs": [{"id": 8, "job_type": "strategy", "status": "completed", "attempt_count": 2}],
            "latest_failed_job": {
                "id": 9,
                "job_type": "strategy",
                "status": "failed",
                "attempt_count": 3,
                "error_message": "strategy failed",
            },
            "latest_retry_job": {
                "id": 8,
                "job_type": "strategy",
                "status": "completed",
                "attempt_count": 2,
            },
            "latest_jobs": [{"id": 9, "job_type": "strategy", "status": "failed"}],
        },
    )

    response = client.get("/queue/summary")

    assert response.status_code == 200
    assert response.json()["counts"]["queued"] == 2
    assert response.json()["metrics"]["retry_job_count"] == 2
    assert response.json()["metrics"]["max_attempt_count"] == 3
    assert response.json()["metrics"]["failure_streak"] == 1
    assert response.json()["metrics"]["recent_failure_count"] == 1
    assert response.json()["job_type_counts"]["strategy"]["failed"] == 1
    assert response.json()["job_type_counts"]["strategy"]["failure_ratio"] == 0.1667
    assert response.json()["job_type_counts"]["strategy"]["latest_failed_job"]["id"] == 9
    assert response.json()["job_type_counts"]["strategy"]["latest_retry_job"]["id"] == 8
    assert response.json()["job_type_counts"]["strategy"]["recent_terminal_statuses"] == ["F", "C"]
    assert response.json()["job_type_counts"]["strategy"]["recent_terminal_trend"] == "FC"
    assert response.json()["failed_jobs"][0]["id"] == 9
    assert response.json()["retry_jobs"][0]["attempt_count"] == 2
    assert response.json()["latest_failed_job"]["error_message"] == "strategy failed"
    assert response.json()["latest_retry_job"]["id"] == 8
    assert response.json()["recent_batches"][0]["statuses"]["market_data"] == "completed"
    assert response.json()["recent_batches"][0]["execution_backend"] == "paper"
    assert response.json()["recent_batches"][0]["source"] == "api_pipeline"
    assert response.json()["recent_batches"][0]["orchestration"] == "queue_batch"
    assert response.json()["latest_incomplete_batch"]["statuses"]["strategy"] == "queued"
    assert response.json()["latest_incomplete_batch"]["execution_backend"] == "paper"
    assert response.json()["latest_incomplete_batch"]["source"] == "api_pipeline"
    assert response.json()["latest_completed_batch"]["statuses"]["execution"] == "completed"
    assert response.json()["latest_completed_batch"]["execution_backend"] == "paper"
    assert response.json()["latest_completed_batch"]["orchestration"] == "queue_dispatch"
    assert response.json()["latest_jobs"][0]["job_type"] == "strategy"


def test_get_job_queue_summary_includes_quality_metrics() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        from app.core import job_queue as job_queue_module

        original_status = job_queue_module.get_execution_backend_status
        original_name = job_queue_module.get_execution_adapter_name
        job_queue_module.get_execution_backend_status = lambda: {
            "backend": "paper",
            "description": "Paper broker execution backend.",
            "dry_run": False,
            "can_execute_orders": True,
            "is_live": False,
            "placeholder": False,
            "status": "ok",
        }
        job_queue_module.get_execution_adapter_name = lambda: "paper"
        market_job_id = enqueue_job(connection, "market_data", payload={"symbol_names": ["BTCUSDT"]})
        strategy_job_id = enqueue_job(connection, "strategy", payload={"strategy_names": ["ppo"]})
        execution_job_id = enqueue_job(connection, "execution", payload={"symbol_names": ["ETHUSDT"]})

        leased_market_job = lease_next_job(connection, job_type="market_data")
        assert leased_market_job is not None
        complete_job(connection, market_job_id, result={"saved_klines": 5})

        leased_strategy_job = lease_next_job(connection, job_type="strategy")
        assert leased_strategy_job is not None
        fail_job(connection, strategy_job_id, "strategy failed")
        retried_strategy_job = retry_job(connection, strategy_job_id)
        assert retried_strategy_job["attempt_count"] == 1

        leased_retried_strategy_job = lease_next_job(connection, job_type="strategy")
        assert leased_retried_strategy_job is not None
        fail_job(connection, strategy_job_id, "strategy failed again")

        summary = get_job_queue_summary(connection)

        assert summary["counts"] == {
            "queued": 1,
            "leased": 0,
            "completed": 1,
            "failed": 1,
            "total": 3,
        }
        assert summary["metrics"] == {
            "success_ratio": 0.3333,
            "failure_ratio": 0.3333,
            "avg_attempt_count": 1.0,
            "max_attempt_count": 2,
            "retry_job_count": 1,
            "failure_streak": 1,
            "recent_failure_count": 1,
            "recent_retry_count": 1,
        }
        assert summary["job_type_counts"]["market_data"]["success_ratio"] == 1.0
        assert summary["job_type_counts"]["strategy"]["failure_ratio"] == 1.0
        assert summary["job_type_counts"]["strategy"]["avg_attempt_count"] == 2.0
        assert summary["job_type_counts"]["strategy"]["max_attempt_count"] == 2
        assert summary["job_type_counts"]["strategy"]["latest_failed_job"]["id"] == strategy_job_id
        assert summary["job_type_counts"]["strategy"]["latest_retry_job"]["id"] == strategy_job_id
        assert summary["job_type_counts"]["strategy"]["recent_terminal_statuses"] == ["F"]
        assert summary["job_type_counts"]["strategy"]["recent_terminal_trend"] == "F"
        assert summary["job_type_counts"]["market_data"]["recent_terminal_statuses"] == ["C"]
        assert summary["job_type_counts"]["market_data"]["recent_terminal_trend"] == "C"
        assert summary["job_type_counts"]["market_data"]["latest_failed_job"] is None
        assert summary["job_type_counts"]["execution"]["total"] == 1
        assert summary["retry_jobs"][0]["id"] == strategy_job_id
        assert summary["retry_jobs"][0]["attempt_count"] == 2
        assert summary["failed_jobs"][0]["id"] == strategy_job_id
        assert summary["latest_failed_job"]["id"] == strategy_job_id
        assert summary["latest_failed_job"]["error_message"] == "strategy failed again"
        assert summary["latest_retry_job"]["id"] == strategy_job_id
        assert summary["latest_retry_job"]["attempt_count"] == 2
        assert summary["recent_batches"] == []
        assert summary["latest_incomplete_batch"] is None
        assert summary["latest_completed_batch"] is None
        assert summary["latest_jobs"][0]["payload"]["execution_backend"] == "paper"
        assert execution_job_id in [job["id"] for job in summary["latest_jobs"]]
    finally:
        if "original_status" in locals():
            job_queue_module.get_execution_backend_status = original_status
        if "original_name" in locals():
            job_queue_module.get_execution_adapter_name = original_name
        connection.close()


def test_get_job_queue_summary_includes_batch_source_and_orchestration() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["BTCUSDT"],
            payload={"source": "api_pipeline", "orchestration": "queue_batch"},
        )

        summary = get_job_queue_summary(connection)

        assert summary["recent_batches"][0]["source"] == "api_pipeline"
        assert summary["recent_batches"][0]["orchestration"] == "queue_batch"
        assert summary["latest_incomplete_batch"]["source"] == "api_pipeline"
        assert summary["latest_incomplete_batch"]["orchestration"] == "queue_batch"
    finally:
        connection.close()


def test_get_job_queue_summary_includes_batch_age_seconds(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["BTCUSDT"],
            payload={"source": "api_pipeline", "orchestration": "queue_batch"},
        )
        connection.execute(
            "UPDATE job_queue SET created_at = ? WHERE payload_json LIKE ?",
            ("2026-03-19 10:00:00", '%"batch_id"%'),
        )
        connection.commit()
        monkeypatch.setattr("app.query.job_queue_summary.utc_now", lambda: datetime(2026, 3, 19, 10, 5, 0, tzinfo=timezone.utc))

        summary = get_job_queue_summary(connection)

        assert summary["recent_batches"][0]["created_at"] == "2026-03-19 10:00:00"
        assert summary["recent_batches"][0]["age_seconds"] == 300
        assert summary["latest_incomplete_batch"]["age_seconds"] == 300
    finally:
        connection.close()


def test_get_job_queue_summary_tolerates_non_json_payload_rows() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO job_queue (job_type, status, payload_json, result_json, error_message, attempt_count)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            ("pipeline_run", "completed", "pipeline", "Pipeline run completed.", None, 1),
        )
        connection.commit()

        summary = get_job_queue_summary(connection)

        assert summary["latest_jobs"][0]["job_type"] == "pipeline_run"
        assert summary["latest_jobs"][0]["payload"] is None
        assert summary["latest_jobs"][0]["result"] is None
    finally:
        connection.close()


def test_build_health_report_includes_queue_summary(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.health.checks.get_connection",
        lambda: sqlite3.connect(":memory:"),
    )
    monkeypatch.setattr(
        "app.health.checks.get_database_info",
        lambda: {"backend": "sqlite", "sqlite_path": ":memory:"},
    )
    monkeypatch.setattr("app.health.checks.database_check", lambda connection: {"status": "ok"})
    monkeypatch.setattr("app.health.checks.candle_check", lambda connection: {"status": "ok"})
    monkeypatch.setattr("app.health.checks.pipeline_check", lambda connection: {"status": "ok"})
    monkeypatch.setattr(
        "app.health.checks.queue_check",
        lambda connection: {
            "status": "degraded",
            "counts": {"queued": 1, "leased": 0, "completed": 3, "failed": 1, "total": 5},
            "latest_jobs": [{"id": 4, "job_type": "execution", "status": "failed"}],
            "reason": "Queue contains failed jobs.",
        },
    )
    monkeypatch.setattr("app.health.checks.heartbeat_check", lambda connection: {"status": "ok", "components": []})
    monkeypatch.setattr(
        "app.health.checks.execution_backend_check",
        lambda: {"status": "ok", "backend": "paper", "can_execute_orders": True, "dry_run": False, "placeholder": False},
    )
    monkeypatch.setattr(
        "app.health.checks.broker_protection_check",
        lambda connection, execution_backend_check, pipeline_check: {"status": "ok"},
    )
    monkeypatch.setattr(
        "app.health.checks.get_stop_status",
        lambda: {"stopped": False, "stop_file": "runtime/scheduler.stop"},
    )
    monkeypatch.setattr("app.health.checks.read_scheduler_log", lambda lines=1: [])
    monkeypatch.setattr(
        "app.health.checks.get_kill_switch_status",
        lambda: {"enabled": False, "kill_switch_file": "runtime/kill.switch"},
    )

    report = __import__("app.health.checks", fromlist=["build_health_report"]).build_health_report()

    assert report["status"] == "degraded"
    assert report["checks"]["queue"]["status"] == "degraded"
    assert report["checks"]["queue"]["counts"]["failed"] == 1


def test_queue_check_marks_stale_incomplete_batch_as_degraded(monkeypatch) -> None:
    monkeypatch.setattr("app.health.checks.QUEUE_BATCH_STALENESS_SECONDS", 300)
    monkeypatch.setattr(
        "app.health.checks.get_job_queue_summary",
        lambda connection: {
            "counts": {"queued": 2, "leased": 0, "completed": 2, "failed": 0, "total": 4},
            "latest_failed_job": None,
            "latest_retry_job": None,
            "latest_jobs": [],
            "recent_batches": [
                {
                    "batch_id": "batch-123",
                    "age_seconds": 420,
                    "source": "scheduler_pipeline",
                    "orchestration": "queue_batch",
                    "statuses": {"market_data": "completed", "strategy": "queued", "risk": "queued", "execution": "queued"},
                }
            ],
            "latest_incomplete_batch": {
                "batch_id": "batch-123",
                "age_seconds": 420,
                "source": "scheduler_pipeline",
                "orchestration": "queue_batch",
                "statuses": {"market_data": "completed", "strategy": "queued", "risk": "queued", "execution": "queued"},
            },
            "latest_completed_batch": None,
        },
    )

    result = __import__("app.health.checks", fromlist=["queue_check"]).queue_check(object())

    assert result["status"] == "degraded"
    assert result["reason"] == "Queue contains stale incomplete batches."
    assert result["batch_staleness_threshold_seconds"] == 300
    assert result["latest_incomplete_batch"]["age_seconds"] == 420


def test_heartbeat_check_marks_stale_workers_as_degraded(monkeypatch) -> None:
    fixed_now = datetime(2026, 3, 19, 12, 0, 0, tzinfo=timezone.utc)
    monkeypatch.setattr("app.health.checks.utc_now", lambda: fixed_now)
    monkeypatch.setattr("app.health.checks.WORKER_HEARTBEAT_STALENESS_SECONDS", 60)

    connection = make_connection()
    try:
        run_migrations(connection)
        upsert_heartbeat(connection, "strategy_worker", "ok", "Strategy loop completed.")
        connection.execute(
            "UPDATE runtime_heartbeats SET last_seen_at = ? WHERE component = ?",
            ("2026-03-19 11:58:00", "strategy_worker"),
        )
        connection.commit()

        result = __import__("app.health.checks", fromlist=["heartbeat_check"]).heartbeat_check(connection)

        assert result["status"] == "degraded"
        assert result["reason"] == "Runtime heartbeat contains stale worker components."
        worker_entry = result["components"][0]
        assert worker_entry["component"] == "strategy_worker"
        assert worker_entry["stale"] is True
        assert worker_entry["age_seconds"] == 120
    finally:
        connection.close()


def test_root_redirects_to_admin() -> None:
    client = TestClient(app)

    response = client.get("/", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/admin"


def test_render_admin_page_selects_default_pipeline_orchestration(monkeypatch) -> None:
    monkeypatch.setattr("app.core.settings.DEFAULT_PIPELINE_ORCHESTRATION", "queue_drain")

    html = __import__("app.admin.page", fromlist=["render_admin_page"]).render_admin_page()

    assert '<option value="queue_drain" selected>queue_drain</option>' in html


def test_queue_alert_includes_batch_source_and_orchestration(monkeypatch, tmp_path) -> None:
    sent_messages = []
    monkeypatch.setattr("app.alerting.base.send_telegram_message", lambda text: sent_messages.append(text) or {"sent": True})
    monkeypatch.setattr("app.alerting.queue.QUEUE_ALERT_STATE_FILE", tmp_path / "queue_alert.json")

    result = __import__("app.alerting.queue", fromlist=["maybe_send_queue_alert"]).maybe_send_queue_alert(
        {
            "checks": {
                "queue": {
                    "status": "degraded",
                    "counts": {"failed": 1},
                    "latest_failed_job": {
                        "id": 42,
                        "job_type": "execution",
                        "attempt_count": 2,
                        "error_message": "broker unavailable",
                    },
                    "latest_incomplete_batch": {
                        "batch_id": "batch-123",
                        "source": "api_pipeline",
                        "orchestration": "queue_batch",
                        "age_seconds": 42,
                    },
                }
            }
        }
    )

    assert result["sent"] is True
    assert "source=api_pipeline" in sent_messages[0]
    assert "orchestration=queue_batch" in sent_messages[0]
    assert "batch_age=42s" in sent_messages[0]


def test_queue_alert_sends_for_stale_incomplete_batch_without_failed_jobs(monkeypatch, tmp_path) -> None:
    sent_messages = []
    monkeypatch.setattr("app.alerting.base.send_telegram_message", lambda text: sent_messages.append(text) or {"sent": True})
    monkeypatch.setattr("app.alerting.queue.QUEUE_ALERT_STATE_FILE", tmp_path / "queue_alert.json")

    result = __import__("app.alerting.queue", fromlist=["maybe_send_queue_alert"]).maybe_send_queue_alert(
        {
            "checks": {
                "queue": {
                    "status": "degraded",
                    "reason": "Queue contains stale incomplete batches.",
                    "counts": {"failed": 0},
                    "latest_failed_job": None,
                    "latest_incomplete_batch": {
                        "batch_id": "batch-123",
                        "source": "scheduler_pipeline",
                        "orchestration": "queue_batch",
                        "age_seconds": 601,
                    },
                }
            }
        }
    )

    assert result["sent"] is True
    assert "stale incomplete batch" in sent_messages[0]
    assert "source=scheduler_pipeline" in sent_messages[0]
    assert "orchestration=queue_batch" in sent_messages[0]
    assert "batch_age=601s" in sent_messages[0]


