import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app
from app.core.db import get_database_info
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import save_klines
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
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
from app.risk.risk_service import ensure_table as ensure_risk_table
from app.risk.risk_service import evaluate_latest_signal
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.ma_cross import insert_signal
from app.strategy.ma_cross import generate_signal
from app.system.kill_switch import disable_kill_switch
from app.system.kill_switch import enable_kill_switch
from app.scheduler.runner import run_scheduler
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
            ("buy-daily-1", 1, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED", "2026-03-18 10:00:00"),
        )
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-daily-1", 2, "BTCUSDT", "1m", "manual_test", "SELL", 1.0, 25.0, "FILLED", "2026-03-18 10:05:00"),
        )
        insert_fill(connection, 1, "BTCUSDT", "BUY", 1.0, 100.0, "2026-03-18 10:00:00")
        insert_fill(connection, 2, "BTCUSDT", "SELL", 1.0, 25.0, "2026-03-18 10:05:00")
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
            ("buy-daily-2", 1, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED", "2026-03-18 11:00:00"),
        )
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-daily-2", 2, "BTCUSDT", "1m", "manual_test", "SELL", 1.0, 25.0, "FILLED", "2026-03-18 11:05:00"),
        )
        insert_fill(connection, 1, "BTCUSDT", "BUY", 1.0, 100.0, "2026-03-18 11:00:00")
        insert_fill(connection, 2, "BTCUSDT", "SELL", 1.0, 25.0, "2026-03-18 11:05:00")
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
            ("buy-prev-day", 1, "BTCUSDT", "1m", "manual_test", "BUY", 1.0, 100.0, "FILLED", "2026-03-17 10:00:00"),
        )
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            ("sell-prev-day", 2, "BTCUSDT", "1m", "manual_test", "SELL", 1.0, 25.0, "FILLED", "2026-03-17 10:05:00"),
        )
        insert_fill(connection, 1, "BTCUSDT", "BUY", 1.0, 100.0, "2026-03-17 10:00:00")
        insert_fill(connection, 2, "BTCUSDT", "SELL", 1.0, 25.0, "2026-03-17 10:05:00")
        connection.commit()

        assert get_daily_realized_pnl(connection, "BTCUSDT", pnl_date="2026-03-17") == -75.0

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
        "app.pipeline.run_pipeline.fetch_klines",
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


def test_admin_page_is_served() -> None:
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Admin Console" in response.text
    assert "/pipeline/run" in response.text
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
    assert 'id="alerting-runtime-status"' in response.text
    assert 'id="issue-strip"' in response.text
    assert 'id="pipeline-status"' in response.text
    assert "Last Pipeline" in response.text
    assert "Send Test Alert" in response.text
    assert "Soak Validation" in response.text
    assert "Record Snapshot" in response.text


def test_root_redirects_to_admin() -> None:
    client = TestClient(app)

    response = client.get("/", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "/admin"


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
        "app.pipeline.run_pipeline.fetch_klines",
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


def test_run_scheduler_records_soak_snapshot(monkeypatch, tmp_path) -> None:
    log_path = tmp_path / "scheduler.log"
    db_path = tmp_path / "scheduler-heartbeat.db"
    recorded = []
    monkeypatch.setattr("app.scheduler.runner.LOG_FILE", log_path)
    monkeypatch.setattr("app.scheduler.runner.stop_requested", lambda: False)
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.scheduler.runner.run_pipeline_collect",
        lambda: {
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
    assert "run=1 signal=BUY risk=APPROVED execution=FILLED BUY" in log_text
    assert "soak_snapshot status=ok" in log_text

    connection = sqlite3.connect(db_path)
    try:
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()
    assert any(item["component"] == "scheduler" and item["status"] == "ok" for item in heartbeats)


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
