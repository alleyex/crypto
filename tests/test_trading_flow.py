import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from fastapi.testclient import TestClient

from app.api.main import app
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import save_klines
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
from app.pipeline.run_pipeline import run_pipeline_collect
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_fills
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
        ensure_signals_table(connection)
        ensure_positions_table(connection)
        ensure_risk_table(connection)
        connection.execute(
            """
            INSERT INTO positions (symbol, qty, avg_price, realized_pnl)
            VALUES (?, ?, ?, ?);
            """,
            ("BTCUSDT", 0.0, 0.0, -75.0),
        )
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


def test_admin_page_is_served() -> None:
    client = TestClient(app)

    response = client.get("/admin")

    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "Admin Console" in response.text
    assert "/pipeline/run" in response.text


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
