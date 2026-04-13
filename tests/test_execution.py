import json
import sqlite3
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.db import get_table_columns
from app.core.migrations import run_migrations
from app.data.candles_service import save_klines
from app.execution.adapter import (
    NoopExecutionAdapter,
    SimulatedLiveExecutionAdapter,
    get_execution_adapter_name,
    get_execution_backend_status,
)
from app.execution.binance_broker import BinanceAPIError
from app.execution import live_broker
from app.execution.live_broker import SimulatedBrokerClient
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
from app.execution.paper_broker import execute_pending_approved_risks
from app.execution.runtime import get_execution_backend_runtime_status
from app.execution.runtime import set_execution_backend
from app.pipeline.execution_job import run_execution_job
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_audit_events
from app.query.read_service import get_fills
from app.query.read_service import get_orders
from app.query.read_service import get_positions
from app.risk.risk_service import evaluate_signal_id
from app.strategy.signal_service import insert_signal
from conftest import make_connection, make_kline


def seed_candles(connection: sqlite3.Connection, closes: list[float]) -> None:
    run_migrations(connection)
    klines = [make_kline((index + 1) * 60_000, close) for index, close in enumerate(closes)]
    save_klines(connection, klines)

def test_run_execution_job_executes_multiple_pending_risk_events() -> None:
    connection = make_connection()
    try:
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
        evaluate_signal_id(connection, int(btc_signal["id"]), cooldown_seconds=0)
        evaluate_signal_id(connection, int(eth_signal["id"]), cooldown_seconds=0)

        execution_result = run_execution_job(connection)

        assert [step["step"] for step in execution_result["steps"]] == [
            "paper_execute",
            "paper_execute",
            "update_positions",
            "update_pnl",
            "reconcile_orphan_orders",
        ]
        assert [step["symbol"] for step in execution_result["steps"][:2]] == ["BTCUSDT", "ETHUSDT"]
    finally:
        connection.close()


def test_run_execution_job_with_selected_risk_events_only_executes_current_run() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        ensure_execution_tables(connection)

        save_klines(connection, [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])])
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, close) for index, close in enumerate([20, 21, 22, 23, 24])],
            symbol="ETHUSDT",
        )
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, close) for index, close in enumerate([30, 31, 32, 33, 34])],
            symbol="SOLUSDT",
        )

        existing_signal = insert_signal(connection, "BUY", symbol="SOLUSDT", strategy_name="manual_test")
        existing_risk = evaluate_signal_id(connection, int(existing_signal["id"]), cooldown_seconds=0)
        assert existing_risk is not None

        btc_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        eth_signal = insert_signal(connection, "BUY", symbol="ETHUSDT", strategy_name="manual_test")
        btc_risk = evaluate_signal_id(connection, int(btc_signal["id"]), cooldown_seconds=0)
        eth_risk = evaluate_signal_id(connection, int(eth_signal["id"]), cooldown_seconds=0)
        assert btc_risk is not None
        assert eth_risk is not None

        execution_result = run_execution_job(connection, risk_event_ids=[btc_risk["id"], eth_risk["id"]])

        assert [step["step"] for step in execution_result["steps"]] == [
            "paper_execute",
            "paper_execute",
            "update_positions",
            "update_pnl",
            "reconcile_orphan_orders",
        ]
        assert [step["symbol"] for step in execution_result["steps"][:2]] == ["BTCUSDT", "ETHUSDT"]
        order_symbols = [order["symbol"] for order in get_orders(connection, limit=10)]
        assert order_symbols == ["ETHUSDT", "BTCUSDT"]
    finally:
        connection.close()


def test_run_execution_job_with_selected_symbols_only_executes_matching_pending_risks() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        ensure_execution_tables(connection)

        save_klines(connection, [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])])
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, close) for index, close in enumerate([20, 21, 22, 23, 24])],
            symbol="ETHUSDT",
        )
        save_klines(
            connection,
            [make_kline((index + 1) * 60_000, close) for index, close in enumerate([30, 31, 32, 33, 34])],
            symbol="SOLUSDT",
        )

        btc_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        eth_signal = insert_signal(connection, "BUY", symbol="ETHUSDT", strategy_name="manual_test")
        sol_signal = insert_signal(connection, "BUY", symbol="SOLUSDT", strategy_name="manual_test")
        evaluate_signal_id(connection, int(btc_signal["id"]), cooldown_seconds=0)
        evaluate_signal_id(connection, int(eth_signal["id"]), cooldown_seconds=0)
        evaluate_signal_id(connection, int(sol_signal["id"]), cooldown_seconds=0)

        execution_result = run_execution_job(connection, symbol_names=["BTCUSDT", "ETHUSDT"])

        assert [step["step"] for step in execution_result["steps"]] == [
            "paper_execute",
            "paper_execute",
            "update_positions",
            "update_pnl",
            "reconcile_orphan_orders",
        ]
        assert [step["symbol"] for step in execution_result["steps"][:2]] == ["BTCUSDT", "ETHUSDT"]
        order_symbols = [order["symbol"] for order in get_orders(connection, limit=10)]
        assert order_symbols == ["ETHUSDT", "BTCUSDT"]
    finally:
        connection.close()


def test_run_execution_job_uses_execution_adapter(monkeypatch) -> None:
    connection = make_connection()
    adapter_calls: list[tuple[str, object]] = []

    class FakeExecutionAdapter:
        name = "fake"
        is_live = False

        def ensure_tables(self, conn) -> None:
            adapter_calls.append(("ensure_tables", conn))

        def execute_risk_event_ids(self, conn, risk_event_ids, order_qty=0.001):
            adapter_calls.append(("execute_risk_event_ids", list(risk_event_ids)))
            return [{"status": "FILLED", "symbol": "BTCUSDT", "risk_event_id": 1, "order_id": 7}]

        def execute_pending_approved_risks(self, conn, order_qty=0.001, symbol_names=None):
            adapter_calls.append(("execute_pending_approved_risks", list(symbol_names or [])))
            return []

        def execute_latest_risk(self, conn, order_qty=0.001):
            adapter_calls.append(("execute_latest_risk", conn))
            return None

    try:
        monkeypatch.setattr("app.pipeline.execution_job.get_execution_adapter", lambda: FakeExecutionAdapter())
        monkeypatch.setattr("app.pipeline.execution_job.update_positions", lambda conn: ["BTCUSDT"])
        monkeypatch.setattr("app.pipeline.execution_job.update_pnl_snapshots", lambda conn: 1)

        execution_result = run_execution_job(connection, risk_event_ids=[1, 2])

        assert adapter_calls == [
            ("ensure_tables", connection),
            ("execute_risk_event_ids", [1, 2]),
        ]
        assert execution_result["steps"] == [
            {"step": "paper_execute", "status": "FILLED", "symbol": "BTCUSDT", "risk_event_id": 1, "order_id": 7},
            {"step": "update_positions", "updated_symbols": ["BTCUSDT"]},
            {"step": "update_pnl", "snapshot_count": 1},
            {"step": "reconcile_orphan_orders", "status": "ok", "reconciled_count": 0},
        ]
    finally:
        connection.close()


def test_scan_orphan_orders_detects_unfilled_order() -> None:
    """scan_orphan_orders returns orders that have no fill and are not terminal."""
    from app.pipeline.execution_job import scan_orphan_orders
    from app.core.migrations import run_migrations

    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            "INSERT INTO orders (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status, risk_event_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            ("test-coid-1", "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, "FILLED", 1),
        )
        connection.commit()

        orphans = scan_orphan_orders(connection)

        assert len(orphans) == 1
        assert orphans[0]["symbol"] == "BTCUSDT"
        assert orphans[0]["side"] == "BUY"
        assert orphans[0]["status"] == "FILLED"
    finally:
        connection.close()


def test_scan_orphan_orders_ignores_terminal_orders() -> None:
    """Orders with CANCELLED/REJECTED/EXPIRED status must not appear as orphans."""
    from app.pipeline.execution_job import scan_orphan_orders
    from app.core.migrations import run_migrations

    connection = make_connection()
    try:
        run_migrations(connection)
        for idx, status in enumerate(("CANCELLED", "REJECTED", "EXPIRED")):
            connection.execute(
                "INSERT INTO orders (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status, risk_event_id)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
                (f"test-coid-{idx}", "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, status, idx + 1),
            )
        connection.commit()

        orphans = scan_orphan_orders(connection)

        assert orphans == []
    finally:
        connection.close()


def test_execution_job_reconcile_step_synthesizes_fill_for_orphan() -> None:
    """run_execution_job reconciles orphan order by creating a fill and rebuilding PnL."""
    from app.core.migrations import run_migrations

    connection = make_connection()
    try:
        run_migrations(connection)
        seed_candles(connection, [50000.0] * 5)
        connection.execute(
            "INSERT INTO orders (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status, risk_event_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            ("test-coid-orphan", "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, "FILLED", 1),
        )
        connection.commit()

        result = run_execution_job(connection)

        reconcile_step = next(s for s in result["steps"] if s["step"] == "reconcile_orphan_orders")
        assert reconcile_step["status"] == "reconciled"
        assert reconcile_step["reconciled_count"] == 1
        assert reconcile_step["results"][0]["action"] == "fill_synthesized"
        assert reconcile_step["results"][0]["fill_price"] == 50000.0

        # Verify fill was actually inserted
        fill_count = connection.execute("SELECT COUNT(*) FROM fills;").fetchone()[0]
        assert fill_count == 1
    finally:
        connection.close()


def test_execution_job_reconcile_step_skips_orphan_when_no_candle_data() -> None:
    """If no candle data is available, orphan reconcile is skipped gracefully."""
    from app.core.migrations import run_migrations

    connection = make_connection()
    try:
        run_migrations(connection)
        # No candles seeded
        connection.execute(
            "INSERT INTO orders (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status, risk_event_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            ("test-coid-nocandle", "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, "FILLED", 1),
        )
        connection.commit()

        result = run_execution_job(connection)

        reconcile_step = next(s for s in result["steps"] if s["step"] == "reconcile_orphan_orders")
        assert reconcile_step["reconciled_count"] == 1
        assert reconcile_step["results"][0]["action"] == "skipped"
        assert reconcile_step["results"][0]["reason"] == "no_candle_data"
        # No fill should have been inserted
        fill_count = connection.execute("SELECT COUNT(*) FROM fills;").fetchone()[0]
        assert fill_count == 0
    finally:
        connection.close()


def test_execution_job_reconcile_flags_live_orphan_for_manual_review() -> None:
    """For live backends, orphan orders are flagged for manual review, not auto-filled."""
    from app.core.migrations import run_migrations
    from app.pipeline.execution_job import reconcile_orphan_orders

    connection = make_connection()
    captured_events: list[dict] = []
    import app.pipeline.execution_job as ejmod
    original = ejmod.insert_event
    ejmod.insert_event = lambda conn, event_type, status, source, message, payload=None: captured_events.append(
        {"event_type": event_type, "status": status}
    ) or 1
    try:
        run_migrations(connection)
        seed_candles(connection, [50000.0] * 5)
        connection.execute(
            "INSERT INTO orders (client_order_id, symbol, timeframe, strategy_name, side, qty, price, status, risk_event_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);",
            ("test-live-orphan", "BTCUSDT", "1m", "ppo", "BUY", 0.001, 50000.0, "NEW", 1),
        )
        connection.commit()

        results = reconcile_orphan_orders(connection, is_live=True)

        assert len(results) == 1
        assert results[0]["action"] == "flagged_for_manual_review"
        # No fill should be inserted for live backends
        fill_count = connection.execute("SELECT COUNT(*) FROM fills;").fetchone()[0]
        assert fill_count == 0
        live_events = [e for e in captured_events if e["event_type"] == "orphan_order_live"]
        assert len(live_events) == 1
        assert live_events[0]["status"] == "critical"
    finally:
        ejmod.insert_event = original
        connection.close()


def test_noop_execution_adapter_skips_pending_risks() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        ensure_execution_tables(connection)

        save_klines(connection, [make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])])
        btc_signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        btc_risk = evaluate_signal_id(connection, int(btc_signal["id"]), cooldown_seconds=0)
        assert btc_risk is not None

        adapter = NoopExecutionAdapter()
        execution_results = adapter.execute_pending_approved_risks(connection, symbol_names=["BTCUSDT"])

        assert execution_results == [
            {
                "risk_event_id": int(btc_risk["id"]),
                "decision": "SKIPPED",
                "reason": "Execution backend noop",
            }
        ]
        assert get_orders(connection, limit=10) == []
    finally:
        connection.close()


def test_get_execution_adapter_name_reads_runtime_backend(monkeypatch) -> None:
    monkeypatch.setattr("app.execution.runtime.EXECUTION_BACKEND", "noop")
    assert get_execution_adapter_name() == "noop"


def test_get_execution_backend_status_reports_capabilities(monkeypatch) -> None:
    monkeypatch.setattr("app.execution.runtime.EXECUTION_BACKEND", "noop")
    assert get_execution_backend_status() == {
        "backend": "noop",
        "description": "No-op execution backend for dry-run validation.",
        "dry_run": True,
        "can_execute_orders": False,
        "is_live": False,
        "placeholder": False,
        "status": "ok",
    }


def test_get_execution_backend_status_supports_simulated_live(monkeypatch) -> None:
    monkeypatch.setattr("app.execution.runtime.EXECUTION_BACKEND", "simulated_live")
    assert get_execution_backend_status() == {
        "backend": "simulated_live",
        "description": "Live-style execution backend backed by a simulated broker client.",
        "dry_run": False,
        "can_execute_orders": True,
        "is_live": False,
        "placeholder": False,
        "status": "ok",
    }


def test_get_execution_backend_status_supports_binance(monkeypatch) -> None:
    monkeypatch.setattr("app.execution.runtime.EXECUTION_BACKEND", "binance")
    monkeypatch.setattr("app.core.settings.BINANCE_API_KEY", "test-key")
    monkeypatch.setattr("app.core.settings.BINANCE_API_SECRET", "test-secret")
    status = get_execution_backend_status()
    assert status["backend"] == "binance"
    assert status["is_live"] is True
    assert status["can_execute_orders"] is True
    assert status["dry_run"] is False
    assert status["placeholder"] is False
    assert status["status"] == "ok"


def test_binance_broker_client_raises_without_credentials() -> None:
    from app.execution.binance_broker import BinanceBrokerClient

    client = BinanceBrokerClient(api_key="", api_secret="", testnet=True)
    try:
        client.place_order(symbol="BTCUSDT", side="BUY", qty=0.001, ref_price=50000.0)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "CRYPTO_BINANCE_API_KEY" in str(exc)


def test_binance_broker_client_uses_testnet_url_by_default() -> None:
    from app.execution.binance_broker import BinanceBrokerClient, TESTNET_BASE_URL, MAINNET_BASE_URL

    testnet_client = BinanceBrokerClient(api_key="k", api_secret="s", testnet=True)
    mainnet_client = BinanceBrokerClient(api_key="k", api_secret="s", testnet=False)
    assert testnet_client._base_url == TESTNET_BASE_URL
    assert mainnet_client._base_url == MAINNET_BASE_URL


def test_binance_broker_client_place_order_calls_api(monkeypatch) -> None:
    from app.execution.binance_broker import BinanceBrokerClient

    posted: list = []

    class FakeResponse:
        def raise_for_status(self): pass
        def json(self):
            return {
                "orderId": 99,
                "status": "FILLED",
                "executedQty": "0.001",
                "fills": [{"price": "51000.0", "qty": "0.001"}],
            }

    def fake_request(method, url, headers=None, timeout=None):
        posted.append((method, url))
        return FakeResponse()

    monkeypatch.setattr("app.execution.binance_broker.requests.request", fake_request)

    client = BinanceBrokerClient(api_key="test-key", api_secret="test-secret", testnet=True)
    result = client.place_order(symbol="BTCUSDT", side="BUY", qty=0.001, ref_price=50000.0)

    assert len(posted) == 1
    assert posted[0][0] == "POST"
    assert "testnet.binance.vision" in posted[0][1]
    assert "BTCUSDT" in posted[0][1]
    assert "BUY" in posted[0][1]
    assert result["status"] == "FILLED"
    assert result["fill_price"] == 51000.0
    assert result["fill_qty"] == 0.001
    assert result["order_id"] == "99"


def test_binance_broker_client_check_account_connectivity_calls_api(monkeypatch) -> None:
    from app.execution.binance_broker import BinanceBrokerClient

    requested: list = []

    class FakeResponse:
        def raise_for_status(self): pass
        def json(self):
            return {
                "accountType": "SPOT",
                "canTrade": True,
                "canDeposit": True,
                "canWithdraw": True,
                "balances": [{"asset": "BTC", "free": "1.0", "locked": "0.0"}],
            }

    def fake_request(method, url, headers=None, timeout=None):
        requested.append((method, url))
        return FakeResponse()

    monkeypatch.setattr("app.execution.binance_broker.requests.request", fake_request)

    client = BinanceBrokerClient(api_key="test-key", api_secret="test-secret", testnet=True)
    result = client.check_account_connectivity()

    assert requested[0][0] == "GET"
    assert "/api/v3/account" in requested[0][1]
    assert result["status"] == "ok"
    assert result["broker"] == "binance"
    assert result["balance_count"] == 1


def test_binance_broker_client_check_order_request_calls_test_endpoint(monkeypatch) -> None:
    from app.execution.binance_broker import BinanceBrokerClient

    requested: list = []

    class FakeResponse:
        def raise_for_status(self): pass
        def json(self):
            return {}

    def fake_request(method, url, headers=None, timeout=None):
        requested.append((method, url))
        return FakeResponse()

    monkeypatch.setattr("app.execution.binance_broker.requests.request", fake_request)

    client = BinanceBrokerClient(api_key="test-key", api_secret="test-secret", testnet=True)
    result = client.check_order_request(symbol="BTCUSDT", side="BUY", qty=0.001)

    assert requested[0][0] == "POST"
    assert "/api/v3/order/test" in requested[0][1]
    assert result == {
        "status": "ok",
        "broker": "binance",
        "symbol": "BTCUSDT",
        "side": "BUY",
        "qty": 0.001,
        "validated": True,
    }


def test_binance_broker_client_retries_after_syncing_server_time_on_recv_window_error(monkeypatch) -> None:
    from requests import HTTPError

    from app.execution.binance_broker import BinanceBrokerClient

    calls: list[str] = []

    class ErrorResponse:
        status_code = 400
        text = '{"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}'

        def raise_for_status(self):
            raise HTTPError("400 error")

        def json(self):
            return {"code": -1021, "msg": "Timestamp for this request is outside of the recvWindow."}

    class TimeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"serverTime": 1700000005000}

    class SuccessResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return []

    def fake_request(method, url, headers=None, timeout=None, params=None):
        calls.append(url)
        if "/fapi/v1/userTrades" in url and len([item for item in calls if "/fapi/v1/userTrades" in item]) == 1:
            return ErrorResponse()
        if url.endswith("/fapi/v1/time"):
            return TimeResponse()
        return SuccessResponse()

    monkeypatch.setattr("app.execution.binance_broker.requests.request", fake_request)
    monkeypatch.setattr("app.execution.binance_broker.time.time", lambda: 1700000000.0)

    client = BinanceBrokerClient(
        api_key="test-key",
        api_secret="test-secret",
        futures=True,
        futures_api_key="test-futures-key",
        futures_api_secret="test-futures-secret",
        testnet=True,
    )

    result = client.get_user_trades("SOLUSDT", start_time=1, end_time=2, limit=10)

    assert result == []
    assert any("/fapi/v1/time" in url for url in calls)
    user_trade_calls = [url for url in calls if "/fapi/v1/userTrades" in url]
    assert len(user_trade_calls) == 2


def test_binance_broker_client_weighted_avg_fill_price() -> None:
    from app.execution.binance_broker import _weighted_avg_fill_price

    fills = [
        {"price": "50000.0", "qty": "0.001"},
        {"price": "50200.0", "qty": "0.002"},
    ]
    avg = _weighted_avg_fill_price(fills)
    expected = (50000.0 * 0.001 + 50200.0 * 0.002) / 0.003
    assert abs(avg - expected) < 0.01


def test_simulated_broker_client_place_order_returns_fill_at_ref_price() -> None:
    client = SimulatedBrokerClient()
    result = client.place_order(symbol="BTCUSDT", side="BUY", qty=0.001, ref_price=50000.0)
    assert result == {
        "status": "FILLED",
        "fill_price": 50000.0,
        "fill_qty": 0.001,
    }
    assert client.broker_name == "simulated"


def test_live_broker_execute_risk_event_id_writes_order_and_fill() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        broker = SimulatedBrokerClient()
        result = live_broker.execute_risk_event_id(connection, int(risk["id"]), broker)

        assert result is not None
        assert result["status"] == "FILLED"
        assert result["symbol"] == "BTCUSDT"
        assert result["side"] == "BUY"
        assert result["qty"] == 0.001
        assert result["broker"] == "simulated"
        assert result["broker_order_id"] is None

        orders = get_orders(connection, limit=5)
        assert len(orders) == 1
        assert orders[0]["status"] == "FILLED"
        assert orders[0]["broker_name"] == "simulated"
        assert orders[0]["broker_order_id"] is None

        fills = get_fills(connection, limit=5)
        assert len(fills) == 1
        assert fills[0]["price"] == result["price"]
    finally:
        connection.close()


def test_order_migration_adds_broker_metadata_columns() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        columns = set(get_table_columns(connection, "orders"))
        assert "broker_name" in columns
        assert "broker_order_id" in columns
    finally:
        connection.close()


def test_live_broker_persists_external_broker_order_id() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        class FakeBroker:
            broker_name = "binance"

            def place_order(self, symbol, side, qty, ref_price):
                return {
                    "status": "FILLED",
                    "fill_price": ref_price,
                    "fill_qty": qty,
                    "order_id": "binance-123",
                }

        result = live_broker.execute_risk_event_id(connection, int(risk["id"]), FakeBroker())

        assert result is not None
        assert result["broker"] == "binance"
        assert result["broker_order_id"] == "binance-123"

        order = get_orders(connection, limit=1)[0]
        assert order["broker_name"] == "binance"
        assert order["broker_order_id"] == "binance-123"
    finally:
        connection.close()


def test_live_broker_persists_fill_commission_metadata() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        class FakeBroker:
            broker_name = "binance"

            def place_order(self, symbol, side, qty, ref_price):
                return {
                    "status": "FILLED",
                    "fill_price": ref_price,
                    "fill_qty": qty,
                    "order_id": "binance-456",
                    "commission": 0.028,
                    "commission_asset": "USDT",
                    "quote_qty": 70.0,
                    "transact_time": 1774500000000,
                }

        result = live_broker.execute_risk_event_id(connection, int(risk["id"]), FakeBroker())

        assert result is not None
        assert result["commission"] == pytest.approx(0.028)
        assert result["commission_asset"] == "USDT"

        fill = get_fills(connection, limit=1)[0]
        assert fill["commission"] == pytest.approx(0.028)
        assert fill["commission_asset"] == "USDT"
        assert fill["quote_qty"] == pytest.approx(70.0)
        assert fill["transact_time"] == 1774500000000

        events = get_audit_events(connection, limit=5)
        order_event = next(event for event in events if event["event_type"] == "order")
        payload = json.loads(order_event["payload_json"])
        assert payload["commission"] == pytest.approx(0.028)
        assert payload["broker_order_id"] == "binance-456"
    finally:
        connection.close()


def test_live_broker_execute_risk_event_id_keeps_new_order_unfilled() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        class FakeBroker:
            broker_name = "binance"

            def place_order(self, symbol, side, qty, ref_price):
                return {
                    "status": "NEW",
                    "fill_price": 0.0,
                    "fill_qty": 0.0,
                    "order_id": "binance-new-1",
                }

        result = live_broker.execute_risk_event_id(connection, int(risk["id"]), FakeBroker())

        assert result is not None
        assert result["status"] == "NEW"
        assert result["qty"] == pytest.approx(0.001)
        assert result["fill_qty"] == pytest.approx(0.0)
        assert result["fill_price"] is None

        orders = get_orders(connection, limit=5)
        assert len(orders) == 1
        assert orders[0]["status"] == "NEW"
        assert orders[0]["qty"] == pytest.approx(0.001)
        assert orders[0]["price"] == pytest.approx(14.0)

        fills = get_fills(connection, limit=5)
        assert fills == []
    finally:
        connection.close()


def test_risk_service_allows_binance_futures_short_entry_for_ppo(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)

        signal = insert_signal(connection, "SELL", symbol="SOLUSDT", strategy_name="ppo")

        monkeypatch.setattr("app.risk.risk_service.read_configured_execution_backend", lambda: "binance")
        monkeypatch.setattr("app.risk.risk_service.BINANCE_FUTURES", True)
        monkeypatch.setattr("app.risk.risk_service._get_exchange_position_qty", lambda symbol: 0.0)
        monkeypatch.setattr(
            "app.risk.risk_service._get_strategy_target_position",
            lambda strategy_name, symbol, timeframe: -1,
        )

        risk = evaluate_signal_id(
            connection,
            int(signal["id"]),
            order_qty=1.0,
            max_position_qty=1.0,
            cooldown_seconds=0,
        )

        assert risk is not None
        assert risk["decision"] == "APPROVED"
        assert risk["reason"] == "Passed basic risk checks."
    finally:
        connection.close()


def test_live_broker_executes_binance_futures_reversal_to_short(monkeypatch) -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "SELL", symbol="BTCUSDT", strategy_name="ppo")
        connection.execute(
            """
            INSERT INTO risk_events (
                signal_id, symbol, timeframe, strategy_name, signal_type, decision, reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (int(signal["id"]), "BTCUSDT", "1m", "ppo", "SELL", "APPROVED", "ok"),
        )
        connection.commit()

        class FakeBroker:
            broker_name = "binance"

            def __init__(self) -> None:
                self.calls: list[dict[str, float | str]] = []

            def get_positions(self, *, symbol=None, include_flat=False):
                return [{"symbol": "BTCUSDT", "qty": 1.0}]

            def place_order(self, symbol, side, qty, ref_price):
                self.calls.append(
                    {"symbol": symbol, "side": side, "qty": qty, "ref_price": ref_price}
                )
                return {
                    "status": "FILLED",
                    "fill_price": ref_price,
                    "fill_qty": qty,
                    "order_id": "reversal-1",
                }

        broker = FakeBroker()
        monkeypatch.setattr("app.execution.live_broker.read_configured_execution_backend", lambda: "binance")
        monkeypatch.setattr("app.execution.live_broker.BINANCE_FUTURES", True)
        monkeypatch.setattr(
            "app.execution.live_broker._get_strategy_target_position",
            lambda strategy_name, symbol, timeframe: -1,
        )
        monkeypatch.setattr(
            "app.execution.live_broker.get_risk_config",
            lambda connection, strategy_name: (SimpleNamespace(order_qty=1.0), None),
        )

        result = live_broker.execute_risk_event_id(connection, 1, broker, order_qty=1.0)

        assert result is not None
        assert broker.calls == [
                {
                    "symbol": "BTCUSDT",
                    "side": "SELL",
                    "qty": pytest.approx(2.0),
                    "ref_price": pytest.approx(14.0),
            }
        ]
        assert result["side"] == "SELL"
        assert result["qty"] == pytest.approx(2.0)
        assert result["target_position"] == -1
        assert result["current_position_qty"] == pytest.approx(1.0)
        assert result["target_qty"] == pytest.approx(-1.0)
    finally:
        connection.close()


def test_live_broker_logs_structured_failure_event() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        class FailingBroker:
            broker_name = "binance"

            def place_order(self, symbol, side, qty, ref_price):
                raise BinanceAPIError(
                    "Binance API request failed with status 400. code=-2010 msg=Account has insufficient balance for requested action.",
                    status_code=400,
                    url="https://testnet.binance.vision/api/v3/order?symbol=BTCUSDT",
                    response_text='{"code":-2010,"msg":"Account has insufficient balance for requested action."}',
                    response_json={"code": -2010, "msg": "Account has insufficient balance for requested action."},
                )

        with pytest.raises(BinanceAPIError):
            live_broker.execute_risk_event_id(connection, int(risk["id"]), FailingBroker())

        events = get_audit_events(connection, limit=5)
        failed_event = next(event for event in events if event["event_type"] == "order" and event["status"] == "failed")
        payload = json.loads(failed_event["payload_json"])
        assert payload["error_type"] == "BinanceAPIError"
        assert payload["status_code"] == 400
        assert payload["binance_code"] == -2010
        assert "insufficient balance" in payload["binance_msg"].lower()
    finally:
        connection.close()


def test_live_broker_skips_already_executed_risk_event() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        broker = SimulatedBrokerClient()
        risk_id = int(risk["id"])
        live_broker.execute_risk_event_id(connection, risk_id, broker)
        second = live_broker.execute_risk_event_id(connection, risk_id, broker)

        assert second is not None
        assert second.get("reason") == "Already executed"
        assert len(get_orders(connection, limit=10)) == 1
    finally:
        connection.close()


def test_simulated_live_adapter_executes_pending_risks_end_to_end() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        adapter = SimulatedLiveExecutionAdapter()
        results = adapter.execute_pending_approved_risks(connection, symbol_names=["BTCUSDT"])

        assert len(results) == 1
        result = results[0]
        assert result["status"] == "FILLED"
        assert result["symbol"] == "BTCUSDT"
        assert result["broker"] == "simulated"

        orders = get_orders(connection, limit=5)
        assert len(orders) == 1

        fills = get_fills(connection, limit=5)
        assert len(fills) == 1
    finally:
        connection.close()


def test_simulated_live_adapter_execute_risk_event_ids() -> None:
    connection = make_connection()
    try:
        seed_candles(connection, [10.0, 11.0, 12.0, 13.0, 14.0])
        run_migrations(connection)
        ensure_execution_tables(connection)

        signal = insert_signal(connection, "BUY", symbol="BTCUSDT", strategy_name="manual_test")
        risk = evaluate_signal_id(connection, int(signal["id"]), cooldown_seconds=0)
        assert risk is not None

        adapter = SimulatedLiveExecutionAdapter()
        results = adapter.execute_risk_event_ids(connection, [int(risk["id"])])

        assert len(results) == 1
        assert results[0]["status"] == "FILLED"
        assert results[0]["broker"] == "simulated"
    finally:
        connection.close()


def test_binance_broker_client_futures_new_order_without_exchange_trade_stays_unfilled(monkeypatch) -> None:
    from app.execution.binance_broker import BinanceBrokerClient

    calls: list[tuple[str, str]] = []

    def fake_signed_request(self, method, endpoint, params):
        calls.append((method, endpoint))
        if method == "POST":
            return {
                "orderId": 123,
                "status": "NEW",
                "executedQty": "0",
                "avgPrice": "0",
                "cumQuote": "0",
            }
        return []

    monkeypatch.setattr(BinanceBrokerClient, "_signed_request", fake_signed_request)

    client = BinanceBrokerClient(
        api_key="test-key",
        api_secret="test-secret",
        futures=True,
        futures_api_key="test-futures-key",
        futures_api_secret="test-futures-secret",
        testnet=True,
    )
    result = client.place_order(symbol="SOLUSDT", side="BUY", qty=1.0, ref_price=84.5)

    assert result["status"] == "NEW"
    assert result["fill_qty"] == pytest.approx(0.0)
    assert result["fill_price"] == pytest.approx(0.0)
    assert result["order_id"] == "123"
    assert calls == [
        ("POST", "/fapi/v1/order"),
        ("GET", "/fapi/v1/userTrades"),
    ]


def test_simulated_live_adapter_is_not_placeholder() -> None:
    adapter = SimulatedLiveExecutionAdapter()
    assert adapter.placeholder is False
    assert adapter.is_live is False
    assert adapter.can_execute_orders is True
    assert adapter.dry_run is False
    assert adapter.name == "simulated_live"


def test_execution_backend_runtime_status_round_trip(tmp_path, monkeypatch) -> None:
    backend_file = tmp_path / "execution.backend"
    monkeypatch.setattr("app.execution.runtime.EXECUTION_BACKEND_FILE", backend_file)
    monkeypatch.setattr("app.execution.runtime.RUNTIME_DIR", tmp_path)

    result = set_execution_backend("noop")

    assert result["backend"] == "noop"
    assert get_execution_backend_runtime_status() == {
        "backend": "noop",
        "default_backend": "paper",
        "available_backends": ["paper", "noop", "simulated_live", "binance"],
        "execution_backend_file": str(backend_file),
    }


