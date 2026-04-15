import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

import pytest
from fastapi.testclient import TestClient

from app.api.main import app
from app.core.db import get_connection, list_tables
from app.core.job_queue import (
    complete_job,
    enqueue_job,
    enqueue_pipeline_jobs,
    fail_batch_jobs,
    fail_job,
    get_job,
    lease_job_by_id,
    lease_next_job,
    list_jobs,
    reclaim_stale_leased_jobs,
    retry_job,
    run_next_pipeline_batch,
    run_pipeline_batch,
    touch_job_lease,
)
from app.core.job_runner import run_next_queued_job
from app.core.migrations import run_migrations
from app.data.binance_client import fetch_klines
from app.data.candles_service import save_klines
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.run_pipeline import run_pipeline_collect
from app.pipeline.strategy_job import run_strategy_job
from app.pipeline.strategy_job import run_strategy_jobs
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions
from app.query.read_service import (
    get_audit_events,
    get_execution_report,
    get_job_queue_jobs,
    get_orders,
    get_positions,
    get_risk_events,
    get_signals,
    get_strategy_activity_summary,
    get_strategy_closed_trades,
)
from app.risk.risk_service import evaluate_signal_id
from app.strategy.registry import generate_registered_signal
from app.strategy.registry import list_registered_strategies
from app.strategy.signal_service import insert_signal
from app.system.heartbeat import get_heartbeats
from app.system.heartbeat import upsert_heartbeat
from app.system.kill_switch import enable_kill_switch
from conftest import make_connection, make_kline


def seed_candles(connection: sqlite3.Connection, closes: list[float]) -> None:
    run_migrations(connection)
    klines = [make_kline((index + 1) * 60_000, close) for index, close in enumerate(closes)]
    save_klines(connection, klines)

def test_pipeline_run_endpoint_accepts_strategy_name_for_direct_orchestration(monkeypatch) -> None:
    client = TestClient(app)
    called: list[dict[str, object]] = []

    monkeypatch.setattr(
        "app.api.routes.pipeline.run_pipeline_collect",
        lambda strategy_name="ppo", symbol_names=None: called.append(
            {"strategy_name": strategy_name, "symbol_names": symbol_names}
        ) or {
            "status": "completed",
            "strategy_name": strategy_name,
            "requested_symbol_names": symbol_names,
            "steps": [],
        },
    )

    response = client.post(
        "/pipeline/run",
        json={"strategy_name": "ppo", "symbol_names": ["BTCUSDT", "ETHUSDT"], "orchestration": "direct"},
    )

    assert response.status_code == 200
    assert response.json()["strategy_name"] == "ppo"
    assert response.json()["requested_symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert called == [{"strategy_name": "ppo", "symbol_names": ["BTCUSDT", "ETHUSDT"]}]


def test_pipeline_run_endpoint_supports_queue_dispatch(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, Any] = {}

    class DummyConnection:
        def close(self) -> None:
            return None

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)

    def fake_enqueue_pipeline_jobs(connection, **kwargs):
        captured["kwargs"] = kwargs
        return [
            {"batch_id": "batch-123", "job_id": 11, "job_type": "market_data", "payload": {}},
            {"batch_id": "batch-123", "job_id": 12, "job_type": "strategy", "payload": {}},
            {"batch_id": "batch-123", "job_id": 13, "job_type": "execution", "payload": {}},
        ]

    monkeypatch.setattr("app.api.routes.pipeline.enqueue_pipeline_jobs", fake_enqueue_pipeline_jobs)

    response = client.post(
        "/pipeline/run",
        json={
            "strategy_name": "ppo",
            "symbol_names": ["BTCUSDT", "ETHUSDT"],
            "orchestration": "queue_dispatch",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["orchestration"] == "queue_dispatch"
    assert response.json()["batch_id"] == "batch-123"
    assert captured["kwargs"] == {
        "payload": {"orchestration": "queue_dispatch", "source": "api_pipeline"},
        "strategy_name": "ppo",
        "symbol_names": ["BTCUSDT", "ETHUSDT"],
    }


def test_pipeline_run_endpoint_supports_queue_drain(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, Any] = {}
    audit_calls: list[dict[str, Any]] = []

    class DummyConnection:
        def close(self) -> None:
            return None

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.pipeline.insert_event",
        lambda connection, event_type, status, source, message, payload=None: audit_calls.append(
            {"event_type": event_type, "status": status, "source": source, "message": message, "payload": payload}
        ),
    )
    def fake_run_pipeline_batch(connection, batch_id=None):
        captured["batch_id"] = batch_id
        return {
            "status": "completed",
            "batch_id": "batch-123",
            "jobs": [
                {"id": 11, "job_type": "market_data"},
                {"id": 12, "job_type": "strategy"},
                {"id": 13, "job_type": "execution"},
            ],
            "job": {"id": 13, "job_type": "execution"},
            "result": {"status": "ok", "steps": [{"step": "paper_execute", "status": "FILLED", "side": "BUY"}]},
            "remaining_job_types": [],
        }

    monkeypatch.setattr("app.api.routes.pipeline.run_pipeline_batch", fake_run_pipeline_batch)

    response = client.post(
        "/pipeline/run",
        json={
            "strategy_name": "ppo",
            "symbol_names": ["BTCUSDT"],
            "orchestration": "queue_drain",
            "batch_id": "batch-stale-1",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["orchestration"] == "queue_drain"
    assert response.json()["batch_id"] == "batch-123"
    assert response.json()["strategy_name"] == "ppo"
    assert response.json()["requested_symbol_names"] == ["BTCUSDT"]
    assert response.json()["requested_batch_id"] == "batch-stale-1"
    assert captured["batch_id"] == "batch-stale-1"
    assert audit_calls[0]["event_type"] == "queue_control"
    assert audit_calls[0]["status"] == "completed"
    assert audit_calls[0]["payload"]["action"] == "recover_pipeline_batch"
    assert audit_calls[0]["payload"]["requested_batch_id"] == "batch-stale-1"


def test_pipeline_run_endpoint_supports_queue_batch(monkeypatch) -> None:
    client = TestClient(app)

    class DummyConnection:
        def close(self) -> None:
            return None

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.pipeline.enqueue_and_run_pipeline_batch",
        lambda connection, **kwargs: {
            "status": "completed",
            "batch_id": "batch-123",
            "jobs": [{"id": 11, "job_type": "market_data"}],
            "job": {"id": 11, "job_type": "market_data"},
            "result": {"status": "ok", "steps": [{"step": "save_klines", "saved_klines": 5}]},
            "remaining_job_types": [],
        },
    )

    response = client.post(
        "/pipeline/run",
        json={
            "strategy_name": "ppo",
            "symbol_names": ["BTCUSDT"],
            "orchestration": "queue_batch",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["orchestration"] == "queue_batch"
    assert response.json()["batch_id"] == "batch-123"


def test_pipeline_run_endpoint_uses_default_orchestration_setting(monkeypatch) -> None:
    client = TestClient(app)

    class DummyConnection:
        def close(self) -> None:
            return None

    monkeypatch.setattr("app.api.routes.pipeline.DEFAULT_PIPELINE_ORCHESTRATION", "queue_dispatch")
    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.pipeline.enqueue_pipeline_jobs",
        lambda connection, **kwargs: [{"batch_id": "batch-123", "job_id": 11, "job_type": "market_data"}],
    )

    response = client.post(
        "/pipeline/run",
        json={"strategy_name": "ppo", "symbol_names": ["BTCUSDT"]},
    )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["orchestration"] == "queue_dispatch"


def test_strategies_endpoint_lists_registered_strategies() -> None:
    client = TestClient(app)

    response = client.get("/strategies")

    assert response.status_code == 200
    payload = response.json()
    assert payload["default_strategy"] == "ppo"
    assert "ppo" in payload["strategies"]
    assert "ppo" in payload["strategies"]


def test_strategy_summary_endpoint_returns_grouped_activity(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr("app.api.deps.get_connection", lambda: object())
    monkeypatch.setattr(
        "app.api.routes.strategies.get_strategy_activity_summary",
        lambda connection, include_live_book=False: [
            {
                "strategy_name": "ppo",
                "latest_signal": {"signal_type": "BUY"},
                "latest_risk": {"decision": "APPROVED"},
                "latest_order": {"status": "FILLED"},
                "latest_fill": {"side": "BUY"},
                "bid_price": 70850.1,
                "ask_price": 70850.9,
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
                "strategy_name": "ppo",
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

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)

    response = client.get("/strategies/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["strategy_name"] == "ppo"
    assert payload[0]["latest_risk"]["decision"] == "APPROVED"
    assert payload[0]["latest_fill"]["side"] == "BUY"
    assert payload[0]["bid_price"] == 70850.1
    assert payload[0]["ask_price"] == 70850.9
    assert payload[0]["filled_order_count"] == 1
    assert payload[0]["gross_realized_pnl"] == 12.5
    assert payload[0]["winning_trade_count"] == 1
    assert payload[1]["strategy_name"] == "ppo"


def test_strategy_summary_endpoint_uses_exchange_latest_closed_trade_when_binance_is_source_of_truth(monkeypatch) -> None:
    client = TestClient(app)

    class DummyConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.strategies.get_strategy_activity_summary",
        lambda connection, include_live_book=False: [
            {
                "strategy_name": "ppo",
                "latest_signal": {"signal_type": "SELL", "symbol": "SOLUSDT"},
                "latest_risk": {"decision": "APPROVED"},
                "latest_order": {"status": "FILLED", "symbol": "SOLUSDT"},
                "latest_fill": {"side": "SELL", "symbol": "SOLUSDT"},
                "latest_closed_trade": {"realized_pnl": 123.0, "closed_at": "stale"},
                "gross_realized_pnl": 0.0,
                "total_commission": 0.0,
                "net_realized_pnl": 0.0,
                "filled_order_count": 0,
                "filled_qty_total": 0.0,
                "buy_fill_count": 0,
                "sell_fill_count": 0,
                "realized_trade_count": 0,
                "winning_trade_count": 0,
                "losing_trade_count": 0,
                "breakeven_trade_count": 0,
            }
        ],
    )
    monkeypatch.setattr("app.api.routes.strategies.binance_futures_enabled", lambda: True)
    monkeypatch.setattr(
        "app.api.routes.strategies.get_binance_futures_positions",
        lambda symbol=None, include_flat=False: [
            {"symbol": "SOLUSDT", "qty": -1.0, "avg_price": 82.27, "unrealized_pnl": 0.43}
        ],
    )
    monkeypatch.setattr(
        "app.api.routes.strategies.normalize_binance_futures_position",
        lambda symbol, position: {"symbol": symbol, "qty": -1.0, "avg_price": 82.27, "unrealized_pnl": 0.43},
    )
    monkeypatch.setattr(
        "app.api.routes.strategies.build_exchange_trade_snapshot",
        lambda symbol, strategy_name=None, days=7, limit=1000, start_date=None, end_date=None: {
            "exchange": {
                "summary": {"fills": 2, "notional": 166.01362},
                "trades": [],
            },
            "trades": [
                {"symbol": symbol, "side": "SELL", "qty": 1.0, "commission": 0.1, "commission_asset": "USDT", "realized_pnl": 0.0},
                {"symbol": symbol, "side": "BUY", "qty": 1.0, "commission": 0.1, "commission_asset": "USDT", "realized_pnl": -0.73681},
            ],
            "daily": [],
            "gross_pnl": -0.73681,
            "total_fees": 0.2,
            "realized_trade_count": 1,
            "win_trade_count": 0,
            "loss_trade_count": 1,
            "best_trade": -0.73681,
            "worst_trade": -0.73681,
            "latest_closed_trade": {
                "strategy_name": strategy_name,
                "symbol": symbol,
                "timeframe": None,
                "qty": 1.0,
                "entry_price": None,
                "exit_price": 83.00681,
                "realized_pnl": -0.73681,
                "closed_at": "2026-04-09T02:34:44+00:00",
                "order_id": "short-close",
                "status": "loss",
                "source": "binance_user_trades",
            },
            "recent_closed_trades": [],
            "filled_qty_total": 2.0,
            "buy_count": 1,
            "sell_count": 1,
        },
    )

    response = client.get("/strategies/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["latest_closed_trade"]["realized_pnl"] == pytest.approx(-0.73681)
    assert payload[0]["latest_closed_trade"]["closed_at"] == "2026-04-09T02:34:44+00:00"
    assert payload[0]["latest_closed_trade"]["status"] == "loss"
    assert payload[0]["latest_exchange_closed_trade"]["realized_pnl"] == pytest.approx(-0.73681)
    assert payload[0]["net_position_qty"] == pytest.approx(-1.0)


def test_strategy_summary_endpoint_tolerates_binance_position_timeout(monkeypatch) -> None:
    client = TestClient(app)

    class DummyConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.strategies.get_strategy_activity_summary",
        lambda connection, include_live_book=False: [
            {
                "strategy_name": "ppo",
                "latest_signal": {"signal_type": "BUY", "symbol": "SOLUSDT"},
                "latest_risk": {"decision": "APPROVED"},
                "latest_order": {"status": "FILLED", "symbol": "SOLUSDT"},
                "latest_fill": {"side": "BUY", "symbol": "SOLUSDT"},
                "filled_order_count": 1,
                "filled_qty_total": 1.0,
                "gross_realized_pnl": 0.0,
                "buy_fill_count": 1,
                "sell_fill_count": 0,
                "realized_trade_count": 0,
                "winning_trade_count": 0,
                "losing_trade_count": 0,
                "breakeven_trade_count": 0,
            }
        ],
    )
    monkeypatch.setattr("app.api.routes.strategies.binance_futures_enabled", lambda: True)
    monkeypatch.setattr(
        "app.api.routes.strategies.get_binance_futures_positions",
        lambda symbol=None, include_flat=False: (_ for _ in ()).throw(TimeoutError("positionRisk timeout")),
    )
    monkeypatch.setattr(
        "app.api.routes.strategies.build_exchange_trade_snapshot",
        lambda symbol, strategy_name=None, days=7, limit=1000, start_date=None, end_date=None: {
            "exchange": {
                "summary": {"fills": 1, "notional": 163.74},
                "trades": [],
            },
            "trades": [
                {
                    "trade_id": 2,
                    "order_id": "short-close",
                    "symbol": symbol,
                    "side": "BUY",
                    "price": 81.87,
                    "qty": 2.0,
                    "commission": 0.1,
                    "commission_asset": "USDT",
                    "realized_pnl": 0.4,
                    "transact_time": 2000,
                    "created_at": "2026-04-09T04:34:28+00:00",
                },
            ],
            "daily": [],
            "gross_pnl": 0.4,
            "total_fees": 0.1,
            "realized_trade_count": 1,
            "win_trade_count": 1,
            "loss_trade_count": 0,
            "best_trade": 0.4,
            "worst_trade": 0.4,
            "latest_closed_trade": {
                "strategy_name": strategy_name,
                "symbol": symbol,
                "timeframe": None,
                "qty": 2.0,
                "entry_price": None,
                "exit_price": 81.87,
                "realized_pnl": 0.4,
                "closed_at": "2026-04-09T04:34:28+00:00",
                "order_id": "short-close",
                "status": "win",
                "source": "binance_user_trades",
            },
            "recent_closed_trades": [],
            "filled_qty_total": 2.0,
            "buy_count": 1,
            "sell_count": 0,
        },
    )

    response = client.get("/strategies/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["latest_closed_trade"]["realized_pnl"] == pytest.approx(0.4)
    assert payload[0]["latest_exchange_closed_trade"]["realized_pnl"] == pytest.approx(0.4)
    assert payload[0]["exchange_current_position"]["qty"] == pytest.approx(0.0)


def test_get_strategy_activity_summary_handles_mixed_timestamp_types(monkeypatch) -> None:
    from datetime import datetime, timezone

    from app.query.read_service import get_strategy_activity_summary

    class DummyConnection:
        def execute(self, query, params=()):
            class DummyCursor:
                def fetchone(self_inner):
                    return None

            return DummyCursor()

    monkeypatch.setattr("app.query.activity_summary.list_registered_strategies", lambda: ["ppo"])
    monkeypatch.setattr(
        "app.query.read_service.get_signals",
        lambda connection, limit=100: [
            {
                "id": 1,
                "symbol": "BTCUSDT",
                "timeframe": "5m",
                "strategy_name": "ppo",
                "signal_type": "BUY",
                "created_at": "2026-04-01 12:00:00",
            }
        ],
    )
    monkeypatch.setattr(
        "app.query.read_service.get_risk_events",
        lambda connection, limit=100: [
            {
                "id": 1,
                "strategy_name": "ppo",
                "decision": "APPROVED",
                "created_at": datetime(2026, 4, 1, 12, 1, tzinfo=timezone.utc),
            }
        ],
    )
    monkeypatch.setattr(
        "app.query.read_service.get_all_orders",
        lambda connection: [
            {
                "id": 10,
                "symbol": "BTCUSDT",
                "timeframe": "5m",
                "strategy_name": "ppo",
                "side": "BUY",
                "qty": 0.001,
                "price": 70000,
                "status": "FILLED",
                "created_at": "2026-04-01 12:02:00",
            }
        ],
    )
    monkeypatch.setattr(
        "app.query.read_service.get_all_fills",
        lambda connection: [
            {
                "order_id": 10,
                "created_at": datetime(2026, 4, 1, 12, 3, tzinfo=timezone.utc),
                "commission": 0.0,
                "commission_asset": "USDT",
            }
        ],
    )
    monkeypatch.setattr("app.query.activity_summary.get_strategy_closed_trades", lambda connection, limit, per_table_limit: [])

    payload = get_strategy_activity_summary(DummyConnection())

    assert payload[0]["strategy_name"] == "ppo"
    assert payload[0]["latest_activity_at"] == "2026-04-01T12:03:00+00:00"


def test_strategy_closed_trades_endpoint_returns_recent_trades(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, object] = {}

    class DummyConnection:
        def close(self) -> None:
            pass

    def fake_get_strategy_closed_trades(connection, limit=20, strategy_name=None):
        captured["limit"] = limit
        captured["strategy_name"] = strategy_name
        return [
            {
                "strategy_name": "ppo",
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
    monkeypatch.setattr("app.api.routes.trades.get_strategy_closed_trades", fake_get_strategy_closed_trades)

    response = client.get("/strategies/closed-trades?limit=10&strategy_name=ppo")

    assert response.status_code == 200
    assert captured == {"limit": 10, "strategy_name": "ppo"}
    payload = response.json()
    assert payload[0]["strategy_name"] == "ppo"
    assert payload[0]["realized_pnl"] == 10.0


def test_positions_endpoint_falls_back_to_local_positions_when_binance_times_out(monkeypatch) -> None:
    client = TestClient(app)

    class DummyConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr("app.api.routes.positions.binance_futures_enabled", lambda: True)
    monkeypatch.setattr(
        "app.api.routes.positions.get_binance_futures_positions",
        lambda include_flat=False: (_ for _ in ()).throw(TimeoutError("positionRisk timeout")),
    )
    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.positions.get_positions",
        lambda connection, limit=10: [
            {"symbol": "SOLUSDT", "qty": 1.0, "avg_price": 81.87, "realized_pnl": 0.0, "updated_at": "2026-04-09T04:34:28+00:00"}
        ],
    )

    response = client.get("/positions?limit=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["symbol"] == "SOLUSDT"
    assert payload[0]["avg_price"] == pytest.approx(81.87)


def test_testnet_execution_report_includes_latest_exchange_closed_trade(monkeypatch) -> None:
    client = TestClient(app)

    class DummyConnection:
        def close(self) -> None:
            pass

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.reports.get_execution_report",
        lambda connection, symbol="BTCUSDT", strategy_name=None, days=7, limit=10: {
            "summary": {"symbol": symbol, "strategy_name": strategy_name or "all"},
            "recent_closed_trades": [],
            "recent_fills": [],
            "daily": [],
        },
    )
    monkeypatch.setattr("app.execution.exchange_trades.binance_futures_enabled", lambda: True)
    monkeypatch.setattr(
        "app.execution.exchange_trades.build_exchange_trade_snapshot",
        lambda symbol, strategy_name=None, days=7, limit=100, start_date=None, end_date=None: {
            "exchange": {
                "summary": {"fills": 1, "notional": 163.74},
                "trades": [],
            },
            "trades": [
                {
                    "trade_id": 1,
                    "order_id": "close-1",
                    "symbol": symbol,
                    "side": "BUY",
                    "price": 81.87,
                    "qty": 2.0,
                    "quote_qty": 163.74,
                    "commission": 0.1,
                    "commission_asset": "USDT",
                    "realized_pnl": 0.4,
                    "created_at": "2026-04-09T04:34:28+00:00",
                }
            ],
            "daily": [
                {
                    "trade_date": "2026-04-09",
                    "fills": 1,
                    "notional": 163.74,
                    "gross_pnl": 0.4,
                    "fees": 0.1,
                    "net_pnl": 0.3,
                }
            ],
            "gross_pnl": 0.4,
            "total_fees": 0.1,
            "realized_trade_count": 1,
            "win_trade_count": 1,
            "loss_trade_count": 0,
            "best_trade": 0.4,
            "worst_trade": 0.4,
            "latest_closed_trade": {
                "strategy_name": "ppo",
                "symbol": symbol,
                "qty": 2.0,
                "entry_price": None,
                "exit_price": 81.87,
                "realized_pnl": 0.4,
                "closed_at": "2026-04-09T04:34:28+00:00",
                "order_id": "close-1",
                "status": "win",
                "source": "binance_user_trades",
            },
            "recent_closed_trades": [
                {
                    "strategy_name": "ppo",
                    "symbol": symbol,
                    "timeframe": "1m",
                    "qty": 2.0,
                    "entry_price": None,
                    "exit_price": 81.87,
                    "realized_pnl": 0.4,
                    "closed_at": "2026-04-09T04:34:28+00:00",
                    "hold_minutes": None,
                    "order_id": "close-1",
                    "status": "win",
                    "source": "binance_user_trades",
                }
            ],
            "filled_qty_total": 2.0,
            "buy_count": 1,
            "sell_count": 0,
        },
    )
    monkeypatch.setattr(
        "app.execution.exchange_trades.get_binance_futures_position",
        lambda symbol: {"symbol": symbol, "qty": 1.0, "avg_price": 81.87, "unrealized_pnl": -0.01, "source": "binance_futures"},
    )

    response = client.get("/reports/testnet-execution?symbol=SOLUSDT&strategy_name=ppo&days=7&limit=10")

    assert response.status_code == 200
    payload = response.json()
    assert payload["summary"]["latest_exchange_closed_trade"]["realized_pnl"] == pytest.approx(0.4)
    assert payload["recent_closed_trades"][0]["realized_pnl"] == pytest.approx(0.4)


def test_scheduler_strategy_endpoints_round_trip(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "app.api.routes.scheduler.get_strategy_status",
        lambda: {
            "strategy_name": "ppo",
            "strategy_names": ["ppo", "ppo"],
            "disabled_strategy_names": ["ppo"],
            "effective_strategy_names": ["ppo"],
            "effective_strategy_limit": 1,
            "strategy_priorities": {"ppo": 0, "ppo": 10},
            "disabled_strategy_notes": {"ppo": "cooldown investigation"},
            "default_strategy": "ppo",
            "strategy_file": "runtime/scheduler.strategy",
            "disabled_strategy_file": "runtime/scheduler.strategy.disabled",
            "priority_file": "runtime/scheduler.strategy.priority.json",
            "disabled_reason_file": "runtime/scheduler.strategy.disabled.reason.json",
            "effective_limit_file": "runtime/scheduler.strategy.limit",
            "available_strategies": ["ppo", "ppo"],
        },
    )
    def fake_set_active_strategies(strategy_names, **kwargs):
        captured["active"] = strategy_names
        captured["active_kwargs"] = kwargs
        return {
            "strategy_name": strategy_names[0],
            "strategy_names": strategy_names,
            "strategy_file": "runtime/scheduler.strategy",
        }

    def fake_set_disabled_strategies(strategy_names, **kwargs):
        captured["disabled"] = strategy_names
        captured["disabled_kwargs"] = kwargs
        return {
            "disabled_strategy_names": strategy_names,
            "disabled_strategy_file": "runtime/scheduler.strategy.disabled",
        }

    def fake_set_strategy_priorities(strategy_priorities, **kwargs):
        captured["priorities"] = strategy_priorities
        captured["priorities_kwargs"] = kwargs
        return {
            "strategy_priorities": strategy_priorities,
            "priority_file": "runtime/scheduler.strategy.priority.json",
        }

    def fake_set_disabled_strategy_notes(strategy_notes, **kwargs):
        captured["notes"] = strategy_notes
        captured["notes_kwargs"] = kwargs
        return {
            "disabled_strategy_notes": strategy_notes,
            "disabled_reason_file": "runtime/scheduler.strategy.disabled.reason.json",
        }

    def fake_set_effective_strategy_limit(limit, **kwargs):
        captured["limit"] = limit
        captured["limit_kwargs"] = kwargs
        return {
            "effective_strategy_limit": limit,
            "effective_limit_file": "runtime/scheduler.strategy.limit",
        }

    monkeypatch.setattr("app.api.routes.scheduler.set_active_strategies", fake_set_active_strategies)
    monkeypatch.setattr("app.api.routes.scheduler.set_disabled_strategies", fake_set_disabled_strategies)
    monkeypatch.setattr("app.api.routes.scheduler.set_strategy_priorities", fake_set_strategy_priorities)
    monkeypatch.setattr("app.api.routes.scheduler.set_disabled_strategy_notes", fake_set_disabled_strategy_notes)
    monkeypatch.setattr("app.api.routes.scheduler.set_effective_strategy_limit", fake_set_effective_strategy_limit)

    status_response = client.get("/scheduler/strategy")
    update_response = client.post(
        "/scheduler/strategy",
        json={
            "strategy_names": ["ppo", "ppo"],
            "disabled_strategy_names": ["ppo"],
            "strategy_priorities": {"ppo": 0, "ppo": 10},
            "disabled_strategy_notes": {"ppo": "cooldown investigation"},
            "effective_strategy_limit": 1,
            "audit_action": "save_strategy_state",
            "audit_message": "Applied scheduler strategy state from admin.",
        },
    )

    assert status_response.status_code == 200
    assert status_response.json()["strategy_name"] == "ppo"
    assert status_response.json()["strategy_names"] == ["ppo", "ppo"]
    assert status_response.json()["disabled_strategy_names"] == ["ppo"]
    assert status_response.json()["effective_strategy_names"] == ["ppo"]
    assert status_response.json()["effective_strategy_limit"] == 1
    assert update_response.status_code == 200
    assert update_response.json()["strategy_names"] == ["ppo", "ppo"]
    assert update_response.json()["disabled_strategy_names"] == ["ppo"]
    assert update_response.json()["strategy_priorities"] == {"ppo": 0, "ppo": 10}
    assert update_response.json()["disabled_strategy_notes"] == {"ppo": "cooldown investigation"}
    assert update_response.json()["effective_strategy_limit"] == 1
    assert captured == {
        "active": ["ppo", "ppo"],
        "disabled": ["ppo"],
        "priorities": {"ppo": 0, "ppo": 10},
        "notes": {"ppo": "cooldown investigation"},
        "limit": 1,
        "active_kwargs": {
            "audit_action": "save_strategy_state",
            "audit_message": "Applied scheduler strategy state from admin.",
        },
        "disabled_kwargs": {
            "audit_action": "save_strategy_state",
            "audit_message": "Applied scheduler strategy state from admin.",
        },
        "priorities_kwargs": {
            "audit_action": "save_strategy_state",
            "audit_message": "Applied scheduler strategy state from admin.",
        },
        "notes_kwargs": {
            "audit_action": "save_strategy_state",
            "audit_message": "Applied scheduler strategy state from admin.",
        },
        "limit_kwargs": {
            "audit_action": "save_strategy_state",
            "audit_message": "Applied scheduler strategy state from admin.",
        },
    }


def test_scheduler_strategy_preset_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, object] = {}

    def fake_get_strategy_status():
        return {
            "strategy_name": "ppo",
            "strategy_names": ["ppo"],
            "disabled_strategy_names": [],
            "effective_strategy_names": ["ppo"],
            "effective_strategy_limit": None,
            "strategy_priorities": {"ppo": 0},
            "disabled_strategy_notes": {},
            "default_strategy": "ppo",
            "strategy_file": "runtime/scheduler.strategy",
            "disabled_strategy_file": "runtime/scheduler.strategy.disabled",
            "priority_file": "runtime/scheduler.strategy.priority.json",
            "disabled_reason_file": "runtime/scheduler.strategy.disabled.reason.json",
            "effective_limit_file": "runtime/scheduler.strategy.limit",
            "available_strategies": ["ppo"],
        }

    def fake_set_strategy_priorities(strategy_priorities, **kwargs):
        captured["priorities"] = strategy_priorities
        captured["kwargs"] = kwargs
        return {
            "strategy_priorities": strategy_priorities,
            "priority_file": "runtime/scheduler.strategy.priority.json",
        }

    monkeypatch.setattr("app.api.routes.scheduler.get_strategy_status", fake_get_strategy_status)
    monkeypatch.setattr("app.api.routes.scheduler.set_strategy_priorities", fake_set_strategy_priorities)

    response = client.post("/scheduler/strategy/preset", json={"preset": "active_first"})

    assert response.status_code == 200
    assert captured["priorities"] == {"ppo": 0}
    assert captured["kwargs"]["audit_action"] == "priority_preset:active_first"
    assert captured["kwargs"]["extra_payload"] == {"preset": "active_first"}
    assert response.json()["strategy_names"] == ["ppo"]


def test_scheduler_symbol_endpoints_round_trip(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "app.api.routes.scheduler.get_symbol_status",
        lambda: {
            "symbol": "BTCUSDT",
            "symbol_names": ["BTCUSDT", "ETHUSDT"],
            "default_symbol": "BTCUSDT",
            "symbol_file": "runtime/scheduler.symbols",
            "available_symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"],
        },
    )

    def fake_set_active_symbols(symbol_names, **kwargs):
        captured["symbols"] = symbol_names
        captured["kwargs"] = kwargs
        return {
            "symbol": symbol_names[0],
            "symbol_names": symbol_names,
            "symbol_file": "runtime/scheduler.symbols",
        }

    monkeypatch.setattr("app.api.routes.scheduler.set_active_symbols", fake_set_active_symbols)

    status_response = client.get("/scheduler/symbols")
    update_response = client.post("/scheduler/symbols", json={"symbol_names": ["BTCUSDT", "ETHUSDT"]})

    assert status_response.status_code == 200
    assert status_response.json()["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert update_response.status_code == 200
    assert update_response.json()["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert captured["symbols"] == ["BTCUSDT", "ETHUSDT"]
    assert captured["kwargs"]["audit_action"] == "set_active_symbols"


def test_reclaim_stale_leased_jobs_resets_expired_lease() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(connection, "strategy", payload={"strategy_names": ["ppo"]})
        leased = lease_next_job(connection, job_type="strategy")
        assert leased is not None
        assert leased["status"] == "leased"

        # Simulate a stale lease by back-dating started_at.
        connection.execute(
            "UPDATE job_queue SET started_at = '2000-01-01 00:00:00' WHERE id = ?;",
            (job_id,),
        )
        connection.commit()

        reclaimed = reclaim_stale_leased_jobs(connection, lease_timeout_seconds=300)
        assert reclaimed == 1

        recovered = get_job(connection, job_id)
        assert recovered is not None
        assert recovered["status"] == "queued"
        assert recovered["started_at"] is None
    finally:
        connection.close()


def test_reclaim_stale_leased_jobs_ignores_fresh_leases() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        enqueue_job(connection, "strategy", payload={"strategy_names": ["ppo"]})
        leased = lease_next_job(connection, job_type="strategy")
        assert leased is not None

        # Fresh lease — should NOT be reclaimed.
        reclaimed = reclaim_stale_leased_jobs(connection, lease_timeout_seconds=300)
        assert reclaimed == 0

        job = get_job(connection, int(leased["id"]))
        assert job is not None
        assert job["status"] == "leased"
    finally:
        connection.close()


def test_reclaim_stale_leased_jobs_ignores_completed_and_failed() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(connection, "strategy", payload={})
        complete_job(connection, job_id, result={"status": "ok"})

        reclaimed = reclaim_stale_leased_jobs(connection, lease_timeout_seconds=0)
        assert reclaimed == 0

        job = get_job(connection, job_id)
        assert job is not None
        assert job["status"] == "completed"
    finally:
        connection.close()


def test_reclaim_stale_leased_jobs_supports_mixed_timestamp_formats() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(connection, "strategy", payload={})
        connection.execute(
            """
            UPDATE job_queue
            SET status = 'leased', started_at = ?
            WHERE id = ?;
            """,
            ("2000-01-01T00:00:00+00:00", job_id),
        )
        connection.commit()

        reclaimed = reclaim_stale_leased_jobs(connection, lease_timeout_seconds=300)
        assert reclaimed == 1

        job = get_job(connection, job_id)
        assert job is not None
        assert job["status"] == "queued"
        assert job["started_at"] is None
    finally:
        connection.close()


def test_reclaim_stale_leased_jobs_keeps_running_training_jobs_leased() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        training_job_id = connection.execute(
            """
            INSERT INTO training_jobs (
                symbol, timeframe, feature_set, status, params_json, progress_json, created_at, started_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                "BTCUSDT",
                "1m",
                "v1",
                "running",
                json.dumps({"job_type": "ppo", "total_steps": 10_000}),
                json.dumps({"pct": 0, "step": 0, "total": 10_000}),
                "2000-01-01T00:00:00+00:00",
                "2000-01-01T00:00:00+00:00",
            ),
        ).lastrowid
        connection.commit()

        queue_job_id = enqueue_job(connection, "training_ppo", payload={"training_job_id": int(training_job_id)})
        connection.execute(
            """
            UPDATE job_queue
            SET status = 'leased', started_at = ?
            WHERE id = ?;
            """,
            ("2000-01-01T00:00:00+00:00", queue_job_id),
        )
        connection.commit()

        reclaimed = reclaim_stale_leased_jobs(connection, lease_timeout_seconds=300)
        assert reclaimed == 0

        job = get_job(connection, queue_job_id)
        assert job is not None
        assert job["status"] == "leased"
    finally:
        connection.close()


def test_touch_job_lease_refreshes_started_at_for_leased_job() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(connection, "strategy", payload={"strategy_names": ["ppo"]})
        leased = lease_job_by_id(connection, job_id)
        assert leased is not None

        connection.execute(
            "UPDATE job_queue SET started_at = '2000-01-01 00:00:00' WHERE id = ?;",
            (job_id,),
        )
        connection.commit()

        touch_job_lease(connection, job_id)
        refreshed = get_job(connection, job_id)
        assert refreshed is not None
        assert refreshed["status"] == "leased"
        assert refreshed["started_at"] is not None
        assert str(refreshed["started_at"]).startswith("20")
        assert refreshed["started_at"] != "2000-01-01 00:00:00"
    finally:
        connection.close()


def test_job_queue_lifecycle_round_trip() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)

        first_job_id = enqueue_job(
            connection,
            "strategy",
            payload={"strategy_names": ["ppo", "ppo"], "symbol_names": ["BTCUSDT", "ETHUSDT"]},
        )
        second_job_id = enqueue_job(connection, "execution", payload={"symbol_names": ["ETHUSDT"]})

        listed_jobs = list_jobs(connection, limit=10)
        assert [job["id"] for job in listed_jobs] == [second_job_id, first_job_id]
        assert listed_jobs[1]["payload"]["strategy_names"] == ["ppo", "ppo"]

        leased_job = lease_next_job(connection, job_type="strategy")
        assert leased_job is not None
        assert leased_job["id"] == first_job_id
        assert leased_job["status"] == "leased"
        assert leased_job["attempt_count"] == 1
        assert leased_job["started_at"] is not None

        complete_job(connection, first_job_id, result={"generated_signal_count": 2})
        completed_job = get_job(connection, first_job_id)
        assert completed_job is not None
        assert completed_job["status"] == "completed"
        assert completed_job["result"] == {"generated_signal_count": 2}
        assert completed_job["completed_at"] is not None

        fail_job(connection, second_job_id, "execution worker unavailable", result={"symbol_names": ["ETHUSDT"]})
        failed_job = get_job(connection, second_job_id)
        assert failed_job is not None
        assert failed_job["status"] == "failed"
        assert failed_job["error_message"] == "execution worker unavailable"
        assert failed_job["result"] == {"symbol_names": ["ETHUSDT"]}

        retried_job = retry_job(connection, second_job_id)
        assert retried_job["status"] == "queued"
        assert retried_job["error_message"] is None
        assert retried_job["result"] is None
        assert retried_job["started_at"] is None
        assert retried_job["completed_at"] is None

        queue_rows = get_job_queue_jobs(connection, limit=10)
        assert queue_rows[0]["id"] == second_job_id
        assert queue_rows[0]["payload"] == {
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
            "symbol_names": ["ETHUSDT"],
        }
        assert "job_queue" in list_tables(connection)
    finally:
        connection.close()


def test_backtest_loader_accepts_full_iso_utc_start() -> None:
    from app.backtest.loader import load_candles_from_db

    connection = make_connection()
    try:
        run_migrations(connection)
        run_migrations(connection)
        save_klines(
            connection,
            [
                make_kline(1_775_692_800_000, 100.0),
                make_kline(1_775_692_860_000, 101.0),
                make_kline(1_775_692_920_000, 102.0),
            ],
        )
        candles = load_candles_from_db(
            connection,
            symbol="BTCUSDT",
            start="2026-04-09T00:01:00+00:00",
        )
        assert len(candles) == 2
        assert candles[0]["close"] == 101.0
    finally:
        connection.close()


def test_job_queue_persists_utc_iso_timestamps() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(connection, "strategy", payload={"strategy_names": ["ppo"]})
        queued_job = get_job(connection, job_id)
        assert queued_job is not None
        assert "T" in str(queued_job["created_at"])
        assert str(queued_job["created_at"]).endswith("+00:00")

        leased_job = lease_next_job(connection, job_type="strategy")
        assert leased_job is not None
        assert "T" in str(leased_job["started_at"])
        assert str(leased_job["started_at"]).endswith("+00:00")

        complete_job(connection, job_id, result={"status": "ok"})
        completed_job = get_job(connection, job_id)
        assert completed_job is not None
        assert "T" in str(completed_job["completed_at"])
        assert str(completed_job["completed_at"]).endswith("+00:00")
    finally:
        connection.close()


def test_run_next_queued_job_persists_structured_error_detail() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(connection, "execution", payload={"symbol_names": ["BTCUSDT"]})

        class RichError(RuntimeError):
            def to_payload(self):
                return {"status_code": 400, "binance_code": -2010, "binance_msg": "insufficient balance"}

        def boom(connection, risk_event_ids=None, symbol_names=None):
            raise RichError("execution failed")

        import app.core.job_runner as jrmod
        original_run_execution_job = jrmod.run_execution_job
        jrmod.run_execution_job = boom
        try:
            result = run_next_queued_job(connection, job_type="execution")
        finally:
            jrmod.run_execution_job = original_run_execution_job

        assert result["status"] == "failed"
        failed_job = get_job(connection, job_id)
        assert failed_job is not None
        assert failed_job["result"]["error_detail"]["status_code"] == 400
        assert failed_job["result"]["error_detail"]["binance_code"] == -2010
        assert failed_job["result"]["error_detail"]["binance_msg"] == "insufficient balance"
    finally:
        connection.close()


def test_training_job_create_persists_utc_iso_created_at() -> None:
    from app.training.job_service import create_job, get_job as get_training_job

    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = create_job(connection, "SOLUSDT", "1m", "v1", params={"alpha": 1})
        job = get_training_job(connection, job_id)
        assert job is not None
        assert "T" in str(job["created_at"])
        assert str(job["created_at"]).endswith("+00:00")
    finally:
        connection.close()


def test_complete_job_propagates_strategy_signal_ids_to_dependent_risk_job() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        strategy_job_id = enqueue_job(connection, "strategy", payload={"symbol_names": ["BTCUSDT"]})
        risk_job_id = enqueue_job(connection, "risk", payload={"symbol_names": ["BTCUSDT"]}, depends_on_job_id=strategy_job_id)

        complete_job(connection, strategy_job_id, result={"status": "ok", "signal_ids": [7, 8]})

        risk_job = get_job(connection, risk_job_id)
        assert risk_job is not None
        assert risk_job["payload"]["signal_ids"] == [7, 8]
    finally:
        connection.close()


def test_complete_job_propagates_risk_event_ids_to_dependent_execution_job() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        risk_job_id = enqueue_job(connection, "risk", payload={"symbol_names": ["BTCUSDT"]})
        execution_job_id = enqueue_job(connection, "execution", payload={"symbol_names": ["BTCUSDT"]}, depends_on_job_id=risk_job_id)

        complete_job(connection, risk_job_id, result={"status": "ok", "risk_event_ids": [21]})

        execution_job = get_job(connection, execution_job_id)
        assert execution_job is not None
        assert execution_job["payload"]["risk_event_ids"] == [21]
    finally:
        connection.close()


def test_enqueue_pipeline_jobs_creates_ordered_queue_batch() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)

        jobs = __import__("app.core.job_queue", fromlist=["enqueue_pipeline_jobs"]).enqueue_pipeline_jobs(
            connection,
            strategy_name="ppo",
            strategy_names=["ppo"],
            symbol_names=["BTCUSDT", "ETHUSDT"],
            payload={"source": "test_batch"},
        )

        assert [job["job_type"] for job in jobs] == ["market_data", "strategy", "risk", "execution"]
        assert len({job["batch_id"] for job in jobs}) == 1
        queue_rows = list_jobs(connection, limit=10)
        assert [job["job_type"] for job in queue_rows] == ["execution", "risk", "strategy", "market_data"]
        assert queue_rows[0]["payload"]["strategy_names"] == ["ppo"]
        assert queue_rows[0]["payload"]["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
        assert queue_rows[0]["payload"]["batch_id"] == jobs[0]["batch_id"]
        # dependency chain: market_data → strategy → risk → execution
        md_job = next(j for j in jobs if j["job_type"] == "market_data")
        st_job = next(j for j in jobs if j["job_type"] == "strategy")
        rk_job = next(j for j in jobs if j["job_type"] == "risk")
        ex_job = next(j for j in jobs if j["job_type"] == "execution")
        assert md_job["depends_on_job_id"] is None
        assert st_job["depends_on_job_id"] == md_job["job_id"]
        assert rk_job["depends_on_job_id"] == st_job["job_id"]
        assert ex_job["depends_on_job_id"] == rk_job["job_id"]
    finally:
        connection.close()


def test_lease_next_job_respects_depends_on_job_id() -> None:
    """Each pipeline job must not be leasable until its dependency is completed."""
    connection = make_connection()
    try:
        run_migrations(connection)
        jobs = enqueue_pipeline_jobs(connection, strategy_name="ppo")
        md_job_id = next(j["job_id"] for j in jobs if j["job_type"] == "market_data")
        st_job_id = next(j["job_id"] for j in jobs if j["job_type"] == "strategy")
        rk_job_id = next(j["job_id"] for j in jobs if j["job_type"] == "risk")
        ex_job_id = next(j["job_id"] for j in jobs if j["job_type"] == "execution")

        # only market_data should be leasable before anything is completed
        leasable = lease_next_job(connection, job_type="strategy")
        assert leasable is None, "strategy job must not be leasable while market_data is queued"

        leasable = lease_next_job(connection, job_type="risk")
        assert leasable is None, "risk job must not be leasable while strategy is queued"

        leasable = lease_next_job(connection, job_type="execution")
        assert leasable is None, "execution job must not be leasable while risk is queued"

        # complete market_data — now strategy becomes leasable
        lease_job_by_id(connection, md_job_id)
        complete_job(connection, md_job_id, result={"status": "ok"})

        leasable_st = lease_next_job(connection, job_type="strategy")
        assert leasable_st is not None
        assert leasable_st["id"] == st_job_id

        # risk and execution still blocked
        leasable_rk = lease_next_job(connection, job_type="risk")
        assert leasable_rk is None, "risk job must not be leasable while strategy is leased"

        complete_job(connection, st_job_id, result={"status": "ok"})
        leasable_rk = lease_next_job(connection, job_type="risk")
        assert leasable_rk is not None
        assert leasable_rk["id"] == rk_job_id

        # execution still blocked until risk is completed
        leasable_ex = lease_next_job(connection, job_type="execution")
        assert leasable_ex is None, "execution job must not be leasable while risk is leased"

        complete_job(connection, rk_job_id, result={"status": "ok"})
        leasable_ex = lease_next_job(connection, job_type="execution")
        assert leasable_ex is not None
        assert leasable_ex["id"] == ex_job_id
    finally:
        connection.close()


def test_queue_job_endpoints_round_trip(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, Any] = {}

    def fake_enqueue_job(connection, job_type, payload=None):
        captured["job_type"] = job_type
        captured["payload"] = payload
        return 42

    monkeypatch.setattr("app.api.routes.queue.enqueue_job", fake_enqueue_job)
    monkeypatch.setattr(
        "app.api.routes.queue.list_queue_jobs",
        lambda connection, limit=20, status=None, job_type=None: [
            {
                "id": 42,
                "job_type": job_type or "strategy",
                "status": status or "queued",
                "payload_json": json.dumps(
                    {"strategy_names": ["ppo"], "symbol_names": ["BTCUSDT"]},
                    sort_keys=True,
                ),
                "payload": {"strategy_names": ["ppo"], "symbol_names": ["BTCUSDT"]},
                "result_json": None,
                "result": None,
                "error_message": None,
                "attempt_count": 0,
                "created_at": "2026-03-19 10:00:00",
                "started_at": None,
                "completed_at": None,
            }
        ],
    )

    create_response = client.post(
        "/queue/jobs",
        json={
            "job_type": "strategy",
            "strategy_names": ["ppo", "ppo"],
            "symbol_names": ["BTCUSDT", "ETHUSDT", "BTCUSDT"],
            "payload": {"source": "admin"},
        },
    )
    list_response = client.get("/queue/jobs", params={"job_type": "strategy", "status": "queued"})

    assert create_response.status_code == 200
    assert create_response.json()["status"] == "queued"
    assert create_response.json()["job_id"] == 42
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
        "source": "admin",
        "strategy_names": ["ppo"],
        "symbol_names": ["BTCUSDT", "ETHUSDT"],
    }
    assert list_response.status_code == 200
    assert list_response.json()[0]["job_type"] == "strategy"
    assert list_response.json()[0]["status"] == "queued"


def test_clear_queue_batch_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    class DummyConnection:
        def close(self) -> None:
            return None

    monkeypatch.setattr("app.api.main.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.main.insert_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        "app.api.routes.queue.fail_batch_jobs",
        lambda connection, batch_id, **kwargs: [
            {
                "id": 11,
                "job_type": "strategy",
                "status": "failed",
                "payload": {"batch_id": batch_id},
                "result": {"cleared_batch_id": batch_id, "source": "admin_queue_clear"},
                "error_message": kwargs["error_message"],
            }
        ],
    )

    response = client.post("/queue/batches/batch-123/clear")

    assert response.status_code == 200
    assert response.json()["status"] == "cleared"
    assert response.json()["batch_id"] == "batch-123"
    assert response.json()["cleared_job_count"] == 1
    assert response.json()["jobs"][0]["error_message"] == "Queue batch cleared from admin."


def test_enqueue_pipeline_queue_jobs_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, Any] = {}

    def fake_enqueue_pipeline_jobs(connection, **kwargs):
        captured.update(kwargs)
        return [
            {"job_id": 10, "job_type": "market_data", "payload": kwargs},
            {"job_id": 11, "job_type": "strategy", "payload": kwargs},
            {"job_id": 12, "job_type": "risk", "payload": kwargs},
            {"job_id": 13, "job_type": "execution", "payload": kwargs},
        ]

    monkeypatch.setattr("app.api.routes.queue.enqueue_pipeline_jobs", fake_enqueue_pipeline_jobs)

    response = client.post(
        "/queue/jobs/enqueue-pipeline",
        json={
            "strategy_name": "ppo",
            "strategy_names": ["ppo", "ppo"],
            "symbol_names": ["BTCUSDT", "ETHUSDT"],
            "payload": {"source": "api_test"},
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["job_count"] == 4
    assert response.json()["job_types"] == ["market_data", "strategy", "risk", "execution"]
    assert captured["strategy_name"] == "ppo"
    assert captured["strategy_names"] == ["ppo", "ppo"]
    assert captured["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert captured["payload"] == {"source": "api_test"}


def test_run_next_pipeline_batch_endpoint(monkeypatch) -> None:
    client = TestClient(app)

    monkeypatch.setattr(
        "app.api.routes.queue.run_next_pipeline_batch",
        lambda connection: {
            "status": "completed",
            "batch_id": "batch-1234",
            "remaining_job_types": ["strategy", "risk", "execution"],
            "job": {"id": 10, "job_type": "market_data"},
        },
    )

    response = client.post("/queue/jobs/run-next-pipeline")

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["batch_id"] == "batch-1234"
    assert response.json()["remaining_job_types"] == ["strategy", "risk", "execution"]


def test_run_next_pipeline_batch_drains_oldest_job_in_batch(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        jobs = __import__("app.core.job_queue", fromlist=["enqueue_pipeline_jobs"]).enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["BTCUSDT"],
        )

        def fake_run_next_queued_job(conn, job_type=None):
            return {
                "status": "completed",
                "job": {"id": jobs[0]["job_id"], "job_type": job_type},
                "result": {"status": "ok"},
            }

        monkeypatch.setattr("app.core.pipeline_orchestration.run_next_queued_job", fake_run_next_queued_job)

        result = __import__("app.core.job_queue", fromlist=["run_next_pipeline_batch"]).run_next_pipeline_batch(connection)

        assert result["status"] == "completed"
        assert result["batch_id"] == jobs[0]["batch_id"]
        assert result["job"]["job_type"] == "market_data"
        assert result["remaining_job_types"] == ["strategy", "risk", "execution"]
    finally:
        connection.close()


def test_run_next_pipeline_batch_targets_requested_batch(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        older_jobs = enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["BTCUSDT"],
            payload={"source": "older_batch"},
        )
        newer_jobs = enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["ETHUSDT"],
            payload={"source": "newer_batch"},
        )
        leased_job_ids: list[int] = []

        def fake_run_leased_queue_job(conn, leased_job):
            leased_job_ids.append(int(leased_job["id"]))
            return {
                "status": "completed",
                "job": {"id": int(leased_job["id"]), "job_type": leased_job["job_type"]},
                "result": {"status": "ok"},
            }

        monkeypatch.setattr("app.core.pipeline_orchestration._run_leased_queue_job", fake_run_leased_queue_job)

        result = run_next_pipeline_batch(connection, batch_id=older_jobs[0]["batch_id"])

        assert result["status"] == "completed"
        assert result["batch_id"] == older_jobs[0]["batch_id"]
        assert result["job"]["id"] == older_jobs[0]["job_id"]
        assert result["remaining_job_types"] == ["strategy", "risk", "execution"]
        assert leased_job_ids == [older_jobs[0]["job_id"]]
        queued_jobs = list_jobs(connection, limit=10, status="queued")
        assert newer_jobs[0]["job_id"] in [job["id"] for job in queued_jobs]
    finally:
        connection.close()


def test_fail_batch_jobs_marks_only_requested_batch_as_failed() -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        target_jobs = enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["BTCUSDT"],
        )
        other_jobs = enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["ETHUSDT"],
        )

        failed_jobs = fail_batch_jobs(
            connection,
            target_jobs[0]["batch_id"],
            error_message="Queue batch cleared from admin.",
            result={"source": "admin_queue_clear"},
        )

        assert [job["id"] for job in failed_jobs] == [item["job_id"] for item in target_jobs]
        refreshed_target_jobs = [get_job(connection, item["job_id"]) for item in target_jobs]
        assert all(job is not None and job["status"] == "failed" for job in refreshed_target_jobs)
        assert all(job is not None and job["error_message"] == "Queue batch cleared from admin." for job in refreshed_target_jobs)
        refreshed_other_jobs = [get_job(connection, item["job_id"]) for item in other_jobs]
        assert all(job is not None and job["status"] == "queued" for job in refreshed_other_jobs)
    finally:
        connection.close()


def test_run_pipeline_batch_drains_full_batch(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        monkeypatch.setattr(
            "app.pipeline.runtime_summary.record_heartbeat",
            lambda component, status, message, payload=None: upsert_heartbeat(
                connection,
                component,
                status,
                message,
                payload,
            ),
        )
        jobs = enqueue_pipeline_jobs(
            connection,
            strategy_names=["ppo"],
            symbol_names=["BTCUSDT"],
        )
        completed = iter(
            [
                {
                    "status": "completed",
                    "batch_id": jobs[0]["batch_id"],
                    "remaining_job_types": ["strategy", "risk", "execution"],
                    "job": {"id": jobs[0]["job_id"], "job_type": "market_data"},
                    "result": {"steps": [{"step": "save_klines", "saved_klines": 5}]},
                    "execution_backend_status": {"backend": "paper"},
                },
                {
                    "status": "completed",
                    "batch_id": jobs[0]["batch_id"],
                    "remaining_job_types": ["risk", "execution"],
                    "job": {"id": jobs[1]["job_id"], "job_type": "strategy"},
                    "result": {"steps": [{"step": "generate_signal", "signal_type": "BUY"}]},
                    "execution_backend_status": {"backend": "paper"},
                },
                {
                    "status": "completed",
                    "batch_id": jobs[0]["batch_id"],
                    "remaining_job_types": ["execution"],
                    "job": {"id": jobs[2]["job_id"], "job_type": "risk"},
                    "result": {"steps": [{"step": "evaluate_risk", "decision": "APPROVED"}]},
                    "execution_backend_status": {"backend": "paper"},
                },
                {
                    "status": "completed",
                    "batch_id": jobs[0]["batch_id"],
                    "remaining_job_types": [],
                    "job": {"id": jobs[3]["job_id"], "job_type": "execution"},
                    "result": {"steps": [{"step": "paper_execute", "status": "FILLED", "side": "BUY"}]},
                    "execution_backend_status": {"backend": "paper"},
                },
            ]
        )

        monkeypatch.setattr("app.core.pipeline_orchestration.run_next_pipeline_batch", lambda conn: next(completed))

        result = run_pipeline_batch(connection)

        assert result["status"] == "completed"
        assert result["batch_id"] == jobs[0]["batch_id"]
        assert [job["job_type"] for job in result["jobs"]] == ["market_data", "strategy", "risk", "execution"]
        assert [step["step"] for step in result["result"]["steps"]] == [
            "save_klines",
            "generate_signal",
            "evaluate_risk",
            "paper_execute",
        ]
        assert result["remaining_job_types"] == []
        heartbeats = get_heartbeats(connection)
        pipeline_heartbeat = next(item for item in heartbeats if item["component"] == "pipeline")
        assert pipeline_heartbeat["status"] == "completed"
        assert pipeline_heartbeat["message"] == "Pipeline run completed."
        payload = json.loads(pipeline_heartbeat["payload_json"]) if pipeline_heartbeat["payload_json"] else {}
        assert payload["strategy_names"] == ["ppo"]
        assert payload["symbol_names"] == ["BTCUSDT"]
        assert payload["generated_signal_count"] == 1
        assert payload["approved_risk_count"] == 1
        assert payload["filled_execution_count"] == 1
    finally:
        connection.close()


def test_run_next_queued_job_completes_strategy_job(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(
            connection,
            "strategy",
            payload={"strategy_names": ["ppo", "ppo"], "symbol_names": ["BTCUSDT"]},
        )

        def fake_run_strategy_jobs(conn, strategy_names, symbol_names=None):
            assert strategy_names == ["ppo", "ppo"]
            assert symbol_names == ["BTCUSDT"]
            return {
                "status": "ok",
                "strategy_names": strategy_names,
                "symbol_names": symbol_names,
                "steps": [{"step": "generate_signal", "strategy_name": "ppo", "symbol": "BTCUSDT", "signal_type": "BUY"}],
            }

        monkeypatch.setattr("app.core.job_runner.run_strategy_jobs", fake_run_strategy_jobs)

        result = run_next_queued_job(connection, job_type="strategy")

        assert result["status"] == "completed"
        assert result["job"]["id"] == job_id
        assert result["job"]["status"] == "completed"
        assert result["result"]["strategy_names"] == ["ppo", "ppo"]
        assert result["result"]["execution_backend_status"]["backend"] == "paper"
        assert result["execution_backend_status"]["backend"] == "paper"
        assert get_job(connection, job_id)["status"] == "completed"  # type: ignore[index]
    finally:
        connection.close()


def test_run_next_queued_job_marks_failure(monkeypatch) -> None:
    connection = make_connection()
    try:
        run_migrations(connection)
        job_id = enqueue_job(connection, "market_data", payload={"symbol_names": ["BTCUSDT"]})

        def fake_run_market_data_job(conn, symbol_names=None):
            raise RuntimeError("market data unavailable")

        monkeypatch.setattr("app.core.job_runner.run_market_data_job", fake_run_market_data_job)

        result = run_next_queued_job(connection, job_type="market_data")

        assert result["status"] == "failed"
        assert result["job"]["id"] == job_id
        assert result["job"]["status"] == "failed"
        assert result["job"]["error_message"] == "market data unavailable"
        assert result["error_type"] == "RuntimeError"
        assert result["execution_backend_status"]["backend"] == "paper"
        assert result["job"]["result"]["execution_backend_status"]["backend"] == "paper"
    finally:
        connection.close()


def test_run_next_queue_job_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, Any] = {}

    def fake_run_next_queued_job(connection, job_type=None):
        captured["job_type"] = job_type
        return {"status": "completed", "job": {"id": 9, "job_type": job_type or "strategy"}}

    monkeypatch.setattr("app.api.routes.queue.run_next_queued_job", fake_run_next_queued_job)

    response = client.post("/queue/jobs/run-next", json={"job_type": "strategy"})

    assert response.status_code == 200
    assert captured["job_type"] == "strategy"
    assert response.json()["status"] == "completed"
    assert response.json()["job"]["id"] == 9


def test_retry_queue_job_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        "app.api.routes.queue.retry_job",
        lambda connection, job_id: {
            "id": job_id,
            "job_type": "strategy",
            "status": "queued",
            "payload": {"strategy_names": ["ppo"]},
        },
    )

    response = client.post("/queue/jobs/42/retry")

    assert response.status_code == 200
    assert response.json()["status"] == "queued"
    assert response.json()["job_id"] == 42
    assert response.json()["job"]["status"] == "queued"


def test_clear_queue_batch_endpoint_logs_audit_event(monkeypatch) -> None:
    client = TestClient(app)
    audit_calls: list[dict[str, Any]] = []

    class DummyConnection:
        def close(self) -> None:
            return None

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr(
        "app.api.routes.queue.insert_event",
        lambda connection, event_type, status, source, message, payload=None: audit_calls.append(
            {"event_type": event_type, "status": status, "source": source, "message": message, "payload": payload}
        ),
    )
    monkeypatch.setattr(
        "app.api.routes.queue.fail_batch_jobs",
        lambda connection, batch_id, **kwargs: [
            {
                "id": 11,
                "job_type": "strategy",
                "status": "failed",
                "payload": {"batch_id": batch_id},
                "result": {"cleared_batch_id": batch_id, "source": "admin_queue_clear"},
                "error_message": kwargs["error_message"],
            }
        ],
    )

    response = client.post("/queue/batches/batch-123/clear")

    assert response.status_code == 200
    assert response.json()["status"] == "cleared"
    assert response.json()["batch_id"] == "batch-123"
    assert response.json()["cleared_job_count"] == 1
    assert audit_calls[0]["event_type"] == "queue_control"
    assert audit_calls[0]["status"] == "cleared"
    assert audit_calls[0]["payload"]["action"] == "clear_pipeline_batch"
    assert audit_calls[0]["payload"]["batch_id"] == "batch-123"


def test_reconcile_orders_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    audit_calls: list[dict[str, Any]] = []

    class DummyConnection:
        def close(self) -> None:
            return None

    monkeypatch.setattr("app.api.deps.get_connection", lambda: DummyConnection())
    monkeypatch.setattr("app.api.deps.run_migrations", lambda connection: None)
    monkeypatch.setattr("app.api.routes.orders.reconcile_orphan_orders", lambda connection, **kwargs: [])
    monkeypatch.setattr("app.api.routes.orders.update_positions", lambda connection: 2)
    monkeypatch.setattr("app.api.routes.orders.update_pnl_snapshots", lambda connection: 3)
    monkeypatch.setattr("app.api.routes.orders.get_orders", lambda connection, limit=5: [{"id": 11, "status": "PENDING"}])
    monkeypatch.setattr(
        "app.api.routes.orders.insert_event",
        lambda connection, event_type, status, source, message, payload=None: audit_calls.append(
            {"event_type": event_type, "status": status, "source": source, "message": message, "payload": payload}
        ),
    )

    response = client.post(
        "/orders/reconcile",
        json={
            "audit_action": "broker_protection:reconcile_orders",
            "audit_message": "Order reconciliation triggered from broker protection recommendation.",
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "reconciled"
    assert response.json()["updated_symbols"] == 2
    assert response.json()["snapshot_count"] == 3
    assert response.json()["orders"] == [{"id": 11, "status": "PENDING"}]
    assert audit_calls[0]["event_type"] == "execution_control"
    assert audit_calls[0]["status"] == "reconciled"
    assert audit_calls[0]["payload"]["action"] == "broker_protection:reconcile_orders"
    assert audit_calls[0]["message"] == "Order reconciliation triggered from broker protection recommendation."


def test_run_market_data_job_supports_multiple_symbols(monkeypatch) -> None:
    saved_calls: list[tuple[str, list[list]]] = []

    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda symbol="BTCUSDT", interval="1m", limit=5: [[1, "1", "2", "0", "1", "10", 2, "20", 1, "5", "10"]],
    )
    monkeypatch.setattr("app.pipeline.market_data_job.get_latest_open_time", lambda connection, symbol, timeframe: None)

    def fake_save_klines(connection, klines, symbol="BTCUSDT", timeframe="1m"):
        saved_calls.append((symbol, klines))
        return len(klines)

    monkeypatch.setattr("app.pipeline.market_data_job.save_klines", fake_save_klines)
    monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT", "ETHUSDT"])
    monkeypatch.setattr("app.scheduler.control.read_active_timeframes", lambda: ["1m"])

    result = run_market_data_job(connection=None)  # type: ignore[arg-type]

    assert result["saved_klines"] == 2
    assert result["symbol_names"] == ["BTCUSDT", "ETHUSDT"]
    assert result["timeframes"] == ["1m"]
    assert result["symbol_results"] == [
        {"symbol": "BTCUSDT", "timeframe": "1m", "saved_klines": 1, "mode": "seed"},
        {"symbol": "ETHUSDT", "timeframe": "1m", "saved_klines": 1, "mode": "seed"},
    ]
    assert [symbol for symbol, _ in saved_calls] == ["BTCUSDT", "ETHUSDT"]


def test_scheduler_strategy_limit_preset_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "app.api.routes.scheduler.get_strategy_status",
        lambda: {
            "strategy_name": "ppo",
            "strategy_names": ["ppo", "ppo"],
            "disabled_strategy_names": [],
            "effective_strategy_names": ["ppo", "ppo"],
            "effective_strategy_limit": None,
            "strategy_priorities": {"ppo": 0, "ppo": 1},
            "disabled_strategy_notes": {},
            "default_strategy": "ppo",
            "strategy_file": "runtime/scheduler.strategy",
            "disabled_strategy_file": "runtime/scheduler.strategy.disabled",
            "priority_file": "runtime/scheduler.strategy.priority.json",
            "disabled_reason_file": "runtime/scheduler.strategy.disabled.reason.json",
            "effective_limit_file": "runtime/scheduler.strategy.limit",
            "available_strategies": ["ppo", "ppo"],
        },
    )
    def fake_set_effective_strategy_limit(limit, **kwargs):
        captured["limit"] = limit
        captured["kwargs"] = kwargs
        return {"effective_strategy_limit": limit}

    monkeypatch.setattr("app.api.routes.scheduler.set_effective_strategy_limit", fake_set_effective_strategy_limit)

    response = client.post("/scheduler/strategy/limit-preset", json={"preset": "top_2"})

    assert response.status_code == 200
    assert captured["limit"] == 2
    assert captured["kwargs"]["audit_action"] == "limit_preset:top_2"
    assert captured["kwargs"]["extra_payload"] == {"preset": "top_2"}
    assert response.json()["strategy_names"] == ["ppo", "ppo"]


def test_favicon_returns_no_content() -> None:
    client = TestClient(app)

    response = client.get("/favicon.ico")

    assert response.status_code == 204


def test_alerts_status_reports_configuration(monkeypatch) -> None:
    monkeypatch.setattr("app.api.routes.execution.telegram_configured", lambda: True)
    client = TestClient(app)

    response = client.get("/alerts/status")

    assert response.status_code == 200
    assert response.json() == {"telegram_configured": True}


def test_alerts_test_endpoint_returns_sender_result(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.api.routes.execution.send_telegram_message",
        lambda text: {"sent": True, "response": {"ok": True, "text": text}},
    )
    client = TestClient(app)

    response = client.post("/alerts/test", json={"message": "hello"})

    assert response.status_code == 200
    assert response.json()["sent"] is True
    assert response.json()["response"]["text"] == "hello"


def test_send_telegram_message_returns_not_configured_when_env_missing(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_BOT_TOKEN", "")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_CHAT_ID", "")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_MESSAGE_STATE_FILE", tmp_path / "telegram_state.json")
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


def test_send_telegram_message_returns_failure_instead_of_raising(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_CHAT_ID", "chat")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_MESSAGE_STATE_FILE", tmp_path / "telegram_state.json")
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


def test_send_telegram_message_logs_successful_delivery(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_BOT_TOKEN", "token")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_CHAT_ID", "chat")
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_MESSAGE_STATE_FILE", tmp_path / "telegram_state.json")
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
    monkeypatch.setattr("app.alerting.telegram.TELEGRAM_MESSAGE_STATE_FILE", tmp_path / "telegram_state.json")
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
        run_migrations(connection)
        upsert_heartbeat(connection, "scheduler", "ok", "Scheduler loop completed.")
    finally:
        connection.close()

    monkeypatch.setattr("app.api.main._health_cache", {})
    monkeypatch.setattr("app.api.main._health_cache_ts", 0.0)
    monkeypatch.setattr(
        "app.health.checks.get_database_info",
        lambda: {"backend": "postgres", "database_url": "postgresql://test/db"},
    )
    monkeypatch.setattr("app.health.checks.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.health.checks.get_stop_status",
        lambda: {"stopped": False, "stop_file": str(tmp_path / "scheduler.stop")},
    )
    monkeypatch.setattr(
        "app.health.checks.get_kill_switch_status",
        lambda: {"enabled": False, "kill_switch_file": str(tmp_path / "kill.switch")},
    )
    monkeypatch.setattr("app.health.checks.read_scheduler_log", lambda lines=1: [])

    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["database_info"]["backend"] == "postgres"
    assert payload["database_info"]["database_url"] == "postgresql://test/db"
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

    monkeypatch.setattr("app.health.checks.get_connection", lambda: DummyConnection())
    monkeypatch.setattr(
        "app.health.checks.get_database_info",
        lambda: {
            "backend": "postgres",
            "database_url": "postgresql://crypto:crypto@127.0.0.1:5432/crypto",
        },
    )
    monkeypatch.setattr("app.health.checks.utc_now", lambda: datetime.fromtimestamp(0, tz=timezone.utc))
    monkeypatch.setattr(
        "app.health.checks.get_stop_status",
        lambda: {"stopped": False, "stop_file": "runtime/scheduler.stop"},
    )
    monkeypatch.setattr("app.health.checks.read_scheduler_log", lambda lines=1: [])
    monkeypatch.setattr(
        "app.health.checks.get_kill_switch_status",
        lambda: {"enabled": False, "kill_switch_file": "runtime/kill.switch"},
    )

    payload = app.openapi()  # keep app imported/initialized
    del payload
    report = __import__("app.health.checks", fromlist=["build_health_report"]).build_health_report()

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


def test_scheduler_blocks_when_kill_switch_enabled(monkeypatch) -> None:
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: True)
    monkeypatch.setattr(
        "app.scheduler.runner.get_kill_switch_status",
        lambda: {"enabled": True, "kill_switch_file": "runtime/kill.switch"},
    )

    from app.scheduler.runner import _run_scheduled_job

    for mode in ("pipeline", "market-data-only", "strategy-only", "risk-only", "execution-only"):
        result = _run_scheduled_job(mode)
        assert result["status"] == "blocked", f"mode={mode} should be blocked"
        assert result["steps"][0]["step"] == "kill_switch"
        assert result["steps"][0]["status"] == "blocked"
        assert result["steps"][0]["enabled"] is True


def test_scheduler_proceeds_when_kill_switch_disabled(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr("app.scheduler.runner.kill_switch_enabled", lambda: False)
    ran = []
    monkeypatch.setattr(
        "app.scheduler.runner.run_pipeline_collect",
        lambda **kwargs: ran.append("pipeline") or {"status": "ok", "steps": []},
    )

    from app.scheduler.runner import _run_scheduled_job

    result = _run_scheduled_job("pipeline")
    assert result["status"] == "ok"
    assert ran == ["pipeline"]


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
    monkeypatch.setattr("app.api.deps.get_connection", lambda: sqlite3.connect(db_path))

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

    monkeypatch.setattr("app.pipeline.run_pipeline.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.audit.service.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr("app.system.heartbeat.get_connection", lambda: sqlite3.connect(db_path))
    monkeypatch.setattr(
        "app.pipeline.market_data_job.fetch_klines",
        lambda symbol="BTCUSDT", interval="1m", limit=5: [
            make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])
        ],
    )
    monkeypatch.setattr("app.pipeline.run_pipeline.kill_switch_enabled", lambda: False)
    from app.strategy.signal_service import insert_signal as _insert_signal
    monkeypatch.setattr(
        "app.pipeline.strategy_job.generate_registered_signal",
        lambda conn, strategy_name="ppo", symbol="BTCUSDT", timeframe="1m": _insert_signal(
            conn, "BUY", symbol=symbol, timeframe=timeframe, strategy_name=strategy_name,
            short_ma=12.0, long_ma=10.0,
        ),
    )

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
        run_migrations(connection)
        monkeypatch.setattr("app.scheduler.control.read_active_symbols", lambda: ["BTCUSDT"])
        monkeypatch.setattr(
            "app.pipeline.market_data_job.fetch_klines",
            lambda symbol="BTCUSDT", interval="1m", limit=5: [
                make_kline((index + 1) * 60_000, close) for index, close in enumerate([10, 11, 12, 13, 14])
            ],
        )
        monkeypatch.setattr(
            "app.pipeline.strategy_job.generate_registered_signal",
            lambda conn, strategy_name="ppo", symbol="BTCUSDT", timeframe="1m": {
                "id": 1,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_name": strategy_name,
                "signal_type": "BUY",
                "short_ma": 13.0,
                "long_ma": 12.0,
            },
        )
        monkeypatch.setattr(
            "app.pipeline.risk_job.evaluate_signal_ids",
            lambda conn, signal_ids, **kw: [
                {
                    "id": sid,
                    "signal_id": sid,
                    "symbol": "BTCUSDT",
                    "timeframe": "1m",
                    "strategy_name": "ppo",
                    "signal_type": "BUY",
                    "decision": "APPROVED",
                    "reason": "Passed basic risk checks.",
                }
                for sid in signal_ids
            ],
        )

        from app.pipeline.risk_job import run_risk_job

        market_result = run_market_data_job(connection)
        strategy_result = run_strategy_job(connection)
        risk_result = run_risk_job(connection, signal_ids=strategy_result.get("signal_ids"))
        execution_result = run_execution_job(connection, risk_event_ids=risk_result.get("risk_event_ids"))

        assert market_result == {
            "step": "save_klines",
            "saved_klines": 5,
            "symbol_names": ["BTCUSDT"],
            "timeframes": ["1m"],
            "symbol_results": [{"symbol": "BTCUSDT", "timeframe": "1m", "saved_klines": 5, "mode": "seed"}],
        }
        assert [step["step"] for step in strategy_result["steps"]] == ["generate_signal"]
        assert [step["step"] for step in risk_result["steps"]] == ["evaluate_risk"]
        assert [step["step"] for step in execution_result["steps"]] == ["paper_execute", "update_positions", "update_pnl", "reconcile_orphan_orders"]
    finally:
        connection.close()

