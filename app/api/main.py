import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Literal, List, Optional, Union

import requests
from fastapi import BackgroundTasks
from fastapi import FastAPI, HTTPException, Query
from fastapi import Response
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from app.audit.service import log_event
from app.alerting.broker import maybe_send_broker_alert
from app.alerting.execution import maybe_send_execution_alert
from app.admin.page import render_admin_page
from app.alerting.health import maybe_send_health_alert
from app.alerting.queue import maybe_send_queue_alert
from app.alerting.telegram import send_telegram_message
from app.alerting.telegram import telegram_configured
from app.alerting.worker import maybe_send_worker_alert
from app.core.db import DB_FILE, get_connection
from app.core.db import DBConnection
from app.core.db import DBError
from app.core.db import get_database_info
from app.core.db import parse_db_timestamp
from app.core.db import list_tables
from app.core.db import table_exists
from app.core.job_queue import build_job_payload
from app.core.job_queue import JOB_TYPES
from app.core.job_queue import enqueue_pipeline_jobs
from app.core.job_queue import enqueue_job
from app.core.job_queue import enqueue_and_run_pipeline_batch
from app.core.job_queue import fail_batch_jobs
from app.core.job_queue import list_jobs as list_queue_jobs
from app.core.job_queue import retry_job
from app.core.job_queue import run_pipeline_batch
from app.core.job_queue import run_next_pipeline_batch
from app.core.job_queue import run_next_queued_job
from app.core.migrations import run_migrations
from app.core.settings import CANDLE_STALENESS_SECONDS
from app.core.settings import COOLDOWN_SECONDS
from app.core.settings import DEFAULT_PIPELINE_ORCHESTRATION
from app.core.settings import ORDER_STALENESS_SECONDS
from app.core.settings import QUEUE_BATCH_STALENESS_SECONDS
from app.core.settings import RISK_REJECTION_STREAK_THRESHOLD
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.data.symbols import DEFAULT_SYMBOL
from app.core.settings import DEFAULT_ORDER_QTY
from app.execution.adapter import get_execution_backend_status
from app.execution.adapter import get_execution_adapter_name
from app.execution.runtime import get_execution_backend_runtime_status
from app.execution.runtime import set_execution_backend
from app.core.settings import MAX_DAILY_LOSS
from app.core.settings import WORKER_HEARTBEAT_STALENESS_SECONDS
from app.core.settings import MAX_POSITION_QTY
from app.execution.adapter import get_execution_adapter
from app.pipeline.execution_job import reconcile_orphan_orders
from app.pipeline.run_pipeline import run_pipeline_collect
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.portfolio.positions_service import update_positions
from app.query.read_service import get_candles
from app.query.read_service import get_audit_events
from app.query.read_service import get_fills
from app.query.read_service import get_job_queue_summary
from app.query.read_service import get_orders
from app.query.read_service import get_pnl_snapshots
from app.query.read_service import get_positions
from app.query.read_service import get_risk_events
from app.portfolio.portfolio_service import get_portfolio_config
from app.portfolio.portfolio_service import get_portfolio_summary
from app.portfolio.portfolio_service import set_portfolio_config
from app.risk.risk_config import delete_risk_config
from app.risk.risk_config import get_risk_config
from app.risk.risk_config import list_risk_configs
from app.risk.risk_config import set_risk_config
from app.query.read_service import get_strategy_closed_trades
from app.query.read_service import get_signals
from app.query.read_service import get_strategy_activity_summary
from app.scheduler.control import clear_stop_flag
from app.scheduler.control import build_strategy_priority_preset
from app.scheduler.control import get_symbol_status
from app.scheduler.control import get_strategy_status
from app.scheduler.control import get_stop_status
from app.scheduler.control import read_scheduler_log
from app.scheduler.control import set_active_symbols
from app.scheduler.control import set_active_strategy
from app.scheduler.control import set_active_strategies
from app.scheduler.control import set_disabled_strategies
from app.scheduler.control import set_disabled_strategy_notes
from app.scheduler.control import set_effective_strategy_limit
from app.scheduler.control import set_strategy_priorities
from app.scheduler.control import set_stop_flag
from app.scheduler.runner import LOG_DIR
from app.scheduler.runner import LOG_FILE
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.ma_cross import insert_signal
from app.strategy.registry import list_registered_strategies
from app.system.kill_switch import disable_kill_switch
from app.system.heartbeat import get_heartbeats
from app.system.kill_switch import enable_kill_switch
from app.system.kill_switch import get_kill_switch_status
from app.metrics.metrics_service import build_metrics
from app.backtest.history_service import compare_runs as compare_backtest_runs
from app.backtest.history_service import get_best_sweep_run
from app.backtest.history_service import get_champion_run
from app.backtest.history_service import get_equity_curve as get_backtest_equity_curve
from app.backtest.history_service import get_run as get_backtest_run
from app.backtest.history_service import get_walk_forward_group
from app.backtest.history_service import leaderboard_runs as leaderboard_backtest_runs
from app.backtest.history_service import list_experiments as list_backtest_experiments
from app.backtest.history_service import list_runs as list_backtest_runs
from app.backtest.history_service import list_walk_forward_groups
from app.backtest.history_service import persist_run as persist_backtest_run
from app.backtest.history_service import promote_run as promote_backtest_run
from app.backtest.history_service import update_run as update_backtest_run
from app.backtest.loader import load_candles_from_db
from app.backtest.runner import run_backtest
from app.backtest.sweep import run_parameter_sweep
from app.backtest.walk_forward import run_walk_forward
from app.validation.soak_history import read_soak_validation_history
from app.validation.soak_history import record_soak_validation_snapshot
from app.validation.soak_history import build_soak_history_summary
from app.validation.soak_report import build_soak_validation_report


@asynccontextmanager
async def lifespan(_: FastAPI):
    connection = get_connection()
    try:
        run_migrations(connection)
    finally:
        connection.close()
    yield


app = FastAPI(title="Crypto Trading MVP API", lifespan=lifespan)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _database_check(connection: DBConnection) -> dict[str, Any]:
    database_info = get_database_info()
    table_names = list_tables(connection)
    return {
        "status": "ok",
        "database": database_info.get("database_url", str(DB_FILE)),
        "table_count": len(table_names),
        "tables": table_names,
    }


def _candle_check(connection: DBConnection) -> dict[str, Any]:
    latest_candle = connection.execute(
        """
        SELECT symbol, timeframe, open_time, close_time
        FROM candles
        ORDER BY open_time DESC
        LIMIT 1;
        """
    ).fetchone()
    if latest_candle is None:
        return {
            "status": "degraded",
            "reason": "No candle data found.",
        }

    symbol, timeframe, open_time, close_time = latest_candle
    latest_close_at = datetime.fromtimestamp(int(close_time) / 1000, tz=timezone.utc)
    age_seconds = int((_utc_now() - latest_close_at).total_seconds())
    status = "ok" if age_seconds <= CANDLE_STALENESS_SECONDS else "degraded"
    return {
        "status": status,
        "symbol": symbol,
        "timeframe": timeframe,
        "latest_open_time": int(open_time),
        "latest_close_time": int(close_time),
        "latest_close_at": latest_close_at.isoformat(),
        "age_seconds": age_seconds,
        "staleness_threshold_seconds": CANDLE_STALENESS_SECONDS,
    }


def _scheduler_check() -> dict[str, Any]:
    stop_status = get_stop_status()
    log_lines = read_scheduler_log(lines=1)
    status = "degraded" if stop_status["stopped"] else "ok"
    result: dict[str, Any] = {
        "status": status,
        "stopped": stop_status["stopped"],
        "stop_file": stop_status["stop_file"],
        "log_file": str(LOG_DIR),
        "log_exists": bool(log_lines) or LOG_FILE.exists(),
        "last_log_line": log_lines[0] if log_lines else None,
    }
    if stop_status["stopped"]:
        result["reason"] = "Scheduler stop flag is set."
    elif not log_lines:
        result["status"] = "degraded"
        result["reason"] = "Scheduler log is empty."
    return result


def _kill_switch_check() -> dict[str, Any]:
    kill_switch_status = get_kill_switch_status()
    result: dict[str, Any] = {
        "status": "degraded" if kill_switch_status["enabled"] else "ok",
        "enabled": kill_switch_status["enabled"],
        "kill_switch_file": kill_switch_status["kill_switch_file"],
    }
    if kill_switch_status["enabled"]:
        result["reason"] = "Kill switch is enabled."
    return result


def _execution_backend_check() -> dict[str, Any]:
    backend_status = get_execution_backend_status()
    return {
        "status": "ok",
        "backend": backend_status["backend"],
        "dry_run": backend_status["dry_run"],
        "can_execute_orders": backend_status["can_execute_orders"],
        "is_live": backend_status.get("is_live", False),
        "placeholder": backend_status.get("placeholder", False),
    }


def _broker_protection_check(
    connection: DBConnection,
    execution_backend_check: dict[str, Any],
    pipeline_check: dict[str, Any],
) -> dict[str, Any]:
    latest_run = pipeline_check.get("latest_run", {}) if isinstance(pipeline_check, dict) else {}
    latest_order = pipeline_check.get("latest_order") if isinstance(pipeline_check, dict) else None
    approved_risk_count = int(latest_run.get("approved_risk_count") or 0) if isinstance(latest_run, dict) else 0
    result: dict[str, Any] = {
        "status": "ok",
        "backend": execution_backend_check.get("backend"),
        "can_execute_orders": bool(execution_backend_check.get("can_execute_orders")),
        "dry_run": bool(execution_backend_check.get("dry_run")),
        "placeholder": bool(execution_backend_check.get("placeholder")),
        "approved_risk_count": approved_risk_count,
        "order_staleness_threshold_seconds": ORDER_STALENESS_SECONDS,
        "risk_rejection_streak_threshold": RISK_REJECTION_STREAK_THRESHOLD,
        "severity": "info",
        "reason_code": None,
        "recommended_action": None,
    }
    if isinstance(latest_order, dict):
        result["latest_order"] = latest_order
    latest_fill = pipeline_check.get("latest_fill") if isinstance(pipeline_check, dict) else None
    if isinstance(latest_fill, dict):
        result["latest_fill"] = latest_fill
    latest_risk = pipeline_check.get("latest_risk") if isinstance(pipeline_check, dict) else None
    if isinstance(latest_risk, dict):
        result["latest_risk"] = latest_risk

    if not result["can_execute_orders"] and approved_risk_count > 0:
        result["status"] = "degraded"
        result["severity"] = "critical"
        result["reason_code"] = "backend_cannot_execute"
        result["reason"] = "Execution backend cannot execute approved orders."
        result["recommended_action"] = "switch_to_paper_backend"
        return result

    if result["placeholder"] and approved_risk_count > 0:
        result["status"] = "degraded"
        result["severity"] = "high"
        result["reason_code"] = "placeholder_backend_pending_orders"
        result["reason"] = "Execution backend is placeholder while approved orders are pending."
        result["recommended_action"] = "switch_to_paper_backend"
        return result

    if isinstance(latest_order, dict):
        latest_order_status = str(latest_order.get("status") or "").upper()
        latest_order_age = latest_order.get("age_seconds")
        if latest_order_status in {"NEW", "PENDING", "SUBMITTED", "PARTIALLY_FILLED"} and latest_order_age is not None and int(latest_order_age) > ORDER_STALENESS_SECONDS:
            result["status"] = "degraded"
            result["severity"] = "high" if latest_order_status == "PARTIALLY_FILLED" else "medium"
            result["reason_code"] = f"stale_order_{latest_order_status.lower()}"
            result["reason"] = "Latest order is stale and still not terminal."
            result["recommended_action"] = "inspect_and_reconcile_orders"
            return result

    unfilled_order_count = int(pipeline_check.get("unfilled_order_count") or 0) if isinstance(pipeline_check, dict) else 0
    if unfilled_order_count > 0:
        result["status"] = "degraded"
        result["severity"] = "high"
        result["reason_code"] = "unfilled_orders_detected"
        result["reason"] = f"{unfilled_order_count} order(s) have no corresponding fill."
        result["recommended_action"] = "inspect_and_reconcile_orders"
        result["unfilled_order_count"] = unfilled_order_count
        return result

    rejection_rows = connection.execute(
        """
        SELECT decision, reason
        FROM risk_events
        ORDER BY id DESC
        LIMIT ?;
        """,
        (RISK_REJECTION_STREAK_THRESHOLD,),
    ).fetchall()
    if len(rejection_rows) >= RISK_REJECTION_STREAK_THRESHOLD and all(str(row[0]).upper() == "REJECTED" for row in rejection_rows):
        latest_rejection_reason = str(rejection_rows[0][1] or "")
        result["status"] = "degraded"
        result["rejected_risk_streak"] = len(rejection_rows)
        result["latest_rejection_reason"] = latest_rejection_reason
        if "Daily loss limit breached" in latest_rejection_reason:
            result["severity"] = "critical"
            result["reason_code"] = "risk_reject_daily_loss_limit"
            result["recommended_action"] = "enable_kill_switch"
        elif "Cooldown active" in latest_rejection_reason:
            result["severity"] = "medium"
            result["reason_code"] = "risk_reject_cooldown_streak"
            result["recommended_action"] = "pause_scheduler"
        else:
            result["severity"] = "medium"
            result["reason_code"] = "risk_reject_streak"
            result["recommended_action"] = "inspect_risk_rules"
        result["reason"] = "Recent risk evaluations are repeatedly rejected."
        return result

    return result


def _pipeline_check(connection: DBConnection) -> dict[str, Any]:
    required_tables = ("signals", "risk_events", "orders")
    missing_tables = [table_name for table_name in required_tables if not table_exists(connection, table_name)]
    if missing_tables:
        return {
            "status": "degraded",
            "reason": f"Pipeline tables missing: {', '.join(missing_tables)}.",
        }

    latest_signal = connection.execute(
        """
        SELECT signal_type, created_at
        FROM signals
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    latest_risk = connection.execute(
        """
        SELECT decision, reason, created_at
        FROM risk_events
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    latest_order = connection.execute(
        """
        SELECT side, symbol, qty, status, created_at
        FROM orders
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    latest_fill = connection.execute(
        """
        SELECT symbol, side, qty, price, created_at
        FROM fills
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    unfilled_order_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM orders o
        LEFT JOIN fills f ON f.order_id = o.id
        WHERE f.id IS NULL
          AND o.status NOT IN ('CANCELLED', 'REJECTED', 'EXPIRED');
        """
    ).fetchone()
    pipeline_heartbeat = connection.execute(
        """
        SELECT status, message, payload_json, last_seen_at
        FROM runtime_heartbeats
        WHERE component = 'pipeline'
        LIMIT 1;
        """
    ).fetchone()

    if latest_signal is None and latest_risk is None and latest_order is None and pipeline_heartbeat is None:
        return {
            "status": "degraded",
            "reason": "No pipeline activity found yet.",
        }

    result: dict[str, Any] = {"status": "ok"}
    if pipeline_heartbeat is not None:
        heartbeat_payload = json.loads(pipeline_heartbeat[2]) if pipeline_heartbeat[2] else {}
        result["latest_run"] = {
            "status": pipeline_heartbeat[0],
            "message": pipeline_heartbeat[1],
            "created_at": pipeline_heartbeat[3],
            "age_seconds": int((_utc_now() - parse_db_timestamp(pipeline_heartbeat[3])).total_seconds()),
            "step_count": heartbeat_payload.get("step_count"),
            "strategy_name": heartbeat_payload.get("strategy_name"),
            "strategy_names": heartbeat_payload.get("strategy_names", []),
            "symbol_names": heartbeat_payload.get("symbol_names", []),
            "execution_backend": heartbeat_payload.get("execution_backend") or get_execution_adapter_name(),
            "execution_backend_status": heartbeat_payload.get("execution_backend_status") or get_execution_backend_status(),
            "generated_signal_count": heartbeat_payload.get("generated_signal_count"),
            "approved_risk_count": heartbeat_payload.get("approved_risk_count"),
            "rejected_risk_count": heartbeat_payload.get("rejected_risk_count"),
            "filled_execution_count": heartbeat_payload.get("filled_execution_count"),
        }
    if latest_signal is not None:
        result["latest_signal"] = {
            "signal_type": latest_signal[0],
            "created_at": latest_signal[1],
            "age_seconds": int((_utc_now() - parse_db_timestamp(latest_signal[1])).total_seconds()),
        }
    if latest_risk is not None:
        result["latest_risk"] = {
            "decision": latest_risk[0],
            "reason": latest_risk[1],
            "created_at": latest_risk[2],
            "age_seconds": int((_utc_now() - parse_db_timestamp(latest_risk[2])).total_seconds()),
        }
    if latest_order is not None:
        result["latest_order"] = {
            "side": latest_order[0],
            "symbol": latest_order[1],
            "qty": latest_order[2],
            "status": latest_order[3],
            "created_at": latest_order[4],
            "age_seconds": int((_utc_now() - parse_db_timestamp(latest_order[4])).total_seconds()),
        }
    if latest_fill is not None:
        result["latest_fill"] = {
            "symbol": latest_fill[0],
            "side": latest_fill[1],
            "qty": latest_fill[2],
            "price": latest_fill[3],
            "created_at": latest_fill[4],
            "age_seconds": int((_utc_now() - parse_db_timestamp(latest_fill[4])).total_seconds()),
        }
    result["unfilled_order_count"] = int(unfilled_order_count[0]) if unfilled_order_count else 0
    return result


def _queue_check(connection: DBConnection) -> dict[str, Any]:
    summary = get_job_queue_summary(connection)
    counts = summary["counts"]
    latest_incomplete_batch = summary.get("latest_incomplete_batch")
    stale_batch = bool(
        latest_incomplete_batch
        and latest_incomplete_batch.get("age_seconds") is not None
        and int(latest_incomplete_batch["age_seconds"]) > QUEUE_BATCH_STALENESS_SECONDS
    )
    status = "degraded" if counts["failed"] > 0 or stale_batch else "ok"
    result: dict[str, Any] = {
        "status": status,
        "counts": counts,
        "latest_failed_job": summary.get("latest_failed_job"),
        "latest_retry_job": summary.get("latest_retry_job"),
        "latest_incomplete_batch": latest_incomplete_batch,
        "latest_completed_batch": summary.get("latest_completed_batch"),
        "recent_batches": summary.get("recent_batches", []),
        "latest_jobs": summary["latest_jobs"],
        "batch_staleness_threshold_seconds": QUEUE_BATCH_STALENESS_SECONDS,
    }
    if counts["failed"] > 0:
        result["reason"] = "Queue contains failed jobs."
    elif stale_batch:
        result["reason"] = "Queue contains stale incomplete batches."
    return result


def _heartbeat_check(connection: DBConnection) -> dict[str, Any]:
    heartbeats = get_heartbeats(connection)
    if not heartbeats:
        return {
            "status": "degraded",
            "reason": "No runtime heartbeats recorded yet.",
            "components": [],
        }

    worker_components = {"data_worker", "strategy_worker", "risk_worker", "execution_worker"}
    enriched_heartbeats: list[dict[str, Any]] = []
    stale_workers: list[dict[str, Any]] = []
    degraded: list[dict[str, Any]] = []
    for item in heartbeats:
        heartbeat = dict(item)
        age_seconds = int((_utc_now() - parse_db_timestamp(item["last_seen_at"])).total_seconds())
        heartbeat["age_seconds"] = age_seconds
        heartbeat["staleness_threshold_seconds"] = WORKER_HEARTBEAT_STALENESS_SECONDS
        heartbeat["stale"] = bool(
            heartbeat["component"] in worker_components and age_seconds > WORKER_HEARTBEAT_STALENESS_SECONDS
        )
        enriched_heartbeats.append(heartbeat)
        if heartbeat["status"] in ("failed", "stopped"):
            degraded.append(heartbeat)
        if heartbeat["stale"]:
            stale_workers.append(heartbeat)

    if stale_workers and not degraded:
        reason = "Runtime heartbeat contains stale worker components."
    elif degraded:
        reason = "Runtime heartbeat contains degraded components."
    else:
        reason = None
    return {
        "status": "degraded" if degraded or stale_workers else "ok",
        "components": enriched_heartbeats,
        "reason": reason,
    }


def build_health_report() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    overall_status = "ok"

    try:
        connection = get_connection()
    except DBError as exc:
        return {
            "status": "error",
            "database": get_database_info().get("database_url", str(DB_FILE)),
            "checks": {
                "database": {
                    "status": "error",
                    "reason": str(exc),
                }
            },
        }

    try:
        checks["database"] = _database_check(connection)
        checks["candles"] = _candle_check(connection)
        checks["pipeline"] = _pipeline_check(connection)
        checks["queue"] = _queue_check(connection)
        checks["heartbeats"] = _heartbeat_check(connection)
        checks["execution_backend"] = _execution_backend_check()
        checks["broker_protection"] = _broker_protection_check(connection, checks["execution_backend"], checks["pipeline"])
    except DBError as exc:
        checks["database"] = {
            "status": "error",
            "reason": str(exc),
            "database": str(DB_FILE),
        }
        overall_status = "error"
    finally:
        connection.close()

    checks["scheduler"] = _scheduler_check()
    checks["kill_switch"] = _kill_switch_check()

    for check in checks.values():
        check_status = check.get("status", "ok")
        if check_status == "error":
            overall_status = "error"
            break
        if check_status == "degraded" and overall_status != "error":
            overall_status = "degraded"

    return {
        "status": overall_status,
        "checked_at": _utc_now().isoformat(),
        "database": get_database_info().get("database_url", str(DB_FILE)),
        "database_info": get_database_info(),
        "config": {
            "order_qty": DEFAULT_ORDER_QTY,
            "max_position_qty": MAX_POSITION_QTY,
            "cooldown_seconds": COOLDOWN_SECONDS,
            "candle_staleness_seconds": CANDLE_STALENESS_SECONDS,
            "max_daily_loss": MAX_DAILY_LOSS,
            "risk_config_overrides": "see /risk-config",
        },
        "checks": checks,
    }


class TestSignalRequest(BaseModel):
    signal_type: Literal["BUY", "SELL", "HOLD"]
    symbol: str = DEFAULT_SYMBOL
    timeframe: str = "1m"
    strategy_name: str = "manual_test"


class AlertTestRequest(BaseModel):
    message: str = "Crypto alert test message."


class SchedulerControlRequest(BaseModel):
    audit_action: Optional[str] = None
    audit_message: Optional[str] = None


class KillSwitchControlRequest(BaseModel):
    reason: Optional[str] = None
    source: Optional[str] = None
    notify_message: Optional[str] = None


class ReconcileOrdersRequest(BaseModel):
    audit_action: Optional[str] = None
    audit_message: Optional[str] = None


class PipelineRunRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    symbol_names: Optional[List[str]] = None


class QueueJobRequest(BaseModel):
    job_type: Literal["market_data", "strategy", "risk", "execution"]
    strategy_name: Optional[str] = None
    strategy_names: Optional[List[str]] = None
    symbol_names: Optional[List[str]] = None
    payload: Optional[Dict[str, Any]] = None


class QueueRunRequest(BaseModel):
    job_type: Optional[Literal["market_data", "strategy", "risk", "execution"]] = None


class SchedulerStrategyRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    strategy_names: Optional[List[str]] = None
    disabled_strategy_names: Optional[List[str]] = None
    strategy_priorities: Optional[Dict[str, int]] = None
    disabled_strategy_notes: Optional[Dict[str, str]] = None
    effective_strategy_limit: Optional[int] = None
    audit_action: Optional[str] = None
    audit_message: Optional[str] = None


class SchedulerStrategyPresetRequest(BaseModel):
    preset: Literal["sequential", "reverse", "active_first", "reset"]


class SchedulerStrategyLimitPresetRequest(BaseModel):
    preset: Literal["top_1", "top_2", "all_enabled"]


class SchedulerSymbolsRequest(BaseModel):
    symbol: str = DEFAULT_SYMBOL
    symbol_names: Optional[List[str]] = None


class QueuePipelineRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    strategy_names: Optional[List[str]] = None
    symbol_names: Optional[List[str]] = None
    payload: Optional[Dict[str, Any]] = None


class PipelineRunRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    symbol_names: Optional[List[str]] = None
    orchestration: Optional[Literal["direct", "queue_dispatch", "queue_drain", "queue_batch"]] = None
    batch_id: Optional[str] = None


class ExecutionBackendRequest(BaseModel):
    backend: str
    audit_action: Optional[str] = None
    audit_message: Optional[str] = None


def _build_queue_job_payload(payload: QueueJobRequest) -> dict[str, Any]:
    return build_job_payload(
        strategy_name=payload.strategy_name,
        strategy_names=payload.strategy_names,
        symbol_names=payload.symbol_names,
        payload=payload.payload,
    )


def _log_queue_control_event(
    *,
    status: str,
    message: str,
    action: str,
    payload: Optional[dict[str, Any]] = None,
) -> None:
    log_event(
        event_type="queue_control",
        status=status,
        source="queue_control",
        message=message,
        payload={"action": action, **(payload or {})},
    )


@app.get("/health")
def health(background_tasks: BackgroundTasks) -> dict[str, Any]:
    report = build_health_report()
    background_tasks.add_task(maybe_send_broker_alert, report)
    background_tasks.add_task(maybe_send_execution_alert, report)
    background_tasks.add_task(maybe_send_health_alert, report)
    background_tasks.add_task(maybe_send_queue_alert, report)
    background_tasks.add_task(maybe_send_worker_alert, report)
    return report


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/admin", status_code=307)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/admin", response_class=HTMLResponse)
def admin() -> str:
    return render_admin_page()


@app.get("/alerts/status")
def alerts_status() -> dict[str, bool]:
    return {"telegram_configured": telegram_configured()}


@app.get("/execution/backend")
def execution_backend() -> dict[str, Union[bool, str, list[str]]]:
    return {
        **get_execution_backend_status(),
        **get_execution_backend_runtime_status(),
    }


@app.post("/execution/backend")
def execution_backend_update(payload: ExecutionBackendRequest) -> dict[str, Union[bool, str, list[str]]]:
    set_execution_backend(
        payload.backend,
        audit_action=payload.audit_action or f"set_execution_backend:{payload.backend}",
        audit_message=payload.audit_message or f"Execution backend set to {payload.backend}.",
    )
    return {
        **get_execution_backend_status(),
        **get_execution_backend_runtime_status(),
    }


@app.get("/execution/backend/check")
def execution_backend_check() -> dict[str, Union[bool, str, int]]:
    backend_status = get_execution_backend_status()
    backend = str(backend_status["backend"])
    if backend != "binance":
        return {
            "status": "skipped",
            "backend": backend,
            "reason": "Remote connectivity checks are only implemented for the binance backend.",
        }

    try:
        from app.execution.binance_broker import BinanceBrokerClient

        client = BinanceBrokerClient()
        return client.check_account_connectivity()
    except ValueError as exc:
        return {
            "status": "error",
            "backend": backend,
            "reason": str(exc),
        }
    except requests.RequestException as exc:
        return {
            "status": "error",
            "backend": backend,
            "reason": str(exc),
        }


@app.post("/alerts/test")
def alerts_test(payload: AlertTestRequest) -> dict[str, Any]:
    return send_telegram_message(payload.message)


@app.get("/candles")
def candles(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_candles(connection, limit=limit)
    finally:
        connection.close()


@app.get("/audit-events")
def audit_events(limit: int = Query(default=20, ge=1, le=200)) -> list[dict]:
    connection = get_connection()
    try:
        return get_audit_events(connection, limit=limit)
    finally:
        connection.close()


@app.get("/queue/jobs")
def queue_jobs(
    limit: int = Query(default=20, ge=1, le=200),
    status: Optional[str] = Query(default=None),
    job_type: Optional[Literal["market_data", "strategy", "risk", "execution"]] = Query(default=None),
) -> list[dict[str, Any]]:
    connection = get_connection()
    try:
        return list_queue_jobs(connection, limit=limit, status=status, job_type=job_type)
    finally:
        connection.close()


@app.get("/queue/summary")
def queue_summary() -> dict[str, Any]:
    connection = get_connection()
    try:
        return get_job_queue_summary(connection)
    finally:
        connection.close()


@app.post("/queue/jobs")
def create_queue_job(payload: QueueJobRequest) -> dict[str, Any]:
    connection = get_connection()
    try:
        job_payload = _build_queue_job_payload(payload)
        job_id = enqueue_job(connection, payload.job_type, payload=job_payload or None)
        return {
            "status": "queued",
            "job_id": job_id,
            "job_type": payload.job_type,
            "available_job_types": list(JOB_TYPES),
            "payload": job_payload,
            "job": list_queue_jobs(connection, limit=1, job_type=payload.job_type)[0],
        }
    finally:
        connection.close()


@app.post("/queue/jobs/enqueue-pipeline")
def create_pipeline_queue_jobs(payload: QueuePipelineRequest) -> dict[str, Any]:
    connection = get_connection()
    try:
        jobs = enqueue_pipeline_jobs(
            connection,
            strategy_name=payload.strategy_name,
            strategy_names=payload.strategy_names,
            symbol_names=payload.symbol_names,
            payload=payload.payload,
        )
        return {
            "status": "queued",
            "job_count": len(jobs),
            "job_types": [job["job_type"] for job in jobs],
            "jobs": jobs,
        }
    finally:
        connection.close()


@app.post("/queue/jobs/run-next")
def run_next_queue_job(payload: Optional[QueueRunRequest] = None) -> dict[str, Any]:
    connection = get_connection()
    try:
        job_type = payload.job_type if payload is not None else None
        return run_next_queued_job(connection, job_type=job_type)
    finally:
        connection.close()


@app.post("/queue/jobs/run-next-pipeline")
def run_next_pipeline_queue_batch() -> dict[str, Any]:
    connection = get_connection()
    try:
        return run_next_pipeline_batch(connection)
    finally:
        connection.close()


@app.post("/queue/jobs/{job_id}/retry")
def retry_queue_job(job_id: int) -> dict[str, Any]:
    connection = get_connection()
    try:
        job = retry_job(connection, job_id)
        return {
            "status": "queued",
            "job_id": job_id,
            "job": job,
        }
    finally:
        connection.close()


@app.post("/queue/batches/{batch_id}/clear")
def clear_queue_batch(batch_id: str) -> dict[str, Any]:
    connection = get_connection()
    try:
        jobs = fail_batch_jobs(
            connection,
            batch_id,
            error_message="Queue batch cleared from admin.",
            result={"cleared_batch_id": batch_id, "source": "admin_queue_clear"},
        )
        status = "cleared" if jobs else "empty"
        _log_queue_control_event(
            status=status,
            message="Cleared queued pipeline batch from admin." if jobs else "No queued jobs found for pipeline batch clear request.",
            action="clear_pipeline_batch",
            payload={
                "batch_id": batch_id,
                "cleared_job_count": len(jobs),
                "job_ids": [int(job["id"]) for job in jobs],
            },
        )
        return {
            "status": status,
            "batch_id": batch_id,
            "cleared_job_count": len(jobs),
            "jobs": jobs,
        }
    finally:
        connection.close()


@app.get("/signals")
def signals(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_signals(connection, limit=limit)
    finally:
        connection.close()


@app.get("/strategies")
def strategies() -> dict[str, Any]:
    return {
        "default_strategy": DEFAULT_STRATEGY_NAME,
        "strategies": list_registered_strategies(),
    }


@app.get("/strategies/summary")
def strategy_summary() -> list[dict[str, Any]]:
    connection = get_connection()
    try:
        return get_strategy_activity_summary(connection)
    finally:
        connection.close()


@app.get("/strategies/closed-trades")
def strategy_closed_trades(
    limit: int = Query(default=20, ge=1, le=200),
    strategy_name: Optional[str] = Query(default=None),
) -> list[dict[str, Any]]:
    connection = get_connection()
    try:
        return get_strategy_closed_trades(connection, limit=limit, strategy_name=strategy_name)
    finally:
        connection.close()


@app.get("/risk-events")
def risk_events(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_risk_events(connection, limit=limit)
    finally:
        connection.close()


class RiskConfigUpdateRequest(BaseModel):
    order_qty: Optional[float] = None
    max_position_qty: Optional[float] = None
    cooldown_seconds: Optional[int] = None
    max_daily_loss: Optional[float] = None


@app.get("/risk-config")
def list_risk_config() -> dict:
    """List all per-strategy risk configs and the global defaults."""
    connection = get_connection()
    try:
        run_migrations(connection)
        overrides = list_risk_configs(connection)
        return {
            "global_defaults": {
                "order_qty": DEFAULT_ORDER_QTY,
                "max_position_qty": MAX_POSITION_QTY,
                "cooldown_seconds": COOLDOWN_SECONDS,
                "max_daily_loss": MAX_DAILY_LOSS,
            },
            "overrides": overrides,
        }
    finally:
        connection.close()


@app.get("/risk-config/{strategy_name}")
def get_risk_config_for_strategy(strategy_name: str) -> dict:
    """Return the effective risk config for a strategy (per-strategy or global defaults)."""
    connection = get_connection()
    try:
        run_migrations(connection)
        cfg, is_default = get_risk_config(connection, strategy_name)
        result = cfg.to_dict()
        result["is_default"] = is_default
        return result
    finally:
        connection.close()


@app.post("/risk-config/{strategy_name}")
def update_risk_config_for_strategy(strategy_name: str, body: RiskConfigUpdateRequest) -> dict:
    """Set or update per-strategy risk config.  Only provided fields are changed."""
    connection = get_connection()
    try:
        run_migrations(connection)
        cfg = set_risk_config(
            connection,
            strategy_name,
            order_qty=body.order_qty,
            max_position_qty=body.max_position_qty,
            cooldown_seconds=body.cooldown_seconds,
            max_daily_loss=body.max_daily_loss,
        )
        log_event(
            event_type="risk_config",
            status="ok",
            source="api",
            message=f"Risk config updated for strategy={strategy_name!r}.",
            payload=cfg.to_dict(),
        )
        return {"status": "ok", "config": cfg.to_dict()}
    finally:
        connection.close()


@app.delete("/risk-config/{strategy_name}")
def reset_risk_config_for_strategy(strategy_name: str) -> dict:
    """Remove per-strategy override; the strategy reverts to global defaults."""
    connection = get_connection()
    try:
        run_migrations(connection)
        deleted = delete_risk_config(connection, strategy_name)
        log_event(
            event_type="risk_config",
            status="ok",
            source="api",
            message=f"Risk config override deleted for strategy={strategy_name!r}. deleted={deleted}",
            payload={"strategy_name": strategy_name, "deleted": deleted},
        )
        return {"status": "ok", "deleted": deleted}
    finally:
        connection.close()


@app.get("/orders")
def orders(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_orders(connection, limit=limit)
    finally:
        connection.close()


@app.get("/fills")
def fills(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_fills(connection, limit=limit)
    finally:
        connection.close()


@app.get("/positions")
def positions(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_positions(connection, limit=limit)
    finally:
        connection.close()


@app.get("/pnl")
def pnl(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_pnl_snapshots(connection, limit=limit)
    finally:
        connection.close()


class PortfolioConfigUpdateRequest(BaseModel):
    total_capital: Optional[float] = None
    max_strategy_allocation_pct: Optional[float] = None
    max_total_exposure_pct: Optional[float] = None


@app.get("/portfolio")
def portfolio_summary() -> dict:
    """Cross-strategy exposure summary with per-position notional and limit violations."""
    connection = get_connection()
    try:
        run_migrations(connection)
        return get_portfolio_summary(connection)
    finally:
        connection.close()


@app.get("/portfolio/config")
def portfolio_config() -> dict:
    """Return current portfolio capital and exposure limit config."""
    connection = get_connection()
    try:
        run_migrations(connection)
        return get_portfolio_config(connection).to_dict()
    finally:
        connection.close()


@app.post("/portfolio/config")
def update_portfolio_config(body: PortfolioConfigUpdateRequest) -> dict:
    """Set portfolio capital and exposure limits.  Only provided fields are changed."""
    connection = get_connection()
    try:
        run_migrations(connection)
        cfg = set_portfolio_config(
            connection,
            total_capital=body.total_capital,
            max_strategy_allocation_pct=body.max_strategy_allocation_pct,
            max_total_exposure_pct=body.max_total_exposure_pct,
        )
        log_event(
            event_type="portfolio_config",
            status="ok",
            source="api",
            message="Portfolio config updated.",
            payload=cfg.to_dict(),
        )
        return {"status": "ok", "config": cfg.to_dict()}
    finally:
        connection.close()


@app.post("/pipeline/run")
def run_pipeline_endpoint(payload: Optional[PipelineRunRequest] = None) -> dict:
    strategy_name = payload.strategy_name if payload is not None else DEFAULT_STRATEGY_NAME
    symbol_names = payload.symbol_names if payload is not None else None
    batch_id = payload.batch_id if payload is not None else None
    orchestration = payload.orchestration if payload is not None and payload.orchestration is not None else DEFAULT_PIPELINE_ORCHESTRATION
    if orchestration == "queue_dispatch":
        connection = get_connection()
        try:
            jobs = enqueue_pipeline_jobs(
                connection,
                strategy_name=strategy_name,
                symbol_names=symbol_names,
                payload={"source": "api_pipeline", "orchestration": orchestration},
            )
            return {
                "status": "queued",
                "orchestration": orchestration,
                "strategy_name": strategy_name,
                "requested_symbol_names": symbol_names,
                "batch_id": jobs[0]["batch_id"] if jobs else None,
                "jobs": jobs,
            }
        finally:
            connection.close()
    if orchestration == "queue_batch":
        connection = get_connection()
        try:
            result = enqueue_and_run_pipeline_batch(
                connection,
                strategy_name=strategy_name,
                symbol_names=symbol_names,
                payload={"source": "api_pipeline", "orchestration": orchestration},
            )
            return {
                **result,
                "orchestration": orchestration,
                "strategy_name": strategy_name,
                "requested_symbol_names": symbol_names,
            }
        finally:
            connection.close()
    if orchestration == "queue_drain":
        connection = get_connection()
        try:
            result = run_pipeline_batch(connection, batch_id=batch_id)
            _log_queue_control_event(
                status=str(result.get("status") or "unknown"),
                message="Recovered queued pipeline batch." if batch_id else "Drained next queued pipeline batch.",
                action="recover_pipeline_batch",
                payload={
                    "orchestration": orchestration,
                    "requested_batch_id": batch_id,
                    "result_batch_id": result.get("batch_id"),
                    "result_status": result.get("status"),
                },
            )
            return {
                **result,
                "orchestration": orchestration,
                "strategy_name": strategy_name,
                "requested_symbol_names": symbol_names,
                "requested_batch_id": batch_id,
            }
        finally:
            connection.close()
    return run_pipeline_collect(strategy_name=strategy_name, symbol_names=symbol_names)


@app.post("/signals/test")
def create_test_signal(payload: TestSignalRequest) -> dict:
    connection = get_connection()
    try:
        ensure_signals_table(connection)
        return insert_signal(
            connection,
            signal_type=payload.signal_type,
            symbol=payload.symbol,
            timeframe=payload.timeframe,
            strategy_name=payload.strategy_name,
        )
    finally:
        connection.close()


@app.post("/positions/rebuild")
def rebuild_positions() -> dict[str, int]:
    connection = get_connection()
    try:
        ensure_positions_table(connection)
        updated_symbols = update_positions(connection)
        return {"updated_symbols": updated_symbols}
    finally:
        connection.close()


@app.post("/orders/reconcile")
def reconcile_orders(payload: Optional[ReconcileOrdersRequest] = None) -> dict[str, Any]:
    connection = get_connection()
    try:
        adapter = get_execution_adapter()
        orphan_results = reconcile_orphan_orders(connection, is_live=adapter.is_live)
        ensure_positions_table(connection)
        updated_symbols = update_positions(connection)
        ensure_pnl_table(connection)
        snapshot_count = update_pnl_snapshots(connection)
        latest_orders = get_orders(connection, limit=5)
        log_event(
            event_type="execution_control",
            status="reconciled",
            source="execution_control",
            message=payload.audit_message if payload is not None and payload.audit_message is not None else "Order reconciliation completed.",
            payload={
                "action": payload.audit_action if payload is not None and payload.audit_action is not None else "reconcile_orders",
                "orphan_reconcile_count": len(orphan_results),
                "updated_symbols": updated_symbols,
                "snapshot_count": snapshot_count,
                "latest_order_count": len(latest_orders),
            },
        )
        return {
            "status": "reconciled",
            "orphan_results": orphan_results,
            "updated_symbols": updated_symbols,
            "snapshot_count": snapshot_count,
            "orders": latest_orders,
        }
    finally:
        connection.close()


@app.post("/pnl/update")
def update_pnl() -> dict[str, int]:
    connection = get_connection()
    try:
        ensure_pnl_table(connection)
        snapshot_count = update_pnl_snapshots(connection)
        return {"snapshot_count": snapshot_count}
    finally:
        connection.close()


@app.get("/metrics")
def metrics(period_hours: int = Query(default=24, ge=1, le=168)) -> dict[str, Any]:
    connection = get_connection()
    try:
        return build_metrics(connection, period_hours=period_hours)
    finally:
        connection.close()


@app.get("/validation/soak")
def soak_validation() -> dict[str, Any]:
    return build_soak_validation_report()


@app.post("/validation/soak/record")
def record_soak_validation() -> dict[str, Any]:
    return record_soak_validation_snapshot()


@app.get("/validation/soak/history")
def soak_validation_history(limit: int = Query(default=20, ge=1, le=200)) -> list[dict[str, Any]]:
    return read_soak_validation_history(limit=limit)


@app.get("/validation/soak/history/summary")
def soak_validation_history_summary() -> dict[str, Any]:
    return build_soak_history_summary()


@app.get("/scheduler/status")
def scheduler_status() -> dict:
    return get_stop_status()


@app.get("/scheduler/strategy")
def scheduler_strategy_status() -> dict[str, Any]:
    return get_strategy_status()


@app.get("/scheduler/symbols")
def scheduler_symbol_status() -> dict[str, Any]:
    return get_symbol_status()


@app.post("/scheduler/strategy")
def scheduler_strategy_update(payload: SchedulerStrategyRequest) -> dict[str, Any]:
    if any(
        value is not None
        for value in (
            payload.strategy_names,
            payload.disabled_strategy_names,
            payload.strategy_priorities,
            payload.disabled_strategy_notes,
            payload.effective_strategy_limit,
        )
    ):
        if payload.strategy_names is not None:
            set_active_strategies(
                payload.strategy_names,
                audit_action=payload.audit_action or "set_active_strategies",
                audit_message=payload.audit_message or "Scheduler active strategies updated.",
            )
        if payload.disabled_strategy_names is not None:
            set_disabled_strategies(
                payload.disabled_strategy_names,
                audit_action=payload.audit_action or "set_disabled_strategies",
                audit_message=payload.audit_message or "Scheduler disabled strategies updated.",
            )
        if payload.strategy_priorities is not None:
            set_strategy_priorities(
                payload.strategy_priorities,
                audit_action=payload.audit_action or "set_strategy_priorities",
                audit_message=payload.audit_message or "Scheduler strategy priorities updated.",
            )
        if payload.disabled_strategy_notes is not None:
            set_disabled_strategy_notes(
                payload.disabled_strategy_notes,
                audit_action=payload.audit_action or "set_disabled_strategy_notes",
                audit_message=payload.audit_message or "Scheduler disabled strategy notes updated.",
            )
        if payload.effective_strategy_limit is not None or "effective_strategy_limit" in payload.__fields_set__:
            set_effective_strategy_limit(
                payload.effective_strategy_limit,
                audit_action=payload.audit_action or "set_effective_strategy_limit",
                audit_message=payload.audit_message or "Scheduler effective strategy limit updated.",
            )
        return get_strategy_status()
    return set_active_strategy(payload.strategy_name)


@app.post("/scheduler/symbols")
def scheduler_symbols_update(payload: SchedulerSymbolsRequest) -> dict[str, Any]:
    if payload.symbol_names is not None:
        set_active_symbols(
            payload.symbol_names,
            audit_action="set_active_symbols",
            audit_message="Scheduler active symbols updated.",
        )
        return get_symbol_status()
    return set_active_symbols([payload.symbol])


@app.post("/scheduler/strategy/preset")
def scheduler_strategy_apply_preset(payload: SchedulerStrategyPresetRequest) -> dict[str, Any]:
    status = get_strategy_status()
    priorities = build_strategy_priority_preset(
        payload.preset,
        available_strategies=status.get("available_strategies"),
        active_strategy_names=status.get("strategy_names"),
    )
    set_strategy_priorities(
        priorities,
        audit_action=f"priority_preset:{payload.preset}",
        audit_message=f"Applied scheduler priority preset: {payload.preset}.",
        extra_payload={"preset": payload.preset},
    )
    return get_strategy_status()


@app.post("/scheduler/strategy/limit-preset")
def scheduler_strategy_apply_limit_preset(payload: SchedulerStrategyLimitPresetRequest) -> dict[str, Any]:
    preset_to_limit = {
        "top_1": 1,
        "top_2": 2,
        "all_enabled": None,
    }
    set_effective_strategy_limit(
        preset_to_limit[payload.preset],
        audit_action=f"limit_preset:{payload.preset}",
        audit_message=f"Applied scheduler limit preset: {payload.preset}.",
        extra_payload={"preset": payload.preset},
    )
    return get_strategy_status()


@app.post("/scheduler/stop")
def scheduler_stop(payload: Optional[SchedulerControlRequest] = None) -> Dict[str, Union[str, bool]]:
    stop_file = set_stop_flag(
        audit_action=payload.audit_action if payload is not None and payload.audit_action is not None else "stop",
        audit_message=payload.audit_message if payload is not None and payload.audit_message is not None else "Scheduler stop flag set.",
    )
    return {"stopped": True, "stop_file": stop_file}


@app.post("/scheduler/start")
def scheduler_start(payload: Optional[SchedulerControlRequest] = None) -> Dict[str, Union[str, bool]]:
    removed, stop_file = clear_stop_flag(
        audit_action=payload.audit_action if payload is not None and payload.audit_action is not None else "start",
        audit_message=payload.audit_message if payload is not None and payload.audit_message is not None else "Scheduler stop flag cleared.",
    )
    return {"stopped": False, "stop_file": stop_file, "flag_removed": removed}


@app.get("/scheduler/logs")
def scheduler_logs(
    lines: int = Query(default=20, ge=1, le=500),
    mode: str = Query(default="all"),
) -> Dict[str, Any]:
    return {"mode": mode, "lines": read_scheduler_log(lines=lines, mode=mode)}


@app.get("/kill-switch/status")
def kill_switch_status() -> dict:
    return get_kill_switch_status()


@app.post("/kill-switch/enable")
def kill_switch_enable(payload: Optional[KillSwitchControlRequest] = None) -> Dict[str, Union[str, bool]]:
    kill_switch_file = enable_kill_switch(
        reason=payload.reason if payload is not None and payload.reason is not None else "Kill switch enabled.",
        source=payload.source if payload is not None and payload.source is not None else "kill_switch",
        notify_message=payload.notify_message if payload is not None else "Crypto alert: kill switch has been enabled.",
    )
    return {"enabled": True, "kill_switch_file": kill_switch_file}


@app.post("/kill-switch/disable")
def kill_switch_disable() -> Dict[str, Union[str, bool]]:
    removed, kill_switch_file = disable_kill_switch()
    return {"enabled": False, "kill_switch_file": kill_switch_file, "flag_removed": removed}


# ---------------------------------------------------------------------------
# Backtest endpoints
# ---------------------------------------------------------------------------

class BacktestSweepRequest(BaseModel):
    symbol: str = DEFAULT_SYMBOL
    strategy: str = DEFAULT_STRATEGY_NAME
    days: int = 30
    param_grid: Dict[str, List[float]] = {}
    sort_by: str = "sharpe_ratio"
    fill_on: str = "close"
    initial_capital: float = 10000.0
    experiment_name: Optional[str] = None


class ApplyBestSweepParamsRequest(BaseModel):
    symbol: Optional[str] = None
    sort_by: str = "sharpe_ratio"
    min_trade_count: int = Field(default=1, ge=0)


class BacktestWalkForwardRequest(BaseModel):
    symbol: str = DEFAULT_SYMBOL
    strategy: str = DEFAULT_STRATEGY_NAME
    days: int = 30
    n_splits: int = 5
    order_qty: float = DEFAULT_ORDER_QTY
    max_position_qty: float = MAX_POSITION_QTY
    fill_on: str = "close"
    initial_capital: float = 10000.0
    experiment_name: Optional[str] = None


class BacktestRunUpdateRequest(BaseModel):
    notes: Optional[str] = None
    tags: Optional[Dict[str, Any]] = None


def _backtest_start_iso(days: int) -> str:
    from datetime import timedelta
    return (_utc_now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")


@app.get("/backtest")
def backtest(
    symbol: str = Query(default=DEFAULT_SYMBOL),
    strategy: str = Query(default=DEFAULT_STRATEGY_NAME),
    days: int = Query(default=30, ge=1, le=365),
    order_qty: float = Query(default=DEFAULT_ORDER_QTY, gt=0),
    max_position_qty: float = Query(default=MAX_POSITION_QTY, gt=0),
    fill_on: str = Query(default="close", pattern="^(close|next_open)$"),
    initial_capital: float = Query(default=10000.0, gt=0),
    experiment_name: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    """Run a backtest over recent candles loaded from the DB.

    Returns metrics (total_return_pct, sharpe_ratio, max_drawdown_pct,
    win_rate_pct, profit_factor), equity_curve, and trades.
    """
    if strategy not in list_registered_strategies():
        return {"error": f"Unknown strategy: {strategy!r}. Available: {list_registered_strategies()}"}
    connection = get_connection()
    try:
        candles = load_candles_from_db(
            connection,
            symbol=symbol,
            start=_backtest_start_iso(days),
        )
        if not candles:
            return {
                "error": f"No candles found for symbol={symbol!r} in the last {days} days.",
                "symbol": symbol,
                "strategy": strategy,
                "days": days,
            }
        result = run_backtest(
            symbol=symbol,
            strategy_name=strategy,
            candles=candles,
            initial_capital=initial_capital,
            order_qty=order_qty,
            max_position_qty=max_position_qty,
            fill_on=fill_on,
        )
        persist_backtest_run(
            connection, run_type="single", result=result, days=days,
            fill_on=fill_on, experiment_name=experiment_name,
            equity_curve=result.get("equity_curve"),
        )
        return result
    finally:
        connection.close()


@app.get("/backtest/history")
def backtest_history(
    symbol: Optional[str] = Query(default=None),
    strategy: Optional[str] = Query(default=None),
    run_type: Optional[str] = Query(default=None, pattern="^(single|sweep|walk_forward)$"),
    experiment_name: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> Dict[str, Any]:
    """Return paginated history of all persisted backtest runs."""
    connection = get_connection()
    try:
        return list_backtest_runs(
            connection,
            symbol=symbol,
            strategy_name=strategy,
            run_type=run_type,
            experiment_name=experiment_name,
            limit=limit,
            offset=offset,
        )
    finally:
        connection.close()


@app.get("/backtest/experiments")
def backtest_experiments() -> Dict[str, Any]:
    """Return sorted list of distinct experiment names."""
    connection = get_connection()
    try:
        run_migrations(connection)
        return {"experiments": list_backtest_experiments(connection)}
    finally:
        connection.close()


@app.get("/backtest/compare")
def backtest_compare(
    ids: str = Query(description="Comma-separated run IDs, e.g. 1,2,3"),
) -> Dict[str, Any]:
    """Return multiple runs side-by-side with per-metric best run id."""
    try:
        run_ids = [int(i.strip()) for i in ids.split(",") if i.strip()]
    except ValueError:
        raise HTTPException(status_code=422, detail="ids must be comma-separated integers.")
    if not run_ids:
        raise HTTPException(status_code=422, detail="At least one id is required.")
    connection = get_connection()
    try:
        run_migrations(connection)
        return compare_backtest_runs(connection, run_ids)
    finally:
        connection.close()


@app.get("/backtest/leaderboard/{strategy_name}")
def backtest_leaderboard(
    strategy_name: str,
    sort_by: str = Query(default="sharpe_ratio"),
    limit: int = Query(default=10, ge=1, le=100),
) -> Dict[str, Any]:
    """Return top runs for a strategy sorted by a metric column."""
    connection = get_connection()
    try:
        run_migrations(connection)
        try:
            runs = leaderboard_backtest_runs(
                connection,
                strategy_name=strategy_name,
                sort_by=sort_by,
                limit=limit,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        return {"strategy_name": strategy_name, "sort_by": sort_by, "runs": runs}
    finally:
        connection.close()


@app.get("/backtest/runs/{run_id}")
def backtest_get_run(run_id: int) -> Dict[str, Any]:
    """Return a single backtest run by id."""
    connection = get_connection()
    try:
        run_migrations(connection)
        run = get_backtest_run(connection, run_id)
        if run is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")
        return run
    finally:
        connection.close()


@app.get("/backtest/runs/{run_id}/equity-curve")
def backtest_run_equity_curve(run_id: int) -> Dict[str, Any]:
    """Return the stored equity curve for a run.
    Returns an empty list if the run exists but has no stored curve."""
    connection = get_connection()
    try:
        run_migrations(connection)
        curve = get_backtest_equity_curve(connection, run_id)
        if curve is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")
        return {"run_id": run_id, "equity_curve": curve}
    finally:
        connection.close()


@app.patch("/backtest/runs/{run_id}")
def backtest_update_run(run_id: int, body: BacktestRunUpdateRequest) -> Dict[str, Any]:
    """Update mutable fields (notes, tags) on an existing run."""
    connection = get_connection()
    try:
        run_migrations(connection)
        updated = update_backtest_run(connection, run_id, notes=body.notes, tags=body.tags)
        if updated is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")
        return updated
    finally:
        connection.close()


@app.post("/backtest/runs/{run_id}/promote")
def backtest_promote_run(run_id: int) -> Dict[str, Any]:
    """Mark a run as champion for its strategy. Clears promoted_at from all other runs
    of the same strategy."""
    connection = get_connection()
    try:
        run_migrations(connection)
        promoted = promote_backtest_run(connection, run_id)
        if promoted is None:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found.")
        log_event(
            event_type="param_sync",
            status="ok",
            source="api",
            message=(
                f"Run {run_id} promoted as champion for strategy={promoted['strategy_name']!r}."
            ),
            payload={"run_id": run_id, "strategy_name": promoted["strategy_name"]},
        )
        return {"status": "ok", "run": promoted}
    finally:
        connection.close()


@app.get("/backtest/champion/{strategy_name}")
def backtest_champion(strategy_name: str) -> Dict[str, Any]:
    """Return the current champion run for a strategy."""
    connection = get_connection()
    try:
        run_migrations(connection)
        run = get_champion_run(connection, strategy_name)
        if run is None:
            raise HTTPException(
                status_code=404,
                detail=f"No champion run found for strategy={strategy_name!r}.",
            )
        return run
    finally:
        connection.close()


@app.get("/backtest/walk-forward/groups")
def backtest_wf_groups() -> Dict[str, Any]:
    """Return summary list of all persisted walk-forward groups, newest first."""
    connection = get_connection()
    try:
        run_migrations(connection)
        return {"groups": list_walk_forward_groups(connection)}
    finally:
        connection.close()


@app.get("/backtest/walk-forward/groups/{wf_group_id}")
def backtest_wf_group(wf_group_id: str) -> Dict[str, Any]:
    """Return all folds and aggregate stats for a walk-forward group."""
    connection = get_connection()
    try:
        run_migrations(connection)
        group = get_walk_forward_group(connection, wf_group_id)
        if group is None:
            raise HTTPException(
                status_code=404,
                detail=f"Walk-forward group {wf_group_id!r} not found.",
            )
        return group
    finally:
        connection.close()


@app.post("/backtest/sweep")
def backtest_sweep(req: BacktestSweepRequest) -> Dict[str, Any]:
    """Run a parameter grid search over recent candles.

    param_grid keys: order_qty, max_position_qty, cooldown_seconds, max_daily_loss.
    Results are sorted by sort_by (default: sharpe_ratio, best first).
    """
    if req.strategy not in list_registered_strategies():
        return {"error": f"Unknown strategy: {req.strategy!r}. Available: {list_registered_strategies()}"}
    if not req.param_grid:
        return {"error": "param_grid must not be empty."}
    connection = get_connection()
    try:
        candles = load_candles_from_db(
            connection,
            symbol=req.symbol,
            start=_backtest_start_iso(req.days),
        )
        if not candles:
            return {"error": f"No candles found for symbol={req.symbol!r} in the last {req.days} days."}
        try:
            results = run_parameter_sweep(
                symbol=req.symbol,
                strategy_name=req.strategy,
                candles=candles,
                param_grid={k: list(v) for k, v in req.param_grid.items()},
                sort_by=req.sort_by,
                initial_capital=req.initial_capital,
                fill_on=req.fill_on,
            )
        except ValueError as exc:
            return {"error": str(exc)}
        for combo in results:
            persist_backtest_run(
                connection,
                run_type="sweep",
                result={
                    "symbol": req.symbol,
                    "strategy_name": req.strategy,
                    "candle_count": len(candles),
                    "trade_count": combo.get("trade_count", 0),
                    "metrics": combo.get("metrics", {}),
                },
                days=req.days,
                fill_on=req.fill_on,
                params=combo.get("params"),
                experiment_name=req.experiment_name,
            )
        return {
            "symbol": req.symbol,
            "strategy": req.strategy,
            "days": req.days,
            "candle_count": len(candles),
            "combination_count": len(results),
            "sort_by": req.sort_by,
            "results": results,
        }
    finally:
        connection.close()


@app.post("/backtest/walk-forward")
def backtest_walk_forward(req: BacktestWalkForwardRequest) -> Dict[str, Any]:
    """Run expanding-window walk-forward validation over recent candles.

    Returns per-fold train/test metrics and aggregated out-of-sample statistics.
    All folds are persisted under a shared wf_group_id for later aggregation.
    """
    import uuid as _uuid
    if req.strategy not in list_registered_strategies():
        return {"error": f"Unknown strategy: {req.strategy!r}. Available: {list_registered_strategies()}"}
    connection = get_connection()
    try:
        candles = load_candles_from_db(
            connection,
            symbol=req.symbol,
            start=_backtest_start_iso(req.days),
        )
        if not candles:
            return {"error": f"No candles found for symbol={req.symbol!r} in the last {req.days} days."}
        try:
            wf_result = run_walk_forward(
                symbol=req.symbol,
                strategy_name=req.strategy,
                candles=candles,
                n_splits=req.n_splits,
                initial_capital=req.initial_capital,
                order_qty=req.order_qty,
                max_position_qty=req.max_position_qty,
                fill_on=req.fill_on,
            )
        except ValueError as exc:
            return {"error": str(exc)}
        wf_group_id = str(_uuid.uuid4())
        for split in wf_result.get("splits", []):
            persist_backtest_run(
                connection,
                run_type="walk_forward",
                result={
                    "symbol": req.symbol,
                    "strategy_name": req.strategy,
                    "candle_count": split.get("test_candle_count", 0),
                    "trade_count": split.get("test_trade_count", 0),
                    "metrics": split.get("test_metrics", {}),
                },
                days=req.days,
                fill_on=req.fill_on,
                params={
                    "fold": split.get("fold"),
                    "train_candle_count": split.get("train_candle_count"),
                    "test_candle_count": split.get("test_candle_count"),
                },
                experiment_name=req.experiment_name,
                wf_group_id=wf_group_id,
                fold_index=split.get("fold"),
                equity_curve=split.get("test_equity_curve"),
            )
        wf_result["wf_group_id"] = wf_group_id
        return wf_result
    finally:
        connection.close()


@app.post("/backtest/sweep/{strategy}/apply-best-params")
def apply_best_sweep_params(
    strategy: str,
    body: ApplyBestSweepParamsRequest,
) -> Dict[str, Any]:
    """Apply the best persisted sweep run's params to risk_configs for *strategy*.

    Finds the sweep row with the best value of sort_by (default: sharpe_ratio),
    then calls set_risk_config() with only the params that were varied in the sweep.
    Fields not present in the sweep's params_json are left at their current values.
    """
    if strategy not in list_registered_strategies():
        raise HTTPException(
            status_code=404,
            detail=f"Unknown strategy: {strategy!r}. Available: {list_registered_strategies()}",
        )
    connection = get_connection()
    try:
        run_migrations(connection)
        try:
            best = get_best_sweep_run(
                connection,
                strategy_name=strategy,
                symbol=body.symbol,
                sort_by=body.sort_by,
                min_trade_count=body.min_trade_count,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc))
        if best is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"No sweep runs found for strategy={strategy!r}"
                    + (f", symbol={body.symbol!r}" if body.symbol else "")
                    + f" with trade_count >= {body.min_trade_count}."
                ),
            )
        params = best["params"]
        cfg = set_risk_config(
            connection,
            strategy,
            order_qty=float(params["order_qty"]) if "order_qty" in params else None,
            max_position_qty=float(params["max_position_qty"]) if "max_position_qty" in params else None,
            cooldown_seconds=int(params["cooldown_seconds"]) if "cooldown_seconds" in params else None,
            max_daily_loss=float(params["max_daily_loss"]) if "max_daily_loss" in params else None,
        )
        result = {
            "status": "ok",
            "strategy": strategy,
            "source_run": {
                "id": best["id"],
                "symbol": best["symbol"],
                "created_at": best["created_at"],
                "sort_by": body.sort_by,
                "sort_value": best["metrics"].get(body.sort_by),
                "trade_count": best["trade_count"],
                "params_applied": params,
            },
            "config": cfg.to_dict(),
        }
        log_event(
            event_type="param_sync",
            status="ok",
            source="api",
            message=(
                f"Best sweep params applied to strategy={strategy!r} "
                f"from run_id={best['id']} (sort_by={body.sort_by}, "
                f"value={best['metrics'].get(body.sort_by)})."
            ),
            payload={
                "strategy": strategy,
                "source_run_id": best["id"],
                "sort_by": body.sort_by,
                "sort_value": best["metrics"].get(body.sort_by),
                "params_applied": params,
            },
        )
        return result
    finally:
        connection.close()
