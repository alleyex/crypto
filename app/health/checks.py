"""Health check functions for the /health endpoint.

Extracted from app/api/main.py so that each check can be tested
independently without needing a running FastAPI application.
"""
from datetime import datetime, timezone
from typing import Any

from app.core.db import (
    DBConnection,
    DBError,
    get_connection,
    get_database_info,
    list_tables,
    parse_db_timestamp,
    table_exists,
    utc_now,
)
from app.core.settings import (
    CANDLE_STALENESS_SECONDS,
    COOLDOWN_SECONDS,
    DEFAULT_ORDER_QTY,
    MAX_DAILY_LOSS,
    MAX_POSITION_QTY,
    QUEUE_BATCH_STALENESS_SECONDS,
    WORKER_HEARTBEAT_STALENESS_SECONDS,
)
from app.data.candles_service import candle_staleness_threshold_seconds
from app.execution.adapter import get_execution_backend_status
from app.health.broker_check import broker_protection_check
from app.health.pipeline_check import pipeline_check
from app.query.read_service import get_job_queue_summary
from app.scheduler.control import get_stop_status, read_scheduler_log
from app.scheduler.runner import LOG_DIR, LOG_FILE
from app.system.heartbeat import get_heartbeats
from app.system.kill_switch import get_kill_switch_status

def database_check(connection: DBConnection) -> dict[str, Any]:
    database_info = get_database_info()
    table_names = list_tables(connection)
    return {
        "status": "ok",
        "database": database_info.get("database_url", ""),
        "table_count": len(table_names),
        "tables": table_names,
    }

def candle_check(connection: DBConnection) -> dict[str, Any]:
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
    age_seconds = int((utc_now() - latest_close_at).total_seconds())
    staleness_threshold = candle_staleness_threshold_seconds(timeframe)
    status = "ok" if age_seconds <= staleness_threshold else "degraded"
    return {
        "status": status,
        "symbol": symbol,
        "timeframe": timeframe,
        "latest_open_time": int(open_time),
        "latest_close_time": int(close_time),
        "latest_close_at": latest_close_at.isoformat(),
        "age_seconds": age_seconds,
        "staleness_threshold_seconds": staleness_threshold,
    }

def scheduler_check() -> dict[str, Any]:
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

def kill_switch_check() -> dict[str, Any]:
    kill_switch_status = get_kill_switch_status()
    result: dict[str, Any] = {
        "status": "degraded" if kill_switch_status["enabled"] else "ok",
        "enabled": kill_switch_status["enabled"],
        "kill_switch_file": kill_switch_status["kill_switch_file"],
    }
    if kill_switch_status["enabled"]:
        result["reason"] = "Kill switch is enabled."
    return result

def execution_backend_check() -> dict[str, Any]:
    backend_status = get_execution_backend_status()
    return {
        "status": "ok",
        "backend": backend_status["backend"],
        "dry_run": backend_status["dry_run"],
        "can_execute_orders": backend_status["can_execute_orders"],
        "is_live": backend_status.get("is_live", False),
        "placeholder": backend_status.get("placeholder", False),
    }

def queue_check(connection: DBConnection) -> dict[str, Any]:
    summary = get_job_queue_summary(connection)
    counts = summary["counts"]
    latest_incomplete_batch = summary.get("latest_incomplete_batch")
    stale_batch = bool(
        latest_incomplete_batch
        and latest_incomplete_batch.get("age_seconds") is not None
        and int(latest_incomplete_batch["age_seconds"]) > QUEUE_BATCH_STALENESS_SECONDS
    )
    metrics = summary.get("metrics", {})
    active_failures = bool(
        counts["failed"] > 0
        and (
            int(metrics.get("failure_streak") or 0) > 0
            or int(metrics.get("recent_failure_count") or 0) > 0
        )
    )
    status = "degraded" if active_failures or stale_batch else "ok"
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
    if active_failures:
        result["reason"] = "Queue contains recently failed jobs."
    elif counts["failed"] > 0:
        result["historical_failed_count"] = counts["failed"]
    if stale_batch:
        result["reason"] = "Queue contains stale incomplete batches."
    return result

def heartbeat_check(connection: DBConnection) -> dict[str, Any]:
    heartbeats = get_heartbeats(connection)
    if not heartbeats:
        return {
            "status": "degraded",
            "reason": "No runtime heartbeats recorded yet.",
            "components": [],
        }

    worker_components = {
        "data_worker",
        "strategy_worker",
        "risk_worker",
        "execution_worker",
        "futures_orderbook_collector",
        "futures_aggtrade_collector",
        "futures_premium_collector",
        "futures_open_interest_collector",
        "futures_liquidation_collector",
    }
    enriched_heartbeats: list[dict[str, Any]] = []
    stale_workers: list[dict[str, Any]] = []
    degraded: list[dict[str, Any]] = []
    for item in heartbeats:
        heartbeat = dict(item)
        age_seconds = int(
            (utc_now() - parse_db_timestamp(item["last_seen_at"])).total_seconds()
        )
        heartbeat["age_seconds"] = age_seconds
        heartbeat["staleness_threshold_seconds"] = WORKER_HEARTBEAT_STALENESS_SECONDS
        heartbeat["stale"] = bool(
            heartbeat["component"] in worker_components
            and age_seconds > WORKER_HEARTBEAT_STALENESS_SECONDS
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
            "database": get_database_info().get("database_url", ""),
            "checks": {
                "database": {
                    "status": "error",
                    "reason": str(exc),
                }
            },
        }

    try:
        checks["database"] = database_check(connection)
        checks["candles"] = candle_check(connection)
        checks["pipeline"] = pipeline_check(connection)
        checks["queue"] = queue_check(connection)
        checks["heartbeats"] = heartbeat_check(connection)
        checks["execution_backend"] = execution_backend_check()
        checks["broker_protection"] = broker_protection_check(
            connection, checks["execution_backend"], checks["pipeline"]
        )
    except DBError as exc:
        checks["database"] = {
            "status": "error",
            "reason": str(exc),
            "database": get_database_info().get("database_url", ""),
        }
        overall_status = "error"
    finally:
        connection.close()

    checks["scheduler"] = scheduler_check()
    checks["kill_switch"] = kill_switch_check()

    for check in checks.values():
        check_status = check.get("status", "ok")
        if check_status == "error":
            overall_status = "error"
            break
        if check_status == "degraded" and overall_status != "error":
            overall_status = "degraded"

    return {
        "status": overall_status,
        "checked_at": utc_now().isoformat(),
        "database": get_database_info().get("database_url", ""),
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
