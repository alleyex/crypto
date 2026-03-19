import json
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, Literal, List, Optional, Union

from fastapi import BackgroundTasks
from fastapi import FastAPI, Query
from fastapi import Response
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from app.admin.page import render_admin_page
from app.alerting.health import maybe_send_health_alert
from app.alerting.telegram import send_telegram_message
from app.alerting.telegram import telegram_configured
from app.core.db import DB_FILE, get_connection
from app.core.db import DBConnection
from app.core.db import DBError
from app.core.db import get_database_info
from app.core.db import parse_db_timestamp
from app.core.db import list_tables
from app.core.db import table_exists
from app.core.job_queue import build_job_payload
from app.core.job_queue import JOB_TYPES
from app.core.job_queue import enqueue_job
from app.core.job_queue import list_jobs as list_queue_jobs
from app.core.job_queue import retry_job
from app.core.job_queue import run_next_queued_job
from app.core.migrations import run_migrations
from app.core.settings import CANDLE_STALENESS_SECONDS
from app.core.settings import COOLDOWN_SECONDS
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.data.symbols import DEFAULT_SYMBOL
from app.core.settings import DEFAULT_ORDER_QTY
from app.core.settings import MAX_DAILY_LOSS
from app.core.settings import MAX_POSITION_QTY
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
from app.validation.soak_history import read_soak_validation_history
from app.validation.soak_history import record_soak_validation_snapshot
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
        SELECT side, status, created_at
        FROM orders
        ORDER BY id DESC
        LIMIT 1;
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
            "status": latest_order[1],
            "created_at": latest_order[2],
            "age_seconds": int((_utc_now() - parse_db_timestamp(latest_order[2])).total_seconds()),
        }
    return result


def _queue_check(connection: DBConnection) -> dict[str, Any]:
    summary = get_job_queue_summary(connection)
    counts = summary["counts"]
    status = "degraded" if counts["failed"] > 0 else "ok"
    result: dict[str, Any] = {
        "status": status,
        "counts": counts,
        "latest_jobs": summary["latest_jobs"],
    }
    if counts["failed"] > 0:
        result["reason"] = "Queue contains failed jobs."
    return result


def _heartbeat_check(connection: DBConnection) -> dict[str, Any]:
    heartbeats = get_heartbeats(connection)
    if not heartbeats:
        return {
            "status": "degraded",
            "reason": "No runtime heartbeats recorded yet.",
            "components": [],
        }

    degraded = [item for item in heartbeats if item["status"] in ("failed", "stopped")]
    return {
        "status": "degraded" if degraded else "ok",
        "components": heartbeats,
        "reason": "Runtime heartbeat contains degraded components." if degraded else None,
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


class PipelineRunRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    symbol_names: Optional[List[str]] = None


class QueueJobRequest(BaseModel):
    job_type: Literal["market_data", "strategy", "execution"]
    strategy_name: Optional[str] = None
    strategy_names: Optional[List[str]] = None
    symbol_names: Optional[List[str]] = None
    payload: Optional[Dict[str, Any]] = None


class QueueRunRequest(BaseModel):
    job_type: Optional[Literal["market_data", "strategy", "execution"]] = None


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


def _build_queue_job_payload(payload: QueueJobRequest) -> dict[str, Any]:
    return build_job_payload(
        strategy_name=payload.strategy_name,
        strategy_names=payload.strategy_names,
        symbol_names=payload.symbol_names,
        payload=payload.payload,
    )


@app.get("/health")
def health(background_tasks: BackgroundTasks) -> dict[str, Any]:
    report = build_health_report()
    background_tasks.add_task(maybe_send_health_alert, report)
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
    job_type: Optional[Literal["market_data", "strategy", "execution"]] = Query(default=None),
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


@app.post("/queue/jobs/run-next")
def run_next_queue_job(payload: Optional[QueueRunRequest] = None) -> dict[str, Any]:
    connection = get_connection()
    try:
        job_type = payload.job_type if payload is not None else None
        return run_next_queued_job(connection, job_type=job_type)
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


@app.post("/pipeline/run")
def run_pipeline_endpoint(payload: Optional[PipelineRunRequest] = None) -> dict:
    strategy_name = payload.strategy_name if payload is not None else DEFAULT_STRATEGY_NAME
    symbol_names = payload.symbol_names if payload is not None else None
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


@app.post("/pnl/update")
def update_pnl() -> dict[str, int]:
    connection = get_connection()
    try:
        ensure_pnl_table(connection)
        snapshot_count = update_pnl_snapshots(connection)
        return {"snapshot_count": snapshot_count}
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
def scheduler_stop() -> Dict[str, Union[str, bool]]:
    stop_file = set_stop_flag()
    return {"stopped": True, "stop_file": stop_file}


@app.post("/scheduler/start")
def scheduler_start() -> Dict[str, Union[str, bool]]:
    removed, stop_file = clear_stop_flag()
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
def kill_switch_enable() -> Dict[str, Union[str, bool]]:
    kill_switch_file = enable_kill_switch()
    return {"enabled": True, "kill_switch_file": kill_switch_file}


@app.post("/kill-switch/disable")
def kill_switch_disable() -> Dict[str, Union[str, bool]]:
    removed, kill_switch_file = disable_kill_switch()
    return {"enabled": False, "kill_switch_file": kill_switch_file, "flag_removed": removed}
