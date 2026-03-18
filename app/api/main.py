import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Literal, List, Union

from fastapi import FastAPI, Query
from fastapi import Response
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from app.admin.page import render_admin_page
from app.core.db import DB_FILE, get_connection
from app.core.settings import CANDLE_STALENESS_SECONDS
from app.core.settings import COOLDOWN_SECONDS
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
from app.query.read_service import get_orders
from app.query.read_service import get_pnl_snapshots
from app.query.read_service import get_positions
from app.query.read_service import get_risk_events
from app.query.read_service import get_signals
from app.scheduler.control import clear_stop_flag
from app.scheduler.control import get_stop_status
from app.scheduler.control import read_scheduler_log
from app.scheduler.control import set_stop_flag
from app.scheduler.runner import LOG_FILE
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.ma_cross import insert_signal
from app.system.kill_switch import disable_kill_switch
from app.system.kill_switch import enable_kill_switch
from app.system.kill_switch import get_kill_switch_status


app = FastAPI(title="Crypto Trading MVP API")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_sqlite_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)


def _database_check(connection: sqlite3.Connection) -> dict[str, Any]:
    row = connection.execute("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name ASC;").fetchall()
    table_names = [item[0] for item in row]
    return {
        "status": "ok",
        "database": str(DB_FILE),
        "table_count": len(table_names),
        "tables": table_names,
    }


def _candle_check(connection: sqlite3.Connection) -> dict[str, Any]:
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
        "log_file": str(LOG_FILE),
        "log_exists": LOG_FILE.exists(),
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


def _pipeline_check(connection: sqlite3.Connection) -> dict[str, Any]:
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

    if latest_signal is None and latest_risk is None and latest_order is None:
        return {
            "status": "degraded",
            "reason": "No pipeline activity found yet.",
        }

    result: dict[str, Any] = {"status": "ok"}
    if latest_signal is not None:
        result["latest_signal"] = {
            "signal_type": latest_signal[0],
            "created_at": latest_signal[1],
            "age_seconds": int((_utc_now() - _parse_sqlite_timestamp(latest_signal[1])).total_seconds()),
        }
    if latest_risk is not None:
        result["latest_risk"] = {
            "decision": latest_risk[0],
            "reason": latest_risk[1],
            "created_at": latest_risk[2],
            "age_seconds": int((_utc_now() - _parse_sqlite_timestamp(latest_risk[2])).total_seconds()),
        }
    if latest_order is not None:
        result["latest_order"] = {
            "side": latest_order[0],
            "status": latest_order[1],
            "created_at": latest_order[2],
            "age_seconds": int((_utc_now() - _parse_sqlite_timestamp(latest_order[2])).total_seconds()),
        }
    return result


def build_health_report() -> dict[str, Any]:
    checks: dict[str, Any] = {}
    overall_status = "ok"

    try:
        connection = get_connection()
    except sqlite3.Error as exc:
        return {
            "status": "error",
            "database": str(DB_FILE),
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
    except sqlite3.Error as exc:
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
        "database": str(DB_FILE),
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
    symbol: str = "BTCUSDT"
    timeframe: str = "1m"
    strategy_name: str = "manual_test"


@app.get("/health")
def health() -> dict[str, Any]:
    return build_health_report()


@app.get("/", include_in_schema=False)
def root() -> RedirectResponse:
    return RedirectResponse(url="/admin", status_code=307)


@app.get("/favicon.ico", include_in_schema=False)
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/admin", response_class=HTMLResponse)
def admin() -> str:
    return render_admin_page()


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


@app.get("/signals")
def signals(limit: int = Query(default=5, ge=1, le=100)) -> list[dict]:
    connection = get_connection()
    try:
        return get_signals(connection, limit=limit)
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
def run_pipeline_endpoint() -> dict:
    return run_pipeline_collect()


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


@app.get("/scheduler/status")
def scheduler_status() -> dict:
    return get_stop_status()


@app.post("/scheduler/stop")
def scheduler_stop() -> Dict[str, Union[str, bool]]:
    stop_file = set_stop_flag()
    return {"stopped": True, "stop_file": stop_file}


@app.post("/scheduler/start")
def scheduler_start() -> Dict[str, Union[str, bool]]:
    removed, stop_file = clear_stop_flag()
    return {"stopped": False, "stop_file": stop_file, "flag_removed": removed}


@app.get("/scheduler/logs")
def scheduler_logs(lines: int = Query(default=20, ge=1, le=500)) -> Dict[str, List[str]]:
    return {"lines": read_scheduler_log(lines=lines)}


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
