import os
from contextlib import asynccontextmanager
from typing import Any, Union

from fastapi import BackgroundTasks
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi import Response
from fastapi.responses import HTMLResponse
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from app.admin.page import render_admin_page
from app.alerting.broker import maybe_send_broker_alert
from app.alerting.execution import maybe_send_execution_alert
from app.alerting.health import maybe_send_health_alert
from app.alerting.queue import maybe_send_queue_alert
from app.alerting.worker import maybe_send_worker_alert
from app.audit.service import insert_event
from app.core.db import get_connection
from app.core.db import DBConnection
from app.core.db import DBError
from app.core.db import get_database_info
from app.core.db import parse_db_timestamp
from app.core.db import utc_now_iso
from app.core.job_queue import build_job_payload
from app.core.job_queue import JOB_TYPES
from app.core.job_queue import enqueue_job
from app.core.job_queue import list_jobs as list_queue_jobs
from app.core.job_queue import retry_job
from app.core.job_runner import run_next_queued_job
from app.core.migrations import run_migrations
from app.core.settings import COOLDOWN_SECONDS
from app.core.settings import STOP_LOSS_PCT
from app.core.settings import EXECUTION_BACKEND
from app.core.settings import BINANCE_FUTURES
from app.core.settings import DEFAULT_ORDER_QTY
from app.execution.runtime import get_execution_backend_runtime_status
from app.health.checks import build_health_report

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

@asynccontextmanager
async def lifespan(_: FastAPI):
    connection = get_connection()
    try:
        run_migrations(connection)
        # Mark any jobs that were left in 'running' state from a previous
        # process as failed for in-process training types. Queue-backed PPO
        # jobs are recovered by the training worker and must not be failed here.
        connection.execute(
            "UPDATE training_jobs SET status='failed', error='API process restarted while training was in progress' "
            "WHERE status='running' AND (params_json IS NULL OR params_json NOT LIKE ?);",
            ('%"job_type": "ppo"%',),
        )
        connection.commit()
    finally:
        connection.close()
    yield

app = FastAPI(title="Crypto Trading MVP API", lifespan=lifespan)

from app.api.routes import (
    backtest, backtest_sweep, execution, features, futures_data, inference, maintenance,
    market_data, metrics, orders, pipeline, portfolio, positions, queue,
    registry, reports, risk, scheduler, strategies, trades, training,
    training_ppo, training_rl, validation,
)
app.include_router(backtest.router)
app.include_router(backtest_sweep.router)
app.include_router(execution.router)
app.include_router(features.router)
app.include_router(futures_data.router)
app.include_router(inference.router)
app.include_router(maintenance.router)
app.include_router(market_data.router)
app.include_router(metrics.router)
app.include_router(orders.router)
app.include_router(pipeline.router)
app.include_router(portfolio.router)
app.include_router(positions.router)
app.include_router(queue.router)
app.include_router(registry.router)
app.include_router(reports.router)
app.include_router(risk.router)
app.include_router(scheduler.router)
app.include_router(strategies.router)
app.include_router(trades.router)
app.include_router(training.router)
app.include_router(training_ppo.router)
app.include_router(training_rl.router)
app.include_router(validation.router)

_health_cache: dict[str, Any] = {}
_health_cache_ts: float = 0.0
_HEALTH_CACHE_TTL: float = 5.0  # seconds

@app.get("/health/live")
def live_health() -> dict[str, str]:
    return {"status": "ok"}

@app.get("/health")
def health(background_tasks: BackgroundTasks) -> dict[str, Any]:
    global _health_cache, _health_cache_ts
    import time as _time
    now = _time.monotonic()
    if now - _health_cache_ts < _HEALTH_CACHE_TTL and _health_cache:
        report = _health_cache
    else:
        report = build_health_report()
        _health_cache = report
        _health_cache_ts = now
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
def admin() -> HTMLResponse:
    response = HTMLResponse(render_admin_page())
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

