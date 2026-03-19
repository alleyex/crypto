import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.strategy_job import run_strategy_job
from app.pipeline.strategy_job import run_strategy_jobs
from app.pipeline.run_pipeline import run_pipeline_collect
from app.core.db import get_connection
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.system.heartbeat import record_heartbeat


LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "scheduler.log"
DATA_WORKER_LOG_FILE = LOG_DIR / "data-worker.log"
STRATEGY_WORKER_LOG_FILE = LOG_DIR / "strategy-worker.log"
EXECUTION_WORKER_LOG_FILE = LOG_DIR / "execution-worker.log"
RUNTIME_DIR = Path("runtime")
STOP_FILE = RUNTIME_DIR / "scheduler.stop"
SCHEDULER_MODES = ("pipeline", "market-data-only", "strategy-only", "execution-only")


def _summarize_result(result: dict) -> str:
    signal = "n/a"
    decision = "n/a"
    execution = "n/a"

    for step in result.get("steps", []):
        if step["step"] == "generate_signal" and "signal_type" in step:
            signal = step["signal_type"]
        elif step["step"] == "evaluate_risk" and "decision" in step:
            decision = step["decision"]
        elif step["step"] == "paper_execute":
            if step.get("status") == "FILLED":
                execution = f"FILLED {step['side']}"
            elif "decision" in step:
                execution = step["decision"]
            else:
                execution = step.get("reason", "SKIPPED")

    return f"signal={signal} risk={decision} execution={execution}"


def get_scheduler_log_file(mode: str = "pipeline") -> Path:
    if mode == "pipeline":
        return LOG_FILE
    if mode == "market-data-only":
        return DATA_WORKER_LOG_FILE
    if mode == "strategy-only":
        return STRATEGY_WORKER_LOG_FILE
    if mode == "execution-only":
        return EXECUTION_WORKER_LOG_FILE
    raise ValueError(f"Unsupported scheduler mode: {mode}")


def get_scheduler_log_files() -> dict[str, Path]:
    return {
        "pipeline": LOG_FILE,
        "market-data-only": DATA_WORKER_LOG_FILE,
        "strategy-only": STRATEGY_WORKER_LOG_FILE,
        "execution-only": EXECUTION_WORKER_LOG_FILE,
    }


def _write_log(line: str, mode: str = "pipeline") -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with get_scheduler_log_file(mode).open("a", encoding="utf-8") as file:
        file.write(line + "\n")


def _scheduler_component_name(mode: str) -> str:
    if mode == "pipeline":
        return "scheduler"
    if mode == "market-data-only":
        return "data_worker"
    if mode == "strategy-only":
        return "strategy_worker"
    if mode == "execution-only":
        return "execution_worker"
    raise ValueError(f"Unsupported scheduler mode: {mode}")


def _run_scheduled_job(
    mode: str,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    strategy_names: Optional[list[str]] = None,
) -> dict:
    if mode == "pipeline":
        return run_pipeline_collect(strategy_name=strategy_name)

    connection = get_connection()
    try:
        run_migrations(connection)
        if mode == "market-data-only":
            return {"steps": [run_market_data_job(connection)]}
        if mode == "strategy-only":
            return run_strategy_jobs(connection, strategy_names or [strategy_name])
        if mode == "execution-only":
            return {"steps": list(run_execution_job(connection)["steps"])}
    finally:
        connection.close()

    raise ValueError(f"Unsupported scheduler mode: {mode}")


def _resolve_active_strategy(mode: str, fallback_strategy_name: str) -> str:
    return _resolve_active_strategies(mode, fallback_strategy_name)[0]


def _resolve_active_strategies(mode: str, fallback_strategy_name: str) -> list[str]:
    if mode not in ("pipeline", "strategy-only"):
        return [fallback_strategy_name]
    try:
        from app.scheduler.control import read_effective_active_strategies

        return read_effective_active_strategies()
    except Exception:
        return [fallback_strategy_name]


def _format_strategy_log_label(strategy_names: list[str]) -> str:
    if len(strategy_names) == 1:
        return f"strategy={strategy_names[0]}"
    return "strategies=" + ",".join(strategy_names)


def _summarize_strategy_payload(strategy_names: list[str]) -> dict:
    return {
        "strategy_name": strategy_names[0],
        "strategy_names": strategy_names,
    }


def stop_requested() -> bool:
    return STOP_FILE.exists()


def _record_soak_snapshot() -> None:
    try:
        from app.validation.soak_history import record_soak_validation_snapshot

        report = record_soak_validation_snapshot()
        snapshot_line = (
            f"[{datetime.now().isoformat(timespec='seconds')}] "
            f"soak_snapshot status={report.get('status', 'unknown')}"
        )
        print(snapshot_line)
        _write_log(snapshot_line, mode="pipeline")
    except Exception as exc:
        error_line = (
            f"[{datetime.now().isoformat(timespec='seconds')}] "
            f"soak_snapshot failed: {exc}"
        )
        print(error_line)
        _write_log(error_line, mode="pipeline")


def run_scheduler(
    interval_seconds: int = 60,
    iterations: Optional[int] = None,
    mode: str = "pipeline",
    strategy_name: str = DEFAULT_STRATEGY_NAME,
) -> None:
    if mode not in SCHEDULER_MODES:
        raise ValueError(
            f"Unsupported scheduler mode: {mode}. Expected one of: {', '.join(SCHEDULER_MODES)}"
        )

    component = _scheduler_component_name(mode)
    run_count = 0
    connection = get_connection()
    try:
        run_migrations(connection)
    finally:
        connection.close()

    while True:
        if stop_requested():
            stopped_at = datetime.now().isoformat(timespec="seconds")
            log_line = f"[{stopped_at}] scheduler stopped by flag: {STOP_FILE}"
            print(log_line)
            _write_log(log_line, mode)
            record_heartbeat(
                component=component,
                status="stopped",
                message=f"{component} stopped by flag.",
                payload={"stop_file": str(STOP_FILE), "mode": mode},
            )
            break

        run_count += 1
        started_at = datetime.now().isoformat(timespec="seconds")
        active_strategy_names = _resolve_active_strategies(mode, strategy_name)
        if not active_strategy_names:
            log_line = f"[{started_at}] run={run_count} mode={mode} strategies=none skipped=no-enabled-active-strategies"
            print(log_line)
            _write_log(log_line, mode)
            record_heartbeat(
                component=component,
                status="ok",
                message=f"{component} loop skipped because no enabled active strategies are configured.",
                payload={"run_count": run_count, "mode": mode, "strategy_names": [], "skipped": True},
            )
            _record_soak_snapshot()
            if iterations is not None and run_count >= iterations:
                break
            time.sleep(interval_seconds)
            continue

        active_strategy_name = active_strategy_names[0]
        record_heartbeat(
            component=component,
            status="running",
            message=f"{component} loop started.",
            payload={"run_count": run_count, "mode": mode, **_summarize_strategy_payload(active_strategy_names)},
        )
        result = _run_scheduled_job(mode, strategy_name=active_strategy_name, strategy_names=active_strategy_names)
        summary = _summarize_result(result)
        strategy_label = _format_strategy_log_label(active_strategy_names)
        log_line = f"[{started_at}] run={run_count} mode={mode} {strategy_label} {summary}"
        print(log_line)
        _write_log(log_line, mode)
        record_heartbeat(
            component=component,
            status="ok",
            message=f"{component} loop completed.",
            payload={"run_count": run_count, "summary": summary, "mode": mode, **_summarize_strategy_payload(active_strategy_names)},
        )
        _record_soak_snapshot()

        if iterations is not None and run_count >= iterations:
            break

        time.sleep(interval_seconds)
