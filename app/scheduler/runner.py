import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from app.pipeline.run_pipeline import run_pipeline_collect


LOG_DIR = Path("logs")
LOG_FILE = LOG_DIR / "scheduler.log"
RUNTIME_DIR = Path("runtime")
STOP_FILE = RUNTIME_DIR / "scheduler.stop"


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


def _write_log(line: str) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


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
        _write_log(snapshot_line)
    except Exception as exc:
        error_line = (
            f"[{datetime.now().isoformat(timespec='seconds')}] "
            f"soak_snapshot failed: {exc}"
        )
        print(error_line)
        _write_log(error_line)


def run_scheduler(interval_seconds: int = 60, iterations: Optional[int] = None) -> None:
    run_count = 0

    while True:
        if stop_requested():
            stopped_at = datetime.now().isoformat(timespec="seconds")
            log_line = f"[{stopped_at}] scheduler stopped by flag: {STOP_FILE}"
            print(log_line)
            _write_log(log_line)
            break

        run_count += 1
        started_at = datetime.now().isoformat(timespec="seconds")
        result = run_pipeline_collect()
        summary = _summarize_result(result)
        log_line = f"[{started_at}] run={run_count} {summary}"
        print(log_line)
        _write_log(log_line)
        _record_soak_snapshot()

        if iterations is not None and run_count >= iterations:
            break

        time.sleep(interval_seconds)
