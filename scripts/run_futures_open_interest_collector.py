#!/usr/bin/env python3

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_PID_FILE = PROJECT_ROOT / "runtime" / "futures_open_interest.pid"
_STOP_FILE = PROJECT_ROOT / "runtime" / "futures_open_interest.stop"
_LOG_DIR = PROJECT_ROOT / "logs"
_LOG_FILE = _LOG_DIR / "futures-open-interest-worker.log"


def _acquire_singleton() -> None:
    if _PID_FILE.exists():
        try:
            existing_pid = int(_PID_FILE.read_text().strip())
            os.kill(existing_pid, 0)
            print(f"[futures-open-interest] Already running (PID {existing_pid}). Exiting.", flush=True)
            sys.exit(0)
        except (ProcessLookupError, PermissionError, ValueError):
            pass
    _PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    _PID_FILE.write_text(str(os.getpid()))


def _release_singleton() -> None:
    try:
        _PID_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def _ensure_project_venv_python() -> None:
    if not EXPECTED_PYTHON.exists():
        return
    current_python = Path(sys.executable).resolve()
    expected_python = EXPECTED_PYTHON.resolve()
    if current_python == expected_python:
        return
    os.execv(str(expected_python), [str(expected_python), __file__, *sys.argv[1:]])


def _write_log(line: str) -> None:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    with _LOG_FILE.open("a", encoding="utf-8") as file:
        file.write(line + "\n")


def _stop_requested() -> bool:
    return _STOP_FILE.exists()


_ensure_project_venv_python()
sys.path.insert(0, str(PROJECT_ROOT))

from app.core.db import get_connection
from app.core.env import load_dotenv_file
from app.core.migrations import run_migrations
from app.pipeline.futures_open_interest_job import run_futures_open_interest_job
from app.system.heartbeat import record_heartbeat

load_dotenv_file(PROJECT_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the futures open interest collector on a fixed interval.")
    parser.add_argument("--interval", type=int, default=60, help="Collection interval in seconds. Default: 60")
    parser.add_argument("--iterations", type=int, default=None, help="Optional number of runs before exit")
    return parser.parse_args()


def main() -> None:
    _acquire_singleton()
    try:
        args = parse_args()
        connection = get_connection()
        try:
            run_migrations(connection)
        finally:
            connection.close()

        run_count = 0
        while True:
            if _stop_requested():
                stopped_at = datetime.now().isoformat(timespec="seconds")
                line = f"[{stopped_at}] futures open interest collector stopped by flag: {_STOP_FILE}"
                print(line, flush=True)
                _write_log(line)
                record_heartbeat(
                    component="futures_open_interest_collector",
                    status="stopped",
                    message="futures_open_interest_collector stopped by flag.",
                    payload={"stop_file": str(_STOP_FILE)},
                )
                break

            run_count += 1
            started_at = datetime.now().isoformat(timespec="seconds")
            record_heartbeat(
                component="futures_open_interest_collector",
                status="running",
                message="futures_open_interest_collector loop started.",
                payload={"run_count": run_count, "interval_seconds": args.interval},
            )
            try:
                connection = get_connection()
                try:
                    result = run_futures_open_interest_job(connection)
                finally:
                    connection.close()
                line = (
                    f"[{started_at}] run={run_count} step=futures_open_interest "
                    f"status={result.get('status')} saved={result.get('saved', 0)} "
                    f"source_counts={result.get('source_counts', {})}"
                )
                print(line, flush=True)
                _write_log(line)
            except Exception as exc:
                line = f"[{started_at}] run={run_count} step=futures_open_interest status=error error={exc}"
                print(line, flush=True)
                _write_log(line)
                record_heartbeat(
                    component="futures_open_interest_collector",
                    status="failed",
                    message=f"futures_open_interest_collector loop failed: {exc}",
                    payload={"run_count": run_count, "interval_seconds": args.interval},
                )

            if args.iterations is not None and run_count >= args.iterations:
                break
            time.sleep(args.interval)
    finally:
        _release_singleton()


if __name__ == "__main__":
    main()
