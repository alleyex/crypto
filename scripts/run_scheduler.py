#!/usr/bin/env python3

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_PID_FILE = PROJECT_ROOT / "runtime" / "scheduler.pid"


def _acquire_singleton() -> None:
    """Ensure only one scheduler instance runs. Exit immediately if another is alive."""
    if _PID_FILE.exists():
        try:
            existing_pid = int(_PID_FILE.read_text().strip())
            os.kill(existing_pid, 0)  # signal 0 = just check existence
            print(f"[scheduler] Already running (PID {existing_pid}). Exiting.", flush=True)
            sys.exit(0)
        except (ProcessLookupError, PermissionError):
            pass  # stale PID file — overwrite it
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


_ensure_project_venv_python()

sys.path.insert(0, str(PROJECT_ROOT))

from app.core.env import load_dotenv_file


load_dotenv_file(PROJECT_ROOT)

from app.core.settings import DEFAULT_STRATEGY_NAME
from app.core.settings import DEFAULT_PIPELINE_ORCHESTRATION
from app.scheduler.runner import run_scheduler
from app.scheduler.runner import SCHEDULER_MODES


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the trading pipeline on a fixed interval.")
    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Interval in seconds between pipeline runs. Default: 60",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Optional number of runs before exit. Default: run forever",
    )
    parser.add_argument(
        "--mode",
        choices=SCHEDULER_MODES,
        default="pipeline",
        help="Scheduled job mode. Default: pipeline",
    )
    parser.add_argument(
        "--strategy",
        default=DEFAULT_STRATEGY_NAME,
        help=f"Strategy name for pipeline/strategy-only runs. Default: {DEFAULT_STRATEGY_NAME}",
    )
    parser.add_argument(
        "--queue-dispatch",
        action="store_true",
        help="Enqueue jobs instead of executing them directly. In pipeline mode, enqueue a full pipeline batch.",
    )
    parser.add_argument(
        "--queue-drain",
        action="store_true",
        help="Drain queued jobs instead of executing direct scheduler jobs. In pipeline mode, drain the next pipeline batch step.",
    )
    parser.add_argument(
        "--orchestration",
        choices=("default", "direct", "queue_dispatch", "queue_drain", "queue_batch"),
        default="default",
        help=(
            "Pipeline orchestration mode override. "
            f"Default: use CRYPTO_PIPELINE_ORCHESTRATION ({DEFAULT_PIPELINE_ORCHESTRATION})."
        ),
    )
    return parser.parse_args()


def main() -> None:
    _acquire_singleton()
    try:
        args = parse_args()
        queue_dispatch = args.queue_dispatch
        queue_drain = args.queue_drain
        pipeline_orchestration_override = None if args.orchestration == "default" else args.orchestration
        if args.orchestration != "default":
            queue_dispatch = args.orchestration == "queue_dispatch"
            queue_drain = args.orchestration == "queue_drain"
        run_scheduler(
            interval_seconds=args.interval,
            iterations=args.iterations,
            mode=args.mode,
            strategy_name=args.strategy,
            queue_dispatch=queue_dispatch,
            queue_drain=queue_drain,
            pipeline_orchestration_override=pipeline_orchestration_override,
        )
    finally:
        _release_singleton()


if __name__ == "__main__":
    main()
