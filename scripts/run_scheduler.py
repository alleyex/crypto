#!/usr/bin/env python3

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _pid_file_for_mode(mode: str) -> Path:
    """Return a mode-specific PID file so different scheduler modes don't collide."""
    safe_mode = mode.replace("-", "_")
    return PROJECT_ROOT / "runtime" / f"scheduler_{safe_mode}.pid"


# Resolved after args are parsed; placeholder until then.
_pid_file: Path = PROJECT_ROOT / "runtime" / "scheduler.pid"


def _is_scheduler_process(pid: int) -> bool:
    """Return True only if the given PID is actually running this scheduler script."""
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    # Verify the process is actually our scheduler (not a reused PID)
    cmdline_file = Path(f"/proc/{pid}/cmdline")
    if cmdline_file.exists():
        try:
            cmdline = cmdline_file.read_bytes().replace(b"\x00", b" ").decode(errors="replace")
            return "run_scheduler.py" in cmdline
        except OSError:
            pass
    # On non-Linux (no /proc), fall back to just pid-exists check
    return True


def _acquire_singleton(pid_file: Path) -> None:
    """Ensure only one scheduler instance of this mode runs. Exit immediately if another is alive."""
    if pid_file.exists():
        try:
            existing_pid = int(pid_file.read_text().strip())
            if _is_scheduler_process(existing_pid):
                print(f"[scheduler] Already running (PID {existing_pid}). Exiting.", flush=True)
                sys.exit(0)
        except (ValueError, OSError):
            pass  # unreadable or stale PID file — overwrite it
    pid_file.write_text(str(os.getpid()))


def _release_singleton(pid_file: Path) -> None:
    try:
        pid_file.unlink(missing_ok=True)
    except Exception:
        pass


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
        "--orchestration",
        choices=("default", "direct", "queue_dispatch", "queue_drain", "queue_batch"),
        default="default",
        help=(
            "Orchestration mode. "
            f"Default: use CRYPTO_PIPELINE_ORCHESTRATION env var ({DEFAULT_PIPELINE_ORCHESTRATION})."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    pid_file = _pid_file_for_mode(args.mode)
    _acquire_singleton(pid_file)
    try:
        orchestration = None if args.orchestration == "default" else args.orchestration
        run_scheduler(
            interval_seconds=args.interval,
            iterations=args.iterations,
            mode=args.mode,
            strategy_name=args.strategy,
            orchestration=orchestration,
        )
    finally:
        _release_singleton(pid_file)


if __name__ == "__main__":
    main()
