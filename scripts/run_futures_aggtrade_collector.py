#!/usr/bin/env python3

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"
_PID_FILE = PROJECT_ROOT / "runtime" / "futures_aggtrade.pid"
_STOP_FILE = PROJECT_ROOT / "runtime" / "futures_aggtrade.stop"
_LOG_DIR = PROJECT_ROOT / "logs"
_LOG_FILE = _LOG_DIR / "futures-aggtrade-worker.log"


def _acquire_singleton() -> None:
    if _PID_FILE.exists():
        try:
            existing_pid = int(_PID_FILE.read_text().strip())
            os.kill(existing_pid, 0)
            print(f"[futures-aggtrade] Already running (PID {existing_pid}). Exiting.", flush=True)
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

from app.alerting.telegram import send_telegram_message
from app.audit.service import log_event
from app.core.db import get_connection
from app.core.env import load_dotenv_file
from app.core.migrations import run_migrations
from app.data.futures_aggtrade_service import configured_futures_aggtrade_symbols
from app.data.futures_aggtrade_service import reset_futures_aggtrade_runtime
from app.pipeline.futures_aggtrade_job import run_futures_aggtrade_job
from app.system.heartbeat import record_heartbeat

load_dotenv_file(PROJECT_ROOT)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the futures aggTrade collector on a fixed interval.")
    parser.add_argument("--interval", type=int, default=60, help="Collection interval in seconds. Default: 60")
    parser.add_argument("--iterations", type=int, default=None, help="Optional number of runs before exit")
    return parser.parse_args()


def _should_restart_collector(result: dict, expected_symbols: list[str]) -> bool:
    collector = dict(result.get("collector") or {})
    source_counts = dict(result.get("source_counts") or {})
    symbol_runtime = dict(collector.get("symbol_runtime") or {})
    ws_available = bool(collector.get("ws_available", True))
    if not ws_available:
        return True
    if source_counts.get("ws", 0) > 0:
        return False
    covered = [symbol for symbol in expected_symbols if symbol in symbol_runtime]
    return len(covered) == 0


def _notify_watchdog_restart(run_count: int, expected_symbols: list[str], result: dict) -> None:
    payload = {
        "run_count": run_count,
        "symbols": expected_symbols,
        "source_counts": dict(result.get("source_counts") or {}),
        "collector": dict(result.get("collector") or {}),
    }
    log_event(
        event_type="futures_aggtrade_watchdog_restart",
        status="warning",
        source="futures_aggtrade_collector",
        message="Futures aggTrade watchdog restarted collector after stale WS runtime.",
        payload=payload,
    )
    send_telegram_message(
        "Crypto alert: futures aggTrade watchdog restarted collector after stale WS runtime."
    )


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
        stale_ws_loops = 0
        while True:
            if _stop_requested():
                stopped_at = datetime.now().isoformat(timespec="seconds")
                line = f"[{stopped_at}] futures aggTrade collector stopped by flag: {_STOP_FILE}"
                print(line, flush=True)
                _write_log(line)
                record_heartbeat(
                    component="futures_aggtrade_collector",
                    status="stopped",
                    message="futures_aggtrade_collector stopped by flag.",
                    payload={"stop_file": str(_STOP_FILE)},
                )
                break

            run_count += 1
            started_at = datetime.now().isoformat(timespec="seconds")
            record_heartbeat(
                component="futures_aggtrade_collector",
                status="running",
                message="futures_aggtrade_collector loop started.",
                payload={"run_count": run_count, "interval_seconds": args.interval},
            )
            try:
                connection = get_connection()
                try:
                    result = run_futures_aggtrade_job(connection)
                finally:
                    connection.close()
                expected_symbols = configured_futures_aggtrade_symbols()
                if _should_restart_collector(result, expected_symbols):
                    stale_ws_loops += 1
                else:
                    stale_ws_loops = 0
                if stale_ws_loops >= 2:
                    reset_futures_aggtrade_runtime(expected_symbols)
                    stale_ws_loops = 0
                    restart_line = f"[{started_at}] watchdog=restarted reason=stale_ws_runtime"
                    print(restart_line, flush=True)
                    _write_log(restart_line)
                    _notify_watchdog_restart(run_count, expected_symbols, result)
                line = (
                    f"[{started_at}] run={run_count} step=futures_aggtrade "
                    f"status={result.get('status')} saved={result.get('saved', 0)} "
                    f"source_counts={result.get('source_counts', {})}"
                )
                print(line, flush=True)
                _write_log(line)
            except Exception as exc:
                line = f"[{started_at}] run={run_count} step=futures_aggtrade status=error error={exc}"
                print(line, flush=True)
                _write_log(line)
                record_heartbeat(
                    component="futures_aggtrade_collector",
                    status="failed",
                    message=f"futures_aggtrade_collector loop failed: {exc}",
                    payload={"run_count": run_count, "interval_seconds": args.interval},
                )

            if args.iterations is not None and run_count >= args.iterations:
                break
            time.sleep(args.interval)
    finally:
        _release_singleton()


if __name__ == "__main__":
    main()
