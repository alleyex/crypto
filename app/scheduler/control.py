from typing import Dict, List, Tuple, Union

from app.alerting.telegram import send_telegram_message
from app.audit.service import log_event
from app.scheduler.runner import LOG_FILE
from app.scheduler.runner import get_scheduler_log_file
from app.scheduler.runner import get_scheduler_log_files
from app.scheduler.runner import RUNTIME_DIR
from app.scheduler.runner import STOP_FILE


def set_stop_flag() -> str:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    STOP_FILE.write_text("stop\n", encoding="utf-8")
    send_telegram_message("Crypto alert: scheduler stop flag has been set.")
    log_event(
        event_type="scheduler_control",
        status="stopped",
        source="scheduler_control",
        message="Scheduler stop flag set.",
        payload={"stop_file": str(STOP_FILE)},
    )
    return str(STOP_FILE)


def clear_stop_flag() -> Tuple[bool, str]:
    if STOP_FILE.exists():
        STOP_FILE.unlink()
        log_event(
            event_type="scheduler_control",
            status="started",
            source="scheduler_control",
            message="Scheduler stop flag cleared.",
            payload={"stop_file": str(STOP_FILE), "flag_removed": True},
        )
        return True, str(STOP_FILE)
    log_event(
        event_type="scheduler_control",
        status="started",
        source="scheduler_control",
        message="Scheduler start requested but no stop flag was present.",
        payload={"stop_file": str(STOP_FILE), "flag_removed": False},
    )
    return False, str(STOP_FILE)


def get_stop_status() -> Dict[str, Union[str, bool]]:
    return {
        "stopped": STOP_FILE.exists(),
        "stop_file": str(STOP_FILE),
    }


def read_scheduler_log(lines: int = 50, mode: str = "all") -> List[str]:
    if mode != "all":
        log_file = get_scheduler_log_file(mode)
        if not log_file.exists():
            return []
        content = log_file.read_text(encoding="utf-8").splitlines()
        return content[-lines:]

    combined: list[str] = []
    for log_file in get_scheduler_log_files().values():
        if log_file.exists():
            combined.extend(log_file.read_text(encoding="utf-8").splitlines())
    combined.sort()
    return combined[-lines:]
