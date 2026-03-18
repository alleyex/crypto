from typing import Dict, List, Tuple, Union

from app.scheduler.runner import LOG_FILE
from app.scheduler.runner import RUNTIME_DIR
from app.scheduler.runner import STOP_FILE


def set_stop_flag() -> str:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    STOP_FILE.write_text("stop\n", encoding="utf-8")
    return str(STOP_FILE)


def clear_stop_flag() -> Tuple[bool, str]:
    if STOP_FILE.exists():
        STOP_FILE.unlink()
        return True, str(STOP_FILE)
    return False, str(STOP_FILE)


def get_stop_status() -> Dict[str, Union[str, bool]]:
    return {
        "stopped": STOP_FILE.exists(),
        "stop_file": str(STOP_FILE),
    }


def read_scheduler_log(lines: int = 50) -> List[str]:
    if not LOG_FILE.exists():
        return []
    content = LOG_FILE.read_text(encoding="utf-8").splitlines()
    return content[-lines:]
