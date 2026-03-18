from pathlib import Path
from typing import Dict, Tuple, Union

from app.audit.service import log_event


RUNTIME_DIR = Path("runtime")
KILL_SWITCH_FILE = RUNTIME_DIR / "kill.switch"


def enable_kill_switch() -> str:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    KILL_SWITCH_FILE.write_text("kill\n", encoding="utf-8")
    log_event(
        event_type="kill_switch",
        status="enabled",
        source="kill_switch",
        message="Kill switch enabled.",
        payload={"kill_switch_file": str(KILL_SWITCH_FILE)},
    )
    return str(KILL_SWITCH_FILE)


def disable_kill_switch() -> Tuple[bool, str]:
    if KILL_SWITCH_FILE.exists():
        KILL_SWITCH_FILE.unlink()
        log_event(
            event_type="kill_switch",
            status="disabled",
            source="kill_switch",
            message="Kill switch disabled.",
            payload={"kill_switch_file": str(KILL_SWITCH_FILE), "flag_removed": True},
        )
        return True, str(KILL_SWITCH_FILE)
    log_event(
        event_type="kill_switch",
        status="disabled",
        source="kill_switch",
        message="Kill switch disable requested but no flag was present.",
        payload={"kill_switch_file": str(KILL_SWITCH_FILE), "flag_removed": False},
    )
    return False, str(KILL_SWITCH_FILE)


def get_kill_switch_status() -> Dict[str, Union[str, bool]]:
    return {
        "enabled": KILL_SWITCH_FILE.exists(),
        "kill_switch_file": str(KILL_SWITCH_FILE),
    }


def kill_switch_enabled() -> bool:
    return KILL_SWITCH_FILE.exists()
