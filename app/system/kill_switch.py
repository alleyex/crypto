from pathlib import Path
from typing import Dict, Optional, Tuple, Union

from app.alerting.telegram import send_telegram_message
from app.audit.service import log_event


RUNTIME_DIR = Path("runtime")
KILL_SWITCH_FILE = RUNTIME_DIR / "kill.switch"


def enable_kill_switch(
    reason: str = "Kill switch enabled.",
    source: str = "kill_switch",
    notify_message: Optional[str] = "Crypto alert: kill switch has been enabled.",
) -> str:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    already_enabled = KILL_SWITCH_FILE.exists()
    KILL_SWITCH_FILE.write_text("kill\n", encoding="utf-8")
    if notify_message and not already_enabled:
        send_telegram_message(notify_message)
    log_event(
        event_type="kill_switch",
        status="enabled" if not already_enabled else "already_enabled",
        source=source,
        message=reason,
        payload={"kill_switch_file": str(KILL_SWITCH_FILE), "already_enabled": already_enabled},
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
