from pathlib import Path
from typing import Dict, Tuple, Union


RUNTIME_DIR = Path("runtime")
KILL_SWITCH_FILE = RUNTIME_DIR / "kill.switch"


def enable_kill_switch() -> str:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    KILL_SWITCH_FILE.write_text("kill\n", encoding="utf-8")
    return str(KILL_SWITCH_FILE)


def disable_kill_switch() -> Tuple[bool, str]:
    if KILL_SWITCH_FILE.exists():
        KILL_SWITCH_FILE.unlink()
        return True, str(KILL_SWITCH_FILE)
    return False, str(KILL_SWITCH_FILE)


def get_kill_switch_status() -> Dict[str, Union[str, bool]]:
    return {
        "enabled": KILL_SWITCH_FILE.exists(),
        "kill_switch_file": str(KILL_SWITCH_FILE),
    }


def kill_switch_enabled() -> bool:
    return KILL_SWITCH_FILE.exists()
