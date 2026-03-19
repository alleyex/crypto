from typing import Dict, List, Tuple, Union

from app.alerting.telegram import send_telegram_message
from app.audit.service import log_event
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.strategy.registry import list_registered_strategies
from app.scheduler.runner import LOG_FILE
from app.scheduler.runner import get_scheduler_log_file
from app.scheduler.runner import get_scheduler_log_files
from app.scheduler.runner import RUNTIME_DIR
from app.scheduler.runner import STOP_FILE

STRATEGY_FILE = RUNTIME_DIR / "scheduler.strategy"
DISABLED_STRATEGY_FILE = RUNTIME_DIR / "scheduler.strategy.disabled"


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


def read_active_strategy() -> str:
    return read_active_strategies()[0]


def read_active_strategies() -> list[str]:
    if not STRATEGY_FILE.exists():
        return [DEFAULT_STRATEGY_NAME]

    configured_names = [
        item.strip()
        for item in STRATEGY_FILE.read_text(encoding="utf-8").splitlines()
        if item.strip()
    ]
    if not configured_names:
        return [DEFAULT_STRATEGY_NAME]

    allowed_names = set(list_registered_strategies())
    normalized_names = [name for name in dict.fromkeys(configured_names) if name in allowed_names]
    if not normalized_names:
        return [DEFAULT_STRATEGY_NAME]
    return normalized_names


def read_disabled_strategies() -> list[str]:
    if not DISABLED_STRATEGY_FILE.exists():
        return []

    configured_names = [
        item.strip()
        for item in DISABLED_STRATEGY_FILE.read_text(encoding="utf-8").splitlines()
        if item.strip()
    ]
    allowed_names = set(list_registered_strategies())
    return [name for name in dict.fromkeys(configured_names) if name in allowed_names]


def read_effective_active_strategies() -> list[str]:
    disabled_names = set(read_disabled_strategies())
    return [name for name in read_active_strategies() if name not in disabled_names]


def set_active_strategy(strategy_name: str) -> Dict[str, str]:
    result = set_active_strategies([strategy_name])
    return {"strategy_name": result["strategy_name"], "strategy_file": result["strategy_file"]}


def set_active_strategies(strategy_names: list[str]) -> Dict[str, Union[str, list[str]]]:
    if not strategy_names:
        raise ValueError("At least one strategy must be provided.")

    unique_names = list(dict.fromkeys(strategy_names))
    allowed_names = set(list_registered_strategies())
    invalid_names = [name for name in unique_names if name not in allowed_names]
    if invalid_names:
        raise ValueError(f"Unknown strategies: {', '.join(invalid_names)}")

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    STRATEGY_FILE.write_text("\n".join(unique_names) + "\n", encoding="utf-8")
    log_event(
        event_type="scheduler_control",
        status="updated",
        source="scheduler_control",
        message="Scheduler active strategies updated.",
        payload={
            "strategy_name": unique_names[0],
            "strategy_names": unique_names,
            "strategy_file": str(STRATEGY_FILE),
        },
    )
    return {
        "strategy_name": unique_names[0],
        "strategy_names": unique_names,
        "strategy_file": str(STRATEGY_FILE),
    }


def set_disabled_strategies(strategy_names: list[str]) -> Dict[str, Union[str, list[str]]]:
    unique_names = list(dict.fromkeys(strategy_names))
    allowed_names = set(list_registered_strategies())
    invalid_names = [name for name in unique_names if name not in allowed_names]
    if invalid_names:
        raise ValueError(f"Unknown strategies: {', '.join(invalid_names)}")

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    if unique_names:
        DISABLED_STRATEGY_FILE.write_text("\n".join(unique_names) + "\n", encoding="utf-8")
    elif DISABLED_STRATEGY_FILE.exists():
        DISABLED_STRATEGY_FILE.unlink()

    log_event(
        event_type="scheduler_control",
        status="updated",
        source="scheduler_control",
        message="Scheduler disabled strategies updated.",
        payload={
            "disabled_strategy_names": unique_names,
            "disabled_strategy_file": str(DISABLED_STRATEGY_FILE),
        },
    )
    return {
        "disabled_strategy_names": unique_names,
        "disabled_strategy_file": str(DISABLED_STRATEGY_FILE),
    }


def get_strategy_status() -> Dict[str, Union[str, List[str]]]:
    active_strategy_names = read_active_strategies()
    disabled_strategy_names = read_disabled_strategies()
    effective_strategy_names = [name for name in active_strategy_names if name not in set(disabled_strategy_names)]
    available_strategies = list_registered_strategies()
    return {
        "strategy_name": active_strategy_names[0],
        "strategy_names": active_strategy_names,
        "disabled_strategy_names": disabled_strategy_names,
        "effective_strategy_names": effective_strategy_names,
        "default_strategy": DEFAULT_STRATEGY_NAME,
        "strategy_file": str(STRATEGY_FILE),
        "disabled_strategy_file": str(DISABLED_STRATEGY_FILE),
        "available_strategies": available_strategies,
        "strategy_entries": [
            {
                "strategy_name": name,
                "active": name in active_strategy_names,
                "enabled": name not in disabled_strategy_names,
                "effective": name in effective_strategy_names,
            }
            for name in available_strategies
        ],
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
