import json
from typing import Dict, List, Optional, Tuple, Union

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
PRIORITY_FILE = RUNTIME_DIR / "scheduler.strategy.priority.json"
DISABLED_REASON_FILE = RUNTIME_DIR / "scheduler.strategy.disabled.reason.json"
EFFECTIVE_LIMIT_FILE = RUNTIME_DIR / "scheduler.strategy.limit"


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


def read_strategy_priorities() -> dict[str, int]:
    if not PRIORITY_FILE.exists():
        return {}

    try:
        payload = json.loads(PRIORITY_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    allowed_names = set(list_registered_strategies())
    normalized: dict[str, int] = {}
    for name, value in payload.items():
        if name not in allowed_names:
            continue
        try:
            normalized[str(name)] = int(value)
        except (TypeError, ValueError):
            continue
    return normalized


def read_disabled_strategy_notes() -> dict[str, str]:
    if not DISABLED_REASON_FILE.exists():
        return {}

    try:
        payload = json.loads(DISABLED_REASON_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}

    if not isinstance(payload, dict):
        return {}

    allowed_names = set(list_registered_strategies())
    normalized: dict[str, str] = {}
    for name, value in payload.items():
        if name not in allowed_names or value is None:
            continue
        text = str(value).strip()
        if text:
            normalized[str(name)] = text
    return normalized


def read_effective_strategy_limit() -> Optional[int]:
    if not EFFECTIVE_LIMIT_FILE.exists():
        return None
    try:
        value = int(EFFECTIVE_LIMIT_FILE.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    return value if value > 0 else None


def read_effective_active_strategies() -> list[str]:
    disabled_names = set(read_disabled_strategies())
    active_names = [name for name in read_active_strategies() if name not in disabled_names]
    priorities = read_strategy_priorities()
    indexed_names = list(enumerate(active_names))
    indexed_names.sort(key=lambda item: (priorities.get(item[1], item[0]), item[0]))
    ordered_names = [name for _, name in indexed_names]
    effective_limit = read_effective_strategy_limit()
    if effective_limit is not None:
        return ordered_names[:effective_limit]
    return ordered_names


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


def set_strategy_priorities(strategy_priorities: dict[str, int]) -> Dict[str, Union[str, dict[str, int]]]:
    allowed_names = set(list_registered_strategies())
    invalid_names = [name for name in strategy_priorities if name not in allowed_names]
    if invalid_names:
        raise ValueError(f"Unknown strategies: {', '.join(invalid_names)}")

    normalized = {str(name): int(value) for name, value in strategy_priorities.items()}
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    if normalized:
        PRIORITY_FILE.write_text(json.dumps(normalized, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    elif PRIORITY_FILE.exists():
        PRIORITY_FILE.unlink()

    log_event(
        event_type="scheduler_control",
        status="updated",
        source="scheduler_control",
        message="Scheduler strategy priorities updated.",
        payload={
            "strategy_priorities": normalized,
            "priority_file": str(PRIORITY_FILE),
        },
    )
    return {
        "strategy_priorities": normalized,
        "priority_file": str(PRIORITY_FILE),
    }


def set_disabled_strategy_notes(strategy_notes: dict[str, str]) -> Dict[str, Union[str, dict[str, str]]]:
    allowed_names = set(list_registered_strategies())
    invalid_names = [name for name in strategy_notes if name not in allowed_names]
    if invalid_names:
        raise ValueError(f"Unknown strategies: {', '.join(invalid_names)}")

    normalized = {
        str(name): str(value).strip()
        for name, value in strategy_notes.items()
        if str(value).strip()
    }
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    if normalized:
        DISABLED_REASON_FILE.write_text(json.dumps(normalized, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    elif DISABLED_REASON_FILE.exists():
        DISABLED_REASON_FILE.unlink()

    log_event(
        event_type="scheduler_control",
        status="updated",
        source="scheduler_control",
        message="Scheduler disabled strategy notes updated.",
        payload={
            "disabled_strategy_notes": normalized,
            "disabled_reason_file": str(DISABLED_REASON_FILE),
        },
    )
    return {
        "disabled_strategy_notes": normalized,
        "disabled_reason_file": str(DISABLED_REASON_FILE),
    }


def set_effective_strategy_limit(limit: Optional[int]) -> Dict[str, Union[str, int, None]]:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    normalized_limit = int(limit) if limit is not None else None
    if normalized_limit is not None and normalized_limit <= 0:
        normalized_limit = None

    if normalized_limit is None:
        if EFFECTIVE_LIMIT_FILE.exists():
            EFFECTIVE_LIMIT_FILE.unlink()
    else:
        EFFECTIVE_LIMIT_FILE.write_text(f"{normalized_limit}\n", encoding="utf-8")

    log_event(
        event_type="scheduler_control",
        status="updated",
        source="scheduler_control",
        message="Scheduler effective strategy limit updated.",
        payload={
            "effective_strategy_limit": normalized_limit,
            "effective_limit_file": str(EFFECTIVE_LIMIT_FILE),
        },
    )
    return {
        "effective_strategy_limit": normalized_limit,
        "effective_limit_file": str(EFFECTIVE_LIMIT_FILE),
    }


def get_strategy_status() -> Dict[str, Union[str, List[str]]]:
    active_strategy_names = read_active_strategies()
    disabled_strategy_names = read_disabled_strategies()
    strategy_priorities = read_strategy_priorities()
    disabled_strategy_notes = read_disabled_strategy_notes()
    effective_strategy_limit = read_effective_strategy_limit()
    effective_strategy_names = read_effective_active_strategies()
    available_strategies = list_registered_strategies()
    return {
        "strategy_name": active_strategy_names[0],
        "strategy_names": active_strategy_names,
        "disabled_strategy_names": disabled_strategy_names,
        "effective_strategy_names": effective_strategy_names,
        "strategy_priorities": strategy_priorities,
        "disabled_strategy_notes": disabled_strategy_notes,
        "effective_strategy_limit": effective_strategy_limit,
        "default_strategy": DEFAULT_STRATEGY_NAME,
        "strategy_file": str(STRATEGY_FILE),
        "disabled_strategy_file": str(DISABLED_STRATEGY_FILE),
        "priority_file": str(PRIORITY_FILE),
        "disabled_reason_file": str(DISABLED_REASON_FILE),
        "effective_limit_file": str(EFFECTIVE_LIMIT_FILE),
        "available_strategies": available_strategies,
        "strategy_entries": [
            {
                "strategy_name": name,
                "active": name in active_strategy_names,
                "enabled": name not in disabled_strategy_names,
                "effective": name in effective_strategy_names,
                "priority": strategy_priorities.get(name),
                "disabled_reason": disabled_strategy_notes.get(name),
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
