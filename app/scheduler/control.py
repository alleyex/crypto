import json
from typing import Dict, List, Literal, Optional, Tuple, Union

from app.alerting.telegram import send_telegram_message
from app.audit.service import log_event
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.data.symbols import DEFAULT_SYMBOL
from app.data.symbols import list_supported_symbols
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
SYMBOL_FILE = RUNTIME_DIR / "scheduler.symbols"
StrategyPriorityPreset = Literal["sequential", "reverse", "active_first", "reset"]


def _log_scheduler_control_event(
    *,
    status: str,
    message: str,
    payload: dict[str, Union[str, int, bool, list[str], dict[str, int], dict[str, str], None]],
    action: str,
) -> None:
    audit_payload = {"action": action, **payload}
    log_event(
        event_type="scheduler_control",
        status=status,
        source="scheduler_control",
        message=message,
        payload=audit_payload,
    )


def set_stop_flag(
    *,
    audit_action: str = "stop",
    audit_message: str = "Scheduler stop flag set.",
) -> str:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    STOP_FILE.write_text("stop\n", encoding="utf-8")
    send_telegram_message("Crypto alert: scheduler stop flag has been set.")
    _log_scheduler_control_event(
        status="stopped",
        action=audit_action,
        message=audit_message,
        payload={"stop_file": str(STOP_FILE)},
    )
    return str(STOP_FILE)


def clear_stop_flag(
    *,
    audit_action: str = "start",
    audit_message: str = "Scheduler stop flag cleared.",
) -> Tuple[bool, str]:
    if STOP_FILE.exists():
        STOP_FILE.unlink()
        _log_scheduler_control_event(
            status="started",
            action=audit_action,
            message=audit_message,
            payload={"stop_file": str(STOP_FILE), "flag_removed": True},
        )
        return True, str(STOP_FILE)
    _log_scheduler_control_event(
        status="started",
        action=audit_action,
        message=audit_message if audit_message != "Scheduler stop flag cleared." else "Scheduler start requested but no stop flag was present.",
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


def read_active_symbols() -> list[str]:
    if not SYMBOL_FILE.exists():
        return [DEFAULT_SYMBOL]

    configured_names = [
        item.strip()
        for item in SYMBOL_FILE.read_text(encoding="utf-8").splitlines()
        if item.strip()
    ]
    if not configured_names:
        return [DEFAULT_SYMBOL]

    allowed_names = set(list_supported_symbols())
    normalized_names = [name for name in dict.fromkeys(configured_names) if name in allowed_names]
    if not normalized_names:
        return [DEFAULT_SYMBOL]
    return normalized_names


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


def build_strategy_priority_preset(
    preset: StrategyPriorityPreset,
    available_strategies: Optional[list[str]] = None,
    active_strategy_names: Optional[list[str]] = None,
) -> dict[str, int]:
    available_names = list(dict.fromkeys(available_strategies or list_registered_strategies()))
    active_names = list(dict.fromkeys(active_strategy_names or read_active_strategies()))

    if preset in {"sequential", "reset"}:
        ordered_names = available_names
    elif preset == "reverse":
        ordered_names = list(reversed(available_names))
    elif preset == "active_first":
        active_set = set(active_names)
        ordered_names = [name for name in available_names if name in active_set]
        ordered_names.extend(name for name in available_names if name not in active_set)
    else:
        raise ValueError(f"Unknown strategy priority preset: {preset}")

    return {strategy_name: index for index, strategy_name in enumerate(ordered_names)}


def set_active_strategy(strategy_name: str) -> Dict[str, str]:
    result = set_active_strategies([strategy_name])
    return {"strategy_name": result["strategy_name"], "strategy_file": result["strategy_file"]}


def set_active_symbols(
    symbol_names: list[str],
    *,
    audit_action: str = "set_active_symbols",
    audit_message: str = "Scheduler active symbols updated.",
) -> Dict[str, Union[str, list[str]]]:
    if not symbol_names:
        raise ValueError("At least one symbol must be provided.")

    unique_names = list(dict.fromkeys(symbol_names))
    allowed_names = set(list_supported_symbols())
    invalid_names = [name for name in unique_names if name not in allowed_names]
    if invalid_names:
        raise ValueError(f"Unknown symbols: {', '.join(invalid_names)}")

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    SYMBOL_FILE.write_text("\n".join(unique_names) + "\n", encoding="utf-8")
    _log_scheduler_control_event(
        status="updated",
        action=audit_action,
        message=audit_message,
        payload={
            "symbol": unique_names[0],
            "symbol_names": unique_names,
            "symbol_file": str(SYMBOL_FILE),
        },
    )
    return {
        "symbol": unique_names[0],
        "symbol_names": unique_names,
        "symbol_file": str(SYMBOL_FILE),
    }


def set_active_strategies(
    strategy_names: list[str],
    *,
    audit_action: str = "set_active_strategies",
    audit_message: str = "Scheduler active strategies updated.",
) -> Dict[str, Union[str, list[str]]]:
    if not strategy_names:
        raise ValueError("At least one strategy must be provided.")

    unique_names = list(dict.fromkeys(strategy_names))
    allowed_names = set(list_registered_strategies())
    invalid_names = [name for name in unique_names if name not in allowed_names]
    if invalid_names:
        raise ValueError(f"Unknown strategies: {', '.join(invalid_names)}")

    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    STRATEGY_FILE.write_text("\n".join(unique_names) + "\n", encoding="utf-8")
    _log_scheduler_control_event(
        status="updated",
        action=audit_action,
        message=audit_message,
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


def set_disabled_strategies(
    strategy_names: list[str],
    *,
    audit_action: str = "set_disabled_strategies",
    audit_message: str = "Scheduler disabled strategies updated.",
) -> Dict[str, Union[str, list[str]]]:
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

    _log_scheduler_control_event(
        status="updated",
        action=audit_action,
        message=audit_message,
        payload={
            "disabled_strategy_names": unique_names,
            "disabled_strategy_file": str(DISABLED_STRATEGY_FILE),
        },
    )
    return {
        "disabled_strategy_names": unique_names,
        "disabled_strategy_file": str(DISABLED_STRATEGY_FILE),
    }


def set_strategy_priorities(
    strategy_priorities: dict[str, int],
    *,
    audit_action: str = "set_strategy_priorities",
    audit_message: str = "Scheduler strategy priorities updated.",
    extra_payload: Optional[dict[str, Union[str, int, bool, list[str], dict[str, int], dict[str, str], None]]] = None,
) -> Dict[str, Union[str, dict[str, int]]]:
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

    _log_scheduler_control_event(
        status="updated",
        action=audit_action,
        message=audit_message,
        payload={
            "strategy_priorities": normalized,
            "priority_file": str(PRIORITY_FILE),
            **(extra_payload or {}),
        },
    )
    return {
        "strategy_priorities": normalized,
        "priority_file": str(PRIORITY_FILE),
    }


def set_disabled_strategy_notes(
    strategy_notes: dict[str, str],
    *,
    audit_action: str = "set_disabled_strategy_notes",
    audit_message: str = "Scheduler disabled strategy notes updated.",
) -> Dict[str, Union[str, dict[str, str]]]:
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

    _log_scheduler_control_event(
        status="updated",
        action=audit_action,
        message=audit_message,
        payload={
            "disabled_strategy_notes": normalized,
            "disabled_reason_file": str(DISABLED_REASON_FILE),
        },
    )
    return {
        "disabled_strategy_notes": normalized,
        "disabled_reason_file": str(DISABLED_REASON_FILE),
    }


def set_effective_strategy_limit(
    limit: Optional[int],
    *,
    audit_action: str = "set_effective_strategy_limit",
    audit_message: str = "Scheduler effective strategy limit updated.",
    extra_payload: Optional[dict[str, Union[str, int, bool, list[str], dict[str, int], dict[str, str], None]]] = None,
) -> Dict[str, Union[str, int, None]]:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    normalized_limit = int(limit) if limit is not None else None
    if normalized_limit is not None and normalized_limit <= 0:
        normalized_limit = None

    if normalized_limit is None:
        if EFFECTIVE_LIMIT_FILE.exists():
            EFFECTIVE_LIMIT_FILE.unlink()
    else:
        EFFECTIVE_LIMIT_FILE.write_text(f"{normalized_limit}\n", encoding="utf-8")

    _log_scheduler_control_event(
        status="updated",
        action=audit_action,
        message=audit_message,
        payload={
            "effective_strategy_limit": normalized_limit,
            "effective_limit_file": str(EFFECTIVE_LIMIT_FILE),
            **(extra_payload or {}),
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


def get_symbol_status() -> Dict[str, Union[str, List[str]]]:
    active_symbol_names = read_active_symbols()
    available_symbols = list_supported_symbols()
    return {
        "symbol": active_symbol_names[0],
        "symbol_names": active_symbol_names,
        "default_symbol": DEFAULT_SYMBOL,
        "symbol_file": str(SYMBOL_FILE),
        "available_symbols": available_symbols,
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
