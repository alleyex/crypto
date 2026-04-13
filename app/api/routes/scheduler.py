from typing import Any, Literal, Union

from fastapi import APIRouter, Query

from app.api.errors import http_bad_request
from pydantic import BaseModel

from app.data.symbols import DEFAULT_SYMBOL
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.scheduler.control import (
    build_strategy_priority_preset,
    clear_stop_flag,
    get_stop_status,
    get_strategy_status,
    get_symbol_status,
    read_active_timeframes,
    read_scheduler_log,
    set_active_strategies,
    set_active_strategy,
    set_active_symbols,
    set_active_timeframes,
    set_disabled_strategies,
    set_disabled_strategy_notes,
    set_effective_strategy_limit,
    set_stop_flag,
    set_strategy_priorities,
)
from app.system.kill_switch import (
    disable_kill_switch,
    enable_kill_switch,
    get_kill_switch_status,
)

router = APIRouter()

class SchedulerControlRequest(BaseModel):
    audit_action: str | None = None
    audit_message: str | None = None

class SchedulerStrategyRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    strategy_names: list[str] | None = None
    disabled_strategy_names: list[str] | None = None
    strategy_priorities: dict[str, int] | None = None
    disabled_strategy_notes: dict[str, str] | None = None
    effective_strategy_limit: int | None = None
    audit_action: str | None = None
    audit_message: str | None = None

class SchedulerStrategyPresetRequest(BaseModel):
    preset: Literal["sequential", "reverse", "active_first", "reset"]

class SchedulerStrategyLimitPresetRequest(BaseModel):
    preset: Literal["top_1", "top_2", "all_enabled"]

class SchedulerSymbolsRequest(BaseModel):
    symbol: str = DEFAULT_SYMBOL
    symbol_names: list[str] | None = None

class SchedulerTimeframesRequest(BaseModel):
    timeframe_names: list[str]

class KillSwitchControlRequest(BaseModel):
    reason: str | None = None
    source: str | None = None
    notify_message: str | None = None

@router.get("/scheduler/status")
def scheduler_status() -> dict:
    return get_stop_status()

@router.get("/scheduler/strategy")
def scheduler_strategy_status() -> dict[str, Any]:
    return get_strategy_status()

@router.get("/scheduler/symbols")
def scheduler_symbol_status() -> dict[str, Any]:
    return get_symbol_status()

@router.post("/scheduler/strategy")
def scheduler_strategy_update(payload: SchedulerStrategyRequest) -> dict[str, Any]:
    if any(
        value is not None
        for value in (
            payload.strategy_names,
            payload.disabled_strategy_names,
            payload.strategy_priorities,
            payload.disabled_strategy_notes,
            payload.effective_strategy_limit,
        )
    ):
        if payload.strategy_names is not None:
            set_active_strategies(
                payload.strategy_names,
                audit_action=payload.audit_action or "set_active_strategies",
                audit_message=payload.audit_message or "Scheduler active strategies updated.",
            )
        if payload.disabled_strategy_names is not None:
            set_disabled_strategies(
                payload.disabled_strategy_names,
                audit_action=payload.audit_action or "set_disabled_strategies",
                audit_message=payload.audit_message or "Scheduler disabled strategies updated.",
            )
        if payload.strategy_priorities is not None:
            set_strategy_priorities(
                payload.strategy_priorities,
                audit_action=payload.audit_action or "set_strategy_priorities",
                audit_message=payload.audit_message or "Scheduler strategy priorities updated.",
            )
        if payload.disabled_strategy_notes is not None:
            set_disabled_strategy_notes(
                payload.disabled_strategy_notes,
                audit_action=payload.audit_action or "set_disabled_strategy_notes",
                audit_message=payload.audit_message or "Scheduler disabled strategy notes updated.",
            )
        if payload.effective_strategy_limit is not None or "effective_strategy_limit" in payload.__fields_set__:
            set_effective_strategy_limit(
                payload.effective_strategy_limit,
                audit_action=payload.audit_action or "set_effective_strategy_limit",
                audit_message=payload.audit_message or "Scheduler effective strategy limit updated.",
            )
        return get_strategy_status()
    return set_active_strategy(payload.strategy_name)

@router.post("/scheduler/symbols")
def scheduler_symbols_update(payload: SchedulerSymbolsRequest) -> dict[str, Any]:
    if payload.symbol_names is not None:
        set_active_symbols(
            payload.symbol_names,
            audit_action="set_active_symbols",
            audit_message="Scheduler active symbols updated.",
        )
        return get_symbol_status()
    return set_active_symbols([payload.symbol])

@router.get("/scheduler/timeframes")
def scheduler_timeframes_get() -> dict[str, Any]:
    from app.data.candles_service import TIMEFRAME_INTERVAL_MS
    active = read_active_timeframes()
    return {
        "timeframe_names": active,
        "supported_timeframes": sorted(TIMEFRAME_INTERVAL_MS.keys()),
    }

@router.post("/scheduler/timeframes")
def scheduler_timeframes_update(payload: SchedulerTimeframesRequest) -> dict[str, Any]:
    try:
        return set_active_timeframes(
            payload.timeframe_names,
            audit_action="set_active_timeframes",
            audit_message="Scheduler active timeframes updated.",
        )
    except ValueError as exc:
        http_bad_request(str(exc))

@router.post("/scheduler/strategy/preset")
def scheduler_strategy_apply_preset(payload: SchedulerStrategyPresetRequest) -> dict[str, Any]:
    status = get_strategy_status()
    priorities = build_strategy_priority_preset(
        payload.preset,
        available_strategies=status.get("available_strategies"),
        active_strategy_names=status.get("strategy_names"),
    )
    set_strategy_priorities(
        priorities,
        audit_action=f"priority_preset:{payload.preset}",
        audit_message=f"Applied scheduler priority preset: {payload.preset}.",
        extra_payload={"preset": payload.preset},
    )
    return get_strategy_status()

@router.post("/scheduler/strategy/limit-preset")
def scheduler_strategy_apply_limit_preset(payload: SchedulerStrategyLimitPresetRequest) -> dict[str, Any]:
    preset_to_limit = {
        "top_1": 1,
        "top_2": 2,
        "all_enabled": None,
    }
    set_effective_strategy_limit(
        preset_to_limit[payload.preset],
        audit_action=f"limit_preset:{payload.preset}",
        audit_message=f"Applied scheduler limit preset: {payload.preset}.",
        extra_payload={"preset": payload.preset},
    )
    return get_strategy_status()

@router.post("/scheduler/stop")
def scheduler_stop(payload: SchedulerControlRequest | None = None) -> dict[str, Union[str, bool]]:
    stop_file = set_stop_flag(
        audit_action=payload.audit_action if payload is not None and payload.audit_action is not None else "stop",
        audit_message=payload.audit_message if payload is not None and payload.audit_message is not None else "Scheduler stop flag set.",
    )
    return {"stopped": True, "stop_file": stop_file}

@router.post("/scheduler/start")
def scheduler_start(payload: SchedulerControlRequest | None = None) -> dict[str, Union[str, bool]]:
    removed, stop_file = clear_stop_flag(
        audit_action=payload.audit_action if payload is not None and payload.audit_action is not None else "start",
        audit_message=payload.audit_message if payload is not None and payload.audit_message is not None else "Scheduler stop flag cleared.",
    )
    return {"stopped": False, "stop_file": stop_file, "flag_removed": removed}

@router.get("/scheduler/logs")
def scheduler_logs(
    lines: int = Query(default=20, ge=1, le=500),
    mode: str = Query(default="all"),
) -> dict[str, Any]:
    return {"mode": mode, "lines": read_scheduler_log(lines=lines, mode=mode)}

# ---------------------------------------------------------------------------
# Kill-switch endpoints
# ---------------------------------------------------------------------------

@router.get("/kill-switch/status")
def kill_switch_status() -> dict:
    return get_kill_switch_status()

@router.post("/kill-switch/enable")
def kill_switch_enable(payload: KillSwitchControlRequest | None = None) -> dict[str, Union[str, bool]]:
    kill_switch_file = enable_kill_switch(
        reason=payload.reason if payload is not None and payload.reason is not None else "Kill switch enabled.",
        source=payload.source if payload is not None and payload.source is not None else "kill_switch",
        notify_message=payload.notify_message if payload is not None else "Crypto alert: kill switch has been enabled.",
    )
    return {"enabled": True, "kill_switch_file": kill_switch_file}

@router.post("/kill-switch/disable")
def kill_switch_disable() -> dict[str, Union[str, bool]]:
    removed, kill_switch_file = disable_kill_switch()
    return {"enabled": False, "kill_switch_file": kill_switch_file, "flag_removed": removed}
