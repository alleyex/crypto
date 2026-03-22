from typing import Any, Dict, Optional

from app.audit.service import log_event
from app.core.db import get_database_label
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.execution.adapter import get_execution_adapter_name
from app.execution.adapter import get_execution_backend_status
from app.system.heartbeat import record_heartbeat


def build_pipeline_payload(result: Dict[str, Any]) -> Dict[str, Any]:
    symbol_names: list[str] = []
    strategy_names: list[str] = []
    generated_signal_count = 0
    approved_risk_count = 0
    rejected_risk_count = 0
    filled_execution_count = 0

    for step_result in result.get("steps", []):
        symbol_name = step_result.get("symbol")
        if symbol_name is not None and symbol_name not in symbol_names:
            symbol_names.append(symbol_name)

        strategy_name = step_result.get("strategy_name")
        if strategy_name is not None and strategy_name not in strategy_names:
            strategy_names.append(strategy_name)

        if step_result.get("step") == "save_klines":
            for symbol_result in step_result.get("symbol_results", []):
                result_symbol = symbol_result.get("symbol")
                if result_symbol is not None and result_symbol not in symbol_names:
                    symbol_names.append(result_symbol)

        if step_result.get("step") == "generate_signal" and "signal_type" in step_result:
            generated_signal_count += 1
        elif step_result.get("step") == "evaluate_risk":
            if step_result.get("decision") == "APPROVED":
                approved_risk_count += 1
            elif step_result.get("decision") == "REJECTED":
                rejected_risk_count += 1
        elif step_result.get("step") == "paper_execute" and step_result.get("status") == "FILLED":
            filled_execution_count += 1

    requested_symbols = list(dict.fromkeys(result.get("requested_symbol_names") or result.get("symbol_names") or []))
    for symbol_name in requested_symbols:
        if symbol_name not in symbol_names:
            symbol_names.append(symbol_name)

    requested_strategies = list(dict.fromkeys(result.get("strategy_names") or []))
    for strategy_name in requested_strategies:
        if strategy_name not in strategy_names:
            strategy_names.append(strategy_name)

    if not strategy_names and result.get("strategy_name") is not None:
        strategy_names = [str(result["strategy_name"])]

    payload: Dict[str, Any] = {
        "step_count": len(result.get("steps", [])),
        "strategy_name": result.get("strategy_name", DEFAULT_STRATEGY_NAME),
        "strategy_names": strategy_names,
        "symbol_names": symbol_names,
        "execution_backend": get_execution_adapter_name(),
        "execution_backend_status": result.get("execution_backend_status") or get_execution_backend_status(),
        "generated_signal_count": generated_signal_count,
        "approved_risk_count": approved_risk_count,
        "rejected_risk_count": rejected_risk_count,
        "filled_execution_count": filled_execution_count,
    }
    if "database" in result:
        payload["database"] = result["database"]
    return payload


def record_pipeline_runtime(
    result: Dict[str, Any],
    *,
    status: str,
    message: str,
    source: str = "pipeline",
) -> Dict[str, Any]:
    if "database" not in result:
        result["database"] = get_database_label()
    payload = build_pipeline_payload(result)
    try:
        record_heartbeat(component="pipeline", status=status, message=message, payload=payload)
    except Exception:
        pass
    try:
        log_event(
            event_type="pipeline_run",
            status=status,
            source=source,
            message=message,
            payload={**result, "summary": payload},
        )
    except Exception:
        pass
    return result
