from typing import Any, Dict, List, Optional

from app.audit.service import log_event
from app.core.db import DB_FILE, get_connection
from app.core.db import get_database_label
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.execution.adapter import get_execution_adapter_name
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.strategy_job import run_strategy_job
from app.system.heartbeat import record_heartbeat
from app.system.kill_switch import get_kill_switch_status
from app.system.kill_switch import kill_switch_enabled


def _build_pipeline_payload(result: Dict[str, Any]) -> Dict[str, Any]:
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

    if not strategy_names and result.get("strategy_name") is not None:
        strategy_names = [str(result["strategy_name"])]

    payload: Dict[str, Any] = {
        "step_count": len(result.get("steps", [])),
        "strategy_name": result.get("strategy_name", DEFAULT_STRATEGY_NAME),
        "strategy_names": strategy_names,
        "symbol_names": symbol_names,
        "execution_backend": get_execution_adapter_name(),
        "generated_signal_count": generated_signal_count,
        "approved_risk_count": approved_risk_count,
        "rejected_risk_count": rejected_risk_count,
        "filled_execution_count": filled_execution_count,
    }
    if "database" in result:
        payload["database"] = result["database"]
    return payload


def _step_scope_prefix(step_result: Dict[str, Any]) -> str:
    scope_parts: List[str] = []
    if "strategy_name" in step_result:
        scope_parts.append(f"strategy={step_result['strategy_name']}")
    if "symbol" in step_result:
        scope_parts.append(f"symbol={step_result['symbol']}")
    if not scope_parts:
        return ""
    return "[" + " ".join(scope_parts) + "] "


def _safe_record_heartbeat(
    component: str,
    status: str,
    message: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        record_heartbeat(component=component, status=status, message=message, payload=payload)
    except Exception:
        pass


def _safe_log_event(
    event_type: str,
    status: str,
    source: str,
    message: str,
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        log_event(event_type=event_type, status=status, source=source, message=message, payload=payload)
    except Exception:
        pass


def _finalize_result(result: Dict[str, Any], status: str, message: str) -> Dict[str, Any]:
    _safe_record_heartbeat(
        component="pipeline",
        status=status,
        message=message,
        payload=_build_pipeline_payload(result),
    )
    _safe_log_event(
        event_type="pipeline_run",
        status=status,
        source="pipeline",
        message=message,
        payload={**result, "summary": _build_pipeline_payload(result)},
    )
    return result


def _pipeline_failure_result(result: Dict[str, Any], step: str, exc: Exception) -> Dict[str, Any]:
    result["steps"].append(
        {
            "step": step,
            "status": "failed",
            "error": str(exc),
            "error_type": exc.__class__.__name__,
        }
    )
    return _finalize_result(result, "failed", f"Pipeline run failed during {step}: {exc}")


def _initial_pipeline_failure_result(
    database_label: str,
    step: str,
    exc: Exception,
    strategy_name: str,
) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "database": database_label,
        "strategy_name": strategy_name,
        "steps": [
            {
                "step": step,
                "status": "failed",
                "error": str(exc),
                "error_type": exc.__class__.__name__,
            }
        ],
    }
    return _finalize_result(result, "failed", f"Pipeline run failed during {step}: {exc}")


def run_pipeline_collect(
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    symbol_names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    database_label = get_database_label()
    try:
        connection = get_connection()
        try:
            run_migrations(connection)
        finally:
            connection.close()
    except Exception as exc:
        return _initial_pipeline_failure_result(database_label, "run_migrations", exc, strategy_name)
    result: Dict[str, Any] = {"database": database_label, "strategy_name": strategy_name, "steps": []}
    if symbol_names is not None:
        result["requested_symbol_names"] = list(dict.fromkeys(symbol_names))
    _safe_record_heartbeat(
        component="pipeline",
        status="started",
        message="Pipeline run started.",
        payload=_build_pipeline_payload(
            {"database": database_label, "strategy_name": strategy_name, "steps": []}
        ),
    )
    _safe_log_event(
        event_type="pipeline_run",
        status="started",
        source="pipeline",
        message="Pipeline run started.",
        payload={
            "database": database_label,
            "strategy_name": strategy_name,
            "summary": _build_pipeline_payload(
                {"database": database_label, "strategy_name": strategy_name, "steps": []}
            ),
        },
    )

    if kill_switch_enabled():
        result["steps"].append(
            {
                "step": "kill_switch",
                "status": "blocked",
                **get_kill_switch_status(),
                "reason": "Kill switch is enabled.",
            }
        )
        return _finalize_result(result, "blocked", "Pipeline run blocked by kill switch.")

    connection = get_connection()
    try:
        current_step = "save_klines"
        try:
            run_migrations(connection)
            result["steps"].append(run_market_data_job(connection, symbol_names=symbol_names))

            current_step = "generate_signal"
            strategy_job_result = run_strategy_job(connection, strategy_name=strategy_name, symbol_names=symbol_names)
            result["steps"].extend(strategy_job_result["steps"])
            if strategy_job_result.get("status") == "completed":
                return _finalize_result(
                    result,
                    "completed",
                    str(strategy_job_result["terminal_message"]),
                )

            current_step = "paper_execute"
            execution_job_result = run_execution_job(connection, risk_event_ids=strategy_job_result.get("risk_event_ids"))
            result["steps"].extend(execution_job_result["steps"])
        except Exception as exc:
            return _pipeline_failure_result(result, current_step, exc)
    finally:
        connection.close()

    return _finalize_result(result, "completed", "Pipeline run completed.")


def print_pipeline_result(result: Dict[str, Any]) -> None:
    for step_result in result["steps"]:
        step = step_result["step"]
        if step == "save_klines":
            symbol_results = step_result.get("symbol_results") or []
            if symbol_results:
                for symbol_result in symbol_results:
                    print(
                        f"[symbol={symbol_result['symbol']}] "
                        f"saved_klines={symbol_result['saved_klines']} to {DB_FILE}"
                    )
            else:
                print(f"Saved {step_result['saved_klines']} klines to {DB_FILE}")
        elif step == "generate_signal":
            if step_result.get("status") == "skipped":
                print(f"{_step_scope_prefix(step_result)}{step_result['reason']}")
            else:
                prefix = _step_scope_prefix(step_result)
                print(f"{prefix}short_ma={step_result['short_ma']:.2f}")
                print(f"{prefix}long_ma={step_result['long_ma']:.2f}")
                print(f"{prefix}signal={step_result['signal_type']}")
        elif step == "evaluate_risk":
            if step_result.get("status") == "skipped":
                print(f"{_step_scope_prefix(step_result)}{step_result['reason']}")
            else:
                prefix = _step_scope_prefix(step_result)
                print(f"{prefix}decision={step_result['decision']}")
                print(f"{prefix}reason={step_result['reason']}")
        elif step == "paper_execute":
            prefix = _step_scope_prefix(step_result)
            if step_result.get("status") == "FILLED":
                print(f"{prefix}executed_signal={step_result['side']}")
                print(f"{prefix}qty={step_result['qty']}")
                print(f"{prefix}price={step_result['price']}")
                print(f"{prefix}order_status=FILLED")
            elif "decision" in step_result:
                print(f"{prefix}risk_event_status={step_result['decision']}")
            else:
                print(f"{prefix}execution_skipped={step_result.get('reason', step_result)}")
        elif step == "update_positions":
            print("Positions updated.")
        elif step == "update_pnl":
            print(f"Saved {step_result['snapshot_count']} pnl snapshot(s).")


def run_pipeline() -> Dict[str, Any]:
    result = run_pipeline_collect()
    print_pipeline_result(result)
    return result
