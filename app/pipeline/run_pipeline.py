from typing import Any, Dict, List, Optional

from app.core.db import DB_FILE, get_connection
from app.core.db import get_database_label
from app.core.job_queue import run_job
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.execution.adapter import get_execution_backend_status
from app.execution.adapter import get_execution_adapter_name
from app.system.kill_switch import get_kill_switch_status
from app.system.kill_switch import kill_switch_enabled
from app.pipeline.runtime_summary import build_pipeline_payload
from app.pipeline.runtime_summary import record_pipeline_runtime


def _step_scope_prefix(step_result: Dict[str, Any]) -> str:
    scope_parts: List[str] = []
    if "strategy_name" in step_result:
        scope_parts.append(f"strategy={step_result['strategy_name']}")
    if "symbol" in step_result:
        scope_parts.append(f"symbol={step_result['symbol']}")
    if not scope_parts:
        return ""
    return "[" + " ".join(scope_parts) + "] "


def _finalize_result(result: Dict[str, Any], status: str, message: str) -> Dict[str, Any]:
    return record_pipeline_runtime(result, status=status, message=message, source="pipeline")


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
    record_pipeline_runtime(
        {"database": database_label, "strategy_name": strategy_name, "steps": []},
        status="started",
        message="Pipeline run started.",
        source="pipeline",
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
            result["steps"].append(
                run_job(
                    connection,
                    "market_data",
                    payload={"symbol_names": symbol_names},
                )
            )

            current_step = "generate_signal"
            strategy_job_result = run_job(
                connection,
                "strategy",
                payload={
                    "strategy_name": strategy_name,
                    "symbol_names": symbol_names,
                },
            )
            result["steps"].extend(strategy_job_result["steps"])
            if strategy_job_result.get("status") == "completed":
                return _finalize_result(
                    result,
                    "completed",
                    str(strategy_job_result["terminal_message"]),
                )

            current_step = "evaluate_risk"
            risk_job_result = run_job(
                connection,
                "risk",
                payload={
                    "signal_ids": strategy_job_result.get("signal_ids"),
                    "symbol_names": symbol_names,
                },
            )
            result["steps"].extend(risk_job_result["steps"])
            if risk_job_result.get("status") == "completed":
                return _finalize_result(
                    result,
                    "completed",
                    str(risk_job_result.get("terminal_message", "Pipeline run completed with skipped risk evaluation.")),
                )

            current_step = "paper_execute"
            execution_job_result = run_job(
                connection,
                "execution",
                payload={
                    "risk_event_ids": risk_job_result.get("risk_event_ids"),
                    "symbol_names": symbol_names,
                },
            )
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
