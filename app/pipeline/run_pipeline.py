from typing import Any, Dict, List, Optional

from app.audit.service import log_event
from app.core.db import DB_FILE, get_connection
from app.core.db import get_database_label
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.strategy_job import run_strategy_job
from app.system.heartbeat import record_heartbeat
from app.system.kill_switch import get_kill_switch_status
from app.system.kill_switch import kill_switch_enabled


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
        payload={"step_count": len(result.get("steps", []))},
    )
    _safe_log_event(
        event_type="pipeline_run",
        status=status,
        source="pipeline",
        message=message,
        payload=result,
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


def _initial_pipeline_failure_result(database_label: str, step: str, exc: Exception) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "database": database_label,
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


def run_pipeline_collect() -> Dict[str, Any]:
    database_label = get_database_label()
    try:
        connection = get_connection()
        try:
            run_migrations(connection)
        finally:
            connection.close()
    except Exception as exc:
        return _initial_pipeline_failure_result(database_label, "run_migrations", exc)
    result: Dict[str, Any] = {"database": database_label, "steps": []}
    _safe_record_heartbeat(
        component="pipeline",
        status="started",
        message="Pipeline run started.",
        payload={"database": database_label},
    )
    _safe_log_event(
        event_type="pipeline_run",
        status="started",
        source="pipeline",
        message="Pipeline run started.",
        payload={"database": database_label},
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
            result["steps"].append(run_market_data_job(connection))

            current_step = "generate_signal"
            strategy_job_result = run_strategy_job(connection, strategy_name=DEFAULT_STRATEGY_NAME)
            result["steps"].extend(strategy_job_result["steps"])
            if strategy_job_result.get("status") == "completed":
                return _finalize_result(
                    result,
                    "completed",
                    str(strategy_job_result["terminal_message"]),
                )

            current_step = "paper_execute"
            execution_job_result = run_execution_job(connection)
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
            print(f"Saved {step_result['saved_klines']} klines to {DB_FILE}")
        elif step == "generate_signal":
            if step_result.get("status") == "skipped":
                print(step_result["reason"])
            else:
                print(f"short_ma={step_result['short_ma']:.2f}")
                print(f"long_ma={step_result['long_ma']:.2f}")
                print(f"signal={step_result['signal_type']}")
        elif step == "evaluate_risk":
            if step_result.get("status") == "skipped":
                print(step_result["reason"])
            else:
                print(f"decision={step_result['decision']}")
                print(f"reason={step_result['reason']}")
        elif step == "paper_execute":
            if step_result.get("status") == "FILLED":
                print(f"executed_signal={step_result['side']}")
                print(f"qty={step_result['qty']}")
                print(f"price={step_result['price']}")
                print("order_status=FILLED")
            elif "decision" in step_result:
                print(f"Latest risk event is not executable: {step_result['decision']}")
            else:
                print(f"Execution skipped: {step_result.get('reason', step_result)}")
        elif step == "update_positions":
            print("Positions updated.")
        elif step == "update_pnl":
            print(f"Saved {step_result['snapshot_count']} pnl snapshot(s).")


def run_pipeline() -> Dict[str, Any]:
    result = run_pipeline_collect()
    print_pipeline_result(result)
    return result
