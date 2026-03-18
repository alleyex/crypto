from typing import Any, Dict, List

from app.audit.service import log_event
from app.core.db import DB_FILE, get_connection
from app.data.binance_client import fetch_klines
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import save_klines
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.portfolio.positions_service import update_positions
from app.risk.risk_service import ensure_table as ensure_risk_table
from app.risk.risk_service import evaluate_latest_signal
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.ma_cross import generate_signal
from app.system.heartbeat import record_heartbeat
from app.system.kill_switch import get_kill_switch_status
from app.system.kill_switch import kill_switch_enabled


def _finalize_result(result: Dict[str, Any], status: str, message: str) -> Dict[str, Any]:
    record_heartbeat(
        component="pipeline",
        status=status,
        message=message,
        payload={"step_count": len(result.get("steps", []))},
    )
    log_event(
        event_type="pipeline_run",
        status=status,
        source="pipeline",
        message=message,
        payload=result,
    )
    return result


def run_pipeline_collect() -> Dict[str, Any]:
    result: Dict[str, Any] = {"database": str(DB_FILE), "steps": []}
    record_heartbeat(
        component="pipeline",
        status="started",
        message="Pipeline run started.",
        payload={"database": str(DB_FILE)},
    )
    log_event(
        event_type="pipeline_run",
        status="started",
        source="pipeline",
        message="Pipeline run started.",
        payload={"database": str(DB_FILE)},
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
        ensure_candles_table(connection)
        klines = fetch_klines()
        saved_klines = save_klines(connection, klines)
        result["steps"].append({"step": "save_klines", "saved_klines": saved_klines})

        ensure_signals_table(connection)
        signal_result = generate_signal(connection)
        if signal_result is None:
            result["steps"].append(
                {"step": "generate_signal", "status": "skipped", "reason": "Not enough candle data"}
            )
            return _finalize_result(result, "completed", "Pipeline run completed with skipped signal generation.")
        result["steps"].append({"step": "generate_signal", **signal_result})

        ensure_positions_table(connection)
        ensure_risk_table(connection)
        risk_result = evaluate_latest_signal(connection)
        if risk_result is None:
            result["steps"].append({"step": "evaluate_risk", "status": "skipped", "reason": "No signal found"})
            return _finalize_result(result, "completed", "Pipeline run completed with skipped risk evaluation.")
        result["steps"].append({"step": "evaluate_risk", **risk_result})

        ensure_execution_tables(connection)
        execution_result = execute_latest_risk(connection)
        if execution_result is None:
            result["steps"].append({"step": "paper_execute", "status": "skipped", "reason": "No risk event found"})
        else:
            result["steps"].append({"step": "paper_execute", **execution_result})

        updated_positions = update_positions(connection)
        result["steps"].append({"step": "update_positions", "updated_symbols": updated_positions})

        ensure_pnl_table(connection)
        snapshot_count = update_pnl_snapshots(connection)
        result["steps"].append({"step": "update_pnl", "snapshot_count": snapshot_count})
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
