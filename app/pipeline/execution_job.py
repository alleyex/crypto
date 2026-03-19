from typing import Any, Dict, Optional

from app.core.db import DBConnection
from app.execution.adapter import get_execution_adapter
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions


def run_execution_job(
    connection: DBConnection,
    risk_event_ids: Optional[list[int]] = None,
    symbol_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    execution_adapter = get_execution_adapter()
    execution_adapter.ensure_tables(connection)
    if risk_event_ids is not None:
        execution_results = execution_adapter.execute_risk_event_ids(connection, risk_event_ids)
        if execution_results:
            paper_execute_steps = [{"step": "paper_execute", **execution_result} for execution_result in execution_results]
        else:
            paper_execute_steps = [{"step": "paper_execute", "status": "skipped", "reason": "No risk events selected"}]
    else:
        execution_results = execution_adapter.execute_pending_approved_risks(connection, symbol_names=symbol_names)
        if execution_results:
            paper_execute_steps = [{"step": "paper_execute", **execution_result} for execution_result in execution_results]
        else:
            latest_execution_result = execution_adapter.execute_latest_risk(connection)
            if latest_execution_result is None:
                paper_execute_steps = [{"step": "paper_execute", "status": "skipped", "reason": "No risk event found"}]
            else:
                paper_execute_steps = [{"step": "paper_execute", **latest_execution_result}]

    updated_positions = update_positions(connection)
    ensure_pnl_table(connection)
    snapshot_count = update_pnl_snapshots(connection)
    return {
        "status": "ok",
        "steps": paper_execute_steps
        + [
            {"step": "update_positions", "updated_symbols": updated_positions},
            {"step": "update_pnl", "snapshot_count": snapshot_count},
        ],
    }
