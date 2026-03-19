from typing import Any, Dict

from app.core.db import DBConnection
from app.execution.paper_broker import ensure_tables as ensure_execution_tables
from app.execution.paper_broker import execute_latest_risk
from app.portfolio.pnl_service import ensure_table as ensure_pnl_table
from app.portfolio.pnl_service import update_pnl_snapshots
from app.portfolio.positions_service import update_positions


def run_execution_job(connection: DBConnection) -> Dict[str, Any]:
    ensure_execution_tables(connection)
    execution_result = execute_latest_risk(connection)
    if execution_result is None:
        paper_execute_step = {"step": "paper_execute", "status": "skipped", "reason": "No risk event found"}
    else:
        paper_execute_step = {"step": "paper_execute", **execution_result}

    updated_positions = update_positions(connection)
    ensure_pnl_table(connection)
    snapshot_count = update_pnl_snapshots(connection)
    return {
        "status": "ok",
        "steps": [
            paper_execute_step,
            {"step": "update_positions", "updated_symbols": updated_positions},
            {"step": "update_pnl", "snapshot_count": snapshot_count},
        ],
    }
