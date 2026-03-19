from typing import Any, Dict
from typing import Optional

from app.core.db import DBConnection
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.risk.risk_service import ensure_table as ensure_risk_table
from app.risk.risk_service import evaluate_latest_signal
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.ma_cross import generate_signal


def run_strategy_job(connection: DBConnection) -> Dict[str, Any]:
    ensure_signals_table(connection)
    signal_result = generate_signal(connection)
    if signal_result is None:
        return {
            "status": "completed",
            "steps": [
                {"step": "generate_signal", "status": "skipped", "reason": "Not enough candle data"},
            ],
            "terminal_message": "Pipeline run completed with skipped signal generation.",
        }

    ensure_positions_table(connection)
    ensure_risk_table(connection)
    risk_result = evaluate_latest_signal(connection)
    if risk_result is None:
        return {
            "status": "completed",
            "steps": [
                {"step": "generate_signal", **signal_result},
                {"step": "evaluate_risk", "status": "skipped", "reason": "No signal found"},
            ],
            "terminal_message": "Pipeline run completed with skipped risk evaluation.",
        }

    return {
        "status": "ok",
        "steps": [
            {"step": "generate_signal", **signal_result},
            {"step": "evaluate_risk", **risk_result},
        ],
    }
