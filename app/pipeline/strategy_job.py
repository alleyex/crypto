from typing import Any, Dict
from typing import Optional

from app.core.db import DBConnection
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.risk.risk_service import ensure_table as ensure_risk_table
from app.risk.risk_service import evaluate_latest_signal
from app.strategy.ma_cross import ensure_table as ensure_signals_table
from app.strategy.registry import generate_registered_signal


def run_strategy_job(
    connection: DBConnection,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
) -> Dict[str, Any]:
    ensure_signals_table(connection)
    signal_result = generate_registered_signal(connection, strategy_name=strategy_name)
    if signal_result is None:
        return {
            "status": "completed",
            "steps": [
                {
                    "step": "generate_signal",
                    "status": "skipped",
                    "reason": "Not enough candle data",
                    "strategy_name": strategy_name,
                },
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
