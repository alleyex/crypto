from typing import Any, Dict, Optional

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
    symbol_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    ensure_signals_table(connection)
    if symbol_names is None:
        from app.scheduler.control import read_active_symbols

        symbol_names = read_active_symbols()
    active_symbol_names = list(dict.fromkeys(symbol_names))
    signal_steps: list[dict[str, Any]] = []
    generated_signal_results: list[dict[str, Any]] = []
    for symbol_name in active_symbol_names:
        signal_result = generate_registered_signal(connection, strategy_name=strategy_name, symbol=symbol_name)
        if signal_result is None:
            signal_steps.append(
                {
                    "step": "generate_signal",
                    "status": "skipped",
                    "reason": "Not enough candle data",
                    "strategy_name": strategy_name,
                    "symbol": symbol_name,
                }
            )
            continue
        generated_signal_results.append(signal_result)
        signal_steps.append({"step": "generate_signal", **signal_result})

    if not generated_signal_results:
        return {
            "status": "completed",
            "steps": signal_steps,
            "terminal_message": "Pipeline run completed with skipped signal generation.",
        }

    ensure_positions_table(connection)
    ensure_risk_table(connection)
    risk_result = evaluate_latest_signal(connection)
    if risk_result is None:
        return {
            "status": "completed",
            "steps": signal_steps + [{"step": "evaluate_risk", "status": "skipped", "reason": "No signal found"}],
            "terminal_message": "Pipeline run completed with skipped risk evaluation.",
        }

    return {
        "status": "ok",
        "steps": signal_steps + [{"step": "evaluate_risk", **risk_result}],
    }


def run_strategy_jobs(
    connection: DBConnection,
    strategy_names: list[str],
) -> Dict[str, Any]:
    normalized_names = list(dict.fromkeys(strategy_names or [DEFAULT_STRATEGY_NAME]))
    results = [run_strategy_job(connection, strategy_name=name) for name in normalized_names]

    if any(result.get("status") == "ok" for result in results):
        status = "ok"
    else:
        status = "completed"

    return {
        "status": status,
        "strategy_names": normalized_names,
        "steps": [step for result in results for step in result.get("steps", [])],
        "results": results,
    }
