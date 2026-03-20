from typing import Any, Dict, Optional

from app.core.db import DBConnection
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.risk.risk_service import evaluate_signal_id
from app.risk.risk_service import ensure_table as ensure_risk_table
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
    risk_steps: list[dict[str, Any]] = []
    generated_risk_results: list[dict[str, Any]] = []
    for signal_result in generated_signal_results:
        risk_result = evaluate_signal_id(connection, int(signal_result["id"]))
        if risk_result is None:
            risk_steps.append({"step": "evaluate_risk", "status": "skipped", "reason": "No signal found"})
            continue
        generated_risk_results.append(risk_result)
        risk_steps.append({"step": "evaluate_risk", **risk_result})

    if not generated_risk_results:
        return {
            "status": "completed",
            "steps": signal_steps + risk_steps,
            "terminal_message": "Pipeline run completed with skipped risk evaluation.",
        }

    return {
        "status": "ok",
        "symbol_names": active_symbol_names,
        "risk_event_ids": [int(result["id"]) for result in generated_risk_results],
        "steps": signal_steps + risk_steps,
    }


def run_strategy_jobs(
    connection: DBConnection,
    strategy_names: list[str],
    symbol_names: Optional[list[str]] = None,
) -> Dict[str, Any]:
    normalized_names = list(dict.fromkeys(strategy_names or [DEFAULT_STRATEGY_NAME]))
    results = []
    for name in normalized_names:
        try:
            results.append(run_strategy_job(connection, strategy_name=name, symbol_names=symbol_names))
        except Exception as exc:
            results.append(
                {
                    "status": "error",
                    "strategy_name": name,
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "steps": [
                        {
                            "step": "run_strategy_job",
                            "status": "error",
                            "strategy_name": name,
                            "error": str(exc),
                            "error_type": type(exc).__name__,
                        }
                    ],
                }
            )

    has_error = any(result.get("status") == "error" for result in results)
    has_ok = any(result.get("status") == "ok" for result in results)
    if has_error:
        status = "partial_error"
    elif has_ok:
        status = "ok"
    else:
        status = "completed"

    return {
        "status": status,
        "strategy_names": normalized_names,
        "symbol_names": list(dict.fromkeys(symbol_names or [])),
        "steps": [step for result in results for step in result.get("steps", [])],
        "results": results,
    }
