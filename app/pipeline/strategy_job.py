from typing import Any

from app.core.db import DBConnection
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.core.utils import dedup_ordered
from app.strategy.registry import generate_registered_signal

def run_strategy_job(
    connection: DBConnection,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    symbol_names: list[str] | None = None,
    timeframe_names: list[str] | None = None,
) -> dict[str, Any]:
    if symbol_names is None or timeframe_names is None:
        from app.scheduler.control import read_active_symbols
        from app.scheduler.control import read_active_timeframes

    if symbol_names is None:
        symbol_names = read_active_symbols()
    if timeframe_names is None:
        timeframe_names = read_active_timeframes()
    active_symbol_names = dedup_ordered(symbol_names)
    active_timeframe_names = dedup_ordered(timeframe_names)
    signal_steps: list[dict[str, Any]] = []
    generated_signal_results: list[dict[str, Any]] = []
    had_error = False
    for symbol_name in active_symbol_names:
        for timeframe_name in active_timeframe_names:
            try:
                signal_result = generate_registered_signal(
                    connection,
                    strategy_name=strategy_name,
                    symbol=symbol_name,
                    timeframe=timeframe_name,
                )
            except Exception as exc:
                had_error = True
                signal_steps.append(
                    {
                        "step": "generate_signal",
                        "status": "error",
                        "reason": str(exc),
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                        "strategy_name": strategy_name,
                        "symbol": symbol_name,
                        "timeframe": timeframe_name,
                    }
                )
                continue
            if signal_result is None:
                signal_steps.append(
                    {
                        "step": "generate_signal",
                        "status": "skipped",
                        "reason": "Not enough candle data or model unavailable",
                        "strategy_name": strategy_name,
                        "symbol": symbol_name,
                        "timeframe": timeframe_name,
                    }
                )
                continue
            generated_signal_results.append(signal_result)
            signal_steps.append({"step": "generate_signal", **signal_result})

    if not generated_signal_results:
        return {
            "status": "partial_error" if had_error else "completed",
            "steps": signal_steps,
            "terminal_message": (
                "Pipeline run completed with strategy errors."
                if had_error
                else "Pipeline run completed with skipped signal generation."
            ),
        }

    signal_ids = [int(s["id"]) for s in generated_signal_results]
    return {
        "status": "partial_error" if had_error else "ok",
        "symbol_names": active_symbol_names,
        "timeframe_names": active_timeframe_names,
        "signal_ids": signal_ids,
        "steps": signal_steps,
    }

def run_strategy_jobs(
    connection: DBConnection,
    strategy_names: list[str],
    symbol_names: list[str] | None = None,
    timeframe_names: list[str] | None = None,
) -> dict[str, Any]:
    normalized_names = dedup_ordered(strategy_names or [DEFAULT_STRATEGY_NAME])
    results = []
    for name in normalized_names:
        try:
            results.append(
                run_strategy_job(
                    connection,
                    strategy_name=name,
                    symbol_names=symbol_names,
                    timeframe_names=timeframe_names,
                )
            )
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

    aggregated_signal_ids: list[int] = []
    aggregated_symbol_names: list[str] = []
    aggregated_timeframe_names: list[str] = []
    for result in results:
        aggregated_signal_ids.extend(int(item) for item in list(result.get("signal_ids") or []))
        aggregated_symbol_names.extend(str(item) for item in list(result.get("symbol_names") or []))
        aggregated_timeframe_names.extend(str(item) for item in list(result.get("timeframe_names") or []))

    return {
        "status": status,
        "strategy_names": normalized_names,
        "symbol_names": dedup_ordered((symbol_names or []) + aggregated_symbol_names),
        "timeframe_names": dedup_ordered((timeframe_names or []) + aggregated_timeframe_names),
        "signal_ids": aggregated_signal_ids,
        "steps": [step for result in results for step in result.get("steps", [])],
        "results": results,
    }
