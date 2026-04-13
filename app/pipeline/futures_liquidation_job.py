"""Futures liquidation collection job."""

from typing import Any

from app.core.db import DBConnection
from app.data.futures_liquidation_service import collect_futures_liquidation_minutes
from app.data.futures_liquidation_service import configured_futures_liquidation_symbols
from app.data.futures_liquidation_service import is_futures_liquidation_collection_enabled
from app.pipeline.futures_collector_base import run_futures_collector

def run_futures_liquidation_job(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> dict[str, Any]:
    if not is_futures_liquidation_collection_enabled():
        return {"step": "futures_liquidation", "status": "disabled", "saved": 0}

    symbols = list(symbol_names or configured_futures_liquidation_symbols())
    result = collect_futures_liquidation_minutes(connection, symbols)
    return run_futures_collector(
        connection,
        step="futures_liquidation",
        component="futures_liquidation_collector",
        message_label="Futures liquidation minutes",
        result=result,
        symbols=symbols,
        include_collector_in_heartbeat=True,
    )
