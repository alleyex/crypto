"""Futures premium metrics collection job."""

from typing import Any

from app.core.db import DBConnection
from app.data.futures_premium_service import collect_futures_premium_metrics
from app.data.futures_premium_service import configured_futures_premium_symbols
from app.data.futures_premium_service import is_futures_premium_collection_enabled
from app.pipeline.futures_collector_base import run_futures_collector

def run_futures_premium_job(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> dict[str, Any]:
    if not is_futures_premium_collection_enabled():
        return {"step": "futures_premium", "status": "disabled", "saved": 0}

    symbols = list(symbol_names or configured_futures_premium_symbols())
    result = collect_futures_premium_metrics(connection, symbols)
    return run_futures_collector(
        connection,
        step="futures_premium",
        component="futures_premium_collector",
        message_label="Futures premium metrics",
        result=result,
        symbols=symbols,
    )
