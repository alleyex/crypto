"""Futures open interest collection job."""

from typing import Any

from app.core.db import DBConnection
from app.data.futures_open_interest_service import collect_futures_open_interest_metrics
from app.data.futures_open_interest_service import configured_futures_open_interest_symbols
from app.data.futures_open_interest_service import is_futures_open_interest_collection_enabled
from app.pipeline.futures_collector_base import run_futures_collector

def run_futures_open_interest_job(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> dict[str, Any]:
    if not is_futures_open_interest_collection_enabled():
        return {"step": "futures_open_interest", "status": "disabled", "saved": 0}

    symbols = list(symbol_names or configured_futures_open_interest_symbols())
    result = collect_futures_open_interest_metrics(connection, symbols)
    return run_futures_collector(
        connection,
        step="futures_open_interest",
        component="futures_open_interest_collector",
        message_label="Futures open interest metrics",
        result=result,
        symbols=symbols,
        include_symbols_in_payload=True,
    )
