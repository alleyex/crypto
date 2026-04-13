"""Futures aggTrade minute collection job."""

from typing import Any

from app.core.db import DBConnection
from app.data.futures_aggtrade_service import collect_futures_aggtrade_minutes
from app.data.futures_aggtrade_service import configured_futures_aggtrade_symbols
from app.data.futures_aggtrade_service import is_futures_aggtrade_collection_enabled
from app.pipeline.futures_collector_base import run_futures_collector

def run_futures_aggtrade_job(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> dict[str, Any]:
    if not is_futures_aggtrade_collection_enabled():
        return {"step": "futures_aggtrade", "status": "disabled", "saved": 0}

    symbols = list(symbol_names or configured_futures_aggtrade_symbols())
    result = collect_futures_aggtrade_minutes(connection, symbols)
    return run_futures_collector(
        connection,
        step="futures_aggtrade",
        component="futures_aggtrade_collector",
        message_label="Futures aggTrade minutes",
        result=result,
        symbols=symbols,
        include_collector_in_heartbeat=True,
    )
