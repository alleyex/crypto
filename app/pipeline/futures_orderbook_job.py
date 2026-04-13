"""Futures order book snapshot collection job."""

from typing import Any

from app.core.db import DBConnection
from app.data.futures_orderbook_service import collect_futures_orderbook_snapshots
from app.data.futures_orderbook_service import configured_futures_orderbook_symbols
from app.data.futures_orderbook_service import is_futures_orderbook_collection_enabled
from app.pipeline.futures_collector_base import run_futures_collector

def run_futures_orderbook_job(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> dict[str, Any]:
    if not is_futures_orderbook_collection_enabled():
        return {"step": "futures_orderbook", "status": "disabled", "saved": 0}

    symbols = list(symbol_names or configured_futures_orderbook_symbols())
    result = collect_futures_orderbook_snapshots(connection, symbols)
    return run_futures_collector(
        connection,
        step="futures_orderbook",
        component="futures_orderbook_collector",
        message_label="Futures order book snapshots",
        result=result,
        symbols=symbols,
        include_collector_in_heartbeat=True,
    )
