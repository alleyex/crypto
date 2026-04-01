"""Order book snapshot collection job.

Runs once per scheduler loop (every ~1 minute).
Checks runtime/orderbook.enabled before doing anything.
"""

from typing import Any, Dict, List, Optional

from app.core.db import DBConnection
from app.data.orderbook_service import (
    fetch_orderbook_snapshot,
    is_orderbook_collection_enabled,
    save_orderbook_snapshot,
)
from app.system.heartbeat import upsert_heartbeat


def run_orderbook_job(
    connection: DBConnection,
    symbol_names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Fetch and store one order book snapshot per symbol.

    Returns a result dict with step='orderbook'.
    """
    if not is_orderbook_collection_enabled():
        return {"step": "orderbook", "status": "disabled", "saved": 0}

    if symbol_names is None:
        from app.scheduler.control import read_active_symbols
        symbol_names = read_active_symbols()

    saved = 0
    errors = []

    for symbol in symbol_names:
        try:
            snapshot = fetch_orderbook_snapshot(symbol)
            save_orderbook_snapshot(connection, snapshot)
            saved += 1
        except Exception as exc:
            errors.append({"symbol": symbol, "error": str(exc)})

    job_status = "ok" if not errors else ("partial" if saved > 0 else "error")
    upsert_heartbeat(
        connection,
        component="orderbook_collector",
        status=job_status,
        message=f"Order book snapshots saved: {saved}/{len(symbol_names)}.",
        payload={"saved": saved, "errors": errors},
    )

    result: Dict[str, Any] = {
        "step":   "orderbook",
        "status": job_status,
        "saved":  saved,
    }
    if errors:
        result["errors"] = errors
    return result
