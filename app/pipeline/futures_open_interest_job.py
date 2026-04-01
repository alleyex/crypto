"""Futures open interest collection job."""

from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from app.core.db import DBConnection
from app.data.futures_open_interest_service import collect_futures_open_interest_metrics
from app.data.futures_open_interest_service import configured_futures_open_interest_symbols
from app.data.futures_open_interest_service import is_futures_open_interest_collection_enabled
from app.system.heartbeat import upsert_heartbeat


def run_futures_open_interest_job(
    connection: DBConnection,
    symbol_names: Optional[List[str]] = None,
) -> Dict[str, Any]:
    if not is_futures_open_interest_collection_enabled():
        return {"step": "futures_open_interest", "status": "disabled", "saved": 0}

    symbols = list(symbol_names or configured_futures_open_interest_symbols())
    result = collect_futures_open_interest_metrics(connection, symbols)
    saved = int(result.get("saved", 0))
    errors = list(result.get("errors", []))
    source_counts = dict(result.get("source_counts", {}))

    job_status = "ok" if not errors else ("partial" if saved > 0 else "error")
    upsert_heartbeat(
        connection,
        component="futures_open_interest_collector",
        status=job_status,
        message=f"Futures open interest metrics saved: {saved}/{len(symbols)}.",
        payload={
            "saved": saved,
            "errors": errors,
            "source_counts": source_counts,
            "symbols": symbols,
        },
    )

    payload: Dict[str, Any] = {
        "step": "futures_open_interest",
        "status": job_status,
        "saved": saved,
        "source_counts": source_counts,
        "symbols": symbols,
    }
    if errors:
        payload["errors"] = errors
    return payload
