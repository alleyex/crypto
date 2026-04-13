"""Shared logic for futures collector pipeline jobs.

All five futures collector jobs (aggtrade, orderbook, premium,
open_interest, liquidation) follow the same pattern:

  1. Guard: return early if collection is disabled.
  2. Resolve symbol list (override or configured default).
  3. Call the domain-specific collect function.
  4. Extract saved / errors / source_counts from the result.
  5. Derive job_status and upsert a heartbeat.
  6. Return a typed step payload.

This helper handles steps 4–6 so each job file only needs to
implement steps 1–3 with its own service functions.
"""

from typing import Any

from app.core.db import DBConnection
from app.system.heartbeat import upsert_heartbeat

def run_futures_collector(
    connection: DBConnection,
    *,
    step: str,
    component: str,
    message_label: str,
    result: dict[str, Any],
    symbols: list[str],
    include_collector_in_heartbeat: bool = False,
    include_symbols_in_payload: bool = False,
) -> dict[str, Any]:
    """Build heartbeat and step payload from a collector result dict.

    Args:
        connection: Active DB connection for heartbeat upsert.
        step: Step label used in the returned payload (e.g. "futures_aggtrade").
        component: Heartbeat component name (e.g. "futures_aggtrade_collector").
        message_label: Human-readable label for the heartbeat message.
        result: Raw dict returned by the domain collect function.
        symbols: Symbol list that was collected (used for counts and heartbeat).
        include_collector_in_heartbeat: Include ``result["collector"]`` in heartbeat.
        include_symbols_in_payload: Include symbol list in the returned payload.
    """
    saved = int(result.get("saved", 0))
    errors = list(result.get("errors", []))
    source_counts = dict(result.get("source_counts", {}))

    job_status = "ok" if not errors else ("partial" if saved > 0 else "error")

    heartbeat_payload: dict[str, Any] = {
        "saved": saved,
        "errors": errors,
        "source_counts": source_counts,
        "symbols": symbols,
    }
    if include_collector_in_heartbeat:
        heartbeat_payload["collector"] = result.get("collector") or {}

    upsert_heartbeat(
        connection,
        component=component,
        status=job_status,
        message=f"{message_label} saved: {saved}/{len(symbols)}.",
        payload=heartbeat_payload,
    )

    payload: dict[str, Any] = {
        "step": step,
        "status": job_status,
        "saved": saved,
        "source_counts": source_counts,
    }
    if include_symbols_in_payload:
        payload["symbols"] = symbols
    if errors:
        payload["errors"] = errors
    return payload
