from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends

from app.api.deps import get_db
from app.api.errors import http_bad_request
from app.core.db import DBConnection
from app.system.heartbeat import get_heartbeat_payload

router = APIRouter()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _collector_status(
    connection: DBConnection,
    component: str,
    is_enabled_fn,
    configured_symbols_fn,
    get_stats_fn,
    total_key: str = "total_minutes",
    stats_kwargs: dict | None = None,
) -> dict[str, Any]:
    """Build a standard collector-status response dict."""
    payload, last_heartbeat_at = get_heartbeat_payload(connection, component)
    stats = get_stats_fn(connection, **(stats_kwargs or {}))
    return {
        "enabled": is_enabled_fn(),
        "configured_symbols": configured_symbols_fn(),
        "symbols": stats,
        total_key: sum(s["total"] for s in stats),
        "collector": payload,
        "last_heartbeat_at": last_heartbeat_at,
    }


# ---------------------------------------------------------------------------
# Order book
# ---------------------------------------------------------------------------

@router.get("/orderbook/futures/status")
def get_futures_orderbook_status(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    """Return futures order book collection status and per-symbol stats."""
    from app.data.futures_orderbook_service import (
        configured_futures_orderbook_symbols,
        get_futures_orderbook_stats,
        is_futures_orderbook_collection_enabled,
    )
    payload, last_heartbeat_at = get_heartbeat_payload(connection, "futures_orderbook_collector")
    collector_runtime = dict(payload.get("collector") or {})
    stats = get_futures_orderbook_stats(
        connection,
        runtime=(collector_runtime.get("symbol_runtime") or {}),
    )
    cutoff_24h = datetime.now(timezone.utc) - timedelta(days=1)
    watchdog_row = connection.execute(
        """
        SELECT
            COUNT(*) AS restart_count_24h,
            MAX(created_at) AS last_restart_at
        FROM audit_events
        WHERE event_type = 'futures_orderbook_watchdog_restart'
          AND created_at >= ?
        """,
        (cutoff_24h,),
    ).fetchone()
    watchdog_restart_count_24h = int(watchdog_row[0] or 0) if watchdog_row is not None else 0
    last_watchdog_restart_at = watchdog_row[1] if watchdog_row is not None else None
    return {
        "enabled": is_futures_orderbook_collection_enabled(),
        "configured_symbols": configured_futures_orderbook_symbols(),
        "symbols": stats,
        "total_snapshots": sum(s["total"] for s in stats),
        "collector": collector_runtime,
        "last_heartbeat_at": last_heartbeat_at,
        "watchdog_restart_count_24h": watchdog_restart_count_24h,
        "last_watchdog_restart_at": last_watchdog_restart_at,
    }


@router.get("/orderbook/futures/recent")
def get_recent_futures_orderbook(
    symbol: str,
    limit: int = 5,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return recent futures order book snapshots for a symbol."""
    from app.data.futures_orderbook_service import get_recent_futures_orderbook_snapshots

    requested_symbol = str(symbol or "").strip().upper()
    if not requested_symbol:
        http_bad_request("symbol is required")

    rows = get_recent_futures_orderbook_snapshots(connection, requested_symbol, limit=max(1, min(int(limit), 20)))
    return {
        "symbol": requested_symbol,
        "rows": rows,
    }


@router.post("/orderbook/futures/enable")
def enable_futures_orderbook() -> dict[str, Any]:
    """Enable futures order book collection."""
    from app.data.futures_orderbook_service import enable_futures_orderbook_collection
    enable_futures_orderbook_collection()
    return {"enabled": True}


@router.post("/orderbook/futures/pause")
def pause_futures_orderbook() -> dict[str, Any]:
    """Pause futures order book collection."""
    from app.data.futures_orderbook_service import disable_futures_orderbook_collection
    disable_futures_orderbook_collection()
    return {"enabled": False}


# ---------------------------------------------------------------------------
# aggTrades
# ---------------------------------------------------------------------------

@router.get("/aggtrades/futures/status")
def get_futures_aggtrade_status(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    """Return futures aggTrade collection status and per-symbol stats."""
    from app.data.futures_aggtrade_service import (
        configured_futures_aggtrade_symbols,
        get_futures_aggtrade_stats,
        is_futures_aggtrade_collection_enabled,
    )
    payload, last_heartbeat_at = get_heartbeat_payload(connection, "futures_aggtrade_collector")
    collector_runtime = dict(payload.get("collector") or {})
    stats = get_futures_aggtrade_stats(
        connection,
        runtime=(collector_runtime.get("symbol_runtime") or {}),
    )
    return {
        "enabled": is_futures_aggtrade_collection_enabled(),
        "configured_symbols": configured_futures_aggtrade_symbols(),
        "symbols": stats,
        "total_minutes": sum(s["total"] for s in stats),
        "collector": collector_runtime,
        "last_heartbeat_at": last_heartbeat_at,
    }


@router.get("/aggtrades/futures/recent")
def get_recent_futures_aggtrades(
    symbol: str,
    limit: int = 5,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Return recent futures aggTrade minute aggregates for a symbol."""
    from app.data.futures_aggtrade_service import get_recent_futures_aggtrade_minutes

    requested_symbol = str(symbol or "").strip().upper()
    if not requested_symbol:
        http_bad_request("symbol is required")

    rows = get_recent_futures_aggtrade_minutes(connection, requested_symbol, limit=max(1, min(int(limit), 20)))
    return {
        "symbol": requested_symbol,
        "rows": rows,
    }


# ---------------------------------------------------------------------------
# Premium metrics
# ---------------------------------------------------------------------------

@router.get("/premium/futures/status")
def get_futures_premium_status(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    """Return futures premium metrics collection status and per-symbol stats."""
    from app.data.futures_premium_service import (
        configured_futures_premium_symbols,
        get_futures_premium_stats,
        is_futures_premium_collection_enabled,
    )
    return _collector_status(
        connection,
        component="futures_premium_collector",
        is_enabled_fn=is_futures_premium_collection_enabled,
        configured_symbols_fn=configured_futures_premium_symbols,
        get_stats_fn=get_futures_premium_stats,
    )


# ---------------------------------------------------------------------------
# Open interest
# ---------------------------------------------------------------------------

@router.get("/open-interest/futures/status")
def get_futures_open_interest_status(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    """Return futures open interest collection status and per-symbol stats."""
    from app.data.futures_open_interest_service import (
        configured_futures_open_interest_symbols,
        get_futures_open_interest_stats,
        is_futures_open_interest_collection_enabled,
    )
    return _collector_status(
        connection,
        component="futures_open_interest_collector",
        is_enabled_fn=is_futures_open_interest_collection_enabled,
        configured_symbols_fn=configured_futures_open_interest_symbols,
        get_stats_fn=get_futures_open_interest_stats,
    )


# ---------------------------------------------------------------------------
# Liquidations
# ---------------------------------------------------------------------------

@router.get("/liquidations/futures/status")
def get_futures_liquidation_status(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    """Return futures liquidation collection status and per-symbol stats."""
    from app.data.futures_liquidation_service import (
        configured_futures_liquidation_symbols,
        get_futures_liquidation_stats,
        is_futures_liquidation_collection_enabled,
    )
    payload, last_heartbeat_at = get_heartbeat_payload(connection, "futures_liquidation_collector")
    symbol_runtime = (payload.get("collector") or {}).get("symbol_runtime") or {}
    stats = get_futures_liquidation_stats(connection, runtime=symbol_runtime)
    return {
        "enabled": is_futures_liquidation_collection_enabled(),
        "configured_symbols": configured_futures_liquidation_symbols(),
        "symbols": stats,
        "total_minutes": sum(s["total"] for s in stats),
        "collector": payload,
        "last_heartbeat_at": last_heartbeat_at,
    }
