from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query

from app.api.errors import http_bad_request
from pydantic import BaseModel, Field

from app.core.db import DBConnection, parse_db_timestamp, utc_now

from app.core.settings import FUTURES_CANDLE_SYMBOLS, WORKER_HEARTBEAT_STALENESS_SECONDS
from app.system.heartbeat import get_heartbeat_row
from app.data.candles_service import get_candles_status
from app.data.futures_candles_service import (
    delete_candles as delete_futures_candles,
    get_status as get_futures_candles_status,
)
from app.pipeline.futures_market_data_job import run_futures_market_data_job
from app.pipeline.market_data_job import run_market_data_job
from app.query.read_service import get_candles, get_futures_candles
from app.api.deps import get_db

router = APIRouter()

class MarketDataFetchRequest(BaseModel):
    symbols: list[str] | None = None
    timeframes: list[str] | None = None  # e.g. ["1m", "5m", "1h"]
    limit: int = Field(default=100, ge=1, le=1000)
    start_date: str | None = None  # ISO date string e.g. "2024-01-01"

class FuturesCollectorConfigRequest(BaseModel):
    enabled_collectors: list[str] = Field(default_factory=list)

@router.get("/candles")
def candles(
    limit: int = Query(default=5, ge=1, le=100),
    symbol: str | None = Query(default=None),
    timeframe: list[str] | None = Query(default=None),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_candles(connection, limit=limit, symbol=symbol, timeframes=timeframe or None)

@router.get("/candles/futures")
def futures_candles(
    limit: int = Query(default=5, ge=1, le=100),
    symbol: str | None = Query(default=None),
    timeframe: list[str] | None = Query(default=None),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_futures_candles(connection, limit=limit, symbol=symbol, timeframes=timeframe or None)

@router.get("/candles/status")
def candles_status(connection: DBConnection = Depends(get_db)) -> list[dict]:
    """Return per-(symbol, timeframe) candle statistics: count, latest, staleness, gap estimate."""
    return get_candles_status(connection)

@router.get("/candles/futures/status")
def futures_candles_status(connection: DBConnection = Depends(get_db)) -> list[dict]:
    """Return per-(symbol, timeframe) futures candle statistics: count, latest, staleness, gap estimate."""
    return get_futures_candles_status(connection)

@router.get("/candles/futures/collector/status")
def futures_candles_collector_status(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    hb = get_heartbeat_row(connection, "futures_candles_collector")
    if hb is None:
        return {
            "enabled": True,
            "status": "unknown",
            "message": "No futures candles collector heartbeat recorded yet.",
            "last_seen_at": None,
            "age_seconds": None,
            "staleness_threshold_seconds": WORKER_HEARTBEAT_STALENESS_SECONDS,
            "stale": True,
            "payload": {},
        }
    age_seconds = int((utc_now() - parse_db_timestamp(hb["last_seen_at"])).total_seconds())
    return {
        "enabled": True,
        "status": hb["status"],
        "message": hb["message"],
        "last_seen_at": hb["last_seen_at"],
        "age_seconds": age_seconds,
        "staleness_threshold_seconds": WORKER_HEARTBEAT_STALENESS_SECONDS,
        "stale": age_seconds > WORKER_HEARTBEAT_STALENESS_SECONDS,
        "payload": hb["payload"],
    }

@router.post("/market-data/fetch")
def market_data_fetch(
    body: MarketDataFetchRequest = MarketDataFetchRequest(),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Trigger an independent market data fetch without running the full pipeline.

    Pass start_date (e.g. "2024-01-01") to paginate through all candles since that date.
    Otherwise fetches the latest `limit` candles (max 1000) per symbol.
    """
    start_ms: int | None = None
    if body.start_date:
        try:
            dt = datetime.strptime(body.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            start_ms = int(dt.timestamp() * 1000)
        except ValueError:
            http_bad_request("Invalid start_date format. Use YYYY-MM-DD.")

    result = run_market_data_job(
        connection,
        symbol_names=body.symbols,
        timeframes=body.timeframes,
        limit=body.limit,
        start_ms=start_ms,
    )
    return {"status": "ok", "start_date": body.start_date, **result}

@router.post("/market-data/futures/fetch")
def futures_market_data_fetch(
    body: MarketDataFetchRequest = MarketDataFetchRequest(),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Trigger an independent futures market data fetch without running the full pipeline."""
    start_ms: int | None = None
    if body.start_date:
        try:
            dt = datetime.strptime(body.start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            start_ms = int(dt.timestamp() * 1000)
        except ValueError:
            http_bad_request("Invalid start_date format. Use YYYY-MM-DD.")

    result = run_futures_market_data_job(
        connection,
        symbol_names=body.symbols,
        timeframes=body.timeframes,
        limit=body.limit,
        start_ms=start_ms,
    )
    return {"status": "ok", "start_date": body.start_date, **result}

@router.post("/market-data/futures/clear")
def futures_market_data_clear(
    body: MarketDataFetchRequest = MarketDataFetchRequest(),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    symbols = body.symbols or None
    timeframes = body.timeframes or None
    if not symbols and not timeframes:
        http_bad_request("Select at least one symbol or timeframe to clear.")

    deleted = delete_futures_candles(connection, symbols=symbols, timeframes=timeframes)
    return {
        "status": "ok",
        "deleted_rows": deleted,
        "symbol_names": symbols or [],
        "timeframes": timeframes or [],
        "market": "futures",
    }

@router.get("/market-data/futures/config")
def futures_market_data_config() -> dict[str, Any]:
    from app.data.candles_service import TIMEFRAME_INTERVAL_MS

    return {
        "symbol_names": list(FUTURES_CANDLE_SYMBOLS),
        "supported_timeframes": sorted(TIMEFRAME_INTERVAL_MS.keys()),
    }

@router.get("/collectors/futures/config")
def get_futures_collectors_config() -> dict[str, Any]:
    from app.data.futures_aggtrade_service import is_futures_aggtrade_collection_enabled
    from app.data.futures_liquidation_service import is_futures_liquidation_collection_enabled
    from app.data.futures_open_interest_service import is_futures_open_interest_collection_enabled
    from app.data.futures_orderbook_service import is_futures_orderbook_collection_enabled
    from app.data.futures_premium_service import is_futures_premium_collection_enabled

    collectors = [
        {
            "key": "futures_orderbook",
            "label": "Order Book",
            "enabled": is_futures_orderbook_collection_enabled(),
        },
        {
            "key": "futures_aggtrade",
            "label": "aggTrades",
            "enabled": is_futures_aggtrade_collection_enabled(),
        },
        {
            "key": "futures_premium",
            "label": "Premium",
            "enabled": is_futures_premium_collection_enabled(),
        },
        {
            "key": "futures_open_interest",
            "label": "Open Interest",
            "enabled": is_futures_open_interest_collection_enabled(),
        },
        {
            "key": "futures_liquidation",
            "label": "Liquidations",
            "enabled": is_futures_liquidation_collection_enabled(),
        },
    ]
    return {
        "collectors": collectors,
        "enabled_collectors": [item["key"] for item in collectors if item["enabled"]],
    }

@router.post("/collectors/futures/config")
def set_futures_collectors_config(body: FuturesCollectorConfigRequest) -> dict[str, Any]:
    from app.data.futures_aggtrade_service import disable_futures_aggtrade_collection
    from app.data.futures_aggtrade_service import enable_futures_aggtrade_collection
    from app.data.futures_liquidation_service import disable_futures_liquidation_collection
    from app.data.futures_liquidation_service import enable_futures_liquidation_collection
    from app.data.futures_open_interest_service import disable_futures_open_interest_collection
    from app.data.futures_open_interest_service import enable_futures_open_interest_collection
    from app.data.futures_orderbook_service import disable_futures_orderbook_collection
    from app.data.futures_orderbook_service import enable_futures_orderbook_collection
    from app.data.futures_premium_service import disable_futures_premium_collection
    from app.data.futures_premium_service import enable_futures_premium_collection

    handlers = {
        "futures_orderbook": (enable_futures_orderbook_collection, disable_futures_orderbook_collection, "Order Book"),
        "futures_aggtrade": (enable_futures_aggtrade_collection, disable_futures_aggtrade_collection, "aggTrades"),
        "futures_premium": (enable_futures_premium_collection, disable_futures_premium_collection, "Premium"),
        "futures_open_interest": (enable_futures_open_interest_collection, disable_futures_open_interest_collection, "Open Interest"),
        "futures_liquidation": (enable_futures_liquidation_collection, disable_futures_liquidation_collection, "Liquidations"),
    }
    requested = {str(key).strip() for key in body.enabled_collectors if str(key).strip()}
    unknown = sorted(requested - set(handlers))
    if unknown:
        http_bad_request(f"Unknown collector keys: {', '.join(unknown)}")

    changed: list[dict[str, Any]] = []
    for key, (enable_fn, disable_fn, label) in handlers.items():
        should_enable = key in requested
        if should_enable:
            enable_fn()
        else:
            disable_fn()
        changed.append({"key": key, "label": label, "enabled": should_enable})

    return {
        "status": "ok",
        "collectors": changed,
        "enabled_collectors": [item["key"] for item in changed if item["enabled"]],
    }
