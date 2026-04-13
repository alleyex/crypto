from typing import Any

from fastapi import APIRouter, Depends, Query

from app.api.errors import http_bad_request, http_bad_gateway

from app.core.db import DBConnection
from app.execution.exchange_trades import (
    enrich_report_with_exchange_snapshot,
    get_exchange_trades_for_window,
    resolve_report_window,
)
from app.query.read_service import get_execution_report
from app.api.deps import get_db

router = APIRouter()

@router.get("/reports/testnet-execution")
def testnet_execution_report(
    symbol: str = Query(default="BTCUSDT"),
    strategy_name: str | None = Query(default=None),
    days: int = Query(default=7, ge=1, le=30),
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(default=10, ge=1, le=50),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    try:
        resolved_days, _, _, window_meta = resolve_report_window(
            days=days,
            start_date=start_date,
            end_date=end_date,
        )
    except ValueError as exc:
        http_bad_request(str(exc))
    report = get_execution_report(
        connection,
        symbol=symbol,
        strategy_name=strategy_name,
        days=resolved_days,
        limit=limit,
    )
    report.setdefault("summary", {}).update(window_meta)
    report["summary"]["days"] = resolved_days
    enrich_report_with_exchange_snapshot(
        report, symbol, strategy_name, resolved_days, window_meta,
        start_date, end_date, limit,
    )
    return report

@router.get("/reports/exchange-trades")
def exchange_trades_report(
    symbol: str = Query(default="BTCUSDT"),
    days: int = Query(default=7, ge=1, le=30),
    start_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    end_date: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    try:
        exchange = get_exchange_trades_for_window(
            symbol,
            days=days,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except ValueError as exc:
        http_bad_request(str(exc))
    except Exception as exc:
        http_bad_gateway(f"Unable to fetch Binance trade history: {exc}")
    return exchange
