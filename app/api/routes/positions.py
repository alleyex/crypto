from fastapi import APIRouter, Depends, Query

from app.core.db import DBConnection
from app.execution.exchange_trades import binance_futures_enabled, get_binance_futures_positions
from app.query.read_service import get_positions
from app.api.deps import get_db

router = APIRouter()

@router.get("/positions")
def positions(
    limit: int = Query(default=5, ge=1, le=100),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    if binance_futures_enabled():
        try:
            return get_binance_futures_positions(include_flat=False)[:limit]
        except Exception:
            return get_positions(connection, limit=limit)
    return get_positions(connection, limit=limit)
