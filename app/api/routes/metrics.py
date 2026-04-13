from typing import Any

from fastapi import APIRouter, Depends, Query

from app.core.db import DBConnection
from app.metrics.metrics_service import build_metrics
from app.api.deps import get_db

router = APIRouter()

@router.get("/metrics")
def metrics(
    period_hours: int = Query(default=24, ge=1, le=168),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    return build_metrics(connection, period_hours=period_hours)
