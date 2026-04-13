from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field

from app.core.db import DBConnection

from app.query.read_service import get_audit_events
from app.system.retention import run_retention
from app.api.deps import get_db

router = APIRouter()

class RetentionRequest(BaseModel):
    audit_days: int = Field(default=90, ge=1, description="Delete audit_events older than N days")
    job_queue_days: int = Field(default=30, ge=1, description="Delete done/failed job_queue rows older than N days")

@router.get("/audit-events")
def audit_events(
    limit: int = Query(default=20, ge=1, le=200),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_audit_events(connection, limit=limit)

@router.post("/maintenance/retention")
def maintenance_retention(
    body: RetentionRequest = RetentionRequest(),
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    """Purge old audit_events and completed job_queue rows to keep the database lean."""
    result = run_retention(
        connection,
        audit_days=body.audit_days,
        job_queue_days=body.job_queue_days,
    )
    return {"status": "ok", **result}
