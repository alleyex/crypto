from typing import Any, Literal

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.audit.service import insert_event

from app.core.db import DBConnection
from app.core.job_queue import (
    JOB_TYPES,
    build_job_payload,
    enqueue_job,
    enqueue_pipeline_jobs,
    fail_batch_jobs,
    list_jobs as list_queue_jobs,
    retry_job,
)
from app.core.job_runner import run_next_queued_job
from app.core.pipeline_orchestration import run_next_pipeline_batch
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.query.read_service import get_job_queue_summary
from app.api.deps import get_db

router = APIRouter()

class QueueJobRequest(BaseModel):
    job_type: Literal["market_data", "strategy", "risk", "execution"]
    strategy_name: str | None = None
    strategy_names: list[str] | None = None
    symbol_names: list[str] | None = None
    payload: dict[str, Any] | None = None

class QueueRunRequest(BaseModel):
    job_type: Literal["market_data", "strategy", "risk", "execution"] | None = None

class QueuePipelineRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    strategy_names: list[str] | None = None
    symbol_names: list[str] | None = None
    payload: dict[str, Any] | None = None

def _build_queue_job_payload(payload: QueueJobRequest) -> dict[str, Any]:
    return build_job_payload(
        strategy_name=payload.strategy_name,
        strategy_names=payload.strategy_names,
        symbol_names=payload.symbol_names,
        payload=payload.payload,
    )

def _log_queue_control_event(
    connection,
    *,
    status: str,
    message: str,
    action: str,
    payload: dict[str, Any] | None = None,
) -> None:
    insert_event(
        connection,
        event_type="queue_control",
        status=status,
        source="queue_control",
        message=message,
        payload={"action": action, **(payload or {})},
    )

@router.get("/queue/jobs")
def queue_jobs(
    limit: int = Query(default=20, ge=1, le=200),
    status: str | None = Query(default=None),
    job_type: Literal["market_data", "strategy", "risk", "execution"] | None = Query(default=None),
    connection: DBConnection = Depends(get_db),
) -> list[dict[str, Any]]:
    return list_queue_jobs(connection, limit=limit, status=status, job_type=job_type)

@router.get("/queue/summary")
def queue_summary(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    return get_job_queue_summary(connection)

@router.post("/queue/jobs")
def create_queue_job(
    payload: QueueJobRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    job_payload = _build_queue_job_payload(payload)
    job_id = enqueue_job(connection, payload.job_type, payload=job_payload or None)
    return {
        "status": "queued",
        "job_id": job_id,
        "job_type": payload.job_type,
        "available_job_types": list(JOB_TYPES),
        "payload": job_payload,
        "job": list_queue_jobs(connection, limit=1, job_type=payload.job_type)[0],
    }

@router.post("/queue/jobs/enqueue-pipeline")
def create_pipeline_queue_jobs(
    payload: QueuePipelineRequest,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    jobs = enqueue_pipeline_jobs(
        connection,
        strategy_name=payload.strategy_name,
        strategy_names=payload.strategy_names,
        symbol_names=payload.symbol_names,
        payload=payload.payload,
    )
    return {
        "status": "queued",
        "job_count": len(jobs),
        "job_types": [job["job_type"] for job in jobs],
        "jobs": jobs,
    }

@router.post("/queue/jobs/run-next")
def run_next_queue_job(
    payload: QueueRunRequest | None = None,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    job_type = payload.job_type if payload is not None else None
    return run_next_queued_job(connection, job_type=job_type)

@router.post("/queue/jobs/run-next-pipeline")
def run_next_pipeline_queue_batch(connection: DBConnection = Depends(get_db)) -> dict[str, Any]:
    return run_next_pipeline_batch(connection)

@router.post("/queue/jobs/{job_id}/retry")
def retry_queue_job(
    job_id: int,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    job = retry_job(connection, job_id)
    return {
        "status": "queued",
        "job_id": job_id,
        "job": job,
    }

@router.post("/queue/batches/{batch_id}/clear")
def clear_queue_batch(
    batch_id: str,
    connection: DBConnection = Depends(get_db),
) -> dict[str, Any]:
    jobs = fail_batch_jobs(
        connection,
        batch_id,
        error_message="Queue batch cleared from admin.",
        result={"cleared_batch_id": batch_id, "source": "admin_queue_clear"},
    )
    status = "cleared" if jobs else "empty"
    _log_queue_control_event(
        connection,
        status=status,
        message="Cleared queued pipeline batch from admin." if jobs else "No queued jobs found for pipeline batch clear request.",
        action="clear_pipeline_batch",
        payload={
            "batch_id": batch_id,
            "cleared_job_count": len(jobs),
            "job_ids": [int(job["id"]) for job in jobs],
        },
    )
    return {
        "status": status,
        "batch_id": batch_id,
        "cleared_job_count": len(jobs),
        "jobs": jobs,
    }
