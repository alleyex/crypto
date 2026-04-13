from typing import Any, Literal

from fastapi import APIRouter, Depends

from app.audit.service import insert_event

from app.core.db import DBConnection
from app.core.job_queue import enqueue_pipeline_jobs
from app.core.pipeline_orchestration import enqueue_and_run_pipeline_batch
from app.core.pipeline_orchestration import run_pipeline_batch
from app.core.settings import DEFAULT_PIPELINE_ORCHESTRATION, DEFAULT_STRATEGY_NAME
from app.pipeline.run_pipeline import run_pipeline_collect
from pydantic import BaseModel
from app.api.deps import get_db

router = APIRouter()

class PipelineRunRequest(BaseModel):
    strategy_name: str = DEFAULT_STRATEGY_NAME
    symbol_names: list[str] | None = None
    orchestration: Literal["direct", "queue_dispatch", "queue_drain", "queue_batch"] | None = None
    batch_id: str | None = None

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

@router.post("/pipeline/run")
def run_pipeline_endpoint(
    payload: PipelineRunRequest | None = None,
    connection: DBConnection = Depends(get_db),
) -> dict:
    strategy_name = payload.strategy_name if payload is not None else DEFAULT_STRATEGY_NAME
    symbol_names = payload.symbol_names if payload is not None else None
    batch_id = payload.batch_id if payload is not None else None
    orchestration = (
        payload.orchestration
        if payload is not None and payload.orchestration is not None
        else DEFAULT_PIPELINE_ORCHESTRATION
    )
    if orchestration == "queue_dispatch":
        jobs = enqueue_pipeline_jobs(
            connection,
            strategy_name=strategy_name,
            symbol_names=symbol_names,
            payload={"source": "api_pipeline", "orchestration": orchestration},
        )
        return {
            "status": "queued",
            "orchestration": orchestration,
            "strategy_name": strategy_name,
            "requested_symbol_names": symbol_names,
            "batch_id": jobs[0]["batch_id"] if jobs else None,
            "jobs": jobs,
        }
    if orchestration == "queue_batch":
        result = enqueue_and_run_pipeline_batch(
            connection,
            strategy_name=strategy_name,
            symbol_names=symbol_names,
            payload={"source": "api_pipeline", "orchestration": orchestration},
        )
        return {
            **result,
            "orchestration": orchestration,
            "strategy_name": strategy_name,
            "requested_symbol_names": symbol_names,
        }
    if orchestration == "queue_drain":
        result = run_pipeline_batch(connection, batch_id=batch_id)
        _log_queue_control_event(
            connection,
            status=str(result.get("status") or "unknown"),
            message=(
                "Recovered queued pipeline batch."
                if batch_id
                else "Drained next queued pipeline batch."
            ),
            action="recover_pipeline_batch",
            payload={
                "orchestration": orchestration,
                "requested_batch_id": batch_id,
                "result_batch_id": result.get("batch_id"),
                "result_status": result.get("status"),
            },
        )
        return {
            **result,
            "orchestration": orchestration,
            "strategy_name": strategy_name,
            "requested_symbol_names": symbol_names,
            "requested_batch_id": batch_id,
        }
    return run_pipeline_collect(strategy_name=strategy_name, symbol_names=symbol_names)
