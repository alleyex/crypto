"""Job execution layer.

Separated from job_queue.py (queue data layer) so that the two
responsibilities — managing queue state vs. dispatching work — live in
distinct modules.
"""
import json
from typing import Any

from app.core.db import DBConnection
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.execution.adapter import get_execution_backend_status
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.risk_job import run_risk_job
from app.pipeline.strategy_job import run_strategy_job, run_strategy_jobs
from app.training.ppo_queue_job import run_ppo_training_job

def run_job(
    connection: DBConnection,
    job_type: str,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_payload = dict(payload or {})
    if job_type == "market_data":
        return run_market_data_job(
            connection,
            symbol_names=normalized_payload.get("symbol_names"),
        )

    if job_type == "strategy":
        strategy_names = normalized_payload.get("strategy_names") or []
        strategy_name = normalized_payload.get("strategy_name")
        symbol_names = normalized_payload.get("symbol_names")
        if strategy_names:
            return run_strategy_jobs(
                connection,
                strategy_names=[str(name) for name in strategy_names],
                symbol_names=symbol_names,
            )
        return run_strategy_job(
            connection,
            strategy_name=str(strategy_name or DEFAULT_STRATEGY_NAME),
            symbol_names=symbol_names,
        )

    if job_type == "risk":
        signal_ids = normalized_payload.get("signal_ids")
        normalized_signal_ids = [int(i) for i in signal_ids] if signal_ids is not None else None
        return run_risk_job(connection, signal_ids=normalized_signal_ids)

    if job_type == "execution":
        risk_event_ids = normalized_payload.get("risk_event_ids")
        symbol_names = normalized_payload.get("symbol_names")
        normalized_risk_event_ids = [int(item) for item in risk_event_ids] if risk_event_ids is not None else None
        return run_execution_job(
            connection,
            risk_event_ids=normalized_risk_event_ids,
            symbol_names=symbol_names,
        )

    if job_type == "training_ppo":
        training_job_id = normalized_payload.get("training_job_id")
        if training_job_id is None:
            raise ValueError("training_ppo job requires training_job_id.")
        return run_ppo_training_job(
            int(training_job_id),
            queue_job_id=int(normalized_payload["queue_job_id"]) if normalized_payload.get("queue_job_id") is not None else None,
        )

    raise ValueError(f"Unsupported job type: {job_type}")

def _propagate_dependent_job_payload(
    connection: DBConnection,
    job_id: int,
    *,
    result: dict[str, Any] | None = None,
) -> None:
    from app.core.db import fetch_all_as_dicts
    from app.core.job_queue import _serialize_payload, get_job

    parent_job = get_job(connection, job_id)
    if parent_job is None:
        return

    parent_type = str(parent_job["job_type"])
    if parent_type == "strategy":
        propagated_fields = {"signal_ids": list((result or {}).get("signal_ids") or [])}
        target_job_type = "risk"
    elif parent_type == "risk":
        propagated_fields = {"risk_event_ids": list((result or {}).get("risk_event_ids") or [])}
        target_job_type = "execution"
    else:
        return

    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT id, payload_json
        FROM job_queue
        WHERE depends_on_job_id = ? AND status = 'queued' AND job_type = ?
        ORDER BY id ASC;
        """,
        (job_id, target_job_type),
    )
    if not rows:
        return

    params = []
    for row in rows:
        raw = row.get("payload_json")
        current_payload = json.loads(raw) if raw else {}
        current_payload.update(propagated_fields)
        params.append((_serialize_payload(current_payload), int(row["id"])))

    connection.executemany(
        "UPDATE job_queue SET payload_json = ? WHERE id = ?;",
        params,
    )
    connection.commit()

def _run_leased_job(connection: DBConnection, job: dict[str, Any]) -> dict[str, Any]:
    payload = dict(job.get("payload") or {})
    payload.setdefault("queue_job_id", int(job["id"]))
    return run_job(
        connection,
        job_type=str(job["job_type"]),
        payload=payload,
    )

def _run_leased_queue_job(connection: DBConnection, leased_job: dict[str, Any]) -> dict[str, Any]:
    from app.core.job_queue import complete_job, fail_job, get_job

    job_id = int(leased_job["id"])
    backend_status = get_execution_backend_status()
    try:
        result = _run_leased_job(connection, leased_job)
        result_with_backend = {**result, "execution_backend_status": backend_status}
        complete_job(connection, job_id, result=result_with_backend)
        completed_job = get_job(connection, job_id)
        return {
            "status": "completed",
            "job": completed_job,
            "result": result_with_backend,
            "execution_backend_status": backend_status,
        }
    except Exception as exc:
        if exc.__class__.__name__ in {"CancelledTrainingJob", "MissingTrainingJob"}:
            payload = (
                exc.to_payload()
                if hasattr(exc, "to_payload") and callable(getattr(exc, "to_payload"))
                else {
                    "status": "cancelled",
                    "training_job_id": ((leased_job.get("payload") or {}).get("training_job_id")),
                }
            )
            result = {
                **payload,
                "execution_backend_status": backend_status,
            }
            complete_job(connection, job_id, result=result)
            completed_job = get_job(connection, job_id)
            return {
                "status": "completed",
                "job": completed_job,
                "result": result,
                "execution_backend_status": backend_status,
            }
        error_detail: dict[str, Any] = {
            "error_type": exc.__class__.__name__,
            "execution_backend_status": backend_status,
        }
        if hasattr(exc, "to_payload") and callable(getattr(exc, "to_payload")):
            extra = exc.to_payload()
            if isinstance(extra, dict):
                error_detail["error_detail"] = extra
        fail_job(connection, job_id, str(exc), result=error_detail)
        failed_job = get_job(connection, job_id)
        return {
            "status": "failed",
            "job": failed_job,
            "error": str(exc),
            "error_type": exc.__class__.__name__,
            "execution_backend_status": backend_status,
        }

def run_next_queued_job(connection: DBConnection, job_type: str | None = None) -> dict[str, Any]:
    from app.core.job_queue import lease_next_job

    leased_job = lease_next_job(connection, job_type=job_type)
    if leased_job is None:
        return {
            "status": "empty",
            "job_type": job_type,
            "message": "No queued jobs available.",
        }
    return _run_leased_queue_job(connection, leased_job)
