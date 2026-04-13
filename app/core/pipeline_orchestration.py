"""Pipeline batch orchestration: enqueue + drain a full market_data→strategy→risk→execution run."""
from typing import Any

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.job_runner import _run_leased_queue_job, run_next_queued_job
from app.core.settings import DEFAULT_STRATEGY_NAME

def run_next_pipeline_batch(connection: DBConnection, batch_id: str | None = None) -> dict[str, Any]:
    from app.core.job_queue import ensure_table, get_job, lease_job_by_id, _normalize_rows

    ensure_table(connection)
    if batch_id is None:
        row = connection.execute(
            """
            SELECT batch_id FROM job_queue
            WHERE status = 'queued' AND batch_id IS NOT NULL
            ORDER BY created_at ASC, id ASC
            LIMIT 1;
            """
        ).fetchone()
        resolved_batch_id = str(row[0]) if row else None
    else:
        resolved_batch_id = batch_id

    if not resolved_batch_id:
        return {
            "status": "empty",
            "message": "No queued pipeline batches available." if batch_id is None else f"No queued jobs available for pipeline batch {batch_id}.",
        }

    batch_jobs_rows = fetch_all_as_dicts(
        connection,
        """
        SELECT id, job_type, status, payload_json, result_json, error_message,
               attempt_count, depends_on_job_id, created_at, started_at, completed_at
        FROM job_queue
        WHERE batch_id = ? AND status = 'queued'
        ORDER BY id ASC;
        """,
        (resolved_batch_id,),
    )
    batch_jobs = _normalize_rows(batch_jobs_rows)
    if not batch_jobs:
        return {
            "status": "empty",
            "message": f"No queued jobs available for pipeline batch {resolved_batch_id}.",
        }

    next_job = batch_jobs[0]
    if batch_id is None:
        run_result = run_next_queued_job(connection, job_type=str(next_job["job_type"]))
    else:
        leased_job = lease_job_by_id(connection, int(next_job["id"]))
        if leased_job is None:
            return {
                "status": "empty",
                "batch_id": resolved_batch_id,
                "message": f"Unable to lease queued job {next_job['id']} for pipeline batch {resolved_batch_id}.",
            }
        run_result = _run_leased_queue_job(connection, leased_job)
    run_result["batch_id"] = resolved_batch_id
    run_result["remaining_job_types"] = [str(job["job_type"]) for job in batch_jobs[1:]]
    return run_result

def run_pipeline_batch(connection: DBConnection, batch_id: str | None = None) -> dict[str, Any]:
    from app.core.job_queue import ensure_table, get_job
    from app.pipeline.runtime_summary import record_pipeline_runtime

    ensure_table(connection)
    requested_batch_id = batch_id
    current_result = run_next_pipeline_batch(connection) if requested_batch_id is None else run_next_pipeline_batch(connection, batch_id=requested_batch_id)
    if current_result["status"] != "completed":
        return current_result

    batch_id = current_result.get("batch_id")
    jobs: list[dict[str, Any]] = [dict(current_result["job"])]
    steps: list[dict[str, Any]] = list((current_result.get("result") or {}).get("steps") or [])
    execution_backend_status = current_result.get("execution_backend_status")
    initial_job_record = get_job(connection, int(current_result["job"]["id"])) if current_result.get("job") else None
    initial_job_payload = dict((initial_job_record or {}).get("payload") or (current_result.get("job") or {}).get("payload") or {})
    pipeline_context: dict[str, Any] = {
        "strategy_name": str(
            initial_job_payload.get("strategy_name")
            or ((initial_job_payload.get("strategy_names") or [DEFAULT_STRATEGY_NAME])[0])
        ),
        "strategy_names": list(dict.fromkeys(initial_job_payload.get("strategy_names") or [])),
        "requested_symbol_names": list(dict.fromkeys(initial_job_payload.get("symbol_names") or [])),
    }
    if not pipeline_context["strategy_names"] and pipeline_context["strategy_name"]:
        pipeline_context["strategy_names"] = [pipeline_context["strategy_name"]]

    while current_result.get("remaining_job_types"):
        next_result = run_next_pipeline_batch(connection) if requested_batch_id is None else run_next_pipeline_batch(connection, batch_id=requested_batch_id)
        if next_result["status"] != "completed":
            next_result["batch_id"] = batch_id
            next_result["completed_jobs"] = jobs
            next_result["steps"] = steps
            record_pipeline_runtime(
                {
                    **pipeline_context,
                    "steps": steps,
                    "execution_backend_status": execution_backend_status,
                },
                status="failed",
                message=f"Pipeline batch failed during {str(next_result.get('job', {}).get('job_type') or 'unknown')} job.",
                source="pipeline",
            )
            return next_result
        if next_result.get("batch_id") != batch_id:
            raise RuntimeError(
                f"Pipeline batch changed while draining queued jobs: expected {batch_id}, got {next_result.get('batch_id')}"
            )
        jobs.append(dict(next_result["job"]))
        steps.extend(list((next_result.get("result") or {}).get("steps") or []))
        execution_backend_status = next_result.get("execution_backend_status", execution_backend_status)
        current_result = next_result

    result = {
        "status": "completed",
        "batch_id": batch_id,
        "jobs": jobs,
        "job": jobs[-1],
        "result": {
            "status": "ok",
            "steps": steps,
            "execution_backend_status": execution_backend_status,
        },
        "execution_backend_status": execution_backend_status,
        "remaining_job_types": [],
    }
    record_pipeline_runtime(
        {
            **pipeline_context,
            **dict(result["result"]),
        },
        status="completed",
        message="Pipeline run completed.",
        source="pipeline",
    )
    return result

def enqueue_and_run_pipeline_batch(
    connection: DBConnection,
    *,
    strategy_name: str | None = None,
    strategy_names: list[str] | None = None,
    symbol_names: list[str] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from app.core.job_queue import enqueue_pipeline_jobs, lease_job_by_id
    from app.pipeline.runtime_summary import record_pipeline_runtime

    jobs = enqueue_pipeline_jobs(
        connection,
        strategy_name=strategy_name,
        strategy_names=strategy_names,
        symbol_names=symbol_names,
        payload=payload,
    )
    pipeline_context: dict[str, Any] = {
        "strategy_name": strategy_name or (strategy_names[0] if strategy_names else DEFAULT_STRATEGY_NAME),
        "strategy_names": list(dict.fromkeys(strategy_names or ([strategy_name] if strategy_name else []))),
        "requested_symbol_names": list(dict.fromkeys(symbol_names or [])),
    }
    executed_jobs: list[dict[str, Any]] = []
    steps: list[dict[str, Any]] = []
    execution_backend_status = None
    for item in jobs:
        leased_job = lease_job_by_id(connection, int(item["job_id"]))
        if leased_job is None:
            raise RuntimeError(f"Unable to lease queued pipeline job {item['job_id']}.")
        job_result = _run_leased_queue_job(connection, leased_job)
        if job_result["status"] != "completed":
            job_result["batch_id"] = item["batch_id"]
            job_result["jobs"] = executed_jobs
            job_result["steps"] = steps
            job_result["enqueued_jobs"] = jobs
            record_pipeline_runtime(
                {
                    **pipeline_context,
                    "steps": steps,
                    "execution_backend_status": execution_backend_status,
                },
                status="failed",
                message=f"Pipeline batch failed during {item['job_type']} job.",
                source="pipeline",
            )
            return job_result
        executed_jobs.append(dict(job_result["job"]))
        steps.extend(list((job_result.get("result") or {}).get("steps") or []))
        execution_backend_status = job_result.get("execution_backend_status", execution_backend_status)
    result = {
        "status": "completed",
        "batch_id": jobs[0]["batch_id"] if jobs else None,
        "jobs": executed_jobs,
        "job": executed_jobs[-1] if executed_jobs else None,
        "result": {
            "status": "ok",
            "steps": steps,
            "execution_backend_status": execution_backend_status,
        },
        "execution_backend_status": execution_backend_status,
        "remaining_job_types": [],
        "enqueued_jobs": jobs,
    }
    record_pipeline_runtime(
        {
            **pipeline_context,
            **dict(result["result"]),
        },
        status="completed",
        message="Pipeline run completed.",
        source="pipeline",
    )
    return result
