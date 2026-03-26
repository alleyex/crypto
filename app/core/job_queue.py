import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from typing import Optional

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import insert_and_get_rowid
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.execution.adapter import get_execution_backend_status
from app.execution.adapter import get_execution_adapter_name
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.risk_job import run_risk_job
from app.pipeline.runtime_summary import record_pipeline_runtime
from app.pipeline.strategy_job import run_strategy_job
from app.pipeline.strategy_job import run_strategy_jobs


JOB_TYPES = ("market_data", "strategy", "risk", "execution")
JOB_STATUSES = ("queued", "leased", "completed", "failed")
PIPELINE_QUEUE_JOB_TYPES = ("market_data", "strategy", "risk", "execution")


INSERT_JOB_SQL = """
INSERT INTO job_queue (
    job_type,
    status,
    payload_json,
    result_json,
    error_message,
    depends_on_job_id
) VALUES (?, 'queued', ?, NULL, NULL, ?);
"""


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def _serialize_payload(payload: Optional[dict[str, Any]]) -> Optional[str]:
    if payload is None:
        return None
    return json.dumps(payload, ensure_ascii=True, sort_keys=True)


def _normalize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for field_name in ("payload_json", "result_json"):
            raw_value = item.get(field_name)
            parsed_name = field_name.replace("_json", "")
            item[parsed_name] = json.loads(raw_value) if raw_value else None
        normalized.append(item)
    return normalized


def build_job_payload(
    *,
    strategy_name: Optional[str] = None,
    strategy_names: Optional[list[str]] = None,
    symbol_names: Optional[list[str]] = None,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    job_payload: dict[str, Any] = dict(payload or {})
    backend_status = get_execution_backend_status()
    job_payload.setdefault("execution_backend", get_execution_adapter_name())
    job_payload.setdefault("execution_backend_status", backend_status)
    if strategy_name:
        job_payload["strategy_name"] = strategy_name
    if strategy_names:
        job_payload["strategy_names"] = list(dict.fromkeys(strategy_names))
    if symbol_names:
        job_payload["symbol_names"] = list(dict.fromkeys(symbol_names))
    return job_payload


def enqueue_job(
    connection: DBConnection,
    job_type: str,
    payload: Optional[dict[str, Any]] = None,
    depends_on_job_id: Optional[int] = None,
) -> int:
    if job_type not in JOB_TYPES:
        raise ValueError(f"Unsupported job type: {job_type}")
    ensure_table(connection)
    normalized_payload = build_job_payload(payload=payload)
    job_id = insert_and_get_rowid(
        connection,
        INSERT_JOB_SQL,
        (job_type, _serialize_payload(normalized_payload), depends_on_job_id),
    )
    connection.commit()
    return job_id


def enqueue_pipeline_jobs(
    connection: DBConnection,
    *,
    strategy_name: Optional[str] = None,
    strategy_names: Optional[list[str]] = None,
    symbol_names: Optional[list[str]] = None,
    payload: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    ensure_table(connection)
    job_payload = build_job_payload(
        strategy_name=strategy_name,
        strategy_names=strategy_names,
        symbol_names=symbol_names,
        payload=payload,
    )
    batch_id = str(uuid.uuid4())
    job_payload["batch_id"] = batch_id
    jobs: list[dict[str, Any]] = []
    prev_job_id: Optional[int] = None
    for job_type in PIPELINE_QUEUE_JOB_TYPES:
        job_id = enqueue_job(connection, job_type, payload=job_payload or None, depends_on_job_id=prev_job_id)
        jobs.append(
            {
                "batch_id": batch_id,
                "job_id": job_id,
                "job_type": job_type,
                "payload": job_payload,
                "depends_on_job_id": prev_job_id,
            }
        )
        prev_job_id = job_id
    return jobs


def list_jobs(
    connection: DBConnection,
    limit: int = 20,
    status: Optional[str] = None,
    job_type: Optional[str] = None,
) -> list[dict[str, Any]]:
    ensure_table(connection)
    clauses: list[str] = []
    params: list[Any] = []
    if status:
        clauses.append("status = ?")
        params.append(status)
    if job_type:
        clauses.append("job_type = ?")
        params.append(job_type)

    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = fetch_all_as_dicts(
        connection,
        f"""
        SELECT
            id,
            job_type,
            status,
            payload_json,
            result_json,
            error_message,
            attempt_count,
            depends_on_job_id,
            created_at,
            started_at,
            completed_at
        FROM job_queue
        {where_sql}
        ORDER BY id DESC
        LIMIT ?;
        """,
        tuple(params + [limit]),
    )
    return _normalize_rows(rows)


def get_job(connection: DBConnection, job_id: int) -> Optional[dict[str, Any]]:
    ensure_table(connection)
    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT
            id,
            job_type,
            status,
            payload_json,
            result_json,
            error_message,
            attempt_count,
            depends_on_job_id,
            created_at,
            started_at,
            completed_at
        FROM job_queue
        WHERE id = ?
        LIMIT 1;
        """,
        (job_id,),
    )
    normalized = _normalize_rows(rows)
    return normalized[0] if normalized else None


def lease_next_job(connection: DBConnection, job_type: Optional[str] = None) -> Optional[dict[str, Any]]:
    ensure_table(connection)
    clauses = [
        "status = 'queued'",
        "(depends_on_job_id IS NULL OR EXISTS ("
        "SELECT 1 FROM job_queue dep WHERE dep.id = job_queue.depends_on_job_id AND dep.status = 'completed'"
        "))",
    ]
    params: list[Any] = []
    if job_type is not None:
        clauses.append("job_type = ?")
        params.append(job_type)

    rows = fetch_all_as_dicts(
        connection,
        f"""
        SELECT id
        FROM job_queue
        WHERE {' AND '.join(clauses)}
        ORDER BY created_at ASC, id ASC
        LIMIT 1;
        """,
        tuple(params),
    )
    if not rows:
        return None

    job_id = int(rows[0]["id"])
    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'leased',
            attempt_count = attempt_count + 1,
            started_at = CURRENT_TIMESTAMP
        WHERE id = ?;
        """,
        (job_id,),
    )
    connection.commit()
    return get_job(connection, job_id)


def lease_job_by_id(connection: DBConnection, job_id: int) -> Optional[dict[str, Any]]:
    ensure_table(connection)
    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'leased',
            attempt_count = attempt_count + 1,
            started_at = CURRENT_TIMESTAMP
        WHERE id = ? AND status = 'queued';
        """,
        (job_id,),
    )
    connection.commit()
    job = get_job(connection, job_id)
    if job is None or job["status"] != "leased":
        return None
    return job


def complete_job(
    connection: DBConnection,
    job_id: int,
    result: Optional[dict[str, Any]] = None,
) -> None:
    ensure_table(connection)
    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'completed',
            result_json = ?,
            error_message = NULL,
            completed_at = CURRENT_TIMESTAMP
        WHERE id = ?;
        """,
        (_serialize_payload(result), job_id),
    )
    connection.commit()
    _propagate_dependent_job_payload(connection, job_id, result=result)


def fail_job(
    connection: DBConnection,
    job_id: int,
    error_message: str,
    result: Optional[dict[str, Any]] = None,
) -> None:
    ensure_table(connection)
    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'failed',
            result_json = ?,
            error_message = ?,
            completed_at = CURRENT_TIMESTAMP
        WHERE id = ?;
        """,
        (_serialize_payload(result), error_message, job_id),
    )
    connection.commit()


def retry_job(connection: DBConnection, job_id: int) -> dict[str, Any]:
    ensure_table(connection)
    job = get_job(connection, job_id)
    if job is None:
        raise ValueError(f"Unknown job id: {job_id}")
    if job["status"] != "failed":
        raise ValueError(f"Only failed jobs can be retried. Current status: {job['status']}")

    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'queued',
            result_json = NULL,
            error_message = NULL,
            started_at = NULL,
            completed_at = NULL
        WHERE id = ?;
        """,
        (job_id,),
    )
    connection.commit()
    retried_job = get_job(connection, job_id)
    if retried_job is None:
        raise RuntimeError(f"Retried job not found after update: {job_id}")
    return retried_job


def fail_batch_jobs(
    connection: DBConnection,
    batch_id: str,
    *,
    error_message: str,
    result: Optional[dict[str, Any]] = None,
) -> list[dict[str, Any]]:
    ensure_table(connection)
    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT id
        FROM job_queue
        WHERE
            status IN ('queued', 'leased')
            AND payload_json LIKE ?
        ORDER BY id ASC;
        """,
        (f'%"batch_id": "{batch_id}"%',),
    )
    failed_jobs: list[dict[str, Any]] = []
    for row in rows:
        job_id = int(row["id"])
        fail_job(connection, job_id, error_message, result=result)
        failed_job = get_job(connection, job_id)
        if failed_job is not None:
            failed_jobs.append(failed_job)
    return failed_jobs


def run_job(
    connection: DBConnection,
    job_type: str,
    payload: Optional[dict[str, Any]] = None,
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

    raise ValueError(f"Unsupported job type: {job_type}")


def _run_leased_job(connection: DBConnection, job: dict[str, Any]) -> dict[str, Any]:
    return run_job(
        connection,
        job_type=str(job["job_type"]),
        payload=dict(job.get("payload") or {}),
    )


def _propagate_dependent_job_payload(
    connection: DBConnection,
    job_id: int,
    *,
    result: Optional[dict[str, Any]] = None,
) -> None:
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
        WHERE depends_on_job_id = ? AND status = 'queued'
        ORDER BY id ASC;
        """,
        (job_id,),
    )
    for row in rows:
        dependent_id = int(row["id"])
        dependent_job = get_job(connection, dependent_id)
        if dependent_job is None or str(dependent_job["job_type"]) != target_job_type:
            continue
        updated_payload = dict(dependent_job.get("payload") or {})
        updated_payload.update(propagated_fields)
        connection.execute(
            """
            UPDATE job_queue
            SET payload_json = ?
            WHERE id = ?;
            """,
            (_serialize_payload(updated_payload), dependent_id),
        )
    connection.commit()


def run_next_pipeline_batch(connection: DBConnection, batch_id: Optional[str] = None) -> dict[str, Any]:
    ensure_table(connection)
    queued_jobs = list_jobs(connection, limit=200, status="queued")
    resolved_batch_id = batch_id or next(
        (
            str((job.get("payload") or {}).get("batch_id"))
            for job in reversed(queued_jobs)
            if (job.get("payload") or {}).get("batch_id")
        ),
        None,
    )
    if not resolved_batch_id:
        return {
            "status": "empty",
            "message": "No queued pipeline batches available." if batch_id is None else f"No queued jobs available for pipeline batch {batch_id}.",
        }

    batch_jobs = [
        job
        for job in reversed(queued_jobs)
        if str((job.get("payload") or {}).get("batch_id") or "") == resolved_batch_id
    ]
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


def run_pipeline_batch(connection: DBConnection, batch_id: Optional[str] = None) -> dict[str, Any]:
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
    strategy_name: Optional[str] = None,
    strategy_names: Optional[list[str]] = None,
    symbol_names: Optional[list[str]] = None,
    payload: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
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


def _run_leased_queue_job(connection: DBConnection, leased_job: dict[str, Any]) -> dict[str, Any]:
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
        error_detail: dict[str, Any] = {
            "error_type": exc.__class__.__name__,
            "execution_backend_status": backend_status,
        }
        if hasattr(exc, "to_payload") and callable(getattr(exc, "to_payload")):
            extra = exc.to_payload()
            if isinstance(extra, dict):
                error_detail["error_detail"] = extra
        fail_job(
            connection,
            job_id,
            str(exc),
            result=error_detail,
        )
        failed_job = get_job(connection, job_id)
        return {
            "status": "failed",
            "job": failed_job,
            "error": str(exc),
            "error_type": exc.__class__.__name__,
            "execution_backend_status": backend_status,
        }


def run_next_queued_job(connection: DBConnection, job_type: Optional[str] = None) -> dict[str, Any]:
    leased_job = lease_next_job(connection, job_type=job_type)
    if leased_job is None:
        return {
            "status": "empty",
            "job_type": job_type,
            "message": "No queued jobs available.",
        }
    return _run_leased_queue_job(connection, leased_job)


def reclaim_stale_leased_jobs(
    connection: DBConnection,
    lease_timeout_seconds: int = 300,
) -> int:
    """Reset leased jobs older than lease_timeout_seconds back to queued for retry.

    Returns the number of jobs reclaimed.  Call this at the start of each
    worker loop iteration so that jobs left in 'leased' state by a crashed
    worker are automatically recovered without manual intervention.
    """
    ensure_table(connection)
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=lease_timeout_seconds)
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    stale_rows = connection.execute(
        """
        SELECT id FROM job_queue
        WHERE status = 'leased'
          AND started_at IS NOT NULL
          AND started_at < ?;
        """,
        (cutoff_str,),
    ).fetchall()
    if not stale_rows:
        return 0
    stale_ids = [int(row[0]) for row in stale_rows]
    placeholders = ", ".join("?" for _ in stale_ids)
    connection.execute(
        f"UPDATE job_queue SET status = 'queued', started_at = NULL WHERE id IN ({placeholders});",
        tuple(stale_ids),
    )
    connection.commit()
    return len(stale_ids)
