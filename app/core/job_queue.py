import json
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import insert_and_get_rowid
from app.core.db import utc_now_iso
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.execution.adapter import get_execution_backend_status
from app.execution.adapter import get_execution_adapter_name

# Re-exports — callers may still import these from job_queue for backward compat.
from app.core.pipeline_orchestration import enqueue_and_run_pipeline_batch as enqueue_and_run_pipeline_batch  # noqa: F401
from app.core.pipeline_orchestration import run_next_pipeline_batch as run_next_pipeline_batch  # noqa: F401
from app.core.pipeline_orchestration import run_pipeline_batch as run_pipeline_batch  # noqa: F401

JOB_TYPES = ("market_data", "strategy", "risk", "execution", "training_ppo")
JOB_STATUSES = ("queued", "leased", "completed", "failed")
PIPELINE_QUEUE_JOB_TYPES = ("market_data", "strategy", "risk", "execution")

INSERT_JOB_SQL = """
INSERT INTO job_queue (
    job_type,
    status,
    payload_json,
    result_json,
    error_message,
    depends_on_job_id,
    batch_id,
    created_at
) VALUES (?, 'queued', ?, NULL, NULL, ?, ?, ?);
"""

def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)

def _serialize_payload(payload: dict[str, Any] | None) -> str | None:
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
    strategy_name: str | None = None,
    strategy_names: list[str] | None = None,
    symbol_names: list[str] | None = None,
    payload: dict[str, Any] | None = None,
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
    payload: dict[str, Any] | None = None,
    depends_on_job_id: int | None = None,
    batch_id: str | None = None,
) -> int:
    if job_type not in JOB_TYPES:
        raise ValueError(f"Unsupported job type: {job_type}")
    ensure_table(connection)
    normalized_payload = build_job_payload(payload=payload)
    job_id = insert_and_get_rowid(
        connection,
        INSERT_JOB_SQL,
        (job_type, _serialize_payload(normalized_payload), depends_on_job_id, batch_id, utc_now_iso()),
    )
    connection.commit()
    return job_id

def enqueue_pipeline_jobs(
    connection: DBConnection,
    *,
    strategy_name: str | None = None,
    strategy_names: list[str] | None = None,
    symbol_names: list[str] | None = None,
    payload: dict[str, Any] | None = None,
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
    prev_job_id: int | None = None
    for job_type in PIPELINE_QUEUE_JOB_TYPES:
        job_id = enqueue_job(connection, job_type, payload=job_payload or None, depends_on_job_id=prev_job_id, batch_id=batch_id)
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
    status: str | None = None,
    job_type: str | None = None,
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

def get_job(connection: DBConnection, job_id: int) -> dict[str, Any] | None:
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

def lease_next_job(connection: DBConnection, job_type: str | None = None) -> dict[str, Any] | None:
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
            started_at = ?
        WHERE id = ?;
        """,
        (utc_now_iso(), job_id),
    )
    connection.commit()
    return get_job(connection, job_id)

def lease_job_by_id(connection: DBConnection, job_id: int) -> dict[str, Any] | None:
    ensure_table(connection)
    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'leased',
            attempt_count = attempt_count + 1,
            started_at = ?
        WHERE id = ? AND status = 'queued';
        """,
        (utc_now_iso(), job_id),
    )
    connection.commit()
    job = get_job(connection, job_id)
    if job is None or job["status"] != "leased":
        return None
    return job

def complete_job(
    connection: DBConnection,
    job_id: int,
    result: dict[str, Any] | None = None,
) -> None:
    ensure_table(connection)
    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'completed',
            result_json = ?,
            error_message = NULL,
            completed_at = ?
        WHERE id = ?;
        """,
        (_serialize_payload(result), utc_now_iso(), job_id),
    )
    connection.commit()
    # Lazy import to avoid circular dependency (job_runner imports job_queue)
    from app.core.job_runner import _propagate_dependent_job_payload
    _propagate_dependent_job_payload(connection, job_id, result=result)

def fail_job(
    connection: DBConnection,
    job_id: int,
    error_message: str,
    result: dict[str, Any] | None = None,
) -> None:
    ensure_table(connection)
    connection.execute(
        """
        UPDATE job_queue
        SET
            status = 'failed',
            result_json = ?,
            error_message = ?,
            completed_at = ?
        WHERE id = ?;
        """,
        (_serialize_payload(result), error_message, utc_now_iso(), job_id),
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
    result: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    ensure_table(connection)
    connection.execute(
        """
        UPDATE job_queue
        SET status = 'failed',
            result_json = ?,
            error_message = ?,
            completed_at = ?
        WHERE batch_id = ?
          AND status IN ('queued', 'leased');
        """,
        (_serialize_payload(result), error_message, utc_now_iso(), batch_id),
    )
    connection.commit()
    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT id, job_type, status, payload_json, result_json, error_message,
               attempt_count, depends_on_job_id, created_at, started_at, completed_at
        FROM job_queue
        WHERE batch_id = ? AND status = 'failed'
        ORDER BY id ASC;
        """,
        (batch_id,),
    )
    return _normalize_rows(rows)

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
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(seconds=lease_timeout_seconds)).isoformat()
    stale_rows = connection.execute(
        """
        SELECT id FROM job_queue
        WHERE status = 'leased'
          AND started_at IS NOT NULL
          AND started_at < ?
        ORDER BY id ASC;
        """,
        (cutoff_iso,),
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

