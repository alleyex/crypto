import json
from typing import Any
from typing import Optional

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import insert_and_get_rowid
from app.core.migrations import run_migrations
from app.core.settings import DEFAULT_STRATEGY_NAME
from app.pipeline.execution_job import run_execution_job
from app.pipeline.market_data_job import run_market_data_job
from app.pipeline.strategy_job import run_strategy_job
from app.pipeline.strategy_job import run_strategy_jobs


JOB_TYPES = ("market_data", "strategy", "execution")
JOB_STATUSES = ("queued", "leased", "completed", "failed")


INSERT_JOB_SQL = """
INSERT INTO job_queue (
    job_type,
    status,
    payload_json,
    result_json,
    error_message
) VALUES (?, 'queued', ?, NULL, NULL);
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
) -> int:
    if job_type not in JOB_TYPES:
        raise ValueError(f"Unsupported job type: {job_type}")
    ensure_table(connection)
    job_id = insert_and_get_rowid(
        connection,
        INSERT_JOB_SQL,
        (job_type, _serialize_payload(payload)),
    )
    connection.commit()
    return job_id


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
    clauses = ["status = 'queued'"]
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


def _run_leased_job(connection: DBConnection, job: dict[str, Any]) -> dict[str, Any]:
    payload = dict(job.get("payload") or {})
    job_type = str(job["job_type"])

    if job_type == "market_data":
        return run_market_data_job(
            connection,
            symbol_names=payload.get("symbol_names"),
        )

    if job_type == "strategy":
        strategy_names = payload.get("strategy_names") or []
        strategy_name = payload.get("strategy_name")
        symbol_names = payload.get("symbol_names")
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

    if job_type == "execution":
        risk_event_ids = payload.get("risk_event_ids")
        symbol_names = payload.get("symbol_names")
        normalized_risk_event_ids = [int(item) for item in risk_event_ids] if risk_event_ids else None
        return run_execution_job(
            connection,
            risk_event_ids=normalized_risk_event_ids,
            symbol_names=symbol_names,
        )

    raise ValueError(f"Unsupported job type: {job_type}")


def run_next_queued_job(connection: DBConnection, job_type: Optional[str] = None) -> dict[str, Any]:
    leased_job = lease_next_job(connection, job_type=job_type)
    if leased_job is None:
        return {
            "status": "empty",
            "job_type": job_type,
            "message": "No queued jobs available.",
        }

    job_id = int(leased_job["id"])
    try:
        result = _run_leased_job(connection, leased_job)
        complete_job(connection, job_id, result=result)
        completed_job = get_job(connection, job_id)
        return {
            "status": "completed",
            "job": completed_job,
            "result": result,
        }
    except Exception as exc:
        fail_job(
            connection,
            job_id,
            str(exc),
            result={"error_type": exc.__class__.__name__},
        )
        failed_job = get_job(connection, job_id)
        return {
            "status": "failed",
            "job": failed_job,
            "error": str(exc),
            "error_type": exc.__class__.__name__,
        }
