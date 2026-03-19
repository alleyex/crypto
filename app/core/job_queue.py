import json
from typing import Any
from typing import Optional

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import insert_and_get_rowid
from app.core.migrations import run_migrations


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
