"""Job queue summary query: aggregates counts, metrics, and recent batch state."""
import json
from typing import Any

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import parse_db_timestamp
from app.core.db import utc_now

_SELECT_JOB_QUEUE_SQL = """
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
ORDER BY id DESC
LIMIT ?;
"""

def _decode_json_field(value: Any) -> Any:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None

def _get_job_queue_jobs(connection: DBConnection, limit: int = 20) -> list[dict[str, Any]]:
    rows = fetch_all_as_dicts(connection, _SELECT_JOB_QUEUE_SQL, (limit,))
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["payload"] = _decode_json_field(item.get("payload_json"))
        item["result"] = _decode_json_field(item.get("result_json"))
        normalized.append(item)
    return normalized

def get_job_queue_summary(connection: DBConnection) -> dict[str, Any]:
    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT
            status,
            COUNT(*) AS job_count
        FROM job_queue
        GROUP BY status
        ORDER BY status ASC;
        """,
    )
    counts = {str(row["status"]): int(row["job_count"]) for row in rows}
    latest_jobs = _get_job_queue_jobs(connection, limit=5)
    job_type_rows = fetch_all_as_dicts(
        connection,
        """
        SELECT
            job_type,
            status,
            COUNT(*) AS job_count
        FROM job_queue
        GROUP BY job_type, status
        ORDER BY job_type ASC, status ASC;
        """,
    )
    attempt_rows = fetch_all_as_dicts(
        connection,
        """
        SELECT
            job_type,
            AVG(attempt_count) AS avg_attempt_count,
            MAX(attempt_count) AS max_attempt_count
        FROM job_queue
        GROUP BY job_type
        ORDER BY job_type ASC;
        """,
    )
    overall_attempt_row = fetch_all_as_dicts(
        connection,
        """
        SELECT
            AVG(attempt_count) AS avg_attempt_count,
            MAX(attempt_count) AS max_attempt_count
        FROM job_queue;
        """,
    )[0]
    job_type_counts: dict[str, dict[str, int]] = {
        "market_data": {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
        "strategy": {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
        "execution": {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
    }
    for row in job_type_rows:
        job_type = str(row["job_type"])
        status = str(row["status"])
        job_count = int(row["job_count"])
        entry = job_type_counts.setdefault(
            job_type,
            {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
        )
        entry[status] = job_count
        entry["total"] += job_count
    attempt_map = {str(row["job_type"]): row for row in attempt_rows}
    for job_type, entry in job_type_counts.items():
        total = int(entry["total"])
        completed = int(entry["completed"])
        failed = int(entry["failed"])
        attempts = attempt_map.get(job_type, {})
        type_latest_jobs = [job for job in latest_jobs if str(job.get("job_type")) == job_type]
        type_terminal_jobs = [job for job in type_latest_jobs if job["status"] in {"completed", "failed"}]
        entry["success_ratio"] = round(completed / total, 4) if total else 0.0
        entry["failure_ratio"] = round(failed / total, 4) if total else 0.0
        raw_avg = attempts.get("avg_attempt_count")
        raw_max = attempts.get("max_attempt_count")
        entry["avg_attempt_count"] = round(float(raw_avg), 2) if isinstance(raw_avg, (int, float)) else 0.0
        entry["max_attempt_count"] = int(raw_max) if isinstance(raw_max, int) else 0
        entry["latest_failed_job"] = next((job for job in type_latest_jobs if job["status"] == "failed"), None)
        entry["latest_retry_job"] = next((job for job in type_latest_jobs if (int(job["attempt_count"]) if isinstance(job.get("attempt_count"), int) else 0) > 1), None)
        entry["recent_terminal_statuses"] = [
            "F" if job["status"] == "failed" else "C"
            for job in type_terminal_jobs[:3]
        ]
        entry["recent_terminal_trend"] = "".join(entry["recent_terminal_statuses"])
    retry_jobs = [job for job in latest_jobs if (int(job["attempt_count"]) if isinstance(job.get("attempt_count"), int) else 0) > 1]
    latest_failed_job = next((job for job in latest_jobs if job["status"] == "failed"), None)
    latest_retry_job = next((job for job in latest_jobs if (int(job["attempt_count"]) if isinstance(job.get("attempt_count"), int) else 0) > 1), None)
    failure_streak = 0
    terminal_jobs = [job for job in latest_jobs if job["status"] in {"completed", "failed"}]
    for job in terminal_jobs:
        if job["status"] != "failed":
            break
        failure_streak += 1
    total_job_count = sum(counts.values())
    completed_count = counts.get("completed", 0)
    failed_count = counts.get("failed", 0)
    recent_batches: list[dict[str, Any]] = []
    batch_map: dict[str, dict[str, Any]] = {}
    for job in latest_jobs:
        payload = job.get("payload") or {}
        batch_id = payload.get("batch_id")
        if not batch_id:
            continue
        batch_entry = batch_map.get(batch_id)
        if batch_entry is None:
            created_at = job.get("created_at")
            batch_entry = {
                "batch_id": batch_id,
                "job_types": [],
                "statuses": {},
                "strategy_names": payload.get("strategy_names", []),
                "symbol_names": payload.get("symbol_names", []),
                "execution_backend": payload.get("execution_backend"),
                "source": payload.get("source"),
                "orchestration": payload.get("orchestration"),
                "created_at": created_at,
                "age_seconds": int((utc_now() - parse_db_timestamp(str(created_at))).total_seconds()) if created_at else None,
            }
            batch_map[batch_id] = batch_entry
            recent_batches.append(batch_entry)
        elif job.get("created_at") and batch_entry.get("created_at"):
            current_created_at = parse_db_timestamp(str(batch_entry["created_at"]))
            candidate_created_at = parse_db_timestamp(str(job["created_at"]))
            if candidate_created_at < current_created_at:
                batch_entry["created_at"] = job["created_at"]
                batch_entry["age_seconds"] = int((utc_now() - candidate_created_at).total_seconds())
        batch_entry["job_types"].append(job["job_type"])
        batch_entry["statuses"][job["job_type"]] = job["status"]
    latest_incomplete_batch = next(
        (
            batch
            for batch in recent_batches
            if any(status != "completed" for status in (batch.get("statuses") or {}).values())
        ),
        None,
    )
    latest_completed_batch = next(
        (
            batch
            for batch in recent_batches
            if batch.get("statuses") and all(status == "completed" for status in batch.get("statuses", {}).values())
        ),
        None,
    )
    return {
        "counts": {
            "queued": counts.get("queued", 0),
            "leased": counts.get("leased", 0),
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "total": total_job_count,
        },
        "metrics": {
            "success_ratio": round(completed_count / total_job_count, 4) if total_job_count else 0.0,
            "failure_ratio": round(failed_count / total_job_count, 4) if total_job_count else 0.0,
            "avg_attempt_count": round(float(overall_attempt_row.get("avg_attempt_count") or 0.0), 2),
            "max_attempt_count": int(overall_attempt_row.get("max_attempt_count") or 0),
            "retry_job_count": len(retry_jobs),
            "failure_streak": failure_streak,
            "recent_failure_count": len([job for job in latest_jobs if job["status"] == "failed"]),
            "recent_retry_count": len(retry_jobs),
        },
        "job_type_counts": job_type_counts,
        "failed_jobs": [job for job in latest_jobs if job["status"] == "failed"],
        "retry_jobs": retry_jobs,
        "latest_failed_job": latest_failed_job,
        "latest_retry_job": latest_retry_job,
        "recent_batches": recent_batches,
        "latest_incomplete_batch": latest_incomplete_batch,
        "latest_completed_batch": latest_completed_batch,
        "latest_jobs": latest_jobs,
    }
