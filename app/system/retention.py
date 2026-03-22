"""Data retention utilities — purge old records to keep the database lean."""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from app.core.db import DBConnection


def _cutoff_iso(days: int) -> str:
    dt = datetime.now(timezone.utc) - timedelta(days=days)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def purge_old_audit_events(connection: DBConnection, days: int = 90) -> int:
    """Delete audit_events older than `days` days. Returns deleted row count."""
    cutoff = _cutoff_iso(days)
    connection.execute(
        "DELETE FROM audit_events WHERE created_at < ?;",
        (cutoff,),
    )
    connection.commit()
    return connection.execute(
        "SELECT changes();"
    ).fetchone()[0]


def purge_completed_job_queue(connection: DBConnection, days: int = 30) -> int:
    """Delete done/failed job_queue rows older than `days` days."""
    cutoff = _cutoff_iso(days)
    connection.execute(
        "DELETE FROM job_queue WHERE status IN ('done', 'failed') AND created_at < ?;",
        (cutoff,),
    )
    connection.commit()
    return connection.execute(
        "SELECT changes();"
    ).fetchone()[0]


def run_retention(
    connection: DBConnection,
    audit_days: int = 90,
    job_queue_days: int = 30,
) -> Dict[str, Any]:
    """Run all retention tasks and return a summary."""
    audit_deleted = purge_old_audit_events(connection, days=audit_days)
    job_deleted = purge_completed_job_queue(connection, days=job_queue_days)
    return {
        "audit_events_deleted": audit_deleted,
        "job_queue_deleted": job_deleted,
        "audit_retention_days": audit_days,
        "job_queue_retention_days": job_queue_days,
    }
