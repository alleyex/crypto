from pathlib import Path
from typing import Any

from app.alerting.base import run_alert
from app.alerting.state import AlertDeduplicator, build_fingerprint
from app.core.settings import ALERT_REFIRE_SECONDS

RUNTIME_DIR = Path("runtime")
QUEUE_ALERT_STATE_FILE = RUNTIME_DIR / "queue_alert_state.json"


def _get_queue_check(report: dict[str, Any]) -> dict[str, Any]:
    return report.get("checks", {}).get("queue", {})


def _build_fingerprint(report: dict[str, Any]) -> str:
    q = _get_queue_check(report)
    return build_fingerprint({
        "status": q.get("status"),
        "counts": q.get("counts", {}),
        "latest_failed_job": q.get("latest_failed_job"),
        "latest_incomplete_batch": q.get("latest_incomplete_batch"),
    })


def _is_ok(report: dict[str, Any]) -> bool:
    q = _get_queue_check(report)
    counts = q.get("counts", {})
    failed_count = int(counts.get("failed", 0) or 0)
    latest_incomplete_batch = q.get("latest_incomplete_batch") or {}
    stale_batch = (
        q.get("status") == "degraded"
        and q.get("reason") == "Queue contains stale incomplete batches."
        and bool(latest_incomplete_batch)
    )
    return q.get("status") == "ok" or (failed_count <= 0 and not stale_batch)


def _build_message(report: dict[str, Any]) -> str:
    q = _get_queue_check(report)
    counts = q.get("counts", {})
    failed_count = int(counts.get("failed", 0) or 0)
    latest_failed_job = q.get("latest_failed_job") or {}
    latest_incomplete_batch = q.get("latest_incomplete_batch") or {}
    latest_batch = latest_incomplete_batch or q.get("latest_completed_batch") or {}

    if failed_count > 0:
        message = "Crypto alert: queue has failed jobs. failed={failed_count}, latest={job_type}#{job_id}, attempts={attempts}".format(
            failed_count=failed_count,
            job_type=latest_failed_job.get("job_type", "unknown"),
            job_id=latest_failed_job.get("id", "unknown"),
            attempts=latest_failed_job.get("attempt_count", "unknown"),
        )
        if latest_failed_job.get("error_message"):
            message += f", error={latest_failed_job['error_message']}"
    else:
        message = "Crypto alert: queue has stale incomplete batch."
    if latest_batch.get("source"):
        message += f", source={latest_batch['source']}"
    if latest_batch.get("orchestration"):
        message += f", orchestration={latest_batch['orchestration']}"
    if latest_batch.get("age_seconds") is not None:
        message += f", batch_age={latest_batch['age_seconds']}s"
    return message


def maybe_send_queue_alert(report: dict[str, Any]) -> dict[str, Any]:
    queue_check = _get_queue_check(report)
    if not isinstance(queue_check, dict):
        AlertDeduplicator(QUEUE_ALERT_STATE_FILE, ttl_seconds=ALERT_REFIRE_SECONDS).clear()
        return {"sent": False, "reason": "Queue check is unavailable."}

    counts = queue_check.get("counts", {})
    failed_count = int(counts.get("failed", 0) or 0)
    return run_alert(
        QUEUE_ALERT_STATE_FILE,
        report,
        is_ok=_is_ok,
        ok_reason="Queue has no failed jobs.",
        duplicate_reason="Queue alert already sent for current failed state.",
        fingerprint_fn=_build_fingerprint,
        message_fn=_build_message,
        state_fn=lambda fp, r: {"fingerprint": fp, "failed_count": failed_count},
    )
