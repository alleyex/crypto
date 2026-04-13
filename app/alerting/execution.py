from pathlib import Path
from typing import Any

from app.alerting.base import run_alert
from app.alerting.state import build_fingerprint

RUNTIME_DIR = Path("runtime")
EXECUTION_ALERT_STATE_FILE = RUNTIME_DIR / "execution_alert_state.json"


def _get_failed_job(report: dict[str, Any]) -> dict[str, Any] | None:
    queue_check = report.get("checks", {}).get("queue", {})
    job = queue_check.get("latest_failed_job") if isinstance(queue_check, dict) else None
    return job if isinstance(job, dict) and job.get("job_type") == "execution" else None


def _build_fingerprint(report: dict[str, Any]) -> str:
    job = _get_failed_job(report) or {}
    return build_fingerprint({
        "id": job.get("id"),
        "job_type": job.get("job_type"),
        "attempt_count": job.get("attempt_count"),
        "error_message": job.get("error_message"),
    })


def _build_message(report: dict[str, Any]) -> str:
    job = _get_failed_job(report) or {}
    return "Crypto alert: execution job failed. job=#{job_id}, attempts={attempts}, error={error}".format(
        job_id=job.get("id", "unknown"),
        attempts=job.get("attempt_count", "unknown"),
        error=job.get("error_message", "unknown"),
    )


def maybe_send_execution_alert(report: dict[str, Any]) -> dict[str, Any]:
    return run_alert(
        EXECUTION_ALERT_STATE_FILE,
        report,
        is_ok=lambda r: _get_failed_job(r) is None,
        ok_reason="No failed execution queue job.",
        duplicate_reason="Execution alert already sent for current failed job.",
        fingerprint_fn=_build_fingerprint,
        message_fn=_build_message,
        state_fn=lambda fp, r: {"fingerprint": fp, "job_id": (_get_failed_job(r) or {}).get("id")},
    )
