from pathlib import Path
from typing import Any
from typing import Optional

from app.alerting.state import build_fingerprint
from app.alerting.state import clear_alert_state
from app.alerting.state import read_alert_state
from app.alerting.state import write_alert_state
from app.alerting.telegram import send_telegram_message
from app.core.settings import ALERT_REFIRE_SECONDS


RUNTIME_DIR = Path("runtime")
EXECUTION_ALERT_STATE_FILE = RUNTIME_DIR / "execution_alert_state.json"


def _read_state() -> Optional[dict[str, Any]]:
    return read_alert_state(EXECUTION_ALERT_STATE_FILE, ttl_seconds=ALERT_REFIRE_SECONDS)


def _write_state(state: dict[str, Any]) -> None:
    write_alert_state(EXECUTION_ALERT_STATE_FILE, state)


def _clear_state() -> None:
    clear_alert_state(EXECUTION_ALERT_STATE_FILE)


def _build_fingerprint(job: dict[str, Any]) -> str:
    return build_fingerprint({
        "id": job.get("id"),
        "job_type": job.get("job_type"),
        "attempt_count": job.get("attempt_count"),
        "error_message": job.get("error_message"),
    })


def maybe_send_execution_alert(report: dict[str, Any]) -> dict[str, Any]:
    queue_check = report.get("checks", {}).get("queue", {})
    latest_failed_job = queue_check.get("latest_failed_job") if isinstance(queue_check, dict) else None
    if not isinstance(latest_failed_job, dict) or latest_failed_job.get("job_type") != "execution":
        _clear_state()
        return {"sent": False, "reason": "No failed execution queue job."}

    fingerprint = _build_fingerprint(latest_failed_job)
    previous = _read_state()
    if previous is not None and previous.get("fingerprint") == fingerprint:
        return {"sent": False, "reason": "Execution alert already sent for current failed job."}

    message = "Crypto alert: execution job failed. job=#{job_id}, attempts={attempts}, error={error}".format(
        job_id=latest_failed_job.get("id", "unknown"),
        attempts=latest_failed_job.get("attempt_count", "unknown"),
        error=latest_failed_job.get("error_message", "unknown"),
    )
    send_result = send_telegram_message(message)
    if send_result.get("sent"):
        _write_state({"fingerprint": fingerprint, "job_id": latest_failed_job.get("id")})
    return send_result
