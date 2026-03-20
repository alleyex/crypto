import hashlib
import json
from pathlib import Path
from typing import Any
from typing import Optional

from app.alerting.telegram import send_telegram_message


RUNTIME_DIR = Path("runtime")
QUEUE_ALERT_STATE_FILE = RUNTIME_DIR / "queue_alert_state.json"


def _read_state() -> Optional[dict[str, Any]]:
    if not QUEUE_ALERT_STATE_FILE.exists():
        return None
    return json.loads(QUEUE_ALERT_STATE_FILE.read_text(encoding="utf-8"))


def _write_state(state: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    QUEUE_ALERT_STATE_FILE.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _clear_state() -> None:
    if QUEUE_ALERT_STATE_FILE.exists():
        QUEUE_ALERT_STATE_FILE.unlink()


def _build_fingerprint(queue_check: dict[str, Any]) -> str:
    payload = {
        "status": queue_check.get("status"),
        "counts": queue_check.get("counts", {}),
        "latest_failed_job": queue_check.get("latest_failed_job"),
        "latest_incomplete_batch": queue_check.get("latest_incomplete_batch"),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def maybe_send_queue_alert(report: dict[str, Any]) -> dict[str, Any]:
    queue_check = report.get("checks", {}).get("queue", {})
    if not isinstance(queue_check, dict):
        _clear_state()
        return {"sent": False, "reason": "Queue check is unavailable."}

    counts = queue_check.get("counts", {})
    failed_count = int(counts.get("failed", 0) or 0)
    latest_incomplete_batch = queue_check.get("latest_incomplete_batch") or {}
    stale_batch = (
        queue_check.get("status") == "degraded"
        and queue_check.get("reason") == "Queue contains stale incomplete batches."
        and bool(latest_incomplete_batch)
    )
    if queue_check.get("status") == "ok" or (failed_count <= 0 and not stale_batch):
        _clear_state()
        return {"sent": False, "reason": "Queue has no failed jobs."}

    fingerprint = _build_fingerprint(queue_check)
    previous = _read_state()
    if previous is not None and previous.get("fingerprint") == fingerprint:
        return {"sent": False, "reason": "Queue alert already sent for current failed state."}

    latest_failed_job = queue_check.get("latest_failed_job") or {}
    latest_batch = latest_incomplete_batch or queue_check.get("latest_completed_batch") or {}
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

    send_result = send_telegram_message(message)
    if send_result.get("sent"):
        _write_state({"fingerprint": fingerprint, "failed_count": failed_count})
    return send_result
