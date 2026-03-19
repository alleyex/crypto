import hashlib
import json
from pathlib import Path
from typing import Any
from typing import Optional

from app.alerting.telegram import send_telegram_message


RUNTIME_DIR = Path("runtime")
WORKER_ALERT_STATE_FILE = RUNTIME_DIR / "worker_alert_state.json"


def _read_state() -> Optional[dict[str, Any]]:
    if not WORKER_ALERT_STATE_FILE.exists():
        return None
    return json.loads(WORKER_ALERT_STATE_FILE.read_text(encoding="utf-8"))


def _write_state(state: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    WORKER_ALERT_STATE_FILE.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _clear_state() -> None:
    if WORKER_ALERT_STATE_FILE.exists():
        WORKER_ALERT_STATE_FILE.unlink()


def _build_fingerprint(stale_workers: list[dict[str, Any]]) -> str:
    payload = [
        {
            "component": item.get("component"),
            "status": item.get("status"),
            "age_seconds": item.get("age_seconds"),
        }
        for item in stale_workers
    ]
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def maybe_send_worker_alert(report: dict[str, Any]) -> dict[str, Any]:
    heartbeats = report.get("checks", {}).get("heartbeats", {})
    components = heartbeats.get("components", []) if isinstance(heartbeats, dict) else []
    stale_workers = [
        item
        for item in components
        if isinstance(item, dict) and item.get("stale") and str(item.get("component", "")).endswith("_worker")
    ]
    if not stale_workers:
        _clear_state()
        return {"sent": False, "reason": "No stale worker heartbeats."}

    fingerprint = _build_fingerprint(stale_workers)
    previous = _read_state()
    if previous is not None and previous.get("fingerprint") == fingerprint:
        return {"sent": False, "reason": "Worker alert already sent for current stale state."}

    worker_bits = [
        f"{item.get('component')} age={item.get('age_seconds', 'unknown')}s"
        for item in stale_workers
    ]
    message = "Crypto alert: stale worker heartbeats detected. Workers: {workers}".format(
        workers=", ".join(worker_bits),
    )
    send_result = send_telegram_message(message)
    if send_result.get("sent"):
        _write_state({"fingerprint": fingerprint, "worker_count": len(stale_workers)})
    return send_result
