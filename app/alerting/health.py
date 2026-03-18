import hashlib
import json
from pathlib import Path
from typing import Any
from typing import Optional

from app.alerting.telegram import send_telegram_message


RUNTIME_DIR = Path("runtime")
HEALTH_ALERT_STATE_FILE = RUNTIME_DIR / "health_alert_state.json"


def _build_fingerprint(report: dict[str, Any]) -> str:
    payload = {
        "status": report.get("status"),
        "checks": report.get("checks"),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _read_state() -> Optional[dict[str, Any]]:
    if not HEALTH_ALERT_STATE_FILE.exists():
        return None
    return json.loads(HEALTH_ALERT_STATE_FILE.read_text(encoding="utf-8"))


def _write_state(state: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    HEALTH_ALERT_STATE_FILE.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _clear_state() -> None:
    if HEALTH_ALERT_STATE_FILE.exists():
        HEALTH_ALERT_STATE_FILE.unlink()


def maybe_send_health_alert(report: dict[str, Any]) -> dict[str, Any]:
    status = report.get("status", "ok")
    if status == "ok":
        _clear_state()
        return {"sent": False, "reason": "Health status is ok."}

    fingerprint = _build_fingerprint(report)
    previous = _read_state()
    if previous is not None and previous.get("fingerprint") == fingerprint:
        return {"sent": False, "reason": "Health alert already sent for current state."}

    checks = report.get("checks", {})
    degraded_checks = [
        f"{name}:{check.get('status')}"
        for name, check in checks.items()
        if check.get("status") in ("degraded", "error")
    ]
    message = "Crypto alert: health is {status}. Checks: {checks}".format(
        status=status.upper(),
        checks=", ".join(degraded_checks) if degraded_checks else "none",
    )
    send_result = send_telegram_message(message)
    if send_result.get("sent"):
        _write_state({"fingerprint": fingerprint, "status": status})
    return send_result
