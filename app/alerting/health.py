import json
from pathlib import Path
from typing import Any
from typing import Optional

from app.alerting.state import build_fingerprint
from app.alerting.state import clear_alert_state
from app.alerting.state import read_alert_state
from app.alerting.state import write_alert_state
from app.alerting.telegram import send_telegram_message


RUNTIME_DIR = Path("runtime")
HEALTH_ALERT_STATE_FILE = RUNTIME_DIR / "health_alert_state.json"


def _normalize_check(name: str, check: Any) -> dict[str, Any]:
    if not isinstance(check, dict):
        return {"name": name, "status": "unknown"}

    normalized: dict[str, Any] = {
        "name": name,
        "status": check.get("status", "unknown"),
    }
    if "reason" in check and check.get("reason") is not None:
        normalized["reason"] = check.get("reason")

    if name == "kill_switch":
        normalized["enabled"] = bool(check.get("enabled"))
    elif name == "scheduler":
        normalized["stopped"] = bool(check.get("stopped"))
    elif name == "heartbeats":
        normalized["components"] = sorted(
            {
                "component": str(item.get("component")),
                "status": str(item.get("status")),
                "message": str(item.get("message")),
            }
            for item in check.get("components", [])
            if isinstance(item, dict) and item.get("status") in ("failed", "stopped")
        )

    return normalized


def _build_fingerprint(report: dict[str, Any]) -> str:
    checks = report.get("checks", {})
    degraded_checks = {
        name: _normalize_check(name, check)
        for name, check in checks.items()
        if isinstance(check, dict) and check.get("status") in ("degraded", "error")
    }
    return build_fingerprint({
        "status": report.get("status"),
        "checks": degraded_checks,
    })


def _read_state() -> Optional[dict[str, Any]]:
    return read_alert_state(HEALTH_ALERT_STATE_FILE)


def _write_state(state: dict[str, Any]) -> None:
    write_alert_state(HEALTH_ALERT_STATE_FILE, state)


def _clear_state() -> None:
    clear_alert_state(HEALTH_ALERT_STATE_FILE)


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
