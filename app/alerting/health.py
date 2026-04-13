from pathlib import Path
from typing import Any

from app.alerting.base import run_alert
from app.alerting.state import build_fingerprint

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
        components = [
            {
                "component": str(item.get("component")),
                "status": str(item.get("status")),
                "message": str(item.get("message")),
            }
            for item in check.get("components", [])
            if isinstance(item, dict) and item.get("status") in ("failed", "stopped")
        ]
        normalized["components"] = sorted(
            components,
            key=lambda item: (
                item.get("component", ""),
                item.get("status", ""),
                item.get("message", ""),
            ),
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


def _build_message(report: dict[str, Any]) -> str:
    status = report.get("status", "unknown")
    checks = report.get("checks", {})
    degraded_checks = [
        f"{name}:{check.get('status')}"
        for name, check in checks.items()
        if check.get("status") in ("degraded", "error")
    ]
    return "Crypto alert: health is {status}. Checks: {checks}".format(
        status=status.upper(),
        checks=", ".join(degraded_checks) if degraded_checks else "none",
    )


def maybe_send_health_alert(report: dict[str, Any]) -> dict[str, Any]:
    return run_alert(
        HEALTH_ALERT_STATE_FILE,
        report,
        is_ok=lambda r: r.get("status", "ok") == "ok",
        ok_reason="Health status is ok.",
        duplicate_reason="Health alert already sent for current state.",
        fingerprint_fn=_build_fingerprint,
        message_fn=_build_message,
        state_fn=lambda fp, r: {"fingerprint": fp, "status": r.get("status")},
    )
