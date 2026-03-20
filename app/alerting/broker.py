import hashlib
import json
from pathlib import Path
from typing import Any
from typing import Optional

from app.alerting.telegram import send_telegram_message


RUNTIME_DIR = Path("runtime")
BROKER_ALERT_STATE_FILE = RUNTIME_DIR / "broker_alert_state.json"


def _read_state() -> Optional[dict[str, Any]]:
    if not BROKER_ALERT_STATE_FILE.exists():
        return None
    return json.loads(BROKER_ALERT_STATE_FILE.read_text(encoding="utf-8"))


def _write_state(state: dict[str, Any]) -> None:
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    BROKER_ALERT_STATE_FILE.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def _clear_state() -> None:
    if BROKER_ALERT_STATE_FILE.exists():
        BROKER_ALERT_STATE_FILE.unlink()


def _build_fingerprint(check: dict[str, Any]) -> str:
    payload = {
        "status": check.get("status"),
        "reason": check.get("reason"),
        "reason_code": check.get("reason_code"),
        "severity": check.get("severity"),
        "backend": check.get("backend"),
        "approved_risk_count": check.get("approved_risk_count"),
        "latest_order": check.get("latest_order"),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def maybe_send_broker_alert(report: dict[str, Any]) -> dict[str, Any]:
    broker_check = report.get("checks", {}).get("broker_protection", {})
    if not isinstance(broker_check, dict) or broker_check.get("status") != "degraded":
        _clear_state()
        return {"sent": False, "reason": "Broker protection status is ok."}

    fingerprint = _build_fingerprint(broker_check)
    previous = _read_state()
    if previous is not None and previous.get("fingerprint") == fingerprint:
        return {"sent": False, "reason": "Broker alert already sent for current protected state."}

    message = "Crypto alert: broker protection triggered. backend={backend}, reason={reason}".format(
        backend=broker_check.get("backend", "unknown"),
        reason=broker_check.get("reason", "unknown"),
    )
    if broker_check.get("severity"):
        message += f", severity={broker_check['severity']}"
    if broker_check.get("reason_code"):
        message += f", code={broker_check['reason_code']}"
    if broker_check.get("recommended_action"):
        message += f", action={broker_check['recommended_action']}"
    latest_order = broker_check.get("latest_order") or {}
    if latest_order.get("status"):
        message += f", latest_order_status={latest_order['status']}"
    if latest_order.get("age_seconds") is not None:
        message += f", latest_order_age={latest_order['age_seconds']}s"
    if broker_check.get("approved_risk_count") is not None:
        message += f", approved_risk_count={broker_check['approved_risk_count']}"
    if broker_check.get("rejected_risk_streak") is not None:
        message += f", rejected_risk_streak={broker_check['rejected_risk_streak']}"
    if broker_check.get("latest_rejection_reason"):
        message += f", latest_rejection_reason={broker_check['latest_rejection_reason']}"

    send_result = send_telegram_message(message)
    if send_result.get("sent"):
        _write_state({"fingerprint": fingerprint, "backend": broker_check.get("backend")})
    return send_result
