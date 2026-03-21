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
BROKER_ALERT_STATE_FILE = RUNTIME_DIR / "broker_alert_state.json"


def _read_state() -> Optional[dict[str, Any]]:
    return read_alert_state(BROKER_ALERT_STATE_FILE, ttl_seconds=ALERT_REFIRE_SECONDS)


def _write_state(state: dict[str, Any]) -> None:
    write_alert_state(BROKER_ALERT_STATE_FILE, state)


def _clear_state() -> None:
    clear_alert_state(BROKER_ALERT_STATE_FILE)


def _build_fingerprint(check: dict[str, Any]) -> str:
    return build_fingerprint({
        "status": check.get("status"),
        "reason": check.get("reason"),
        "reason_code": check.get("reason_code"),
        "severity": check.get("severity"),
        "backend": check.get("backend"),
        "approved_risk_count": check.get("approved_risk_count"),
        "unfilled_order_count": check.get("unfilled_order_count"),
        "latest_order": check.get("latest_order"),
    })


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
    if broker_check.get("unfilled_order_count"):
        message += f", unfilled_orders={broker_check['unfilled_order_count']}"
    latest_fill = broker_check.get("latest_fill") or {}
    if latest_fill.get("price") is not None:
        message += f", latest_fill_price={latest_fill['price']}"
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
