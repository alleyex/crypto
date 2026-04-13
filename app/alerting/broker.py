from pathlib import Path
from typing import Any

from app.alerting.base import run_alert
from app.alerting.state import build_fingerprint

RUNTIME_DIR = Path("runtime")
BROKER_ALERT_STATE_FILE = RUNTIME_DIR / "broker_alert_state.json"


def _build_fingerprint(check: dict[str, Any]) -> str:
    # Only include stable fields from latest_order — age_seconds and created_at
    # change every scheduler tick and would cause the fingerprint to drift,
    # defeating deduplication and firing an alert on every health check.
    latest_order = check.get("latest_order") or {}
    return build_fingerprint({
        "status": check.get("status"),
        "reason": check.get("reason"),
        "reason_code": check.get("reason_code"),
        "severity": check.get("severity"),
        "backend": check.get("backend"),
        "approved_risk_count": check.get("approved_risk_count"),
        "unfilled_order_count": check.get("unfilled_order_count"),
        "latest_order_id": latest_order.get("id"),
        "latest_order_status": latest_order.get("status"),
        "latest_order_symbol": latest_order.get("symbol"),
    })


def _build_message(check: dict[str, Any]) -> str:
    message = "Crypto alert: broker protection triggered. backend={backend}, reason={reason}".format(
        backend=check.get("backend", "unknown"),
        reason=check.get("reason", "unknown"),
    )
    if check.get("severity"):
        message += f", severity={check['severity']}"
    if check.get("reason_code"):
        message += f", code={check['reason_code']}"
    if check.get("recommended_action"):
        message += f", action={check['recommended_action']}"
    latest_order = check.get("latest_order") or {}
    if latest_order.get("status"):
        message += f", latest_order_status={latest_order['status']}"
    if latest_order.get("age_seconds") is not None:
        message += f", latest_order_age={latest_order['age_seconds']}s"
    if check.get("unfilled_order_count"):
        message += f", unfilled_orders={check['unfilled_order_count']}"
    latest_fill = check.get("latest_fill") or {}
    if latest_fill.get("price") is not None:
        message += f", latest_fill_price={latest_fill['price']}"
    if check.get("approved_risk_count") is not None:
        message += f", approved_risk_count={check['approved_risk_count']}"
    if check.get("anomalous_rejected_risk_streak") is not None:
        message += f", anomalous_rejected_risk_streak={check['anomalous_rejected_risk_streak']}"
    elif check.get("rejected_risk_streak") is not None:
        message += f", rejected_risk_streak={check['rejected_risk_streak']}"
    if check.get("expected_rejected_risk_streak") is not None:
        message += f", expected_rejected_risk_streak={check['expected_rejected_risk_streak']}"
    if check.get("latest_rejection_reason"):
        message += f", latest_rejection_reason={check['latest_rejection_reason']}"
    if check.get("expected_latest_rejection_reason"):
        message += f", expected_latest_rejection_reason={check['expected_latest_rejection_reason']}"
    return message


def maybe_send_broker_alert(report: dict[str, Any]) -> dict[str, Any]:
    broker_check = report.get("checks", {}).get("broker_protection", {})
    return run_alert(
        BROKER_ALERT_STATE_FILE,
        broker_check,
        is_ok=lambda c: not isinstance(c, dict) or c.get("status") != "degraded",
        ok_reason="Broker protection status is ok.",
        duplicate_reason="Broker alert already sent for current protected state.",
        fingerprint_fn=_build_fingerprint,
        message_fn=_build_message,
        state_fn=lambda fp, c: {"fingerprint": fp, "backend": c.get("backend")},
    )
