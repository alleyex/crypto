from typing import Any

import requests

from app.audit.service import log_event
from app.core.settings import TELEGRAM_BOT_TOKEN
from app.core.settings import TELEGRAM_CHAT_ID
from app.core.settings import TELEGRAM_TIMEOUT_SECONDS
from app.system.heartbeat import record_heartbeat


def telegram_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def _audit_alert_delivery(status: str, message: str, payload: dict[str, Any]) -> None:
    try:
        log_event(
            event_type="alert_delivery",
            status=status,
            source="telegram",
            message=message,
            payload=payload,
        )
    except Exception:
        # Alert delivery must remain fail-safe even if audit logging is unavailable.
        return


def send_telegram_message(text: str) -> dict[str, Any]:
    if not telegram_configured():
        result = {
            "sent": False,
            "reason": "Telegram is not configured.",
        }
        _audit_alert_delivery(
            status="skipped",
            message="Telegram delivery skipped because configuration is missing.",
            payload={
                "text": text,
                **result,
            },
        )
        record_heartbeat(
            component="alerting",
            status="skipped",
            message="Telegram delivery skipped because configuration is missing.",
            payload={"text": text},
        )
        return result

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
            },
            timeout=TELEGRAM_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        result = {
            "sent": True,
            "response": payload,
        }
        _audit_alert_delivery(
            status="sent",
            message="Telegram alert delivered.",
            payload={
                "text": text,
                "telegram_ok": payload.get("ok"),
                "chat_id": TELEGRAM_CHAT_ID,
            },
        )
        record_heartbeat(
            component="alerting",
            status="ok",
            message="Telegram alert delivered.",
            payload={"text": text, "chat_id": TELEGRAM_CHAT_ID},
        )
        return result
    except requests.RequestException as exc:
        result = {
            "sent": False,
            "reason": f"Telegram send failed: {exc}",
        }
        _audit_alert_delivery(
            status="failed",
            message="Telegram alert delivery failed.",
            payload={
                "text": text,
                **result,
            },
        )
        record_heartbeat(
            component="alerting",
            status="failed",
            message="Telegram alert delivery failed.",
            payload={"text": text, "reason": result["reason"]},
        )
        return result
