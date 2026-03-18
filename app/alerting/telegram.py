from typing import Any

import requests

from app.core.settings import TELEGRAM_BOT_TOKEN
from app.core.settings import TELEGRAM_CHAT_ID


def telegram_configured() -> bool:
    return bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)


def send_telegram_message(text: str) -> dict[str, Any]:
    if not telegram_configured():
        return {
            "sent": False,
            "reason": "Telegram is not configured.",
        }

    response = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
        },
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    return {
        "sent": True,
        "response": payload,
    }
