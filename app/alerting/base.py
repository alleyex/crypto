"""Shared alert dispatch logic used by all domain alert modules."""

from pathlib import Path
from typing import Any, Callable

from app.alerting.state import AlertDeduplicator
from app.alerting.telegram import send_telegram_message
from app.core.settings import ALERT_REFIRE_SECONDS


def run_alert(
    state_file: Path,
    report: dict[str, Any],
    *,
    is_ok: Callable[[dict[str, Any]], bool],
    ok_reason: str,
    duplicate_reason: str,
    fingerprint_fn: Callable[[dict[str, Any]], str],
    message_fn: Callable[[dict[str, Any]], str],
    state_fn: Callable[[str, dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Generic alert deduplication + dispatch.

    Parameters
    ----------
    state_file:       Path to the JSON file used by AlertDeduplicator.
    report:           The full health/check report dict passed to the caller.
    is_ok:            Returns True when no alert should fire (status healthy).
    ok_reason:        Human-readable reason returned when is_ok is True.
    duplicate_reason: Reason returned when the fingerprint matches the last sent alert.
    fingerprint_fn:   Builds a stable fingerprint string from the report.
    message_fn:       Builds the Telegram message string from the report.
    state_fn:         Builds the dict written to state after a successful send.
    """
    _dedup = AlertDeduplicator(state_file, ttl_seconds=ALERT_REFIRE_SECONDS)
    if is_ok(report):
        _dedup.clear()
        return {"sent": False, "reason": ok_reason}

    fingerprint = fingerprint_fn(report)
    if _dedup.is_duplicate(fingerprint):
        return {"sent": False, "reason": duplicate_reason}

    message = message_fn(report)
    send_result = send_telegram_message(message)
    if send_result.get("sent"):
        _dedup.write(state_fn(fingerprint, report))
    return send_result
