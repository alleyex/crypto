"""Shared alert state utilities.

Every alerting module needs three things:
  1. A stable fingerprint of the current alert condition (for deduplication).
  2. Persistence of that fingerprint between process restarts (runtime JSON file).
  3. Cleanup of that persisted state when the condition clears.

This module provides those three primitives so each alert module can reuse
them without duplicating the hashlib/json/Path boilerplate.

TTL / re-fire
-------------
write_alert_state() stamps every saved state with a ``written_at`` ISO
timestamp.  read_alert_state() accepts an optional ``ttl_seconds`` argument:
if the saved state is older than that many seconds, the function returns None
(as if no state existed) so the same condition re-fires an alert after the
TTL elapses.  Pass ``ttl_seconds=0`` to disable expiry entirely.
"""
import hashlib
import json
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any
from typing import Optional


def build_fingerprint(payload: Any) -> str:
    """Return a stable SHA-256 hex digest of any JSON-serialisable payload.

    Uses sort_keys=True so that dict key order never affects the fingerprint.
    """
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def read_alert_state(
    state_file: Path,
    ttl_seconds: int = 0,
) -> Optional[dict[str, Any]]:
    """Return the persisted alert state dict, or None if absent or expired.

    Args:
        state_file:   Path to the JSON state file.
        ttl_seconds:  Maximum age in seconds before the state is considered
                      expired and treated as absent.  0 means no expiry.
    """
    if not state_file.exists():
        return None
    state = json.loads(state_file.read_text(encoding="utf-8"))
    if ttl_seconds > 0 and "written_at" in state:
        written_at = datetime.fromisoformat(state["written_at"])
        age_seconds = (datetime.now(timezone.utc) - written_at).total_seconds()
        if age_seconds > ttl_seconds:
            return None
    return state


def write_alert_state(state_file: Path, state: dict[str, Any]) -> None:
    """Persist *state* to *state_file*, adding a ``written_at`` timestamp.

    Creates parent directories as needed.
    """
    stamped = {**state, "written_at": datetime.now(timezone.utc).isoformat()}
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(stamped, sort_keys=True), encoding="utf-8")


def clear_alert_state(state_file: Path) -> None:
    """Delete the persisted alert state file if it exists."""
    if state_file.exists():
        state_file.unlink()
