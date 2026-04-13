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
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

def _json_safe_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)

def build_fingerprint(payload: Any) -> str:
    """Return a stable SHA-256 hex digest of any JSON-serialisable payload.

    Uses sort_keys=True so that dict key order never affects the fingerprint.
    """
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True, default=_json_safe_default)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()

def read_alert_state(
    state_file: Path,
    ttl_seconds: int = 0,
) -> dict[str, Any] | None:
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
    state_file.write_text(
        json.dumps(stamped, sort_keys=True, default=_json_safe_default),
        encoding="utf-8",
    )

def clear_alert_state(state_file: Path) -> None:
    """Delete the persisted alert state file if it exists."""
    if state_file.exists():
        state_file.unlink()

class AlertDeduplicator:
    """Binds a state file path to read/write/clear/dedup helpers.

    Each alerting module creates one instance at module level, passing a
    callable that returns the current state_file path so that tests can patch
    the module-level path constant and have it take effect:

        _STATE_FILE = RUNTIME_DIR / "foo_alert_state.json"
        _dedup = AlertDeduplicator(lambda: _STATE_FILE)

    Then uses it inside the ``maybe_send_*`` function instead of the three
    private ``_read_state`` / ``_write_state`` / ``_clear_state`` helpers.
    """

    def __init__(self, state_file: Any, ttl_seconds: int = 0) -> None:
        self._state_file = state_file
        self.ttl_seconds = ttl_seconds

    @property
    def state_file(self) -> Path:
        if callable(self._state_file):
            return self._state_file()
        return self._state_file

    def read(self) -> dict[str, Any] | None:
        return read_alert_state(self.state_file, ttl_seconds=self.ttl_seconds)

    def write(self, state: dict[str, Any]) -> None:
        write_alert_state(self.state_file, state)

    def clear(self) -> None:
        clear_alert_state(self.state_file)

    def is_duplicate(self, fingerprint: str) -> bool:
        """Return True if *fingerprint* matches the stored state (not expired)."""
        previous = self.read()
        return previous is not None and previous.get("fingerprint") == fingerprint
