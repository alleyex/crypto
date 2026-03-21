"""Shared alert state utilities.

Every alerting module needs three things:
  1. A stable fingerprint of the current alert condition (for deduplication).
  2. Persistence of that fingerprint between process restarts (runtime JSON file).
  3. Cleanup of that persisted state when the condition clears.

This module provides those three primitives so each alert module can reuse
them without duplicating the hashlib/json/Path boilerplate.
"""
import hashlib
import json
from pathlib import Path
from typing import Any
from typing import Optional


def build_fingerprint(payload: Any) -> str:
    """Return a stable SHA-256 hex digest of any JSON-serialisable payload.

    Uses sort_keys=True so that dict key order never affects the fingerprint.
    """
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def read_alert_state(state_file: Path) -> Optional[dict[str, Any]]:
    """Return the persisted alert state dict, or None if the file does not exist."""
    if not state_file.exists():
        return None
    return json.loads(state_file.read_text(encoding="utf-8"))


def write_alert_state(state_file: Path, state: dict[str, Any]) -> None:
    """Persist *state* to *state_file*, creating parent directories as needed."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")


def clear_alert_state(state_file: Path) -> None:
    """Delete the persisted alert state file if it exists."""
    if state_file.exists():
        state_file.unlink()
