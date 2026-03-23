import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

RUNTIME_DIR = Path("runtime")
FETCH_HISTORY_FILE = RUNTIME_DIR / "market_fetch_history.jsonl"
MAX_HISTORY_ENTRIES = 200


def record_fetch(result: Dict[str, Any]) -> None:
    """Append a fetch job result to the history log."""
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    entry = {
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "saved_klines": result.get("saved_klines", 0),
        "symbol_names": result.get("symbol_names", []),
        "timeframes": result.get("timeframes", []),
        "symbol_results": result.get("symbol_results", []),
    }
    with open(FETCH_HISTORY_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")

    # Trim to last MAX_HISTORY_ENTRIES
    try:
        lines = FETCH_HISTORY_FILE.read_text().splitlines()
        if len(lines) > MAX_HISTORY_ENTRIES:
            FETCH_HISTORY_FILE.write_text("\n".join(lines[-MAX_HISTORY_ENTRIES:]) + "\n")
    except Exception:
        pass


def read_fetch_history(limit: int = 20) -> List[Dict[str, Any]]:
    """Return the most recent fetch history entries, newest first."""
    if not FETCH_HISTORY_FILE.exists():
        return []
    try:
        lines = FETCH_HISTORY_FILE.read_text().splitlines()
        entries = [json.loads(line) for line in lines if line.strip()]
        return list(reversed(entries[-limit:]))
    except Exception:
        return []
