import json
from pathlib import Path
from typing import Any

from app.validation.soak_report import build_soak_validation_report


RUNTIME_DIR = Path("runtime")
SOAK_HISTORY_FILE = RUNTIME_DIR / "soak_validation_history.jsonl"


def record_soak_validation_snapshot() -> dict[str, Any]:
    report = build_soak_validation_report()
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    with SOAK_HISTORY_FILE.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(report, sort_keys=True) + "\n")
    return report


def read_soak_validation_history(limit: int = 20) -> list[dict[str, Any]]:
    if not SOAK_HISTORY_FILE.exists():
        return []

    records: list[dict[str, Any]] = []
    lines = SOAK_HISTORY_FILE.read_text(encoding="utf-8").splitlines()
    for line in reversed(lines[-limit:]):
        records.append(json.loads(line))
    return records
