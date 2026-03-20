import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from typing import Optional

RUNTIME_DIR = Path("runtime")
SOAK_HISTORY_FILE = RUNTIME_DIR / "soak_validation_history.jsonl"
SOAK_ACCEPTANCE_TARGET_HOURS = 24 * 7


def _parse_checked_at(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    return datetime.fromisoformat(value)


def build_soak_validation_report() -> dict[str, Any]:
    from app.validation.soak_report import build_soak_validation_report as _build_soak_validation_report

    return _build_soak_validation_report()


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


def build_soak_history_summary() -> dict[str, Any]:
    if not SOAK_HISTORY_FILE.exists():
        return {
            "status": "missing",
            "record_count": 0,
            "acceptance_target_hours": SOAK_ACCEPTANCE_TARGET_HOURS,
            "continuous_span_hours": 0.0,
            "remaining_hours_to_target": float(SOAK_ACCEPTANCE_TARGET_HOURS),
            "distinct_utc_dates": 0,
            "ok_count": 0,
            "degraded_count": 0,
            "error_count": 0,
            "meets_weekly_target": False,
        }

    records = [json.loads(line) for line in SOAK_HISTORY_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not records:
        return {
            "status": "empty",
            "record_count": 0,
            "acceptance_target_hours": SOAK_ACCEPTANCE_TARGET_HOURS,
            "continuous_span_hours": 0.0,
            "remaining_hours_to_target": float(SOAK_ACCEPTANCE_TARGET_HOURS),
            "distinct_utc_dates": 0,
            "ok_count": 0,
            "degraded_count": 0,
            "error_count": 0,
            "meets_weekly_target": False,
        }

    checked_times = [_parse_checked_at(record.get("checked_at")) for record in records]
    checked_times = [checked_at for checked_at in checked_times if checked_at is not None]
    first_checked_at = min(checked_times) if checked_times else None
    last_checked_at = max(checked_times) if checked_times else None
    continuous_span_hours = (
        round((last_checked_at - first_checked_at).total_seconds() / 3600, 2)
        if first_checked_at is not None and last_checked_at is not None
        else 0.0
    )
    status_counts = {
        "ok": sum(1 for record in records if record.get("status") == "ok"),
        "degraded": sum(1 for record in records if record.get("status") == "degraded"),
        "error": sum(1 for record in records if record.get("status") == "error"),
    }
    distinct_utc_dates = len(
        {
            checked_at.astimezone(timezone.utc).date().isoformat()
            for checked_at in checked_times
        }
    )
    remaining_hours_to_target = max(0.0, round(SOAK_ACCEPTANCE_TARGET_HOURS - continuous_span_hours, 2))

    return {
        "status": "ok",
        "record_count": len(records),
        "first_checked_at": first_checked_at.isoformat() if first_checked_at is not None else None,
        "last_checked_at": last_checked_at.isoformat() if last_checked_at is not None else None,
        "acceptance_target_hours": SOAK_ACCEPTANCE_TARGET_HOURS,
        "continuous_span_hours": continuous_span_hours,
        "remaining_hours_to_target": remaining_hours_to_target,
        "distinct_utc_dates": distinct_utc_dates,
        "ok_count": status_counts["ok"],
        "degraded_count": status_counts["degraded"],
        "error_count": status_counts["error"],
        "meets_weekly_target": continuous_span_hours >= SOAK_ACCEPTANCE_TARGET_HOURS,
    }
