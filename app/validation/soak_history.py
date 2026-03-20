import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from typing import Optional

RUNTIME_DIR = Path("runtime")
SOAK_HISTORY_FILE = RUNTIME_DIR / "soak_validation_history.jsonl"
SOAK_ACCEPTANCE_TARGET_HOURS = 24 * 7
# Snapshot interval assumed when estimating accumulated hours from ok count.
# Must match the scheduler's --interval setting (default 60 s).
SOAK_SNAPSHOT_INTERVAL_SECONDS = 60
# Minimum accumulated healthy operation hours required for paper trading
# readiness.  Lower than the wall-clock span target to account for planned
# restarts and maintenance windows on a dev machine.
SOAK_ACCUMULATED_TARGET_HOURS = 72


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


def _compute_longest_ok_streak_hours(records: list[dict[str, Any]]) -> float:
    """Return the length in hours of the longest consecutive ok run."""
    longest = 0
    current = 0
    for record in records:
        if record.get("status") == "ok":
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return round(longest * SOAK_SNAPSHOT_INTERVAL_SECONDS / 3600, 2)


def build_soak_history_summary() -> dict[str, Any]:
    _empty: dict[str, Any] = {
        "record_count": 0,
        "acceptance_target_hours": SOAK_ACCEPTANCE_TARGET_HOURS,
        "accumulated_target_hours": SOAK_ACCUMULATED_TARGET_HOURS,
        "continuous_span_hours": 0.0,
        "accumulated_ok_hours": 0.0,
        "longest_ok_streak_hours": 0.0,
        "remaining_span_hours": float(SOAK_ACCEPTANCE_TARGET_HOURS),
        "remaining_accumulated_hours": float(SOAK_ACCUMULATED_TARGET_HOURS),
        "ok_rate": None,
        "distinct_utc_dates": 0,
        "ok_count": 0,
        "degraded_count": 0,
        "error_count": 0,
        "meets_weekly_target": False,
        "meets_accumulated_target": False,
    }

    if not SOAK_HISTORY_FILE.exists():
        return {"status": "missing", **_empty}

    records = [json.loads(line) for line in SOAK_HISTORY_FILE.read_text(encoding="utf-8").splitlines() if line.strip()]
    if not records:
        return {"status": "empty", **_empty}

    checked_times = [_parse_checked_at(record.get("checked_at")) for record in records]
    checked_times = [t for t in checked_times if t is not None]
    first_checked_at = min(checked_times) if checked_times else None
    last_checked_at = max(checked_times) if checked_times else None

    continuous_span_hours = (
        round((last_checked_at - first_checked_at).total_seconds() / 3600, 2)
        if first_checked_at is not None and last_checked_at is not None
        else 0.0
    )

    ok_count = sum(1 for r in records if r.get("status") == "ok")
    degraded_count = sum(1 for r in records if r.get("status") == "degraded")
    error_count = sum(1 for r in records if r.get("status") == "error")

    accumulated_ok_hours = round(ok_count * SOAK_SNAPSHOT_INTERVAL_SECONDS / 3600, 2)
    longest_ok_streak_hours = _compute_longest_ok_streak_hours(records)
    ok_rate = round(ok_count / len(records), 4) if records else None

    distinct_utc_dates = len(
        {t.astimezone(timezone.utc).date().isoformat() for t in checked_times}
    )

    meets_weekly_target = continuous_span_hours >= SOAK_ACCEPTANCE_TARGET_HOURS
    meets_accumulated_target = accumulated_ok_hours >= SOAK_ACCUMULATED_TARGET_HOURS

    return {
        "status": "ok",
        "record_count": len(records),
        "first_checked_at": first_checked_at.isoformat() if first_checked_at is not None else None,
        "last_checked_at": last_checked_at.isoformat() if last_checked_at is not None else None,
        "acceptance_target_hours": SOAK_ACCEPTANCE_TARGET_HOURS,
        "accumulated_target_hours": SOAK_ACCUMULATED_TARGET_HOURS,
        "continuous_span_hours": continuous_span_hours,
        "accumulated_ok_hours": accumulated_ok_hours,
        "longest_ok_streak_hours": longest_ok_streak_hours,
        "remaining_span_hours": max(0.0, round(SOAK_ACCEPTANCE_TARGET_HOURS - continuous_span_hours, 2)),
        "remaining_accumulated_hours": max(0.0, round(SOAK_ACCUMULATED_TARGET_HOURS - accumulated_ok_hours, 2)),
        "ok_rate": ok_rate,
        "distinct_utc_dates": distinct_utc_dates,
        "ok_count": ok_count,
        "degraded_count": degraded_count,
        "error_count": error_count,
        "meets_weekly_target": meets_weekly_target,
        "meets_accumulated_target": meets_accumulated_target,
    }
