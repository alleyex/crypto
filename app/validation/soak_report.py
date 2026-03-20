from datetime import datetime, timezone
from typing import Any
from typing import Optional

from app.core.db import DBConnection
from app.core.db import get_connection
from app.core.db import parse_db_timestamp
from app.core.db import table_exists
from app.core.settings import SOAK_ACTIVITY_STALENESS_SECONDS
from app.scheduler.control import read_scheduler_log
from app.system.heartbeat import get_heartbeats
from app.validation.soak_history import build_soak_history_summary


TRACKED_TABLES = (
    "candles",
    "signals",
    "risk_events",
    "orders",
    "fills",
    "positions",
    "pnl_snapshots",
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _count_rows(connection: DBConnection, table_name: str) -> int:
    if not table_exists(connection, table_name):
        return 0
    row = connection.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()
    return int(row[0]) if row is not None else 0


def _latest_timestamp(
    connection: DBConnection,
    table_name: str,
    column: str = "created_at",
) -> Optional[str]:
    if not table_exists(connection, table_name):
        return None
    row = connection.execute(
        f"SELECT {column} FROM {table_name} ORDER BY {column} DESC LIMIT 1;"
    ).fetchone()
    if row is None:
        return None
    return str(row[0])
def _positions_summary(connection: DBConnection) -> dict[str, float]:
    if not table_exists(connection, "positions"):
        return {"open_symbols": 0, "total_qty": 0.0, "total_realized_pnl": 0.0}

    row = connection.execute(
        """
        SELECT
            COUNT(*) AS open_symbols,
            COALESCE(SUM(qty), 0),
            COALESCE(SUM(realized_pnl), 0)
        FROM positions
        WHERE qty > 0;
        """
    ).fetchone()

    realized_row = connection.execute(
        "SELECT COALESCE(SUM(realized_pnl), 0) FROM positions;"
    ).fetchone()

    return {
        "open_symbols": int(row[0]) if row is not None else 0,
        "total_qty": float(row[1]) if row is not None else 0.0,
        "total_realized_pnl": float(realized_row[0]) if realized_row is not None else 0.0,
    }


def _scheduler_log_summary(lines: list[str]) -> dict[str, Any]:
    last_line = lines[-1] if lines else None
    stopped_by_flag = bool(last_line and "scheduler stopped by flag" in last_line.lower())
    recent_errors = [
        line
        for line in lines
        if any(token in line.lower() for token in ("error", "exception", "traceback"))
    ]
    return {
        "line_count": len(lines),
        "last_line": last_line,
        "stopped_by_flag": stopped_by_flag,
        "recent_error_count": len(recent_errors),
        "recent_errors": recent_errors[-5:],
    }


def build_soak_validation_report(log_lines: int = 200) -> dict[str, Any]:
    scheduler_lines = read_scheduler_log(lines=log_lines)
    connection = get_connection()
    try:
        table_counts = {table_name: _count_rows(connection, table_name) for table_name in TRACKED_TABLES}
        latest_activity = {
            "signals": _latest_timestamp(connection, "signals"),
            "risk_events": _latest_timestamp(connection, "risk_events"),
            "orders": _latest_timestamp(connection, "orders"),
            "pnl_snapshots": _latest_timestamp(connection, "pnl_snapshots"),
        }
        positions = _positions_summary(connection)
        heartbeats = get_heartbeats(connection)
    finally:
        connection.close()

    scheduler = _scheduler_log_summary(scheduler_lines)
    issues: list[str] = []

    if scheduler["line_count"] == 0:
        issues.append("Scheduler log is empty.")
    if scheduler["stopped_by_flag"]:
        issues.append("Scheduler is stopped by flag.")
    if scheduler["recent_error_count"] > 0:
        issues.append("Scheduler log contains error markers.")
    if table_counts["candles"] == 0:
        issues.append("No candles stored.")
    if table_counts["signals"] == 0:
        issues.append("No signals generated.")
    if not heartbeats:
        issues.append("No runtime heartbeats recorded yet.")
    for heartbeat in heartbeats:
        if heartbeat["status"] in ("failed", "stopped"):
            issues.append(
                f"{heartbeat['component']} heartbeat is {heartbeat['status']}: {heartbeat['message']}"
            )

    latest_activity_with_age: dict[str, Any] = {}
    for name, created_at in latest_activity.items():
        if created_at is None:
            latest_activity_with_age[name] = None
            continue

        age_seconds = int((_utc_now() - parse_db_timestamp(created_at)).total_seconds())
        latest_activity_with_age[name] = {
            "created_at": created_at,
            "age_seconds": age_seconds,
        }
        if age_seconds > SOAK_ACTIVITY_STALENESS_SECONDS:
            issues.append(
                f"{name} activity is stale: age_seconds={age_seconds}, "
                f"threshold={SOAK_ACTIVITY_STALENESS_SECONDS}."
            )

    status = "ok" if not issues else "degraded"

    return {
        "status": status,
        "checked_at": _utc_now().isoformat(),
        "issues": issues,
        "scheduler": scheduler,
        "table_counts": table_counts,
        "latest_activity": latest_activity_with_age,
        "positions": positions,
        "staleness_threshold_seconds": SOAK_ACTIVITY_STALENESS_SECONDS,
        "heartbeats": heartbeats,
        "history_summary": build_soak_history_summary(),
    }
