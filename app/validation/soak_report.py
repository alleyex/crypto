import sqlite3
from datetime import datetime, timezone
from typing import Any
from typing import Optional

from app.core.db import get_connection
from app.scheduler.control import read_scheduler_log


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


def _table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1;",
        (table_name,),
    ).fetchone()
    return row is not None


def _count_rows(connection: sqlite3.Connection, table_name: str) -> int:
    if not _table_exists(connection, table_name):
        return 0
    row = connection.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()
    return int(row[0]) if row is not None else 0


def _latest_timestamp(
    connection: sqlite3.Connection,
    table_name: str,
    column: str = "created_at",
) -> Optional[str]:
    if not _table_exists(connection, table_name):
        return None
    row = connection.execute(
        f"SELECT {column} FROM {table_name} ORDER BY {column} DESC LIMIT 1;"
    ).fetchone()
    if row is None:
        return None
    return str(row[0])


def _positions_summary(connection: sqlite3.Connection) -> dict[str, float]:
    if not _table_exists(connection, "positions"):
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
    recent_errors = [
        line
        for line in lines
        if any(token in line.lower() for token in ("error", "exception", "traceback"))
    ]
    return {
        "line_count": len(lines),
        "last_line": lines[-1] if lines else None,
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
    finally:
        connection.close()

    scheduler = _scheduler_log_summary(scheduler_lines)
    issues: list[str] = []

    if scheduler["line_count"] == 0:
        issues.append("Scheduler log is empty.")
    if scheduler["recent_error_count"] > 0:
        issues.append("Scheduler log contains error markers.")
    if table_counts["candles"] == 0:
        issues.append("No candles stored.")
    if table_counts["signals"] == 0:
        issues.append("No signals generated.")

    status = "ok" if not issues else "degraded"

    return {
        "status": status,
        "checked_at": _utc_now().isoformat(),
        "issues": issues,
        "scheduler": scheduler,
        "table_counts": table_counts,
        "latest_activity": latest_activity,
        "positions": positions,
    }
