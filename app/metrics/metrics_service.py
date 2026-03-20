"""
Operational metrics aggregation service.

Queries the local DB to produce a snapshot useful for monitoring:
  - Signal throughput and composition
  - Risk evaluation approve/reject rate and top rejection reasons
  - Execution fill count and notional volume
  - Realized PnL (today + rolling 7-day)
  - Queue job throughput and average latency

All windows are configurable; the default period is 24 hours.
"""
from typing import Any, Dict, List

from app.core.db import DBConnection, table_exists


def _risk_summary(connection: DBConnection, period_hours: int) -> Dict[str, Any]:
    if not table_exists(connection, "risk_events"):
        return {"total": 0, "approved": 0, "rejected": 0, "reject_rate": None, "top_rejection_reasons": []}

    row = connection.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN decision = 'APPROVED' THEN 1 ELSE 0 END) AS approved,
            SUM(CASE WHEN decision = 'REJECTED' THEN 1 ELSE 0 END) AS rejected
        FROM risk_events
        WHERE created_at >= datetime('now', ? || ' hours');
        """,
        (f"-{period_hours}",),
    ).fetchone()
    total = int(row[0] or 0)
    approved = int(row[1] or 0)
    rejected = int(row[2] or 0)
    reject_rate = round(rejected / total, 4) if total > 0 else None

    reason_rows = connection.execute(
        """
        SELECT reason, COUNT(*) AS cnt
        FROM risk_events
        WHERE decision = 'REJECTED'
          AND created_at >= datetime('now', ? || ' hours')
        GROUP BY reason
        ORDER BY cnt DESC
        LIMIT 5;
        """,
        (f"-{period_hours}",),
    ).fetchall()
    top_reasons = [{"reason": r[0], "count": int(r[1])} for r in reason_rows]

    return {
        "total": total,
        "approved": approved,
        "rejected": rejected,
        "reject_rate": reject_rate,
        "top_rejection_reasons": top_reasons,
    }


def _signal_summary(connection: DBConnection, period_hours: int) -> Dict[str, Any]:
    if not table_exists(connection, "signals"):
        return {"total": 0, "by_type": {}}

    rows = connection.execute(
        """
        SELECT signal_type, COUNT(*) AS cnt
        FROM signals
        WHERE created_at >= datetime('now', ? || ' hours')
        GROUP BY signal_type;
        """,
        (f"-{period_hours}",),
    ).fetchall()
    by_type = {r[0]: int(r[1]) for r in rows}
    return {"total": sum(by_type.values()), "by_type": by_type}


def _execution_summary(connection: DBConnection, period_hours: int) -> Dict[str, Any]:
    if not table_exists(connection, "fills"):
        return {"fills": 0, "fill_volume": None, "orders_total": 0}

    fill_row = connection.execute(
        """
        SELECT COUNT(*), SUM(qty * price)
        FROM fills
        WHERE created_at >= datetime('now', ? || ' hours');
        """,
        (f"-{period_hours}",),
    ).fetchone()
    fills = int(fill_row[0] or 0)
    fill_volume = round(float(fill_row[1]), 4) if fill_row[1] is not None else None

    orders_total = 0
    if table_exists(connection, "orders"):
        orders_row = connection.execute("SELECT COUNT(*) FROM orders;").fetchone()
        orders_total = int(orders_row[0] or 0)

    return {"fills": fills, "fill_volume": fill_volume, "orders_total": orders_total}


def _pnl_summary(connection: DBConnection) -> Dict[str, Any]:
    if not table_exists(connection, "daily_realized_pnl"):
        return {"today": None, "last_7_days": []}

    rows = connection.execute(
        """
        SELECT pnl_date, symbol, realized_pnl
        FROM daily_realized_pnl
        ORDER BY pnl_date DESC, symbol ASC
        LIMIT 21;
        """
    ).fetchall()
    last_7: List[Dict[str, Any]] = [
        {"date": r[0], "symbol": r[1], "realized_pnl": round(float(r[2]), 6)}
        for r in rows
    ]
    today_total = None
    if last_7:
        latest_date = last_7[0]["date"]
        today_rows = [r for r in last_7 if r["date"] == latest_date]
        today_total = round(sum(r["realized_pnl"] for r in today_rows), 6)

    return {"today": today_total, "last_7_days": last_7}


def _queue_summary(connection: DBConnection, period_hours: int) -> Dict[str, Any]:
    if not table_exists(connection, "job_queue"):
        return {"avg_job_duration_seconds": None, "completed": 0, "failed": 0}

    dur_row = connection.execute(
        """
        SELECT AVG((julianday(completed_at) - julianday(started_at)) * 86400)
        FROM job_queue
        WHERE status = 'completed'
          AND started_at IS NOT NULL
          AND completed_at IS NOT NULL
          AND completed_at >= datetime('now', ? || ' hours');
        """,
        (f"-{period_hours}",),
    ).fetchone()
    avg_duration = round(float(dur_row[0]), 3) if dur_row[0] is not None else None

    count_rows = connection.execute(
        """
        SELECT status, COUNT(*)
        FROM job_queue
        WHERE completed_at >= datetime('now', ? || ' hours')
          AND status IN ('completed', 'failed')
        GROUP BY status;
        """,
        (f"-{period_hours}",),
    ).fetchall()
    counts = {r[0]: int(r[1]) for r in count_rows}

    return {
        "avg_job_duration_seconds": avg_duration,
        "completed": counts.get("completed", 0),
        "failed": counts.get("failed", 0),
    }


def build_metrics(connection: DBConnection, period_hours: int = 24) -> Dict[str, Any]:
    """Return an operational metrics snapshot for the given look-back window."""
    return {
        "period_hours": period_hours,
        "signals": _signal_summary(connection, period_hours),
        "risk": _risk_summary(connection, period_hours),
        "execution": _execution_summary(connection, period_hours),
        "pnl": _pnl_summary(connection),
        "queue": _queue_summary(connection, period_hours),
    }
