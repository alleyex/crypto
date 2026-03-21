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


def _signal_quality_check(connection: DBConnection) -> dict[str, Any]:
    """Analyse signal / risk / fill pipeline quality for soak reporting."""
    empty: dict[str, Any] = {
        "total_signals": 0,
        "buy_count": 0,
        "sell_count": 0,
        "hold_count": 0,
        "buy_rate": None,
        "sell_rate": None,
        "actionable_rate": None,
        "approval_rate": None,
        "duplicate_rejection_rate": None,
        "execution_rate": None,
        "by_strategy": {},
    }
    if not table_exists(connection, "signals"):
        return empty

    # Signal type distribution
    sig_rows = connection.execute(
        "SELECT signal_type, COUNT(*) FROM signals GROUP BY signal_type;"
    ).fetchall()
    counts = {row[0]: int(row[1]) for row in sig_rows}
    buy_count = counts.get("BUY", 0)
    sell_count = counts.get("SELL", 0)
    hold_count = counts.get("HOLD", 0)
    total_signals = buy_count + sell_count + hold_count
    actionable = buy_count + sell_count

    buy_rate = round(buy_count / total_signals, 4) if total_signals > 0 else None
    sell_rate = round(sell_count / total_signals, 4) if total_signals > 0 else None
    actionable_rate = round(actionable / total_signals, 4) if total_signals > 0 else None

    # Risk event approval rate (BUY/SELL signals only)
    approval_rate = None
    duplicate_rejection_rate = None
    if table_exists(connection, "risk_events"):
        risk_rows = connection.execute(
            "SELECT decision, COUNT(*) FROM risk_events GROUP BY decision;"
        ).fetchall()
        risk_counts = {row[0]: int(row[1]) for row in risk_rows}
        approved = risk_counts.get("APPROVED", 0)
        rejected = risk_counts.get("REJECTED", 0)
        total_risk = approved + rejected
        if total_risk > 0:
            approval_rate = round(approved / total_risk, 4)

        dup_row = connection.execute(
            "SELECT COUNT(*) FROM risk_events WHERE reason = 'Duplicate signal type.';"
        ).fetchone()
        dup_count = int(dup_row[0]) if dup_row else 0
        if total_risk > 0:
            duplicate_rejection_rate = round(dup_count / total_risk, 4)

    # Execution rate: fills / approved risk events
    execution_rate = None
    if table_exists(connection, "risk_events") and table_exists(connection, "fills"):
        approved_row = connection.execute(
            "SELECT COUNT(*) FROM risk_events WHERE decision = 'APPROVED';"
        ).fetchone()
        fill_row = connection.execute("SELECT COUNT(*) FROM fills;").fetchone()
        approved_count = int(approved_row[0]) if approved_row else 0
        fill_count = int(fill_row[0]) if fill_row else 0
        if approved_count > 0:
            execution_rate = round(fill_count / approved_count, 4)

    # Per-strategy signal and approval breakdown
    by_strategy: dict[str, Any] = {}
    strat_rows = connection.execute(
        "SELECT strategy_name, signal_type, COUNT(*) FROM signals GROUP BY strategy_name, signal_type;"
    ).fetchall()
    for strategy_name, signal_type, cnt in strat_rows:
        if strategy_name not in by_strategy:
            by_strategy[strategy_name] = {"signals": 0, "buy": 0, "sell": 0, "hold": 0, "approved": 0, "filled": 0}
        by_strategy[strategy_name]["signals"] += int(cnt)
        key = signal_type.lower()
        if key in ("buy", "sell", "hold"):
            by_strategy[strategy_name][key] += int(cnt)

    if table_exists(connection, "risk_events"):
        appr_rows = connection.execute(
            "SELECT strategy_name, COUNT(*) FROM risk_events WHERE decision = 'APPROVED' GROUP BY strategy_name;"
        ).fetchall()
        for strategy_name, cnt in appr_rows:
            if strategy_name in by_strategy:
                by_strategy[strategy_name]["approved"] = int(cnt)

    if table_exists(connection, "fills") and table_exists(connection, "orders"):
        fill_rows = connection.execute(
            """
            SELECT o.strategy_name, COUNT(*)
            FROM fills f
            JOIN orders o ON o.id = f.order_id
            GROUP BY o.strategy_name;
            """
        ).fetchall()
        for strategy_name, cnt in fill_rows:
            if strategy_name in by_strategy:
                by_strategy[strategy_name]["filled"] = int(cnt)

    return {
        "total_signals": total_signals,
        "buy_count": buy_count,
        "sell_count": sell_count,
        "hold_count": hold_count,
        "buy_rate": buy_rate,
        "sell_rate": sell_rate,
        "actionable_rate": actionable_rate,
        "approval_rate": approval_rate,
        "duplicate_rejection_rate": duplicate_rejection_rate,
        "execution_rate": execution_rate,
        "by_strategy": by_strategy,
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
        signal_quality = _signal_quality_check(connection)
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
    sq = signal_quality
    if sq["total_signals"] > 0 and sq["actionable_rate"] is not None and sq["actionable_rate"] < 0.05:
        issues.append(
            f"Signal actionable rate is very low: {sq['actionable_rate']:.1%} "
            f"(only {sq['buy_count'] + sq['sell_count']} BUY/SELL out of {sq['total_signals']} signals)."
        )
    if sq["approval_rate"] is not None and sq["approval_rate"] < 0.1:
        issues.append(
            f"Risk approval rate is very low: {sq['approval_rate']:.1%}. "
            "Most actionable signals are being rejected."
        )
    if sq["execution_rate"] is not None and sq["execution_rate"] < 0.5:
        issues.append(
            f"Execution rate is low: {sq['execution_rate']:.1%} "
            "(approved risk events are not resulting in fills)."
        )
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
        "signal_quality": signal_quality,
        "history_summary": build_soak_history_summary(),
    }
