"""Broker protection health check."""
from typing import Any

from app.core.db import DBConnection, parse_db_timestamp, table_exists, utc_now
from app.core.settings import ORDER_STALENESS_SECONDS, RISK_REJECTION_STREAK_THRESHOLD
from app.execution.exchange_trades import (
    binance_futures_enabled,
    get_binance_futures_position,
)

def _is_expected_rejection_reason(reason: str) -> bool:
    return (
        "Cooldown active" in reason
        or reason == "Signal is HOLD."
        or reason == "Duplicate signal type."
        or reason == "No position available to sell."
        or reason == "Position already matches target."
    )

def broker_protection_check(
    connection: DBConnection,
    execution_backend: dict[str, Any],
    pipeline: dict[str, Any],
) -> dict[str, Any]:
    latest_run = pipeline.get("latest_run", {}) if isinstance(pipeline, dict) else {}
    latest_order = pipeline.get("latest_order") if isinstance(pipeline, dict) else None
    approved_risk_count = (
        int(latest_run.get("approved_risk_count") or 0) if isinstance(latest_run, dict) else 0
    )
    result: dict[str, Any] = {
        "status": "ok",
        "backend": execution_backend.get("backend"),
        "can_execute_orders": bool(execution_backend.get("can_execute_orders")),
        "dry_run": bool(execution_backend.get("dry_run")),
        "placeholder": bool(execution_backend.get("placeholder")),
        "approved_risk_count": approved_risk_count,
        "order_staleness_threshold_seconds": ORDER_STALENESS_SECONDS,
        "risk_rejection_streak_threshold": RISK_REJECTION_STREAK_THRESHOLD,
        "severity": "info",
        "reason_code": None,
        "recommended_action": None,
    }
    if isinstance(latest_order, dict):
        result["latest_order"] = latest_order
    latest_fill = pipeline.get("latest_fill") if isinstance(pipeline, dict) else None
    if isinstance(latest_fill, dict):
        result["latest_fill"] = latest_fill
    latest_risk = pipeline.get("latest_risk") if isinstance(pipeline, dict) else None
    if isinstance(latest_risk, dict):
        result["latest_risk"] = latest_risk
    observed_symbol = None
    observed_strategy = None
    for candidate in (latest_risk, latest_fill, latest_order, latest_run):
        if not isinstance(candidate, dict):
            continue
        observed_symbol = observed_symbol or candidate.get("symbol")
        observed_strategy = observed_strategy or candidate.get("strategy_name")
    if observed_symbol:
        if binance_futures_enabled():
            result["current_position"] = get_binance_futures_position(str(observed_symbol))
        elif table_exists(connection, "positions"):
            position_row = connection.execute(
                """
                SELECT qty, avg_price, realized_pnl, updated_at
                FROM positions
                WHERE symbol = ?
                LIMIT 1;
                """,
                (observed_symbol,),
            ).fetchone()
            if position_row is not None:
                result["current_position"] = {
                    "symbol": observed_symbol,
                    "qty": position_row[0],
                    "avg_price": position_row[1],
                    "realized_pnl": position_row[2],
                    "updated_at": position_row[3],
                    "age_seconds": int(
                        (utc_now() - parse_db_timestamp(position_row[3])).total_seconds()
                    ),
                }
            else:
                result["current_position"] = {
                    "symbol": observed_symbol,
                    "qty": 0.0,
                    "avg_price": 0.0,
                    "realized_pnl": 0.0,
                    "updated_at": None,
                    "age_seconds": None,
                }
    if observed_symbol and table_exists(connection, "risk_events"):
        if observed_strategy:
            recent_rejection_rows = connection.execute(
                """
                SELECT signal_type, reason, created_at
                FROM risk_events
                WHERE symbol = ?
                  AND strategy_name = ?
                  AND decision = 'REJECTED'
                ORDER BY id DESC
                LIMIT 5;
                """,
                (observed_symbol, observed_strategy),
            ).fetchall()
        else:
            recent_rejection_rows = connection.execute(
                """
                SELECT signal_type, reason, created_at
                FROM risk_events
                WHERE symbol = ?
                  AND decision = 'REJECTED'
                ORDER BY id DESC
                LIMIT 5;
                """,
                (observed_symbol,),
            ).fetchall()
        result["recent_rejection_reasons"] = [
            {
                "signal_type": row[0],
                "reason": row[1],
                "created_at": row[2],
                "age_seconds": int(
                    (utc_now() - parse_db_timestamp(row[2])).total_seconds()
                ),
            }
            for row in recent_rejection_rows
        ]

    if not result["can_execute_orders"] and approved_risk_count > 0:
        result["status"] = "degraded"
        result["severity"] = "critical"
        result["reason_code"] = "backend_cannot_execute"
        result["reason"] = "Execution backend cannot execute approved orders."
        result["recommended_action"] = "switch_to_paper_backend"
        return result

    if result["placeholder"] and approved_risk_count > 0:
        result["status"] = "degraded"
        result["severity"] = "high"
        result["reason_code"] = "placeholder_backend_pending_orders"
        result["reason"] = "Execution backend is placeholder while approved orders are pending."
        result["recommended_action"] = "switch_to_paper_backend"
        return result

    if isinstance(latest_order, dict):
        latest_order_status = str(latest_order.get("status") or "").upper()
        latest_order_age = latest_order.get("age_seconds")
        latest_order_has_fill = bool(latest_order.get("has_fill"))
        if (
            latest_order_status in {"NEW", "PENDING", "SUBMITTED", "PARTIALLY_FILLED"}
            and not latest_order_has_fill
            and latest_order_age is not None
            and int(latest_order_age) > ORDER_STALENESS_SECONDS
        ):
            result["status"] = "degraded"
            result["severity"] = "high" if latest_order_status == "PARTIALLY_FILLED" else "medium"
            result["reason_code"] = f"stale_order_{latest_order_status.lower()}"
            result["reason"] = "Latest order is stale and still not terminal."
            result["recommended_action"] = "inspect_and_reconcile_orders"
            return result

    unfilled_order_count = (
        int(pipeline.get("unfilled_order_count") or 0) if isinstance(pipeline, dict) else 0
    )
    if unfilled_order_count > 0:
        result["status"] = "degraded"
        result["severity"] = "high"
        result["reason_code"] = "unfilled_orders_detected"
        result["reason"] = f"{unfilled_order_count} order(s) have no corresponding fill."
        result["recommended_action"] = "inspect_and_reconcile_orders"
        result["unfilled_order_count"] = unfilled_order_count
        return result

    rejection_rows = connection.execute(
        """
        SELECT decision, reason
        FROM risk_events
        ORDER BY id DESC
        LIMIT ?;
        """,
        (RISK_REJECTION_STREAK_THRESHOLD,),
    ).fetchall()
    if len(rejection_rows) >= RISK_REJECTION_STREAK_THRESHOLD and all(
        str(row[0]).upper() == "REJECTED" for row in rejection_rows
    ):
        latest_rejection_reason = str(rejection_rows[0][1] or "")
        rejection_reasons = [str(row[1] or "") for row in rejection_rows]
        if all(_is_expected_rejection_reason(reason) for reason in rejection_reasons):
            result["expected_rejected_risk_streak"] = len(rejection_rows)
            result["expected_latest_rejection_reason"] = latest_rejection_reason
            return result
        result["status"] = "degraded"
        result["rejected_risk_streak"] = len(rejection_rows)
        result["anomalous_rejected_risk_streak"] = len(rejection_rows)
        result["latest_rejection_reason"] = latest_rejection_reason
        if "Daily loss limit breached" in latest_rejection_reason:
            result["severity"] = "critical"
            result["reason_code"] = "risk_reject_daily_loss_limit"
            result["recommended_action"] = "enable_kill_switch"
        elif "Cooldown active" in latest_rejection_reason:
            result["severity"] = "medium"
            result["reason_code"] = "risk_reject_cooldown_streak"
            result["recommended_action"] = "pause_scheduler"
        else:
            result["severity"] = "medium"
            result["reason_code"] = "risk_reject_streak"
            result["recommended_action"] = "inspect_risk_rules"
        result["reason"] = "Recent risk evaluations are repeatedly rejected."
        return result

    return result
