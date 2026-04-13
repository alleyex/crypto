"""Pipeline health check."""
from typing import Any

from app.core.db import DBConnection, parse_db_timestamp, table_exists, utc_now
from app.execution.adapter import get_execution_adapter_name, get_execution_backend_status
from app.system.heartbeat import get_heartbeat_row

def pipeline_check(connection: DBConnection) -> dict[str, Any]:
    required_tables = ("signals", "risk_events", "orders")
    missing_tables = [
        table_name
        for table_name in required_tables
        if not table_exists(connection, table_name)
    ]
    if missing_tables:
        return {
            "status": "degraded",
            "reason": f"Pipeline tables missing: {', '.join(missing_tables)}.",
        }

    latest_signal = connection.execute(
        """
        SELECT signal_type, created_at
        FROM signals
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    latest_risk = connection.execute(
        """
        SELECT symbol, strategy_name, signal_type, decision, reason, created_at
        FROM risk_events
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    latest_order = connection.execute(
        """
        SELECT id, side, symbol, qty, status, created_at
        FROM orders
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    latest_fill = connection.execute(
        """
        SELECT symbol, side, qty, price, created_at
        FROM fills
        ORDER BY id DESC
        LIMIT 1;
        """
    ).fetchone()
    unfilled_order_count = connection.execute(
        """
        SELECT COUNT(*)
        FROM orders o
        LEFT JOIN fills f ON f.order_id = o.id
        WHERE f.id IS NULL
          AND o.status NOT IN ('CANCELLED', 'REJECTED', 'EXPIRED');
        """
    ).fetchone()
    pipeline_heartbeat = get_heartbeat_row(connection, "pipeline")

    if (
        latest_signal is None
        and latest_risk is None
        and latest_order is None
        and pipeline_heartbeat is None
    ):
        return {
            "status": "degraded",
            "reason": "No pipeline activity found yet.",
        }

    result: dict[str, Any] = {"status": "ok"}
    if pipeline_heartbeat is not None:
        hb_payload = pipeline_heartbeat["payload"]
        result["latest_run"] = {
            "status": pipeline_heartbeat["status"],
            "message": pipeline_heartbeat["message"],
            "created_at": pipeline_heartbeat["last_seen_at"],
            "age_seconds": int(
                (utc_now() - parse_db_timestamp(pipeline_heartbeat["last_seen_at"])).total_seconds()
            ),
            "step_count": hb_payload.get("step_count"),
            "strategy_name": hb_payload.get("strategy_name"),
            "strategy_names": hb_payload.get("strategy_names", []),
            "symbol_names": hb_payload.get("symbol_names", []),
            "execution_backend": hb_payload.get("execution_backend")
            or get_execution_adapter_name(),
            "execution_backend_status": hb_payload.get("execution_backend_status")
            or get_execution_backend_status(),
            "generated_signal_count": hb_payload.get("generated_signal_count"),
            "approved_risk_count": hb_payload.get("approved_risk_count"),
            "rejected_risk_count": hb_payload.get("rejected_risk_count"),
            "filled_execution_count": hb_payload.get("filled_execution_count"),
        }
    if latest_signal is not None:
        result["latest_signal"] = {
            "signal_type": latest_signal[0],
            "created_at": latest_signal[1],
            "age_seconds": int(
                (utc_now() - parse_db_timestamp(latest_signal[1])).total_seconds()
            ),
        }
    if latest_risk is not None:
        result["latest_risk"] = {
            "symbol": latest_risk[0],
            "strategy_name": latest_risk[1],
            "signal_type": latest_risk[2],
            "decision": latest_risk[3],
            "reason": latest_risk[4],
            "created_at": latest_risk[5],
            "age_seconds": int(
                (utc_now() - parse_db_timestamp(latest_risk[5])).total_seconds()
            ),
        }
    if latest_order is not None:
        latest_order_id = int(latest_order[0])
        latest_order_has_fill = (
            connection.execute(
                """
                SELECT 1
                FROM fills
                WHERE order_id = ?
                LIMIT 1;
                """,
                (latest_order_id,),
            ).fetchone()
            is not None
        )
        result["latest_order"] = {
            "id": latest_order_id,
            "side": latest_order[1],
            "symbol": latest_order[2],
            "qty": latest_order[3],
            "status": latest_order[4],
            "created_at": latest_order[5],
            "age_seconds": int(
                (utc_now() - parse_db_timestamp(latest_order[5])).total_seconds()
            ),
            "has_fill": latest_order_has_fill,
        }
    if latest_fill is not None:
        result["latest_fill"] = {
            "symbol": latest_fill[0],
            "side": latest_fill[1],
            "qty": latest_fill[2],
            "price": latest_fill[3],
            "created_at": latest_fill[4],
            "age_seconds": int(
                (utc_now() - parse_db_timestamp(latest_fill[4])).total_seconds()
            ),
        }
    result["unfilled_order_count"] = int(unfilled_order_count[0]) if unfilled_order_count else 0
    return result
