from typing import Any, Dict, List, Optional

from app.core.db import DBConnection
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.risk.risk_service import ensure_table as ensure_risk_table
from app.risk.risk_service import evaluate_signal_ids


_UNEVALUATED_SIGNALS_SQL = """
SELECT s.id
FROM signals s
LEFT JOIN risk_events re ON re.signal_id = s.id
WHERE re.id IS NULL
ORDER BY s.id ASC;
"""


def run_risk_job(
    connection: DBConnection,
    signal_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    ensure_positions_table(connection)
    ensure_risk_table(connection)

    if signal_ids is None:
        rows = connection.execute(_UNEVALUATED_SIGNALS_SQL).fetchall()
        signal_ids = [int(row[0]) for row in rows]

    if not signal_ids:
        return {
            "status": "completed",
            "steps": [],
            "terminal_message": "No unevaluated signals found.",
        }

    risk_results = evaluate_signal_ids(connection, signal_ids)
    risk_steps: list[dict[str, Any]] = [{"step": "evaluate_risk", **r} for r in risk_results]

    if not risk_results:
        return {
            "status": "completed",
            "steps": risk_steps,
            "terminal_message": "Pipeline run completed with skipped risk evaluation.",
        }

    return {
        "status": "ok",
        "risk_event_ids": [int(r["id"]) for r in risk_results],
        "steps": risk_steps,
    }
