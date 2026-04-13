"""Shared SQL queries used by execution broker modules."""

from app.core.db import DBConnection
from app.core.utils import dedup_ordered

SELECT_LATEST_RISK_SQL = """
SELECT
    re.id,
    re.signal_id,
    re.symbol,
    re.timeframe,
    re.strategy_name,
    re.signal_type,
    re.decision
FROM risk_events re
ORDER BY re.id DESC
LIMIT 1;
"""

SELECT_RISK_BY_ID_SQL = """
SELECT
    re.id,
    re.signal_id,
    re.symbol,
    re.timeframe,
    re.strategy_name,
    re.signal_type,
    re.decision
FROM risk_events re
WHERE re.id = ?;
"""


def select_pending_approved_risk_ids(
    connection: DBConnection,
    symbol_names: list[str] | None = None,
) -> list[int]:
    """Return IDs of APPROVED risk events that have no matching order yet."""
    query = """
    SELECT re.id
    FROM risk_events re
    LEFT JOIN orders o ON o.risk_event_id = re.id
    WHERE re.decision = 'APPROVED'
      AND o.id IS NULL
    """
    params: list[str] = []
    filtered = dedup_ordered(symbol_names or [])
    if filtered:
        placeholders = ", ".join("?" for _ in filtered)
        query += f" AND re.symbol IN ({placeholders})"
        params.extend(filtered)
    query += " ORDER BY re.id ASC;"
    rows = connection.execute(query, tuple(params)).fetchall()
    return [int(row[0]) for row in rows]
