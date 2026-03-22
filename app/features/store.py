"""Feature vector persistence service.

Stores pre-computed feature vectors in ``feature_vectors`` table so the same
computation is available for both offline training and live inference without
re-computing on every request.

Schema (created by migration 026):

    feature_vectors(
        id          INTEGER PRIMARY KEY,
        symbol      TEXT    NOT NULL,
        timeframe   TEXT    NOT NULL,
        open_time   INTEGER NOT NULL,
        feature_set TEXT    NOT NULL DEFAULT 'v1',
        features_json TEXT  NOT NULL,
        created_at  TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (symbol, timeframe, open_time, feature_set)
    )
"""

import json
from typing import Any, Dict, List, Optional

from app.core.db import DBConnection, fetch_all_as_dicts

from app.features.compute import (
    FEATURE_SET_VERSION,
    compute_features_for_candles,
)

_UPSERT_SQL = """
INSERT INTO feature_vectors (symbol, timeframe, open_time, feature_set, features_json)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (symbol, timeframe, open_time, feature_set)
DO UPDATE SET features_json = excluded.features_json,
              created_at    = CURRENT_TIMESTAMP;
"""

_SELECT_SQL = """
SELECT id, symbol, timeframe, open_time, feature_set, features_json, created_at
FROM feature_vectors
WHERE symbol = ? AND timeframe = ? AND feature_set = ?
{extra_where}
ORDER BY open_time {order}
LIMIT ? OFFSET ?;
"""

_COUNT_SQL = """
SELECT COUNT(*)
FROM feature_vectors
WHERE symbol = ? AND timeframe = ? AND feature_set = ?
{extra_where};
"""


def materialize_features(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    feature_set: str = FEATURE_SET_VERSION,
) -> int:
    """Compute and persist feature vectors for the given candles.

    Uses INSERT OR REPLACE (UPSERT) so idempotent on re-run.

    Returns the number of rows upserted.
    """
    if not candles:
        return 0

    vectors = compute_features_for_candles(candles)
    count = 0
    for fv in vectors:
        connection.execute(
            _UPSERT_SQL,
            (
                symbol,
                timeframe,
                int(fv["open_time"]),
                feature_set,
                json.dumps(fv, sort_keys=True),
            ),
        )
        count += 1
    connection.commit()
    return count


def get_features(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    feature_set: str = FEATURE_SET_VERSION,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    limit: int = 500,
    offset: int = 0,
    ascending: bool = True,
) -> Dict[str, Any]:
    """Return paginated feature vectors for a symbol/timeframe.

    Parameters
    ----------
    start_time / end_time:
        Optional epoch-ms bounds (inclusive) to filter by ``open_time``.
    ascending:
        If True, returns oldest first; if False, newest first.

    Returns
    -------
    Dict with keys: symbol, timeframe, feature_set, total, limit, offset, vectors.
    Each vector in ``vectors`` includes all feature fields plus metadata.
    """
    extra_clauses: List[str] = []
    extra_params: List[Any] = []
    if start_time is not None:
        extra_clauses.append("AND open_time >= ?")
        extra_params.append(start_time)
    if end_time is not None:
        extra_clauses.append("AND open_time <= ?")
        extra_params.append(end_time)

    extra_where = " ".join(extra_clauses)
    order = "ASC" if ascending else "DESC"

    base_params = (symbol, timeframe, feature_set)

    count_row = connection.execute(
        _COUNT_SQL.format(extra_where=extra_where),
        base_params + tuple(extra_params),
    ).fetchone()
    total = int(count_row[0]) if count_row else 0

    rows = fetch_all_as_dicts(
        connection,
        _SELECT_SQL.format(extra_where=extra_where, order=order),
        base_params + tuple(extra_params) + (limit, offset),
    )

    vectors = []
    for row in rows:
        raw = row.get("features_json") or "{}"
        fv = json.loads(raw)
        fv["id"] = row["id"]
        fv["created_at"] = row["created_at"]
        vectors.append(fv)

    return {
        "symbol": symbol,
        "timeframe": timeframe,
        "feature_set": feature_set,
        "total": total,
        "limit": limit,
        "offset": offset,
        "vectors": vectors,
    }


def get_latest_feature_vector(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    feature_set: str = FEATURE_SET_VERSION,
) -> Optional[Dict[str, Any]]:
    """Return the most recent stored feature vector, or None."""
    row = connection.execute(
        """
        SELECT features_json, created_at, id
        FROM feature_vectors
        WHERE symbol = ? AND timeframe = ? AND feature_set = ?
        ORDER BY open_time DESC
        LIMIT 1;
        """,
        (symbol, timeframe, feature_set),
    ).fetchone()
    if row is None:
        return None
    fv = json.loads(row[0])
    fv["id"] = row[2]
    fv["created_at"] = row[1]
    return fv


def delete_features(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    feature_set: str = FEATURE_SET_VERSION,
) -> int:
    """Delete all stored feature vectors for a symbol/timeframe/feature_set.

    Returns the number of rows deleted.
    """
    cursor = connection.execute(
        "DELETE FROM feature_vectors WHERE symbol = ? AND timeframe = ? AND feature_set = ?;",
        (symbol, timeframe, feature_set),
    )
    connection.commit()
    return cursor.rowcount if hasattr(cursor, "rowcount") else 0
