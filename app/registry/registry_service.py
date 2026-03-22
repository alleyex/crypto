"""Model registry service.

Manages model versions derived from training jobs.

Status lifecycle
----------------
  candidate  → newly registered, not yet serving
  champion   → the single active model for a (symbol, timeframe, feature_set) key
  archived   → retired; kept for audit / rollback

Rules
-----
- Only ONE champion is allowed per (symbol, timeframe, feature_set).
- Promoting a candidate to champion automatically archives the previous champion.
- Archiving the current champion leaves no champion (safe — inference falls back).
- "Rollback" is just promoting an older candidate or archived model.

Schema (migration 028)
----------------------
  model_registry(
    id                INTEGER PRIMARY KEY,
    symbol            TEXT NOT NULL,
    timeframe         TEXT NOT NULL,
    feature_set       TEXT NOT NULL DEFAULT 'v1',
    training_job_id   INTEGER,
    version           TEXT NOT NULL,   -- auto-generated "YYYYMMDDHHMMSSffffff"
    status            TEXT NOT NULL DEFAULT 'candidate',
    model_json        TEXT NOT NULL,
    metrics_json      TEXT,
    notes             TEXT,
    promoted_at       TEXT,
    created_at        TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
  )
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.core.db import DBConnection, fetch_all_as_dicts

_STATUS_CANDIDATE = "candidate"
_STATUS_CHAMPION = "champion"
_STATUS_ARCHIVED = "archived"
VALID_STATUSES = frozenset({_STATUS_CANDIDATE, _STATUS_CHAMPION, _STATUS_ARCHIVED})

_INSERT_SQL = """
INSERT INTO model_registry
    (symbol, timeframe, feature_set, training_job_id, version, status, model_json, metrics_json, notes)
VALUES (?, ?, ?, ?, ?, 'candidate', ?, ?, ?);
"""


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _version_tag() -> str:
    """Generate a sortable version tag from the current UTC timestamp."""
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------

def register_model(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    feature_set: str,
    model: Dict[str, Any],
    training_job_id: Optional[int] = None,
    metrics: Optional[Dict[str, Any]] = None,
    notes: Optional[str] = None,
) -> int:
    """Register a new model version as a candidate. Returns its id."""
    cursor = connection.execute(
        _INSERT_SQL,
        (
            symbol,
            timeframe,
            feature_set,
            training_job_id,
            _version_tag(),
            json.dumps(model, sort_keys=True),
            json.dumps(metrics, sort_keys=True) if metrics is not None else None,
            notes,
        ),
    )
    connection.commit()
    return cursor.lastrowid  # type: ignore[return-value]


def promote_model(
    connection: DBConnection,
    model_id: int,
) -> Optional[Dict[str, Any]]:
    """Promote a model to champion.

    Archives the current champion for the same (symbol, timeframe, feature_set).
    Returns the updated model row, or None if model_id is not found.
    """
    row = _get_raw(connection, model_id)
    if row is None:
        return None

    symbol = row["symbol"]
    timeframe = row["timeframe"]
    feature_set = row["feature_set"]

    # Archive existing champion(s)
    connection.execute(
        """
        UPDATE model_registry
        SET status = ?, promoted_at = NULL
        WHERE symbol = ? AND timeframe = ? AND feature_set = ?
          AND status = ? AND id != ?;
        """,
        (_STATUS_ARCHIVED, symbol, timeframe, feature_set, _STATUS_CHAMPION, model_id),
    )
    # Promote this model
    connection.execute(
        "UPDATE model_registry SET status = ?, promoted_at = ? WHERE id = ?;",
        (_STATUS_CHAMPION, _now_utc(), model_id),
    )
    connection.commit()
    return get_model(connection, model_id)


def archive_model(
    connection: DBConnection,
    model_id: int,
) -> Optional[Dict[str, Any]]:
    """Archive a model (remove it from serving). Returns updated row or None."""
    if _get_raw(connection, model_id) is None:
        return None
    connection.execute(
        "UPDATE model_registry SET status = ?, promoted_at = NULL WHERE id = ?;",
        (_STATUS_ARCHIVED, model_id),
    )
    connection.commit()
    return get_model(connection, model_id)


def update_notes(
    connection: DBConnection,
    model_id: int,
    notes: str,
) -> Optional[Dict[str, Any]]:
    """Update the notes field on a model. Returns updated row or None."""
    if _get_raw(connection, model_id) is None:
        return None
    connection.execute(
        "UPDATE model_registry SET notes = ? WHERE id = ?;",
        (notes, model_id),
    )
    connection.commit()
    return get_model(connection, model_id)


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------

def get_model(
    connection: DBConnection,
    model_id: int,
) -> Optional[Dict[str, Any]]:
    rows = fetch_all_as_dicts(
        connection,
        "SELECT * FROM model_registry WHERE id = ?;",
        (model_id,),
    )
    return _deserialise(rows[0]) if rows else None


def _get_raw(connection: DBConnection, model_id: int) -> Optional[Dict[str, Any]]:
    rows = fetch_all_as_dicts(
        connection,
        "SELECT * FROM model_registry WHERE id = ?;",
        (model_id,),
    )
    return rows[0] if rows else None


def get_champion(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    feature_set: str,
) -> Optional[Dict[str, Any]]:
    """Return the current champion for a (symbol, timeframe, feature_set), or None."""
    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT * FROM model_registry
        WHERE symbol = ? AND timeframe = ? AND feature_set = ? AND status = ?
        ORDER BY promoted_at DESC, id DESC
        LIMIT 1;
        """,
        (symbol, timeframe, feature_set, _STATUS_CHAMPION),
    )
    return _deserialise(rows[0]) if rows else None


def list_models(
    connection: DBConnection,
    symbol: Optional[str] = None,
    timeframe: Optional[str] = None,
    feature_set: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    """Return paginated model registry rows, newest first."""
    clauses: List[str] = []
    params: List[Any] = []
    for col, val in (("symbol", symbol), ("timeframe", timeframe),
                     ("feature_set", feature_set), ("status", status)):
        if val is not None:
            clauses.append(f"{col} = ?")
            params.append(val)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    count_row = connection.execute(
        f"SELECT COUNT(*) FROM model_registry {where};", tuple(params)
    ).fetchone()
    total = int(count_row[0]) if count_row else 0

    rows = fetch_all_as_dicts(
        connection,
        f"SELECT * FROM model_registry {where} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?;",
        tuple(params) + (limit, offset),
    )
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "models": [_deserialise(r) for r in rows],
    }


def list_versions(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    feature_set: str,
) -> List[Dict[str, Any]]:
    """Return all versions for a (symbol, timeframe, feature_set), newest first."""
    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT * FROM model_registry
        WHERE symbol = ? AND timeframe = ? AND feature_set = ?
        ORDER BY created_at DESC, id DESC;
        """,
        (symbol, timeframe, feature_set),
    )
    return [_deserialise(r) for r in rows]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _deserialise(row: Dict[str, Any]) -> Dict[str, Any]:
    result = dict(row)
    for field in ("model_json", "metrics_json"):
        raw = result.pop(field, None)
        key = field.replace("_json", "")
        result[key] = json.loads(raw) if raw else None
    return result
