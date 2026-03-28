"""Training job persistence service.

A training job ties together:
  - input parameters (symbol, timeframe, feature_set, hyperparams)
  - dataset statistics (n_train, n_test, class balance)
  - training metrics (loss, eval metrics on train/test splits)
  - the trained model (JSON-serialised weights stored in DB)

Schema (created by migration 027):

    training_jobs(
        id              INTEGER PRIMARY KEY,
        symbol          TEXT    NOT NULL,
        timeframe       TEXT    NOT NULL,
        feature_set     TEXT    NOT NULL DEFAULT 'v1',
        status          TEXT    NOT NULL DEFAULT 'pending',
        -- 'pending' | 'running' | 'done' | 'failed'
        params_json     TEXT,           -- hyperparameters
        dataset_json    TEXT,           -- dataset stats
        metrics_json    TEXT,           -- train + test eval metrics
        model_json      TEXT,           -- serialised model weights
        error           TEXT,           -- error message if failed
        started_at      TEXT,
        finished_at     TEXT,
        created_at      TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
"""

import json
from typing import Any, Dict, List, Optional

from app.core.db import DBConnection, fetch_all_as_dicts

_INSERT_SQL = """
INSERT INTO training_jobs (symbol, timeframe, feature_set, status, params_json)
VALUES (?, ?, ?, 'pending', ?);
"""

_UPDATE_SQL = """
UPDATE training_jobs
SET status       = ?,
    dataset_json = ?,
    metrics_json = ?,
    model_json   = ?,
    error        = ?,
    started_at   = ?,
    finished_at  = ?
WHERE id = ?;
"""


def create_job(
    connection: DBConnection,
    symbol: str,
    timeframe: str,
    feature_set: str,
    params: Optional[Dict[str, Any]] = None,
) -> int:
    """Insert a new training job and return its id."""
    cursor = connection.execute(
        _INSERT_SQL,
        (
            symbol,
            timeframe,
            feature_set,
            json.dumps(params or {}, sort_keys=True),
        ),
    )
    connection.commit()
    return cursor.lastrowid  # type: ignore[return-value]


def update_job(
    connection: DBConnection,
    job_id: int,
    status: str,
    dataset: Optional[Dict[str, Any]] = None,
    metrics: Optional[Dict[str, Any]] = None,
    model: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
) -> None:
    """Update a training job's status, metrics, and model."""
    connection.execute(
        _UPDATE_SQL,
        (
            status,
            json.dumps(dataset, sort_keys=True) if dataset is not None else None,
            json.dumps(metrics, sort_keys=True) if metrics is not None else None,
            json.dumps(model, sort_keys=True) if model is not None else None,
            error,
            started_at,
            finished_at,
            job_id,
        ),
    )
    connection.commit()


def get_job(
    connection: DBConnection,
    job_id: int,
) -> Optional[Dict[str, Any]]:
    """Return a single training job by id, or None."""
    rows = fetch_all_as_dicts(
        connection,
        "SELECT * FROM training_jobs WHERE id = ?;",
        (job_id,),
    )
    if not rows:
        return None
    return _deserialise(rows[0])


def list_jobs(
    connection: DBConnection,
    symbol: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> Dict[str, Any]:
    """Return paginated training jobs, newest first."""
    clauses: List[str] = []
    params: List[Any] = []
    if symbol:
        clauses.append("symbol = ?")
        params.append(symbol)
    if status:
        clauses.append("status = ?")
        params.append(status)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    count_row = connection.execute(
        f"SELECT COUNT(*) FROM training_jobs {where};", tuple(params)
    ).fetchone()
    total = int(count_row[0]) if count_row else 0

    rows = fetch_all_as_dicts(
        connection,
        f"SELECT * FROM training_jobs {where} ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?;",
        tuple(params) + (limit, offset),
    )
    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "jobs": [_deserialise(r) for r in rows],
    }


def _deserialise(row: Dict[str, Any]) -> Dict[str, Any]:
    """Parse JSON fields in a training_jobs row."""
    result = dict(row)
    for field in ("params_json", "dataset_json", "metrics_json", "model_json"):
        raw = result.pop(field, None)
        key = field.replace("_json", "")
        result[key] = json.loads(raw) if raw else None
    # Keep the key name as progress_json so the JS can read job.progress_json
    raw_progress = result.get("progress_json")
    result["progress_json"] = json.loads(raw_progress) if raw_progress else None
    return result
