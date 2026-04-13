import json
from typing import Any

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import get_connection
from app.core.db import utc_now_iso
from app.core.migrations import run_migrations

CREATE_RUNTIME_HEARTBEATS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS runtime_heartbeats (
    component TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    message TEXT NOT NULL,
    payload_json TEXT,
    last_seen_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

UPSERT_RUNTIME_HEARTBEAT_SQL = """
INSERT INTO runtime_heartbeats (
    component,
    status,
    message,
    payload_json,
    last_seen_at
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(component) DO UPDATE SET
    status = excluded.status,
    message = excluded.message,
    payload_json = excluded.payload_json,
    last_seen_at = excluded.last_seen_at;
"""

def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)

def upsert_heartbeat(
    connection: DBConnection,
    component: str,
    status: str,
    message: str,
    payload: dict[str, Any] | None = None,
) -> None:
    ensure_table(connection)
    connection.execute(
        UPSERT_RUNTIME_HEARTBEAT_SQL,
        (
            component,
            status,
            message,
            json.dumps(payload, ensure_ascii=True, sort_keys=True) if payload is not None else None,
            utc_now_iso(),
        ),
    )
    connection.commit()

def record_heartbeat(
    component: str,
    status: str,
    message: str,
    payload: dict[str, Any] | None = None,
) -> None:
    try:
        connection = get_connection()
    except Exception:
        return

    try:
        upsert_heartbeat(connection, component, status, message, payload)
    except Exception:
        # Heartbeat writes must stay best-effort. A malformed SQLite page or
        # transient DB issue should not bubble up and break API responses or
        # background alert tasks.
        return
    finally:
        connection.close()

def get_heartbeat_payload(
    connection: DBConnection,
    component: str,
) -> tuple[dict[str, Any], str | None]:
    """Return (payload_dict, last_seen_at) for a component heartbeat row.

    Returns ({}, None) when no heartbeat has been recorded yet.
    """
    row = connection.execute(
        "SELECT payload_json, last_seen_at FROM runtime_heartbeats WHERE component = ?",
        (component,),
    ).fetchone()
    if row is None:
        return {}, None
    payload = json.loads(row[0]) if row[0] else {}
    return payload, row[1]


def get_heartbeat_row(
    connection: DBConnection,
    component: str,
) -> dict[str, Any] | None:
    """Return full heartbeat row as a dict (status, message, payload, last_seen_at).

    Returns None when no heartbeat has been recorded yet.
    """
    row = connection.execute(
        "SELECT status, message, payload_json, last_seen_at FROM runtime_heartbeats WHERE component = ?",
        (component,),
    ).fetchone()
    if row is None:
        return None
    return {
        "status": row[0],
        "message": row[1],
        "payload": json.loads(row[2]) if row[2] else {},
        "last_seen_at": row[3],
    }


def get_heartbeats(connection: DBConnection) -> list[dict[str, Any]]:
    ensure_table(connection)
    return fetch_all_as_dicts(
        connection,
        """
        SELECT component, status, message, payload_json, last_seen_at
        FROM runtime_heartbeats
        ORDER BY component ASC;
        """,
    )
