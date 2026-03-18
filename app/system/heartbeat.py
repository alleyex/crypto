import json
import sqlite3
from typing import Any
from typing import Optional

from app.core.db import get_connection


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
) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(component) DO UPDATE SET
    status = excluded.status,
    message = excluded.message,
    payload_json = excluded.payload_json,
    last_seen_at = CURRENT_TIMESTAMP;
"""


def ensure_table(connection: sqlite3.Connection) -> None:
    connection.execute(CREATE_RUNTIME_HEARTBEATS_TABLE_SQL)
    connection.commit()


def upsert_heartbeat(
    connection: sqlite3.Connection,
    component: str,
    status: str,
    message: str,
    payload: Optional[dict[str, Any]] = None,
) -> None:
    ensure_table(connection)
    connection.execute(
        UPSERT_RUNTIME_HEARTBEAT_SQL,
        (
            component,
            status,
            message,
            json.dumps(payload, ensure_ascii=True, sort_keys=True) if payload is not None else None,
        ),
    )
    connection.commit()


def record_heartbeat(
    component: str,
    status: str,
    message: str,
    payload: Optional[dict[str, Any]] = None,
) -> None:
    connection = get_connection()
    try:
        upsert_heartbeat(connection, component, status, message, payload)
    finally:
        connection.close()


def get_heartbeats(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_table(connection)
    connection.row_factory = sqlite3.Row
    rows = connection.execute(
        """
        SELECT component, status, message, payload_json, last_seen_at
        FROM runtime_heartbeats
        ORDER BY component ASC;
        """
    ).fetchall()
    return [dict(row) for row in rows]
