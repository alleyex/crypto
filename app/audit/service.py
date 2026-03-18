import json
import sqlite3
from typing import Any, Optional

from app.core.db import get_connection


CREATE_AUDIT_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    source TEXT NOT NULL,
    message TEXT NOT NULL,
    payload_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


INSERT_AUDIT_EVENT_SQL = """
INSERT INTO audit_events (
    event_type,
    status,
    source,
    message,
    payload_json
) VALUES (?, ?, ?, ?, ?);
"""


def ensure_table(connection: sqlite3.Connection) -> None:
    connection.execute(CREATE_AUDIT_EVENTS_TABLE_SQL)
    connection.commit()


def insert_event(
    connection: sqlite3.Connection,
    event_type: str,
    status: str,
    source: str,
    message: str,
    payload: Optional[dict[str, Any]] = None,
) -> int:
    ensure_table(connection)
    cursor = connection.execute(
        INSERT_AUDIT_EVENT_SQL,
        (
            event_type,
            status,
            source,
            message,
            json.dumps(payload, ensure_ascii=True, sort_keys=True) if payload is not None else None,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def log_event(
    event_type: str,
    status: str,
    source: str,
    message: str,
    payload: Optional[dict[str, Any]] = None,
) -> int:
    connection = get_connection()
    try:
        return insert_event(connection, event_type, status, source, message, payload)
    finally:
        connection.close()
