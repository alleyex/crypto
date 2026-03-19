import json
from typing import Any, Optional

from app.core.db import DBConnection
from app.core.db import get_connection
from app.core.db import insert_and_get_rowid
from app.core.migrations import run_migrations


CREATE_AUDIT_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY,
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


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def insert_event(
    connection: DBConnection,
    event_type: str,
    status: str,
    source: str,
    message: str,
    payload: Optional[dict[str, Any]] = None,
) -> int:
    ensure_table(connection)
    event_id = insert_and_get_rowid(
        connection,
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
    return event_id


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
