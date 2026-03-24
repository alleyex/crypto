import json
from typing import Any
from typing import Optional

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import get_connection
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
) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(component) DO UPDATE SET
    status = excluded.status,
    message = excluded.message,
    payload_json = excluded.payload_json,
    last_seen_at = CURRENT_TIMESTAMP;
"""


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def upsert_heartbeat(
    connection: DBConnection,
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
