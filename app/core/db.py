import importlib
import time
from datetime import datetime
from datetime import timezone
from re import match
from re import sub
from typing import Any

from app.core.settings import DATABASE_URL
from app.core.settings import POSTGRES_CONNECT_RETRIES
from app.core.settings import POSTGRES_CONNECT_RETRY_DELAY_SECONDS

DBConnection = Any
DBError = Exception

def _load_psycopg() -> Any:
    try:
        return importlib.import_module("psycopg")
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "psycopg is not installed. Run `pip install -r requirements.txt` before using PostgreSQL."
        ) from exc

def _rewrite_query_params(query: str) -> str:
    rewritten: list[str] = []
    in_single = False
    in_double = False

    for char in query:
        if char == "'" and not in_double:
            in_single = not in_single
            rewritten.append(char)
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            rewritten.append(char)
            continue
        if char == "?" and not in_single and not in_double:
            rewritten.append("%s")
            continue
        rewritten.append(char)

    return "".join(rewritten)

def _inject_returning_id(query: str) -> str:
    stripped = query.rstrip()
    if stripped.endswith(";"):
        stripped = stripped[:-1]
    return f"{stripped} RETURNING id;"

class PostgresCursorAdapter:
    def __init__(self, cursor: Any, lastrowid: Any = None):
        self._cursor = cursor
        self.lastrowid = lastrowid

    @property
    def description(self) -> Any:
        return self._cursor.description

    def fetchone(self) -> Any:
        return self._cursor.fetchone()

    def fetchall(self) -> Any:
        return self._cursor.fetchall()

class PostgresConnectionAdapter:
    def __init__(self, connection: Any):
        self._connection = connection
        self.row_factory = None

    def execute(self, query: str, params: tuple[Any, ...] = ()) -> PostgresCursorAdapter:
        rewritten_query = _rewrite_query_params(query)
        with self._connection.cursor() as cursor:
            cursor.execute(rewritten_query, params)
            rows = cursor.fetchall() if cursor.description else None
            description = cursor.description
        return _materialize_postgres_cursor(rows, description, None)

    def executemany(self, query: str, seq_of_params: list[tuple[Any, ...]]) -> None:
        if not seq_of_params:
            return
        rewritten_query = _rewrite_query_params(query)
        with self._connection.cursor() as cursor:
            cursor.executemany(rewritten_query, seq_of_params)

    def commit(self) -> None:
        self._connection.commit()

    def rollback(self) -> None:
        self._connection.rollback()

    def close(self) -> None:
        self._connection.close()

def get_backend_name(connection: DBConnection | None = None) -> str:
    if connection is None or isinstance(connection, PostgresConnectionAdapter):
        return "postgres"
    return "sqlite"

def _materialize_postgres_cursor(rows: Any, description: Any, lastrowid: Any) -> PostgresCursorAdapter:
    class MaterializedCursor:
        def __init__(self, result_rows: Any, result_description: Any):
            self._rows = list(result_rows or [])
            self.description = result_description
            self._index = 0

        def fetchone(self) -> Any:
            if self._index >= len(self._rows):
                return None
            row = self._rows[self._index]
            self._index += 1
            return row

        def fetchall(self) -> Any:
            if self._index == 0:
                self._index = len(self._rows)
                return list(self._rows)
            remaining = self._rows[self._index :]
            self._index = len(self._rows)
            return remaining

    return PostgresCursorAdapter(MaterializedCursor(rows, description), lastrowid=lastrowid)

def _table_identifier(table_name: str) -> str:
    if not match(r"^[A-Za-z_][A-Za-z0-9_]*$", table_name):
        raise ValueError(f"Unsupported table name: {table_name}")
    return table_name

def get_connection() -> DBConnection:
    if not DATABASE_URL:
        raise RuntimeError("CRYPTO_DATABASE_URL must be set.")
    psycopg = _load_psycopg()
    last_error: Exception | None = None
    for attempt in range(POSTGRES_CONNECT_RETRIES):
        try:
            return PostgresConnectionAdapter(psycopg.connect(DATABASE_URL))
        except Exception as exc:  # pragma: no cover
            last_error = exc
            if attempt == POSTGRES_CONNECT_RETRIES - 1:
                break
            time.sleep(POSTGRES_CONNECT_RETRY_DELAY_SECONDS)
    raise RuntimeError(
        f"Unable to connect to PostgreSQL after {POSTGRES_CONNECT_RETRIES} attempts: {last_error}"
    ) from last_error

def list_tables(connection: DBConnection, backend: str | None = None) -> list[str]:
    if (backend or get_backend_name(connection)) != "postgres":
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name ASC;"
        ).fetchall()
        return [str(row[0]) for row in rows]
    rows = connection.execute(
        """
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
        ORDER BY tablename ASC;
        """
    ).fetchall()
    return [str(row[0]) for row in rows]

def table_exists(connection: DBConnection, table_name: str, backend: str | None = None) -> bool:
    if (backend or get_backend_name(connection)) != "postgres":
        row = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
            (table_name,),
        ).fetchone()
        return row is not None
    row = connection.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = %s
        LIMIT 1;
        """,
        (table_name,),
    ).fetchone()
    return row is not None

def get_table_columns(connection: DBConnection, table_name: str, backend: str | None = None) -> set[str]:
    if (backend or get_backend_name(connection)) != "postgres":
        rows = connection.execute(
            f"PRAGMA table_info({_table_identifier(table_name)});"
        ).fetchall()
        return {str(row[1]) for row in rows}
    rows = connection.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position ASC;
        """,
        (table_name,),
    ).fetchall()
    return {str(row[0]) for row in rows}

def get_table_column_type(
    connection: DBConnection,
    table_name: str,
    column_name: str,
    backend: str | None = None,
) -> str | None:
    if (backend or get_backend_name(connection)) != "postgres":
        rows = connection.execute(
            f"PRAGMA table_info({_table_identifier(table_name)});"
        ).fetchall()
        for row in rows:
            if str(row[1]) == column_name:
                return str(row[2]).lower()
        return None
    row = connection.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
        LIMIT 1;
        """,
        (table_name, column_name),
    ).fetchone()
    return str(row[0]).lower() if row is not None and row[0] is not None else None

def fetch_all_as_dicts(
    connection: DBConnection,
    query: str,
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    cursor = connection.execute(query, params)
    rows = cursor.fetchall()
    if not getattr(cursor, "description", None):
        return []
    column_names = [str(item[0]) for item in cursor.description]
    return [dict(zip(column_names, row)) for row in rows]

def insert_and_get_rowid(connection: DBConnection, query: str, params: tuple[Any, ...] = ()) -> int:
    cursor = connection.execute(_inject_returning_id(query), params)
    row = cursor.fetchone()
    if row is None:
        raise RuntimeError("PostgreSQL insert did not return an id.")
    return int(row[0])

def get_database_info() -> dict[str, str]:
    return {"backend": "postgres", "database_url": DATABASE_URL}

def get_database_label() -> str:
    return DATABASE_URL

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_db_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)

    text = str(value).strip()
    text = sub(r"([+-]\d{2})$", r"\g<1>00", text)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        parsed = None
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                parsed = datetime.strptime(text, fmt)
                break
            except ValueError:
                continue
        if parsed is None:
            raise

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
