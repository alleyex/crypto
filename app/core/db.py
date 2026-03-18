import sqlite3
from pathlib import Path

from app.core.settings import DATABASE_URL
from app.core.settings import DB_BACKEND
from app.core.settings import SQLITE_PATH

DB_FILE = SQLITE_PATH
DB_DIR = DB_FILE.parent


def ensure_storage_dir() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    if DB_BACKEND != "sqlite":
        raise RuntimeError(
            "PostgreSQL migration path is configured, but the current SQL layer is still SQLite-only. "
            "Set CRYPTO_DB_BACKEND=sqlite or complete the PostgreSQL SQL migration first."
        )
    ensure_storage_dir()
    return sqlite3.connect(DB_FILE)


def get_database_info() -> dict[str, str]:
    info = {
        "backend": DB_BACKEND,
        "sqlite_path": str(DB_FILE),
    }
    if DATABASE_URL:
        info["database_url"] = DATABASE_URL
    return info
