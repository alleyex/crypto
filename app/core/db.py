import sqlite3
from pathlib import Path


DB_DIR = Path("storage")
DB_FILE = DB_DIR / "market_data.db"


def ensure_storage_dir() -> None:
    DB_DIR.mkdir(parents=True, exist_ok=True)


def get_connection() -> sqlite3.Connection:
    ensure_storage_dir()
    return sqlite3.connect(DB_FILE)
