"""Shared FastAPI dependencies."""

from app.core.db import DBConnection, get_connection
from app.core.migrations import run_migrations


def get_db():
    connection = get_connection()
    run_migrations(connection)
    try:
        yield connection
    finally:
        connection.close()
