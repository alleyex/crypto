import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.core.migrations import run_migrations


if __name__ == "__main__":
    connection = get_connection()
    try:
        applied = run_migrations(connection)
    finally:
        connection.close()

    if applied:
        print("Applied migrations:")
        for version in applied:
            print(version)
    else:
        print("No pending migrations.")
