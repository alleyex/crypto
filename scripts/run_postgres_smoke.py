import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.postgres_smoke import run_postgres_migration_smoke
from app.core.postgres_smoke import run_postgres_smoke
from app.core.settings import DATABASE_URL


if __name__ == "__main__":
    result = {
        "connection_smoke": run_postgres_smoke(DATABASE_URL),
        "migration_smoke": run_postgres_migration_smoke(DATABASE_URL),
    }
    print(json.dumps(result, indent=2, sort_keys=True))
