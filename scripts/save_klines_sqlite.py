import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import DB_FILE, get_connection
from app.data.binance_client import fetch_klines
from app.data.candles_service import ensure_table, save_klines


def main() -> None:
    klines = fetch_klines()

    connection = get_connection()
    try:
        ensure_table(connection)
        save_klines(connection, klines)
    finally:
        connection.close()

    print(f"Saved {len(klines)} klines to {DB_FILE}")


if __name__ == "__main__":
    main()
