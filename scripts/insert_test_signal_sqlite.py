import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import DB_FILE, get_connection
from app.strategy.ma_cross import ensure_table, insert_signal


VALID_SIGNALS = {"BUY", "SELL", "HOLD"}


def main() -> None:
    if len(sys.argv) != 2:
        print("Usage: python scripts/insert_test_signal_sqlite.py BUY|SELL|HOLD")
        sys.exit(1)

    signal_type = sys.argv[1].upper()
    if signal_type not in VALID_SIGNALS:
        print(f"Invalid signal type: {signal_type}")
        print("Valid values: BUY, SELL, HOLD")
        sys.exit(1)

    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        sys.exit(1)

    connection = get_connection()
    try:
        ensure_table(connection)
        insert_signal(connection, signal_type=signal_type)
    finally:
        connection.close()

    print(f"Inserted test signal: {signal_type}")


if __name__ == "__main__":
    main()
