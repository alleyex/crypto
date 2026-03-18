import sqlite3
import sys
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"
VALID_SIGNALS = {"BUY", "SELL", "HOLD"}


INSERT_SIGNAL_SQL = """
INSERT INTO signals (
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    short_ma,
    long_ma
) VALUES (?, ?, ?, ?, ?, ?);
"""


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

    connection = sqlite3.connect(DB_FILE)
    try:
        connection.execute(
            INSERT_SIGNAL_SQL,
            ("BTCUSDT", "1m", "manual_test", signal_type, 0.0, 0.0),
        )
        connection.commit()
    finally:
        connection.close()

    print(f"Inserted test signal: {signal_type}")


if __name__ == "__main__":
    main()
