import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.portfolio.positions_service import ensure_table as ensure_positions_table
from app.risk.risk_service import ensure_table, evaluate_latest_signal


def main() -> None:
    connection = get_connection()
    try:
        ensure_positions_table(connection)
        ensure_table(connection)
        result = evaluate_latest_signal(connection)
    finally:
        connection.close()

    if result is None:
        print("No signal found.")
        return

    print(f"signal={result['signal_type']}")
    print(f"decision={result['decision']}")
    print(f"reason={result['reason']}")


if __name__ == "__main__":
    main()
