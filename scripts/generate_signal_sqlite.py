import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.strategy.ma_cross import ensure_table, generate_signal


def main() -> None:
    connection = get_connection()
    try:
        ensure_table(connection)
        result = generate_signal(connection)
    finally:
        connection.close()

    if result is None:
        print("Not enough candle data to generate a signal.")
        return

    print(f"short_ma={result['short_ma']:.2f}")
    print(f"long_ma={result['long_ma']:.2f}")
    print(f"signal={result['signal_type']}")


if __name__ == "__main__":
    main()
