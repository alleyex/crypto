import sqlite3
from pathlib import Path


DB_FILE = Path("storage") / "market_data.db"


CREATE_RISK_EVENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS risk_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER,
    symbol TEXT NOT NULL,
    timeframe TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    decision TEXT NOT NULL,
    reason TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


SELECT_LATEST_SIGNAL_SQL = """
SELECT
    id,
    symbol,
    timeframe,
    strategy_name,
    signal_type
FROM signals
ORDER BY id DESC
LIMIT 1;
"""


SELECT_PREVIOUS_SIGNAL_SQL = """
SELECT signal_type
FROM signals
WHERE id < ?
ORDER BY id DESC
LIMIT 1;
"""


SELECT_POSITION_SQL = """
SELECT qty
FROM positions
WHERE symbol = ?
LIMIT 1;
"""


INSERT_RISK_EVENT_SQL = """
INSERT INTO risk_events (
    signal_id,
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    decision,
    reason
) VALUES (?, ?, ?, ?, ?, ?, ?);
"""


def ensure_signal_id_column(connection: sqlite3.Connection) -> None:
    columns = {
        row[1] for row in connection.execute("PRAGMA table_info(risk_events);").fetchall()
    }
    if "signal_id" not in columns:
        connection.execute("ALTER TABLE risk_events ADD COLUMN signal_id INTEGER;")


def main() -> None:
    if not DB_FILE.exists():
        print(f"Database not found: {DB_FILE}")
        return

    connection = sqlite3.connect(DB_FILE)
    try:
        connection.execute(CREATE_RISK_EVENTS_TABLE_SQL)
        ensure_signal_id_column(connection)

        latest_signal = connection.execute(SELECT_LATEST_SIGNAL_SQL).fetchone()
        if latest_signal is None:
            print("No signal found.")
            return

        signal_id, symbol, timeframe, strategy_name, signal_type = latest_signal

        if signal_type == "HOLD":
            decision = "REJECTED"
            reason = "Signal is HOLD."
        else:
            position_row = connection.execute(
                SELECT_POSITION_SQL,
                (symbol,),
            ).fetchone()
            current_qty = float(position_row[0]) if position_row is not None else 0.0

            if signal_type == "BUY" and current_qty > 0:
                decision = "REJECTED"
                reason = "Existing long position already open."
            elif signal_type == "SELL" and current_qty <= 0:
                decision = "REJECTED"
                reason = "No position available to sell."
            else:
                previous_signal = connection.execute(
                    SELECT_PREVIOUS_SIGNAL_SQL,
                    (signal_id,),
                ).fetchone()

                if previous_signal and previous_signal[0] == signal_type:
                    decision = "REJECTED"
                    reason = "Duplicate signal type."
                else:
                    decision = "APPROVED"
                    reason = "Passed basic risk checks."

        connection.execute(
            INSERT_RISK_EVENT_SQL,
            (
                signal_id,
                symbol,
                timeframe,
                strategy_name,
                signal_type,
                decision,
                reason,
            ),
        )
        connection.commit()
    finally:
        connection.close()

    print(f"signal={signal_type}")
    print(f"decision={decision}")
    print(f"reason={reason}")


if __name__ == "__main__":
    main()
