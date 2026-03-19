from app.core.db import DBConnection
from app.core.migrations import run_migrations
from app.data.candles_service import get_latest_close


CREATE_PNL_SNAPSHOTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pnl_snapshots (
    id INTEGER PRIMARY KEY,
    symbol TEXT NOT NULL,
    qty REAL NOT NULL,
    avg_price REAL NOT NULL,
    market_price REAL NOT NULL,
    unrealized_pnl REAL NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


SELECT_POSITIONS_SQL = """
SELECT symbol, qty, avg_price
FROM positions;
"""


INSERT_PNL_SNAPSHOT_SQL = """
INSERT INTO pnl_snapshots (
    symbol,
    qty,
    avg_price,
    market_price,
    unrealized_pnl
) VALUES (?, ?, ?, ?, ?);
"""


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def update_pnl_snapshots(connection: DBConnection) -> int:
    positions = connection.execute(SELECT_POSITIONS_SQL).fetchall()
    if not positions:
        return 0

    snapshot_count = 0
    for symbol, qty, avg_price in positions:
        market_price = get_latest_close(connection, symbol=symbol, timeframe="1m")
        if market_price is None:
            continue
        qty = float(qty)
        avg_price = float(avg_price)
        unrealized_pnl = (market_price - avg_price) * qty
        connection.execute(
            INSERT_PNL_SNAPSHOT_SQL,
            (symbol, qty, avg_price, market_price, unrealized_pnl),
        )
        snapshot_count += 1

    connection.commit()
    return snapshot_count
