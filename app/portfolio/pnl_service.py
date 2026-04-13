from app.core.db import DBConnection
from app.core.db import table_exists
from app.core.db import utc_now_iso

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
    unrealized_pnl,
    created_at
) VALUES (?, ?, ?, ?, ?, ?);
"""

def _get_latest_prices_by_timeframe(
    connection: DBConnection,
    symbols: list[str],
    timeframe: str,
) -> dict[str, float]:
    """Batch fetch the latest close price for each symbol at a given timeframe."""
    if not symbols or not table_exists(connection, "candles"):
        return {}
    placeholders = ",".join(["?" for _ in symbols])
    rows = connection.execute(
        f"""
        SELECT c.symbol, c.close
        FROM candles c
        WHERE c.id IN (
            SELECT MAX(id) FROM candles
            WHERE symbol IN ({placeholders})
              AND timeframe = ?
            GROUP BY symbol
        );
        """,
        (*symbols, timeframe),
    ).fetchall()
    return {row[0]: float(row[1]) for row in rows}

def update_pnl_snapshots(connection: DBConnection) -> int:
    positions = connection.execute(SELECT_POSITIONS_SQL).fetchall()
    if not positions:
        return 0

    symbols = [row[0] for row in positions]
    prices = _get_latest_prices_by_timeframe(connection, symbols, timeframe="1m")

    snapshot_count = 0
    rows_to_insert: list[tuple] = []
    for symbol, qty, avg_price in positions:
        market_price = prices.get(symbol)
        if market_price is None:
            continue
        qty = float(qty)
        avg_price = float(avg_price)
        unrealized_pnl = (market_price - avg_price) * qty
        rows_to_insert.append((symbol, qty, avg_price, market_price, unrealized_pnl, utc_now_iso()))
        snapshot_count += 1

    if rows_to_insert:
        connection.executemany(INSERT_PNL_SNAPSHOT_SQL, rows_to_insert)
        connection.commit()
    return snapshot_count
