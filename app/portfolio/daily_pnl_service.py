from datetime import datetime, timezone
from typing import Optional

from app.core.db import DBConnection
from app.core.db import parse_db_timestamp
from app.core.db import table_exists
from app.core.migrations import run_migrations

CREATE_DAILY_REALIZED_PNL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS daily_realized_pnl (
    symbol TEXT NOT NULL,
    pnl_date TEXT NOT NULL,
    realized_pnl REAL NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, pnl_date)
);
"""


DELETE_DAILY_REALIZED_PNL_SQL = "DELETE FROM daily_realized_pnl;"


SELECT_FILLS_SQL = """
SELECT symbol, side, qty, price, created_at
FROM fills
ORDER BY id ASC;
"""


UPSERT_DAILY_REALIZED_PNL_SQL = """
INSERT INTO daily_realized_pnl (
    symbol,
    pnl_date,
    realized_pnl,
    updated_at
) VALUES (?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(symbol, pnl_date) DO UPDATE SET
    realized_pnl = excluded.realized_pnl,
    updated_at = CURRENT_TIMESTAMP;
"""


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def _fills_table_exists(connection: DBConnection) -> bool:
    return table_exists(connection, "fills")
def rebuild_daily_realized_pnl(connection: DBConnection) -> int:
    ensure_table(connection)
    connection.execute(DELETE_DAILY_REALIZED_PNL_SQL)

    if not _fills_table_exists(connection):
        connection.commit()
        return 0

    fills = connection.execute(SELECT_FILLS_SQL).fetchall()
    if not fills:
        connection.commit()
        return 0

    positions: dict[str, dict[str, float]] = {}
    daily_pnl: dict[tuple[str, str], float] = {}
    for symbol, side, qty, price, created_at in fills:
        qty = float(qty)
        price = float(price)
        positions.setdefault(symbol, {"qty": 0.0, "cost": 0.0})

        if side == "BUY":
            positions[symbol]["qty"] += qty
            positions[symbol]["cost"] += qty * price
            continue

        current_qty = positions[symbol]["qty"]
        current_cost = positions[symbol]["cost"]
        if current_qty <= 0:
            continue

        sell_qty = min(qty, current_qty)
        avg_price = current_cost / current_qty
        positions[symbol]["qty"] -= sell_qty
        positions[symbol]["cost"] -= sell_qty * avg_price

        pnl_date = parse_db_timestamp(created_at).date().isoformat()
        ledger_key = (symbol, pnl_date)
        daily_pnl[ledger_key] = daily_pnl.get(ledger_key, 0.0) + (price - avg_price) * sell_qty

    for (symbol, pnl_date), realized_pnl in daily_pnl.items():
        connection.execute(
            UPSERT_DAILY_REALIZED_PNL_SQL,
            (symbol, pnl_date, realized_pnl),
        )

    connection.commit()
    return len(daily_pnl)


def get_daily_realized_pnl(
    connection: DBConnection,
    symbol: str,
    pnl_date: Optional[str] = None,
) -> float:
    """Read daily realized PnL from the pre-built ledger table.

    The table is kept current by rebuild_daily_realized_pnl(), which brokers
    call after every fill.  This function is a fast point-read only — it does
    not trigger a rebuild.
    """
    ensure_table(connection)
    target_date = pnl_date or datetime.now(timezone.utc).date().isoformat()
    row = connection.execute(
        """
        SELECT realized_pnl
        FROM daily_realized_pnl
        WHERE symbol = ? AND pnl_date = ?
        LIMIT 1;
        """,
        (symbol, target_date),
    ).fetchone()
    return float(row[0]) if row is not None else 0.0
