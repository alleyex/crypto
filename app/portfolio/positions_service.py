from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.migrations import run_migrations


CREATE_POSITIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS positions (
    symbol TEXT PRIMARY KEY,
    qty REAL NOT NULL,
    avg_price REAL NOT NULL,
    realized_pnl REAL NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


UPSERT_POSITION_SQL = """
INSERT INTO positions (
    symbol,
    qty,
    avg_price,
    realized_pnl,
    updated_at
) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(symbol) DO UPDATE SET
    qty = excluded.qty,
    avg_price = excluded.avg_price,
    realized_pnl = excluded.realized_pnl,
    updated_at = CURRENT_TIMESTAMP;
"""


SELECT_FILLS_SQL = """
SELECT symbol, side, qty, price
FROM fills
ORDER BY id ASC;
"""


def ensure_table(connection: DBConnection) -> None:
    run_migrations(connection)


def update_positions(connection: DBConnection) -> int:
    fills = connection.execute(SELECT_FILLS_SQL).fetchall()
    if not fills:
        return 0

    positions: dict[str, dict[str, float]] = {}
    for symbol, side, qty, price in fills:
        qty = float(qty)
        price = float(price)

        if symbol not in positions:
            positions[symbol] = {"qty": 0.0, "cost": 0.0, "realized_pnl": 0.0}

        if side == "BUY":
            positions[symbol]["qty"] += qty
            positions[symbol]["cost"] += qty * price
        elif side == "SELL":
            current_qty = positions[symbol]["qty"]
            current_cost = positions[symbol]["cost"]
            if current_qty <= 0:
                continue
            sell_qty = min(qty, current_qty)
            avg_price = current_cost / current_qty
            positions[symbol]["qty"] -= sell_qty
            positions[symbol]["cost"] -= sell_qty * avg_price
            positions[symbol]["realized_pnl"] += (price - avg_price) * sell_qty

    # Read existing positions before overwriting so we can detect state transitions.
    existing: dict[str, float] = {}
    for row in connection.execute("SELECT symbol, qty FROM positions;").fetchall():
        existing[row[0]] = float(row[1])

    for symbol, position in positions.items():
        qty = position["qty"]
        realized_pnl = position["realized_pnl"]
        if qty <= 0:
            avg_price = 0.0
            qty = 0.0
        else:
            avg_price = position["cost"] / qty
        connection.execute(UPSERT_POSITION_SQL, (symbol, qty, avg_price, realized_pnl))

        # Emit audit event when position opens or closes.
        old_qty = existing.get(symbol, 0.0)
        if old_qty <= 0 and qty > 0:
            insert_event(
                connection,
                event_type="position",
                status="opened",
                source="positions_service",
                message=f"Position opened for {symbol}: qty={qty}, avg_price={round(avg_price, 4)}.",
                payload={"symbol": symbol, "qty": qty, "avg_price": round(avg_price, 4)},
            )
        elif old_qty > 0 and qty <= 0:
            insert_event(
                connection,
                event_type="position",
                status="closed",
                source="positions_service",
                message=f"Position closed for {symbol}: realized_pnl={round(realized_pnl, 4)}.",
                payload={"symbol": symbol, "realized_pnl": round(realized_pnl, 4), "prev_qty": old_qty},
            )

    connection.commit()
    return len(positions)
