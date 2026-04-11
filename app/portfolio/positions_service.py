from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import utc_now_iso
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
) VALUES (?, ?, ?, ?, ?)
ON CONFLICT(symbol) DO UPDATE SET
    qty = excluded.qty,
    avg_price = excluded.avg_price,
    realized_pnl = excluded.realized_pnl,
    updated_at = excluded.updated_at;
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

        current_qty = positions[symbol]["qty"]

        if side == "BUY":
            if current_qty >= 0:
                # Adding to LONG or opening LONG from flat
                positions[symbol]["qty"] += qty
                positions[symbol]["cost"] += qty * price
            else:
                # Closing SHORT position (current_qty < 0)
                close_qty = min(qty, abs(current_qty))
                avg_short_price = positions[symbol]["cost"] / abs(current_qty) if current_qty != 0 else price
                positions[symbol]["realized_pnl"] += (avg_short_price - price) * close_qty
                positions[symbol]["qty"] += close_qty
                positions[symbol]["cost"] = abs(positions[symbol]["qty"]) * avg_short_price if positions[symbol]["qty"] < 0 else 0.0
                remaining_buy = qty - close_qty
                if remaining_buy > 1e-9:
                    # Flip to LONG with remaining qty
                    positions[symbol]["qty"] += remaining_buy
                    positions[symbol]["cost"] += remaining_buy * price
        elif side == "SELL":
            if current_qty <= 0:
                # Adding to SHORT or opening SHORT from flat
                positions[symbol]["qty"] -= qty
                positions[symbol]["cost"] += qty * price  # cost stored as positive notional
            else:
                # Closing LONG position
                close_qty = min(qty, current_qty)
                avg_price = positions[symbol]["cost"] / current_qty
                positions[symbol]["realized_pnl"] += (price - avg_price) * close_qty
                positions[symbol]["qty"] -= close_qty
                positions[symbol]["cost"] -= close_qty * avg_price
                remaining_sell = qty - close_qty
                if remaining_sell > 1e-9:
                    # Flip to SHORT with remaining qty
                    positions[symbol]["qty"] -= remaining_sell
                    positions[symbol]["cost"] += remaining_sell * price

    # Read existing positions before overwriting so we can detect state transitions.
    existing: dict[str, float] = {}
    for row in connection.execute("SELECT symbol, qty FROM positions;").fetchall():
        existing[row[0]] = float(row[1])

    for symbol, position in positions.items():
        qty = position["qty"]
        realized_pnl = position["realized_pnl"]
        cost = position["cost"]
        if abs(qty) < 1e-9:
            avg_price = 0.0
            qty = 0.0
        else:
            avg_price = cost / abs(qty) if qty != 0 else 0.0
        connection.execute(UPSERT_POSITION_SQL, (symbol, qty, avg_price, realized_pnl, utc_now_iso()))

        # Emit audit event when position opens or closes.
        old_qty = existing.get(symbol, 0.0)
        if abs(old_qty) < 1e-9 and abs(qty) > 1e-9:
            insert_event(
                connection,
                event_type="position",
                status="opened",
                source="positions_service",
                message=f"Position opened for {symbol}: qty={qty}, avg_price={round(avg_price, 4)}.",
                payload={"symbol": symbol, "qty": qty, "avg_price": round(avg_price, 4)},
            )
        elif abs(old_qty) > 1e-9 and abs(qty) < 1e-9:
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
