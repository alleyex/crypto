"""Portfolio service: cross-strategy exposure aggregation and capital limits.

Portfolio config stores a reference total_capital and two ratio limits:
  - max_strategy_allocation_pct: max fraction any single strategy may hold
  - max_total_exposure_pct:      max fraction of all open positions combined

Limits are only enforced when total_capital > 0 (i.e. explicitly configured).
When total_capital == 0 (default) the service is purely informational.
"""
from dataclasses import dataclass
from typing import Optional

from app.core.db import DBConnection
from app.core.db import table_exists


# ---------------------------------------------------------------------------
# Default config values
# ---------------------------------------------------------------------------

DEFAULT_TOTAL_CAPITAL: float = 0.0          # 0 = no enforcement
DEFAULT_MAX_STRATEGY_ALLOCATION_PCT: float = 0.5   # 50 % per strategy
DEFAULT_MAX_TOTAL_EXPOSURE_PCT: float = 0.8        # 80 % total


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------

SELECT_PORTFOLIO_CONFIG_SQL = """
SELECT total_capital, max_strategy_allocation_pct, max_total_exposure_pct, updated_at
FROM portfolio_config
LIMIT 1;
"""

UPSERT_PORTFOLIO_CONFIG_SQL = """
INSERT INTO portfolio_config
    (id, total_capital, max_strategy_allocation_pct, max_total_exposure_pct, updated_at)
VALUES (1, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT (id) DO UPDATE SET
    total_capital = excluded.total_capital,
    max_strategy_allocation_pct = excluded.max_strategy_allocation_pct,
    max_total_exposure_pct = excluded.max_total_exposure_pct,
    updated_at = excluded.updated_at;
"""

SELECT_OPEN_POSITIONS_SQL = """
SELECT symbol, qty, avg_price, realized_pnl, updated_at
FROM positions
WHERE qty > 0;
"""

SELECT_LATEST_CLOSE_SQL = """
SELECT close
FROM candles
WHERE symbol = ?
ORDER BY id DESC
LIMIT 1;
"""

# Per-strategy open qty: trace fills → orders → risk_events → signals.
# We replay all fills grouped by strategy to compute net open qty per strategy.
SELECT_STRATEGY_FILLS_SQL = """
SELECT s.strategy_name, f.symbol, f.side, f.qty
FROM fills f
JOIN orders o ON o.id = f.order_id
JOIN risk_events re ON re.id = o.risk_event_id
JOIN signals s ON s.id = re.signal_id
ORDER BY f.id ASC;
"""

# Pending approved buys: risk_events that are APPROVED BUY but have no order yet.
# Used to prevent double-counting when two risk evaluations race before execution.
SELECT_PENDING_APPROVED_BUYS_SQL = """
SELECT re.symbol, re.strategy_name
FROM risk_events re
LEFT JOIN orders o ON o.risk_event_id = re.id
WHERE re.signal_type = 'BUY'
  AND re.decision = 'APPROVED'
  AND o.id IS NULL;
"""


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class PortfolioConfig:
    total_capital: float
    max_strategy_allocation_pct: float
    max_total_exposure_pct: float
    updated_at: Optional[str] = None

    @property
    def enforcement_active(self) -> bool:
        return self.total_capital > 0

    @property
    def max_strategy_notional(self) -> Optional[float]:
        if self.total_capital <= 0:
            return None
        return self.total_capital * self.max_strategy_allocation_pct

    @property
    def max_total_notional(self) -> Optional[float]:
        if self.total_capital <= 0:
            return None
        return self.total_capital * self.max_total_exposure_pct

    def to_dict(self) -> dict:
        d: dict = {
            "total_capital": self.total_capital,
            "max_strategy_allocation_pct": self.max_strategy_allocation_pct,
            "max_total_exposure_pct": self.max_total_exposure_pct,
            "enforcement_active": self.total_capital > 0,
        }
        if self.max_strategy_notional is not None:
            d["max_strategy_notional"] = self.max_strategy_notional
        if self.max_total_notional is not None:
            d["max_total_notional"] = self.max_total_notional
        if self.updated_at is not None:
            d["updated_at"] = self.updated_at
        return d


# ---------------------------------------------------------------------------
# Config accessors
# ---------------------------------------------------------------------------

def get_portfolio_config(connection: DBConnection) -> PortfolioConfig:
    if not table_exists(connection, "portfolio_config"):
        return PortfolioConfig(
            total_capital=DEFAULT_TOTAL_CAPITAL,
            max_strategy_allocation_pct=DEFAULT_MAX_STRATEGY_ALLOCATION_PCT,
            max_total_exposure_pct=DEFAULT_MAX_TOTAL_EXPOSURE_PCT,
        )
    row = connection.execute(SELECT_PORTFOLIO_CONFIG_SQL).fetchone()
    if row is None:
        return PortfolioConfig(
            total_capital=DEFAULT_TOTAL_CAPITAL,
            max_strategy_allocation_pct=DEFAULT_MAX_STRATEGY_ALLOCATION_PCT,
            max_total_exposure_pct=DEFAULT_MAX_TOTAL_EXPOSURE_PCT,
        )
    return PortfolioConfig(
        total_capital=float(row[0]),
        max_strategy_allocation_pct=float(row[1]),
        max_total_exposure_pct=float(row[2]),
        updated_at=row[3],
    )


def set_portfolio_config(
    connection: DBConnection,
    total_capital: Optional[float] = None,
    max_strategy_allocation_pct: Optional[float] = None,
    max_total_exposure_pct: Optional[float] = None,
) -> PortfolioConfig:
    existing = get_portfolio_config(connection)
    merged = PortfolioConfig(
        total_capital=total_capital if total_capital is not None else existing.total_capital,
        max_strategy_allocation_pct=(
            max_strategy_allocation_pct
            if max_strategy_allocation_pct is not None
            else existing.max_strategy_allocation_pct
        ),
        max_total_exposure_pct=(
            max_total_exposure_pct
            if max_total_exposure_pct is not None
            else existing.max_total_exposure_pct
        ),
    )
    connection.execute(
        UPSERT_PORTFOLIO_CONFIG_SQL,
        (merged.total_capital, merged.max_strategy_allocation_pct, merged.max_total_exposure_pct),
    )
    connection.commit()
    return merged


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_latest_price(connection: DBConnection, symbol: str) -> Optional[float]:
    if not table_exists(connection, "candles"):
        return None
    row = connection.execute(SELECT_LATEST_CLOSE_SQL, (symbol,)).fetchone()
    return float(row[0]) if row is not None else None


def _compute_pending_approved_notional(
    connection: DBConnection,
    order_qty: float,
) -> tuple[float, dict[str, float]]:
    """Return (total_pending_notional, {strategy_name: pending_notional}) for approved but
    unexecuted BUY risk events.  Mirrors the guard in risk_service._get_pending_approved_buy_qty()
    so that portfolio limits account for in-flight approvals and prevent concurrent over-allocation.

    Returns (0.0, {}) when the required tables do not exist yet.
    """
    if not (table_exists(connection, "risk_events") and table_exists(connection, "orders")):
        return 0.0, {}

    rows = connection.execute(SELECT_PENDING_APPROVED_BUYS_SQL).fetchall()
    total = 0.0
    per_strategy: dict[str, float] = {}
    for symbol, strategy_name in rows:
        price = _get_latest_price(connection, symbol)
        if price is None or price <= 0:
            continue
        notional = order_qty * price
        total += notional
        per_strategy[strategy_name] = per_strategy.get(strategy_name, 0.0) + notional
    return total, per_strategy


def _compute_per_strategy_open_qty(connection: DBConnection) -> dict[str, dict[str, float]]:
    """Return {strategy_name: {symbol: net_open_qty}} from fill history."""
    if not (table_exists(connection, "fills") and table_exists(connection, "orders")
            and table_exists(connection, "risk_events") and table_exists(connection, "signals")):
        return {}
    rows = connection.execute(SELECT_STRATEGY_FILLS_SQL).fetchall()
    # {(strategy, symbol): qty}
    state: dict[tuple[str, str], float] = {}
    for strategy_name, symbol, side, qty in rows:
        key = (strategy_name, symbol)
        current = state.get(key, 0.0)
        if side == "BUY":
            state[key] = current + float(qty)
        elif side == "SELL":
            state[key] = max(0.0, current - float(qty))

    result: dict[str, dict[str, float]] = {}
    for (strategy_name, symbol), qty in state.items():
        if qty > 0:
            result.setdefault(strategy_name, {})[symbol] = round(qty, 8)
    return result


# ---------------------------------------------------------------------------
# Portfolio summary
# ---------------------------------------------------------------------------

def get_portfolio_summary(connection: DBConnection) -> dict:
    config = get_portfolio_config(connection)

    # Open positions
    open_positions = []
    total_realized_pnl = 0.0
    total_open_notional = 0.0

    if table_exists(connection, "positions"):
        rows = connection.execute(SELECT_OPEN_POSITIONS_SQL).fetchall()
        for symbol, qty, avg_price, realized_pnl, updated_at in rows:
            qty = float(qty)
            avg_price = float(avg_price)
            realized_pnl = float(realized_pnl)
            total_realized_pnl += realized_pnl

            latest_price = _get_latest_price(connection, symbol)
            notional = round(qty * latest_price, 4) if latest_price is not None else None
            unrealized_pnl = (
                round((latest_price - avg_price) * qty, 4)
                if latest_price is not None
                else None
            )
            if notional is not None:
                total_open_notional += notional

            open_positions.append({
                "symbol": symbol,
                "qty": qty,
                "avg_price": avg_price,
                "latest_price": latest_price,
                "notional": notional,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": round(realized_pnl, 4),
                "updated_at": updated_at,
            })

    # Per-strategy exposure
    strategy_open_qty = _compute_per_strategy_open_qty(connection)
    per_strategy: list[dict] = []
    for strategy_name, symbols in sorted(strategy_open_qty.items()):
        strategy_notional = 0.0
        for symbol, qty in symbols.items():
            price = _get_latest_price(connection, symbol)
            if price is not None:
                strategy_notional += qty * price
        entry: dict = {
            "strategy_name": strategy_name,
            "open_symbols": symbols,
            "open_symbol_count": len(symbols),
            "total_notional": round(strategy_notional, 4),
        }
        if config.max_strategy_notional is not None:
            entry["limit_notional"] = config.max_strategy_notional
            entry["within_limit"] = strategy_notional <= config.max_strategy_notional
        per_strategy.append(entry)

    # Limit check
    violations: list[str] = []
    within_total_limit: Optional[bool] = None
    if config.max_total_notional is not None:
        within_total_limit = total_open_notional <= config.max_total_notional
        if not within_total_limit:
            violations.append(
                f"Total exposure {total_open_notional:.2f} exceeds limit {config.max_total_notional:.2f}."
            )
    for entry in per_strategy:
        if entry.get("within_limit") is False:
            violations.append(
                f"Strategy {entry['strategy_name']} notional {entry['total_notional']:.2f} "
                f"exceeds limit {entry['limit_notional']:.2f}."
            )

    return {
        "config": config.to_dict(),
        "open_positions": open_positions,
        "open_position_count": len(open_positions),
        "total_open_notional": round(total_open_notional, 4),
        "total_realized_pnl": round(total_realized_pnl, 4),
        "per_strategy": per_strategy,
        "violations": violations,
        "within_limits": len(violations) == 0,
    }


# ---------------------------------------------------------------------------
# Risk integration: limit check before approving a BUY
# ---------------------------------------------------------------------------

def check_portfolio_limits(
    connection: DBConnection,
    strategy_name: str,
    symbol: str,
    order_qty: float,
) -> tuple[bool, str]:
    """Return (approved, reason).  Always approved when total_capital == 0.

    Pending approved buys (risk_events APPROVED with no order yet) are included
    in notional calculations to prevent concurrent risk evaluations from
    simultaneously exceeding portfolio limits before either is executed.
    """
    config = get_portfolio_config(connection)
    if config.total_capital <= 0:
        return True, ""

    latest_price = _get_latest_price(connection, symbol)
    if latest_price is None or latest_price <= 0:
        return True, ""   # Can't compute notional — don't block

    proposed_notional = order_qty * latest_price

    # Include in-flight approved buys so concurrent evaluations don't over-allocate.
    pending_total_notional, pending_per_strategy = _compute_pending_approved_notional(
        connection, order_qty
    )

    # 1. Total exposure limit
    if config.max_total_notional is not None:
        if table_exists(connection, "positions"):
            rows = connection.execute(SELECT_OPEN_POSITIONS_SQL).fetchall()
            filled_total = sum(
                float(r[1]) * (p or 0.0)
                for r in rows
                for p in [_get_latest_price(connection, r[0])]
                if p is not None
            )
            current_total = filled_total + pending_total_notional
            if current_total + proposed_notional > config.max_total_notional:
                return (
                    False,
                    f"Portfolio total exposure limit: current={current_total:.2f}, "
                    f"proposed_add={proposed_notional:.2f}, limit={config.max_total_notional:.2f}.",
                )

    # 2. Per-strategy allocation limit
    if config.max_strategy_notional is not None:
        strategy_open = _compute_per_strategy_open_qty(connection)
        strategy_symbols = strategy_open.get(strategy_name, {})
        filled_strategy_notional = sum(
            qty * (p or 0.0)
            for sym, qty in strategy_symbols.items()
            for p in [_get_latest_price(connection, sym)]
            if p is not None
        )
        current_strategy_notional = filled_strategy_notional + pending_per_strategy.get(strategy_name, 0.0)
        if current_strategy_notional + proposed_notional > config.max_strategy_notional:
            return (
                False,
                f"Portfolio strategy allocation limit for {strategy_name}: "
                f"current={current_strategy_notional:.2f}, "
                f"proposed_add={proposed_notional:.2f}, "
                f"limit={config.max_strategy_notional:.2f}.",
            )

    return True, ""
