from dataclasses import dataclass
from typing import Optional

from app.core.db import DBConnection
from app.core.db import table_exists
from app.core.settings import COOLDOWN_SECONDS
from app.core.settings import DEFAULT_ORDER_QTY
from app.core.settings import MAX_DAILY_LOSS
from app.core.settings import MAX_POSITION_QTY


@dataclass
class RiskConfig:
    strategy_name: str
    order_qty: float
    max_position_qty: float
    cooldown_seconds: int
    max_daily_loss: float

    def to_dict(self, updated_at: Optional[str] = None) -> dict:
        d = {
            "strategy_name": self.strategy_name,
            "order_qty": self.order_qty,
            "max_position_qty": self.max_position_qty,
            "cooldown_seconds": self.cooldown_seconds,
            "max_daily_loss": self.max_daily_loss,
            "is_default": False,
        }
        if updated_at is not None:
            d["updated_at"] = updated_at
        return d


SELECT_RISK_CONFIG_SQL = """
SELECT order_qty, max_position_qty, cooldown_seconds, max_daily_loss, updated_at
FROM risk_configs
WHERE strategy_name = ?;
"""

SELECT_ALL_RISK_CONFIGS_SQL = """
SELECT strategy_name, order_qty, max_position_qty, cooldown_seconds, max_daily_loss, updated_at
FROM risk_configs
ORDER BY strategy_name;
"""

UPSERT_RISK_CONFIG_SQL = """
INSERT INTO risk_configs (strategy_name, order_qty, max_position_qty, cooldown_seconds, max_daily_loss, updated_at)
VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT (strategy_name) DO UPDATE SET
    order_qty = excluded.order_qty,
    max_position_qty = excluded.max_position_qty,
    cooldown_seconds = excluded.cooldown_seconds,
    max_daily_loss = excluded.max_daily_loss,
    updated_at = excluded.updated_at;
"""

DELETE_RISK_CONFIG_SQL = "DELETE FROM risk_configs WHERE strategy_name = ?;"


def _global_defaults(strategy_name: str) -> RiskConfig:
    return RiskConfig(
        strategy_name=strategy_name,
        order_qty=DEFAULT_ORDER_QTY,
        max_position_qty=MAX_POSITION_QTY,
        cooldown_seconds=COOLDOWN_SECONDS,
        max_daily_loss=MAX_DAILY_LOSS,
    )


def get_risk_config(connection: DBConnection, strategy_name: str) -> tuple["RiskConfig", bool]:
    """Return (RiskConfig, is_default).  is_default=True when no DB row exists."""
    if not table_exists(connection, "risk_configs"):
        return _global_defaults(strategy_name), True
    row = connection.execute(SELECT_RISK_CONFIG_SQL, (strategy_name,)).fetchone()
    if row is None:
        return _global_defaults(strategy_name), True
    cfg = RiskConfig(
        strategy_name=strategy_name,
        order_qty=float(row[0]),
        max_position_qty=float(row[1]),
        cooldown_seconds=int(row[2]),
        max_daily_loss=float(row[3]),
    )
    return cfg, False


def set_risk_config(
    connection: DBConnection,
    strategy_name: str,
    order_qty: Optional[float] = None,
    max_position_qty: Optional[float] = None,
    cooldown_seconds: Optional[int] = None,
    max_daily_loss: Optional[float] = None,
) -> RiskConfig:
    """Upsert per-strategy risk config, merging with existing or global defaults."""
    existing, _ = get_risk_config(connection, strategy_name)
    merged = RiskConfig(
        strategy_name=strategy_name,
        order_qty=order_qty if order_qty is not None else existing.order_qty,
        max_position_qty=max_position_qty if max_position_qty is not None else existing.max_position_qty,
        cooldown_seconds=cooldown_seconds if cooldown_seconds is not None else existing.cooldown_seconds,
        max_daily_loss=max_daily_loss if max_daily_loss is not None else existing.max_daily_loss,
    )
    connection.execute(
        UPSERT_RISK_CONFIG_SQL,
        (merged.strategy_name, merged.order_qty, merged.max_position_qty, merged.cooldown_seconds, merged.max_daily_loss),
    )
    connection.commit()
    return merged


def delete_risk_config(connection: DBConnection, strategy_name: str) -> bool:
    """Remove per-strategy override, reverting to global defaults.  Returns True if a row was deleted."""
    if not table_exists(connection, "risk_configs"):
        return False
    cursor = connection.execute(DELETE_RISK_CONFIG_SQL, (strategy_name,))
    connection.commit()
    return (cursor.rowcount or 0) > 0


def list_risk_configs(connection: DBConnection) -> list[dict]:
    """Return all stored per-strategy risk configs."""
    if not table_exists(connection, "risk_configs"):
        return []
    rows = connection.execute(SELECT_ALL_RISK_CONFIGS_SQL).fetchall()
    return [
        {
            "strategy_name": row[0],
            "order_qty": float(row[1]),
            "max_position_qty": float(row[2]),
            "cooldown_seconds": int(row[3]),
            "max_daily_loss": float(row[4]),
            "updated_at": row[5],
        }
        for row in rows
    ]
