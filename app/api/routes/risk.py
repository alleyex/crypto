from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from app.audit.service import insert_event
from app.core.db import DBConnection

from app.core.settings import (
    COOLDOWN_SECONDS,
    DEFAULT_ORDER_QTY,
    MAX_DAILY_LOSS,
    MAX_POSITION_QTY,
    STOP_LOSS_PCT,
)
from app.query.read_service import get_risk_events
from app.risk.risk_config import (
    delete_risk_config,
    get_risk_config,
    list_risk_configs,
    set_risk_config,
)
from app.api.deps import get_db

router = APIRouter()

class RiskConfigUpdateRequest(BaseModel):
    order_qty: float | None = None
    max_position_qty: float | None = None
    cooldown_seconds: int | None = None
    stop_loss_pct: float | None = None
    max_daily_loss: float | None = None

@router.get("/risk-events")
def risk_events(
    limit: int = Query(default=5, ge=1, le=100),
    connection: DBConnection = Depends(get_db),
) -> list[dict]:
    return get_risk_events(connection, limit=limit)

@router.get("/risk-config")
def list_risk_config(connection: DBConnection = Depends(get_db)) -> dict:
    """List all per-strategy risk configs and the global defaults."""
    overrides = list_risk_configs(connection)
    return {
        "global_defaults": {
            "order_qty": DEFAULT_ORDER_QTY,
            "max_position_qty": MAX_POSITION_QTY,
            "cooldown_seconds": COOLDOWN_SECONDS,
            "stop_loss_pct": STOP_LOSS_PCT,
            "max_daily_loss": MAX_DAILY_LOSS,
        },
        "overrides": overrides,
    }

@router.get("/risk-config/{strategy_name}")
def get_risk_config_for_strategy(
    strategy_name: str,
    connection: DBConnection = Depends(get_db),
) -> dict:
    """Return the effective risk config for a strategy (per-strategy or global defaults)."""
    cfg, is_default = get_risk_config(connection, strategy_name)
    result = cfg.to_dict()
    result["is_default"] = is_default
    return result

@router.post("/risk-config/{strategy_name}")
def update_risk_config_for_strategy(
    strategy_name: str,
    body: RiskConfigUpdateRequest,
    connection: DBConnection = Depends(get_db),
) -> dict:
    """Set or update per-strategy risk config.  Only provided fields are changed."""
    cfg = set_risk_config(
        connection,
        strategy_name,
        order_qty=body.order_qty,
        max_position_qty=body.max_position_qty,
        cooldown_seconds=body.cooldown_seconds,
        stop_loss_pct=body.stop_loss_pct,
        max_daily_loss=body.max_daily_loss,
    )
    insert_event(
        connection,
        event_type="risk_config",
        status="ok",
        source="api",
        message=f"Risk config updated for strategy={strategy_name!r}.",
        payload=cfg.to_dict(),
    )
    return {"status": "ok", "config": cfg.to_dict()}

@router.delete("/risk-config/{strategy_name}")
def reset_risk_config_for_strategy(
    strategy_name: str,
    connection: DBConnection = Depends(get_db),
) -> dict:
    """Remove per-strategy override; the strategy reverts to global defaults."""
    deleted = delete_risk_config(connection, strategy_name)
    insert_event(
        connection,
        event_type="risk_config",
        status="ok",
        source="api",
        message=f"Risk config override deleted for strategy={strategy_name!r}. deleted={deleted}",
        payload={"strategy_name": strategy_name, "deleted": deleted},
    )
    return {"status": "ok", "deleted": deleted}
