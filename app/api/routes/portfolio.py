
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.audit.service import insert_event
from app.core.db import DBConnection

from app.portfolio.portfolio_service import (
    get_portfolio_config,
    get_portfolio_summary,
    set_portfolio_config,
)
from app.api.deps import get_db

router = APIRouter()

class PortfolioConfigUpdateRequest(BaseModel):
    total_capital: float | None = None
    max_strategy_allocation_pct: float | None = None
    max_total_exposure_pct: float | None = None

@router.get("/portfolio")
def portfolio_summary(connection: DBConnection = Depends(get_db)) -> dict:
    """Cross-strategy exposure summary with per-position notional and limit violations."""
    return get_portfolio_summary(connection)

@router.get("/portfolio/config")
def portfolio_config(connection: DBConnection = Depends(get_db)) -> dict:
    """Return current portfolio capital and exposure limit config."""
    return get_portfolio_config(connection).to_dict()

@router.post("/portfolio/config")
def update_portfolio_config(
    body: PortfolioConfigUpdateRequest,
    connection: DBConnection = Depends(get_db),
) -> dict:
    """Set portfolio capital and exposure limits.  Only provided fields are changed."""
    cfg = set_portfolio_config(
        connection,
        total_capital=body.total_capital,
        max_strategy_allocation_pct=body.max_strategy_allocation_pct,
        max_total_exposure_pct=body.max_total_exposure_pct,
    )
    insert_event(
        connection,
        event_type="portfolio_config",
        status="ok",
        source="api",
        message="Portfolio config updated.",
        payload=cfg.to_dict(),
    )
    return {"status": "ok", "config": cfg.to_dict()}
