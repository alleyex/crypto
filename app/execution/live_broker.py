"""
Live broker abstraction for execution backends.

Defines the BrokerClient protocol and a SimulatedBrokerClient for use with
SimulatedLiveExecutionAdapter.  Future real broker integrations (e.g. Binance,
OKX) should implement BrokerClient and be wired in here.
"""
import uuid
from typing import Protocol, Union

from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.core.db import utc_now_iso
from app.core.settings import BINANCE_FUTURES
from app.data.candles_service import get_latest_close
from app.execution.paper_broker import (
    INSERT_FILL_SQL,
    INSERT_ORDER_SQL,
)
from app.execution.queries import (
    SELECT_LATEST_RISK_SQL,
    SELECT_RISK_BY_ID_SQL,
    select_pending_approved_risk_ids,
)
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl
from app.execution.runtime import read_configured_execution_backend
from app.risk.risk_config import get_risk_config

class BrokerClient(Protocol):
    """Protocol that every broker backend must satisfy.

    A real exchange adapter (e.g. BinanceBrokerClient) would call the exchange
    REST/WebSocket API inside ``place_order`` and return actual fill details.
    """

    broker_name: str

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        ref_price: float,
    ) -> dict[str, Union[str, float]]:
        """Submit an order and return fill details.

        Returns a dict with at least:
          - "status"     : str   (e.g. "FILLED", "OPEN")
          - "fill_price" : float
          - "fill_qty"   : float
        """
        ...

class SimulatedBrokerClient:
    """Broker client that simulates immediate fills at the latest close price.

    Behaviour is intentionally identical to the paper broker in terms of fill
    economics, but the order flow passes through the BrokerClient abstraction so
    that swapping in a real exchange client requires only changing this class.
    """

    broker_name = "simulated"

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        ref_price: float,
    ) -> dict[str, Union[str, float]]:
        return {
            "status": "FILLED",
            "fill_price": ref_price,
            "fill_qty": qty,
        }

def _binance_futures_execution_enabled() -> bool:
    return read_configured_execution_backend() == "binance" and BINANCE_FUTURES

def _get_strategy_target_position(
    strategy_name: str,
    symbol: str,
    timeframe: str,
) -> int | None:
    if strategy_name != "ppo":
        return None
    try:
        from app.strategy.ppo_strategy import get_runtime_target_position

        return get_runtime_target_position(symbol, timeframe)
    except Exception:
        return None

def _get_binance_position_qty(
    broker_client: BrokerClient,
    symbol: str,
) -> float | None:
    get_positions = getattr(broker_client, "get_positions", None)
    if not callable(get_positions):
        return None
    try:
        positions = get_positions(symbol=symbol, include_flat=True)
    except Exception:
        return None
    if not positions:
        return 0.0
    return float(positions[0].get("qty") or 0.0)

def _error_payload(exc: Exception) -> dict[str, object]:
    payload: dict[str, object] = {
        "error_type": exc.__class__.__name__,
        "error_message": str(exc),
    }
    if hasattr(exc, "to_payload") and callable(getattr(exc, "to_payload")):
        extra = exc.to_payload()
        if isinstance(extra, dict):
            payload.update(extra)
    return payload

def execute_risk_event_id(
    connection: DBConnection,
    risk_event_id: int,
    broker_client: BrokerClient,
    order_qty: float = 0.001,
) -> dict[str, Union[float, str, int]] | None:
    risk_event = connection.execute(SELECT_RISK_BY_ID_SQL, (risk_event_id,)).fetchone()
    if risk_event is None:
        return None

    risk_event_id, _, symbol, timeframe, strategy_name, signal_type, decision = risk_event
    if decision != "APPROVED":
        return {"risk_event_id": risk_event_id, "decision": decision}
    if signal_type not in ("BUY", "SELL"):
        return {"risk_event_id": risk_event_id, "decision": "SKIPPED", "signal_type": signal_type}

    existing_order = connection.execute(
        "SELECT id FROM orders WHERE risk_event_id = ? LIMIT 1;",
        (risk_event_id,),
    ).fetchone()
    if existing_order is not None:
        return {"risk_event_id": risk_event_id, "decision": "SKIPPED", "reason": "Already executed"}

    ref_price = get_latest_close(connection, symbol=symbol, timeframe=timeframe)
    if ref_price is None:
        return {"risk_event_id": risk_event_id, "decision": "SKIPPED", "reason": "No candle data"}

    # Use strategy-level risk config order_qty if available
    try:
        strategy_cfg, _ = get_risk_config(connection, strategy_name)
        resolved_qty = float(strategy_cfg.order_qty or order_qty)
    except Exception:
        resolved_qty = order_qty

    target_position = _get_strategy_target_position(strategy_name, symbol, timeframe)
    execution_target_qty = None
    current_position_qty = None
    if _binance_futures_execution_enabled() and target_position in (-1, 0, 1):
        current_position_qty = _get_binance_position_qty(broker_client, symbol)
        if current_position_qty is None:
            # Cannot determine current Binance position — skip execution to prevent
            # uncontrolled accumulation.  A subsequent pipeline cycle will retry.
            insert_event(
                connection,
                event_type="order",
                status="skipped",
                source="live_broker",
                message=f"Skipped {signal_type} for {symbol}: Binance position query failed.",
                payload={
                    "risk_event_id": risk_event_id,
                    "symbol": symbol,
                    "strategy_name": strategy_name,
                    "target_position": target_position,
                    "reason": "binance_position_query_failed",
                },
            )
            return {
                "risk_event_id": risk_event_id,
                "decision": "SKIPPED",
                "reason": "Binance position query failed — cannot calculate delta safely",
                "target_position": target_position,
            }
        execution_target_qty = float(resolved_qty) * float(target_position)
        delta_qty = execution_target_qty - current_position_qty
        if abs(delta_qty) <= 1e-9:
            return {
                "risk_event_id": risk_event_id,
                "decision": "SKIPPED",
                "reason": "Already at target position",
                "target_position": target_position,
                "current_position_qty": current_position_qty,
            }
        signal_type = "BUY" if delta_qty > 0 else "SELL"
        resolved_qty = abs(delta_qty)

    try:
        fill_result = broker_client.place_order(
            symbol=symbol,
            side=signal_type,
            qty=resolved_qty,
            ref_price=ref_price,
        )
    except Exception as exc:
        insert_event(
            connection,
            event_type="order",
            status="failed",
            source="live_broker",
            message=f"{signal_type} order failed for {symbol} via {broker_client.broker_name}.",
            payload={
                "risk_event_id": risk_event_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_name": strategy_name,
                "side": signal_type,
            "qty": resolved_qty,
            "broker": broker_client.broker_name,
            "ref_price": ref_price,
            "target_position": target_position,
            "target_qty": execution_target_qty,
            "current_position_qty": current_position_qty,
            **_error_payload(exc),
        },
    )
        raise
    fill_price = float(fill_result.get("fill_price") or 0.0)
    fill_qty = float(fill_result.get("fill_qty") or 0.0)
    order_status = str(fill_result["status"])
    broker_order_id = fill_result.get("order_id")
    commission = fill_result.get("commission") or None
    commission_asset = fill_result.get("commission_asset") or None
    quote_qty = fill_result.get("quote_qty") or None
    transact_time = fill_result.get("transact_time") or None
    order_price = fill_price if fill_price > 0 else float(ref_price)
    has_real_fill = fill_qty > 0 and order_status.upper() in {"FILLED", "PARTIALLY_FILLED"}

    client_order_id = str(uuid.uuid4())
    order_id = insert_and_get_rowid(
        connection,
        INSERT_ORDER_SQL,
        (
            client_order_id,
            risk_event_id,
            broker_client.broker_name,
            str(broker_order_id) if broker_order_id not in (None, "") else None,
            symbol,
            timeframe,
            strategy_name,
            signal_type,
            resolved_qty,
            order_price,
            order_status,
            utc_now_iso(),
        ),
    )
    if has_real_fill:
        insert_and_get_rowid(
            connection,
            INSERT_FILL_SQL,
            (
                order_id,
                symbol,
                signal_type,
                fill_qty,
                fill_price,
                commission,
                commission_asset,
                quote_qty,
                transact_time,
                utc_now_iso(),
            ),
        )
        rebuild_daily_realized_pnl(connection)
    connection.commit()
    insert_event(
        connection,
        event_type="order",
        status=order_status.lower(),
        source="live_broker",
        message=f"{signal_type} order {order_status} for {symbol} via {broker_client.broker_name}.",
        payload={
            "order_id": order_id,
            "risk_event_id": risk_event_id,
            "symbol": symbol,
            "timeframe": timeframe,
            "strategy_name": strategy_name,
            "side": signal_type,
            "qty": resolved_qty,
            "price": order_price,
            "status": order_status,
            "broker": broker_client.broker_name,
            "broker_order_id": str(broker_order_id) if broker_order_id not in (None, "") else None,
            "target_position": target_position,
            "target_qty": execution_target_qty,
            "current_position_qty": current_position_qty,
            "fill_qty": fill_qty,
            "fill_price": fill_price if has_real_fill else None,
            "commission": commission,
            "commission_asset": commission_asset,
            "quote_qty": quote_qty,
            "transact_time": transact_time,
        },
    )

    return {
        "risk_event_id": risk_event_id,
        "order_id": order_id,
        "symbol": symbol,
        "side": signal_type,
        "qty": resolved_qty,
        "price": order_price,
        "status": order_status,
        "broker": broker_client.broker_name,
        "broker_order_id": str(broker_order_id) if broker_order_id not in (None, "") else None,
        "target_position": target_position,
        "target_qty": execution_target_qty,
        "current_position_qty": current_position_qty,
        "fill_qty": fill_qty,
        "fill_price": fill_price if has_real_fill else None,
        "commission": commission,
        "commission_asset": commission_asset,
        "quote_qty": quote_qty,
        "transact_time": transact_time,
    }

def execute_latest_risk(
    connection: DBConnection,
    broker_client: BrokerClient,
    order_qty: float = 0.001,
) -> dict[str, Union[float, str, int]] | None:
    latest_risk = connection.execute(SELECT_LATEST_RISK_SQL).fetchone()
    if latest_risk is None:
        return None
    return execute_risk_event_id(connection, int(latest_risk[0]), broker_client, order_qty=order_qty)

def execute_pending_approved_risks(
    connection: DBConnection,
    broker_client: BrokerClient,
    order_qty: float = 0.001,
    symbol_names: list[str] | None = None,
) -> list[dict[str, Union[float, str, int]]]:
    pending_ids = select_pending_approved_risk_ids(connection, symbol_names=symbol_names)
    results: list[dict[str, Union[float, str, int]]] = []
    for rid in pending_ids:
        result = execute_risk_event_id(connection, rid, broker_client, order_qty=order_qty)
        if result is not None:
            results.append(result)
    return results

def execute_risk_event_ids(
    connection: DBConnection,
    risk_event_ids: list[int],
    broker_client: BrokerClient,
    order_qty: float = 0.001,
) -> list[dict[str, Union[float, str, int]]]:
    results: list[dict[str, Union[float, str, int]]] = []
    for rid in list(dict.fromkeys(risk_event_ids)):
        result = execute_risk_event_id(connection, int(rid), broker_client, order_qty=order_qty)
        if result is not None:
            results.append(result)
    return results
