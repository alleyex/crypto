"""
Live broker abstraction for execution backends.

Defines the BrokerClient protocol and a SimulatedBrokerClient for use with
SimulatedLiveExecutionAdapter.  Future real broker integrations (e.g. Binance,
OKX) should implement BrokerClient and be wired in here.
"""
import uuid
from typing import Dict, List, Optional, Protocol, Union

from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
from app.data.candles_service import get_latest_close
from app.execution.paper_broker import (
    INSERT_FILL_SQL,
    INSERT_ORDER_SQL,
    SELECT_LATEST_RISK_SQL,
    SELECT_RISK_BY_ID_SQL,
    _select_pending_approved_risk_ids,
)
from app.portfolio.daily_pnl_service import rebuild_daily_realized_pnl


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
    ) -> Dict[str, Union[str, float]]:
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
    ) -> Dict[str, Union[str, float]]:
        return {
            "status": "FILLED",
            "fill_price": ref_price,
            "fill_qty": qty,
        }


def execute_risk_event_id(
    connection: DBConnection,
    risk_event_id: int,
    broker_client: BrokerClient,
    order_qty: float = 0.001,
) -> Optional[Dict[str, Union[float, str, int]]]:
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

    fill_result = broker_client.place_order(
        symbol=symbol,
        side=signal_type,
        qty=order_qty,
        ref_price=ref_price,
    )
    fill_price = float(fill_result["fill_price"])
    fill_qty = float(fill_result["fill_qty"])
    order_status = str(fill_result["status"])

    client_order_id = str(uuid.uuid4())
    order_id = insert_and_get_rowid(
        connection,
        INSERT_ORDER_SQL,
        (
            client_order_id,
            risk_event_id,
            symbol,
            timeframe,
            strategy_name,
            signal_type,
            fill_qty,
            fill_price,
            order_status,
        ),
    )
    insert_and_get_rowid(
        connection,
        INSERT_FILL_SQL,
        (order_id, symbol, signal_type, fill_qty, fill_price),
    )
    rebuild_daily_realized_pnl(connection)
    connection.commit()

    return {
        "risk_event_id": risk_event_id,
        "order_id": order_id,
        "symbol": symbol,
        "side": signal_type,
        "qty": fill_qty,
        "price": fill_price,
        "status": order_status,
        "broker": broker_client.broker_name,
    }


def execute_latest_risk(
    connection: DBConnection,
    broker_client: BrokerClient,
    order_qty: float = 0.001,
) -> Optional[Dict[str, Union[float, str, int]]]:
    latest_risk = connection.execute(SELECT_LATEST_RISK_SQL).fetchone()
    if latest_risk is None:
        return None
    return execute_risk_event_id(connection, int(latest_risk[0]), broker_client, order_qty=order_qty)


def execute_pending_approved_risks(
    connection: DBConnection,
    broker_client: BrokerClient,
    order_qty: float = 0.001,
    symbol_names: Optional[List[str]] = None,
) -> List[Dict[str, Union[float, str, int]]]:
    pending_ids = _select_pending_approved_risk_ids(connection, symbol_names=symbol_names)
    results: List[Dict[str, Union[float, str, int]]] = []
    for rid in pending_ids:
        result = execute_risk_event_id(connection, rid, broker_client, order_qty=order_qty)
        if result is not None:
            results.append(result)
    return results


def execute_risk_event_ids(
    connection: DBConnection,
    risk_event_ids: List[int],
    broker_client: BrokerClient,
    order_qty: float = 0.001,
) -> List[Dict[str, Union[float, str, int]]]:
    results: List[Dict[str, Union[float, str, int]]] = []
    for rid in list(dict.fromkeys(risk_event_ids)):
        result = execute_risk_event_id(connection, int(rid), broker_client, order_qty=order_qty)
        if result is not None:
            results.append(result)
    return results
