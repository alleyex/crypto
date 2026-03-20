from typing import Dict, List, Optional, Protocol, Union

from app.core.db import DBConnection
from app.execution import paper_broker
from app.execution import live_broker
from app.execution.live_broker import SimulatedBrokerClient
from app.execution.runtime import read_configured_execution_backend


ExecutionResult = Dict[str, Union[float, str, int]]

_SIMULATED_BROKER = SimulatedBrokerClient()


class ExecutionAdapter(Protocol):
    name: str
    description: str
    dry_run: bool
    can_execute_orders: bool
    is_live: bool
    placeholder: bool

    def ensure_tables(self, connection: DBConnection) -> None: ...

    def execute_latest_risk(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
    ) -> Optional[ExecutionResult]: ...

    def execute_pending_approved_risks(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
        symbol_names: Optional[List[str]] = None,
    ) -> List[ExecutionResult]: ...

    def execute_risk_event_ids(
        self,
        connection: DBConnection,
        risk_event_ids: List[int],
        order_qty: float = 0.001,
    ) -> List[ExecutionResult]: ...


class PaperExecutionAdapter:
    name = "paper"
    description = "Paper broker execution backend."
    dry_run = False
    can_execute_orders = True
    is_live = False
    placeholder = False

    def ensure_tables(self, connection: DBConnection) -> None:
        paper_broker.ensure_tables(connection)

    def execute_latest_risk(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
    ) -> Optional[ExecutionResult]:
        return paper_broker.execute_latest_risk(connection, order_qty=order_qty)

    def execute_pending_approved_risks(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
        symbol_names: Optional[List[str]] = None,
    ) -> List[ExecutionResult]:
        return paper_broker.execute_pending_approved_risks(
            connection,
            order_qty=order_qty,
            symbol_names=symbol_names,
        )

    def execute_risk_event_ids(
        self,
        connection: DBConnection,
        risk_event_ids: List[int],
        order_qty: float = 0.001,
    ) -> List[ExecutionResult]:
        return paper_broker.execute_risk_event_ids(
            connection,
            risk_event_ids,
            order_qty=order_qty,
        )


class NoopExecutionAdapter:
    name = "noop"
    description = "No-op execution backend for dry-run validation."
    dry_run = True
    can_execute_orders = False
    is_live = False
    placeholder = False

    def ensure_tables(self, connection: DBConnection) -> None:
        paper_broker.ensure_tables(connection)

    def execute_latest_risk(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
    ) -> Optional[ExecutionResult]:
        latest_risk = connection.execute(paper_broker.SELECT_LATEST_RISK_SQL).fetchone()
        if latest_risk is None:
            return None
        return {
            "risk_event_id": int(latest_risk[0]),
            "decision": "SKIPPED",
            "reason": "Execution backend noop",
        }

    def execute_pending_approved_risks(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
        symbol_names: Optional[List[str]] = None,
    ) -> List[ExecutionResult]:
        return [
            {
                "risk_event_id": int(risk_event_id),
                "decision": "SKIPPED",
                "reason": "Execution backend noop",
            }
            for risk_event_id in paper_broker._select_pending_approved_risk_ids(connection, symbol_names=symbol_names)
        ]

    def execute_risk_event_ids(
        self,
        connection: DBConnection,
        risk_event_ids: List[int],
        order_qty: float = 0.001,
    ) -> List[ExecutionResult]:
        return [
            {
                "risk_event_id": int(risk_event_id),
                "decision": "SKIPPED",
                "reason": "Execution backend noop",
            }
            for risk_event_id in list(dict.fromkeys(risk_event_ids))
        ]


class SimulatedLiveExecutionAdapter:
    """Live-style execution adapter backed by SimulatedBrokerClient.

    Unlike PaperExecutionAdapter, this adapter routes all order placement
    through the BrokerClient abstraction defined in live_broker.  Swapping
    SimulatedBrokerClient for a real exchange client (e.g. BinanceBrokerClient)
    is the only change needed to enable live trading.
    """

    name = "simulated_live"
    description = "Live-style execution backend backed by a simulated broker client."
    dry_run = False
    can_execute_orders = True
    is_live = False
    placeholder = False

    def ensure_tables(self, connection: DBConnection) -> None:
        paper_broker.ensure_tables(connection)

    def execute_latest_risk(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
    ) -> Optional[ExecutionResult]:
        return live_broker.execute_latest_risk(connection, _SIMULATED_BROKER, order_qty=order_qty)

    def execute_pending_approved_risks(
        self,
        connection: DBConnection,
        order_qty: float = 0.001,
        symbol_names: Optional[List[str]] = None,
    ) -> List[ExecutionResult]:
        return live_broker.execute_pending_approved_risks(
            connection,
            _SIMULATED_BROKER,
            order_qty=order_qty,
            symbol_names=symbol_names,
        )

    def execute_risk_event_ids(
        self,
        connection: DBConnection,
        risk_event_ids: List[int],
        order_qty: float = 0.001,
    ) -> List[ExecutionResult]:
        return live_broker.execute_risk_event_ids(
            connection,
            risk_event_ids,
            _SIMULATED_BROKER,
            order_qty=order_qty,
        )


def get_execution_adapter() -> ExecutionAdapter:
    configured_backend = read_configured_execution_backend()
    if configured_backend == "paper":
        return PaperExecutionAdapter()
    if configured_backend == "noop":
        return NoopExecutionAdapter()
    if configured_backend == "simulated_live":
        return SimulatedLiveExecutionAdapter()
    raise ValueError(f"Unsupported execution backend: {configured_backend}")


def get_execution_adapter_name() -> str:
    return get_execution_adapter().name


def get_execution_backend_status() -> dict[str, Union[bool, str]]:
    adapter = get_execution_adapter()
    return {
        "backend": adapter.name,
        "description": adapter.description,
        "dry_run": bool(adapter.dry_run),
        "can_execute_orders": bool(adapter.can_execute_orders),
        "is_live": bool(adapter.is_live),
        "placeholder": bool(adapter.placeholder),
        "status": "ok",
    }
