from typing import Dict, List, Optional, Protocol, Union

from app.core.db import DBConnection
from app.execution import paper_broker


ExecutionResult = Dict[str, Union[float, str, int]]


class ExecutionAdapter(Protocol):
    name: str

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


def get_execution_adapter() -> ExecutionAdapter:
    return PaperExecutionAdapter()


def get_execution_adapter_name() -> str:
    return get_execution_adapter().name
