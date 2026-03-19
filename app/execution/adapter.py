from typing import Dict, List, Optional, Protocol, Union

from app.core.db import DBConnection
from app.core.settings import EXECUTION_BACKEND
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


class NoopExecutionAdapter:
    name = "noop"

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


def get_execution_adapter() -> ExecutionAdapter:
    if EXECUTION_BACKEND == "paper":
        return PaperExecutionAdapter()
    if EXECUTION_BACKEND == "noop":
        return NoopExecutionAdapter()
    raise ValueError(f"Unsupported execution backend: {EXECUTION_BACKEND}")


def get_execution_adapter_name() -> str:
    return get_execution_adapter().name
