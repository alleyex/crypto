import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.core.db import get_connection
from app.execution.adapter import get_execution_adapter
from app.execution.adapter import get_execution_adapter_name


def main() -> None:
    connection = get_connection()
    execution_adapter = get_execution_adapter()
    try:
        execution_adapter.ensure_tables(connection)
        result = execution_adapter.execute_latest_risk(connection)
    finally:
        connection.close()

    if result is None:
        print("No risk event found.")
        return

    if result.get("status") == "FILLED":
        print(f"executed_signal={result['side']}")
        print(f"qty={result['qty']}")
        print(f"price={result['price']}")
        print("order_status=FILLED")
        return

    if "decision" in result:
        print(f"execution_backend={get_execution_adapter_name()}")
        print(f"Latest risk event is not executable: {result['decision']}")
    else:
        print(f"Execution skipped: {result}")


if __name__ == "__main__":
    main()
