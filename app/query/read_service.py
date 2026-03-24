import json
from datetime import datetime, timezone
from typing import Any
from typing import Optional

from app.core.db import DBConnection
from app.core.db import fetch_all_as_dicts
from app.core.db import parse_db_timestamp
from app.strategy.registry import list_registered_strategies


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _fetch_all(connection: DBConnection, query: str, limit: int = 5) -> list[dict[str, Any]]:
    return fetch_all_as_dicts(connection, query, (limit,))


def _decode_json_field(value: Any) -> Any:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return None


_CANDLES_COLUMNS = """
    id,
    symbol,
    timeframe,
    open_time,
    open,
    high,
    low,
    close,
    volume,
    close_time,
    quote_asset_volume,
    number_of_trades,
    taker_buy_base_volume,
    taker_buy_quote_volume,
    created_at"""

SELECT_CANDLES_SQL = f"""
SELECT{_CANDLES_COLUMNS}
FROM candles
ORDER BY open_time DESC
LIMIT ?;
"""

SELECT_CANDLES_BY_SYMBOL_SQL = f"""
SELECT{_CANDLES_COLUMNS}
FROM candles
WHERE symbol = ?
ORDER BY open_time DESC
LIMIT ?;
"""



SELECT_SIGNALS_SQL = """
SELECT
    id,
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    short_ma,
    long_ma,
    created_at
FROM signals
ORDER BY id DESC
LIMIT ?;
"""


SELECT_RISK_EVENTS_SQL = """
SELECT
    id,
    signal_id,
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    decision,
    reason,
    created_at
FROM risk_events
ORDER BY id DESC
LIMIT ?;
"""


SELECT_ORDERS_SQL = """
SELECT
    id,
    client_order_id,
    risk_event_id,
    broker_name,
    broker_order_id,
    symbol,
    timeframe,
    strategy_name,
    side,
    qty,
    price,
    status,
    created_at
FROM orders
ORDER BY id DESC
LIMIT ?;
"""


SELECT_FILLS_SQL = """
SELECT
    id,
    order_id,
    symbol,
    side,
    qty,
    price,
    created_at
FROM fills
ORDER BY id DESC
LIMIT ?;
"""


SELECT_POSITIONS_SQL = """
SELECT
    symbol,
    qty,
    avg_price,
    realized_pnl,
    updated_at
FROM positions
ORDER BY symbol ASC
LIMIT ?;
"""


SELECT_PNL_SQL = """
SELECT
    id,
    symbol,
    qty,
    avg_price,
    market_price,
    unrealized_pnl,
    created_at
FROM pnl_snapshots
ORDER BY id DESC
LIMIT ?;
"""


SELECT_AUDIT_EVENTS_SQL = """
SELECT
    id,
    event_type,
    status,
    source,
    message,
    payload_json,
    created_at
FROM audit_events
ORDER BY id DESC
LIMIT ?;
"""


SELECT_JOB_QUEUE_SQL = """
SELECT
    id,
    job_type,
    status,
    payload_json,
    result_json,
    error_message,
    attempt_count,
    created_at,
    started_at,
    completed_at
FROM job_queue
ORDER BY id DESC
LIMIT ?;
"""


def get_candles(connection: DBConnection, limit: int = 5, symbol: Optional[str] = None, timeframes: Optional[list] = None) -> list[dict[str, Any]]:
    if symbol and timeframes:
        placeholders = ",".join("?" * len(timeframes))
        sql = f"SELECT{_CANDLES_COLUMNS}\nFROM candles\nWHERE symbol = ? AND timeframe IN ({placeholders})\nORDER BY open_time DESC\nLIMIT ?;"
        return fetch_all_as_dicts(connection, sql, (symbol, *timeframes, limit))
    if symbol:
        return fetch_all_as_dicts(connection, SELECT_CANDLES_BY_SYMBOL_SQL, (symbol, limit))
    return _fetch_all(connection, SELECT_CANDLES_SQL, limit)


def get_signals(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_SIGNALS_SQL, limit)


def get_risk_events(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_RISK_EVENTS_SQL, limit)


def get_orders(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_ORDERS_SQL, limit)


def get_fills(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_FILLS_SQL, limit)


def get_positions(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_POSITIONS_SQL, limit)


def get_pnl_snapshots(connection: DBConnection, limit: int = 5) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_PNL_SQL, limit)


def get_audit_events(connection: DBConnection, limit: int = 20) -> list[dict[str, Any]]:
    return _fetch_all(connection, SELECT_AUDIT_EVENTS_SQL, limit)


def get_job_queue_jobs(connection: DBConnection, limit: int = 20) -> list[dict[str, Any]]:
    rows = _fetch_all(connection, SELECT_JOB_QUEUE_SQL, limit)
    normalized: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item["payload"] = _decode_json_field(item.get("payload_json"))
        item["result"] = _decode_json_field(item.get("result_json"))
        normalized.append(item)
    return normalized


def get_job_queue_summary(connection: DBConnection) -> dict[str, Any]:
    rows = fetch_all_as_dicts(
        connection,
        """
        SELECT
            status,
            COUNT(*) AS job_count
        FROM job_queue
        GROUP BY status
        ORDER BY status ASC;
        """,
    )
    counts = {str(row["status"]): int(row["job_count"]) for row in rows}
    latest_jobs = get_job_queue_jobs(connection, limit=5)
    job_type_rows = fetch_all_as_dicts(
        connection,
        """
        SELECT
            job_type,
            status,
            COUNT(*) AS job_count
        FROM job_queue
        GROUP BY job_type, status
        ORDER BY job_type ASC, status ASC;
        """,
    )
    attempt_rows = fetch_all_as_dicts(
        connection,
        """
        SELECT
            job_type,
            AVG(CASE WHEN typeof(attempt_count) = 'integer' THEN attempt_count ELSE NULL END) AS avg_attempt_count,
            MAX(CASE WHEN typeof(attempt_count) = 'integer' THEN attempt_count ELSE NULL END) AS max_attempt_count
        FROM job_queue
        GROUP BY job_type
        ORDER BY job_type ASC;
        """,
    )
    overall_attempt_row = fetch_all_as_dicts(
        connection,
        """
        SELECT
            AVG(CASE WHEN typeof(attempt_count) = 'integer' THEN attempt_count ELSE NULL END) AS avg_attempt_count,
            MAX(CASE WHEN typeof(attempt_count) = 'integer' THEN attempt_count ELSE NULL END) AS max_attempt_count
        FROM job_queue;
        """,
    )[0]
    job_type_counts: dict[str, dict[str, int]] = {
        "market_data": {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
        "strategy": {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
        "execution": {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
    }
    for row in job_type_rows:
        job_type = str(row["job_type"])
        status = str(row["status"])
        job_count = int(row["job_count"])
        entry = job_type_counts.setdefault(
            job_type,
            {"queued": 0, "leased": 0, "completed": 0, "failed": 0, "total": 0},
        )
        entry[status] = job_count
        entry["total"] += job_count
    attempt_map = {str(row["job_type"]): row for row in attempt_rows}
    for job_type, entry in job_type_counts.items():
        total = int(entry["total"])
        completed = int(entry["completed"])
        failed = int(entry["failed"])
        attempts = attempt_map.get(job_type, {})
        type_latest_jobs = [job for job in latest_jobs if str(job.get("job_type")) == job_type]
        type_terminal_jobs = [job for job in type_latest_jobs if job["status"] in {"completed", "failed"}]
        entry["success_ratio"] = round(completed / total, 4) if total else 0.0
        entry["failure_ratio"] = round(failed / total, 4) if total else 0.0
        raw_avg = attempts.get("avg_attempt_count")
        raw_max = attempts.get("max_attempt_count")
        entry["avg_attempt_count"] = round(float(raw_avg), 2) if isinstance(raw_avg, (int, float)) else 0.0
        entry["max_attempt_count"] = int(raw_max) if isinstance(raw_max, int) else 0
        entry["latest_failed_job"] = next((job for job in type_latest_jobs if job["status"] == "failed"), None)
        entry["latest_retry_job"] = next((job for job in type_latest_jobs if (int(job["attempt_count"]) if isinstance(job.get("attempt_count"), int) else 0) > 1), None)
        entry["recent_terminal_statuses"] = [
            "F" if job["status"] == "failed" else "C"
            for job in type_terminal_jobs[:3]
        ]
        entry["recent_terminal_trend"] = "".join(entry["recent_terminal_statuses"])
    retry_jobs = [job for job in latest_jobs if (int(job["attempt_count"]) if isinstance(job.get("attempt_count"), int) else 0) > 1]
    latest_failed_job = next((job for job in latest_jobs if job["status"] == "failed"), None)
    latest_retry_job = next((job for job in latest_jobs if (int(job["attempt_count"]) if isinstance(job.get("attempt_count"), int) else 0) > 1), None)
    failure_streak = 0
    terminal_jobs = [job for job in latest_jobs if job["status"] in {"completed", "failed"}]
    for job in terminal_jobs:
        if job["status"] != "failed":
            break
        failure_streak += 1
    total_job_count = sum(counts.values())
    completed_count = counts.get("completed", 0)
    failed_count = counts.get("failed", 0)
    recent_batches: list[dict[str, Any]] = []
    batch_map: dict[str, dict[str, Any]] = {}
    for job in latest_jobs:
        payload = job.get("payload") or {}
        batch_id = payload.get("batch_id")
        if not batch_id:
            continue
        batch_entry = batch_map.get(batch_id)
        if batch_entry is None:
            created_at = job.get("created_at")
            batch_entry = {
                "batch_id": batch_id,
                "job_types": [],
                "statuses": {},
                "strategy_names": payload.get("strategy_names", []),
                "symbol_names": payload.get("symbol_names", []),
                "execution_backend": payload.get("execution_backend"),
                "source": payload.get("source"),
                "orchestration": payload.get("orchestration"),
                "created_at": created_at,
                "age_seconds": int((_utc_now() - parse_db_timestamp(str(created_at))).total_seconds()) if created_at else None,
            }
            batch_map[batch_id] = batch_entry
            recent_batches.append(batch_entry)
        elif job.get("created_at") and batch_entry.get("created_at"):
            current_created_at = parse_db_timestamp(str(batch_entry["created_at"]))
            candidate_created_at = parse_db_timestamp(str(job["created_at"]))
            if candidate_created_at < current_created_at:
                batch_entry["created_at"] = job["created_at"]
                batch_entry["age_seconds"] = int((_utc_now() - candidate_created_at).total_seconds())
        batch_entry["job_types"].append(job["job_type"])
        batch_entry["statuses"][job["job_type"]] = job["status"]
    latest_incomplete_batch = next(
        (
            batch
            for batch in recent_batches
            if any(status != "completed" for status in (batch.get("statuses") or {}).values())
        ),
        None,
    )
    latest_completed_batch = next(
        (
            batch
            for batch in recent_batches
            if batch.get("statuses") and all(status == "completed" for status in batch.get("statuses", {}).values())
        ),
        None,
    )
    return {
        "counts": {
            "queued": counts.get("queued", 0),
            "leased": counts.get("leased", 0),
            "completed": counts.get("completed", 0),
            "failed": counts.get("failed", 0),
            "total": total_job_count,
        },
        "metrics": {
            "success_ratio": round(completed_count / total_job_count, 4) if total_job_count else 0.0,
            "failure_ratio": round(failed_count / total_job_count, 4) if total_job_count else 0.0,
            "avg_attempt_count": round(float(overall_attempt_row.get("avg_attempt_count") or 0.0), 2),
            "max_attempt_count": int(overall_attempt_row.get("max_attempt_count") or 0),
            "retry_job_count": len(retry_jobs),
            "failure_streak": failure_streak,
            "recent_failure_count": len([job for job in latest_jobs if job["status"] == "failed"]),
            "recent_retry_count": len(retry_jobs),
        },
        "job_type_counts": job_type_counts,
        "failed_jobs": [job for job in latest_jobs if job["status"] == "failed"],
        "retry_jobs": retry_jobs,
        "latest_failed_job": latest_failed_job,
        "latest_retry_job": latest_retry_job,
        "recent_batches": recent_batches,
        "latest_incomplete_batch": latest_incomplete_batch,
        "latest_completed_batch": latest_completed_batch,
        "latest_jobs": latest_jobs,
    }


def get_strategy_activity_summary(
    connection: DBConnection,
    per_table_limit: int = 100,
) -> list[dict[str, Any]]:
    strategy_names = list_registered_strategies()
    signals = get_signals(connection, limit=per_table_limit)
    risk_events = get_risk_events(connection, limit=per_table_limit)
    orders = get_orders(connection, limit=per_table_limit)
    fills = get_fills(connection, limit=per_table_limit)
    closed_trades = get_strategy_closed_trades(
        connection,
        limit=max(len(strategy_names), per_table_limit),
        per_table_limit=per_table_limit,
    )
    latest_closed_trades: dict[str, Any] = {}
    for item in closed_trades:
        key = str(item["strategy_name"])
        if key not in latest_closed_trades:
            latest_closed_trades[key] = item

    summaries: list[dict[str, Any]] = []
    for strategy_name in strategy_names:
        latest_signal = next((item for item in signals if item["strategy_name"] == strategy_name), None)
        latest_risk = next((item for item in risk_events if item["strategy_name"] == strategy_name), None)
        latest_order = next((item for item in orders if item["strategy_name"] == strategy_name), None)
        strategy_orders = [item for item in orders if item["strategy_name"] == strategy_name]
        order_ids = {item["id"] for item in strategy_orders}
        latest_fill = next((item for item in fills if item["order_id"] in order_ids), None)
        latest_closed_trade = latest_closed_trades.get(strategy_name)
        latest_activity_at = max(
            (
                timestamp
                for timestamp in (
                    latest_fill["created_at"] if latest_fill is not None else None,
                    latest_order["created_at"] if latest_order is not None else None,
                    latest_risk["created_at"] if latest_risk is not None else None,
                    latest_signal["created_at"] if latest_signal is not None else None,
                )
                if timestamp is not None
            ),
            default=None,
        )
        filled_order_count = sum(1 for item in strategy_orders if item["status"] == "FILLED")
        filled_orders = list(reversed([item for item in strategy_orders if item["status"] == "FILLED"]))

        gross_realized_pnl = 0.0
        filled_qty_total = 0.0
        buy_fill_count = 0
        sell_fill_count = 0
        realized_trade_count = 0
        winning_trade_count = 0
        losing_trade_count = 0
        breakeven_trade_count = 0
        positions_by_symbol: dict[str, dict[str, float]] = {}

        for order in filled_orders:
            symbol = order["symbol"]
            qty = float(order["qty"])
            price = float(order["price"])
            filled_qty_total += qty
            position = positions_by_symbol.setdefault(symbol, {"qty": 0.0, "cost": 0.0})

            if order["side"] == "BUY":
                buy_fill_count += 1
                position["qty"] += qty
                position["cost"] += qty * price
            elif order["side"] == "SELL" and position["qty"] > 0:
                sell_fill_count += 1
                sell_qty = min(qty, position["qty"])
                average_cost = position["cost"] / position["qty"]
                position["qty"] -= sell_qty
                position["cost"] -= sell_qty * average_cost
                realized_pnl = (price - average_cost) * sell_qty
                gross_realized_pnl += realized_pnl
                realized_trade_count += 1
                if realized_pnl > 0:
                    winning_trade_count += 1
                elif realized_pnl < 0:
                    losing_trade_count += 1
                else:
                    breakeven_trade_count += 1

        net_position_qty = sum(item["qty"] for item in positions_by_symbol.values())
        open_entry_price = None
        for pos in positions_by_symbol.values():
            if pos["qty"] > 0:
                open_entry_price = pos["cost"] / pos["qty"]
                break

        # Current price from latest candle for this strategy's symbol/timeframe
        current_price: float | None = None
        price_symbol: str | None = None
        sig_symbol = latest_signal["symbol"] if latest_signal else None
        sig_timeframe = latest_signal["timeframe"] if latest_signal else None
        if sig_symbol and sig_timeframe:
            price_row = connection.execute(
                "SELECT close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY open_time DESC LIMIT 1",
                (sig_symbol, sig_timeframe),
            ).fetchone()
            if price_row:
                current_price = float(price_row[0])
                price_symbol = sig_symbol

        summaries.append(
            {
                "strategy_name": strategy_name,
                "latest_signal": latest_signal,
                "latest_risk": latest_risk,
                "latest_order": latest_order,
                "latest_fill": latest_fill,
                "latest_closed_trade": latest_closed_trade,
                "latest_activity_at": latest_activity_at,
                "latest_order_at": latest_order["created_at"] if latest_order is not None else None,
                "latest_fill_at": latest_fill["created_at"] if latest_fill is not None else None,
                "filled_order_count": filled_order_count,
                "filled_qty_total": filled_qty_total,
                "net_position_qty": net_position_qty,
                "open_entry_price": open_entry_price,
                "current_price": current_price,
                "price_symbol": price_symbol,
                "gross_realized_pnl": gross_realized_pnl,
                "buy_fill_count": buy_fill_count,
                "sell_fill_count": sell_fill_count,
                "realized_trade_count": realized_trade_count,
                "winning_trade_count": winning_trade_count,
                "losing_trade_count": losing_trade_count,
                "breakeven_trade_count": breakeven_trade_count,
                "has_activity": any(
                    item is not None for item in (latest_signal, latest_risk, latest_order, latest_fill)
                ),
            }
        )

    return summaries


def get_strategy_closed_trades(
    connection: DBConnection,
    limit: int = 20,
    per_table_limit: int = 200,
    strategy_name: Optional[str] = None,
) -> list[dict[str, Any]]:
    orders = get_orders(connection, limit=per_table_limit)
    fills = get_fills(connection, limit=per_table_limit)
    fills_by_order_id = {int(item["order_id"]): item for item in fills}
    strategy_filter = strategy_name.strip() if strategy_name else None
    filled_orders = list(reversed([item for item in orders if item["status"] == "FILLED"]))
    positions_by_key: dict[tuple[str, str], dict[str, float]] = {}
    closed_trades: list[dict[str, Any]] = []

    for order in filled_orders:
        current_strategy_name = str(order["strategy_name"])
        if strategy_filter and current_strategy_name != strategy_filter:
            continue
        symbol = str(order["symbol"])
        key = (current_strategy_name, symbol)
        position = positions_by_key.setdefault(key, {"qty": 0.0, "cost": 0.0})

        qty = float(order["qty"])
        price = float(order["price"])
        if order["side"] == "BUY":
            position["qty"] += qty
            position["cost"] += qty * price
            continue

        if order["side"] != "SELL" or position["qty"] <= 0:
            continue

        close_qty = min(qty, position["qty"])
        average_entry_price = position["cost"] / position["qty"]
        latest_fill = fills_by_order_id.get(int(order["id"]))
        realized_pnl = (price - average_entry_price) * close_qty
        position["qty"] -= close_qty
        position["cost"] -= close_qty * average_entry_price
        closed_trades.append(
            {
                "strategy_name": current_strategy_name,
                "symbol": symbol,
                "qty": close_qty,
                "entry_price": average_entry_price,
                "exit_price": price,
                "realized_pnl": realized_pnl,
                "closed_at": latest_fill["created_at"] if latest_fill is not None else order["created_at"],
                "order_id": order["id"],
                "status": "win" if realized_pnl > 0 else "loss" if realized_pnl < 0 else "breakeven",
            }
        )

    closed_trades.sort(key=lambda item: str(item["closed_at"]), reverse=True)
    return closed_trades[:limit]
