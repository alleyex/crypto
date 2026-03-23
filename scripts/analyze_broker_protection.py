import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.api.main import build_health_report
from app.core.db import get_connection
from app.core.db import parse_db_timestamp


DEFAULT_LIMIT = 10
EXPECTED_REJECTION_REASONS = (
    "Signal is HOLD.",
    "Duplicate signal type.",
)


def _row_to_signal(row: tuple[Any, ...], now_ts) -> dict[str, Any]:
    created_at = row[5]
    age_seconds = int((now_ts - parse_db_timestamp(created_at)).total_seconds())
    return {
        "id": row[0],
        "symbol": row[1],
        "timeframe": row[2],
        "strategy_name": row[3],
        "signal_type": row[4],
        "created_at": created_at,
        "age_seconds": age_seconds,
    }


def _classify_reason(reason: str) -> str:
    if reason in EXPECTED_REJECTION_REASONS or reason.startswith("Cooldown active:"):
        return "expected"
    return "anomalous"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze broker protection state and recent risk streaks.")
    parser.add_argument("--symbol", help="Filter to a symbol inferred from latest risk/fill/order when omitted.")
    parser.add_argument("--strategy", help="Filter to a strategy inferred from latest risk/run when omitted.")
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="Number of recent rows to show per section.")
    parser.add_argument("--json", action="store_true", help="Emit raw JSON instead of a readable summary.")
    args = parser.parse_args()

    health = build_health_report()
    broker = health.get("checks", {}).get("broker_protection", {})
    pipeline = health.get("checks", {}).get("pipeline", {})

    inferred_symbol = args.symbol
    inferred_strategy = args.strategy
    for candidate in (
        broker.get("latest_risk"),
        broker.get("latest_fill"),
        broker.get("latest_order"),
        pipeline.get("latest_run"),
    ):
        if not isinstance(candidate, dict):
            continue
        inferred_symbol = inferred_symbol or candidate.get("symbol")
        inferred_strategy = inferred_strategy or candidate.get("strategy_name")

    connection = get_connection()
    try:
        now_ts = parse_db_timestamp(health["checked_at"])

        risk_query = """
        SELECT id, symbol, timeframe, strategy_name, signal_type, decision, reason, created_at
        FROM risk_events
        WHERE 1=1
        """
        risk_params: list[Any] = []
        if inferred_symbol:
            risk_query += " AND symbol = ?"
            risk_params.append(inferred_symbol)
        if inferred_strategy:
            risk_query += " AND strategy_name = ?"
            risk_params.append(inferred_strategy)
        risk_query += " ORDER BY id DESC LIMIT ?"
        risk_params.append(args.limit)
        risk_rows = connection.execute(risk_query, tuple(risk_params)).fetchall()

        signal_query = """
        SELECT id, symbol, timeframe, strategy_name, signal_type, created_at
        FROM signals
        WHERE 1=1
        """
        signal_params: list[Any] = []
        if inferred_symbol:
            signal_query += " AND symbol = ?"
            signal_params.append(inferred_symbol)
        if inferred_strategy:
            signal_query += " AND strategy_name = ?"
            signal_params.append(inferred_strategy)
        signal_query += " ORDER BY id DESC LIMIT ?"
        signal_params.append(args.limit)
        signal_rows = connection.execute(signal_query, tuple(signal_params)).fetchall()

        fill_query = """
        SELECT id, symbol, side, qty, price, created_at
        FROM fills
        WHERE 1=1
        """
        fill_params: list[Any] = []
        if inferred_symbol:
            fill_query += " AND symbol = ?"
            fill_params.append(inferred_symbol)
        fill_query += " ORDER BY id DESC LIMIT ?"
        fill_params.append(args.limit)
        fill_rows = connection.execute(fill_query, tuple(fill_params)).fetchall()

        position_rows = connection.execute(
            """
            SELECT symbol, qty, avg_price, realized_pnl, updated_at
            FROM positions
            WHERE (? IS NULL OR symbol = ?)
            ORDER BY symbol ASC
            LIMIT ?;
            """,
            (inferred_symbol, inferred_symbol, args.limit),
        ).fetchall()

        risk_config_rows = connection.execute(
            """
            SELECT strategy_name, order_qty, max_position_qty, cooldown_seconds, max_daily_loss, updated_at
            FROM risk_configs
            WHERE (? IS NULL OR strategy_name = ?)
            ORDER BY strategy_name ASC
            LIMIT ?;
            """,
            (inferred_strategy, inferred_strategy, args.limit),
        ).fetchall()
    finally:
        connection.close()

    recent_risk_events = [
        {
            "id": row[0],
            "symbol": row[1],
            "timeframe": row[2],
            "strategy_name": row[3],
            "signal_type": row[4],
            "decision": row[5],
            "reason": row[6],
            "reason_class": _classify_reason(str(row[6] or "")) if str(row[5]).upper() == "REJECTED" else "approved",
            "created_at": row[7],
            "age_seconds": int((now_ts - parse_db_timestamp(row[7])).total_seconds()),
        }
        for row in risk_rows
    ]
    recent_signals = [_row_to_signal(row, now_ts) for row in signal_rows]
    recent_fills = [
        {
            "id": row[0],
            "symbol": row[1],
            "side": row[2],
            "qty": row[3],
            "price": row[4],
            "created_at": row[5],
            "age_seconds": int((now_ts - parse_db_timestamp(row[5])).total_seconds()),
        }
        for row in fill_rows
    ]
    current_positions = [
        {
            "symbol": row[0],
            "qty": row[1],
            "avg_price": row[2],
            "realized_pnl": row[3],
            "updated_at": row[4],
            "age_seconds": int((now_ts - parse_db_timestamp(row[4])).total_seconds()) if row[4] else None,
        }
        for row in position_rows
    ]
    risk_configs = [
        {
            "strategy_name": row[0],
            "order_qty": row[1],
            "max_position_qty": row[2],
            "cooldown_seconds": row[3],
            "max_daily_loss": row[4],
            "updated_at": row[5],
        }
        for row in risk_config_rows
    ]

    rejected_streak = []
    for event in recent_risk_events:
        if event["decision"] != "REJECTED":
            break
        rejected_streak.append(event)

    summary = {
        "checked_at": health.get("checked_at"),
        "symbol": inferred_symbol,
        "strategy_name": inferred_strategy,
        "broker_protection": broker,
        "pipeline": {
            "latest_run": pipeline.get("latest_run"),
            "latest_signal": pipeline.get("latest_signal"),
            "latest_risk": pipeline.get("latest_risk"),
            "latest_order": pipeline.get("latest_order"),
            "latest_fill": pipeline.get("latest_fill"),
            "unfilled_order_count": pipeline.get("unfilled_order_count"),
        },
        "derived": {
            "rejected_streak_length": len(rejected_streak),
            "rejected_streak_reasons": [event["reason"] for event in rejected_streak],
            "rejected_streak_all_expected": bool(rejected_streak) and all(
                event["reason_class"] == "expected" for event in rejected_streak
            ),
            "latest_rejected_reason_class": rejected_streak[0]["reason_class"] if rejected_streak else None,
        },
        "recent_risk_events": recent_risk_events,
        "recent_signals": recent_signals,
        "recent_fills": recent_fills,
        "current_positions": current_positions,
        "risk_configs": risk_configs,
    }

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    print(f"checked_at={summary['checked_at']}")
    print(f"symbol={summary['symbol'] or 'n/a'} strategy={summary['strategy_name'] or 'n/a'}")
    print(
        "broker_protection="
        f"{broker.get('status', 'unknown')}"
        f" severity={broker.get('severity', 'n/a')}"
        f" reason_code={broker.get('reason_code') or 'n/a'}"
    )
    print(
        "derived_reject_streak="
        f"{summary['derived']['rejected_streak_length']}"
        f" all_expected={summary['derived']['rejected_streak_all_expected']}"
        f" latest_reason_class={summary['derived']['latest_rejected_reason_class'] or 'n/a'}"
    )
    print("")
    print("Recent risk events:")
    for event in recent_risk_events:
        print(
            f"- id={event['id']} {event['created_at']} age={event['age_seconds']}s "
            f"{event['signal_type']} {event['decision']} [{event['reason_class']}] {event['reason']}"
        )
    print("")
    print("Recent signals:")
    for signal in recent_signals:
        print(
            f"- id={signal['id']} {signal['created_at']} age={signal['age_seconds']}s "
            f"{signal['signal_type']} {signal['symbol']} {signal['strategy_name']}"
        )
    print("")
    print("Recent fills:")
    for fill in recent_fills:
        print(
            f"- id={fill['id']} {fill['created_at']} age={fill['age_seconds']}s "
            f"{fill['symbol']} {fill['side']} qty={fill['qty']} price={fill['price']}"
        )
    print("")
    print("Current positions:")
    for position in current_positions:
        print(
            f"- {position['symbol']} qty={position['qty']} avg_price={position['avg_price']} "
            f"realized_pnl={position['realized_pnl']} updated_at={position['updated_at']}"
        )
    print("")
    print("Risk configs:")
    for cfg in risk_configs:
        print(
            f"- {cfg['strategy_name']} order_qty={cfg['order_qty']} max_position_qty={cfg['max_position_qty']} "
            f"cooldown_seconds={cfg['cooldown_seconds']} max_daily_loss={cfg['max_daily_loss']} "
            f"updated_at={cfg['updated_at']}"
        )


if __name__ == "__main__":
    main()
