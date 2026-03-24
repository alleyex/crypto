"""Phase 1 — Candle data validation for feature engineering.

Checks:
  1. Row count and time range per symbol+timeframe
  2. Column type correctness (numeric fields)
  3. Negative value check (volume, trades)
  4. taker_base <= volume integrity
  5. UTC timezone assumption (open_time is epoch-ms)
  6. Gap detection (missing candles)
  7. Longest consecutive run (must be >= MIN_VALID_ROWS for feature computation)
"""

import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.core.db import get_connection
from app.data.candles_service import TIMEFRAME_INTERVAL_MS

MIN_VALID_ROWS = 120  # minimum consecutive candles needed for feature computation
GAP_TOLERANCE = 1.5   # diff > interval * tolerance → gap


def fmt_time(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def check_pair(connection, symbol: str, timeframe: str) -> dict:
    interval_ms = TIMEFRAME_INTERVAL_MS.get(timeframe, 60_000)

    rows = connection.execute(
        """
        SELECT open_time, open, high, low, close, volume,
               quote_asset_volume, number_of_trades,
               taker_buy_base_volume, taker_buy_quote_volume
        FROM candles
        WHERE symbol = ? AND timeframe = ?
        ORDER BY open_time ASC
        """,
        (symbol, timeframe),
    ).fetchall()

    total = len(rows)
    if total == 0:
        return {"symbol": symbol, "timeframe": timeframe, "total": 0, "status": "EMPTY"}

    issues = []

    # --- 1. Time range ---
    first_ms = rows[0][0]
    last_ms  = rows[-1][0]

    # --- 2. Type check + value checks ---
    negative_vol   = 0
    negative_trade = 0
    taker_exceeds  = 0
    null_fields    = 0

    for r in rows:
        ot, op, hi, lo, cl, vol, qvol, trades, tbase, tquote = r

        # Null / None checks on critical fields
        if None in (op, hi, lo, cl, vol):
            null_fields += 1

        # Negative volume
        if vol is not None and float(vol) < 0:
            negative_vol += 1

        # Negative trades
        if trades is not None and int(trades) < 0:
            negative_trade += 1

        # taker_base <= volume
        if tbase is not None and vol is not None:
            if float(tbase) > float(vol) * 1.001:  # allow 0.1% rounding
                taker_exceeds += 1

    if null_fields:
        issues.append(f"null critical fields: {null_fields} rows")
    if negative_vol:
        issues.append(f"negative volume: {negative_vol} rows")
    if negative_trade:
        issues.append(f"negative trades: {negative_trade} rows")
    if taker_exceeds:
        issues.append(f"taker_base > volume: {taker_exceeds} rows")

    # --- 3. Duplicate open_time ---
    times = [r[0] for r in rows]
    duplicates = total - len(set(times))
    if duplicates:
        issues.append(f"duplicate open_time: {duplicates}")

    # --- 4. Gap detection + longest consecutive run ---
    gaps = []
    run = 1
    max_run = 1
    current_run = 1

    for i in range(1, len(times)):
        diff = times[i] - times[i - 1]
        if diff > interval_ms * GAP_TOLERANCE:
            missing = round(diff / interval_ms) - 1
            gaps.append({"at": fmt_time(times[i - 1]), "missing": missing})
            current_run = 1
        else:
            current_run += 1
            max_run = max(max_run, current_run)

    # --- 5. Determine status ---
    if issues:
        status = "WARN"
    elif gaps:
        status = "GAPS"
    else:
        status = "OK"

    return {
        "symbol":        symbol,
        "timeframe":     timeframe,
        "total":         total,
        "first":         fmt_time(first_ms),
        "last":          fmt_time(last_ms),
        "gaps":          len(gaps),
        "gap_details":   gaps[:5],  # show first 5
        "max_run":       max_run,
        "ready":         max_run >= MIN_VALID_ROWS,
        "issues":        issues,
        "status":        status,
    }


def print_report(result: dict) -> None:
    sym = result["symbol"]
    tf  = result["timeframe"]
    sep = "─" * 52

    print(f"\n{sep}")
    print(f"  {sym} / {tf}")
    print(sep)

    if result["status"] == "EMPTY":
        print("  ⚠️  No data found.")
        return

    # Summary
    print(f"  Total rows   : {result['total']:,}")
    print(f"  First candle : {result['first']} UTC")
    print(f"  Last candle  : {result['last']} UTC")
    print(f"  Gaps found   : {result['gaps']}")
    print(f"  Longest run  : {result['max_run']:,} candles", end="")
    print(f"  {'✅ ready' if result['ready'] else f'❌ need {MIN_VALID_ROWS}+'}")

    # Issues
    if result["issues"]:
        print(f"\n  ⚠️  Issues:")
        for issue in result["issues"]:
            print(f"     • {issue}")
    else:
        print(f"\n  ✅ No data integrity issues")

    # Gap details
    if result["gap_details"]:
        print(f"\n  Gap details (first {len(result['gap_details'])}):")
        for g in result["gap_details"]:
            print(f"     • {g['at']} UTC — {g['missing']} missing candle(s)")

    # Overall status
    label = {"OK": "✅ PASS", "GAPS": "⚠️  PASS with gaps", "WARN": "❌ FAIL"}.get(result["status"], result["status"])
    print(f"\n  Status : {label}")


def main():
    connection = get_connection()
    try:
        pairs = connection.execute(
            "SELECT DISTINCT symbol, timeframe FROM candles ORDER BY symbol, timeframe"
        ).fetchall()

        if not pairs:
            print("No candle data found in database.")
            return

        print("\n╔══════════════════════════════════════════════════════╗")
        print("║     Phase 1 — Candle Validation for Features        ║")
        print(f"║     MIN_VALID_ROWS = {MIN_VALID_ROWS}                              ║")
        print("╚══════════════════════════════════════════════════════╝")

        all_ready = True
        for symbol, timeframe in pairs:
            result = check_pair(connection, symbol, timeframe)
            print_report(result)
            if not result.get("ready", False):
                all_ready = False

        print("\n" + "═" * 52)
        if all_ready:
            print("  ✅ All pairs ready — proceed to Phase 4")
        else:
            print(f"  ⚠️  Some pairs have < {MIN_VALID_ROWS} consecutive candles")
            print("     Run market data fetch to collect more history.")
        print("═" * 52 + "\n")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
