"""
Binance Testnet end-to-end validation script.

Steps:
  1. Account connectivity  — verifies API key / secret and trading permissions
  2. Dry-run order         — validates a BTCUSDT BUY 0.001 without submitting
  3. Full pipeline run     — market_data → strategy → risk → execution
                             (one real paper-sized order on testnet)

Usage:
    CRYPTO_BINANCE_API_KEY=<key> \
    CRYPTO_BINANCE_API_SECRET=<secret> \
    CRYPTO_BINANCE_TESTNET=true \
    CRYPTO_EXECUTION_BACKEND=binance \
    python scripts/validate_binance_testnet.py

All output is printed to stdout.  Exit code 0 = all steps passed.
"""
import json
import os
import sys

# ── ensure project root is on sys.path ────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.db import get_connection
from app.core.migrations import run_migrations
from app.core.settings import BINANCE_API_KEY, BINANCE_API_SECRET, BINANCE_TESTNET
from app.execution.binance_broker import BinanceBrokerClient
from app.pipeline.run_pipeline import run_pipeline_collect


# ── helpers ───────────────────────────────────────────────────────────────────

def _print_step(n: int, title: str) -> None:
    print(f"\n{'='*60}")
    print(f"Step {n}: {title}")
    print("="*60)


def _print_result(label: str, data: dict) -> None:
    print(f"{label}:")
    print(json.dumps(data, indent=2, default=str))


def _fail(msg: str) -> None:
    print(f"\n❌  FAIL — {msg}", file=sys.stderr)
    sys.exit(1)


def _ok(msg: str) -> None:
    print(f"✓  {msg}")


# ── Step 1: credentials present ───────────────────────────────────────────────

def check_credentials() -> None:
    _print_step(1, "Credentials check")
    if not BINANCE_API_KEY:
        _fail("CRYPTO_BINANCE_API_KEY is not set.")
    if not BINANCE_API_SECRET:
        _fail("CRYPTO_BINANCE_API_SECRET is not set.")
    mode = "testnet" if BINANCE_TESTNET else "MAINNET (⚠ live money)"
    _ok(f"API key present ({BINANCE_API_KEY[:6]}…)")
    _ok(f"API secret present")
    _ok(f"Mode: {mode}")


# ── Step 2: account connectivity ──────────────────────────────────────────────

def check_account() -> None:
    _print_step(2, "Account connectivity (GET /api/v3/account)")
    client = BinanceBrokerClient()
    try:
        result = client.check_account_connectivity()
    except Exception as exc:
        _fail(str(exc))
    _print_result("Account", result)
    if result.get("status") != "ok":
        _fail(f"Unexpected status: {result.get('status')}")
    if not result.get("can_trade"):
        _fail("Account does not have trading permission.")
    _ok("Account connectivity OK, trading enabled.")


# ── Step 3: dry-run order ──────────────────────────────────────────────────────

def check_dry_run_order(symbol: str = "BTCUSDT", qty: float = 0.001) -> None:
    _print_step(3, f"Dry-run order (POST /api/v3/order/test  {symbol} BUY {qty})")
    client = BinanceBrokerClient()
    try:
        result = client.check_order_request(symbol, "BUY", qty)
    except Exception as exc:
        _fail(str(exc))
    _print_result("Dry-run order", result)
    if not result.get("validated"):
        _fail("Dry-run order did not pass Binance validation.")
    _ok("Dry-run order accepted by Binance.")


# ── Step 4: full pipeline run ─────────────────────────────────────────────────

def run_full_pipeline() -> None:
    _print_step(4, "Full pipeline run  (market_data → strategy → risk → execution)")
    print("  backend = binance (testnet)\n")

    connection = get_connection()
    try:
        run_migrations(connection)
    finally:
        connection.close()

    result = run_pipeline_collect()
    _print_result("Pipeline result", result)

    steps = result.get("steps", [])
    failed_steps = [s for s in steps if s.get("status") == "failed"]
    if failed_steps:
        _fail(f"Pipeline step failed: {failed_steps[0]}")

    step_names = [s.get("step") for s in steps]
    print(f"\n  Steps executed: {step_names}")

    filled = [s for s in steps if s.get("step") == "paper_execute" and s.get("status") == "FILLED"]
    if filled:
        for f in filled:
            _ok(f"Filled: {f.get('side')} {f.get('qty')} {f.get('symbol')} @ {f.get('price')}")
    else:
        _ok(f"Pipeline completed — no fills this run (normal if signal=HOLD or risk rejected).")


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Binance Testnet Validation")
    print(f"{'─'*60}")

    check_credentials()
    check_account()
    check_dry_run_order()
    run_full_pipeline()

    print(f"\n{'='*60}")
    print("All validation steps passed.")
    print("="*60)


if __name__ == "__main__":
    main()
