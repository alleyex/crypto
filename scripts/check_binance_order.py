#!/usr/bin/env python3

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"


def _ensure_project_venv_python() -> None:
    if not EXPECTED_PYTHON.exists():
        return

    current_python = Path(sys.executable).resolve()
    expected_python = EXPECTED_PYTHON.resolve()
    if current_python == expected_python:
        return

    os.execv(str(expected_python), [str(expected_python), __file__, *sys.argv[1:]])


_ensure_project_venv_python()

sys.path.insert(0, str(PROJECT_ROOT))

from app.execution.binance_broker import BinanceBrokerClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Validate a signed Binance Spot test order without placing a real order.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--side", choices=("BUY", "SELL"), default="BUY")
    parser.add_argument("--qty", type=float, default=0.001)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = BinanceBrokerClient()
    result = client.check_order_request(symbol=args.symbol, side=args.side, qty=args.qty)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
