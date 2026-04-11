#!/usr/bin/env python3
"""Probe Hyblock data availability for the configured futures symbols.

This is intentionally small and explicit: once a Hyblock API key is available,
use it to query the availability endpoint for the six tracked futures symbols
and print the earliest available timestamp per symbol.
"""

from __future__ import annotations

import argparse
import os
from typing import Iterable

import requests


DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "1000PEPEUSDT",
]

# Hyblock docs examples use lowercase symbols and the binance_perp_stable exchange.
DEFAULT_EXCHANGE = "binance_perp_stable"
DEFAULT_URL = "https://api.hyblockcapital.com/v1/data-availability"
DEFAULT_HEADER = "X-API-KEY"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.getenv("HYBLOCK_AVAILABILITY_URL", DEFAULT_URL))
    parser.add_argument("--header-name", default=os.getenv("HYBLOCK_API_HEADER", DEFAULT_HEADER))
    parser.add_argument("--api-key", default=os.getenv("HYBLOCK_API_KEY", ""))
    parser.add_argument("--exchange", default=os.getenv("HYBLOCK_EXCHANGE", DEFAULT_EXCHANGE))
    parser.add_argument("--symbols", nargs="*", default=DEFAULT_SYMBOLS)
    parser.add_argument("--timeout", type=int, default=30)
    return parser.parse_args()


def normalize_symbol(symbol: str) -> str:
    symbol = symbol.strip().upper()
    if symbol == "1000PEPEUSDT":
        return "1000pepeusdt"
    return symbol.lower()


def chunked(items: list[str], size: int) -> Iterable[list[str]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def request_availability(
    *,
    url: str,
    header_name: str,
    api_key: str,
    exchange: str,
    symbols: list[str],
    timeout: int,
) -> list[dict]:
    if not api_key:
        raise RuntimeError("HYBLOCK_API_KEY is required.")

    headers = {header_name: api_key}
    payload = {
        "exchange": exchange,
        "symbols": [normalize_symbol(symbol) for symbol in symbols],
    }
    response = requests.post(url, json=payload, headers=headers, timeout=timeout)
    response.raise_for_status()
    body = response.json()
    if isinstance(body, dict):
        return list(body.get("data") or [])
    raise RuntimeError(f"Unexpected response type: {type(body)!r}")


def main() -> int:
    args = parse_args()
    rows: list[dict] = []
    for symbol_group in chunked(list(args.symbols), 20):
        rows.extend(
            request_availability(
                url=args.url,
                header_name=args.header_name,
                api_key=args.api_key,
                exchange=args.exchange,
                symbols=symbol_group,
                timeout=args.timeout,
            )
        )

    rows_by_symbol = {str(row.get("symbol") or "").upper(): row for row in rows}
    print(f"exchange={args.exchange}")
    for symbol in args.symbols:
        row = rows_by_symbol.get(symbol.upper())
        if not row:
            print(f"{symbol}\tmissing")
            continue
        print(
            f"{symbol}\t{row.get('availabilityFrom')}\t{row.get('exchange')}\t{row.get('symbol')}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
