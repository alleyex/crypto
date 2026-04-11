#!/usr/bin/env python3
"""Backfill Binance Futures mark/index 1m klines into futures_premium_metrics."""

from __future__ import annotations

import argparse
from datetime import date
from datetime import datetime
from datetime import timezone

import psycopg
import requests


DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "1000PEPEUSDT",
]
DEFAULT_DSN = "postgresql://crypto:crypto@127.0.0.1:5432/crypto"
MARK_URL = "https://fapi.binance.com/fapi/v1/markPriceKlines"
INDEX_URL = "https://fapi.binance.com/fapi/v1/indexPriceKlines"
INTERVAL = "1m"
LIMIT = 1500
TIMEOUT = 30
RETRIES = 6
BACKOFF_SECONDS = 2.0

UPSERT_SQL = """
INSERT INTO futures_premium_metrics
    (symbol, timestamp_ms, mark_price, index_price, estimated_settle_price,
     last_funding_rate, next_funding_time_ms, mark_index_basis_pct,
     mark_index_spread_bps, source)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    mark_price = excluded.mark_price,
    index_price = excluded.index_price,
    estimated_settle_price = excluded.estimated_settle_price,
    last_funding_rate = excluded.last_funding_rate,
    next_funding_time_ms = excluded.next_funding_time_ms,
    mark_index_basis_pct = excluded.mark_index_basis_pct,
    mark_index_spread_bps = excluded.mark_index_spread_bps,
    source = excluded.source
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--symbols", nargs="*", default=DEFAULT_SYMBOLS)
    parser.add_argument("--months", type=int, default=6)
    parser.add_argument("--timeout", type=int, default=TIMEOUT)
    return parser.parse_args()


def iter_target_months(month_count: int, today: date | None = None) -> list[tuple[int, int]]:
    today = today or datetime.now(timezone.utc).date()
    year = today.year
    month = today.month
    result: list[tuple[int, int]] = []
    for offset in range(month_count, 0, -1):
        y = year
        m = month - offset
        while m <= 0:
            y -= 1
            m += 12
        result.append((y, m))
    return result


def month_bounds_ms(year: int, month: int) -> tuple[int, int]:
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000) - 60_000


def fetch_klines(url: str, params: dict, timeout: int) -> list[list]:
    rows: list[list] = []
    cursor = int(params["startTime"])
    end_time = int(params["endTime"])
    while cursor <= end_time:
        local_params = dict(params)
        local_params["startTime"] = cursor
        last_error: Exception | None = None
        batch: list[list] = []
        for attempt in range(RETRIES):
            try:
                response = requests.get(url, params=local_params, timeout=timeout)
                response.raise_for_status()
                batch = response.json() or []
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt == RETRIES - 1:
                    raise
                import time
                sleep_seconds = BACKOFF_SECONDS * (attempt + 1)
                print(
                    f"  request retry {attempt + 1} for {url} start={cursor}, sleeping {sleep_seconds:.1f}s",
                    flush=True,
                )
                time.sleep(sleep_seconds)
        if last_error is not None:
            raise last_error
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < LIMIT:
            break
        cursor = int(batch[-1][0]) + 60_000
    return rows


def fetch_mark_rows(symbol: str, start_ms: int, end_ms: int, timeout: int) -> dict[int, float]:
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": LIMIT,
    }
    rows = fetch_klines(MARK_URL, params, timeout)
    return {int(row[0]): float(row[4]) for row in rows}


def fetch_index_rows(pair: str, start_ms: int, end_ms: int, timeout: int) -> dict[int, float]:
    params = {
        "pair": pair,
        "interval": INTERVAL,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": LIMIT,
    }
    rows = fetch_klines(INDEX_URL, params, timeout)
    return {int(row[0]): float(row[4]) for row in rows}


def build_rows(symbol: str, mark_rows: dict[int, float], index_rows: dict[int, float]) -> list[tuple]:
    rows: list[tuple] = []
    for timestamp_ms in sorted(set(mark_rows) & set(index_rows)):
        mark_price = mark_rows[timestamp_ms]
        index_price = index_rows[timestamp_ms]
        basis_pct = None
        spread_bps = None
        if index_price and index_price > 0:
            basis_pct = (mark_price / index_price) - 1.0
            spread_bps = basis_pct * 10_000.0
        rows.append(
            (
                symbol,
                timestamp_ms,
                round(mark_price, 8),
                round(index_price, 8),
                None,
                None,
                None,
                round(basis_pct, 10) if basis_pct is not None else None,
                round(spread_bps, 6) if spread_bps is not None else None,
                "archive",
            )
        )
    return rows


def main() -> int:
    args = parse_args()
    months = iter_target_months(args.months)
    print("Target months:", ", ".join(f"{y:04d}-{m:02d}" for y, m in months), flush=True)
    with psycopg.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            for symbol in args.symbols:
                pair = symbol.upper().replace("1000PEPEUSDT", "PEPEUSDT")
                for year, month in months:
                    start_ms, end_ms = month_bounds_ms(year, month)
                    print(f"[{symbol}] processing {year:04d}-{month:02d}", flush=True)
                    mark_rows = fetch_mark_rows(symbol.upper(), start_ms, end_ms, args.timeout)
                    index_rows = fetch_index_rows(pair, start_ms, end_ms, args.timeout)
                    rows = build_rows(symbol.upper(), mark_rows, index_rows)
                    for idx in range(0, len(rows), 10_000):
                        cur.executemany(UPSERT_SQL, rows[idx : idx + 10_000])
                        conn.commit()
                    print(
                        f"  mark={len(mark_rows)} index={len(index_rows)} inserted={len(rows)}",
                        flush=True,
                    )
    print("Completed premium history backfill", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
