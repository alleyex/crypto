#!/usr/bin/env python3
"""Backfill missing futures aggTrade minute aggregates via Binance REST."""

from __future__ import annotations

import argparse
from datetime import datetime
from datetime import timezone

import psycopg
import requests


DEFAULT_DSN = "postgresql://crypto:crypto@127.0.0.1:5432/crypto"
DEFAULT_SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "1000PEPEUSDT",
]
REST_URL = "https://fapi.binance.com/fapi/v1/aggTrades"
LIMIT = 1000
TIMEOUT = 30
RETRIES = 6
BACKOFF_SECONDS = 2.0

UPSERT_SQL = """
INSERT INTO futures_aggtrade_minutes
    (symbol, timestamp_ms, trade_count, taker_buy_count, taker_sell_count,
     qty_total, qty_taker_buy, qty_taker_sell,
     quote_total, quote_taker_buy, quote_taker_sell,
     price_open, price_high, price_low, price_close,
     vwap, avg_trade_size, first_trade_id, last_trade_id,
     first_event_ms, last_event_ms, active_seconds, coverage_ratio, source)
VALUES
    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(symbol, timestamp_ms) DO UPDATE SET
    trade_count = excluded.trade_count,
    taker_buy_count = excluded.taker_buy_count,
    taker_sell_count = excluded.taker_sell_count,
    qty_total = excluded.qty_total,
    qty_taker_buy = excluded.qty_taker_buy,
    qty_taker_sell = excluded.qty_taker_sell,
    quote_total = excluded.quote_total,
    quote_taker_buy = excluded.quote_taker_buy,
    quote_taker_sell = excluded.quote_taker_sell,
    price_open = excluded.price_open,
    price_high = excluded.price_high,
    price_low = excluded.price_low,
    price_close = excluded.price_close,
    vwap = excluded.vwap,
    avg_trade_size = excluded.avg_trade_size,
    first_trade_id = excluded.first_trade_id,
    last_trade_id = excluded.last_trade_id,
    first_event_ms = excluded.first_event_ms,
    last_event_ms = excluded.last_event_ms,
    active_seconds = excluded.active_seconds,
    coverage_ratio = excluded.coverage_ratio,
    source = excluded.source
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=DEFAULT_DSN)
    parser.add_argument("--symbols", nargs="*", default=DEFAULT_SYMBOLS)
    parser.add_argument("--max-gap-minutes", type=int, default=500)
    return parser.parse_args()


def fetch_rows(symbol: str, minute_ms: int) -> list[dict]:
    cursor = minute_ms
    rows: list[dict] = []
    while cursor <= minute_ms + 59_999:
        params = {
            "symbol": symbol,
            "startTime": cursor,
            "endTime": minute_ms + 59_999,
            "limit": LIMIT,
        }
        last_error: Exception | None = None
        batch: list[dict] = []
        for attempt in range(RETRIES):
            try:
                resp = requests.get(REST_URL, params=params, timeout=TIMEOUT)
                if resp.status_code == 429:
                    raise requests.HTTPError("429 Too Many Requests", response=resp)
                resp.raise_for_status()
                batch = resp.json() or []
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt == RETRIES - 1:
                    raise
                sleep_seconds = BACKOFF_SECONDS * (attempt + 1)
                print(
                    f"  rate-limited/error for {symbol} minute={minute_ms} attempt={attempt + 1}, sleeping {sleep_seconds:.1f}s",
                    flush=True,
                )
                import time
                time.sleep(sleep_seconds)
        if last_error is not None:
            raise last_error
        if not batch:
            break
        rows.extend(batch)
        if len(batch) < LIMIT:
            break
        cursor = int(batch[-1]["T"]) + 1
    return rows


def build_snapshot(symbol: str, minute_ms: int, rows: list[dict]) -> tuple | None:
    minute_rows = [row for row in rows if (int(row["T"]) // 60_000) * 60_000 == minute_ms]
    if not minute_rows:
        return None
    first = minute_rows[0]
    price_open = float(first["p"])
    price_high = price_open
    price_low = price_open
    price_close = price_open
    trade_count = 0
    taker_buy_count = 0
    taker_sell_count = 0
    qty_total = 0.0
    qty_taker_buy = 0.0
    qty_taker_sell = 0.0
    quote_total = 0.0
    quote_taker_buy = 0.0
    quote_taker_sell = 0.0
    price_qty_sum = 0.0
    first_trade_id = int(first["a"])
    last_trade_id = int(first["a"])
    first_event_ms = int(first["T"])
    last_event_ms = int(first["T"])
    active_seconds: set[int] = set()

    for row in minute_rows:
        trade_id = int(row["a"])
        event_ms = int(row["T"])
        price = float(row["p"])
        qty = float(row["q"])
        buyer_is_maker = bool(row["m"])
        quote_qty = price * qty
        trade_count += 1
        qty_total += qty
        quote_total += quote_qty
        price_qty_sum += price * qty
        price_high = max(price_high, price)
        price_low = min(price_low, price)
        price_close = price
        first_trade_id = min(first_trade_id, trade_id)
        last_trade_id = max(last_trade_id, trade_id)
        first_event_ms = min(first_event_ms, event_ms)
        last_event_ms = max(last_event_ms, event_ms)
        active_seconds.add(event_ms // 1000)
        if buyer_is_maker:
            taker_sell_count += 1
            qty_taker_sell += qty
            quote_taker_sell += quote_qty
        else:
            taker_buy_count += 1
            qty_taker_buy += qty
            quote_taker_buy += quote_qty

    vwap = quote_total / qty_total if qty_total > 0 else None
    avg_trade_size = qty_total / trade_count if trade_count > 0 else None
    return (
        symbol,
        minute_ms,
        trade_count,
        taker_buy_count,
        taker_sell_count,
        round(qty_total, 8),
        round(qty_taker_buy, 8),
        round(qty_taker_sell, 8),
        round(quote_total, 8),
        round(quote_taker_buy, 8),
        round(quote_taker_sell, 8),
        round(price_open, 8),
        round(price_high, 8),
        round(price_low, 8),
        round(price_close, 8),
        round(vwap, 8) if vwap is not None else None,
        round(avg_trade_size, 8) if avg_trade_size is not None else None,
        first_trade_id,
        last_trade_id,
        first_event_ms,
        last_event_ms,
        len(active_seconds),
        round(len(active_seconds) / 60.0, 6),
        "rest_backfill",
    )


def list_missing_minutes(conn: psycopg.Connection, symbols: list[str], max_gap_minutes: int) -> dict[str, list[int]]:
    result: dict[str, list[int]] = {}
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH ordered AS (
              SELECT symbol,
                     timestamp_ms,
                     LAG(timestamp_ms) OVER (PARTITION BY symbol ORDER BY timestamp_ms) AS prev_ts
              FROM futures_aggtrade_minutes
              WHERE symbol = ANY(%s)
            ),
            gaps AS (
              SELECT symbol, prev_ts, timestamp_ms,
                     ((timestamp_ms - prev_ts)/60000 - 1) AS missing_minutes
              FROM ordered
              WHERE prev_ts IS NOT NULL AND timestamp_ms - prev_ts > 60000
            )
            SELECT symbol, prev_ts, timestamp_ms, missing_minutes
            FROM gaps
            WHERE missing_minutes > 0 AND missing_minutes <= %s
            ORDER BY symbol, prev_ts
            """,
            (symbols, max_gap_minutes),
        )
        for symbol, prev_ts, timestamp_ms, missing_minutes in cur.fetchall():
            missing = [
                int(prev_ts + step * 60_000)
                for step in range(1, int(missing_minutes) + 1)
            ]
            result.setdefault(symbol, []).extend(missing)
    return result


def main() -> int:
    args = parse_args()
    total_requested = 0
    total_inserted = 0
    total_empty = 0
    with psycopg.connect(args.dsn) as conn:
        missing = list_missing_minutes(conn, [s.upper() for s in args.symbols], args.max_gap_minutes)
        with conn.cursor() as cur:
            for symbol in args.symbols:
                symbol = symbol.upper()
                minute_list = missing.get(symbol, [])
                if not minute_list:
                    print(f"[{symbol}] no gaps <= {args.max_gap_minutes} minutes")
                    continue
                print(f"[{symbol}] backfilling {len(minute_list)} missing minutes", flush=True)
                symbol_inserted = 0
                symbol_empty = 0
                for idx, minute_ms in enumerate(minute_list, start=1):
                    total_requested += 1
                    rows = fetch_rows(symbol, minute_ms)
                    snapshot = build_snapshot(symbol, minute_ms, rows)
                    if snapshot is None:
                        symbol_empty += 1
                        total_empty += 1
                        continue
                    cur.execute(UPSERT_SQL, snapshot)
                    symbol_inserted += 1
                    total_inserted += 1
                    if idx % 50 == 0:
                        conn.commit()
                        ts = datetime.fromtimestamp(minute_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
                        print(f"  {idx}/{len(minute_list)} upserted, latest={ts} UTC", flush=True)
                conn.commit()
                print(
                    f"[{symbol}] completed: inserted={symbol_inserted} empty={symbol_empty}",
                    flush=True,
                )
    print(
        f"Done: requested={total_requested} inserted={total_inserted} empty={total_empty}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
