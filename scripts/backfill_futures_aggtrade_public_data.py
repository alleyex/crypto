#!/usr/bin/env python3
"""Backfill Binance Futures aggTrades archive into 1-minute aggregates.

Downloads recent Binance public-data aggTrades archives for the configured
USD-M perpetual symbols, aggregates event-level rows into 1-minute buckets,
and upserts them into futures_aggtrade_minutes in PostgreSQL.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import tempfile
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Iterable
from typing import Iterator
from typing import TypeVar

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
ARCHIVE_BASE = "https://data.binance.vision/data/futures/um"
REQUEST_TIMEOUT = 120
CHUNK_SIZE = 10_000

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


@dataclass
class MinuteBucket:
    symbol: str
    timestamp_ms: int
    trade_count: int
    taker_buy_count: int
    taker_sell_count: int
    qty_total: float
    qty_taker_buy: float
    qty_taker_sell: float
    quote_total: float
    quote_taker_buy: float
    quote_taker_sell: float
    price_open: float
    price_high: float
    price_low: float
    price_close: float
    price_qty_sum: float
    first_trade_id: int
    last_trade_id: int
    first_event_ms: int
    last_event_ms: int
    active_seconds: set[int]

    @classmethod
    def from_trade(
        cls,
        *,
        symbol: str,
        minute_ms: int,
        trade_id: int,
        event_ms: int,
        price: float,
        qty: float,
        buyer_is_maker: bool,
    ) -> "MinuteBucket":
        quote_qty = price * qty
        return cls(
            symbol=symbol,
            timestamp_ms=minute_ms,
            trade_count=1,
            taker_buy_count=0 if buyer_is_maker else 1,
            taker_sell_count=1 if buyer_is_maker else 0,
            qty_total=qty,
            qty_taker_buy=0.0 if buyer_is_maker else qty,
            qty_taker_sell=qty if buyer_is_maker else 0.0,
            quote_total=quote_qty,
            quote_taker_buy=0.0 if buyer_is_maker else quote_qty,
            quote_taker_sell=quote_qty if buyer_is_maker else 0.0,
            price_open=price,
            price_high=price,
            price_low=price,
            price_close=price,
            price_qty_sum=price * qty,
            first_trade_id=trade_id,
            last_trade_id=trade_id,
            first_event_ms=event_ms,
            last_event_ms=event_ms,
            active_seconds={event_ms // 1000},
        )

    def update(self, *, trade_id: int, event_ms: int, price: float, qty: float, buyer_is_maker: bool) -> None:
        quote_qty = price * qty
        self.trade_count += 1
        self.qty_total += qty
        self.quote_total += quote_qty
        self.price_qty_sum += price * qty
        self.price_high = max(self.price_high, price)
        self.price_low = min(self.price_low, price)
        self.price_close = price
        self.first_trade_id = min(self.first_trade_id, trade_id)
        self.last_trade_id = max(self.last_trade_id, trade_id)
        self.first_event_ms = min(self.first_event_ms, event_ms)
        self.last_event_ms = max(self.last_event_ms, event_ms)
        if buyer_is_maker:
            self.taker_sell_count += 1
            self.qty_taker_sell += qty
            self.quote_taker_sell += quote_qty
        else:
            self.taker_buy_count += 1
            self.qty_taker_buy += qty
            self.quote_taker_buy += quote_qty
        self.active_seconds.add(event_ms // 1000)

    def db_row(self) -> tuple[object, ...]:
        active_seconds = len(self.active_seconds)
        vwap = self.quote_total / self.qty_total if self.qty_total > 0 else None
        avg_trade_size = self.qty_total / self.trade_count if self.trade_count > 0 else None
        return (
            self.symbol,
            self.timestamp_ms,
            self.trade_count,
            self.taker_buy_count,
            self.taker_sell_count,
            round(self.qty_total, 8),
            round(self.qty_taker_buy, 8),
            round(self.qty_taker_sell, 8),
            round(self.quote_total, 8),
            round(self.quote_taker_buy, 8),
            round(self.quote_taker_sell, 8),
            round(self.price_open, 8),
            round(self.price_high, 8),
            round(self.price_low, 8),
            round(self.price_close, 8),
            round(vwap, 8) if vwap is not None else None,
            round(avg_trade_size, 8) if avg_trade_size is not None else None,
            self.first_trade_id,
            self.last_trade_id,
            self.first_event_ms,
            self.last_event_ms,
            active_seconds,
            round(active_seconds / 60.0, 6),
            "archive",
        )


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


def month_days(year: int, month: int) -> Iterator[date]:
    current = date(year, month, 1)
    while current.month == month:
        yield current
        current += timedelta(days=1)


def archive_urls(symbol: str, year: int, month: int, prefer_daily: bool) -> list[str]:
    monthly = f"{ARCHIVE_BASE}/monthly/aggTrades/{symbol}/{symbol}-aggTrades-{year:04d}-{month:02d}.zip"
    daily = [
        f"{ARCHIVE_BASE}/daily/aggTrades/{symbol}/{symbol}-aggTrades-{day.strftime('%Y-%m-%d')}.zip"
        for day in month_days(year, month)
    ]
    return [*daily, monthly] if prefer_daily else [monthly, *daily]


def download_archive(url: str) -> Path | None:
    response = requests.get(url, stream=True, timeout=REQUEST_TIMEOUT)
    if response.status_code == 404:
        response.close()
        return None
    response.raise_for_status()
    suffix = "-" + Path(url).name
    with tempfile.NamedTemporaryFile(prefix="aggtrade-", suffix=suffix, delete=False) as tmp:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                tmp.write(chunk)
        return Path(tmp.name)


def process_zip(path: Path, symbol: str, buckets: dict[int, MinuteBucket]) -> int:
    processed = 0
    with zipfile.ZipFile(path) as zf:
        members = [name for name in zf.namelist() if name.endswith(".csv")]
        if not members:
            return 0
        with zf.open(members[0], "r") as raw:
            wrapper = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            reader = csv.reader(wrapper)
            for row in reader:
                if not row:
                    continue
                if row[0] == "agg_trade_id":
                    continue
                trade_id = int(row[0])
                price = float(row[1])
                qty = float(row[2])
                event_ms = int(row[5])
                buyer_is_maker = row[6].strip().lower() == "true"
                minute_ms = (event_ms // 60_000) * 60_000
                bucket = buckets.get(minute_ms)
                if bucket is None:
                    buckets[minute_ms] = MinuteBucket.from_trade(
                        symbol=symbol,
                        minute_ms=minute_ms,
                        trade_id=trade_id,
                        event_ms=event_ms,
                        price=price,
                        qty=qty,
                        buyer_is_maker=buyer_is_maker,
                    )
                else:
                    bucket.update(
                        trade_id=trade_id,
                        event_ms=event_ms,
                        price=price,
                        qty=qty,
                        buyer_is_maker=buyer_is_maker,
                    )
                processed += 1
    return processed


T = TypeVar("T")


def iter_chunks(items: list[T], size: int) -> Iterator[list[T]]:
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def upsert_rows(conn: psycopg.Connection, rows: list[tuple[object, ...]]) -> None:
    with conn.cursor() as cur:
        for chunk in iter_chunks(rows, CHUNK_SIZE):
            cur.executemany(UPSERT_SQL, chunk)
    conn.commit()


def backfill_symbol_months(
    conn: psycopg.Connection,
    symbol: str,
    months: Iterable[tuple[int, int]],
    *,
    prefer_daily: bool,
) -> dict[str, int]:
    total_files = 0
    total_events = 0
    total_minutes = 0
    for year, month in months:
        buckets: dict[int, MinuteBucket] = {}
        downloaded_any = False
        print(f"[{symbol}] processing {year:04d}-{month:02d}", flush=True)
        urls = archive_urls(symbol, year, month, prefer_daily=prefer_daily)
        monthly_missing = False
        for idx, url in enumerate(urls):
            archive_path: Path | None = None
            try:
                archive_path = download_archive(url)
                if archive_path is None:
                    if idx == 0 and not prefer_daily:
                        monthly_missing = True
                        print(f"  monthly archive missing, falling back to daily archives", flush=True)
                        continue
                    continue
                downloaded_any = True
                total_files += 1
                processed = process_zip(archive_path, symbol, buckets)
                total_events += processed
                print(f"  {Path(url).name}: {processed} aggTrades", flush=True)
                if idx == 0 and not prefer_daily:
                    break
            finally:
                if archive_path is not None and archive_path.exists():
                    archive_path.unlink()
        if not downloaded_any:
            print(f"  no archives found for {symbol} {year:04d}-{month:02d}", flush=True)
            continue
        rows = [bucket.db_row() for _, bucket in sorted(buckets.items())]
        upsert_rows(conn, rows)
        total_minutes += len(rows)
        if monthly_missing:
            print(f"  daily fallback complete: {len(rows)} minute rows upserted", flush=True)
        else:
            print(f"  monthly archive complete: {len(rows)} minute rows upserted", flush=True)
    return {"files": total_files, "events": total_events, "minutes": total_minutes}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", default=os.getenv("CRYPTO_DATABASE_URL", DEFAULT_DSN))
    parser.add_argument("--symbols", nargs="*", default=DEFAULT_SYMBOLS)
    parser.add_argument("--months", type=int, default=6, help="Number of full calendar months to backfill.")
    parser.add_argument("--prefer-daily", action="store_true", help="Prefer daily archives over monthly archives.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    months = iter_target_months(args.months)
    print(
        "Target months:",
        ", ".join(f"{year:04d}-{month:02d}" for year, month in months),
        flush=True,
    )
    grand_files = 0
    grand_events = 0
    grand_minutes = 0
    with psycopg.connect(args.dsn) as conn:
        for symbol in args.symbols:
            stats = backfill_symbol_months(conn, symbol.upper(), months, prefer_daily=args.prefer_daily)
            grand_files += stats["files"]
            grand_events += stats["events"]
            grand_minutes += stats["minutes"]
    print(
        f"Completed backfill: files={grand_files} events={grand_events} minute_rows={grand_minutes}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
