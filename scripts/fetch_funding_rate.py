"""Fetch BTCUSDT perpetual futures funding rate history from Binance FAPI.

Saves to data/funding_rate_{SYMBOL}.csv with columns:
  funding_time_ms, funding_rate, mark_price

Binance funding rate settles every 8 hours: 00:00 / 08:00 / 16:00 UTC.
Each request fetches up to 1000 records; script paginates to get full history.

Usage:
    python scripts/fetch_funding_rate.py [--symbol BTCUSDT] [--output data/]
"""

import sys
import time
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import requests
import pandas as pd

FAPI_BASE  = "https://fapi.binance.com"
ENDPOINT   = "/fapi/v1/fundingRate"
MAX_LIMIT  = 1000
SLEEP_SEC  = 0.3   # polite rate limiting


def fetch_page(symbol: str, start_ms: int | None, limit: int = MAX_LIMIT) -> list:
    params: dict = {"symbol": symbol, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms
    resp = requests.get(FAPI_BASE + ENDPOINT, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json()


def fetch_all(symbol: str) -> pd.DataFrame:
    """Paginate through all funding rate history for symbol."""
    all_records = []
    start_ms = None
    page = 0

    while True:
        page += 1
        records = fetch_page(symbol, start_ms=start_ms)
        if not records:
            break

        all_records.extend(records)
        print(f"  page {page:3d}: {len(records)} records  "
              f"(total so far: {len(all_records):,})", end="\r")

        if len(records) < MAX_LIMIT:
            break  # last page

        # Next page starts just after the last record's time
        last_time = int(records[-1]["fundingTime"])
        start_ms = last_time + 1
        time.sleep(SLEEP_SEC)

    print()  # newline after \r
    if not all_records:
        return pd.DataFrame(columns=["funding_time_ms", "funding_rate", "mark_price"])

    df = pd.DataFrame(all_records)
    df = df.rename(columns={
        "fundingTime": "funding_time_ms",
        "fundingRate": "funding_rate",
        "markPrice":   "mark_price",
    })
    df["funding_time_ms"] = df["funding_time_ms"].astype(int)
    df["funding_rate"]    = df["funding_rate"].astype(float)
    df["mark_price"]      = pd.to_numeric(df["mark_price"], errors="coerce")
    df = df.sort_values("funding_time_ms").drop_duplicates("funding_time_ms")
    df = df.reset_index(drop=True)
    return df[["funding_time_ms", "funding_rate", "mark_price"]]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--output", default="data/")
    args = parser.parse_args()

    out_dir = ROOT / args.output
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"funding_rate_{args.symbol}.csv"

    print(f"\n  Fetching funding rate history: {args.symbol}")
    print(f"  Output: {out_path}\n")

    df = fetch_all(args.symbol)

    if df.empty:
        print("  No data returned.")
        return

    # Summary
    first_dt = pd.to_datetime(df["funding_time_ms"].iloc[0],  unit="ms", utc=True)
    last_dt  = pd.to_datetime(df["funding_time_ms"].iloc[-1], unit="ms", utc=True)
    n_days   = (last_dt - first_dt).days
    fr_mean  = df["funding_rate"].mean() * 100
    fr_std   = df["funding_rate"].std()  * 100
    fr_min   = df["funding_rate"].min()  * 100
    fr_max   = df["funding_rate"].max()  * 100

    df.to_csv(out_path, index=False)

    print(f"\n  Saved {len(df):,} records ({n_days} days)")
    print(f"  Range  : {first_dt.date()} → {last_dt.date()}")
    print(f"  Rate   : mean={fr_mean:+.4f}%  std={fr_std:.4f}%"
          f"  min={fr_min:+.4f}%  max={fr_max:+.4f}%")
    print(f"  File   : {out_path}\n")


if __name__ == "__main__":
    main()
