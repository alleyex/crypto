"""Phase 5 — Feature Quality Check.

Checks:
  1. Distribution stats  (mean, std, skew, kurtosis, null %)
  2. Pairwise correlation — flags high-correlation pairs (|r| > CORR_THRESHOLD)
  3. IC (Information Coefficient) — Pearson corr of each feature vs
     forward log-returns (1-bar and 5-bar ahead)

Usage:
    python scripts/validate_features_quality.py [--symbol BTCUSDT] [--timeframe 1m]
"""

import sys
import argparse
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

from app.core.db import get_connection
from app.features.crypto_features import (
    build_crypto_features,
    get_feature_columns,
    MIN_VALID_ROWS,
)

CORR_THRESHOLD = 0.85   # flag pairs with |r| above this
IC_MEANINGFUL = 0.02    # IC above this is considered "has signal"
IC_STRONG = 0.05        # IC above this is considered "strong signal"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tstat(ic: float, n: int) -> float:
    """t-statistic for IC = r with n observations."""
    if n <= 2 or abs(ic) >= 1.0:
        return float("nan")
    denom = math.sqrt((1 - ic ** 2) / (n - 2))
    return ic / denom if denom != 0 else float("nan")


def _ic_label(ic: float) -> str:
    if math.isnan(ic):
        return "  n/a "
    if abs(ic) >= IC_STRONG:
        return "STRONG"
    if abs(ic) >= IC_MEANINGFUL:
        return "signal"
    return "  weak"


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

def check_distributions(df: pd.DataFrame, feat_cols: list) -> pd.DataFrame:
    """Return a DataFrame with distribution stats for each feature."""
    records = []
    for col in feat_cols:
        s = df[col].dropna()
        n_null = df[col].isna().sum()
        null_pct = n_null / len(df) * 100
        records.append({
            "feature":   col,
            "non_null":  len(s),
            "null_pct":  round(null_pct, 1),
            "mean":      round(s.mean(), 4) if len(s) else float("nan"),
            "std":       round(s.std(), 4) if len(s) else float("nan"),
            "min":       round(s.min(), 4) if len(s) else float("nan"),
            "max":       round(s.max(), 4) if len(s) else float("nan"),
            "skew":      round(s.skew(), 3) if len(s) > 3 else float("nan"),
            "kurtosis":  round(s.kurt(), 3) if len(s) > 3 else float("nan"),
        })
    return pd.DataFrame(records)


def check_correlations(df: pd.DataFrame, feat_cols: list) -> list:
    """Return list of high-correlation pairs (|r| > CORR_THRESHOLD)."""
    corr = df[feat_cols].corr()
    high_pairs = []
    for i, a in enumerate(feat_cols):
        for b in feat_cols[i + 1:]:
            r = corr.loc[a, b]
            if abs(r) >= CORR_THRESHOLD:
                high_pairs.append({"feat_a": a, "feat_b": b, "r": round(r, 4)})
    return sorted(high_pairs, key=lambda x: -abs(x["r"]))


def check_ic(df: pd.DataFrame, feat_cols: list) -> pd.DataFrame:
    """Compute IC vs 1-bar and 5-bar forward returns."""
    # Forward returns: shift the computed log-returns backward
    fwd_1 = df["log_ret_1"].shift(-1)
    fwd_5 = df["log_ret_5"].shift(-5) if "log_ret_5" in df.columns else None

    records = []
    for col in feat_cols:
        s = df[col]
        valid_1 = s.notna() & fwd_1.notna()
        ic_1 = s[valid_1].corr(fwd_1[valid_1])
        n_1 = valid_1.sum()
        t_1 = _tstat(ic_1, n_1)

        if fwd_5 is not None:
            valid_5 = s.notna() & fwd_5.notna()
            ic_5 = s[valid_5].corr(fwd_5[valid_5])
            n_5 = valid_5.sum()
        else:
            ic_5, n_5 = float("nan"), 0

        records.append({
            "feature": col,
            "IC_1":    round(ic_1, 5) if not math.isnan(ic_1) else float("nan"),
            "t_1":     round(t_1, 2) if not math.isnan(t_1) else float("nan"),
            "IC_5":    round(ic_5, 5) if not math.isnan(ic_5) else float("nan"),
            "signal":  _ic_label(ic_1),
        })

    return pd.DataFrame(records).sort_values("IC_1", key=abs, ascending=False)


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------

SEP = "─" * 68


def print_distributions(stats: pd.DataFrame) -> None:
    print(f"\n{'─'*68}")
    print(f"  Distribution Stats")
    print(f"{'─'*68}")
    print(f"  {'feature':<32} {'null%':>5}  {'mean':>8}  {'std':>7}  {'skew':>7}  {'kurt':>7}")
    print(f"  {'─'*32} {'─'*5}  {'─'*8}  {'─'*7}  {'─'*7}  {'─'*7}")
    for _, r in stats.iterrows():
        null_flag = " ⚠️" if r["null_pct"] > 5 else ""
        skew_flag = " ⚠️" if abs(r["skew"]) > 3 else ""
        print(
            f"  {r['feature']:<32} {r['null_pct']:>4.1f}%"
            f"  {r['mean']:>8.4f}  {r['std']:>7.4f}"
            f"  {r['skew']:>7.3f}{skew_flag}"
            f"  {r['kurtosis']:>7.1f}"
        )


def print_correlations(pairs: list) -> None:
    print(f"\n{SEP}")
    print(f"  High-Correlation Pairs  (|r| ≥ {CORR_THRESHOLD})")
    print(SEP)
    if not pairs:
        print(f"  ✅ No high-correlation pairs found")
        return
    for p in pairs:
        bar = "█" * int(abs(p["r"]) * 20)
        print(f"  {p['feat_a']:<28} ↔  {p['feat_b']:<28}  r={p['r']:+.4f}  {bar}")


def print_ic(ic_df: pd.DataFrame) -> None:
    print(f"\n{SEP}")
    print(f"  Information Coefficient  (IC vs forward returns)")
    print(f"  IC_1 = corr(feature, fwd_1bar)   IC_5 = corr(feature, fwd_5bar)")
    print(SEP)
    print(f"  {'feature':<32} {'IC_1':>8}  {'t':>6}  {'IC_5':>8}  {'verdict':>8}")
    print(f"  {'─'*32} {'─'*8}  {'─'*6}  {'─'*8}  {'─'*8}")
    for _, r in ic_df.iterrows():
        ic1_str = f"{r['IC_1']:+.5f}" if not math.isnan(r["IC_1"]) else "    n/a"
        t_str   = f"{r['t_1']:+.2f}"  if not math.isnan(r["t_1"]) else "   n/a"
        ic5_str = f"{r['IC_5']:+.5f}" if not math.isnan(r["IC_5"]) else "    n/a"
        print(f"  {r['feature']:<32} {ic1_str}  {t_str}  {ic5_str}  {r['signal']:>8}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="1m")
    args = parser.parse_args()

    symbol, tf = args.symbol, args.timeframe

    print(f"\n╔{'═'*66}╗")
    print(f"║  Phase 5 — Feature Quality Check{'':33}║")
    print(f"║  Symbol: {symbol}  Timeframe: {tf}{'':42}║")
    print(f"╚{'═'*66}╝")

    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT open_time, open, high, low, close, volume,
                   quote_asset_volume, number_of_trades,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM candles
            WHERE symbol = ? AND timeframe = ?
            ORDER BY open_time ASC
            """,
            (symbol, tf),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"  No data found for {symbol}/{tf}")
        return

    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume",
    ]
    df_raw = pd.DataFrame(rows, columns=cols)
    print(f"\n  Loaded {len(df_raw):,} candles from DB")

    df = build_crypto_features(df_raw)

    # Drop warm-up rows (before MIN_VALID_ROWS — features mostly NaN there)
    df = df.iloc[MIN_VALID_ROWS:].reset_index(drop=True)
    print(f"  Analysing {len(df):,} rows (after {MIN_VALID_ROWS}-row warm-up)")

    feat_cols = get_feature_columns()

    # 1. Distributions
    stats = check_distributions(df, feat_cols)
    print_distributions(stats)

    # 2. Correlations
    pairs = check_correlations(df, feat_cols)
    print_correlations(pairs)

    # 3. IC
    ic_df = check_ic(df, feat_cols)
    print_ic(ic_df)

    # Summary
    n_signal = (ic_df["signal"].str.strip().isin(["signal", "STRONG"])).sum()
    n_strong = (ic_df["signal"].str.strip() == "STRONG").sum()
    n_high_corr = len(pairs)

    print(f"\n{'═'*68}")
    print(f"  Features with signal : {n_signal} / {len(feat_cols)}"
          f"  ({n_strong} strong)")
    if n_high_corr:
        print(f"  ⚠️  High-correlation pairs : {n_high_corr}  (consider dropping one)")
    else:
        print(f"  ✅ No multicollinearity issues")
    print(f"{'═'*68}\n")


if __name__ == "__main__":
    main()
