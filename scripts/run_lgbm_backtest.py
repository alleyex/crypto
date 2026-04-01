"""LGBM Threshold Backtest — long/flat only, with fees.

Validates whether LGBM features have *real* trading edge after accounting for
transaction costs, independent of PPO training stability.

Strategy:
  - Long  when P(up next bar) >= threshold
  - Flat  when P(up next bar) <  threshold
  - Fee   = fee_rate per side on every position change

Output per threshold:
  - Cumulative net edge, win rate per trade, avg holding bars, n_trades, Sharpe

Usage:
    python scripts/run_lgbm_backtest.py [--symbol BTCUSDT] [--timeframe 15m]
        [--train-bars 10000] [--test-bars 2000] [--n-folds 10]
        [--fee 0.001] [--thresholds 0.50 0.51 0.52 0.53 0.54 0.55]
"""

import sys
import argparse
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import lightgbm as lgb

from app.core.db import get_connection
from app.features.crypto_features import (
    build_crypto_features,
    get_feature_columns,
    MIN_VALID_ROWS,
)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DEFAULT_TRAIN_BARS  = 10_000
DEFAULT_TEST_BARS   = 2_000
DEFAULT_N_FOLDS     = 10
DEFAULT_FEE         = 0.001   # 0.1% per side
DEFAULT_THRESHOLDS  = [0.50, 0.51, 0.52, 0.53, 0.54, 0.55]

LGB_PARAMS = {
    "objective":         "binary",
    "metric":            "binary_logloss",
    "n_estimators":      200,
    "learning_rate":     0.05,
    "num_leaves":        31,
    "min_child_samples": 50,
    "feature_fraction":  0.8,
    "bagging_fraction":  0.8,
    "bagging_freq":      5,
    "verbose":           -1,
    "n_jobs":            -1,
}

SEP = "─" * 72


# ---------------------------------------------------------------------------
# Core backtest logic
# ---------------------------------------------------------------------------

def _simulate(proba: np.ndarray, log_ret: np.ndarray,
              threshold: float, fee_rate: float) -> dict:
    """Simulate long/flat strategy for one fold.

    Returns per-trade breakdown and aggregate stats.
    """
    n = len(proba)
    pos = (proba >= threshold).astype(float)   # 1=long, 0=flat

    # Bar-level PnL
    bar_pnl = pos * log_ret                    # gain while long
    changes  = np.abs(np.diff(pos, prepend=0.0))
    fees     = changes * fee_rate              # 1 side per change event

    net_bar = bar_pnl - fees
    cum_edge = float(net_bar.sum())

    # Trade-level analysis
    trades = []
    in_trade = False
    entry_idx = 0
    trade_pnl = 0.0
    for i in range(n):
        if not in_trade and pos[i] == 1:
            in_trade = True
            entry_idx = i
            trade_pnl = -fee_rate              # entry fee
        if in_trade:
            trade_pnl += log_ret[i]
            if pos[i] == 0 or i == n - 1:
                trade_pnl -= fee_rate          # exit fee
                trades.append({
                    "holding": i - entry_idx + 1,
                    "pnl":     trade_pnl,
                })
                in_trade = False
                trade_pnl = 0.0

    n_trades    = len(trades)
    win_rate    = float(np.mean([t["pnl"] > 0 for t in trades])) if trades else float("nan")
    avg_hold    = float(np.mean([t["holding"] for t in trades])) if trades else float("nan")
    avg_pnl     = float(np.mean([t["pnl"] for t in trades])) if trades else float("nan")
    long_pct    = float(pos.mean())

    # Sharpe of net bar returns
    std = net_bar.std()
    sharpe = float((net_bar.mean() / std) * math.sqrt(len(net_bar) * 365 * 24 * 4)) \
        if std > 0 else float("nan")   # annualised for 15m; adjust externally if needed

    return {
        "cum_edge":  cum_edge,
        "n_trades":  n_trades,
        "win_rate":  win_rate,
        "avg_hold":  avg_hold,
        "avg_pnl":   avg_pnl,
        "long_pct":  long_pct,
        "sharpe":    sharpe,
    }


# ---------------------------------------------------------------------------
# Walk-forward
# ---------------------------------------------------------------------------

def run_backtest(df: pd.DataFrame, feat_cols: list,
                 train_bars: int, test_bars: int, n_folds: int,
                 fee_rate: float, thresholds: list) -> dict:
    df = df.copy()
    df["fwd_log_ret"] = df["log_ret_1"].shift(-1)
    df = df.dropna(subset=feat_cols + ["fwd_log_ret"]).reset_index(drop=True)
    df["target"] = (df["fwd_log_ret"] > 0).astype(int)

    total_needed = train_bars + test_bars * n_folds
    if len(df) < total_needed:
        raise ValueError(
            f"Need {total_needed:,} rows but only {len(df):,} available."
        )

    # Results: threshold -> list of fold dicts
    results: dict[float, list] = {t: [] for t in thresholds}

    for fold in range(n_folds):
        train_start = fold * test_bars
        train_end   = train_start + train_bars
        test_end    = train_end + test_bars

        X_train = df[feat_cols].iloc[train_start:train_end]
        y_train = df["target"].iloc[train_start:train_end].values
        X_test  = df[feat_cols].iloc[train_end:test_end]
        log_ret = df["fwd_log_ret"].iloc[train_end:test_end].values

        model = lgb.LGBMClassifier(**LGB_PARAMS)
        model.fit(X_train, y_train)
        proba = model.predict_proba(X_test)[:, 1]

        for thresh in thresholds:
            sim = _simulate(proba, log_ret, thresh, fee_rate)
            sim["fold"] = fold + 1
            results[thresh].append(sim)

        # Progress
        accs = [(proba >= 0.5).astype(int) == df["target"].iloc[train_end:test_end].values]
        print(f"  fold {fold+1:2d}/{n_folds}  proba_range=[{proba.min():.3f},{proba.max():.3f}]"
              f"  pos_rate@0.52={float((proba>=0.52).mean()):4.1%}")

    return results


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------

def _agg(folds: list, key: str) -> tuple:
    vals = [f[key] for f in folds if not math.isnan(f[key])]
    if not vals:
        return float("nan"), float("nan")
    return float(np.mean(vals)), float(np.std(vals))


def print_results(results: dict, fee_rate: float, n_folds: int,
                  test_bars: int) -> None:
    print(f"\n{SEP}")
    print(f"  Threshold Sweep  (fee={fee_rate*100:.2f}%/side, "
          f"{n_folds} folds × {test_bars:,} bars)")
    print(SEP)
    print(f"  {'Thresh':>7}  {'CumEdge':>9}  {'Trades':>6}  "
          f"{'WinRate':>7}  {'AvgHold':>7}  {'AvgPnL':>8}  {'Sharpe':>7}  {'Long%':>6}")
    print(f"  {'─'*7}  {'─'*9}  {'─'*6}  {'─'*7}  {'─'*7}  {'─'*8}  {'─'*7}  {'─'*6}")

    best_thresh = None
    best_edge   = float("-inf")

    for thresh, folds in results.items():
        total_edge = sum(f["cum_edge"] for f in folds)
        total_trades = sum(f["n_trades"] for f in folds)
        avg_win, _  = _agg(folds, "win_rate")
        avg_hold, _ = _agg(folds, "avg_hold")
        avg_pnl, _  = _agg(folds, "avg_pnl")
        avg_sharpe, _ = _agg(folds, "sharpe")
        avg_long, _ = _agg(folds, "long_pct")

        edge_str   = f"{total_edge:+.5f}"
        sharpe_str = f"{avg_sharpe:+.2f}" if not math.isnan(avg_sharpe) else "   n/a"
        win_str    = f"{avg_win:.1%}" if not math.isnan(avg_win) else "   n/a"
        hold_str   = f"{avg_hold:.1f}" if not math.isnan(avg_hold) else "  n/a"
        pnl_str    = f"{avg_pnl:+.5f}" if not math.isnan(avg_pnl) else "    n/a"
        long_str   = f"{avg_long:.1%}" if not math.isnan(avg_long) else " n/a"

        marker = " ◀" if total_edge == max(
            sum(f["cum_edge"] for f in v) for v in results.values()
        ) else ""
        print(f"  {thresh:>7.2f}  {edge_str:>9}  {total_trades:>6}  "
              f"{win_str:>7}  {hold_str:>7}  {pnl_str:>8}  {sharpe_str:>7}  {long_str:>6}{marker}")

        if total_edge > best_edge:
            best_edge   = total_edge
            best_thresh = thresh

    print(f"\n  Best threshold: {best_thresh:.2f}  →  total edge = {best_edge:+.5f}")


def print_fold_detail(folds: list, thresh: float) -> None:
    print(f"\n{SEP}")
    print(f"  Per-Fold Detail  (threshold={thresh:.2f})")
    print(SEP)
    print(f"  {'Fold':>4}  {'CumEdge':>9}  {'Trades':>6}  {'WinRate':>7}  "
          f"{'AvgHold':>7}  {'Long%':>6}")
    print(f"  {'─'*4}  {'─'*9}  {'─'*6}  {'─'*7}  {'─'*7}  {'─'*6}")
    for f in folds:
        win_str  = f"{f['win_rate']:.1%}" if not math.isnan(f["win_rate"]) else "   n/a"
        hold_str = f"{f['avg_hold']:.1f}" if not math.isnan(f["avg_hold"]) else "  n/a"
        print(f"  {f['fold']:>4}  {f['cum_edge']:>+9.5f}  {f['n_trades']:>6}  "
              f"{win_str:>7}  {hold_str:>7}  {f['long_pct']:>5.1%}")

    total = sum(f["cum_edge"] for f in folds)
    wins  = sum(1 for f in folds if f["cum_edge"] > 0)
    print(f"\n  Total: {total:+.5f}  Winning folds: {wins}/{len(folds)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",     default="BTCUSDT")
    parser.add_argument("--timeframe",  default="15m")
    parser.add_argument("--train-bars", type=int,   default=DEFAULT_TRAIN_BARS)
    parser.add_argument("--test-bars",  type=int,   default=DEFAULT_TEST_BARS)
    parser.add_argument("--n-folds",    type=int,   default=DEFAULT_N_FOLDS)
    parser.add_argument("--fee",        type=float, default=DEFAULT_FEE)
    parser.add_argument("--thresholds", type=float, nargs="+",
                        default=DEFAULT_THRESHOLDS)
    args = parser.parse_args()

    print(f"\n╔{'═'*70}╗")
    print(f"║  LGBM Threshold Backtest — Long/Flat + Fees{'':27}║")
    print(f"║  {args.symbol}/{args.timeframe}  train={args.train_bars:,}  "
          f"test={args.test_bars:,}  folds={args.n_folds}  fee={args.fee*100:.2f}%{'':14}║")
    print(f"╚{'═'*70}╝")

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
            (args.symbol, args.timeframe),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"  No data for {args.symbol}/{args.timeframe}")
        return

    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "quote_asset_volume", "number_of_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume",
    ]
    df_raw = pd.DataFrame(rows, columns=cols)
    print(f"\n  Loaded {len(df_raw):,} candles")

    df = build_crypto_features(df_raw)
    df = df.iloc[MIN_VALID_ROWS:].reset_index(drop=True)
    feat_cols = get_feature_columns()
    print(f"  Features built: {len(feat_cols)} columns  ({len(df):,} rows after warm-up)")

    print(f"\n  Running {args.n_folds} folds × {len(args.thresholds)} thresholds...")
    results = run_backtest(
        df, feat_cols,
        train_bars=args.train_bars,
        test_bars=args.test_bars,
        n_folds=args.n_folds,
        fee_rate=args.fee,
        thresholds=args.thresholds,
    )

    print_results(results, fee_rate=args.fee,
                  n_folds=args.n_folds, test_bars=args.test_bars)

    # Show fold detail for the best threshold
    best_thresh = max(
        results.keys(),
        key=lambda t: sum(f["cum_edge"] for f in results[t])
    )
    print_fold_detail(results[best_thresh], thresh=best_thresh)

    # Verdict
    best_total = sum(f["cum_edge"] for f in results[best_thresh])
    best_wins  = sum(1 for f in results[best_thresh] if f["cum_edge"] > 0)
    print(f"\n{'═'*72}")
    if best_total > 0.01 and best_wins >= 7:
        print(f"  ✅ Features show real trading edge after fees — good foundation for RL")
    elif best_total > 0 and best_wins >= 5:
        print(f"  ⚠️  Marginal edge — signal exists but weak; consider new data sources")
    else:
        print(f"  ❌ No net edge after fees — features insufficient at this timeframe/fee")
    print(f"{'═'*72}\n")


if __name__ == "__main__":
    main()
