"""Phase 8 — LightGBM Walk-Forward Baseline Validation.

Strategy:
  - Target: sign of next-bar log return (binary: 1=up, 0=down/flat)
  - Walk-forward: fixed train window, step forward by STEP_SIZE bars
  - No random split — time order is strictly preserved
  - Evaluate: accuracy, log-loss, directional Sharpe, feature importance

Usage:
    python scripts/run_lgbm_walkforward.py [--symbol BTCUSDT] [--timeframe 1m]
        [--train-bars 5000] [--test-bars 1000] [--n-folds 10]
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
DEFAULT_TRAIN_BARS = 5000
DEFAULT_TEST_BARS  = 1000
DEFAULT_N_FOLDS    = 10

LGB_PARAMS = {
    "objective":        "binary",
    "metric":           "binary_logloss",
    "n_estimators":     200,
    "learning_rate":    0.05,
    "num_leaves":       31,
    "min_child_samples": 50,
    "feature_fraction": 0.8,
    "bagging_fraction": 0.8,
    "bagging_freq":     5,
    "verbose":          -1,
    "n_jobs":           -1,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sharpe(returns: np.ndarray, periods_per_year: int = 525_600) -> float:
    """Annualised Sharpe of a return series (1m default: 525600 bars/yr)."""
    if len(returns) < 2:
        return float("nan")
    mu  = returns.mean()
    std = returns.std()
    if std == 0:
        return float("nan")
    return (mu / std) * math.sqrt(periods_per_year)


def _accuracy(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    return float((y_true == y_pred).mean())


def _log_loss(y_true: np.ndarray, proba: np.ndarray, eps: float = 1e-7) -> float:
    p = np.clip(proba, eps, 1 - eps)
    return float(-np.mean(y_true * np.log(p) + (1 - y_true) * np.log(1 - p)))


# ---------------------------------------------------------------------------
# Walk-forward loop
# ---------------------------------------------------------------------------

def run_walkforward(
    df: pd.DataFrame,
    feat_cols: list,
    train_bars: int,
    test_bars: int,
    n_folds: int,
) -> dict:
    """Run walk-forward validation; returns dict of aggregated results."""

    # Target: 1 if next bar closes higher, else 0
    df = df.copy()
    df["target"] = (df["log_ret_1"].shift(-1) > 0).astype(int)
    df = df.dropna(subset=feat_cols + ["target"]).reset_index(drop=True)

    total_needed = train_bars + test_bars * n_folds
    if len(df) < total_needed:
        raise ValueError(
            f"Need {total_needed:,} rows but only {len(df):,} available. "
            f"Reduce n_folds or train/test bars."
        )

    fold_results = []
    importance_accum: dict = {f: 0.0 for f in feat_cols}

    for fold in range(n_folds):
        train_start = fold * test_bars
        train_end   = train_start + train_bars
        test_end    = train_end + test_bars

        X_train = df[feat_cols].iloc[train_start:train_end]
        y_train = df["target"].iloc[train_start:train_end].values
        X_test  = df[feat_cols].iloc[train_end:test_end]
        y_test  = df["target"].iloc[train_end:test_end].values

        model = lgb.LGBMClassifier(**LGB_PARAMS)
        model.fit(X_train, y_train)

        proba  = model.predict_proba(X_test)[:, 1]
        pred   = (proba >= 0.5).astype(int)

        # Directional returns: go long if pred=1, short if pred=0
        actual_ret = df["log_ret_1"].iloc[train_end:test_end].values
        signal_ret = np.where(pred == 1, actual_ret, -actual_ret)

        acc     = _accuracy(y_test, pred)
        ll      = _log_loss(y_test, proba)
        sharpe  = _sharpe(signal_ret)
        pos_rate = pred.mean()

        fold_results.append({
            "fold":      fold + 1,
            "acc":       acc,
            "log_loss":  ll,
            "sharpe":    sharpe,
            "pos_rate":  pos_rate,
            "n_test":    len(y_test),
        })

        # Accumulate feature importance
        imp = model.feature_importances_
        for fname, fval in zip(feat_cols, imp):
            importance_accum[fname] += float(fval)

    # Average importance across folds
    avg_importance = {k: v / n_folds for k, v in importance_accum.items()}

    return {
        "folds":       fold_results,
        "importance":  avg_importance,
    }


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------

SEP = "─" * 64


def print_fold_results(folds: list) -> None:
    print(f"\n{SEP}")
    print(f"  Walk-Forward Fold Results")
    print(SEP)
    print(f"  {'Fold':>4}  {'Acc':>6}  {'LogLoss':>8}  {'Sharpe':>8}  {'Long%':>6}")
    print(f"  {'─'*4}  {'─'*6}  {'─'*8}  {'─'*8}  {'─'*6}")
    for r in folds:
        sharpe_str = f"{r['sharpe']:+.2f}" if not math.isnan(r["sharpe"]) else "   n/a"
        print(
            f"  {r['fold']:>4}  {r['acc']:>6.3f}  {r['log_loss']:>8.4f}"
            f"  {sharpe_str:>8}  {r['pos_rate']:>5.1%}"
        )


def print_summary(folds: list) -> None:
    accs    = [r["acc"]      for r in folds]
    losses  = [r["log_loss"] for r in folds]
    sharpes = [r["sharpe"]   for r in folds if not math.isnan(r["sharpe"])]

    print(f"\n{SEP}")
    print(f"  Summary  ({len(folds)} folds)")
    print(SEP)
    print(f"  Accuracy  : {np.mean(accs):.4f}  ±{np.std(accs):.4f}"
          f"  (baseline ≈ 0.500)")
    print(f"  Log-loss  : {np.mean(losses):.4f}  ±{np.std(losses):.4f}"
          f"  (baseline ≈ 0.693)")
    if sharpes:
        avg_sh = np.mean(sharpes)
        label = "STRONG" if avg_sh > 1.0 else ("signal" if avg_sh > 0.3 else "weak")
        print(f"  Sharpe    : {avg_sh:+.3f}  ±{np.std(sharpes):.3f}  → {label}")
    else:
        print(f"  Sharpe    : n/a")


def print_importance(importance: dict) -> None:
    ranked = sorted(importance.items(), key=lambda x: -x[1])
    total  = sum(v for _, v in ranked) or 1.0

    print(f"\n{SEP}")
    print(f"  Feature Importance  (avg gain across folds)")
    print(SEP)
    for feat, val in ranked:
        pct = val / total * 100
        bar = "█" * max(1, int(pct / 2))
        print(f"  {feat:<32} {pct:5.1f}%  {bar}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",      default="BTCUSDT")
    parser.add_argument("--timeframe",   default="1m")
    parser.add_argument("--train-bars",  type=int, default=DEFAULT_TRAIN_BARS)
    parser.add_argument("--test-bars",   type=int, default=DEFAULT_TEST_BARS)
    parser.add_argument("--n-folds",     type=int, default=DEFAULT_N_FOLDS)
    args = parser.parse_args()

    symbol, tf = args.symbol, args.timeframe

    print(f"\n╔{'═'*62}╗")
    print(f"║  Phase 8 — LightGBM Walk-Forward Baseline{'':20}║")
    print(f"║  {symbol}/{tf}  train={args.train_bars:,}  test={args.test_bars:,}  folds={args.n_folds}{'':14}║")
    print(f"╚{'═'*62}╝")

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
    print(f"\n  Loaded {len(df_raw):,} candles")

    df = build_crypto_features(df_raw)
    df = df.iloc[MIN_VALID_ROWS:].reset_index(drop=True)
    print(f"  Features built, {len(df):,} rows after warm-up")

    feat_cols = get_feature_columns()
    print(f"  Feature columns: {len(feat_cols)}")

    print(f"\n  Running {args.n_folds} walk-forward folds...")
    results = run_walkforward(
        df,
        feat_cols,
        train_bars=args.train_bars,
        test_bars=args.test_bars,
        n_folds=args.n_folds,
    )

    print_fold_results(results["folds"])
    print_summary(results["folds"])
    print_importance(results["importance"])

    # Final verdict
    accs    = [r["acc"]    for r in results["folds"]]
    sharpes = [r["sharpe"] for r in results["folds"]
               if not math.isnan(r["sharpe"])]
    avg_acc    = np.mean(accs)
    avg_sharpe = np.mean(sharpes) if sharpes else float("nan")

    print(f"\n{'═'*64}")
    if avg_acc > 0.515 or avg_sharpe > 0.5:
        print(f"  ✅ Features show exploitable signal — proceed to RL agent")
    elif avg_acc > 0.505 or avg_sharpe > 0.0:
        print(f"  ⚠️  Marginal signal — features need enrichment before RL")
    else:
        print(f"  ❌ No signal detected — revisit feature engineering")
    print(f"{'═'*64}\n")


if __name__ == "__main__":
    main()
