#!/usr/bin/env python3
"""Greedy forward feature selection using LGBM walk-forward accuracy.

Algorithm:
  1. Pre-rank all candidate features by Rank IC (SNR proxy)
  2. Start with an empty selected set
  3. For each candidate (in Rank IC order), evaluate accuracy with it added
  4. Keep the feature if it improves accuracy by >= MIN_GAIN
  5. Report the final optimal feature subset

Usage:
    python scripts/run_lgbm_forward_selection.py [--symbol BTCUSDT] [--timeframe 15m]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd
import lightgbm as lgb

# ── Parameters ────────────────────────────────────────────────────────────────
TRAIN_BARS = 10_000   # ~104 days of 15m bars
TEST_BARS  =  2_000   # ~21 days
N_FOLDS    =     10
MIN_GAIN   =  0.0003  # minimum accuracy improvement to keep a feature

LGB_PARAMS = {
    "objective":         "binary",
    "metric":            "binary_logloss",
    "n_estimators":      100,
    "learning_rate":     0.05,
    "num_leaves":        31,
    "min_child_samples": 50,
    "feature_fraction":  0.8,
    "bagging_fraction":  0.8,
    "bagging_freq":      5,
    "verbose":           -1,
    "n_jobs":            -1,
}

# ── Candidate features pre-ranked by Rank IC from SNR analysis ────────────────
# (run scripts/run_feature_snr.py to regenerate this ranking)
CANDIDATES: list[str] = [
    # Strong (Rank IC >= 0.03) — sorted by Rank IC desc
    "rsi_14",           # 0.0389
    "log_ret_3",        # 0.0388
    "stoch_k",          # 0.0386 (V5 new)
    "dist_to_high_20",  # 0.0379
    "flow_imbalance_5", # 0.0355
    "bb_pct",           # 0.0355
    "price_z_20",       # 0.0351
    "ret_z_5",          # 0.0344 (V5 new)
    "log_ret_5",        # 0.0348
    "dist_vwap_20",     # 0.0347
    "flow_imbalance",   # 0.0340
    "trend_strength",   # 0.0331
    "dist_sma_20",      # 0.0330
    "rsi_56",           # 0.0321 (V5 new)
    "close_location",   # 0.0325
    "stoch_d",          # 0.0317 (V5 new)
    "log_ret_1",        # 0.0324
    "dist_prev_day_high", # 0.0298
    "dist_sma_60",      # 0.0295
    "price_z_50",       # 0.0286
    "dist_day_open",    # 0.0270 (V5 new)
    # Medium (0.01 <= Rank IC < 0.03)
    "flow_imbalance_20", # 0.0217
    "momentum_20_60",   # 0.0201
    "dist_to_low_20",   # 0.0191
    "momentum_10_20",   # 0.0178
    "hl_spread",        # 0.0181
    "macd_hist_norm",   # 0.0137
    "lower_wick_ratio", # 0.0105
    "atr_14_norm_z",    # 0.0100
    "atr_ratio_14_50",  # 0.0098
    "upper_wick_ratio", # 0.0099
    "bb_width_norm",    # 0.0083
    "dist_prev_day_low",# 0.0199
    # Weak (Rank IC < 0.01) — included to confirm they add no value
    "streak",
    "avg_quote_per_trade_z",
    "liquidity_proxy_z",
    "hl_spread_z",
    "log_ret_10",
    "log_ret_20",
    "rv_20_z",          # 0.0059
    "hour_sin",         # 0.0065
    "hour_cos",         # 0.0079
    "is_asia_session",
    "is_us_session",
    "log_trades_z",     # 0.0034
    "dow_sin",          # 0.0040
    "log_vol_z",        # 0.0019
    "dow_cos",          # 0.0015
    "adx_14",           # 0.0013
]


# ── Walk-forward scorer ────────────────────────────────────────────────────────

def _score(df: pd.DataFrame, feat_cols: list[str]) -> float:
    """Return mean walk-forward accuracy over N_FOLDS."""
    if not feat_cols:
        return 0.5  # random baseline

    data = df.copy()
    data["fwd_log_ret_1"] = data["log_ret_1"].shift(-1)
    data = data.dropna(subset=feat_cols + ["fwd_log_ret_1"]).reset_index(drop=True)
    data["target"] = (data["fwd_log_ret_1"] > 0).astype(int)

    total_needed = TRAIN_BARS + TEST_BARS * N_FOLDS
    if len(data) < total_needed:
        return 0.5

    accs = []
    for fold in range(N_FOLDS):
        ts = fold * TEST_BARS
        te = ts + TRAIN_BARS
        ve = te + TEST_BARS

        X_tr = data[feat_cols].iloc[ts:te]
        y_tr = data["target"].iloc[ts:te].values
        X_va = data[feat_cols].iloc[te:ve]
        y_va = data["target"].iloc[te:ve].values

        model = lgb.LGBMClassifier(**LGB_PARAMS)
        model.fit(X_tr, y_tr)
        pred = (model.predict_proba(X_va)[:, 1] >= 0.5).astype(int)
        accs.append(float((y_va == pred).mean()))

    return float(np.mean(accs))


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",    default="BTCUSDT")
    parser.add_argument("--timeframe", default="15m")
    args = parser.parse_args()

    from app.core.db import get_connection
    from app.features.crypto_features import build_crypto_features, MIN_VALID_ROWS

    print(f"Loading {args.symbol}/{args.timeframe} candles...")
    conn = get_connection()
    rows = conn.execute(
        """SELECT open_time, open, high, low, close, volume,
                  quote_asset_volume, number_of_trades,
                  taker_buy_base_volume, taker_buy_quote_volume
           FROM candles WHERE symbol=? AND timeframe=? ORDER BY open_time ASC""",
        (args.symbol, args.timeframe),
    ).fetchall()
    conn.close()

    cols = ["open_time", "open", "high", "low", "close", "volume",
            "quote_asset_volume", "number_of_trades",
            "taker_buy_base_volume", "taker_buy_quote_volume"]
    df = build_crypto_features(
        pd.DataFrame(rows, columns=cols)
    ).iloc[MIN_VALID_ROWS:].reset_index(drop=True)
    print(f"Rows after warm-up: {len(df):,}\n")

    # Filter candidates to only columns that actually exist
    available = [f for f in CANDIDATES if f in df.columns]
    missing   = [f for f in CANDIDATES if f not in df.columns]
    if missing:
        print(f"[WARN] missing columns (skipped): {missing}\n")

    print(f"Candidate features: {len(available)}")
    print(f"Walk-forward: train={TRAIN_BARS:,}  test={TEST_BARS:,}  folds={N_FOLDS}")
    print(f"Min gain to keep feature: {MIN_GAIN}\n")
    print("=" * 64)

    selected: list[str] = []
    baseline_acc = _score(df, selected)  # 0.5
    current_acc  = baseline_acc

    kept_log: list[tuple[str, float, float]] = []
    dropped_log: list[tuple[str, float]] = []

    for i, feat in enumerate(available):
        trial = selected + [feat]
        acc   = _score(df, trial)
        gain  = acc - current_acc
        symbol = "✓" if gain >= MIN_GAIN else "✗"
        print(f"  [{i+1:2d}/{len(available)}] {symbol} {feat:<32}  acc={acc:.4f}  Δ={gain:+.4f}")

        if gain >= MIN_GAIN:
            selected.append(feat)
            current_acc = acc
            kept_log.append((feat, acc, gain))
        else:
            dropped_log.append((feat, gain))

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 64)
    print(f"RESULT: {len(selected)} features kept  (baseline={baseline_acc:.4f} → final={current_acc:.4f})")
    print("=" * 64)
    print("\nSelected feature set:")
    for j, (f, acc, g) in enumerate(kept_log):
        print(f"  {j+1:2d}. {f:<32}  cumulative_acc={acc:.4f}  Δ={g:+.4f}")

    print("\nDropped (no contribution):")
    dropped_log.sort(key=lambda x: x[1])
    for f, g in dropped_log:
        print(f"  ✗  {f:<32}  Δ={g:+.4f}")

    print(f"\nFinal feature list (copy into get_feature_columns):")
    print(repr(selected))


if __name__ == "__main__":
    main()
