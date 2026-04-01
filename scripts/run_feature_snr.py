#!/usr/bin/env python3
"""Feature SNR (Signal-to-Noise Ratio) analysis by layer.

Loads BTCUSDT 15m candles from DB, computes all features, then evaluates
each feature layer by IC / Rank IC / sign_sharpe against forward returns.

Usage:
    python scripts/run_feature_snr.py [--symbol BTCUSDT] [--timeframe 15m] [--horizons 1,5,20]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import numpy as np
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# Feature layers — incremental, each layer ADDS to the previous
# ──────────────────────────────────────────────────────────────────────────────
LAYERS: dict[str, list[str]] = {
    "L0_returns": [
        "log_ret_1", "log_ret_3", "log_ret_5",
    ],
    "L1_kbar": [
        "hl_spread", "close_location", "upper_wick_ratio", "lower_wick_ratio",
    ],
    "L2_trend": [
        "dist_sma_20", "dist_sma_60",
        "momentum_10_20", "momentum_20_60",
        "price_z_20", "price_z_50",
    ],
    "L3_oscillators": [
        "rsi_14", "adx_14", "macd_hist_norm",
    ],
    "L4_volatility": [
        "atr_14_norm_z", "rv_20_z", "bb_pct", "bb_width_norm",
        "atr_ratio_14_50",
    ],
    "L5_volume_flow": [
        "log_vol_z", "log_trades_z",
        "flow_imbalance", "flow_imbalance_5", "flow_imbalance_20",
        "vol_ratio_10_50",
    ],
    "L6_levels": [
        "dist_to_high_20", "dist_to_low_20",
        "dist_vwap_20", "trend_strength",
        "dist_prev_day_high", "dist_prev_day_low",
    ],
    "L7_time": [
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    ],
    "L8_v5_new": [
        "stoch_k", "stoch_d",
        "cci_20",
        "ret_z_5",
        "rsi_56", "rsi_224",
        "dist_day_open",
        "bb_squeeze",
    ],
}


def _compute_ic(feature: pd.Series, fwd_ret: pd.Series) -> tuple[float, float]:
    """Return (pearson_ic, spearman_rank_ic) dropping NaNs."""
    aligned = pd.DataFrame({"f": feature, "r": fwd_ret}).dropna()
    if len(aligned) < 50:
        return 0.0, 0.0
    ic = float(aligned["f"].corr(aligned["r"]))
    rank_ic = float(aligned["f"].rank().corr(aligned["r"].rank()))
    if not np.isfinite(ic):
        ic = 0.0
    if not np.isfinite(rank_ic):
        rank_ic = 0.0
    return ic, rank_ic


def _sign_sharpe(feature: pd.Series, fwd_ret: pd.Series) -> float:
    """Sharpe of long/short signal: sign(feature) x fwd_ret."""
    aligned = pd.DataFrame({"f": feature, "r": fwd_ret}).dropna()
    if len(aligned) < 50:
        return 0.0
    signal = np.sign(aligned["f"].to_numpy(dtype=float))
    active = signal != 0
    if not active.any():
        return 0.0
    signed = signal[active] * aligned["r"].to_numpy(dtype=float)[active]
    std = signed.std()
    if std < 1e-12:
        return 0.0
    return float(signed.mean() / std)


def analyze_layer(df: pd.DataFrame, features: list[str], horizons: list[int]) -> pd.DataFrame:
    rows = []
    for feat in features:
        if feat not in df.columns:
            rows.append({"feature": feat, "missing": True})
            continue
        for h in horizons:
            fwd = np.log(df["close"] / df["close"].shift(-h))
            ic, rank_ic = _compute_ic(df[feat], fwd)
            ss = _sign_sharpe(df[feat], fwd)
            rows.append({
                "feature": feat,
                "horizon": h,
                "IC": round(ic, 4),
                "Rank_IC": round(rank_ic, 4),
                "sign_sharpe": round(ss, 4),
                "abs_rank_ic": round(abs(rank_ic), 4),
            })
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",    default="BTCUSDT")
    parser.add_argument("--timeframe", default="15m")
    parser.add_argument("--horizons",  default="1,5,20")
    args = parser.parse_args()
    horizons = [int(h) for h in args.horizons.split(",")]

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
    df = build_crypto_features(pd.DataFrame(rows, columns=cols)).iloc[MIN_VALID_ROWS:].reset_index(drop=True)
    print(f"Rows after warm-up: {len(df)}\n")

    # Incremental: each layer adds to cumulative feature set
    cumulative: list[str] = []
    summary_rows = []

    for layer_name, layer_feats in LAYERS.items():
        cumulative.extend(layer_feats)
        avail = [f for f in layer_feats if f in df.columns]
        missing = [f for f in layer_feats if f not in df.columns]
        if missing:
            print(f"  [WARN] missing columns: {missing}")

        result = analyze_layer(df, avail, horizons)
        if result.empty or "IC" not in result.columns:
            continue

        # Per-layer summary (avg over horizons per feature, then mean across features)
        feat_summary = (
            result.groupby("feature")[["abs_rank_ic", "sign_sharpe"]]
            .mean()
            .reset_index()
        )
        avg_rank_ic  = feat_summary["abs_rank_ic"].mean()
        avg_ss       = feat_summary["sign_sharpe"].mean()
        best_feat    = feat_summary.sort_values("abs_rank_ic", ascending=False).iloc[0]

        print(f"{'='*60}")
        print(f"Layer: {layer_name}  ({len(avail)} features)")
        print(f"  avg |Rank IC|  = {avg_rank_ic:.4f}")
        print(f"  avg sign_sharpe = {avg_ss:.4f}")
        print(f"  best feature    = {best_feat['feature']} (|Rank IC|={best_feat['abs_rank_ic']:.4f})")
        print()

        # Per-feature breakdown
        h_pivot = result.pivot_table(index="feature", columns="horizon", values=["IC", "Rank_IC", "sign_sharpe"])
        h_pivot.columns = [f"{m}_h{h}" for m, h in h_pivot.columns]
        h_pivot = h_pivot.reset_index()
        h_pivot["avg_abs_rank_ic"] = feat_summary.set_index("feature").loc[h_pivot["feature"].values, "abs_rank_ic"].values
        h_pivot = h_pivot.sort_values("avg_abs_rank_ic", ascending=False)

        print(f"  {'Feature':<28} {'|RIC|':>6} {'RIC_h1':>8} {'RIC_h5':>8} {'RIC_h20':>8}")
        for _, row in h_pivot.iterrows():
            print(f"  {row['feature']:<28} {row['avg_abs_rank_ic']:>6.4f}"
                  f" {row.get('Rank_IC_h1', 0):>8.4f}"
                  f" {row.get('Rank_IC_h5', 0):>8.4f}"
                  f" {row.get('Rank_IC_h20', 0):>8.4f}")
        print()

        summary_rows.append({
            "layer": layer_name,
            "n_features": len(avail),
            "avg_abs_rank_ic": round(avg_rank_ic, 4),
            "avg_sign_sharpe": round(avg_ss, 4),
        })

    print("="*60)
    print("LAYER SUMMARY")
    print(f"  {'Layer':<25} {'N':>3}  {'|RankIC|':>8}  {'SignSharpe':>10}")
    for r in summary_rows:
        print(f"  {r['layer']:<25} {r['n_features']:>3}  {r['avg_abs_rank_ic']:>8.4f}  {r['avg_sign_sharpe']:>10.4f}")


if __name__ == "__main__":
    main()
