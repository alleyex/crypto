"""PPO Training + Walk-Forward Validation.

Workflow:
  1. Load candles from DB, build V1 features
  2. Train PPO on TRAIN portion (chronologically first)
  3. Walk-forward evaluation on N held-out windows (no overlap with train)
  4. Save model to runtime/models/
  5. Print full report

Walk-forward design:
  Data is split chronologically: first TRAIN_FRAC → training,
  remaining → evaluation windows of EVAL_EP_LEN bars each (no data leakage).

Usage:
    python scripts/train_ppo.py [--symbol BTCUSDT] [--timeframe 1m]
        [--steps 1000000] [--eval-windows 8]
"""

import sys
import argparse
import math
import warnings
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
from stable_baselines3 import PPO
from stable_baselines3.common.monitor import Monitor

from app.core.db import get_connection
from app.features.crypto_features import build_crypto_features, get_feature_columns, MIN_VALID_ROWS
from app.rl.crypto_env import CryptoTradingEnv, FEE_PER_SIDE

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
TRAIN_FRAC    = 0.70      # 70% of data for training
TRAIN_EP_LEN  = 1440      # episode length during training (1 day of 1m)
EVAL_EP_LEN   = 2880      # evaluation window length (2 days)
DEFAULT_STEPS = 1_000_000
DEFAULT_WINDOWS = 8

MODELS_DIR = ROOT / "runtime" / "models"

PPO_KWARGS = dict(
    learning_rate = 3e-4,
    n_steps       = 2048,
    batch_size    = 256,
    n_epochs      = 10,
    gamma         = 0.99,
    gae_lambda    = 0.95,
    clip_range    = 0.2,
    ent_coef      = 0.01,
    policy_kwargs = {"net_arch": [128, 128]},
    verbose       = 0,
    seed          = 42,
)

SEP = "─" * 66


# ---------------------------------------------------------------------------
# Evaluation helpers
# ---------------------------------------------------------------------------

def _run_episode(env: CryptoTradingEnv, model=None) -> dict:
    """Run one episode. model=None → always long (buy-and-hold)."""
    obs, _ = env.reset()
    total_r = 0.0
    pos_counts = {-1: 0, 0: 0, 1: 0}
    n_trades = 0
    prev_pos = 0

    while True:
        if model is not None:
            action, _ = model.predict(obs, deterministic=True)
            action = int(action)
        else:
            action = 1  # always long

        obs, r, done, trunc, info = env.step(action)
        total_r += r
        pos = info.get("position", 0)
        pos_counts[pos] = pos_counts.get(pos, 0) + 1
        if pos != prev_pos:
            n_trades += 1
        prev_pos = pos
        if done or trunc:
            break

    total_steps = sum(pos_counts.values())
    return {
        "log_ret":  total_r,
        "pct_ret":  math.exp(total_r) - 1,
        "n_trades": n_trades,
        "long_pct":  pos_counts.get(1,  0) / max(total_steps, 1),
        "short_pct": pos_counts.get(-1, 0) / max(total_steps, 1),
        "flat_pct":  pos_counts.get(0,  0) / max(total_steps, 1),
    }


def walk_forward_eval(df: pd.DataFrame, model, eval_start_idx: int,
                      n_windows: int, ep_len: int) -> list:
    """Evaluate model on N sequential non-overlapping windows."""
    results = []
    idx = eval_start_idx
    available = len(df) - idx - 1

    if available < ep_len:
        raise ValueError(
            f"Not enough eval data: need {ep_len}, have {available}"
        )

    actual_windows = min(n_windows, available // ep_len)

    for w in range(actual_windows):
        window_df = df.iloc[idx: idx + ep_len + 1].reset_index(drop=True)

        ppo_env = CryptoTradingEnv(window_df, episode_length=ep_len, deterministic=True)
        bnh_env = CryptoTradingEnv(window_df, episode_length=ep_len, deterministic=True)

        ppo_r = _run_episode(ppo_env, model)
        bnh_r = _run_episode(bnh_env, model=None)

        results.append({
            "window":     w + 1,
            "start_bar":  idx,
            "ppo":        ppo_r,
            "bnh":        bnh_r,
            "beats_bnh":  ppo_r["log_ret"] > bnh_r["log_ret"],
        })
        idx += ep_len

    return results


# ---------------------------------------------------------------------------
# Print helpers
# ---------------------------------------------------------------------------

def print_walkforward(results: list) -> None:
    print(f"\n{SEP}")
    print(f"  Walk-Forward Evaluation  ({len(results)} windows × {EVAL_EP_LEN} bars)")
    print(SEP)
    print(f"  {'Win':>3}  {'PPO ret':>9}  {'B&H ret':>9}  {'Edge':>8}  "
          f"{'Long%':>6}  {'Short%':>7}  {'Trades':>7}")
    print(f"  {'─'*3}  {'─'*9}  {'─'*9}  {'─'*8}  {'─'*6}  {'─'*7}  {'─'*7}")

    for r in results:
        ppo = r["ppo"]
        bnh = r["bnh"]
        edge = ppo["log_ret"] - bnh["log_ret"]
        flag = "✅" if r["beats_bnh"] else "❌"
        print(
            f"  {r['window']:>3}  {ppo['pct_ret']:>+8.2%}  {bnh['pct_ret']:>+8.2%}"
            f"  {edge:>+7.4f}  {ppo['long_pct']:>5.0%}  "
            f"{ppo['short_pct']:>6.0%}  {ppo['n_trades']:>6}  {flag}"
        )


def print_summary(results: list, train_steps: int, symbol: str, tf: str) -> None:
    ppo_rets = [r["ppo"]["log_ret"] for r in results]
    bnh_rets = [r["bnh"]["log_ret"] for r in results]
    edges    = [p - b for p, b in zip(ppo_rets, bnh_rets)]
    wins     = sum(1 for r in results if r["beats_bnh"])

    avg_ppo  = np.mean(ppo_rets)
    avg_bnh  = np.mean(bnh_rets)
    avg_edge = np.mean(edges)
    win_rate = wins / len(results)

    print(f"\n{SEP}")
    print(f"  Summary  —  {symbol}/{tf}  ({train_steps:,} training steps)")
    print(SEP)
    print(f"  Win rate vs B&H : {wins}/{len(results)}  ({win_rate:.0%})")
    print(f"  Avg PPO return  : {avg_ppo:+.4f}  ({math.exp(avg_ppo)-1:+.2%})")
    print(f"  Avg B&H return  : {avg_bnh:+.4f}  ({math.exp(avg_bnh)-1:+.2%})")
    print(f"  Avg edge        : {avg_edge:+.4f}")

    print(f"\n{'═'*66}")
    if win_rate >= 0.75 and avg_edge > 0:
        verdict = "✅ PASS — model shows consistent edge, ready to proceed"
    elif win_rate >= 0.5 and avg_edge > 0:
        verdict = "⚠️  MARGINAL — positive edge but inconsistent, train longer"
    else:
        verdict = "❌ FAIL — no consistent edge, review features or reward"
    print(f"  {verdict}")
    print(f"{'═'*66}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol",       default="BTCUSDT")
    parser.add_argument("--timeframe",    default="1m")
    parser.add_argument("--steps",        type=int, default=DEFAULT_STEPS)
    parser.add_argument("--eval-windows", type=int, default=DEFAULT_WINDOWS)
    args = parser.parse_args()

    symbol, tf = args.symbol, args.timeframe

    print(f"\n╔{'═'*64}╗")
    print(f"║  PPO Training + Walk-Forward Validation{'':25}║")
    print(f"║  {symbol}/{tf}  steps={args.steps:,}  eval_windows={args.eval_windows}{'':19}║")
    print(f"╚{'═'*64}╝")

    # --- Load data ---
    print("\n  Loading candles from DB...")
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT open_time, open, high, low, close, volume,
                   quote_asset_volume, number_of_trades,
                   taker_buy_base_volume, taker_buy_quote_volume
            FROM candles WHERE symbol=? AND timeframe=? ORDER BY open_time ASC
            """,
            (symbol, tf),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"  No data for {symbol}/{tf}")
        return

    cols = ["open_time","open","high","low","close","volume",
            "quote_asset_volume","number_of_trades",
            "taker_buy_base_volume","taker_buy_quote_volume"]
    df = build_crypto_features(
        pd.DataFrame(rows, columns=cols)
    ).iloc[MIN_VALID_ROWS:].reset_index(drop=True)

    n_total     = len(df)
    train_end   = int(n_total * TRAIN_FRAC)
    train_df    = df.iloc[:train_end].reset_index(drop=True)
    print(f"  Total: {n_total:,} rows  │  Train: {train_end:,}  │  Eval: {n_total-train_end:,}")

    # --- Train ---
    print(f"\n  Training PPO ({args.steps:,} steps)...")
    train_env = Monitor(CryptoTradingEnv(train_df, episode_length=TRAIN_EP_LEN, seed=0))
    model = PPO("MlpPolicy", train_env, **PPO_KWARGS)
    model.learn(total_timesteps=args.steps)
    print("  Training complete.")

    # --- Walk-forward eval ---
    print(f"\n  Running {args.eval_windows}-window walk-forward on held-out data...")
    results = walk_forward_eval(
        df,
        model,
        eval_start_idx = train_end,
        n_windows      = args.eval_windows,
        ep_len         = EVAL_EP_LEN,
    )
    print_walkforward(results)
    print_summary(results, args.steps, symbol, tf)

    # --- Save model ---
    MODELS_DIR.mkdir(parents=True, exist_ok=True)
    model_path = MODELS_DIR / f"ppo_{symbol}_{tf}"
    model.save(str(model_path))
    print(f"  Model saved → {model_path}.zip")


if __name__ == "__main__":
    main()
