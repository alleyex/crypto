"""Minimal episodic trading environment for REINFORCE experiments.

Episode
-------
One pass through a sequence of (feature_row, close_price) pairs.

Actions
-------
  0  HOLD  — flat, no position; reward = 0
  1  LONG  — enter/hold long; reward = log-return to next candle less fees

Reward
------
  gross_t = log(close_{t+1} / close_t)  if action = LONG
  gross_t = 0                            if action = HOLD

  fee_t = fee_rate  when position changes between HOLD and LONG
  fee_t = 0         otherwise

  r_t = gross_t - fee_t

Terminal step (last candle): reward = 0 regardless of action.

Observations
------------
A flat list of floats (one feature row).  None values are pre-filled
to 0.0 at construction time.
"""

import math
from typing import Any, Dict, List, Optional, Tuple


class TradingEnv:
    """Tabular trading environment driven by pre-computed feature rows."""

    def __init__(
        self,
        feature_rows: List[List[float]],
        closes: List[float],
        fee_rate: float = 0.0,
    ) -> None:
        if len(feature_rows) != len(closes):
            raise ValueError("feature_rows and closes must have the same length.")
        if len(feature_rows) < 2:
            raise ValueError("Need at least 2 steps for a meaningful episode.")
        if fee_rate < 0:
            raise ValueError("fee_rate must be non-negative.")
        self._rows = feature_rows
        self._closes = closes
        self._fee_rate = fee_rate
        self._t: int = 0
        self._done: bool = False
        self._position: int = 0

    # ------------------------------------------------------------------
    # Gym-like interface
    # ------------------------------------------------------------------

    def reset(self) -> List[float]:
        self._t = 0
        self._done = False
        self._position = 0
        return self._rows[0]

    def step(self, action: int) -> Tuple[Optional[List[float]], float, bool]:
        """Take one step.

        Returns
        -------
        (next_obs, reward, done)
        next_obs is None when done.
        """
        if self._done:
            raise RuntimeError("Episode is done. Call reset() first.")

        t = self._t
        n = len(self._rows)
        is_last = (t == n - 1)

        new_position = 1 if action == 1 else 0
        fee = self._fee_rate if new_position != self._position else 0.0

        # Reward: log-return if LONG, but 0 on the last step (no next candle)
        if new_position == 1 and not is_last:
            c_now = self._closes[t]
            c_next = self._closes[t + 1]
            gross_reward = math.log(c_next / c_now) if c_now > 0 else 0.0
        else:
            gross_reward = 0.0

        reward = gross_reward - fee
        self._position = new_position

        self._t += 1
        if self._t >= n:
            self._done = True
            return None, reward, True

        return self._rows[self._t], reward, False

    @property
    def n_steps(self) -> int:
        return len(self._rows)

    @property
    def n_features(self) -> int:
        return len(self._rows[0]) if self._rows else 0


# ---------------------------------------------------------------------------
# Utility: compute cumulative return + Sharpe from a list of per-step rewards
# ---------------------------------------------------------------------------

def episode_metrics(rewards: List[float], actions: List[int]) -> Dict[str, Any]:
    """Summarise one episode's rewards and actions."""
    n = len(rewards)
    if n == 0:
        return {
            "n_steps": 0, "n_trades": 0,
            "cumulative_return": 0.0,
            "sharpe": None,
            "mean_reward": None,
        }

    n_trades = sum(1 for a in actions if a == 1)
    cum_return = sum(rewards)  # sum of log-returns ≈ log of total multiplier

    trade_rewards = [r for r, a in zip(rewards, actions) if a == 1]
    if len(trade_rewards) >= 2:
        mean_r = sum(trade_rewards) / len(trade_rewards)
        variance = sum((r - mean_r) ** 2 for r in trade_rewards) / len(trade_rewards)
        import math as _math
        std_r = _math.sqrt(variance) if variance > 0 else 0.0
        sharpe = (mean_r / std_r) * _math.sqrt(252 * 24 * 60) if std_r > 0 else None
    else:
        sharpe = None

    return {
        "n_steps": n,
        "n_trades": n_trades,
        "cumulative_return": round(cum_return, 6),
        "sharpe": round(sharpe, 4) if sharpe is not None else None,
        "mean_reward": round(sum(rewards) / n, 8),
    }


def buy_and_hold_return(closes: List[float]) -> float:
    """Log-return of a pure buy-and-hold strategy over the close series."""
    if len(closes) < 2 or closes[0] <= 0:
        return 0.0
    return math.log(closes[-1] / closes[0])


def buy_and_hold_return_after_fees(closes: List[float], fee_rate: float = 0.0) -> float:
    """Log-return of buy-and-hold with one entry fee and one exit fee."""
    gross = buy_and_hold_return(closes)
    if fee_rate <= 0 or len(closes) < 2:
        return gross
    return gross - (2.0 * fee_rate)
