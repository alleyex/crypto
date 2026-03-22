"""Build a supervised training dataset from feature vectors.

Target
------
Binary next-period direction:
  y = 1  if close[t+1] > close[t]   (price went up)
  y = 0  otherwise                   (price flat or down)

Features used (all numeric, None → 0.0)
----------------------------------------
  returns_1, returns_5, returns_20
  ma_cross_5_20
  rsi_14         (scaled by /100 to [0,1])
  macd_hist
  bb_pct_b
  volatility_20

Any row where ``close`` is None is dropped.
The last row in the feature vector list is also dropped (no next-period label).
"""

from typing import Any, Dict, List, Optional, Tuple

# Ordered list of feature names used as model input.
FEATURE_NAMES: List[str] = [
    "returns_1",
    "returns_5",
    "returns_20",
    "ma_cross_5_20",
    "rsi_14",
    "macd_hist",
    "bb_pct_b",
    "volatility_20",
]


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _extract_row(fv: Dict[str, Any]) -> List[float]:
    """Return a single feature row as a list of floats."""
    row = []
    for name in FEATURE_NAMES:
        val = _safe_float(fv.get(name), 0.0)
        # Scale RSI to [0, 1]
        if name == "rsi_14":
            val = val / 100.0
        row.append(val)
    return row


def build_dataset(
    vectors: List[Dict[str, Any]],
) -> Tuple[List[List[float]], List[int], List[int]]:
    """Build (X, y, open_times) from a list of feature-vector dicts.

    Parameters
    ----------
    vectors:
        Feature-vector dicts in chronological order (oldest first).
        Each dict must contain at least ``close`` and ``open_time``.

    Returns
    -------
    X:         list of feature rows (floats), length N-1
    y:         list of binary labels (0 or 1), length N-1
    open_times: list of epoch-ms timestamps for each row, length N-1
                (timestamp of the observation, not the label)
    """
    # Filter out rows where close is missing
    valid = [v for v in vectors if v.get("close") is not None]

    X: List[List[float]] = []
    y: List[int] = []
    open_times: List[int] = []

    for i in range(len(valid) - 1):
        current = valid[i]
        next_v = valid[i + 1]

        current_close = _safe_float(current.get("close"))
        next_close = _safe_float(next_v.get("close"))

        if current_close <= 0:
            continue

        label = 1 if next_close > current_close else 0
        X.append(_extract_row(current))
        y.append(label)
        open_times.append(int(current.get("open_time", 0)))

    return X, y, open_times


def train_test_split(
    X: List[List[float]],
    y: List[int],
    open_times: List[int],
    test_ratio: float = 0.2,
) -> Tuple[
    List[List[float]], List[int], List[int],
    List[List[float]], List[int], List[int],
]:
    """Chronological split — no shuffling (time-series safe).

    Returns (X_train, y_train, times_train, X_test, y_test, times_test).
    """
    n = len(X)
    split = max(1, int(n * (1.0 - test_ratio)))
    return (
        X[:split], y[:split], open_times[:split],
        X[split:], y[split:], open_times[split:],
    )


def dataset_summary(y: List[int]) -> Dict[str, Any]:
    """Return class balance statistics for a label list."""
    n = len(y)
    if n == 0:
        return {"n": 0, "n_up": 0, "n_down": 0, "pct_up": None}
    n_up = sum(y)
    return {
        "n": n,
        "n_up": n_up,
        "n_down": n - n_up,
        "pct_up": round(n_up / n, 4),
    }
