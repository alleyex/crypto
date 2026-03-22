"""Pure-Python logistic regression trainer.

No external ML library required — uses only the standard library and math.

Model
-----
Binary logistic regression trained with mini-batch gradient descent.
Weights are stored as a plain list (one per feature) plus a bias term.
The model is serialised to JSON for portability.

Public API
----------
train(X, y, ...)  -> TrainResult
predict_proba(weights, bias, X) -> List[float]
predict(weights, bias, X, threshold) -> List[int]
evaluate(y_true, y_pred) -> Dict
"""

import json
import math
import random
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Sigmoid
# ---------------------------------------------------------------------------

def _sigmoid(z: float) -> float:
    """Numerically stable sigmoid."""
    if z >= 0:
        return 1.0 / (1.0 + math.exp(-z))
    exp_z = math.exp(z)
    return exp_z / (1.0 + exp_z)


# ---------------------------------------------------------------------------
# Core training
# ---------------------------------------------------------------------------

def _dot(weights: List[float], row: List[float]) -> float:
    return sum(w * x for w, x in zip(weights, row))


def _forward(weights: List[float], bias: float, row: List[float]) -> float:
    return _sigmoid(_dot(weights, row) + bias)


def train(
    X: List[List[float]],
    y: List[int],
    n_features: int,
    learning_rate: float = 0.01,
    n_epochs: int = 100,
    batch_size: int = 32,
    l2_lambda: float = 1e-4,
    seed: int = 42,
) -> Dict[str, Any]:
    """Train a logistic regression model via mini-batch gradient descent.

    Parameters
    ----------
    X:             Training feature matrix (list of rows).
    y:             Binary labels (0 or 1).
    n_features:    Number of features per row (used to init weights).
    learning_rate: Step size for gradient descent.
    n_epochs:      Number of passes over training data.
    batch_size:    Mini-batch size (0 or >= len(X) → full-batch).
    l2_lambda:     L2 regularisation coefficient.
    seed:          Random seed for reproducibility.

    Returns
    -------
    Dict with keys: weights (list), bias (float), train_loss (float per epoch),
    n_epochs, learning_rate, l2_lambda.
    """
    if not X or not y:
        raise ValueError("X and y must be non-empty.")
    if len(X) != len(y):
        raise ValueError("X and y must have the same length.")

    rng = random.Random(seed)

    # Xavier initialisation
    scale = math.sqrt(2.0 / n_features) if n_features > 0 else 1.0
    weights = [rng.gauss(0, scale) for _ in range(n_features)]
    bias = 0.0

    n = len(X)
    actual_batch = n if (batch_size <= 0 or batch_size >= n) else batch_size

    loss_history: List[float] = []

    for epoch in range(n_epochs):
        # Shuffle indices each epoch
        indices = list(range(n))
        rng.shuffle(indices)

        epoch_loss = 0.0
        n_batches = 0

        for start in range(0, n, actual_batch):
            batch_idx = indices[start: start + actual_batch]
            batch_size_actual = len(batch_idx)

            # Gradients
            grad_w = [0.0] * n_features
            grad_b = 0.0
            batch_loss = 0.0

            for i in batch_idx:
                p = _forward(weights, bias, X[i])
                err = p - y[i]

                for j in range(n_features):
                    grad_w[j] += err * X[i][j]
                grad_b += err

                # Binary cross-entropy
                p_clamped = max(1e-12, min(1.0 - 1e-12, p))
                label = y[i]
                batch_loss += -(label * math.log(p_clamped) + (1 - label) * math.log(1 - p_clamped))

            # Average + L2 regularisation for weights
            for j in range(n_features):
                grad_w[j] = grad_w[j] / batch_size_actual + l2_lambda * weights[j]
                weights[j] -= learning_rate * grad_w[j]
            bias -= learning_rate * (grad_b / batch_size_actual)

            epoch_loss += batch_loss / batch_size_actual
            n_batches += 1

        loss_history.append(round(epoch_loss / n_batches, 6))

    return {
        "weights": weights,
        "bias": bias,
        "train_loss_history": loss_history,
        "final_train_loss": loss_history[-1] if loss_history else None,
        "n_epochs": n_epochs,
        "learning_rate": learning_rate,
        "l2_lambda": l2_lambda,
        "n_features": n_features,
        "n_train": len(X),
    }


# ---------------------------------------------------------------------------
# Inference helpers
# ---------------------------------------------------------------------------

def predict_proba(
    weights: List[float],
    bias: float,
    X: List[List[float]],
) -> List[float]:
    """Return P(y=1) for each row."""
    return [_forward(weights, bias, row) for row in X]


def predict(
    weights: List[float],
    bias: float,
    X: List[List[float]],
    threshold: float = 0.5,
) -> List[int]:
    """Return binary predictions."""
    return [1 if p >= threshold else 0 for p in predict_proba(weights, bias, X)]


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------

def evaluate(
    y_true: List[int],
    y_pred: List[int],
) -> Dict[str, Any]:
    """Compute accuracy, precision, recall, F1 for binary classification."""
    n = len(y_true)
    if n == 0:
        return {"n": 0, "accuracy": None, "precision": None, "recall": None, "f1": None}

    tp = sum(1 for t, p in zip(y_true, y_pred) if t == 1 and p == 1)
    fp = sum(1 for t, p in zip(y_true, y_pred) if t == 0 and p == 1)
    fn = sum(1 for t, p in zip(y_true, y_pred) if t == 1 and p == 0)
    correct = sum(1 for t, p in zip(y_true, y_pred) if t == p)

    accuracy = round(correct / n, 4)
    precision = round(tp / (tp + fp), 4) if (tp + fp) > 0 else None
    recall = round(tp / (tp + fn), 4) if (tp + fn) > 0 else None
    f1: Optional[float] = None
    if precision is not None and recall is not None and (precision + recall) > 0:
        f1 = round(2 * precision * recall / (precision + recall), 4)

    return {
        "n": n,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": correct - tp,
    }


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

def model_to_dict(
    train_result: Dict[str, Any],
    feature_names: List[str],
    symbol: str,
    timeframe: str,
    feature_set: str,
) -> Dict[str, Any]:
    """Wrap training output into a portable model dict."""
    return {
        "model_type": "logistic_regression_v1",
        "symbol": symbol,
        "timeframe": timeframe,
        "feature_set": feature_set,
        "feature_names": feature_names,
        "weights": train_result["weights"],
        "bias": train_result["bias"],
        "n_features": train_result["n_features"],
        "n_train": train_result["n_train"],
        "n_epochs": train_result["n_epochs"],
        "learning_rate": train_result["learning_rate"],
        "l2_lambda": train_result["l2_lambda"],
        "final_train_loss": train_result["final_train_loss"],
    }


def model_from_json(raw: str) -> Dict[str, Any]:
    return json.loads(raw)
