import numpy as np
import pandas as pd

from scripts import run_lgbm_walkforward as wf


class _FakeModel:
    def __init__(self, **kwargs):
        self.feature_importances_ = np.array([1.0])

    def fit(self, X, y):
        self.seen_X = X
        self.seen_y = y
        return self

    def predict_proba(self, X):
        return np.tile(np.array([[0.4, 0.6]]), (len(X), 1))


def test_run_walkforward_uses_forward_return_for_signal_ret(monkeypatch):
    captured = {}

    def fake_sharpe(returns, periods_per_year=525_600):
        del periods_per_year
        captured["returns"] = np.array(returns, copy=True)
        return 0.0

    monkeypatch.setattr(wf.lgb, "LGBMClassifier", _FakeModel)
    monkeypatch.setattr(wf, "_sharpe", fake_sharpe)

    df = pd.DataFrame(
        {
            "feature_a": [10, 20, 30, 40, 50, 60],
            "log_ret_1": [0.10, 0.20, 0.30, 0.40, 0.50, np.nan],
        }
    )

    result = wf.run_walkforward(
        df=df,
        feat_cols=["feature_a"],
        train_bars=2,
        test_bars=2,
        n_folds=1,
    )

    assert result["folds"][0]["n_test"] == 2
    assert np.allclose(captured["returns"], np.array([0.40, 0.50]))

