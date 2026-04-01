from __future__ import annotations

import pandas as pd

from app.features.crypto_features import build_crypto_features


def test_build_crypto_features_merges_orderbook_asof_without_future_leakage() -> None:
    candles = pd.DataFrame(
        {
            "open_time": [1_000, 2_000, 3_000],
            "open": [100.0, 101.0, 102.0],
            "high": [101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 101.0],
            "close": [100.5, 101.5, 102.5],
            "volume": [10.0, 11.0, 12.0],
            "quote_asset_volume": [1_005.0, 1_116.5, 1_230.0],
            "number_of_trades": [100, 110, 120],
            "taker_buy_base_volume": [5.0, 6.0, 7.0],
            "taker_buy_quote_volume": [502.5, 609.0, 717.5],
        }
    )
    orderbook = pd.DataFrame(
        {
            "timestamp_ms": [900, 2_500],
            "ob_imbalance": [0.2, -0.4],
            "spread_pct": [0.0001, 0.0002],
            "mid_price": [100.4, 102.7],
        }
    )

    result = build_crypto_features(candles, orderbook_df=orderbook)

    assert result["ob_imbalance"].tolist() == [0.2, 0.2, -0.4]
    assert result["spread_bps"].tolist() == [1.0, 1.0, 2.0]
    assert result["ob_imbalance_5_mean"].round(6).tolist() == [0.2, 0.2, 0.0]

    mid_dev = result["mid_dev_from_close"].round(8).tolist()
    assert mid_dev == [-0.00099502, -0.01000000, 0.00195122]
