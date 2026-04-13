import pytest

from app.core.migrations import run_migrations
from app.query.read_service import get_execution_report
from conftest import make_connection


def test_get_execution_report_prefers_binance_user_trade_commission(monkeypatch) -> None:
    from app.execution.binance_broker import BinanceBrokerClient

    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, broker_name, broker_order_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (
                'ppo-buy-1', 1, 'binance', '123', 'SOLUSDT', '1m', 'ppo', 'BUY', 1.0, 79.15, 'NEW', '2026-04-07 19:00:19'
            );
            """
        )
        connection.execute(
            """
            INSERT INTO fills (
                order_id, symbol, side, qty, price, commission, commission_asset, quote_qty, transact_time, created_at
            ) VALUES (
                1, 'SOLUSDT', 'BUY', 1.0, 79.15, NULL, NULL, 79.15, 1775563219000, '2026-04-07 19:00:19'
            );
            """
        )
        connection.commit()

        monkeypatch.setattr(
            BinanceBrokerClient,
            "get_user_trades",
            lambda self, symbol, start_time=None, end_time=None, limit=1000: [
                {
                    "symbol": symbol,
                    "orderId": 123,
                    "price": "78.9100",
                    "qty": "1.0",
                    "quoteQty": "78.91",
                    "commission": "0.031564",
                    "commissionAsset": "USDT",
                    "time": 1775568906000,
                }
            ],
        )

        report = get_execution_report(connection, symbol="SOLUSDT", strategy_name="ppo", days=30, limit=10)

        assert report["summary"]["fills"] == 1
        assert report["summary"]["fees"] == pytest.approx(0.031564)
        assert report["recent_fills"][0]["commission"] == pytest.approx(0.031564)
        assert report["recent_fills"][0]["price"] == pytest.approx(78.91)
        assert report["recent_fills"][0]["transact_time"] == 1775568906000
    finally:
        connection.close()


def test_get_execution_report_sums_partial_binance_user_trade_commission(monkeypatch) -> None:
    from app.execution.binance_broker import BinanceBrokerClient

    connection = make_connection()
    try:
        run_migrations(connection)
        connection.execute(
            """
            INSERT INTO orders (
                client_order_id, risk_event_id, broker_name, broker_order_id, symbol, timeframe, strategy_name, side, qty, price, status, created_at
            ) VALUES (
                'ppo-sell-1', 2, 'binance', '456', 'SOLUSDT', '1m', 'ppo', 'SELL', 1.0, 79.07, 'FILLED', '2026-04-07 19:05:47'
            );
            """
        )
        connection.commit()

        monkeypatch.setattr(
            BinanceBrokerClient,
            "get_user_trades",
            lambda self, symbol, start_time=None, end_time=None, limit=1000: [
                {
                    "symbol": symbol,
                    "orderId": 456,
                    "price": "79.0700",
                    "qty": "0.4",
                    "quoteQty": "31.628",
                    "commission": "0.0126512",
                    "commissionAsset": "USDT",
                    "time": 1775569547000,
                },
                {
                    "symbol": symbol,
                    "orderId": 456,
                    "price": "79.0700",
                    "qty": "0.6",
                    "quoteQty": "47.442",
                    "commission": "0.0189768",
                    "commissionAsset": "USDT",
                    "time": 1775569547100,
                },
            ],
        )

        report = get_execution_report(connection, symbol="SOLUSDT", strategy_name="ppo", days=30, limit=10)

        assert report["summary"]["fills"] == 2
        assert report["summary"]["fees"] == pytest.approx(0.031628)
        assert report["daily"][0]["fees"] == pytest.approx(0.031628)
    finally:
        connection.close()
