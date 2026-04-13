from typing import Union

from app.audit.service import insert_event
from app.core.db import DBConnection
from app.core.db import insert_and_get_rowid
INSERT_SIGNAL_SQL = """
INSERT INTO signals (
    symbol,
    timeframe,
    strategy_name,
    signal_type,
    short_ma,
    long_ma
) VALUES (?, ?, ?, ?, ?, ?);
"""

def insert_signal(
    connection: DBConnection,
    signal_type: str,
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
    strategy_name: str = "manual_test",
    short_ma: float = 0.0,
    long_ma: float = 0.0,
) -> dict[str, Union[int, float, str]]:
    signal_id = insert_and_get_rowid(
        connection,
        INSERT_SIGNAL_SQL,
        (symbol, timeframe, strategy_name, signal_type, short_ma, long_ma),
    )
    connection.commit()
    if signal_type in ("BUY", "SELL"):
        insert_event(
            connection,
            event_type="signal",
            status=signal_type.lower(),
            source="strategy",
            message=f"{strategy_name} generated {signal_type} signal for {symbol}.",
            payload={
                "signal_id": signal_id,
                "symbol": symbol,
                "timeframe": timeframe,
                "strategy_name": strategy_name,
                "signal_type": signal_type,
                "short_ma": round(short_ma, 6),
                "long_ma": round(long_ma, 6),
            },
        )
    return {
        "id": signal_id,
        "symbol": symbol,
        "timeframe": timeframe,
        "strategy_name": strategy_name,
        "signal_type": signal_type,
        "short_ma": short_ma,
        "long_ma": long_ma,
    }
