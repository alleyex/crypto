from typing import Any, Dict

from app.core.db import DBConnection
from app.data.binance_client import fetch_klines
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import save_klines


def run_market_data_job(connection: DBConnection) -> Dict[str, Any]:
    ensure_candles_table(connection)
    klines = fetch_klines()
    saved_klines = save_klines(connection, klines)
    return {"step": "save_klines", "saved_klines": saved_klines}
