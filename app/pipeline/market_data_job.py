from typing import Any, Dict, Optional

from app.core.db import DBConnection
from app.data.binance_client import fetch_klines
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import save_klines


def run_market_data_job(connection: DBConnection, symbol_names: Optional[list[str]] = None) -> Dict[str, Any]:
    ensure_candles_table(connection)
    if symbol_names is None:
        from app.scheduler.control import read_active_symbols

        symbol_names = read_active_symbols()
    active_symbol_names = list(dict.fromkeys(symbol_names))
    symbol_results = []
    total_saved_klines = 0
    for symbol_name in active_symbol_names:
        klines = fetch_klines(symbol=symbol_name)
        saved_klines = save_klines(connection, klines, symbol=symbol_name)
        total_saved_klines += saved_klines
        symbol_results.append({"symbol": symbol_name, "saved_klines": saved_klines})
    return {
        "step": "save_klines",
        "saved_klines": total_saved_klines,
        "symbol_names": active_symbol_names,
        "symbol_results": symbol_results,
    }
