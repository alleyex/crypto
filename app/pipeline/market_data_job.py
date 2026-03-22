from typing import Any, Dict, Optional

from app.core.db import DBConnection
from app.data.binance_client import fetch_klines
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import save_klines

PAGE_SIZE = 1000


def _fetch_all_since(
    connection: DBConnection,
    symbol: str,
    start_ms: int,
    interval: str = "1m",
) -> int:
    """Paginate through Binance klines from start_ms until no more data."""
    total_saved = 0
    cursor_ms = start_ms
    while True:
        klines = fetch_klines(symbol=symbol, interval=interval, limit=PAGE_SIZE, start_ms=cursor_ms)
        if not klines:
            break
        save_klines(connection, klines, symbol=symbol)
        total_saved += len(klines)
        if len(klines) < PAGE_SIZE:
            break
        cursor_ms = int(klines[-1][0]) + 1
    return total_saved


def run_market_data_job(
    connection: DBConnection,
    symbol_names: Optional[list[str]] = None,
    limit: int = 100,
    start_ms: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_candles_table(connection)
    if symbol_names is None:
        from app.scheduler.control import read_active_symbols
        symbol_names = read_active_symbols()
    active_symbol_names = list(dict.fromkeys(symbol_names))
    symbol_results = []
    total_saved_klines = 0
    for symbol_name in active_symbol_names:
        if start_ms is not None:
            saved = _fetch_all_since(connection, symbol=symbol_name, start_ms=start_ms)
        else:
            klines = fetch_klines(symbol=symbol_name, limit=limit)
            saved = save_klines(connection, klines, symbol=symbol_name)
        total_saved_klines += saved
        symbol_results.append({"symbol": symbol_name, "saved_klines": saved})
    return {
        "step": "save_klines",
        "saved_klines": total_saved_klines,
        "symbol_names": active_symbol_names,
        "symbol_results": symbol_results,
    }
