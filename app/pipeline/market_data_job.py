import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Optional

from app.data.binance_client import fetch_klines
from app.data.candles_service import TIMEFRAME_INTERVAL_MS
from app.data.candles_service import ensure_table as ensure_candles_table
from app.data.candles_service import get_latest_open_time
from app.data.candles_service import save_klines
from app.data.fetch_history import record_fetch

PAGE_SIZE = 1000
SEED_LIMIT = 100  # candles to fetch when no prior data exists
MAX_WORKERS = int(os.getenv("CRYPTO_MARKET_DATA_WORKERS", "4"))


def _fetch_all_since(symbol: str, start_ms: int, interval: str = "1m") -> list:
    """Paginate through Binance klines from start_ms until no more data. Returns raw klines."""
    all_klines: list = []
    cursor_ms = start_ms
    while True:
        klines = fetch_klines(symbol=symbol, interval=interval, limit=PAGE_SIZE, start_ms=cursor_ms)
        if not klines:
            break
        all_klines.extend(klines)
        if len(klines) < PAGE_SIZE:
            break
        cursor_ms = int(klines[-1][0]) + 1
    return all_klines


def _fetch_one(symbol: str, timeframe: str, start_ms: Optional[int], limit: int) -> Dict[str, Any]:
    """Fetch klines for a single symbol+timeframe. Returns dict with klines + metadata."""
    if start_ms is not None:
        klines = _fetch_all_since(symbol=symbol, start_ms=start_ms, interval=timeframe)
        mode = "backfill"
    else:
        klines = fetch_klines(symbol=symbol, interval=timeframe, limit=limit)
        mode = "seed"
    return {"symbol": symbol, "timeframe": timeframe, "klines": klines, "mode": mode}


def run_market_data_job(
    connection,
    symbol_names: Optional[list[str]] = None,
    timeframes: Optional[list[str]] = None,
    limit: int = SEED_LIMIT,
    start_ms: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_candles_table(connection)

    if symbol_names is None:
        from app.scheduler.control import read_active_symbols
        symbol_names = read_active_symbols()
    if timeframes is None:
        from app.scheduler.control import read_active_timeframes
        timeframes = read_active_timeframes()

    active_symbols = list(dict.fromkeys(symbol_names))
    active_timeframes = list(dict.fromkeys(timeframes))

    # Build tasks: determine start_ms per (symbol, timeframe)
    tasks = []
    for symbol in active_symbols:
        for timeframe in active_timeframes:
            if start_ms is not None:
                task_start_ms = start_ms
                mode_hint = "backfill"
            else:
                latest_ms = get_latest_open_time(connection, symbol=symbol, timeframe=timeframe)
                if latest_ms is not None:
                    interval_ms = TIMEFRAME_INTERVAL_MS.get(timeframe, 60_000)
                    task_start_ms = latest_ms + interval_ms
                    mode_hint = "incremental"
                else:
                    task_start_ms = None
                    mode_hint = "seed"
            tasks.append((symbol, timeframe, task_start_ms, mode_hint))

    # Fetch from Binance in parallel (I/O bound)
    fetch_results: Dict[str, Any] = {}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(tasks))) as pool:
        futures = {
            pool.submit(_fetch_one, sym, tf, t_start, limit): (sym, tf, hint)
            for sym, tf, t_start, hint in tasks
        }
        for future in as_completed(futures):
            sym, tf, hint = futures[future]
            try:
                result = future.result()
                result["mode"] = hint  # use pre-determined mode (incremental vs seed vs backfill)
                fetch_results[f"{sym}|{tf}"] = result
            except Exception as exc:
                fetch_results[f"{sym}|{tf}"] = {
                    "symbol": sym, "timeframe": tf, "klines": [], "mode": hint, "error": str(exc)
                }

    # Write to DB sequentially (SQLite single-writer)
    symbol_results = []
    total_saved_klines = 0

    for symbol, timeframe, _, _ in tasks:
        key = f"{symbol}|{timeframe}"
        r = fetch_results.get(key, {})
        klines = r.get("klines", [])
        mode = r.get("mode", "unknown")
        error = r.get("error")

        if error:
            saved = 0
        elif klines:
            saved = save_klines(connection, klines, symbol=symbol, timeframe=timeframe)
        else:
            saved = 0

        total_saved_klines += saved
        entry: Dict[str, Any] = {
            "symbol": symbol,
            "timeframe": timeframe,
            "saved_klines": saved,
            "mode": mode,
        }
        if error:
            entry["error"] = error
        symbol_results.append(entry)

    job_result = {
        "step": "save_klines",
        "saved_klines": total_saved_klines,
        "symbol_names": active_symbols,
        "timeframes": active_timeframes,
        "symbol_results": symbol_results,
    }
    record_fetch(job_result)
    return job_result
