import os
import time
from typing import Optional

import requests


BASE_URL = "https://api.binance.com/api/v3/klines"
BOOK_TICKER_URL = "https://api.binance.com/api/v3/ticker/bookTicker"
TESTNET_BOOK_TICKER_URL = "https://testnet.binance.vision/api/v3/ticker/bookTicker"
DEFAULT_FAKE_CLOSES = (10.0, 11.0, 12.0, 13.0, 14.0)

_DEFAULT_TIMEOUT = 10
_DEFAULT_RETRIES = 3
_DEFAULT_BACKOFF = 1.0


def _build_fake_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    limit: int = 5,
) -> list[list]:
    del symbol
    del interval

    closes_text = os.getenv("CRYPTO_FAKE_KLINE_CLOSES", "").strip()
    closes = [
        float(item.strip())
        for item in closes_text.split(",")
        if item.strip()
    ] if closes_text else list(DEFAULT_FAKE_CLOSES)
    if not closes:
        closes = list(DEFAULT_FAKE_CLOSES)

    closes = closes[-limit:]
    now_ms = int(time.time() // 60 * 60_000)
    start_open_ms = now_ms - (len(closes) * 60_000)
    klines: list[list] = []
    for index, close in enumerate(closes):
        open_time = start_open_ms + (index * 60_000)
        close_text = str(close)
        klines.append(
            [
                open_time,
                str(close - 1),
                str(close + 1),
                str(close - 2),
                close_text,
                "100",
                open_time + 59_999,
                "1000",
                10,
                "50",
                "500",
            ]
        )
    return klines


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, requests.exceptions.ConnectionError):
        return True
    if isinstance(exc, requests.exceptions.Timeout):
        return True
    if isinstance(exc, requests.exceptions.HTTPError):
        status = exc.response.status_code if exc.response is not None else 0
        return status == 429 or status >= 500
    return False


def _use_testnet() -> bool:
    return os.getenv("CRYPTO_BINANCE_TESTNET", "").strip().lower() in ("1", "true", "yes", "on")


def _book_ticker_url() -> str:
    return TESTNET_BOOK_TICKER_URL if _use_testnet() else BOOK_TICKER_URL


def fetch_klines(
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    limit: int = 5,
    start_ms: Optional[int] = None,
) -> list[list]:
    if os.getenv("CRYPTO_USE_FAKE_KLINES", "").strip().lower() in ("1", "true", "yes", "on"):
        return _build_fake_klines(symbol=symbol, interval=interval, limit=limit)

    params: dict = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = start_ms

    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT)))
    max_retries = int(os.getenv("CRYPTO_BINANCE_RETRY_COUNT", str(_DEFAULT_RETRIES)))
    backoff = float(os.getenv("CRYPTO_BINANCE_RETRY_BACKOFF_SECONDS", str(_DEFAULT_BACKOFF)))

    last_exc: Exception = RuntimeError("fetch_klines: no attempts made")
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(BASE_URL, params=params, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries and _is_retryable(exc):
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue
            raise

    raise last_exc


def fetch_book_ticker(symbol: str = "BTCUSDT") -> dict:
    timeout = int(os.getenv("CRYPTO_BINANCE_TIMEOUT_SECONDS", str(_DEFAULT_TIMEOUT)))
    max_retries = int(os.getenv("CRYPTO_BINANCE_RETRY_COUNT", str(_DEFAULT_RETRIES)))
    backoff = float(os.getenv("CRYPTO_BINANCE_RETRY_BACKOFF_SECONDS", str(_DEFAULT_BACKOFF)))

    last_exc: Exception = RuntimeError("fetch_book_ticker: no attempts made")
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(_book_ticker_url(), params={"symbol": symbol}, timeout=timeout)
            response.raise_for_status()
            payload = response.json()
            return {
                "symbol": payload.get("symbol", symbol),
                "bid_price": float(payload["bidPrice"]),
                "bid_qty": float(payload["bidQty"]),
                "ask_price": float(payload["askPrice"]),
                "ask_qty": float(payload["askQty"]),
            }
        except Exception as exc:
            last_exc = exc
            if attempt < max_retries and _is_retryable(exc):
                wait = backoff * (2 ** attempt)
                time.sleep(wait)
                continue
            raise

    raise last_exc
