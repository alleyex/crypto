import os
import time

import requests


BASE_URL = "https://api.binance.com/api/v3/klines"
DEFAULT_FAKE_CLOSES = (10.0, 11.0, 12.0, 13.0, 14.0)


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


def fetch_klines(symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 5) -> list[list]:
    if os.getenv("CRYPTO_USE_FAKE_KLINES", "").strip().lower() in ("1", "true", "yes", "on"):
        return _build_fake_klines(symbol=symbol, interval=interval, limit=limit)

    response = requests.get(
        BASE_URL,
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()
