import requests


BASE_URL = "https://api.binance.com/api/v3/klines"


def fetch_klines(symbol: str = "BTCUSDT", interval: str = "1m", limit: int = 5) -> list[list]:
    response = requests.get(
        BASE_URL,
        params={"symbol": symbol, "interval": interval, "limit": limit},
        timeout=10,
    )
    response.raise_for_status()
    return response.json()
