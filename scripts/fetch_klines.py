import json
from pathlib import Path

import requests


URL = "https://api.binance.com/api/v3/klines"
PARAMS = {
    "symbol": "BTCUSDT",
    "interval": "1m",
    "limit": 5,
}
OUTPUT_DIR = Path("data")
OUTPUT_FILE = OUTPUT_DIR / "btcusdt_1m_klines.json"


def main() -> None:
    response = requests.get(URL, params=PARAMS, timeout=10)
    response.raise_for_status()

    klines = response.json()
    formatted_klines = []

    for item in klines:
        candle = {
            "symbol": "BTCUSDT",
            "timeframe": "1m",
            "open_time": item[0],
            "open": item[1],
            "high": item[2],
            "low": item[3],
            "close": item[4],
            "volume": item[5],
            "close_time": item[6],
            "quote_asset_volume": item[7],
            "number_of_trades": item[8],
            "taker_buy_base_volume": item[9],
            "taker_buy_quote_volume": item[10],
        }
        formatted_klines.append(candle)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_FILE.write_text(json.dumps(formatted_klines, indent=2), encoding="utf-8")

    print(json.dumps(formatted_klines, indent=2))
    print(f"\nSaved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
