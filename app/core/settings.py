import os
from pathlib import Path


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    return float(value)


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    return int(value)


DEFAULT_ORDER_QTY = _get_float("CRYPTO_ORDER_QTY", 0.001)
MAX_POSITION_QTY = _get_float("CRYPTO_MAX_POSITION_QTY", 0.002)
COOLDOWN_SECONDS = _get_int("CRYPTO_COOLDOWN_SECONDS", 300)
CANDLE_STALENESS_SECONDS = _get_int("CRYPTO_CANDLE_STALENESS_SECONDS", 600)
SOAK_ACTIVITY_STALENESS_SECONDS = _get_int("CRYPTO_SOAK_ACTIVITY_STALENESS_SECONDS", 900)
WORKER_HEARTBEAT_STALENESS_SECONDS = _get_int("CRYPTO_WORKER_HEARTBEAT_STALENESS_SECONDS", 180)
MAX_DAILY_LOSS = _get_float("CRYPTO_MAX_DAILY_LOSS", 50.0)
DB_BACKEND = os.getenv("CRYPTO_DB_BACKEND", "sqlite").strip().lower()
SQLITE_PATH = Path(os.getenv("CRYPTO_SQLITE_PATH", "storage/market_data.db"))
DATABASE_URL = os.getenv("CRYPTO_DATABASE_URL", "").strip()
POSTGRES_CONNECT_RETRIES = _get_int("CRYPTO_POSTGRES_CONNECT_RETRIES", 15)
POSTGRES_CONNECT_RETRY_DELAY_SECONDS = _get_float("CRYPTO_POSTGRES_CONNECT_RETRY_DELAY_SECONDS", 1.0)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_TIMEOUT_SECONDS = _get_int("TELEGRAM_TIMEOUT_SECONDS", 5)
DEFAULT_STRATEGY_NAME = os.getenv("CRYPTO_STRATEGY_NAME", "ma_cross").strip() or "ma_cross"
EXECUTION_BACKEND = os.getenv("CRYPTO_EXECUTION_BACKEND", "paper").strip().lower() or "paper"
