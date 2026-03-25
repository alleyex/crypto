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


def _get_choice(name: str, default: str, allowed: tuple[str, ...]) -> str:
    value = os.getenv(name)
    if value is None:
        return default
    normalized = value.strip().lower()
    if normalized in allowed:
        return normalized
    return default


DEFAULT_ORDER_QTY = _get_float("CRYPTO_ORDER_QTY", 0.001)
COMMISSION_RATE = _get_float("CRYPTO_COMMISSION_RATE", 0.001)  # 0.1% per side
MAX_POSITION_QTY = _get_float("CRYPTO_MAX_POSITION_QTY", 0.001)
COOLDOWN_SECONDS = _get_int("CRYPTO_COOLDOWN_SECONDS", 300)
CANDLE_STALENESS_SECONDS = _get_int("CRYPTO_CANDLE_STALENESS_SECONDS", 600)
CANDLE_STALENESS_MULTIPLIER = _get_int("CRYPTO_CANDLE_STALENESS_MULTIPLIER", 3)
SOAK_ACTIVITY_STALENESS_SECONDS = _get_int("CRYPTO_SOAK_ACTIVITY_STALENESS_SECONDS", 900)
WORKER_HEARTBEAT_STALENESS_SECONDS = _get_int("CRYPTO_WORKER_HEARTBEAT_STALENESS_SECONDS", 180)
QUEUE_BATCH_STALENESS_SECONDS = _get_int("CRYPTO_QUEUE_BATCH_STALENESS_SECONDS", 300)
ORDER_STALENESS_SECONDS = _get_int("CRYPTO_ORDER_STALENESS_SECONDS", 300)
RISK_REJECTION_STREAK_THRESHOLD = _get_int("CRYPTO_RISK_REJECTION_STREAK_THRESHOLD", 3)
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
BINANCE_API_KEY = os.getenv("CRYPTO_BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET = os.getenv("CRYPTO_BINANCE_API_SECRET", "").strip()
BINANCE_TESTNET = os.getenv("CRYPTO_BINANCE_TESTNET", "true").strip().lower() != "false"
DEFAULT_PIPELINE_ORCHESTRATION = _get_choice(
    "CRYPTO_PIPELINE_ORCHESTRATION",
    "queue_batch",
    ("direct", "queue_dispatch", "queue_drain", "queue_batch"),
)
JOB_LEASE_TIMEOUT_SECONDS = _get_int("CRYPTO_JOB_LEASE_TIMEOUT_SECONDS", 300)
# How long an alert state is valid before the same condition re-fires an alert.
# Default: 86400 seconds (24 hours). Set to 0 to disable TTL (never re-fire).
ALERT_REFIRE_SECONDS = _get_int("CRYPTO_ALERT_REFIRE_SECONDS", 86400)
