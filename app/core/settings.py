import os


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
