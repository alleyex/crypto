from typing import Callable, Dict, Optional, Union

from app.core.db import DBConnection
from app.strategy.bbands import generate_signal as generate_bbands_signal
from app.strategy.ma_cross import generate_signal as generate_ma_cross_signal
from app.strategy.macd import generate_signal as generate_macd_signal
from app.strategy.momentum_3bar import generate_signal as generate_momentum_3bar_signal
from app.strategy.rsi import generate_signal as generate_rsi_signal


StrategyResult = Optional[Dict[str, Union[float, str]]]
StrategyGenerator = Callable[[DBConnection, str], StrategyResult]


def _run_ma_cross(connection: DBConnection, symbol: str) -> StrategyResult:
    return generate_ma_cross_signal(connection, symbol=symbol)


def _run_momentum_3bar(connection: DBConnection, symbol: str) -> StrategyResult:
    return generate_momentum_3bar_signal(connection, symbol=symbol)


def _run_rsi(connection: DBConnection, symbol: str) -> StrategyResult:
    return generate_rsi_signal(connection, symbol=symbol)


def _run_bbands(connection: DBConnection, symbol: str) -> StrategyResult:
    return generate_bbands_signal(connection, symbol=symbol)


def _run_macd(connection: DBConnection, symbol: str) -> StrategyResult:
    return generate_macd_signal(connection, symbol=symbol)


STRATEGY_REGISTRY: dict[str, StrategyGenerator] = {
    "bbands": _run_bbands,
    "ma_cross": _run_ma_cross,
    "macd": _run_macd,
    "momentum_3bar": _run_momentum_3bar,
    "rsi": _run_rsi,
}


def list_registered_strategies() -> list[str]:
    return sorted(STRATEGY_REGISTRY)


def get_strategy(name: str) -> StrategyGenerator:
    if name not in STRATEGY_REGISTRY:
        raise ValueError(f"Unknown strategy: {name}")
    return STRATEGY_REGISTRY[name]


def generate_registered_signal(
    connection: DBConnection,
    strategy_name: str = "ma_cross",
    symbol: str = "BTCUSDT",
) -> StrategyResult:
    strategy = get_strategy(strategy_name)
    return strategy(connection, symbol)
