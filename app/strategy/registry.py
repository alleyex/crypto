from typing import Callable, Dict, Optional, Union

from app.core.db import DBConnection
from app.strategy.ma_cross import generate_signal as generate_ma_cross_signal
from app.strategy.ppo_strategy import generate_signal as generate_ppo_signal


StrategyResult = Optional[Dict[str, Union[float, str]]]
StrategyGenerator = Callable[[DBConnection, str, str], StrategyResult]


def _run_ma_cross(connection: DBConnection, symbol: str, timeframe: str) -> StrategyResult:
    return generate_ma_cross_signal(connection, symbol=symbol, timeframe=timeframe)


def _run_ppo(connection: DBConnection, symbol: str, timeframe: str) -> StrategyResult:
    return generate_ppo_signal(connection, symbol=symbol, timeframe=timeframe)


STRATEGY_REGISTRY: dict[str, StrategyGenerator] = {
    "ma_cross": _run_ma_cross,
    "ppo": _run_ppo,
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
    timeframe: str = "1m",
) -> StrategyResult:
    strategy = get_strategy(strategy_name)
    return strategy(connection, symbol, timeframe)
