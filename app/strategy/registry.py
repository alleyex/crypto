from typing import Callable, Union

from app.core.db import DBConnection

StrategyResult = dict[str, Union[float, str]] | None
StrategyGenerator = Callable[[DBConnection, str, str], StrategyResult]

def _run_ppo(connection: DBConnection, symbol: str, timeframe: str) -> StrategyResult:
    # Lazy import: avoids loading numpy/pandas/torch at scheduler startup when
    # running a non-PPO strategy.  PyTorch alone can spike virtual memory by
    # several GB during JIT initialisation and has previously triggered OOM kills.
    from app.strategy.ppo_strategy import generate_signal as generate_ppo_signal  # noqa: PLC0415
    return generate_ppo_signal(connection, symbol=symbol, timeframe=timeframe)

STRATEGY_REGISTRY: dict[str, StrategyGenerator] = {
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
    strategy_name: str = "ppo",
    symbol: str = "BTCUSDT",
    timeframe: str = "1m",
) -> StrategyResult:
    strategy = get_strategy(strategy_name)
    return strategy(connection, symbol, timeframe)
