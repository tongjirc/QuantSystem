from __future__ import annotations

from typing import Callable, Dict

import pandas as pd

from strategies.ensemble_strategies import compute_target_weights_voting
from strategies.momentum_portfolio import compute_target_weights as momentum_compute
from strategies.ta_strategies import (
    compute_target_weights_bollinger,
    compute_target_weights_intraday_breakout,
    compute_target_weights_macd,
    compute_target_weights_multi_factor,
    compute_target_weights_multi_factor_board,
    compute_target_weights_rsi,
)

# 统一的策略接口类型：给定价格面板，返回目标权重（截面）
StrategyFn = Callable[..., pd.Series]


STRATEGIES: Dict[str, StrategyFn] = {
    "momentum": momentum_compute,
    "macd": compute_target_weights_macd,
    "bollinger": compute_target_weights_bollinger,
    "intraday_breakout": compute_target_weights_intraday_breakout,
    "rsi": compute_target_weights_rsi,
    "multi_factor": compute_target_weights_multi_factor,
    "multi_factor_board": compute_target_weights_multi_factor_board,
    "voting": compute_target_weights_voting,
}


def get_strategy_fn(name: str) -> StrategyFn:
    if name not in STRATEGIES:
        raise ValueError(f"Unknown strategy '{name}'. Supported: {sorted(STRATEGIES.keys())}")
    return STRATEGIES[name]


def supported_strategies() -> list[str]:
    return sorted(STRATEGIES.keys())


