from __future__ import annotations

from typing import Iterable

import pandas as pd

from config.settings import ALPHA, MAX_GROSS, MAX_WEIGHT, TOP_N
from strategies.momentum_portfolio import allocation_layer, selection_layer
from strategies.momentum_portfolio import compute_target_weights as momentum_compute
from strategies.ta_strategies import (
    compute_target_weights_bollinger,
    compute_target_weights_intraday_breakout,
    compute_target_weights_macd,
    compute_target_weights_multi_factor,
    compute_target_weights_rsi,
)


BASE_STRATEGIES_FOR_VOTING: list = [
    momentum_compute,
    compute_target_weights_multi_factor,
    compute_target_weights_macd,
    compute_target_weights_bollinger,
    compute_target_weights_intraday_breakout,
    compute_target_weights_rsi,
]


def compute_target_weights_voting(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    base_strategies: Iterable = BASE_STRATEGIES_FOR_VOTING,
    **kwargs: object,
) -> pd.Series:
    """
    多策略投票策略：
    - 对每个基础策略计算一份权重向量；
    - 若某只股票在该策略中权重 > 0，则记为 1 票；
    - 最终得分 = 票数（即被多少个策略同时看好）；
    - 使用统一的 allocation_layer 按“票数”进行资金分配。
    """
    votes = pd.Series(0, index=prices.columns, dtype=float)

    for fn in base_strategies:
        try:
            w = fn(prices, top_n=top_n, alpha=alpha, max_weight=max_weight, max_gross=max_gross, **kwargs)
        except Exception:
            continue
        if w is None or w.empty:
            continue
        w = w.reindex(prices.columns).fillna(0.0)
        mask = w > 0
        votes[mask] += 1.0

    if (votes <= 0).all():
        return pd.Series(dtype=float)

    signals = (
        votes.to_frame("score")
        .reset_index()
        .rename(columns={"index": "ts_code"})
        .assign(trend_ok=lambda x: x["score"] > 0, tech_score=lambda x: x["score"])
    )

    selected = selection_layer(signals, top_n=top_n, min_score=0.0, min_tech_score=0.0)
    return allocation_layer(selected, alpha=alpha, max_weight=max_weight, max_gross=max_gross)


