"""
组合回测：每日截面权重 × 日收益，考虑换手成本。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

from config.settings import (
    ALPHA,
    FEE_BPS,
    MAX_GROSS,
    MAX_WEIGHT,
    MIN_TECH_SCORE,
    MOMENTUM_LOOKBACK,
    MOMENTUM_SKIP_LAST,
    REBALANCE_DAYS,
    TOP_N,
    TREND_FAST_MA,
    TREND_SLOW_MA,
)
from strategies.strategy_registry import get_strategy_fn


@dataclass
class PortfolioBacktestResult:
    equity: pd.Series
    daily_return: pd.Series
    turnover: pd.Series
    weights: pd.DataFrame  # index=date, columns=ts_code
    benchmark_equity: Optional[pd.Series] = None  # 等权基准（可选）


def run_portfolio_backtest(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    min_tech_score: float = MIN_TECH_SCORE,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
    fast: int = TREND_FAST_MA,
    slow: int = TREND_SLOW_MA,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    fee_bps: float = FEE_BPS,
    rebalance_days: int = REBALANCE_DAYS,
    strategy: str = "momentum",
    initial_capital: float = 1.0,
) -> PortfolioBacktestResult:
    """
    组合回测：按日推进，每日用「截至当日」的价格截面算目标权重，次日用该权重 × 次日收益。
    - prices: DataFrame, index=date, columns=ts_code, value=close
    - 返回净值曲线、日收益、换手、权重历史。
    """
    prices = prices.sort_index()
    # 更稳健的收益计算：不全局填 0，逐日按可用价格变化计算
    returns = prices.pct_change(fill_method=None)
    trading_days = prices.index.tolist()
    if len(trading_days) < lookback + skip_last + 2:
        empty_equity = pd.Series(dtype=float)
        empty_ret = pd.Series(dtype=float)
        empty_turn = pd.Series(dtype=float)
        empty_w = pd.DataFrame()
        return PortfolioBacktestResult(equity=empty_equity, daily_return=empty_ret, turnover=empty_turn, weights=empty_w)

    # 需要至少 lookback+skip_last 个历史日才能算第一天权重
    start_idx = lookback + skip_last
    if start_idx >= len(trading_days):
        empty_equity = pd.Series(dtype=float)
        empty_ret = pd.Series(dtype=float)
        empty_turn = pd.Series(dtype=float)
        empty_w = pd.DataFrame()
        return PortfolioBacktestResult(equity=empty_equity, daily_return=empty_ret, turnover=empty_turn, weights=empty_w)

    weight_list: list[pd.Series] = []
    dates_used: list[pd.Timestamp] = []

    strategy_fn = get_strategy_fn(strategy)

    for i in range(start_idx, len(trading_days)):
        t = trading_days[i]
        prices_through_t = prices.loc[:t]
        w = strategy_fn(
            prices_through_t,
            top_n=top_n,
            min_score=min_score,
            min_tech_score=min_tech_score,
            lookback=lookback,
            skip_last=skip_last,
            fast=fast,
            slow=slow,
            alpha=alpha,
            max_weight=max_weight,
            max_gross=max_gross,
        )
        w = w.reindex(prices.columns).fillna(0.0)
        weight_list.append(w)
        dates_used.append(t)

    weights_df = pd.concat(weight_list, axis=1).T
    weights_df.index = dates_used
    weights_df.index.name = "date"

    # 收益：次日用前一日权重 × 次日收益
    r_port_list = []
    turnover_list = []
    realized_weights: list[pd.Series] = []
    w_prev = pd.Series(0.0, index=prices.columns)
    for i in range(len(dates_used)):
        t = dates_used[i]
        w_target = weights_df.loc[t]

        # 当日收益：用上一日权重（w_prev）乘当日收益（忽略当日无价格变化的标的）
        ret_t = returns.loc[t]
        mask = ret_t.notna()
        if mask.any():
            r_t = float((w_prev[mask] * ret_t[mask]).sum())
        else:
            r_t = 0.0

        # 调仓逻辑：每 rebalance_days 个交易日才把组合权重对齐到最新目标
        if i == 0 or (rebalance_days > 0 and i % rebalance_days == 0):
            w_curr = w_target
        else:
            w_curr = w_prev

        turnover_t = (w_curr - w_prev).abs().sum()
        turnover_list.append(turnover_t)
        cost_t = (fee_bps / 10000.0) * turnover_t
        r_port_list.append(r_t - cost_t)

        realized_weights.append(w_curr)
        w_prev = w_curr.copy()

    r_port = pd.Series(r_port_list, index=dates_used)
    turnover = pd.Series(turnover_list, index=dates_used)
    equity = (1.0 + r_port).cumprod() * initial_capital
    equity.name = "equity"

    # 等权基准（全市场等权）：仅在有价格变化的标的上取简单平均
    ret_slice = returns.loc[dates_used]
    ret_ew = ret_slice.mean(axis=1, skipna=True)
    benchmark_equity = (1.0 + ret_ew).cumprod() * initial_capital
    benchmark_equity.name = "benchmark_ew"

    realized_weights_df = pd.concat(realized_weights, axis=1).T
    realized_weights_df.index = dates_used
    realized_weights_df.index.name = "date"

    return PortfolioBacktestResult(
        equity=equity,
        daily_return=r_port,
        turnover=turnover,
        weights=realized_weights_df,
        benchmark_equity=benchmark_equity,
    )
