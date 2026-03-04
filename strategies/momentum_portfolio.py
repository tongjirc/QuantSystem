"""
三层架构：信号层 → 选股层 → 资金分配层。
- 信号层：多票动量得分 + 趋势过滤（MA）
- 选股层：Top N（仅保留趋势向上且得分为正）
- 资金分配层：按动量强度加权（非线性），单票与总仓位上限
"""
from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from config.settings import (
    ALPHA,
    MAX_GROSS,
    MAX_WEIGHT,
    MIN_PRICE_FILTER,
    MIN_TECH_SCORE,
    MOMENTUM_LOOKBACK,
    MOMENTUM_SKIP_LAST,
    RSI_LOWER,
    RSI_PERIOD,
    RSI_UPPER,
    TECH_WEIGHT_ABOVE_MA5,
    TECH_WEIGHT_ABOVE_MA60,
    TECH_WEIGHT_MACD_BULLISH,
    TECH_WEIGHT_MA5_GT_MA10_GT_MA20,
    TECH_WEIGHT_PRICE_GT_MIN,
    TECH_WEIGHT_RSI_STRONG,
    TECH_WEIGHT_TODAY_RET_GT_MIN,
    TODAY_RET_MIN,
    TOP_N,
    TREND_FAST_MA,
    TREND_SLOW_MA,
)


# ---------- 信号层：单票动量 + 趋势 ----------
def momentum_score(
    close: pd.Series,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
) -> float:
    """
    动量得分：过去 (lookback - skip_last) 日收益，跳过最近 skip_last 日（减轻反转）。
    lookback=126≈6月, skip_last=21≈1月 → 相当于 5 个月动量。
    """
    if close is None or len(close) < lookback + skip_last:
        return np.nan
    close = close.dropna()
    if len(close) < lookback + skip_last:
        return np.nan
    end_idx = -(skip_last + 1)
    start_idx = -(lookback + skip_last)
    p_end = close.iloc[end_idx]
    p_start = close.iloc[start_idx]
    if p_start <= 0:
        return np.nan
    return float(p_end / p_start - 1.0)


def trend_ok(close: pd.Series, fast: int = TREND_FAST_MA, slow: int = TREND_SLOW_MA) -> bool:
    """趋势过滤：当前快线 > 慢线 才允许做多。"""
    if close is None or len(close) < slow:
        return False
    close = close.dropna()
    if len(close) < slow:
        return False
    ma_f = close.rolling(fast).mean().iloc[-1]
    ma_s = close.rolling(slow).mean().iloc[-1]
    return ma_f > ma_s


def _rsi(close: pd.Series, period: int = RSI_PERIOD) -> float:
    """简单 RSI 计算，用于技术打分。"""
    if close is None or len(close) < period + 1:
        return np.nan
    close = close.dropna()
    if len(close) < period + 1:
        return np.nan
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.rolling(period).mean()
    roll_down = down.rolling(period).mean()
    if roll_down.iloc[-1] <= 0:
        return np.nan
    rs = roll_up.iloc[-1] / roll_down.iloc[-1]
    return float(100.0 - 100.0 / (1.0 + rs))


def _macd(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float, float]:
    """
    返回 (diff, dea)，用于判定 MACD 多头状态。
    这里采用标准 EMA 实现。
    """
    if close is None or len(close) < slow + signal:
        return (np.nan, np.nan)
    close = close.dropna()
    if len(close) < slow + signal:
        return (np.nan, np.nan)
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    diff = ema_fast - ema_slow
    dea = diff.ewm(span=signal, adjust=False).mean()
    return (float(diff.iloc[-1]), float(dea.iloc[-1]))


def technical_score(close: pd.Series) -> float:
    """
    技术面打分：参考用户提供 checklist，仅使用价格序列即可计算的部分。

    条件：
    - 价格在 MA5 之上
    - MA5 > MA10 > MA20
    - MACD 看多（diff > dea）
    - RSI 在 [RSI_LOWER, RSI_UPPER] 区间
    - 价格 > MIN_PRICE_FILTER
    - 今日涨幅 > TODAY_RET_MIN
    - 价格在 MA60 之上
    """
    if close is None or len(close) < 60:
        return 0.0
    close = close.dropna()
    if len(close) < 60:
        return 0.0

    score = 0.0

    # 均线
    ma5 = close.rolling(5).mean().iloc[-1]
    ma10 = close.rolling(10).mean().iloc[-1]
    ma20 = close.rolling(20).mean().iloc[-1]
    ma60 = close.rolling(60).mean().iloc[-1]
    last_price = float(close.iloc[-1])
    prev_price = float(close.iloc[-2]) if len(close) >= 2 else np.nan

    # 价格在 MA5 之上
    if last_price > ma5:
        score += TECH_WEIGHT_ABOVE_MA5

    # MA5 > MA10 > MA20 多头排列
    if ma5 > ma10 > ma20:
        score += TECH_WEIGHT_MA5_GT_MA10_GT_MA20

    # MACD 多头
    diff, dea = _macd(close)
    if not (np.isnan(diff) or np.isnan(dea)) and diff > dea:
        score += TECH_WEIGHT_MACD_BULLISH

    # RSI 在强势区间
    rsi_val = _rsi(close)
    if not np.isnan(rsi_val) and RSI_LOWER <= rsi_val <= RSI_UPPER:
        score += TECH_WEIGHT_RSI_STRONG

    # 价格过滤（> MIN_PRICE_FILTER）
    if last_price > MIN_PRICE_FILTER:
        score += TECH_WEIGHT_PRICE_GT_MIN

    # 今日涨幅 > TODAY_RET_MIN
    if not np.isnan(prev_price) and prev_price > 0:
        today_ret = last_price / prev_price - 1.0
        if today_ret > TODAY_RET_MIN:
            score += TECH_WEIGHT_TODAY_RET_GT_MIN

    # 价格在 MA60 之上
    if last_price > ma60:
        score += TECH_WEIGHT_ABOVE_MA60

    return float(score)


def signal_layer_one(
    close: pd.Series,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
    fast: int = TREND_FAST_MA,
    slow: int = TREND_SLOW_MA,
) -> tuple[float, bool, float]:
    """单只股票：返回 (momentum_score, trend_ok, tech_score)。"""
    score = momentum_score(close, lookback=lookback, skip_last=skip_last)
    ok = trend_ok(close, fast=fast, slow=slow)
    tech = technical_score(close)
    return (score, ok, tech)


def signal_layer_cross_section(
    prices: pd.DataFrame,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
    fast: int = TREND_FAST_MA,
    slow: int = TREND_SLOW_MA,
) -> pd.DataFrame:
    """
    截面信号：prices 为 DataFrame，index=date, columns=ts_code。
    返回 DataFrame：index=ts_code, columns=['score','trend_ok','tech_score']。
    对最后一行（当前截面）计算每只股票的 score / trend_ok / tech_score。
    """
    rows = []
    for ts_code in prices.columns:
        close = prices[ts_code].dropna()
        if len(close) < lookback + skip_last:
            continue
        score, ok, tech = signal_layer_one(
            close,
            lookback=lookback,
            skip_last=skip_last,
            fast=fast,
            slow=slow,
        )
        if np.isnan(score):
            continue
        rows.append({"ts_code": ts_code, "score": score, "trend_ok": ok, "tech_score": tech})
    if not rows:
        return pd.DataFrame(columns=["ts_code", "score", "trend_ok", "tech_score"])
    return pd.DataFrame(rows)


# ---------- 选股层：Top N ----------
def selection_layer(
    signals: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    min_tech_score: float = MIN_TECH_SCORE,
) -> pd.DataFrame:
    """
    只保留：
    - 趋势过滤通过（trend_ok）
    - 动量得分 score >= min_score
    - 技术面打分 tech_score >= min_tech_score（若存在该列）

    再按 (score, tech_score) 降序排序，取 Top N。
    """
    df = signals.copy()
    if "tech_score" in df.columns:
        df = df[df["tech_score"] >= min_tech_score]
    df = df[df["trend_ok"] & (df["score"] >= min_score)]
    if df.empty:
        return df.reset_index(drop=True)
    sort_cols = ["score"]
    if "tech_score" in df.columns:
        sort_cols.append("tech_score")
    df = df.sort_values(sort_cols, ascending=False).head(top_n)
    return df.reset_index(drop=True)


# ---------- 资金分配层：动量强度加权 + 上限 ----------
def allocation_layer(
    selected: pd.DataFrame,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
) -> pd.Series:
    """
    w_i ∝ (max(score_i, 0))^alpha，再单票 cap、总仓位 cap。
    返回 Series：index=ts_code, value=weight (sum <= max_gross)。
    """
    if selected.empty:
        return pd.Series(dtype=float)
    s = selected.set_index("ts_code")["score"].clip(lower=0.0)
    if (s <= 0).all():
        return pd.Series(dtype=float)
    raw = (s ** alpha)
    w = raw / raw.sum()
    w = w.clip(upper=max_weight)
    w = w / w.sum() * max_gross
    return w


# ---------- 三合一：给定价格截面，输出目标权重 ----------
def compute_target_weights(
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
) -> pd.Series:
    """
    信号层 → 选股层 → 资金分配层，返回目标权重 Series (ts_code -> weight)。
    prices: 当前截面或历史截面（最后一行为“当前”），index 需包含足够历史长度。
    """
    signals = signal_layer_cross_section(
        prices,
        lookback=lookback,
        skip_last=skip_last,
        fast=fast,
        slow=slow,
    )
    selected = selection_layer(
        signals,
        top_n=top_n,
        min_score=min_score,
        min_tech_score=min_tech_score,
    )
    weights = allocation_layer(
        selected,
        alpha=alpha,
        max_weight=max_weight,
        max_gross=max_gross,
    )
    return weights
