from __future__ import annotations

import numpy as np
import pandas as pd

from config.settings import (
    ALPHA,
    BOARD_MAX_GROSS,
    BOARD_TOP_K,
    FACTOR_WEIGHT_MACD,
    FACTOR_WEIGHT_MOMENTUM,
    FACTOR_WEIGHT_RSI,
    FACTOR_WEIGHT_TREND,
    MAX_GROSS,
    MAX_WEIGHT,
    MOMENTUM_LOOKBACK,
    MOMENTUM_SKIP_LAST,
    RSI_PERIOD,
    TOP_N,
    TREND_FAST_MA,
    TREND_SLOW_MA,
    VOL_CAP,
)
from data_layer.board_data import load_board_index_close, load_board_members
from strategies.momentum_portfolio import allocation_layer, momentum_score, selection_layer, trend_ok


def _macd_series(close: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[pd.Series, pd.Series]:
    """返回整条 DIF、DEA 序列，用于 MACD 策略打分。"""
    close = close.dropna()
    if close.empty:
        return (pd.Series(dtype=float), pd.Series(dtype=float))
    ema_fast = close.ewm(span=fast, adjust=False).mean()
    ema_slow = close.ewm(span=slow, adjust=False).mean()
    diff = ema_fast - ema_slow
    dea = diff.ewm(span=signal, adjust=False).mean()
    return diff, dea


def _rsi_series(close: pd.Series, period: int = RSI_PERIOD) -> pd.Series:
    """RSI 全序列，用于 RSI 策略。"""
    close = close.dropna()
    if len(close) < period + 1:
        return pd.Series(dtype=float)
    delta = close.diff()
    up = delta.clip(lower=0.0)
    down = -delta.clip(upper=0.0)
    roll_up = up.rolling(period).mean()
    roll_down = down.rolling(period).mean()
    rs = roll_up / roll_down.replace(0, np.nan)
    rsi = 100.0 - 100.0 / (1.0 + rs)
    return rsi


def compute_target_weights_macd(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
    fast: int = TREND_FAST_MA,
    slow: int = TREND_SLOW_MA,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    **_: object,
) -> pd.Series:
    """
    MACD 组合：用 MACD 柱值（DIF-DEA）做截面得分 + 趋势过滤。
    - score: 当前 MACD 柱值（越大越强）
    - trend_ok: 仍沿用 MA20 > MA60 过滤
    """
    rows: list[dict[str, object]] = []
    for ts_code in prices.columns:
        close = prices[ts_code].dropna()
        if len(close) < slow + 30:  # 给 MACD 留一点历史
            continue
        diff, dea = _macd_series(close)
        if diff.empty or dea.empty:
            continue
        macd_bar = diff.iloc[-1] - dea.iloc[-1]
        if np.isnan(macd_bar):
            continue
        ok = trend_ok(close, fast=fast, slow=slow)
        rows.append({"ts_code": ts_code, "score": float(macd_bar), "trend_ok": ok, "tech_score": float(macd_bar)})

    if not rows:
        return pd.Series(dtype=float)
    signals = pd.DataFrame(rows)
    selected = selection_layer(signals, top_n=top_n, min_score=min_score, min_tech_score=0.0)
    return allocation_layer(selected, alpha=alpha, max_weight=max_weight, max_gross=max_gross)


def compute_target_weights_bollinger(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
    fast: int = TREND_FAST_MA,
    slow: int = TREND_SLOW_MA,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    bb_window: int = 20,
    bb_mult: float = 2.0,
    **_: object,
) -> pd.Series:
    """
    布林带突破组合：
    - score: 当前价格相对上轨的突破幅度 (close / upper - 1)
    - 仅在趋势向上（MA20>MA60）时参与排序
    """
    rows: list[dict[str, object]] = []
    for ts_code in prices.columns:
        close = prices[ts_code].dropna()
        if len(close) < max(slow, bb_window) + 1:
            continue
        ma = close.rolling(bb_window).mean()
        std = close.rolling(bb_window).std()
        upper = ma + bb_mult * std
        last_close = close.iloc[-1]
        last_upper = upper.iloc[-1]
        if np.isnan(last_close) or np.isnan(last_upper) or last_upper <= 0:
            continue
        score = last_close / last_upper - 1.0
        ok = trend_ok(close, fast=fast, slow=slow)
        rows.append({"ts_code": ts_code, "score": float(score), "trend_ok": ok, "tech_score": float(score)})

    if not rows:
        return pd.Series(dtype=float)
    signals = pd.DataFrame(rows)
    selected = selection_layer(signals, top_n=top_n, min_score=min_score, min_tech_score=0.0)
    return allocation_layer(selected, alpha=alpha, max_weight=max_weight, max_gross=max_gross)


def compute_target_weights_rsi(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    rsi_period: int = RSI_PERIOD,
    **_: object,
) -> pd.Series:
    """
    RSI 超卖反转组合（只做多超卖标的）：
    - 当 RSI < 30 时，score = 30 - RSI，越低越超卖，得分越高；
    - 否则 score = 0（不参与）。
    """
    rows: list[dict[str, object]] = []
    for ts_code in prices.columns:
        close = prices[ts_code].dropna()
        if len(close) < rsi_period + 1:
            continue
        rsi = _rsi_series(close, period=rsi_period)
        if rsi.empty or np.isnan(rsi.iloc[-1]):
            continue
        last_rsi = float(rsi.iloc[-1])
        if last_rsi < 30.0:
            score = 30.0 - last_rsi
        else:
            score = 0.0
        rows.append({"ts_code": ts_code, "score": float(score), "trend_ok": True, "tech_score": float(score)})

    if not rows:
        return pd.Series(dtype=float)
    signals = pd.DataFrame(rows)
    selected = selection_layer(signals, top_n=top_n, min_score=min_score, min_tech_score=0.0)
    return allocation_layer(selected, alpha=alpha, max_weight=max_weight, max_gross=max_gross)


def compute_target_weights_intraday_breakout(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    breakout_lookback: int = 20,
    **_: object,
) -> pd.Series:
    """
    日内突破策略的日线近似版：
    - 使用收盘价近似“突破今日高点”：close > 过去 N 日最高收盘价
    - score = close / rolling_max(close, N) - 1.0
    """
    rows: list[dict[str, object]] = []
    for ts_code in prices.columns:
        close = prices[ts_code].dropna()
        if len(close) < breakout_lookback + 1:
            continue
        past_max = close.shift(1).rolling(breakout_lookback).max()
        last_close = close.iloc[-1]
        last_max = past_max.iloc[-1]
        if np.isnan(last_close) or np.isnan(last_max) or last_max <= 0:
            continue
        score = last_close / last_max - 1.0
        rows.append({"ts_code": ts_code, "score": float(score), "trend_ok": True, "tech_score": float(score)})

    if not rows:
        return pd.Series(dtype=float)
    signals = pd.DataFrame(rows)
    selected = selection_layer(signals, top_n=top_n, min_score=min_score, min_tech_score=0.0)
    return allocation_layer(selected, alpha=alpha, max_weight=max_weight, max_gross=max_gross)


def compute_target_weights_multi_factor(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
    fast: int = TREND_FAST_MA,
    slow: int = TREND_SLOW_MA,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    **_: object,
) -> pd.Series:
    """
    多因子动量/技术组合（更“干净”的线性打分）：
    因子：
    - MOM: 价格动量（同 momentum 策略）
    - MACD: MACD 柱值
    - RSI: RSI 反转程度（30 以下越低越好）
    - TREND: MA20 > MA60 作为趋势加分

    截面上对各因子做 z-score 后线性组合：
    score = w_mom * z_mom + w_macd * z_macd + w_rsi * z_rsi + w_trend * TREND
    """
    rows: list[dict[str, object]] = []
    for ts_code in prices.columns:
        close = prices[ts_code].dropna()
        if len(close) < max(slow, lookback + skip_last) + 5:
            continue

        # 基础因子
        mom = momentum_score(close, lookback=lookback, skip_last=skip_last)
        if np.isnan(mom):
            continue

        diff, dea = _macd_series(close)
        macd_bar = (diff - dea).iloc[-1] if not diff.empty and not dea.empty else np.nan

        rsi_series = _rsi_series(close, period=RSI_PERIOD)
        rsi_last = rsi_series.iloc[-1] if not rsi_series.empty else np.nan
        # 30 以下越低越好；50~70 区间认为中性
        if not np.isnan(rsi_last) and rsi_last < 30.0:
            rsi_factor = 30.0 - float(rsi_last)
        else:
            rsi_factor = 0.0

        trend_flag = 1.0 if trend_ok(close, fast=fast, slow=slow) else 0.0

        rows.append(
            {
                "ts_code": ts_code,
                "mom": float(mom),
                "macd": float(macd_bar) if not np.isnan(macd_bar) else 0.0,
                "rsi": float(rsi_factor),
                "trend": trend_flag,
            }
        )

    if not rows:
        return pd.Series(dtype=float)

    df = pd.DataFrame(rows).set_index("ts_code")

    def _z(col: str) -> pd.Series:
        s = df[col]
        mean = s.mean()
        std = s.std()
        if std <= 0 or np.isnan(std):
            return pd.Series(0.0, index=s.index)
        return (s - mean) / std

    z_mom = _z("mom")
    z_macd = _z("macd")
    z_rsi = _z("rsi")
    trend = df["trend"]

    score = (
        FACTOR_WEIGHT_MOMENTUM * z_mom
        + FACTOR_WEIGHT_MACD * z_macd
        + FACTOR_WEIGHT_RSI * z_rsi
        + FACTOR_WEIGHT_TREND * trend
    )

    signals = df.copy()
    signals["score"] = score
    signals["trend_ok"] = trend > 0.0
    signals["tech_score"] = score
    signals = signals.reset_index()

    selected = selection_layer(signals, top_n=top_n, min_score=min_score, min_tech_score=0.0)
    return allocation_layer(selected, alpha=alpha, max_weight=max_weight, max_gross=max_gross)


def compute_target_weights_multi_factor_board(
    prices: pd.DataFrame,
    top_n: int = TOP_N,
    min_score: float = 0.0,
    lookback: int = MOMENTUM_LOOKBACK,
    skip_last: int = MOMENTUM_SKIP_LAST,
    fast: int = TREND_FAST_MA,
    slow: int = TREND_SLOW_MA,
    alpha: float = ALPHA,
    max_weight: float = MAX_WEIGHT,
    max_gross: float = MAX_GROSS,
    **_: object,
) -> pd.Series:
    """
    多因子 + 板块中性 / 行业中性策略：
    - 因子：同 multi_factor（mom / macd / rsi / trend）
    - 板块中性：每个板块内单独排序，取前 BOARD_TOP_K
    - 板块趋势过滤：仅在板块指数 MA20>MA60 且板块动量为正的板块中选股
    - 板块权重上限：每个板块总仓位不超过 BOARD_MAX_GROSS
    - 波动率约束：对近 60 日年化波动率 > VOL_CAP 的个股按 VOL_CAP / vol 打折
    - 历史表现加权：板块权重与其过去 lookback 日动量相关（非负）
    """
    members = load_board_members()
    current_date = prices.index[-1]

    board_map = (
        members.drop_duplicates(subset=["ts_code"])[["ts_code", "board_code"]].set_index("ts_code")["board_code"]
    )

    rows: list[dict[str, object]] = []
    for ts_code in prices.columns:
        if ts_code not in board_map.index:
            continue
        close = prices[ts_code].dropna()
        if len(close) < max(slow, lookback + skip_last) + 60:
            continue

        mom = momentum_score(close, lookback=lookback, skip_last=skip_last)
        if np.isnan(mom):
            continue

        diff, dea = _macd_series(close)
        macd_bar = (diff - dea).iloc[-1] if not diff.empty and not dea.empty else np.nan

        rsi_series = _rsi_series(close, period=RSI_PERIOD)
        rsi_last = rsi_series.iloc[-1] if not rsi_series.empty else np.nan
        if not np.isnan(rsi_last) and rsi_last < 30.0:
            rsi_factor = 30.0 - float(rsi_last)
        else:
            rsi_factor = 0.0

        trend_flag = 1.0 if trend_ok(close, fast=fast, slow=slow) else 0.0

        ret = close.pct_change().dropna()
        if len(ret) >= 60:
            vol_60 = ret.iloc[-60:].std() * (252 ** 0.5)
        else:
            vol_60 = np.nan

        rows.append(
            {
                "ts_code": ts_code,
                "board_code": board_map.loc[ts_code],
                "mom": float(mom),
                "macd": float(macd_bar) if not np.isnan(macd_bar) else 0.0,
                "rsi": float(rsi_factor),
                "trend": trend_flag,
                "vol_60": float(vol_60) if not np.isnan(vol_60) else np.nan,
            }
        )

    if not rows:
        return pd.Series(dtype=float)

    df = pd.DataFrame(rows).set_index("ts_code")

    def _z(col: str) -> pd.Series:
        s = df[col]
        mean = s.mean()
        std = s.std()
        if std <= 0 or np.isnan(std):
            return pd.Series(0.0, index=s.index)
        return (s - mean) / std

    z_mom = _z("mom")
    z_macd = _z("macd")
    z_rsi = _z("rsi")
    trend = df["trend"]

    base_score = (
        FACTOR_WEIGHT_MOMENTUM * z_mom
        + FACTOR_WEIGHT_MACD * z_macd
        + FACTOR_WEIGHT_RSI * z_rsi
        + FACTOR_WEIGHT_TREND * trend
    )

    vol = df["vol_60"]
    vol_adj = pd.Series(1.0, index=vol.index)
    mask_vol = vol > VOL_CAP
    vol_adj[mask_vol] = VOL_CAP / vol[mask_vol]

    df["score_raw"] = base_score * vol_adj

    board_codes = df["board_code"].unique()
    board_moments: dict[str, float] = {}
    board_alloc: dict[str, float] = {}
    for b_code in board_codes:
        try:
            idx_close = load_board_index_close(b_code)
        except Exception:
            continue
        idx_hist = idx_close[idx_close.index <= current_date]
        if len(idx_hist) < lookback + 20:
            continue
        ma20 = idx_hist.rolling(20).mean().iloc[-1]
        ma60 = idx_hist.rolling(60).mean().iloc[-1]
        if pd.isna(ma20) or pd.isna(ma60) or ma20 <= ma60:
            continue
        p_end = idx_hist.iloc[-1]
        p_start = idx_hist.iloc[-lookback] if len(idx_hist) >= lookback else idx_hist.iloc[0]
        if p_start <= 0:
            continue
        m_b = float(p_end / p_start - 1.0)
        if m_b <= 0:
            continue
        board_moments[b_code] = m_b

    if not board_moments:
        # 若所有板块不过滤，则退化为普通 multi_factor
        signals = df.copy()
        signals["score"] = df["score_raw"]
        signals["trend_ok"] = True
        signals["tech_score"] = signals["score"]
        signals = signals.reset_index()
        selected = selection_layer(signals, top_n=top_n, min_score=min_score, min_tech_score=0.0)
        return allocation_layer(selected, alpha=alpha, max_weight=max_weight, max_gross=max_gross)

    board_scores = pd.Series(board_moments)
    w_raw = board_scores.clip(lower=0.0)
    w_raw = w_raw / w_raw.sum()
    board_alloc_raw = w_raw * max_gross
    for b in board_alloc_raw.index:
        board_alloc[b] = min(float(board_alloc_raw[b]), BOARD_MAX_GROSS)

    selected_rows: list[dict[str, object]] = []
    for b_code, alloc in board_alloc.items():
        if alloc <= 0:
            continue
        sub = df[df["board_code"] == b_code].copy()
        if sub.empty:
            continue
        sub = sub.sort_values("score_raw", ascending=False).head(BOARD_TOP_K)
        for ts_code, row in sub.iterrows():
            selected_rows.append(
                {"ts_code": ts_code, "board_code": b_code, "score": float(row["score_raw"]), "board_alloc": alloc}
            )

    if not selected_rows:
        return pd.Series(dtype=float)

    sel_df = pd.DataFrame(selected_rows)
    final_w = pd.Series(0.0, index=prices.columns)
    for b_code, group in sel_df.groupby("board_code"):
        alloc = group["board_alloc"].iloc[0]
        s = group.set_index("ts_code")["score"].clip(lower=0.0)
        if (s <= 0).all():
            continue
        raw = s ** alpha
        w_board = raw / raw.sum() * alloc
        w_board = w_board.clip(upper=max_weight)
        for ts_code, w_val in w_board.items():
            if ts_code in final_w.index:
                final_w[ts_code] += float(w_val)

    gross = final_w.abs().sum()
    if gross > 0 and gross > max_gross:
        final_w *= max_gross / gross

    return final_w


