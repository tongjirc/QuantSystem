from __future__ import annotations

"""
数据质量与收益分布检查工具。

包括：
- 单票数据完整性：日期连续性、缺失值、异常价格
- 收益分布统计：均值/波动率/极端值
- 市场整体缺失概览：某日是否大量标的缺数据
"""

import random
from typing import Sequence

import numpy as np
import pandas as pd

from data_layer.daily_csv_store import (
    build_price_panel,
    list_available_ts_codes,
    load_daily,
)


def check_single_symbol(ts_code: str, start: str, end: str) -> None:
    """
    打印单个标的在区间内的数据质量信息。
    """
    print(f"\n=== Single symbol quality: {ts_code} ({start}-{end}) ===")
    df = load_daily(ts_code, start_date=start, end_date=end, columns=["open", "high", "low", "close", "vol", "amount"])
    if df.empty:
        print("  no data.")
        return

    print(f"  rows: {len(df)}, first: {df.index[0].date()}, last: {df.index[-1].date()}")
    # 缺失情况
    na_ratio = df.isna().mean()
    print("  NA ratio per column:")
    for c, r in na_ratio.items():
        print(f"    {c}: {r:.4f}")

    # 日期连续性（以交易日为基准，允许周末/节假日缺）
    date_diff = df.index.to_series().diff().dt.days.dropna()
    print(
        f"  date gaps: max={date_diff.max()}, "
        f"95%={date_diff.quantile(0.95):.2f}, "
        f"99%={date_diff.quantile(0.99):.2f}"
    )

    # 价格/收益分布
    close = df["close"].astype(float)
    ret = close.pct_change().dropna()
    print(
        f"  return stats: mean={ret.mean():.6f}, std={ret.std():.6f}, "
        f"min={ret.min():.4f}, max={ret.max():.4f}"
    )

    # 列出极端收益日（绝对值 > 20%）
    extreme = ret[ret.abs() > 0.2]
    if not extreme.empty:
        print("  extreme daily returns (|r|>20%):")
        for dt, r in extreme.items():
            print(f"    {dt.date()}: {r:.4f}")


def check_market_missing(
    ts_codes: Sequence[str] | None = None,
    start: str = "20200101",
    end: str = "20251231",
) -> None:
    """
    检查全市场在某些日期是否大量缺失 close 价格。
    """
    print(f"\n=== Market-level missing overview ({start}-{end}) ===")
    if ts_codes is None:
        ts_codes = list_available_ts_codes()
    if not ts_codes:
        print("  no symbols found.")
        return

    prices = build_price_panel(list(ts_codes), start_date=start, end_date=end)
    if prices.empty:
        print("  empty price panel.")
        return

    # 每日缺失比例
    na_ratio_daily = prices.isna().mean(axis=1)
    print(
        f"  daily NA ratio: mean={na_ratio_daily.mean():.4f}, "
        f"95%={na_ratio_daily.quantile(0.95):.4f}, "
        f"max={na_ratio_daily.max():.4f}"
    )

    high_na_days = na_ratio_daily[na_ratio_daily > 0.3]
    if not high_na_days.empty:
        print("  days with >30% symbols missing:")
        for dt, r in high_na_days.items():
            print(f"    {dt.date()}: NA ratio={r:.4f}")


def sample_return_distribution(
    ts_codes: Sequence[str] | None = None,
    n_symbols: int = 10,
    start: str = "20200101",
    end: str = "20251231",
) -> None:
    """
    随机抽样若干标的，汇总收益分布统计（均值/波动/极值）。
    """
    print(f"\n=== Sampled return distribution ({start}-{end}) ===")
    if ts_codes is None:
        ts_codes = list_available_ts_codes()
    if not ts_codes:
        print("  no symbols found.")
        return

    ts_codes = list(ts_codes)
    if len(ts_codes) > n_symbols:
        ts_codes = random.sample(ts_codes, n_symbols)

    all_rets: list[pd.Series] = []
    for code in ts_codes:
        try:
            df = load_daily(code, start_date=start, end_date=end, columns=["close"])
        except Exception as e:
            print(f"  {code}: load failed: {e}")
            continue
        if df.empty:
            print(f"  {code}: empty.")
            continue
        ret = df["close"].astype(float).pct_change().dropna()
        print(
            f"  {code}: n={len(ret)}, mean={ret.mean():.6f}, std={ret.std():.6f}, "
            f"min={ret.min():.4f}, max={ret.max():.4f}"
        )
        all_rets.append(ret)

    if not all_rets:
        return

    concat = pd.concat(all_rets, axis=0)
    print(
        f"\n  pooled returns across {len(all_rets)} symbols: "
        f"n={len(concat)}, mean={concat.mean():.6f}, std={concat.std():.6f}, "
        f"min={concat.min():.4f}, max={concat.max():.4f}"
    )


if __name__ == "__main__":
    codes = list_available_ts_codes()
    if codes:
        check_single_symbol(codes[0], "20200101", "20251231")
        sample_return_distribution(codes, n_symbols=10, start="20200101", end="20251231")
        check_market_missing(codes, start="20200101", end="20251231")

