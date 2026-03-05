from __future__ import annotations

"""
用 AKShare 抽样校验 Tushare CSV 数据。

功能：
- 抽样若干 ts_code + 日期区间
- 从本地 CSV 读 Tushare 数据
- 从 AKShare 在线抓相同标的 & 区间
- 对比 open/high/low/close/vol/amount 的差异
"""

import os
import random
from typing import Iterable, Sequence

import akshare as ak
import numpy as np
import pandas as pd

from data_layer.daily_csv_store import default_raw_dir, stock_csv_path, list_available_ts_codes


def _load_local_tushare(ts_code: str, start: str, end: str) -> pd.DataFrame:
    path = stock_csv_path(ts_code)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if "trade_date" not in df.columns:
        raise ValueError(f"Missing trade_date in {path}")
    # trade_date 可能为 int/str，统一转成字符串再按 YYYYMMDD 做区间过滤
    trade_date_str = df["trade_date"].astype(str)
    df = df[(trade_date_str >= str(start)) & (trade_date_str <= str(end))].copy()
    df["trade_date"] = pd.to_datetime(trade_date_str[df.index], format="%Y%m%d")
    df = df.set_index("trade_date").sort_index()
    return df


def _fetch_akshare_daily(ts_code: str, start: str, end: str) -> pd.DataFrame:
    """
    ts_code 形如 600519.SH / 000001.SZ
    AKShare 股票接口符号不带 .SH/.SZ，需要转换。
    """
    code, exch = ts_code.split(".")
    symbol = code
    df = ak.stock_zh_a_hist(
        symbol=symbol,
        period="daily",
        start_date=start,
        end_date=end,
        adjust="qfq",  # 使用前复权价格做对比
    )
    # AKShare 默认列：日期, 开盘, 收盘, 最高, 最低, 成交量, 成交额, 振幅, 涨跌幅, 涨跌额, 换手率
    if df.empty:
        return df
    df = df.rename(
        columns={
            "日期": "trade_date",
            "开盘": "open",
            "收盘": "close",
            "最高": "high",
            "最低": "low",
            "成交量": "vol",
            "成交额": "amount",
        }
    )
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = df.set_index("trade_date").sort_index()
    return df[["open", "high", "low", "close", "vol", "amount"]]


def compare_one_symbol(ts_code: str, start: str, end: str) -> pd.DataFrame:
    """
    返回一个 DataFrame：index=trade_date, columns=每个字段的 Tushare / AKShare / diff。
    """
    ts_df = _load_local_tushare(ts_code, start, end)
    ak_df = _fetch_akshare_daily(ts_code, start, end)
    if ts_df.empty or ak_df.empty:
        print(f"{ts_code}: local or AKShare data empty in range {start}-{end}")
        return pd.DataFrame()

    # 对齐日期
    cols = ["open", "high", "low", "close", "vol", "amount"]
    ts_sub = ts_df[cols].astype(float)
    ak_sub = ak_df[cols].astype(float)

    joined = ts_sub.join(ak_sub, lsuffix="_ts", rsuffix="_ak", how="inner")
    if joined.empty:
        print(f"{ts_code}: no overlapping dates after join")
        return joined

    for c in cols:
        joined[f"{c}_diff"] = joined[f"{c}_ts"] - joined[f"{c}_ak"]
        # 相对误差（避免除 0）
        denom = joined[f"{c}_ak"].replace(0, np.nan)
        joined[f"{c}_rel_diff"] = joined[f"{c}_diff"] / denom

    return joined


def sample_compare(
    ts_codes: Sequence[str] | None = None,
    n_symbols: int = 5,
    start: str = "20200101",
    end: str = "20251231",
) -> None:
    """
    随机抽样若干标的做 Tushare vs AKShare 对比，打印基本误差统计。
    """
    if ts_codes is None:
        ts_codes = list_available_ts_codes()
    if not ts_codes:
        print("No local Tushare CSV found.")
        return

    ts_codes = list(ts_codes)
    if len(ts_codes) > n_symbols:
        ts_codes = random.sample(ts_codes, n_symbols)

    print(f"Sampling {len(ts_codes)} symbols between {start} and {end}")
    for code in ts_codes:
        print(f"\n=== {code} ===")
        try:
            joined = compare_one_symbol(code, start, end)
        except Exception as e:
            print(f"  compare failed: {e}")
            continue
        if joined.empty:
            print("  empty joined data.")
            continue

        for field in ["open", "high", "low", "close", "vol", "amount"]:
            abs_diff = joined[f"{field}_diff"].abs()
            rel_diff = joined[f"{field}_rel_diff"].abs()
            print(
                f"  {field}: "
                f"mean_abs={abs_diff.mean():.4f}, "
                f"max_abs={abs_diff.max():.4f}, "
                f"mean_rel={rel_diff.mean():.6f}, "
                f"max_rel={rel_diff.max():.6f}"
            )


if __name__ == "__main__":
    # 默认简单运行一轮抽样对比
    sample_compare()

