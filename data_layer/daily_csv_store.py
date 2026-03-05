import os
from typing import Iterable, Optional

import pandas as pd


def _quant_root_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def default_raw_dir() -> str:
    # A 股（日线）默认目录：data/raw
    return os.path.join(_quant_root_dir(), "data", "raw")


def default_adj_factor_dir() -> str:
    # 复权因子默认目录：data/adj_factor
    return os.path.join(_quant_root_dir(), "data", "adj_factor")


def stock_csv_path(ts_code: str, raw_dir: Optional[str] = None) -> str:
    raw_dir = raw_dir or default_raw_dir()
    return os.path.join(raw_dir, f"{ts_code}.csv")


def adj_factor_csv_path(ts_code: str, adj_dir: Optional[str] = None) -> str:
    adj_dir = adj_dir or default_adj_factor_dir()
    return os.path.join(adj_dir, f"{ts_code}.csv")


def list_available_ts_codes(raw_dir: Optional[str] = None) -> list[str]:
    raw_dir = raw_dir or default_raw_dir()
    if not os.path.isdir(raw_dir):
        return []
    out: list[str] = []
    for name in os.listdir(raw_dir):
        if name.endswith(".csv"):
            out.append(name[:-4])
    out.sort()
    return out


def load_daily(
    ts_code: str,
    raw_dir: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    """
    Load daily OHLCV data from CSV saved by downloader.

    - ts_code: e.g. "600519.SH"
    - start_date/end_date: "YYYYMMDD" (inclusive)
    - columns: subset of columns to keep (trade_date is always kept)
    """
    path = stock_csv_path(ts_code, raw_dir=raw_dir)
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    df = pd.read_csv(path)
    if df.empty:
        return df

    if "trade_date" not in df.columns:
        raise ValueError(f"Missing trade_date in {path}")

    if columns is not None:
        cols = ["trade_date"] + [c for c in columns if c != "trade_date"]
        missing = [c for c in cols if c not in df.columns]
        if missing:
            raise ValueError(f"Missing columns {missing} in {path}")
        df = df[cols]

    # Filter by date (string compare is safe for YYYYMMDD)
    if start_date:
        df = df[df["trade_date"].astype(str) >= str(start_date)]
    if end_date:
        df = df[df["trade_date"].astype(str) <= str(end_date)]

    df = df.sort_values("trade_date")
    df["trade_date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d")
    df = df.set_index("trade_date")
    return df


def build_price_panel(
    ts_codes: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    raw_dir: Optional[str] = None,
    price_col: str = "close",
) -> pd.DataFrame:
    """
    Build aligned panel: index = trade_date, columns = ts_code, values = close.
    Missing dates/stocks left as NaN.
    """
    raw_dir = raw_dir or default_raw_dir()
    dfs: list[pd.DataFrame] = []
    for ts_code in ts_codes:
        try:
            df = load_daily(ts_code, raw_dir=raw_dir, start_date=start_date, end_date=end_date, columns=[price_col])
            if df.empty:
                continue
            df = df[[price_col]].rename(columns={price_col: ts_code})
            dfs.append(df)
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    panel = pd.concat(dfs, axis=1)
    panel = panel.sort_index()
    return panel


def build_returns_panel(
    ts_codes: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    raw_dir: Optional[str] = None,
) -> pd.DataFrame:
    """Daily returns panel (pct_change on close). Index=date, columns=ts_code."""
    prices = build_price_panel(ts_codes, start_date=start_date, end_date=end_date, raw_dir=raw_dir)
    if prices.empty:
        return pd.DataFrame()
    return prices.pct_change()


def load_adj_factor(
    ts_code: str,
    adj_dir: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    加载 Tushare adj_factor 下载的复权因子。
    """
    path = adj_factor_csv_path(ts_code, adj_dir=adj_dir)
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if df.empty:
        return df
    if "trade_date" not in df.columns or "adj_factor" not in df.columns:
        raise ValueError(f"Missing trade_date/adj_factor in {path}")
    if start_date:
        df = df[df["trade_date"].astype(str) >= str(start_date)]
    if end_date:
        df = df[df["trade_date"].astype(str) <= str(end_date)]
    df = df.sort_values("trade_date")
    df["trade_date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d")
    df = df.set_index("trade_date")
    return df


def build_adj_price_panel(
    ts_codes: list[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    raw_dir: Optional[str] = None,
    adj_dir: Optional[str] = None,
) -> pd.DataFrame:
    """
    使用未复权 close + Tushare adj_factor 构建前复权价格面板。
    算法：adj_close_t = close_t * adj_factor_t / adj_factor_last
    """
    raw_dir = raw_dir or default_raw_dir()
    adj_dir = adj_dir or default_adj_factor_dir()
    dfs: list[pd.Series] = []
    for ts_code in ts_codes:
        try:
            price_df = load_daily(
                ts_code,
                raw_dir=raw_dir,
                start_date=start_date,
                end_date=end_date,
                columns=["close"],
            )
            if price_df.empty:
                continue
            adj_df = load_adj_factor(
                ts_code,
                adj_dir=adj_dir,
                start_date=start_date,
                end_date=end_date,
            )
            if adj_df.empty:
                continue
            joined = price_df.join(adj_df[["adj_factor"]], how="inner")
            if joined.empty:
                continue
            base = joined["adj_factor"].iloc[-1]
            if base <= 0:
                continue
            adj_close = joined["close"] * joined["adj_factor"] / base
            dfs.append(adj_close.rename(ts_code))
        except Exception:
            continue
    if not dfs:
        return pd.DataFrame()
    panel = pd.concat(dfs, axis=1)
    panel = panel.sort_index()
    return panel


