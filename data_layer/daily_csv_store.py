import os
from typing import Iterable, Optional

import pandas as pd


def _quant_root_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def default_raw_dir() -> str:
    # A 股（日线）默认目录：data/raw
    return os.path.join(_quant_root_dir(), "data", "raw")


def stock_csv_path(ts_code: str, raw_dir: Optional[str] = None) -> str:
    raw_dir = raw_dir or default_raw_dir()
    return os.path.join(raw_dir, f"{ts_code}.csv")


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

