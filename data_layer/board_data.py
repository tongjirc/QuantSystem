import os
from typing import Optional

import pandas as pd


def _quant_root_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def board_members_path() -> str:
    return os.path.join(_quant_root_dir(), "data", "board", "board_members.csv")


def load_board_members() -> pd.DataFrame:
    """
    加载板块成份映射：ts_code -> board_code。
    由 downloader/csi500_daily_downloader.py 在下载板块数据时生成。
    """
    path = board_members_path()
    if not os.path.exists(path):
        raise FileNotFoundError(f"Board members mapping not found: {path}")
    df = pd.read_csv(path)
    if "ts_code" not in df.columns or "board_code" not in df.columns:
        raise ValueError(f"Board members file missing ts_code/board_code columns: {path}")
    df["ts_code"] = df["ts_code"].astype(str)
    df["board_code"] = df["board_code"].astype(str)
    return df


def load_board_index_close(board_code: str) -> pd.Series:
    """
    加载单个行业板块指数的收盘价序列。
    文件来源：data/raw/board_{board_code}.csv
    """
    path = os.path.join(_quant_root_dir(), "data", "raw", f"board_{board_code}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    df = pd.read_csv(path)
    if "trade_date" not in df.columns or "close" not in df.columns:
        # 与 downloader 存储格式一致：trade_date, close
        raise ValueError(f"Missing trade_date/close in {path}")
    df["trade_date"] = pd.to_datetime(df["trade_date"].astype(str), format="%Y%m%d")
    df = df.sort_values("trade_date").set_index("trade_date")
    return df["close"]


