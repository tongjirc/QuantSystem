import os
from dataclasses import dataclass
from typing import Optional

import pandas as pd


def default_positions_path() -> str:
    # stored as data file under quant_system/data/
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, "data", "positions.csv")


@dataclass
class Positions:
    df: pd.DataFrame

    def get_qty(self, ts_code: str) -> float:
        row = self.df[self.df["ts_code"] == ts_code]
        if row.empty:
            return 0.0
        return float(row.iloc[0]["qty"])

    def get_target_weight(self, ts_code: str) -> float:
        """
        Optional helper: if positions.csv包含 target_weight 列，则返回对应值（0-1）；否则返回 0.
        """
        if "target_weight" not in self.df.columns:
            return 0.0
        row = self.df[self.df["ts_code"] == ts_code]
        if row.empty:
            return 0.0
        return float(row.iloc[0]["target_weight"])


def load_positions(path: Optional[str] = None) -> Positions:
    path = path or default_positions_path()
    if not os.path.exists(path):
        df = pd.DataFrame(columns=["ts_code", "qty", "target_weight"])
        return Positions(df=df)
    df = pd.read_csv(path)
    if df.empty:
        df = pd.DataFrame(columns=["ts_code", "qty", "target_weight"])
    if "ts_code" not in df.columns or "qty" not in df.columns:
        raise ValueError(f"positions file must contain ts_code, qty: {path}")
    if "target_weight" not in df.columns:
        df["target_weight"] = 0.0
    df["ts_code"] = df["ts_code"].astype(str)
    df["qty"] = df["qty"].astype(float)
    df["target_weight"] = df["target_weight"].astype(float)
    return Positions(df=df)


def save_positions(pos: Positions, path: Optional[str] = None) -> None:
    path = path or default_positions_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pos.df.to_csv(path, index=False)

