import pandas as pd


def add_returns(df: pd.DataFrame, price_col: str = "close") -> pd.DataFrame:
    if price_col not in df.columns:
        raise ValueError(f"Missing {price_col}")
    out = df.copy()
    out["ret_1d"] = out[price_col].pct_change()
    return out


def add_moving_averages(
    df: pd.DataFrame,
    price_col: str = "close",
    windows: tuple[int, ...] = (5, 20, 60),
) -> pd.DataFrame:
    if price_col not in df.columns:
        raise ValueError(f"Missing {price_col}")
    out = df.copy()
    for w in windows:
        out[f"ma_{w}"] = out[price_col].rolling(w).mean()
    return out


def add_volatility(df: pd.DataFrame, ret_col: str = "ret_1d", window: int = 20) -> pd.DataFrame:
    if ret_col not in df.columns:
        raise ValueError(f"Missing {ret_col}; call add_returns first")
    out = df.copy()
    out[f"vol_{window}"] = out[ret_col].rolling(window).std() * (252**0.5)
    return out

