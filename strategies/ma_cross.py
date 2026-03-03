import pandas as pd


def ma_cross_target_position(
    df: pd.DataFrame,
    fast: int = 20,
    slow: int = 60,
    price_col: str = "close",
) -> pd.Series:
    """
    Long-only MA cross.
    Returns a Series of target position (0 or 1) indexed by trade_date.
    """
    if price_col not in df.columns:
        raise ValueError(f"Missing {price_col}")

    ma_fast = df[price_col].rolling(fast).mean()
    ma_slow = df[price_col].rolling(slow).mean()
    target = (ma_fast > ma_slow).astype(int)
    target.name = f"pos_ma{fast}_{slow}"
    return target


def positions_to_trades(target_pos: pd.Series) -> pd.Series:
    """
    Convert target positions to trades:
    +1 buy, -1 sell, 0 hold (per day).
    """
    trades = target_pos.diff().fillna(0)
    trades.name = "trade"
    return trades

