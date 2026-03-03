from dataclasses import dataclass

import pandas as pd


@dataclass
class BacktestResult:
    equity: pd.Series
    daily_return: pd.Series
    trades: pd.Series


def run_long_only_backtest(
    price: pd.Series,
    target_pos: pd.Series,
    fee_bps: float = 2.0,
    slippage_bps: float = 0.0,
    initial_capital: float = 1_000_000.0,
) -> BacktestResult:
    """
    Very simple daily backtest (one asset, long-only, market-on-close style).

    - price: close price series indexed by date
    - target_pos: 0/1 series indexed by date (same index as price or superset)
    - fee_bps/slippage_bps: applied on position changes (turnover)
    """
    df = pd.DataFrame({"price": price}).dropna()
    df["target_pos"] = target_pos.reindex(df.index).ffill().fillna(0).astype(float)
    df["ret"] = df["price"].pct_change().fillna(0.0)

    # trades happen when target position changes
    df["trade"] = df["target_pos"].diff().fillna(df["target_pos"])
    turnover = df["trade"].abs()
    cost_rate = (fee_bps + slippage_bps) / 10000.0
    df["cost"] = turnover * cost_rate

    # apply position from previous close to next close
    df["pos"] = df["target_pos"].shift(1).fillna(0.0)
    df["strategy_ret"] = df["pos"] * df["ret"] - df["cost"]

    equity = (1.0 + df["strategy_ret"]).cumprod() * float(initial_capital)
    equity.name = "equity"

    daily_return = df["strategy_ret"].copy()
    daily_return.name = "daily_return"

    trades = df["trade"].copy()
    trades.name = "trade"

    return BacktestResult(equity=equity, daily_return=daily_return, trades=trades)

