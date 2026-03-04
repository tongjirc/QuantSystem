"""
组合回测流水线：构建截面数据 → 跑组合回测 → 输出指标与可视化。
"""
from __future__ import annotations

import argparse
import os
import sys

# 保证从 quant_system 根目录运行时能 import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from backtest.portfolio_engine import run_portfolio_backtest
from config.settings import (
    ALPHA,
    BACKTEST_DEFAULT_END,
    BACKTEST_DEFAULT_START,
    FEE_BPS,
    MAX_GROSS,
    MAX_WEIGHT,
    MIN_TECH_SCORE,
    MOMENTUM_LOOKBACK,
    MOMENTUM_SKIP_LAST,
    REBALANCE_DAYS,
    TOP_N,
    TREND_FAST_MA,
    TREND_SLOW_MA,
)
from data_layer.daily_csv_store import build_price_panel, list_available_ts_codes, default_raw_dir


def _drawdown(equity: pd.Series) -> pd.Series:
    cummax = equity.cummax()
    return (equity - cummax) / cummax


def main():
    p = argparse.ArgumentParser(description="Portfolio backtest: momentum Top-N + allocation + technical filters")
    p.add_argument("--start", default=BACKTEST_DEFAULT_START, help="Start date YYYYMMDD")
    p.add_argument("--end", default=BACKTEST_DEFAULT_END, help="End date YYYYMMDD")
    p.add_argument("--top-n", type=int, default=TOP_N)
    p.add_argument("--lookback", type=int, default=MOMENTUM_LOOKBACK)
    p.add_argument("--skip-last", type=int, default=MOMENTUM_SKIP_LAST)
    p.add_argument("--fast", type=int, default=TREND_FAST_MA)
    p.add_argument("--slow", type=int, default=TREND_SLOW_MA)
    p.add_argument("--alpha", type=float, default=ALPHA, help="Momentum weight exponent")
    p.add_argument("--max-weight", type=float, default=MAX_WEIGHT)
    p.add_argument("--max-gross", type=float, default=MAX_GROSS)
    p.add_argument("--fee-bps", type=float, default=FEE_BPS)
    p.add_argument(
        "--min-tech-score",
        type=float,
        default=MIN_TECH_SCORE,
        help="Minimum technical checklist score to be eligible for selection",
    )
    p.add_argument(
        "--rebalance-days",
        type=int,
        default=REBALANCE_DAYS,
        help="Rebalance frequency in trading days (e.g. 20 ~ monthly)",
    )
    p.add_argument("--plot", action="store_true", help="Show and save plot")
    p.add_argument("--save-only", action="store_true", help="Save plot to file only (no display)")
    p.add_argument("--out-dir", default=None, help="Directory to save plot (default: data/backtest)")
    args = p.parse_args()

    ts_codes = list_available_ts_codes()
    if not ts_codes:
        raise SystemExit("No raw data found. Run download first (e.g. ./run.sh -l).")

    raw_dir = default_raw_dir()
    prices = build_price_panel(ts_codes, start_date=args.start, end_date=args.end, raw_dir=raw_dir)
    if prices.empty or len(prices) < args.lookback + args.skip_last + 10:
        raise SystemExit("Price panel too short. Check date range and data.")

    res = run_portfolio_backtest(
        prices,
        top_n=args.top_n,
        min_tech_score=args.min_tech_score,
        lookback=args.lookback,
        skip_last=args.skip_last,
        fast=args.fast,
        slow=args.slow,
        alpha=args.alpha,
        max_weight=args.max_weight,
        max_gross=args.max_gross,
        fee_bps=args.fee_bps,
        rebalance_days=args.rebalance_days,
    )

    if res.equity.empty:
        print("Backtest produced no equity (insufficient history).")
        return

    # 指标
    total_ret = res.equity.iloc[-1] / res.equity.iloc[0] - 1.0
    n_days = len(res.equity)
    annual_ret = (1.0 + total_ret) ** (252.0 / max(1, n_days)) - 1.0
    vol = res.daily_return.std() * (252 ** 0.5)
    sharpe = annual_ret / vol if vol > 0 else 0.0
    dd = _drawdown(res.equity)
    mdd = dd.min()

    bench = res.benchmark_equity
    if bench is not None and not bench.empty:
        bench_ret = bench.iloc[-1] / bench.iloc[0] - 1.0
        print(f"Benchmark (EW) total return: {bench_ret*100:.2f}%")
    print(f"Strategy total return: {total_ret*100:.2f}%")
    print(f"Annualized return: {annual_ret*100:.2f}%")
    print(f"Ann. volatility: {vol*100:.2f}%")
    print(f"Sharpe (ann.): {sharpe:.2f}")
    print(f"Max drawdown: {mdd*100:.2f}%")

    if args.plot or args.save_only:
        import matplotlib
        if args.save_only:
            matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates

        out_dir = args.out_dir or os.path.join(os.path.dirname(raw_dir), "backtest")
        os.makedirs(out_dir, exist_ok=True)

        fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
        ax1, ax2, ax3 = axes

        # 1) Equity vs Benchmark
        ax1.plot(res.equity.index, res.equity.values, label="Strategy", color="steelblue", linewidth=2)
        if bench is not None and not bench.empty:
            ax1.plot(bench.index, bench.values, label="Benchmark (EW)", color="gray", linewidth=1, alpha=0.8)
        ax1.set_ylabel("Equity")
        ax1.set_title("Portfolio Equity vs Benchmark")
        ax1.legend(loc="upper left")
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.2f}"))

        # 2) Drawdown
        ax2.fill_between(dd.index, dd.values * 100, 0, color="coral", alpha=0.6)
        ax2.set_ylabel("Drawdown %")
        ax2.set_title("Underwater (Drawdown)")
        ax2.grid(True, alpha=0.3)

        # 3) Turnover
        ax3.plot(res.turnover.index, res.turnover.values * 100, color="green", alpha=0.7)
        ax3.set_ylabel("Turnover %")
        ax3.set_xlabel("Date")
        ax3.set_title("Daily Turnover")
        ax3.grid(True, alpha=0.3)

        for ax in axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
            ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.tight_layout()
        path = os.path.join(out_dir, "portfolio_backtest.png")
        plt.savefig(path, dpi=150)
        print(f"Plot saved: {path}")
        if args.plot:
            plt.show()
        else:
            plt.close()

    return res


if __name__ == "__main__":
    main()
