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
from data_layer.daily_csv_store import (
    build_adj_price_panel,
    build_price_panel,
    list_available_ts_codes,
    default_raw_dir,
)


def _drawdown(equity: pd.Series) -> pd.Series:
    cummax = equity.cummax()
    return (equity - cummax) / cummax


def main() -> None:
    p = argparse.ArgumentParser(description="Sweep multiple strategies and summarize performance.")
    p.add_argument("--start", default=BACKTEST_DEFAULT_START, help="Start date YYYYMMDD")
    p.add_argument("--end", default=BACKTEST_DEFAULT_END, help="End date YYYYMMDD")
    p.add_argument(
        "--price-mode",
        choices=["raw", "adj"],
        default="adj",
        help="Use raw close ('raw') or forward-adjusted price ('adj')",
    )
    p.add_argument(
        "--strategies",
        nargs="*",
        default=["momentum", "multi_factor", "multi_factor_board", "voting"],
        help="List of strategy names to evaluate",
    )
    p.add_argument("--out", default="data/backtest/strategy_sweep.csv", help="Output CSV path")
    args = p.parse_args()

    ts_codes = list_available_ts_codes()
    if not ts_codes:
        raise SystemExit("No raw data found. Run download first (e.g. ./run.sh -l).")

    raw_dir = default_raw_dir()
    if args.price_mode == "adj":
        prices = build_adj_price_panel(ts_codes, start_date=args.start, end_date=args.end, raw_dir=raw_dir)
    else:
        prices = build_price_panel(ts_codes, start_date=args.start, end_date=args.end, raw_dir=raw_dir)
    if prices.empty or len(prices) < MOMENTUM_LOOKBACK + MOMENTUM_SKIP_LAST + 10:
        raise SystemExit("Price panel too short. Check date range and data.")

    rows = []
    for name in args.strategies:
        print(f"\n=== Strategy: {name} ===")
        try:
            res = run_portfolio_backtest(
                prices,
                top_n=TOP_N,
                min_score=0.0,
                min_tech_score=MIN_TECH_SCORE,
                lookback=MOMENTUM_LOOKBACK,
                skip_last=MOMENTUM_SKIP_LAST,
                fast=TREND_FAST_MA,
                slow=TREND_SLOW_MA,
                alpha=ALPHA,
                max_weight=MAX_WEIGHT,
                max_gross=MAX_GROSS,
                fee_bps=FEE_BPS,
                rebalance_days=REBALANCE_DAYS,
                strategy=name,
            )
        except Exception as e:
            print(f"  Strategy {name} failed: {e}")
            continue

        if res.equity.empty:
            print("  No equity generated.")
            continue

        total_ret = res.equity.iloc[-1] / res.equity.iloc[0] - 1.0
        n_days = len(res.equity)
        annual_ret = (1.0 + total_ret) ** (252.0 / max(1, n_days)) - 1.0
        vol = res.daily_return.std() * (252 ** 0.5)
        sharpe = annual_ret / vol if vol > 0 else 0.0
        dd = _drawdown(res.equity)
        mdd = dd.min()

        bench = res.benchmark_equity
        bench_ret = None
        if bench is not None and not bench.empty:
            bench_ret = bench.iloc[-1] / bench.iloc[0] - 1.0

        print(f"  Total return: {total_ret*100:.2f}%")
        print(f"  Annualized: {annual_ret*100:.2f}%")
        print(f"  Vol (ann.): {vol*100:.2f}%")
        print(f"  Sharpe: {sharpe:.2f}")
        print(f"  Max drawdown: {mdd*100:.2f}%")
        if bench_ret is not None:
            print(f"  Benchmark (EW) total: {bench_ret*100:.2f}%")

        rows.append(
            {
                "strategy": name,
                "total_return": total_ret,
                "annual_return": annual_ret,
                "vol_annual": vol,
                "sharpe": sharpe,
                "max_drawdown": mdd,
                "benchmark_total_return": bench_ret,
                "n_days": n_days,
            }
        )

    if not rows:
        print("No strategy results to save.")
        return

    df_out = pd.DataFrame(rows)
    out_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), args.out)
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df_out.to_csv(out_path, index=False)
    print(f"\nSummary saved to {out_path}")
    print(df_out)


if __name__ == "__main__":
    main()

