import argparse

from data_layer.daily_csv_store import load_daily
from strategies.ma_cross import ma_cross_target_position
from backtest.engine import run_long_only_backtest


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--ts-code", required=True, help='e.g. "600519.SH"')
    p.add_argument("--fast", type=int, default=20)
    p.add_argument("--slow", type=int, default=60)
    p.add_argument("--fee-bps", type=float, default=2.0)
    p.add_argument("--plot", action="store_true", help="show matplotlib debug plots")
    args = p.parse_args()

    df = load_daily(args.ts_code)
    if df.empty:
        raise SystemExit(f"No data for {args.ts_code}")

    target = ma_cross_target_position(df, fast=args.fast, slow=args.slow)
    res = run_long_only_backtest(df["close"], target, fee_bps=args.fee_bps)

    print(f"=== Backtest {args.ts_code} MA{args.fast}/{args.slow} ===")
    print(f"Start: {res.equity.index.min().date()}  End: {res.equity.index.max().date()}")
    print(f"Equity: {res.equity.iloc[-1]:,.2f}")
    print(f"Total return: {(res.equity.iloc[-1] / res.equity.iloc[0] - 1) * 100:.2f}%")

    if args.plot:
        import matplotlib.pyplot as plt

        price = df["close"]
        ma_fast = price.rolling(args.fast).mean()
        ma_slow = price.rolling(args.slow).mean()

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), sharex=True)

        ax1.plot(price.index, price.values, label="Close", color="black", linewidth=1)
        ax1.plot(ma_fast.index, ma_fast.values, label=f"MA{args.fast}", color="blue", linewidth=1)
        ax1.plot(ma_slow.index, ma_slow.values, label=f"MA{args.slow}", color="orange", linewidth=1)
        ax1.set_title(f"{args.ts_code} Price & MAs")
        ax1.legend(loc="upper left")
        ax1.grid(True, alpha=0.3)

        ax2.plot(res.equity.index, res.equity.values, label="Equity", color="green")
        ax2.set_title("Equity Curve")
        ax2.legend(loc="upper left")
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    main()


