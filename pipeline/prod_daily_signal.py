import argparse
import datetime as dt
from typing import List, Dict

import pandas as pd

from data_layer.daily_csv_store import list_available_ts_codes, load_daily
from portfolio.positions import load_positions
from sender.feishu_sender import send_by_feishu
from strategies.ma_cross import ma_cross_target_position


def _as_yyyymmdd(d: dt.date) -> str:
    return d.strftime("%Y%m%d")


def _compute_momentum(df: pd.DataFrame) -> float:
    """Simple momentum: close_T / close_0 - 1 over lookback window."""
    close = df["close"].dropna()
    if close.empty:
        return 0.0
    return float(close.iloc[-1] / close.iloc[0] - 1.0)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--fast", type=int, default=20)
    p.add_argument("--slow", type=int, default=60)
    p.add_argument("--lookback-days", type=int, default=260)
    p.add_argument("--top-n", type=int, default=10, help="number of BUY candidates to keep")
    p.add_argument(
        "--max-weight",
        type=float,
        default=0.1,
        help="per-name max target weight (0-1, e.g. 0.1=10%)",
    )
    p.add_argument("--send", action="store_true", help="send summary by Feishu")
    args = p.parse_args()

    pos = load_positions()
    ts_codes = list_available_ts_codes()
    if not ts_codes:
        raise SystemExit("No raw data found under data/raw")

    today = dt.date.today()
    start = today - dt.timedelta(days=int(args.lookback_days))
    start_date = _as_yyyymmdd(start)

    buy_candidates: List[Dict] = []
    sell_candidates: List[Dict] = []

    for ts_code in ts_codes:
        try:
            df = load_daily(ts_code, start_date=start_date, columns=["close"])
            if df.empty or df["close"].dropna().empty:
                continue

            target = ma_cross_target_position(df, fast=args.fast, slow=args.slow)
            latest_target = int(target.iloc[-1])
            holding_qty = pos.get_qty(ts_code)
            holding = 1 if holding_qty > 0 else 0

            momentum = _compute_momentum(df)

            if latest_target == 1 and holding == 0:
                buy_candidates.append(
                    {"ts_code": ts_code, "score": momentum, "holding_qty": holding_qty}
                )
            elif latest_target == 0 and holding == 1:
                sell_candidates.append(
                    {"ts_code": ts_code, "score": momentum, "holding_qty": holding_qty}
                )
        except Exception:
            continue

    buy_candidates.sort(key=lambda x: x["score"], reverse=True)
    top_n = max(0, min(args.top_n, len(buy_candidates)))

    lines: List[str] = []
    header = f"Daily signal (MA{args.fast}/{args.slow})  {today.isoformat()}"

    if top_n == 0 and not sell_candidates:
        body = "No action."
    else:
        if top_n > 0:
            base_w = 1.0 / float(top_n)
            w = min(base_w, float(args.max_weight))
        else:
            w = 0.0

        if top_n > 0:
            lines.append(f"BUY (Top {top_n}, target_weight≈{w*100:.1f}% each):")
            for i in range(top_n):
                c = buy_candidates[i]
                lines.append(
                    f"  BUY {c['ts_code']}  w={w*100:.1f}%  momentum={c['score']*100:.1f}%"
                )

        if sell_candidates:
            lines.append("")
            lines.append("SELL (exit existing holdings):")
            for c in sell_candidates:
                lines.append(
                    f"  SELL {c['ts_code']}  qty={c['holding_qty']:.0f}  momentum={c['score']*100:.1f}%"
                )

        body = "\n".join(lines)

    msg = f"{header}\n\n{body}"

    print(msg)
    if args.send:
        ok = send_by_feishu(msg)
        if not ok:
            raise SystemExit("Feishu send failed (see sender logs)")


if __name__ == "__main__":
    main()


