import os
import sys
import time

import pandas as pd
import tushare as ts

# Allow running as: python downloader/adj_factor_downloader.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import TUSHARE_TOKEN
from downloader.csi500_daily_downloader import load_stock_list

ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

ADJ_DIR = os.path.join("data", "adj_factor")
os.makedirs(ADJ_DIR, exist_ok=True)


def adj_factor_csv_path(ts_code: str) -> str:
    return os.path.join(ADJ_DIR, f"{ts_code}.csv")


def download_adj_factor(ts_code: str, max_retries: int = 5) -> None:
    """
    为单只股票下载全历史复权因子，保存为 data/adj_factor/{ts_code}.csv。
    """
    path = adj_factor_csv_path(ts_code)
    for attempt in range(1, max_retries + 1):
        try:
            df = pro.adj_factor(ts_code=ts_code, start_date="20100101", end_date="20251231")
            if df.empty:
                print(f"{ts_code} adj_factor: 无数据")
                return
            df = df.sort_values("trade_date")
            df.to_csv(path, index=False)
            print(f"{ts_code} adj_factor 下载完成，共 {len(df)} 行 -> {path}")
            # 控制频率，避免触发限流
            time.sleep(0.8)
            return
        except Exception as e:
            print(f"{ts_code} adj_factor 出错，第 {attempt}/{max_retries} 次尝试: {e}")
            backoff = min(30, 2 ** attempt)
            time.sleep(backoff)
    print(f"{ts_code} adj_factor 最多重试 {max_retries} 次仍失败，跳过该股票")


def main() -> None:
    stock_list = load_stock_list()
    total = len(stock_list)
    for i, ts_code in enumerate(stock_list):
        print(f"\n[adj_factor] 进度 {i + 1}/{total}: {ts_code}")
        download_adj_factor(ts_code)


if __name__ == "__main__":
    main()

