import os
import sys
import time
import tushare as ts
import pandas as pd

# Allow running as: python downloader/csi500_daily_downloader.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import TUSHARE_TOKEN

ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

DATA_PATH = os.path.join("data", "raw", "csi500")
os.makedirs(DATA_PATH, exist_ok=True)

# ====== 读取中证500股票列表 ======
def load_stock_list():
    df = pd.read_csv("data/csi500_list.csv")
    
    print("当前列名:", df.columns)

    code_col = df.columns[0]  # 自动取第一列作为代码
    codes = df[code_col].astype(str).tolist()

    ts_codes = []
    for code in codes:
        if code.startswith("6"):
            ts_codes.append(code + ".SH")
        else:
            ts_codes.append(code + ".SZ")

    return ts_codes

# ====== 下载函数 ======
def download_stock(ts_code, max_retries=5):
    file_path = f"{DATA_PATH}/{ts_code}.csv"

    # 先判断是否已有历史数据，决定是全量还是增量
    last_date = None
    if os.path.exists(file_path):
        try:
            existing = pd.read_csv(file_path)
            if "trade_date" in existing.columns and not existing.empty:
                # 已有数据的最新交易日
                last_date_str = str(existing["trade_date"].max())
                last_dt = pd.to_datetime(last_date_str, format="%Y%m%d")
                next_dt = last_dt + pd.Timedelta(days=1)
                last_date = next_dt.strftime("%Y%m%d")
                print(f"{ts_code} 增量更新，已至 {last_date_str}，本次起始 {last_date}")
            else:
                print(f"{ts_code} 本地文件无有效 trade_date 列，按全量重新下载")
        except Exception as e:
            print(f"{ts_code} 读取本地文件失败，按全量重新下载: {e}")

    start_date = last_date or "20100101"

    for attempt in range(1, max_retries + 1):
        try:
            df = pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date="20251231"
            )

            if df.empty:
                if last_date:
                    print(f"{ts_code} 无新增数据（>= {start_date}）")
                else:
                    print(f"{ts_code} 无数据")
                return

            df = df.sort_values("trade_date")

            if os.path.exists(file_path) and last_date:
                # 仅追加新数据，去重
                existing = pd.read_csv(file_path)
                combined = pd.concat([existing, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
                combined.to_csv(file_path, index=False)
                print(f"{ts_code} 增量更新完成，新增 {len(df)} 行，总计 {len(combined)} 行")
            else:
                # 首次或无法识别旧数据，直接覆盖
                df.to_csv(file_path, index=False)
                print(f"{ts_code} 全量下载完成，共 {len(df)} 行")

            # 单个请求后固定等待，保证远低于 50 次/分钟
            time.sleep(1.5)
            return

        except Exception as e:
            msg = str(e)
            # 触发每分钟 50 次限制时，等待一整分钟后重试
            if "每分钟最多访问该接口50次" in msg:
                print(f"{ts_code} 触发频率限制，第 {attempt}/{max_retries} 次重试，等待 65 秒...")
                time.sleep(65)
                continue
            else:
                print(f"{ts_code} 出错（非频率限制），第 {attempt}/{max_retries} 次尝试: {e}")
                # 其它错误做指数退避，避免马上再次撞上同样问题
                backoff = min(30, 2 ** attempt)
                time.sleep(backoff)
        # 如果本轮没 return，就会进入下一轮循环重试

    print(f"{ts_code} 最多重试 {max_retries} 次仍失败，跳过该股票")

def main():
    stock_list = load_stock_list()
    total = len(stock_list)

    for i, ts_code in enumerate(stock_list):
        print(f"\n进度 {i+1}/{total}")
        download_stock(ts_code)

if __name__ == "__main__":
    main()