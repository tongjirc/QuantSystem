import os
import sys
import time
import random

import akshare as ak
import pandas as pd
import tushare as ts

# Allow running as: python downloader/csi500_daily_downloader.py
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import TUSHARE_TOKEN

# 避免本地代理（如 Shadowsocks / Surge 等）干扰 HTTP 请求：
# - 清理常见代理环境变量
# - 设置 NO_PROXY="*"
for _k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"):
    os.environ.pop(_k, None)
os.environ.setdefault("NO_PROXY", "*")

# 默认将日线数据保存到 data/raw，兼容 data_layer.daily_csv_store
DATA_PATH = os.path.join("data", "raw")
os.makedirs(DATA_PATH, exist_ok=True)

# 板块成份映射目录
BOARD_DIR = os.path.join("data", "board")
os.makedirs(BOARD_DIR, exist_ok=True)

# Tushare 客户端（用于在 AkShare 多次失败后作为兜底）
ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()


# ====== 读取中证500股票列表 ======
def load_stock_list() -> list[str]:
    df = pd.read_csv(os.path.join("data", "csi500_list.csv"))
    print("当前列名:", df.columns)

    code_col = df.columns[0]  # 自动取第一列作为代码
    codes = df[code_col].astype(str).tolist()

    ts_codes: list[str] = []
    for code in codes:
        code = code.strip()
        if not code:
            continue
        if code.startswith("6"):
            ts_codes.append(code + ".SH")
        else:
            ts_codes.append(code + ".SZ")
    return ts_codes


def _ts_code_to_ak_symbol(ts_code: str) -> str:
    """
    600519.SH -> 600519
    000001.SZ -> 000001
    """
    return ts_code.split(".")[0]


# ====== AkShare: 单只股票日线，多数据源尝试 ======
def _fetch_stock_daily_ak(symbol: str, start_date: str) -> pd.DataFrame:
    """
    使用 AkShare 默认数据源获取日线数据。
    返回原始 DataFrame（尚未标准化列名）。失败时返回空 DataFrame。
    """
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=start_date,
            end_date="20251231",
            adjust="",
        )
        if df is not None and not df.empty:
            return df
    except Exception as e:
        msg = str(e)
        print(f"  AkShare 默认源出错: {msg}")
    return pd.DataFrame()


# ====== 下载单只股票日线（AkShare + Tushare 兜底） ======
def download_stock_daily(ts_code: str, max_retries: int = 5) -> None:
    file_path = os.path.join(DATA_PATH, f"{ts_code}.csv")

    # 判断是否已有历史数据，决定全量 or 增量
    last_date: str | None = None
    existing: pd.DataFrame | None = None
    if os.path.exists(file_path):
        try:
            existing = pd.read_csv(file_path)
            if "trade_date" in existing.columns and not existing.empty:
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
    symbol = _ts_code_to_ak_symbol(ts_code)

    for attempt in range(1, max_retries + 1):
        try:
            df = _fetch_stock_daily_ak(symbol, start_date=start_date)
            if df.empty:
                if last_date:
                    print(f"{ts_code} AkShare 无新增数据（>= {start_date}）")
                else:
                    print(f"{ts_code} AkShare 无数据")
                # 不立即放弃，交给兜底逻辑
                break

            # 标准化为内部日线格式
            df = df.rename(
                columns={
                    "日期": "trade_date",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "收盘": "close",
                    "成交量": "vol",
                    "成交额": "amount",
                }
            )
            df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "")
            df = df.sort_values("trade_date")
            df.insert(0, "ts_code", ts_code)

            if existing is not None and last_date:
                combined = pd.concat([existing, df], ignore_index=True)
                combined = combined.drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
                combined.to_csv(file_path, index=False)
                print(f"{ts_code} 增量更新完成，新增 {len(df)} 行，总计 {len(combined)} 行")
            else:
                df.to_csv(file_path, index=False)
                print(f"{ts_code} 全量下载完成，共 {len(df)} 行")

            # 关键：随机延时 3~10 秒（降低频率，模拟人类行为）
            sleep_time = random.uniform(3.0, 10.0)
            print(f"{ts_code} 下载完成，等待 {sleep_time:.1f} 秒再请求下一只...")
            time.sleep(sleep_time)
            return

        except Exception as e:
            msg = str(e)
            print(f"{ts_code} AkShare 出错，第 {attempt}/{max_retries} 次尝试: {msg}")
            # 重试也要降频：指数退避 + 随机 3~10 秒
            backoff = min(10, 2 ** attempt)
            sleep_time = random.uniform(3.0, 10.0) + backoff
            print(f"{ts_code} 本次出错后等待 {sleep_time:.1f} 秒再重试...")
            time.sleep(sleep_time)

    print(f"{ts_code} AkShare 最多重试 {max_retries} 次仍失败，尝试使用 Tushare 兜底")

    # --------- AkShare 多次失败后，尝试用 Tushare 作为兜底数据源 ---------
    try:
        df_ts = pro.daily(ts_code=ts_code, start_date=start_date, end_date="20251231")
        if df_ts.empty:
            print(f"{ts_code} Tushare daily 无数据，最终跳过该股票")
            return
        df_ts = df_ts.sort_values("trade_date")
        # 统一字段名，保证与 AkShare 日线兼容
        keep_cols = ["ts_code", "trade_date", "open", "high", "low", "close", "vol", "amount"]
        for c in keep_cols:
            if c not in df_ts.columns:
                # Tushare 日线中 vol/amount 列名为 vol/amount，本身就匹配
                pass
        df_ts = df_ts[keep_cols]
        if existing is not None and last_date:
            combined = pd.concat([existing, df_ts], ignore_index=True)
            combined = combined.drop_duplicates(subset=["trade_date"]).sort_values("trade_date")
            combined.to_csv(file_path, index=False)
            print(f"{ts_code} 使用 Tushare 兜底增量更新，新增 {len(df_ts)} 行，总计 {len(combined)} 行")
        else:
            df_ts.to_csv(file_path, index=False)
            print(f"{ts_code} 使用 Tushare 兜底全量下载完成，共 {len(df_ts)} 行")
    except Exception as e:
        print(f"{ts_code} Tushare 兜底失败，最终跳过该股票: {e}")


def _download_index_daily() -> None:
    """
    下载常见大盘指数日线（含成交量/成交额），例如上证综指、深成指、沪深300。
    """
    index_map = {
        "sh000001": "SSE_Composite",
        "sz399001": "SZSE_Component",
        "sh000300": "CSI_300",
    }
    for symbol, name in index_map.items():
        print(f"\n下载指数日线: {symbol} ({name})")
        df = pd.DataFrame()
        try:
            # 1) 默认东财源
            df = ak.stock_zh_index_daily(symbol=symbol)
        except Exception as e:
            print(f"  指数 {symbol} 默认源失败: {e}")

        if df.empty:
            # 2) 备选：csindex 源（使用短代码）
            try:
                short = symbol[-6:]
                df = ak.index_zh_a_hist(symbol=short, period="daily", start_date="20100101", end_date="20251231", source="csindex")
            except Exception as e:
                print(f"  指数 {symbol} csindex 源失败: {e}")

        if df.empty:
            print(f"  指数 {symbol} 所有数据源均失败或无数据")
            continue

        if "date" in df.columns:
            df = df.rename(
                columns={
                    "date": "trade_date",
                    "open": "open",
                    "high": "high",
                    "low": "low",
                    "close": "close",
                    "volume": "vol",
                    "amount": "amount",
                }
            )
        else:
            df = df.rename(
                columns={
                    "日期": "trade_date",
                    "开盘": "open",
                    "最高": "high",
                    "最低": "low",
                    "收盘": "close",
                    "成交量": "vol",
                    "成交额": "amount",
                }
            )
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")
        df.insert(0, "ts_code", symbol)
        out_path = os.path.join(DATA_PATH, f"index_{symbol}.csv")
        df.to_csv(out_path, index=False)
        print(f"  指数 {symbol} 日线保存至 {out_path}，共 {len(df)} 行")
        sleep_time = random.uniform(3.0, 10.0)
        print(f"  指数 {symbol} 下载后等待 {sleep_time:.1f} 秒...")
        time.sleep(sleep_time)


def _download_board_daily(max_boards: int = 10) -> None:
    """
    尝试下载部分行业板块指数日线（含成交量/成交额），用于板块层面的成交量分析。
    为避免限流，只取前 max_boards 个板块。
    """
    try:
        boards = ak.stock_board_industry_name_em()
    except Exception as e:
        print(f"\n获取板块列表失败: {e}")
        return
    codes = boards["板块代码"].astype(str).tolist()[:max_boards]
    print(f"\n下载前 {len(codes)} 个行业板块指数日线")

    members_rows: list[dict[str, str]] = []

    for code in codes:
        print(f"  板块 {code}")
        try:
            df = ak.stock_board_industry_index_ths(symbol=code, start_date="20100101", end_date="20251231")
        except Exception as e:
            print(f"    下载失败: {e}")
            continue
        if df.empty:
            print("    无数据")
            continue
        df = df.rename(
            columns={
                "日期": "trade_date",
                "开盘": "open",
                "最高": "high",
                "最低": "low",
                "收盘": "close",
                "成交量": "vol",
                "成交额": "amount",
            }
        )
        df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "")
        df.insert(0, "ts_code", code)
        out_path = os.path.join(DATA_PATH, f"board_{code}.csv")
        df.to_csv(out_path, index=False)
        print(f"    板块 {code} 日线保存至 {out_path}，共 {len(df)} 行")
        sleep_time = random.uniform(3.0, 10.0)
        print(f"    板块 {code} 下载后等待 {sleep_time:.1f} 秒...")
        time.sleep(sleep_time)

        # 板块成份股映射（用于板块中性 / 行业中性策略）
        try:
            cons = ak.stock_board_industry_cons_em(symbol=code)
        except Exception as e:
            print(f"    板块 {code} 成份股获取失败: {e}")
            continue
        if cons.empty or "代码" not in cons.columns:
            print(f"    板块 {code} 成份股数据为空或缺少 '代码' 列")
            continue
        for raw in cons["代码"].astype(str):
            raw = raw.strip()
            if not raw:
                continue
            if raw.startswith("6"):
                ts_code = raw + ".SH"
            else:
                ts_code = raw + ".SZ"
            members_rows.append({"ts_code": ts_code, "board_code": code})

    if members_rows:
        import pandas as _pd  # 局部导入避免命名冲突

        members_df = _pd.DataFrame(members_rows).drop_duplicates()
        members_path = os.path.join(BOARD_DIR, "board_members.csv")
        members_df.to_csv(members_path, index=False)
        print(f"\n板块成份映射已保存至 {members_path}，共 {len(members_df)} 行")


def _download_minute_sample(ts_codes: list[str], period: str = "1", max_symbols: int = 5) -> None:
    """
    抽样下载部分成分股的分钟线数据，验证 AkShare 分钟数据接口。
    为控制体量，仅下载前 max_symbols 个标的。
    """
    sample = ts_codes[:max_symbols]
    print(f"\n抽样下载 {len(sample)} 只股票的 {period} 分钟线")

    for ts_code in sample:
        symbol = _ts_code_to_ak_symbol(ts_code)
        print(f"  分钟线: {ts_code} ({symbol}), period={period}")
        try:
            df = ak.stock_zh_a_minute(symbol=symbol, period=period, adjust="")
        except Exception as e:
            print(f"    下载失败: {e}")
            continue
        if df.empty:
            print("    无数据")
            continue
        # 原始列：day / time / open / close / high / low / volume / amount
        df = df.rename(
            columns={
                "day": "trade_date",
                "time": "time",
                "open": "open",
                "high": "high",
                "low": "low",
                "close": "close",
                "volume": "vol",
                "amount": "amount",
            }
        )
        df["trade_date"] = df["trade_date"].astype(str).str.replace("-", "")
        df.insert(0, "ts_code", ts_code)
        out_path = os.path.join(DATA_PATH, f"minute_{ts_code}_{period}min.csv")
        df.to_csv(out_path, index=False)
        print(f"    分钟线保存至 {out_path}，共 {len(df)} 行")
        time.sleep(0.8)


def main() -> None:
    stock_list = load_stock_list()
    total = len(stock_list)

    # 1) CSI500 成分股日线（含成交量/成交额）
    for i, ts_code in enumerate(stock_list):
        print(f"\n进度 {i + 1}/{total}")
        download_stock_daily(ts_code)

    # 2) 大盘指数日线（大盘成交量）
    _download_index_daily()

    # 3) 行业板块指数日线（板块成交量，抽样若干板块）
    _download_board_daily(max_boards=10)

    # 4) 抽样分钟线（单支股票成交量-分钟级）
    _download_minute_sample(stock_list, period="1", max_symbols=5)


if __name__ == "__main__":
    main()
