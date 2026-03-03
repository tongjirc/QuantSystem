import akshare as ak
import pandas as pd
import os

os.makedirs("data", exist_ok=True)

# 获取中证500成分股
df = ak.index_stock_cons(symbol="000905")

print("原始列名:", df.columns)

# 只保留需要列
df = df[["品种代码", "品种名称"]]

# 保存
df.to_csv("data/csi500_list.csv", index=False)

print("中证500成分股已保存到 data/csi500_list.csv")
print("共", len(df), "只股票")