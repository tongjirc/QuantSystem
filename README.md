# 量化交易系统 (Quant Trading System)

A股量化交易系统，支持策略开发、回测、实盘信号生成。

## 项目结构

```text
quant_system/
├── config/          # 配置文件 (API keys, 参数、策略/回测超参数)
├── strategies/      # 交易策略（动量、多技术指标等）
├── backtest/        # 回测引擎（单标的 / 组合）
├── data/            # 数据存储（raw/ backtest/ 等）
├── data_layer/      # 数据读写层（价格/收益面板）
├── downloader/      # 数据下载 (AkShare 为主，兼容 Tushare)
├── features/        # 特征工程 (技术指标)
├── pipeline/        # 信号生产流水线（回测 / 生产）
├── portfolio/       # 仓位管理
├── tools/           # 数据质量检查、数据源对比等工具
├── sender/          # 消息推送 (飞书)
└── run.sh           # 统一运行脚本
```

## 快速开始

### 1. 安装依赖

```bash
cd quant_system
pip install -r requirements.txt
```

### 2. 配置

编辑 `config/settings.py`，填入你的密钥与参数：

- **数据源**：Tushare Token（如仍使用 Tushare）
- **消息推送**：飞书 APP_ID / APP_SECRET
- **大模型**：Minimax / DeepSeek / Gemini API Key（可选）
- **策略 / 回测参数**：如 `MOMENTUM_LOOKBACK`、`TOP_N`、`FEE_BPS` 等

### 3. 常用命令

使用 `run.sh` 管理常见任务（默认在 `quant_system` 根目录执行）：

```bash
# 仅激活虚拟环境
source run.sh

# 仅做数据增量下载（AkShare / Tushare）
./run.sh -l

# 开发模式：下载 + 单只标的回测示例
./run.sh -d

# 生产模式：下载 + 当日全市场信号
./run.sh -p

# 组合回测：下载 + 组合层动量/技术策略回测 + 可视化
./run.sh -b -- --start 20200901 --plot
```

## 模块说明

| 模块 | 功能 |
|------|------|
| `strategies` | 策略逻辑（MA Cross、动量组合、MACD/布林带/RSI 等） |
| `backtest`   | 回测引擎（单标的 & 组合，多策略接口） |
| `data`       | 本地 CSV 行情数据、回测结果图表等 |
| `downloader` | A 股数据下载（默认 AkShare，可扩展 Tushare） |
| `data_layer` | 构建价格/收益面板、字段面板（close / vol / amount 等） |
| `features`   | 各类技术指标计算 |
| `pipeline`   | 开发回测 / 组合回测 / 生产信号流水线 |
| `portfolio`  | 仓位管理与持仓记录 |
| `sender`     | 飞书等消息推送 |
| `tools`      | 数据质量检查、数据源对比（如 Tushare vs AkShare） |

## 策略示例

### MA Cross (移动平均线交叉)

```python
from strategies.ma_cross import ma_cross_target_position

# 生成信号
signal = ma_cross_target_position(df, fast=20, slow=60)
```

## 回测示例

```python
from backtest.engine import run_long_only_backtest

result = run_long_only_backtest(
    price=price_series,
    target_pos=target_positions,
    fee_bps=2.0,  # 万分之2手续费
    initial_capital=1_000_000
)
```

## 数据来源

- **A 股行情**：AkShare（推荐，免费、无需 Token）  
  - 日线：`stock_zh_a_hist`（支持前/后复权与不复权）  
  - 分钟线：`stock_zh_a_minute` / `stock_zh_a_hist_min_em`  
  - 指数 & 板块：`stock_zh_index_daily`、`stock_board_industry_index_ths` 等  
- **备用数据源**：Tushare Pro（需要 Token，部分脚本兼容）
- **本地存储**：`data/raw/*.csv` 为原始行情，`data/backtest/` 为回测输出

## 开发

```bash
# 激活虚拟环境
source venv/bin/activate

# 运行测试
python -m pytest tests/
```

## 注意事项

- 请勿提交 `config/settings.py` 到Git (已配置.gitignore)
- 生产环境请使用环境变量管理敏感信息
- 实盘交易前请务必进行充分回测
