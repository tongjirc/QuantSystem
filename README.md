# 量化交易系统 (Quant Trading System)

A股量化交易系统，支持策略开发、回测、实盘信号生成。

## 项目结构

```
quant_system/
├── config/          # 配置文件 (API keys, 参数)
├── strategies/      # 交易策略
├── backtest/       # 回测引擎
├── data/           # 数据存储
├── data_layer/     # 数据读写层
├── downloader/     # 数据下载 (A股行情)
├── features/       # 特征工程 (技术指标)
├── pipeline/       # 信号生产流水线
├── portfolio/      # 仓位管理
├── sender/        # 消息推送 (飞书)
└── run.sh         # 运行脚本
```

## 快速开始

### 1. 安装依赖

```bash
cd quant_system
pip install -r requirements.txt
```

### 2. 配置

编辑 `config/settings.py`，填入你的API密钥：

- Tushare Token
- 飞书 APP_ID / APP_SECRET
- API Keys (Minimax/DeepSeek/Gemini)

### 3. 运行

```bash
# 回测
python -m pipeline.dev_backtest

# 生产信号
python -m pipeline.prod_daily_signal
```

## 模块说明

| 模块 | 功能 |
|------|------|
| strategies | 策略逻辑 (MA Cross等) |
| backtest | 回测引擎 |
| data | CSV行情数据存储 |
| downloader | A股数据下载 (Tushare) |
| features | 技术指标计算 |
| pipeline | 信号生成流水线 |
| portfolio | 仓位管理 |
| sender | 飞书消息推送 |

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

- A股行情: Tushare (需要Token)
- 本地存储: CSV格式

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
