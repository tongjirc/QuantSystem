#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<EOF
Usage: run [-l | -d | -p | -b]
  -l, --download    只做数据增量下载
  -d, --dev         开发模式：先下载，再单只标的回测示例
  -p, --prod        生产模式：先下载，再当日全市场信号
  -b, --backtest    组合回测：先下载，再动量组合回测+可视化（--plot 可弹窗看图）

示例：
  ./run.sh -l
  ./run.sh -d
  ./run.sh -p
  ./run.sh -b              # 组合回测，图保存到 data/backtest/
  ./run.sh -b -- --plot    # 组合回测并弹窗显示图
  ./run.sh                 # 仅激活虚拟环境
EOF
}

MODE="${1:-}"

source venv/bin/activate

case "$MODE" in

  -l|--download)
    # 激活虚拟环境（始终从项目根目录下的 venv）
    source "$ROOT_DIR/venv/bin/activate"
    # 使用直连方式调用 AkShare，避免本机代理干扰
    env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY NO_PROXY="*" \
      python downloader/csi500_daily_downloader.py
    # 下载前复权因子（Tushare adj_factor），用于构建前复权价格
    python downloader/adj_factor_downloader.py
    ;;

  -d|--dev)
    source "$ROOT_DIR/venv/bin/activate"
    env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY NO_PROXY="*" \
      python downloader/csi500_daily_downloader.py
    python downloader/adj_factor_downloader.py
    # 示例：对 600519.SH 跑一个 MA20/60 回测
    python -m pipeline.dev_backtest --ts-code 600519.SH
    ;;

  -p|--prod)
    source "$ROOT_DIR/venv/bin/activate"
    env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY NO_PROXY="*" \
      python downloader/csi500_daily_downloader.py
    python downloader/adj_factor_downloader.py
    python -m pipeline.prod_daily_signal
    ;;

  -b|--backtest)
    source "$ROOT_DIR/venv/bin/activate"
    env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY NO_PROXY="*" \
      python downloader/csi500_daily_downloader.py
    python downloader/adj_factor_downloader.py
    # 默认使用前复权价格（--price-mode adj），其余参数允许从命令行透传
    python -m pipeline.portfolio_backtest --price-mode adj --save-only "${@:2}"
    ;;

  -h|--help)
    usage
    ;;

  *)
    echo "Unknown mode: $MODE"
    usage
    exit 1
    ;;
esac
