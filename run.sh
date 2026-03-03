#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<EOF
Usage: run [-l | -d | -p]
  -l, --download
               只做数据增量下载
  -d, --dev    开发模式：先下载数据，再对单只标的跑回测示例
  -p, --prod   生产模式：先下载数据，再生成当天全市场信号（暂不发飞书）

示例：
  ./run.sh -l
  ./run.sh -d
  ./run.sh -p
  ./run.sh           # 仅激活虚拟环境
EOF
}

MODE="${1:-}"

# 默认行为：仅激活虚拟环境
if [[ -z "$MODE" ]]; then
  source venv/bin/activate
  echo "Virtualenv activated. You are now in quant_system venv."
  return 0 2>/dev/null || exit 0
fi

case "$MODE" in

  -l|--download)
    source venv/bin/activate
    python downloader/csi500_daily_downloader.py
    ;;

  -d|--dev)
    source venv/bin/activate
    python downloader/csi500_daily_downloader.py
    # 示例：对 600519.SH 跑一个 MA20/60 回测
    python -m pipeline.dev_backtest --ts-code 600519.SH
    ;;

  -p|--prod)
    source venv/bin/activate
    python downloader/csi500_daily_downloader.py
    # 只在终端打印当日建议（如需 Feishu 推送，可在命令后加 --send）
    python -m pipeline.prod_daily_signal
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
