#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
APP_FILE="$ROOT_DIR/05_DashBoard/app.py"
PORT="${1:-8501}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "[ERROR] 未找到 Python 虚拟环境: $PYTHON_BIN"
  echo "请先在项目根目录创建 .venv 并安装依赖。"
  exit 1
fi

if [[ ! -f "$APP_FILE" ]]; then
  echo "[ERROR] 未找到 Dashboard 入口文件: $APP_FILE"
  exit 1
fi

PATTERN="streamlit run .*05_DashBoard/app.py|python -m streamlit run .*05_DashBoard/app.py"
EXISTING_PIDS="$(pgrep -f "$PATTERN" || true)"

if [[ -n "$EXISTING_PIDS" ]]; then
  echo "[INFO] 停止已有 Streamlit 进程: $EXISTING_PIDS"
  kill $EXISTING_PIDS || true
  sleep 1
fi

REMAINING_PIDS="$(pgrep -f "$PATTERN" || true)"
if [[ -n "$REMAINING_PIDS" ]]; then
  echo "[WARN] 强制停止残留进程: $REMAINING_PIDS"
  kill -9 $REMAINING_PIDS || true
  sleep 1
fi

echo "[INFO] 启动 Dashboard: http://127.0.0.1:${PORT}"
cd "$ROOT_DIR"
exec "$PYTHON_BIN" -m streamlit run "$APP_FILE" \
  --server.address 127.0.0.1 \
  --server.port "$PORT" \
  --server.runOnSave true
