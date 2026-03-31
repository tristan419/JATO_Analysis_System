#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# 支持 venv 和 .venv 两种虚拟环境目录
if [[ -x "$ROOT_DIR/venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/venv/bin/python"
elif [[ -x "$ROOT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
else
  echo "[ERROR] 未找到 Python 虚拟环境"
  echo "请确保存在 venv/ 或 .venv/ 目录"
  exit 1
fi

APP_FILE="$ROOT_DIR/05_DashBoard/app.py"
PORT="${1:-8501}"

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
export STREAMLIT_THEME_BASE="light"
export STREAMLIT_THEME_PRIMARY_COLOR="#2563EB"
export STREAMLIT_THEME_BACKGROUND_COLOR="#F8FAFC"
export STREAMLIT_THEME_SECONDARY_BACKGROUND_COLOR="#FFFFFF"
export STREAMLIT_THEME_TEXT_COLOR="#0F172A"
export STREAMLIT_THEME_FONT="sans serif"
exec "$PYTHON_BIN" -m streamlit run "$APP_FILE" \
  --server.address 127.0.0.1 \
  --server.port "$PORT" \
  --server.runOnSave true
