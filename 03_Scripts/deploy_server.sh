#!/bin/bash
# 服务器更新脚本 - 拉取最新代码并重启 Dashboard

set -e

echo "=========================================="
echo "服务器更新部署"
echo "=========================================="

# 检查是否在正确的目录
if [[ ! -f "03_Scripts/restart_dashboard.sh" ]]; then
  echo "[ERROR] 请在项目根目录运行此脚本"
  exit 1
fi

# 停止当前运行的 Dashboard
echo ""
echo "[1/4] 停止当前 Dashboard..."
pkill -f "streamlit run.*app.py" || echo "没有运行中的 Dashboard"

# 拉取最新代码
echo ""
echo "[2/4] 拉取最新代码..."
git fetch origin main
git reset --hard origin/main

# 显示最新提交
echo ""
echo "最新提交:"
git log --oneline -3

# 重启 Dashboard
echo ""
echo "[3/4] 重启 Dashboard..."
bash 03_Scripts/restart_dashboard.sh

# 等待启动
echo ""
echo "[4/4] 等待 Dashboard 启动..."
sleep 3

echo ""
echo "=========================================="
echo "✓ 更新部署成功！"
echo "=========================================="
echo ""
echo "Dashboard 已启动，访问: http://服务器IP:8501"
