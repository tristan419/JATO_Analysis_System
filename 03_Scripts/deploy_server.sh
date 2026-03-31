#!/bin/bash
# 服务器更新脚本 - 拉取最新代码并重启 Dashboard

set -e

echo "=========================================="
echo "服务器更新部署"
echo "=========================================="

# 拉取最新代码
echo ""
echo "[1/4] 拉取最新代码..."
git fetch origin main
git reset --hard origin/main

# 显示最新提交
echo ""
echo "最新提交:"
git log --oneline -3

# 重新加载 systemd 配置
echo ""
echo "[2/4] 重新加载 systemd 配置..."
sudo systemctl daemon-reload

# 重启服务
echo ""
echo "[3/4] 重启 Dashboard 服务..."
if systemctl is-active --quiet jato.service; then
  sudo systemctl restart jato.service
  SERVICE_NAME="jato.service"
elif systemctl is-active --quiet jato-dashboard@8501.service; then
  sudo systemctl restart jato-dashboard@8501.service
  SERVICE_NAME="jato-dashboard@8501.service"
else
  echo "[ERROR] 未找到运行中的服务"
  echo "请手动启动: sudo systemctl start jato-dashboard@8501.service"
  exit 1
fi

# 查看服务状态
echo ""
echo "[4/4] 服务状态:"
sudo systemctl status $SERVICE_NAME --no-pager -l

echo ""
echo "=========================================="
echo "✓ 更新部署成功！"
echo "=========================================="
echo ""
echo "查看日志: sudo journalctl -u $SERVICE_NAME -f"
