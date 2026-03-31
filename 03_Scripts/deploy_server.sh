#!/bin/bash
# 服务器更新脚本 - 拉取最新代码并重启 Dashboard
# 服务器路径: /var/www/JATO_Analysis_System

set -e

echo "=========================================="
echo "服务器更新部署"
echo "=========================================="

# 确认在正确的目录
if [[ "$PWD" != "/var/www/JATO_Analysis_System" ]]; then
  echo "[WARNING] 当前目录: $PWD"
  echo "[WARNING] 期望目录: /var/www/JATO_Analysis_System"
fi

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
if systemctl list-unit-files | grep -q "jato.service"; then
  SERVICE_NAME="jato.service"
elif systemctl list-unit-files | grep -q "jato-dashboard@.service"; then
  SERVICE_NAME="jato-dashboard@8501.service"
else
  echo "[ERROR] 未找到 systemd 服务配置"
  echo "请先安装服务: sudo cp 03_Scripts/deploy/systemd/jato-dashboard@.service /etc/systemd/system/"
  exit 1
fi

echo "重启服务: $SERVICE_NAME"
sudo systemctl restart $SERVICE_NAME

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
