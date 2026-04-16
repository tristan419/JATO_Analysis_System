#!/bin/bash
# 腾讯云诊断脚本 - 一键运行所有诊断

set -e

echo "=========================================="
echo "JATO Dashboard 诊断工具"
echo "=========================================="

# 1. 拉取最新代码
echo ""
echo "[1/4] 拉取最新代码..."
git fetch origin
git pull origin main

# 2. 激活虚拟环境
echo ""
echo "[2/4] 激活虚拟环境..."
source venv/bin/activate

# 3. 检查主题配置
echo ""
echo "[3/4] 检查主题配置..."
python 03_Scripts/diagnostics/check_theme_config.py

# 4. 运行性能诊断
echo ""
echo "[4/4] 运行性能诊断..."
python 03_Scripts/diagnostics/diagnose_performance.py

echo ""
echo "=========================================="
echo "诊断完成！"
echo "=========================================="
