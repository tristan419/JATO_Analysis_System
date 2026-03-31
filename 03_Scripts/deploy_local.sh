#!/bin/bash
# 本地部署脚本 - 提交并推送代码到 GitHub

set -e

echo "=========================================="
echo "本地部署到 GitHub"
echo "=========================================="

# 检查是否有未提交的更改
if [[ -z $(git status -s) ]]; then
  echo "没有需要提交的更改"
  exit 0
fi

# 显示更改
echo ""
echo "待提交的更改:"
git status -s

# 询问提交信息
echo ""
read -p "请输入提交信息: " commit_msg

if [[ -z "$commit_msg" ]]; then
  echo "[ERROR] 提交信息不能为空"
  exit 1
fi

# 提交并推送
echo ""
echo "[1/3] 添加所有更改..."
git add -A

echo "[2/3] 提交更改..."
git commit -m "$commit_msg"

echo "[3/3] 推送到 GitHub..."
git push JATO_Analysis_System main

echo ""
echo "=========================================="
echo "✓ 部署成功！"
echo "=========================================="
echo ""
echo "下一步: 在服务器上运行更新脚本"
echo "ssh 到服务器后执行:"
echo "  cd /var/www/JATO_Analysis_System"
echo "  bash 03_Scripts/deploy_server.sh"
