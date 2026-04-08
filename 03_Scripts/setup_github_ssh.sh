#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# setup_github_ssh.sh
# 在腾讯云服务器上配置 GitHub SSH 访问（走 443 端口绕开封锁）
#
# 用法:
#   bash 03_Scripts/setup_github_ssh.sh
#
# 完成后需要手动把打印的公钥添加到 GitHub:
#   Settings → Deploy keys → Add deploy key (勾选 Allow write access 如需推送)
#   或 Settings → SSH and GPG keys → New SSH key
# ──────────────────────────────────────────────────────────────────────
set -Eeuo pipefail

SSH_KEY_FILE="${SSH_KEY_FILE:-$HOME/.ssh/github_deploy_ed25519}"
SSH_CONFIG_FILE="$HOME/.ssh/config"

echo "[1/4] 生成 SSH 密钥 …"
if [[ -f "$SSH_KEY_FILE" ]]; then
  echo "  密钥已存在: $SSH_KEY_FILE（跳过生成）"
else
  mkdir -p "$HOME/.ssh"
  chmod 700 "$HOME/.ssh"
  ssh-keygen -t ed25519 -C "tencent-deploy@$(hostname)" -f "$SSH_KEY_FILE" -N ""
  echo "  密钥已生成: $SSH_KEY_FILE"
fi

echo
echo "[2/4] 配置 SSH 走 443 端口 …"
# 检查是否已有 github.com 的 Host 配置
if grep -q 'Host github.com' "$SSH_CONFIG_FILE" 2>/dev/null; then
  echo "  $SSH_CONFIG_FILE 中已有 github.com 配置（跳过）"
  echo "  请确认包含 Hostname ssh.github.com 和 Port 443"
else
  cat >> "$SSH_CONFIG_FILE" <<EOF

# ── GitHub SSH over port 443 (bypass China firewall) ──
Host github.com
    Hostname ssh.github.com
    Port 443
    User git
    IdentityFile $SSH_KEY_FILE
    IdentitiesOnly yes
    ServerAliveInterval 60
    ServerAliveCountMax 3
EOF
  chmod 600 "$SSH_CONFIG_FILE"
  echo "  已写入 SSH 配置"
fi

echo
echo "[3/4] 配置 git 全局设置 …"
git config --global http.version HTTP/1.1
git config --global http.postBuffer 524288000
git config --global core.compression 0
echo "  git HTTP/1.1 + 大缓冲区已配置"

echo
echo "[4/4] 测试 GitHub SSH 连接 …"
echo "  (通过 ssh.github.com:443)"
if ssh -T git@github.com -o ConnectTimeout=10 2>&1 | grep -qi 'success\|authenticated'; then
  echo "  ✅ SSH 连接成功！"
else
  echo "  ⚠️  SSH 连接未确认成功（可能密钥尚未添加到 GitHub）"
fi

echo
echo "════════════════════════════════════════════════════════"
echo "  请复制以下公钥，添加到 GitHub:"
echo "  https://github.com/tristan419/JATO_Analysis_System/settings/keys"
echo "════════════════════════════════════════════════════════"
echo
cat "${SSH_KEY_FILE}.pub"
echo
echo "════════════════════════════════════════════════════════"
echo
echo "  添加后，在服务器运行以下命令验证:"
echo "    ssh -T git@github.com"
echo
echo "  然后克隆仓库（SSH方式）:"
echo "    git clone --depth 1 git@github.com:tristan419/JATO_Analysis_System.git /opt/JATO_Analysis_System"
echo
echo "  后续 git pull 就走 SSH 443 端口，稳定快速。"
