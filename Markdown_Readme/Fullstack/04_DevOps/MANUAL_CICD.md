# JATO Fullstack CI/CD 手册（最终版）

> 更新时间：2026-04-09
>
> 本文档是推送与部署的**唯一参考**。服务器初始化和环境搭建请看 `TENCENT_CLOUD_DEPLOY.md`。

---

## 0. 推送方式速查

| 方式 | 命令 | 何时选用 |
|------|------|----------|
| **HTTPS（推荐）** | `git push JATO_Analysis_System main` | 默认方式，依赖 macOS 钥匙串中的凭据 |
| SSH 标准端口 | `git push` (remote = `git@github.com:...`) | 网络允许 22 端口出站时 |
| SSH 443 端口 | `git push` (remote = `git@github-443:...`) | 22 被封但 443 SSH 握手正常时 |

**当前状态（2026-04-09）**：本机 remote `JATO_Analysis_System` 已切为 HTTPS，推送正常。
如果将来需要切回 SSH，执行：

```bash
git remote set-url JATO_Analysis_System git@github.com:tristan419/JATO_Analysis_System.git
```

---

## 1. 本地推送完整流程

### 1.1 发布前检查（三选二即可，前端构建必须过）

```bash
cd /path/to/JATO_Analysis_System

# Python 烟测
python 03_Scripts/ci_smoke_check.py

# 后端编译检查
. .venv/bin/activate
python -m compileall 06_AppPlatform/backend/app

# 前端构建（必须）
cd 06_AppPlatform/frontend
npm ci
npx tsc --noEmit
npm run build
```

### 1.2 选择性提交

不要盲目 `git add .`，只提交本次改动相关的文件：

```bash
cd /path/to/JATO_Analysis_System
git status --short          # 确认改动范围
git add 06_AppPlatform/...  # 只加入目标文件
git commit -m "描述本次改动"
```

### 1.3 推送

```bash
git push JATO_Analysis_System main
```

推送成功后，如果 `06_AppPlatform/**` 路径有变更，GitHub Actions 会自动触发腾讯云部署。

---

## 2. 推送失败排查

### 2.1 快速诊断

```bash
# 1) 确认 remote 地址
git remote -v

# 2) 测试 HTTPS 连通性
curl -sI --connect-timeout 5 https://github.com | head -1

# 3) 测试 SSH 连通性（如果 remote 是 SSH）
ssh -T -o ConnectTimeout=5 git@github.com 2>&1
ssh -T -o ConnectTimeout=5 git@github-443 2>&1

# 4) 检查 SSH agent 身份
ssh-add -l

# 5) 检查 macOS 钥匙串中的 HTTPS 凭据
git credential-osxkeychain get <<EOF
protocol=https
host=github.com
EOF
```

### 2.2 常见问题与修复

| 现象 | 根因 | 修复 |
|------|------|------|
| `Connection reset by peer` (443) | 当前网络封禁了 SSH 协议（即使走 443 端口） | 切 HTTPS：`git remote set-url JATO_Analysis_System https://github.com/tristan419/JATO_Analysis_System.git` |
| `Operation timed out` (22) | 网络封禁 22 端口出站 | 同上，或切 443 SSH |
| `Permission denied (publickey)` | SSH key 未加载或不匹配 | `ssh-add ~/.ssh/id_rsa` |
| `The agent has no identities` | ssh-agent 空的 | `ssh-add ~/.ssh/id_rsa` |
| HTTPS 推送 `403` | 钥匙串中 token 过期 | 在钥匙串中更新 github.com 的凭据，或用 `gh auth login` |
| HTTPS 连接也超时 | 整体外网不通 | 检查代理 / VPN 设置 |

### 2.3 SSH config 参考

当前 `~/.ssh/config` 中的 `github-443` 条目：

```
Host github-443
    HostName ssh.github.com
    User git
    Port 443
    IdentityFile ~/.ssh/id_rsa
    IdentitiesOnly yes
```

当 SSH 完全不通时，**不需要改这个文件**，只需把 git remote 切到 HTTPS 即可。

---

## 3. 自动部署（GitHub Actions → 腾讯云）

Workflow 文件：`.github/workflows/deploy-fullstack-tencent.yml`

### 3.1 触发条件

- push 到 `main` 且修改了 `06_AppPlatform/**`、`03_Scripts/**`、`07_ScrapingToolkit/**` 或部署相关配置
- 或手动 `workflow_dispatch`

### 3.2 必需的 GitHub Secrets

| Secret | 说明 |
|--------|------|
| `SSH_HOST` | 腾讯云服务器公网 IP |
| `SSH_PORT` | SSH 端口，当前生产固定为 `22` |
| `SSH_USER` | SSH 登录用户名 |
| `SSH_PRIVATE_KEY` | 私钥内容（和 `SSH_PASSWORD` 二选一） |
| `SSH_PASSWORD` | 密码（和 `SSH_PRIVATE_KEY` 二选一） |
| `DEPLOY_CERTBOT_EMAIL` | 可选，但推荐；Let's Encrypt 联系邮箱 |

当前生产基线（2026-04-14 已验证通过）：

- `SSH_HOST=150.158.141.14`
- `SSH_PORT=22`
- `SSH_USER=ubuntu`
- `SSH_PRIVATE_KEY` 必须填写本地可直接免密登录腾讯云的完整私钥内容，当前对应 `~/.ssh/tencent_lh.pem`

说明：`SSH_PRIVATE_KEY` 必须是完整原文粘贴，包含 `-----BEGIN RSA PRIVATE KEY-----` 和 `-----END RSA PRIVATE KEY-----`。少一行、少一个换行、粘错另一把 key，都会让 GitHub Actions 在上传归档或 SSH 执行阶段失败。

### 3.3 自动部署流程

1. checkout 代码
2. 打包 deploy 归档（排除 `01_RAW_DATA`, `04_Processed_data`, `node_modules`, `.venv`, `.git`, `Markdown_Readme`, `*.ipynb`, `data_wangler`, `__pycache__`）
3. `scp` 上传归档到服务器 `/tmp/JATO_deploy.tar.gz`（`strip_components: 2`）
4. SSH 登录服务器，解压到 `/opt/JATO_Analysis_System-main`
5. 执行 `03_Scripts/deploy_fullstack_server.sh`（`SKIP_GIT_SYNC=true`）
6. 发布脚本会先清掉已知白名单内的 untracked 残留：`04_Processed_data/.refresh_backups/pre-sync-*`、`Markdown_Readme/Fullstack/*.md`、`Markdown_Readme/Streamlit/*.md`
7. 发布脚本会把 `07_ScrapingToolkit` 安装进服务器 `.venv`，并同步 night window 内的抓取 timer：country news、VOC、MSRP dry-run / ingest
8. workflow 会再补一遍公网入口：默认按 `ojeur.cloud www.ojeur.cloud` 处理；如果仓库 Variable `DEPLOY_SERVER_NAME` 存在，则以它为准
9. 如果 `DEPLOY_ENABLE_HTTPS` 不是 `false`，workflow 会调用 `enable_jato_fullstack_https.sh` 做幂等证书检查；已是 Certbot 管理的 nginx 配置不会被 HTTP 模板覆盖
10. 健康检查 `curl http://127.0.0.1:8000/healthz`

### 3.4 域名 / HTTPS 约定

- 当前生产默认域名：`ojeur.cloud www.ojeur.cloud`
- 可选 Variables：`DEPLOY_SERVER_NAME`、`DEPLOY_ENABLE_HTTPS`
- `DEPLOY_ENABLE_HTTPS=false` 时，只维护 HTTP ingress，不碰证书
- `install_jato_fullstack_nginx.sh` 检测到 `managed by Certbot` 后会跳过覆盖，避免把已签发证书的 nginx 配置冲掉

### 3.5 以后默认怎么发版

日常发布以后默认只走这一条：

1. 本地提交代码后执行 `git push JATO_Analysis_System main`
2. 等 GitHub Actions 自动触发 `deploy-fullstack-tencent`
3. 如果只是想重跑同一版，在 GitHub 网页手动点一次 `workflow_dispatch`

只要 GitHub Secrets 继续保持上面的正确值，后续不需要再手工 SSH 上腾讯云补域名、补 HTTPS 或手动上传归档。

只有下面几类情况才需要跳出这条默认流程：

1. 首次初始化一台新服务器
2. 更换域名或首次接入 HTTPS
3. 线上紧急回滚或做系统级排障

---

## 4. 手动服务端部署

当自动部署不可用时，直接 SSH 到服务器操作。

```bash
ssh <user>@<tencent-cloud-ip>
cd /opt/JATO_Analysis_System-main

VITE_API_BASE=/v1 \
VITE_USER_ROLE=viewer \
VITE_USER_NAME=manual-deploy \
bash 03_Scripts/deploy_fullstack_server.sh
```

脚本自动完成：拉取最新 main → 安装后端依赖 → npm ci + 构建前端 → 重启 `jato-fullstack-backend@8000` → reload nginx。

如果某次你想保留远端 untracked 调试痕迹，可以显式关闭自动清理：

```bash
DEPLOY_PRUNE_UNTRACKED=false bash 03_Scripts/deploy_fullstack_server.sh
```

---

## 5. 发布验收

```bash
# 服务端快速检查
curl -fsS http://127.0.0.1:8000/healthz
curl -I http://127.0.0.1/

# 完整诊断
bash 03_Scripts/print_fullstack_server_diagnostics.sh
```

浏览器检查：
- Dashboard 首页 overview 加载
- 时间序列图表正常
- 高级分析页面出图
- Specification / CRUD 页面可打开

---

## 6. 回退

```bash
cd /opt/JATO_Analysis_System-main
git log --oneline -5             # 找到目标提交
git reset --hard <commit_sha>
bash 03_Scripts/deploy_fullstack_server.sh
curl -fsS http://127.0.0.1:8000/healthz
```

---

## 7. 已知问题与修复记录

### 7.1 scp-action 归档路径（2026-04-08）

`appleboy/scp-action@v0.1.7` 需要 `strip_components: 2`（不是 0），否则文件落在 `/tmp/github/runner_temp/` 而非 `/tmp/`。

### 7.2 Node.js 版本检查（2026-04-08）

`deploy_fullstack_server.sh` 版本校验已放宽为 Node.js 20.10+ 或 22.x+。

### 7.3 deploy 归档体积（2026-04-08）

tar 排除 `01_RAW_DATA` / `04_Processed_data` 等后，归档从 700MB+ 降到约 50MB。

### 7.4 前端白屏（2026-04-08）

`client.ts` 中 `API_BASE` 不能写死 `http://127.0.0.1:8000/v1`。改为 `import.meta.env.VITE_API_BASE ?? "/v1"`，生产走 nginx 同域代理。

### 7.5 Plotly 懒加载致图表空白（2026-04-08）

不用 `React.lazy` 加载 Plotly 组件，改为同步 import `plotly.js-cartesian-dist-min`（约 475KB gzip）。路由级 lazy 拆分 Specification / CRUD。

### 7.6 CI smoke 回归断言（2026-04-08）

`regression_render_strategy_defaults.py` 中 "huge-full-mode" 的 `expected_overview` 修正为 `True`。ci #64 全通过。

### 7.7 本地 git push 失败（2026-04-09）

**现象**：`kex_exchange_identification: read: Connection reset by peer`（443）和 `Operation timed out`（22），本地 git push 完全不通。

**根因**：当前网络环境屏蔽了 SSH 协议出站。端口 22 超时、端口 443 TCP 建连后 SSH 握手被远端重置。而 `curl https://github.com` 正常返回 200，说明 HTTPS 完全可达。git remote 原来配的是 `git@github-443:...`（SSH），所以全部失败。

**修复**：
```bash
git remote set-url JATO_Analysis_System https://github.com/tristan419/JATO_Analysis_System.git
git push JATO_Analysis_System main
```
macOS 钥匙串已有 GitHub HTTPS 凭据，切换后立即推送成功。

**建议**：将 HTTPS 作为默认推送方式。只有明确知道 SSH 可达时才切回 SSH。

### 7.8 腾讯云远端 untracked 脏树（2026-04-13）

**现象**：腾讯云活跃仓库里残留了未跟踪的 refresh backup 目录和临时 Markdown 文档，后续 Git checkout / 部署容易因为工作树脏而变得不可预测。

**修复**：`deploy_fullstack_server.sh` 已新增“已知白名单 untracked 清理”步骤，在构建前自动 prune：

- `04_Processed_data/.refresh_backups/pre-sync-*`
- `Markdown_Readme/Fullstack/*.md`
- `Markdown_Readme/Streamlit/*.md`

**原则**：不做全仓库 `git clean -fdx`，只清明确白名单里的 untracked 临时内容，避免误删数据目录或配置文件。

### 7.9 GitHub Actions SSH 上传失败（2026-04-14）

**现象**：`deploy-fullstack-tencent` 连续多次失败或卡在 `Upload archive to server`，表面上看像 GitHub Actions 自身问题，早期日志里还夹杂 Node.js 20 deprecation warning，容易误判。

**排查结论**：Node.js warning 不是根因。腾讯云服务器上的 `sshd`、22 端口监听和安全组都正常，服务器日志已经明确出现过 GitHub Actions runner 的 `Accepted publickey for ubuntu ...`。真正根因是 GitHub Secrets 里的 SSH 参数曾经配置不正确。

**已验证可用的正确值**：

- `SSH_HOST=150.158.141.14`
- `SSH_PORT=22`
- `SSH_USER=ubuntu`
- `SSH_PRIVATE_KEY=<~/.ssh/tencent_lh.pem 的完整内容>`

**验证结果**：

- `deploy-fullstack-tencent #51`：push 触发，成功
- `deploy-fullstack-tencent #52`：手动 `workflow_dispatch`，成功

**以后如果再遇到同类问题，先按这个顺序查**：

1. 先看 GitHub Secrets 里的 `SSH_HOST`、`SSH_PORT`、`SSH_USER` 是否仍是上面的值
2. 再看 `SSH_PRIVATE_KEY` 是否被粘贴坏了，尤其是头尾行和换行
3. 再去服务器看 `sudo systemctl status ssh`、`sudo ss -ltnp | grep :22` 和 `/var/log/auth.log` 或 `journalctl -u ssh`

**结论**：这条 CI/CD 链路现在已经打通。后续常规发布默认只走 `git push JATO_Analysis_System main` 或手动 `workflow_dispatch` 即可。
