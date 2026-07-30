# JATO Fullstack 腾讯云 Ubuntu 部署手册

本文对应当前 Fullstack 方案，不走 Docker。生产形态固定为：

- React + Vite 前端先构建成静态文件
- nginx 负责静态文件和反向代理
- FastAPI 后端通过 systemd 常驻
- 服务器更新通过 SSH 拉代码并执行发布脚本

如果你想尽量少输命令，使用第 3.1 节的一键脚本；启动前仍必须先解析并显式传入本次代码对应的完整 `DEPLOY_COMMIT_SHA`。

## 当前生产实例基线（2026-04-11）

后续容量评估、性能瓶颈讨论和 MSRP 抓取调度，默认以这台腾讯云实例为基线：

| 项目 | 当前规格 |
| --- | --- |
| 实例类型 | 入门型实例 |
| CPU | 4 vCPU |
| 内存 | 16GB |
| 系统盘 | SSD 云硬盘 180GB |
| 月流量包 | 2000GB / 月 |
| 带宽 | 12Mbps |
| 到期时间 | 2027-03-31 14:50:03 |

这组规格应作为后续以下工作的默认基线：

1. Fullstack 页面响应与并发判断。
2. MSRP 抓取任务的并发度和调度时窗评估。
3. Headless browser、PostgreSQL、FastAPI、nginx 共机部署时的资源预算。
4. 后续性能 bottleneck 分析与扩容阈值判断。

## 1. 目标目录与端口

- 仓库目录：`/opt/JATO_Analysis_System-main`
- 后端服务：`jato-fullstack-backend@8000`
- 后端监听：`127.0.0.1:8000`
- 前端静态目录：`/opt/JATO_Analysis_System-main/06_AppPlatform/frontend/dist`
- 外部入口：nginx `80`

### 1.1 发布前自动清理规则

`03_Scripts/deploy_fullstack_server.sh` 现在会在构建前自动清理一批“已知安全”的 untracked 残留，避免腾讯云仓库因为历史备份目录或临时文档变脏：

- `04_Processed_data/.refresh_backups/pre-sync-*`
- `Markdown_Readme/Fullstack/*.md`
- `Markdown_Readme/Streamlit/*.md`

这里不是全量 `git clean -fdx`，而是只清这些明确白名单路径下的 untracked 项；已跟踪文件不会被删，`04_Processed_data` 主数据目录也不会被碰。

如果某次你明确不想执行这一步，可以临时关闭：

```bash
DEPLOY_PRUNE_UNTRACKED=false bash 03_Scripts/deploy_fullstack_server.sh
```

如果后续还有新的服务器侧临时垃圾路径需要纳入，可以覆盖：

```bash
DEPLOY_UNTRACKED_CLEAN_PATTERNS='04_Processed_data/.refresh_backups/pre-sync-* tmp/deploy-* Markdown_Readme/Fullstack/*.md Markdown_Readme/Streamlit/*.md' \
bash 03_Scripts/deploy_fullstack_server.sh
```

## 2. 服务器前置依赖

推荐以 Ubuntu 22.04 或 24.04 为基础镜像。

如果你直接使用一键脚本，这一节的安装动作会自动完成，可以跳到第 3 节。

```bash
sudo apt-get update
sudo apt-get install -y git curl nginx python3 python3-venv python3-pip

curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt-get install -y nodejs

node -v
npm -v
python3 --version
```

要求：

- Node.js 不低于 20.19.0
- Python 建议 3.10 及以上

## 3. 首次拉取代码

如果腾讯云上直接访问 GitHub 不稳定，优先用 codeload 的 archive 下载。这个方式不依赖 git clone，通常比镜像更稳定。下载命令带进度条和超时，这样不会看起来像“卡死”：

```bash
sudo mkdir -p /opt
sudo chown "$USER":"$USER" /opt

cd /opt
DEPLOY_COMMIT_SHA="$(
  curl -fsSL https://api.github.com/repos/tristan419/JATO_Analysis_System/commits/main \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["sha"])'
)"
[[ "$DEPLOY_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]] || {
  echo "无法解析 main 的完整 commit SHA" >&2
  exit 1
}
curl -# -L --connect-timeout 15 --max-time 600 \
  "https://codeload.github.com/tristan419/JATO_Analysis_System/tar.gz/${DEPLOY_COMMIT_SHA}" \
  -o JATO_Analysis_System-main.tar.gz
mkdir -p JATO_Analysis_System-main
tar -xzf JATO_Analysis_System-main.tar.gz \
  --strip-components=1 -C JATO_Analysis_System-main
printf '%s\n' "$DEPLOY_COMMIT_SHA" > JATO_Analysis_System-main/.bootstrap-commit-sha
cd /opt/JATO_Analysis_System-main

python3 -m venv .venv
. .venv/bin/activate
python -m pip install --upgrade pip
pip install -r 06_AppPlatform/backend/requirements.txt
```

如果你更想保留 git 历史，也可以先尝试镜像 clone；但当镜像不稳定时，codeload 这条路径更稳。当前脚本已经支持 archive-based bootstrap，所以下载后直接跑一键脚本即可。

如果腾讯云机器下载 archive 也很慢或经常中断，直接改成“本地下载后上传到服务器”：

```bash
# 在你自己的电脑上执行
DEPLOY_COMMIT_SHA="$(
  curl -fsSL https://api.github.com/repos/tristan419/JATO_Analysis_System/commits/main \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["sha"])'
)"
[[ "$DEPLOY_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]] || exit 1
curl -# -L --connect-timeout 15 --max-time 600 \
  "https://codeload.github.com/tristan419/JATO_Analysis_System/tar.gz/${DEPLOY_COMMIT_SHA}" \
  -o JATO_Analysis_System-main.tar.gz
printf '%s\n' "$DEPLOY_COMMIT_SHA" > JATO_Analysis_System-main.sha
scp JATO_Analysis_System-main.tar.gz JATO_Analysis_System-main.sha \
  root@<你的腾讯云IP>:/opt/

# 在腾讯云服务器上执行
cd /opt
mkdir -p JATO_Analysis_System-main
tar -xzf JATO_Analysis_System-main.tar.gz \
  --strip-components=1 -C JATO_Analysis_System-main
mv JATO_Analysis_System-main.sha JATO_Analysis_System-main/.bootstrap-commit-sha
cd /opt/JATO_Analysis_System-main
```

## 3.1 一键初始化、部署并启动

完成仓库拉取后，直接执行：

```bash
cd /opt/JATO_Analysis_System-main
if git -C . rev-parse --verify HEAD >/dev/null 2>&1; then
  DEPLOY_COMMIT_SHA="$(git -C . rev-parse --verify HEAD)"
else
  DEPLOY_COMMIT_SHA="$(cat .bootstrap-commit-sha)"
fi
[[ "$DEPLOY_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]] || {
  echo "缺少与下载内容绑定的完整 DEPLOY_COMMIT_SHA" >&2
  exit 1
}
DEPLOY_COMMIT_SHA="$DEPLOY_COMMIT_SHA" \
  bash 03_Scripts/tencent_fullstack_bootstrap.sh
```

常用覆盖参数：

```bash
cd /opt/JATO_Analysis_System-main
if git -C . rev-parse --verify HEAD >/dev/null 2>&1; then
  DEPLOY_COMMIT_SHA="$(git -C . rev-parse --verify HEAD)"
else
  DEPLOY_COMMIT_SHA="$(cat .bootstrap-commit-sha)"
fi
[[ "$DEPLOY_COMMIT_SHA" =~ ^[0-9a-f]{40}$ ]] || exit 1
SERVER_NAME=your.domain.com \
BACKEND_PORT=8000 \
APP_BACKEND_WORKERS=2 \
APP_AUTH_ENABLED=false \
VITE_API_BASE=/v1 \
DEPLOY_COMMIT_SHA="$DEPLOY_COMMIT_SHA" \
  bash 03_Scripts/tencent_fullstack_bootstrap.sh
```

不要把 `refs/heads/main` 当作部署身份传给 bootstrap：它是可变引用。archive 路径必须先解析一次 full SHA，用同一个 SHA 下载 tarball，并通过 `DEPLOY_COMMIT_SHA` 显式传给脚本。

这个脚本会自动做完：

- 检查 sudo 权限
- 安装 Python、Node.js、nginx 等基础依赖
- 创建 `.venv`
- 渲染 `/etc/jato-fullstack/backend.env`
- 安装并启用 `jato-fullstack-backend@8000`
- 把 `07_ScrapingToolkit` 安装进服务器 `.venv`
- 同步并启用抓取 timer：country news、VOC、MSRP
- 安装前端依赖并构建 `dist`
- 安装 nginx 配置并重启服务
- 做本地健康检查

如果脚本失败，终端会自动打印一段以 `BEGIN JATO FULLSTACK DIAGNOSTICS` 开头的诊断块。你把整段复制给我就行。

## 4. 配置后端环境变量

如果你已经跑过一键脚本，这一节通常已经自动完成；只在你需要手动改 token 或数据路径时再编辑。

复制模板并编辑：

```bash
sudo mkdir -p /etc/jato-fullstack
sudo cp 03_Scripts/deploy/systemd/jato-fullstack-backend.env.example /etc/jato-fullstack/backend.env
sudo nano /etc/jato-fullstack/backend.env
```

至少确认这些值：

- `APP_PROJECT_ROOT=/opt/JATO_Analysis_System-main`
- `APP_BACKEND_WORKERS=2`
- `APP_AUTH_ENABLED=false`
- `APP_AUTH_TOKEN=仅在服务器本地填写；不要提交进仓库`
- `JATO_PARQUET_PATH=/opt/JATO_Analysis_System-main/04_Processed_data/jato_full_archive.parquet`
- `APP_GOOGLE_OAUTH_PROXY_URL=http://127.0.0.1:7897`（腾讯云大陆用于 Google OAuth token/userinfo 出站；海外机房可留空）
- `JATO_PARTITIONED_PATH=/opt/JATO_Analysis_System-main/04_Processed_data/partitioned_dataset_v1`
- `APP_CRUD_DATA_PATH=/opt/JATO_Analysis_System-main/04_Processed_data/app_entities.json`

启动时会执行后端环境变量校验：

- `APP_DATABASE_ENABLED=true` 时，`APP_DATABASE_URL` 不能为空，否则后端会直接启动失败。
- `APP_DATABASE_URL` 已填写但 `APP_DATABASE_ENABLED=false` 时，只记录 warning，因为这通常表示 DB 写入被临时关闭。
- `APP_STARTUP_VALIDATION_MODE=warn` 适合本地或临时调试；`strict` 适合生产发布前检查。
- 如果生产要求 Country Copilot 必须可用，设置 `APP_COUNTRY_COPILOT_REQUIRE_LLM_KEY=true`，并在服务器本地填写 `DEEPSEEK_API_KEY`。
- 校验日志只输出变量名和缺失原因，不输出 secret 值。

Google 登录的生产链路是：前端从 `www.ojeur.cloud` 或 `intl.ojeur.cloud` 请求 `www.ojeur.cloud/v1/auth/google/auth-url`，Google callback 固定回到 `https://www.ojeur.cloud/v1/auth/google/callback`，后端再按 OAuth state 里的安全 frontend origin 回跳到发起登录的页面。腾讯云大陆机房访问 `oauth2.googleapis.com` 时需要本机 mihomo 代理，默认 `APP_GOOGLE_OAUTH_PROXY_URL=http://127.0.0.1:7897`。

如果页面显示 `Google auth failed` 或 `SSLEOFError`，先在服务器上检查：

```bash
systemctl is-active mihomo
ss -ltnp | grep ':7897'
curl -I --max-time 20 --proxy http://127.0.0.1:7897 https://oauth2.googleapis.com/token
curl -sS https://www.ojeur.cloud/_deploy_status.txt | sed -n '/---google oauth proxy---/,/---release---/p'
```

`curl -I` 只要能连通并返回 HTTP 头即可，HTTP 400/404/405 都代表网络出口通了；连接超时、EOF、connection closed 才是代理或出口问题。GitHub Actions 部署会自动刷新 mihomo 订阅、重启服务，并把 Google 代理检查写入 `_deploy_status.txt`。

生产建议在 GitHub Secrets 里配置 `MIHOMO_SUB_URL`，让部署时明确刷新订阅。未配置时脚本会先读取服务器本机受限文件 `/etc/mihomo/subscription_url`，再尝试从 `MIHOMO_DB_PATH` 或 `/home/*/.local/share/0dcloud/database.sqlite` 发现 0dcloud profile URL；如果都不可用，刷新会跳过并沿用现有 `/etc/mihomo/config.yaml`。

订阅格式优先使用 Clash/mihomo YAML。如果订阅服务的 Clash 链接不可用，但 Shadowrocket 链接能返回 base64 包装的 `ss://` 列表，`03_Scripts/deploy/update_mihomo_subscription.sh` 会在服务器端转换为 mihomo YAML，再复用同一套本地绑定、DNS、`auto` 代理组和 Google 连通性检查流程。转换器会拒绝 `127.0.0.1` / `localhost` 这类只能配合图形化客户端本机转发使用的节点，因为它们在腾讯云上没有远端代理能力。不要把真实订阅 URL 写进仓库；可放到 GitHub Secret `MIHOMO_SUB_URL`，或在服务器上写入 root-only 文件：

如果订阅站对腾讯云直连出口返回 403 或连接失败，部署脚本会先尝试直连，再通过服务器现有的本机 mihomo 代理 `http://127.0.0.1:7897` 重试下载订阅；可用 `MIHOMO_SUB_PROXY_URL` 覆盖这个代理地址。

如果腾讯云和本机代理都无法下载订阅，GitHub Actions 会尽量在 Runner 侧用 `MIHOMO_SUB_URL` 生成预配置好的 mihomo config，并随部署包发送到服务器；服务器安装后会删除部署目录里的临时 config。Google OAuth 代理检测仍失败时，部署脚本会调用 `03_Scripts/deploy/select_mihomo_google_proxy.sh` 通过本机 mihomo controller 选择可访问 `oauth2.googleapis.com` 的节点。

```bash
sudo install -d -m 700 /etc/mihomo
sudo sh -c 'printf "%s\n" "$MIHOMO_SUB_URL" > /etc/mihomo/subscription_url'
sudo chmod 600 /etc/mihomo/subscription_url
```

线上需要 News/VOC/MSRP 写 PostgreSQL 时，后端 env 至少应包含：

```bash
APP_DATABASE_ENABLED=true
APP_DATABASE_URL=postgresql+asyncpg://<user>:<password>@127.0.0.1:5432/jato
APP_STARTUP_VALIDATION_MODE=strict
```

## 4.1 MSRP 任务环境变量

如果你要在这台腾讯云机器上跑 MSRP 批任务，建议单独再放一个任务 env 文件，而不是把批处理参数混进前端或 systemd 服务模板里：

```bash
sudo cp 03_Scripts/deploy/systemd/jato-msrp.env.example /etc/jato-fullstack/msrp.env
sudo nano /etc/jato-fullstack/msrp.env
```

这个文件主要放三类变量：

- 任务模式：手工运行时可用 `JATO_MSRP_MODE=dryrun` 或 `ingest`；线上 timer 会按实例名覆盖成 nightly dry-run / weekly ingest
- 调度节流：`JATO_MSRP_COUNTRIES`、`JATO_MSRP_PAUSE_SECONDS`、`JATO_MSRP_STOP_ON_FAILURE`
- 可选诊断密钥：`NVIDIA_API_KEY`、`NVAPI_KEY`、`HF_TOKEN`

`03_Scripts/run_msrp_low_concurrency.sh` 会先读取 `/etc/jato-fullstack/backend.env`，再读取 `/etc/jato-fullstack/msrp.env`。这意味着常规 ingest 场景下可以直接复用后端的 `APP_AUTH_TOKEN`，不需要再复制一份。

## 4.2 Country News 任务环境变量

Country news 继续保持 **DB-first** 目标形态：线上定时任务写 PostgreSQL snapshot，不要求保留 `04_Processed_data/news` 原始目录。

```bash
sudo cp 03_Scripts/deploy/systemd/jato-country-news.env.example /etc/jato-fullstack/country-news.env
sudo nano /etc/jato-fullstack/country-news.env
```

默认 nightly timer 会在 `23:15` 触发 `jato-country-news-sync.service`。

## 4.3 VOC 任务环境变量

VOC 按当前 VOC 文档执行，线上目标形态是：

1. 保留 `04_Processed_data/voc/<country>/raw/*.json`
2. 继续产出 `enriched/customer_insight_signals.json`
3. 继续产出 `deck/customer_insight_deck.json`
4. 同步 raw layer 到 PostgreSQL staging（`ops.voc_source_runs` / `ops.voc_raw_documents`）

```bash
sudo cp 03_Scripts/deploy/systemd/jato-voc.env.example /etc/jato-fullstack/voc.env
sudo nano /etc/jato-fullstack/voc.env
```

默认 nightly timer 会在 `01:45` 触发 `jato-voc-forum-sync.service`。

## 5. 安装 systemd 后端服务

```bash
sudo cp 03_Scripts/deploy/systemd/jato-fullstack-backend@.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now jato-fullstack-backend@8000
sudo systemctl status jato-fullstack-backend@8000 --no-pager
```

健康检查：

```bash
curl -fsS http://127.0.0.1:8000/healthz
```

## 6. 首次构建与发布前端

仓库已经提供统一发布脚本：`03_Scripts/deploy_fullstack_server.sh`。

首次执行前，确认：

- `.venv` 已创建
- `npm` 和 `node` 已可用
- `jato-fullstack-backend@8000` 已存在

执行：

```bash
cd /opt/JATO_Analysis_System-main
VITE_API_BASE=/v1 \
VITE_ASSET_BASE_URL= \
VITE_USER_ROLE=viewer \
VITE_USER_NAME=anonymous \
bash 03_Scripts/deploy_fullstack_server.sh
```

说明：

- `VITE_API_BASE=/v1` 代表前端走同域 API，不把后端地址写死到构建产物里
- `VITE_ASSET_BASE_URL` 留空时静态资源仍走源站；如果海外访问慢，可以填海外 CDN 域名，例如 `https://static.example.com/`，只让 JS/CSS/assets 走 CDN，API 仍走 `/v1`
- `VITE_AUTH_TOKEN` 默认不建议写进前端构建产物，建议用户首次登录后在页面 Access Control 里填写 token
- 脚本会先清掉已知白名单内的 untracked 脏树，再继续 git / build / restart；这样不会再因为 refresh backup 或临时 Markdown 残留把远端工作树弄脏

如果你只是想重新部署代码，不想重复初始化，直接运行这一节的 `03_Scripts/deploy_fullstack_server.sh` 即可。

## 6.1 默认 nightly 抓取窗口

当前腾讯云默认把三条抓取都放进 `23:00-07:00` 窗口：

| Timer | 默认时间 | 目标形态 |
|------|------|------|
| `jato-country-news-sync.timer` | 每日 `23:15` | DB-first country digest snapshot |
| `jato-voc-forum-sync.timer` | 每日 `01:45` | raw + enriched + deck artifact，并同步 raw 到 PG staging |
| `jato-msrp-dryrun.timer` | 每日 `03:30` | 低并发 nightly dry-run |
| `jato-msrp-ingest.timer` | 每周六 `05:30` | 低并发正式 ingest |

传统单槽部署会把这些 unit / timer 同步到 `/etc/systemd/system`，并重启 timer
以应用新的时间表。蓝绿发布只刷新 unit 定义：切换前原子记录每个 timer 的
enabled/active 状态，结束或回滚时逐项恢复并验真；原本 disabled 或 inactive
的 timer 不会被部署流程意外启动。

## 7. 安装 nginx

```bash
cd /opt/JATO_Analysis_System-main
sudo chmod +x 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
sudo SERVER_NAME=_ BACKEND_PORT=8000 FRONTEND_ROOT=/opt/JATO_Analysis_System-main/06_AppPlatform/frontend/dist \
  bash 03_Scripts/deploy/nginx/install_jato_fullstack_nginx.sh
```

如果你已经有域名，把 `SERVER_NAME=_` 换成真实域名，并在 DNS 中把 A 记录指向腾讯云服务器公网 IP。

## 7.1 启用 HTTPS（Let's Encrypt）

域名已经指向腾讯云公网 IP 后，可以直接执行：

```bash
cd /opt/JATO_Analysis_System-main
sudo SERVER_NAME="ojeur.cloud www.ojeur.cloud" \
  CERTBOT_EMAIL='you@example.com' \
  bash 03_Scripts/deploy/nginx/enable_jato_fullstack_https.sh
```

如果你暂时不想给 Let's Encrypt 留联系邮箱，也可以不传 `CERTBOT_EMAIL`；脚本会退回 `--register-unsafely-without-email`。

说明：

- 脚本会先确保 80 端口 nginx 配置存在，再安装 `certbot` 和 `python3-certbot-nginx`
- 证书签发成功后会自动把 `80 -> 443` 跳转配好
- 以后再次执行是幂等的：证书未到期时不会重复签发
- `install_jato_fullstack_nginx.sh` 现在会检测 `managed by Certbot` 标记，默认不覆盖证书版 nginx 配置，避免后续发布把 HTTPS 冲掉

## 8. 防火墙与安全组

腾讯云安全组至少放通：

- `22/tcp` 用于 SSH
- `80/tcp` 用于 nginx
- `443/tcp` 用于 HTTPS

如果服务器本机启用了 UFW：

```bash
sudo ufw allow OpenSSH
sudo ufw allow 'Nginx Full'
sudo ufw enable
sudo ufw status
```

## 9. 验证项

```bash
curl -fsS http://127.0.0.1:8000/healthz
curl -I http://127.0.0.1/
curl -I http://127.0.0.1/assets/
```

如果要验证带鉴权接口：

```bash
curl -fsS http://127.0.0.1/v1/metadata/columns \
  -H 'X-Auth-Token: 你的token' \
  -H 'X-User-Role: admin' \
  -H 'X-User-Name: deploy-check'
```

浏览器检查：

- 首页能正常打开
- Dashboard `Load Overview` 成功
- CRUD 页面能正常分页
- Network 中 `/v1/...` 请求返回 200

## 9.1 一键打印诊断信息

任何时候只要你想把服务器当前状态贴给我，直接运行：

```bash
cd /opt/JATO_Analysis_System-main
bash 03_Scripts/print_fullstack_server_diagnostics.sh
```

请从 `BEGIN JATO FULLSTACK DIAGNOSTICS` 到 `END JATO FULLSTACK DIAGNOSTICS` 整段复制。

## 10. 后续自动部署

仓库新增 workflow：`.github/workflows/deploy-fullstack-tencent.yml`。

首次上线后，再配置这些 GitHub Secrets / Variables：

- Secrets: `SSH_HOST`, `SSH_USER`, `SSH_PRIVATE_KEY`
- Secrets: `DEPLOY_CERTBOT_EMAIL`（推荐，Let's Encrypt 联系邮箱）
- Secrets: `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`
- Secrets: `GOOGLE_OAUTH_PROXY_URL`（可选；腾讯云大陆默认回落到 `http://127.0.0.1:7897`）
- Secrets: `MIHOMO_SUB_URL`（推荐；用于部署时刷新腾讯云本机 mihomo 订阅，支持 Clash/mihomo YAML 或 Shadowrocket `ss://` 列表订阅，日志会脱敏，不打印完整 URL）
- Variables: `DEPLOY_REPO_DIR`
- Variables: `FULLSTACK_BACKEND_SERVICE_NAME`
- Variables: `DEPLOY_BRANCH`
- Variables: `DEPLOY_SERVER_NAME`（可选，未设置时当前 workflow 默认回落到 `ojeur.cloud www.ojeur.cloud`）
- Variables: `DEPLOY_ENABLE_HTTPS`（可选，默认 `true`）
- Variables: `MIHOMO_DB_PATH`（可选；未设置 `MIHOMO_SUB_URL` 且服务器本机 `/etc/mihomo/subscription_url` 不存在时，用于指定 0dcloud sqlite profile 路径）
- Variables: `FULLSTACK_VITE_API_BASE`
- Variables: `FULLSTACK_VITE_USER_ROLE`
- Variables: `FULLSTACK_VITE_USER_NAME`
- Variables: `VITE_ASSET_BASE_URL`（可选，海外 CDN 静态资源 base URL）
- Environment / deploy script: `REPO_REMOTE_URL`

推荐默认值：

- `DEPLOY_REPO_DIR=/opt/JATO_Analysis_System-main`
- `FULLSTACK_BACKEND_SERVICE_NAME=jato-fullstack-backend@8000`
- `DEPLOY_BRANCH=main`
- `DEPLOY_SERVER_NAME=ojeur.cloud www.ojeur.cloud`
- `DEPLOY_ENABLE_HTTPS=true`
- `FULLSTACK_VITE_API_BASE=/v1`
- `VITE_ASSET_BASE_URL=`（未接 CDN 时留空；接海外 CDN 后填 CDN 域名并保留结尾 `/`）
- `REPO_REMOTE_URL=https://gitclone.com/github.com/tristan419/JATO_Analysis_System.git`

GitHub Actions 自动部署现在会在代码发布后补一遍 ingress：

- 如果 nginx 还是纯 HTTP，会按 `DEPLOY_SERVER_NAME` 补齐站点配置
- 如果已经是 Certbot 管理的 HTTPS，会保留现有证书配置并做幂等校验
- 因此后续 `push main` 不需要再手动处理域名绑定或因为 nginx 重装把 HTTPS 冲掉

## 11. 回滚

查看最近提交：

```bash
cd /opt/JATO_Analysis_System-main
git log --oneline -5
```

回滚到指定提交后重新发布：

```bash
cd /opt/JATO_Analysis_System-main
git reset --hard <commit_sha>
bash 03_Scripts/deploy_fullstack_server.sh
```

这是服务器侧回滚命令，只建议在明确知道目标提交的情况下执行。

## 12. 带宽、并发与页面拆分建议（12Mbps / 4C16G 场景）

### 12.1 核心判断

当前 Fullstack 架构已经基本是“前后端只传数据”的模式：

- 浏览器通过 `/v1/...` API 请求聚合结果、分页明细和图表数据
- 后端在服务器本地读取分区 Parquet，并按筛选条件过滤、聚合、分页后返回 JSON
- 浏览器负责收到数据后画图，而不是直接下载原始 Parquet / Excel

因此，12Mbps 带宽场景下的首要瓶颈不是 API 数据体积，而是前端静态资源，尤其是 Plotly 相关 JS 包。

### 12.2 已验证的线上传输规模（2026-04-08）

以下数据来自已部署的腾讯云实例，对浏览器实际压缩传输体积做的抽样测量：

| 项目 | 压缩后体积 | 说明 |
| --- | --- | --- |
| `/v1/analysis/overview` | 约 522B | 首屏 KPI + 年/月概览 |
| `/v1/analysis/time-series-grouped` | 约 3.2KB | 一个典型分组图请求 |
| `/v1/analysis/detail`（100 行、5 列） | 约 10.5KB | 一页明细数据 |
| `/assets/react-vendor-*.js` | 约 59KB gzip | React 基础运行时 |
| `/assets/router-vendor-*.js` | 约 29KB gzip | 路由代码 |
| `/assets/plotly-vendor-*.js` | 约 475KB gzip | 已切到 cartesian Plotly bundle，仍是 Dashboard 首屏最大 chunk，但已明显收敛 |

结论：

- API 返回体积已经很小，没必要为了“省公网带宽”把原始数据直接推给前端
- 如果要优先优化体验，应先处理静态资源体积和缓存命中率，而不是先引入数据库

### 12.3 当前推荐架构

针对腾讯云 12Mbps 带宽，推荐继续保持以下原则：

1. 前端只请求聚合 JSON、分页明细和导出文件，不下发原始大表
2. 后端继续负责本地筛选、聚合和分页，数据源仍以分区 Parquet 为主
3. Overview、时间序列、分组图等接口优先返回聚合结果，不暴露明细级大结果集
4. 明细查询和 CSV 导出保持按页或按按钮触发，避免用户一进首页就拉大数据块
5. `/assets/` 继续使用 hash 文件名 + immutable 长缓存；如果未来有更多首次访问用户，再优先接 CDN

只有当查询延迟、并发压力、物化聚合需求明显提升时，才考虑引入中间层分析引擎。

### 12.4 4C16G / 180GB / 12Mbps 下 20 人同时使用的判断

这台机器的瓶颈顺序通常是：

1. 公网带宽
2. 单实例 Python 查询吞吐
3. 前端大体积图表库首次下载

轻量并发压测结果（从外网发起，包含公网链路抖动与当前单实例部署影响）：

| 接口 | 并发数 | 结果 |
| --- | --- | --- |
| `overview` | 5 | 全部成功，平均约 2.93s |
| `overview` | 10 | 9 成功 1 超时，平均约 3.43s |
| `overview` | 20 | 18 成功 2 超时，平均约 6.67s，最长约 17.65s |
| `time-series-grouped` | 5 | 4 成功 1 超时，平均约 2.09s |
| `time-series-grouped` | 10 | 8 成功 2 超时，平均约 4.03s |
| `time-series-grouped` | 20 | 19 成功 1 超时，平均约 7.94s，最长约 17.97s |
| `detail`（100 行 5 列） | 10 | 全部成功，平均约 3.61s，最长约 9.99s |

如何解读：

- 如果是 20 个人“错峰查看、偶尔切筛选”，这台 4C16G 机器可以先跑
- 如果是 20 个人“同时高频切筛选、切图、导出、看明细”，当前单实例会开始出现超时和明显排队
- 如果 20 个人几乎同时第一次打开页面，当前 `plotly-vendor` 约 475KB gzip，20 人约 9.5MB 出口流量；12Mbps 理论吞吐约 1.5MB/s，依然会排队，但已经比早期 2.9MB bundle 明显改善

所以，对这台机器的现实判断是：

- 适合 `20 人以内的轻中度看板浏览`
- 不适合 `20 人同时重度交互 + 首次冷启动访问`

### 12.4.1 MSRP 抓取任务在这台机器上的调度建议

对于 MSRP 抓取，这台 4C16G 机器的约束与看板浏览不同，核心原则是“低频批处理、错峰运行、限制浏览器并发”：

1. MSRP 抓取属于周级批处理任务，不需要白天高频执行。
2. 推荐把抓取时窗放在 `00:00-06:00`，避开白天用户访问和前端构建发布时段。
3. 对需要 Playwright / Patchright 的动态站点，建议按国家-品牌串行或小并发执行，默认并发控制在 `1-2` 个 headless browser 任务。
4. 不建议在这台机器上一次性把“21 国 × 每国 Top 10 品牌”全部并发打满；应按批次切分，允许任务跨多个夜间窗口完成。
5. 对低优先级品牌，允许“抓取慢但稳定”，优先保证 source registry 可维护和失败可重试，而不是追求一次夜间全量完成。

### 12.4.2 直接可用的低并发执行脚本

仓库里已经补了一个顺序执行的封装脚本：`03_Scripts/run_msrp_low_concurrency.sh`。

它的执行策略是：

- 先加载 `/etc/jato-fullstack/backend.env` 和 `/etc/jato-fullstack/msrp.env`
- 按国家顺序逐个调用 `03_Scripts/batch_dryrun.py` 或 `03_Scripts/batch_ingest.py`
- 国家之间默认冷却 `20` 秒
- 内置 `flock` 锁，避免 cron 重叠启动
- 只要某个国家出现失败，就可以按 `JATO_MSRP_STOP_ON_FAILURE=true` 直接停下

手工 dry-run：

```bash
cd /opt/JATO_Analysis_System-main
bash 03_Scripts/run_msrp_low_concurrency.sh
```

手工 ingest：

```bash
cd /opt/JATO_Analysis_System-main
JATO_MSRP_MODE=ingest \
JATO_MSRP_COUNTRIES=se,hr \
bash 03_Scripts/run_msrp_low_concurrency.sh
```

如果你要先把动态站点放在第一夜窗口，建议直接拆成两段：

```bash
cd /opt/JATO_Analysis_System-main
JATO_MSRP_COUNTRIES=se,hr bash 03_Scripts/run_msrp_low_concurrency.sh
JATO_MSRP_COUNTRIES=hu,no,at,cz,ch bash 03_Scripts/run_msrp_low_concurrency.sh
```

推荐 cron：

```bash
sudo crontab -e
```

```cron
20 0 * * * cd /opt/JATO_Analysis_System-main && /bin/bash 03_Scripts/run_msrp_low_concurrency.sh >> /var/log/jato-msrp-cron.log 2>&1
```

如果你确认当天 dry-run 已经稳定，再单独加 ingest 窗口：

```cron
50 2 * * 6 cd /opt/JATO_Analysis_System-main && JATO_MSRP_MODE=ingest /bin/bash 03_Scripts/run_msrp_low_concurrency.sh >> /var/log/jato-msrp-cron.log 2>&1
```

对于常规 scrape，本机只需要 `APP_AUTH_TOKEN` 即可；只有你要在腾讯云上手工运行页面分析器时，才需要额外在 `msrp.env` 里注入 `NVIDIA_API_KEY` 或 `HF_TOKEN`。

如果你希望 20 个分析用户稳定同时在线，优先顺序建议是：

1. 让静态资源走 CDN 或更强缓存命中
2. 把明细表和导出从 Dashboard 主页面拆出去
3. 控制明细页默认页大小与导出上限
4. 再考虑增加后端 worker 或第二个实例

### 12.5 是否应该把明细放到独立页面

建议：**是，值得拆。**

原因不是“明细接口体积太大”，而是它会拉高主页面的状态复杂度和交互耦合度：

- Dashboard 首页的首要目标是尽快出 KPI 和图表
- 明细表、列选择、分页、CSV 导出属于另一类使用路径
- 把明细留在同一页，会让 Dashboard 挂上更多本不属于首屏的状态、按钮和副作用
- 后续如果要给明细单独做权限、缓存、虚拟滚动或更严格限流，独立页面更好做

当前路由只有 Dashboard 和 CRUD 两页，结构很简单，所以拆页成本不高。推荐做法：

1. 在 `src/pages/` 新增一个专门页面
2. 从 Dashboard 中迁出明细表、列选择、分页、CSV 导出相关状态和请求
3. Dashboard 只保留 overview、grouped time series、advanced chart 等看板能力
4. 通过 URL query 共享筛选条件，这样 Dashboard 和明细页可以互相跳转并保留同一套 filters

命名上，如果该页主要展示车型规格和行级数据，可以用：

- `DetailPage.tsx`
- `SpecificationPage.tsx`
- `DatasetExplorerPage.tsx`

其中：

- 如果你想强调“规格查看”，`SpecificationPage.tsx` 可以
- 如果你想强调“行级明细浏览”，`DetailPage.tsx` 会更直接

### 12.6 什么时候才需要中间层数据库

仅仅因为带宽只有 12Mbps，**不需要**现在就上中间层数据库。

更合适的触发条件是：

- 多个高频筛选组合需要重复秒级响应
- 单实例 Parquet + PyArrow 在 10 到 20 个活跃分析用户下已经明显排队
- 需要更强的物化聚合、SQL 分析或跨时间窗口缓存
- 你准备把这个系统从“内部看板”升级为“多人持续在线的数据产品”

如果后续真要上中间层，优先考虑分析型方案：

- 单机优先：DuckDB
- 多用户和更高并发：ClickHouse

不建议为了这个场景先上通用 OLTP 数据库，只会增加迁移和维护成本，收益不对等。

### 12.7 腾讯云服务器内存不足时的应急方案

如果服务器只有 2-4GB 内存：

```bash
# 查看当前内存使用
free -h

# 如果 npm ci 或 vite build 被 OOMKill：
# 方案 1：增加 swap
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

# 方案 2：限制 Node.js 内存
NODE_OPTIONS="--max-old-space-size=1024" npm run build
```

## 13. 腾讯云 8000/8001 蓝绿发布

生产发布不再先覆盖 `/opt/JATO_Analysis_System-main`。主流程由
`03_Scripts/deploy/tencent_bluegreen_release.sh` 管理：

1. 把已校验的不可变 artifact 安装到
   `/opt/jato/releases/<commit>/<archive-sha256>/`，并创建独立 `.venv`。
2. 从 8000/8001 中选择非 active 端口作为 candidate。candidate 使用 2 个
   Uvicorn worker，但关闭全部预热，cgroup 为 `MemoryHigh=3G`、
   `MemoryMax=4G`。
3. candidate 必须先通过直连 `/healthz`、精确 commit `/readyz`、JATO
   月更 HTTP 423 门禁、cgroup 和无 `jato_monthly_worker.py` 子进程检查。
4. 首版蓝绿禁止 Alembic revision 变化。检测到 `current != heads` 时在
   Nginx 切换前失败，数据库变更必须另走 expand/contract 发布。
5. 部署 marker 先让 Nginx 阻断月更路径，再持有 JATO
   `maintenance-coordination.lock`、worker、active bundle、upload/digest
   等最终锁；已有任务只能自然完成，发布脚本不会取消、重试或改写任务状态。
6. `/etc/jato-fullstack/nginx/active-release.conf` 是唯一切换对象，同时绑定
   backend port 和 frontend root。candidate 配置先执行 `nginx -t`，reload
   后再经 Nginx 校验 `/readyz` 与 `build-meta.json`。`DEPLOY_SERVER_NAME`
   可以包含空格分隔的多个域名；控制器会先校验每个 hostname，再以各自的
   TLS SNI/Host 逐个验真，绝不会把整串域名交给一次 `curl`。
7. 任一步失败都会恢复旧 active include、旧 active-slot 和
   `/opt/jato/active`，验证旧槽仍存活，并记录 `rollback_completed`。
8. 新槽完全就绪后才停止旧槽，再把新槽提升到
   `MemoryHigh=6G`、`MemoryMax=8G`；这样双槽窗口的服务 cgroup 上限为
   8G + 4G，不会同时出现两个 8G 槽。
9. 宿主机上的 MSRP 等定时任务统一访问
   `http://127.0.0.1:18000/v1`。该端口只监听 loopback，并由同一个
   `active-release.conf` 代理到当前 active 槽；禁止把消费者固定到 8000
   或 8001。post-activation 会原子迁移既有 `msrp.env` 中的
   `JATO_API_BASE`，但不会改变 timer 的启用或运行状态。
10. 月更 mutation fence 位于持久路径
    `/var/lib/jato-release/deployment-maintenance`。主机重启不会让一个未完成
    的发布静默丢失门禁；只有控制器完成恢复或确认切换后才能移除 marker。
11. Candidate 构建前要求 `MemAvailable >= 5 GiB`，并再次确认宿主机总内存
    至少 14 GiB、active unit 的实际 `MemoryHigh=6G/MemoryMax=8G` 未漂移，
    且 active 实际内存加 Candidate 4 GiB 上限仍为操作系统保留 2 GiB。
    随后 venv、pip、Playwright 与前端安装全部在同步的
    `jato-bluegreen-candidate-build.scope` 中执行；该 scope 固定为
    `MemoryHigh=3G`、`MemoryMax=4G`、`TasksMax=512` 和 30 分钟上限。
    子进程会在执行重依赖安装前验证自己的 UID、cgroup membership 和实际
    systemd 属性，不满足即失败。最终 runtime seal 完成后、启动 Candidate
    前会重复宿主机内存检查。
12. Candidate 构建前磁盘必须保留
    `max(15 GiB, filesystem 8%)`；runtime seal 后还必须保留
    `max(10 GiB, filesystem 5%)`。空间不足时只允许回收
    `/opt/jato/releases/<commit>/<archive-sha256>` 中具有匹配 identity、
    已达到 settled checkpoint 且未被 active、两个 slot、Nginx、运行进程或
    未完成 checkpoint 引用的旧版本。正常保留最新 3 个未引用版本并只清理
    14 天以上版本；低空间时也绝不清理 24 小时内版本。删除前会原子移入
    可恢复 quarantine 并重新扫描引用，任何歧义都在 Candidate 启动前失败。
    上传 archive、checkpoint、journal 和共享 JATO 数据不属于自动清理范围。
13. 上一版本的 `hermes/deploy_release.json` 固定保存在
    `/var/lib/jato-release/previous-metadata/<candidate-commit>/<archive>.json`。
    该目录与严格 checkpoint namespace 分离；同一 artifact 重试只接受完全
    相同的 metadata，避免后续 SHA 被旁路 JSON 阻断或引用错误回滚版本。

运行时位置：

```text
/opt/jato/releases/<commit>/<archive-sha256>  immutable release
/opt/jato/slots/8000/current                 slot symlink
/opt/jato/slots/8001/current                 slot symlink
/opt/jato/active                             scheduler code symlink
/var/lib/jato-release/active-slot            JATO worker owner
/var/lib/jato-release/deployment-maintenance durable mutation fence
/var/lib/jato-release/scheduler-state.tsv    durable timer state snapshot
/var/lib/jato-release/previous-metadata/...  candidate-bound rollback metadata
/etc/jato-fullstack/nginx/active-release.conf API + frontend atomic route
127.0.0.1:18000                              stable active-slot API for host jobs
```

故障演练由测试环境设置 `BLUEGREEN_FAULT`，支持：

- `candidate_start`
- `candidate_ready`
- `nginx_test`
- `nginx_reload`
- `post_switch_readiness`

生产 workflow 不设置该变量。发生失败时不要手工重试 JATO 任务；先读取
release checkpoint、quiescence evidence、两个 slot 状态和 Nginx active
include，确认旧槽已恢复。

## 14. Feature Candidate canary（不切公网）

PR 合并前的腾讯云验证使用
`03_Scripts/deploy/tencent_feature_candidate_canary.sh`，它不是 production
release 的缩小版，也不能切换流量。调用方必须先用与 production package
相同的排除规则生成不可变 feature archive，上传后提供真实 feature branch、
40 位 commit、archive 字节数和 SHA-256。脚本拒绝 `main`，不会为了复用现有
门禁而把 feature SHA 伪装成 production main。

安全边界如下：

1. 只使用 `/opt/jato-canary` 和 `/var/lib/jato-canary`。checkpoint、evidence
   与最终 receipt 都与 `/var/lib/jato-release` 分离。
2. 外层 SSH/TAT/OrcaTerm 进程只校验并固化不可变输入，然后无等待地启动
   `jato-feature-canary-supervisor-<run-key>.service`。外层返回 0 只表示
   systemd 已接受 durable supervisor，不能当作 canary 通过。supervisor
   是唯一的锁 owner，持有 deploy 账号真实 home 下的 canonical
   `production-deploy.lock`，从 production before snapshot 一直持有到
   controller、build、runtime、cleanup 和 after comparison 完成。
   supervisor 自身固定 `256M/512M`；业务 controller 运行在另一个
   `256M/512M` transient service 中，通过完整 unit identity、supervisor
   MainPID 的 `/proc/<pid>/fd/9` 目标、该 fd 的 `fdinfo/9` 中绑定同一 inode
   的唯一 FLOCK write 记录，以及第二个 nonblocking flock，证明锁
   始终由 supervisor 持有，而不是由第三方抢占，也不由 controller 自行继承
   或重新取得 fd 9。controller 被
   TERM/SIGKILL 时，`KillMode=control-group` 会终止其完整进程树；只有该
   unit 已非 active/activating/deactivating 且 cgroup 无进程后，supervisor
   才会在同一锁窗口进入 recovery-only。网页终端断线或 TAT 15 分钟上限
   因此不会留下失去锁约束的孤儿 build。supervisor 异常退出会由 systemd
   重启，重启后先重新取得 canonical lock，再按 checkpoint 只做恢复，绝不
   重跑业务 canary。supervisor 在请求 systemd 创建 controller 前先持久化
   唯一 `controller_unit_started/in_progress` marker；一旦该边界成立，
   即使 controller 尚未写入自己的 marker 或 staged archive 已清理，后续
   也只能恢复。controller 的 EXIT trap 只能写 draft evidence/checkpoint，
   无权写 terminal receipt。只有 supervisor 在 controller cgroup 已静默、
   当前代 fd 9 已持锁、子 unit/端口已清理，并重新采集和比较 production
   AFTER 后启动的 reconcile 子进程可以原子写 receipt。receipt 固定携带
   `terminalWriter=supervisor_reconcile` 与 writer `InvocationID`；成功结果
   还要求 candidate evidence 中的 supervisor generation（来自只读环境和
   root-owned start permit）与 writer `InvocationID` 完全相同；candidate
   service 自身另有独立的 systemd `InvocationID`，必须与 start permit
   记录的 candidate generation 相同。因此 supervisor 发生跨代重启时旧
   candidate 只能收敛为 failed，不能借新一代快照放行。任意伪造 `HOME`、
   `DEPLOY_STATE_DIR`、lock path 或任一 generation 都会拒绝。
3. source archive 最大 256 MiB、最多 50,000 个成员且展开不超过 2 GiB；
   controller、guard、lock helper 与 readiness verifier 必须和 archive 内
   文件逐个同 SHA。输入会先复制为 root-owned 只读文件。build 使用独立
   transient service，固定 `MemoryHigh=3G`、`MemoryMax=4G`、
   `MemorySwapMax=0`、`TasksMax=512`；deploy 用户 home、JATO 数据和
   `/etc/jato-fullstack` 在 build namespace 内不可见，唯一可写位置是本次
   runtime。controller、build 和 runtime 都使用
   `StopPropagatedFrom + After` 绑定 durable supervisor：supervisor
   退出时三个子 unit 只收到 stop，不会因 supervisor 的
   `Restart=on-failure` 被连带重新执行。每个子 unit 还携带首次 supervisor
   的 32 位 `InvocationID`；controller 和 build 通过 systemd manager 确认
   当前 supervisor 仍是同一代。DynamicUser runtime 不依赖主机 D-Bus：
   它先停在只读 wrapper，不执行 Uvicorn；controller 验证 candidate unit
   的独立 `InvocationID`、实际 MainPID argv、资源限制、环境、
   `After + StopPropagatedFrom` 和 live supervisor 后，才以同目录原子
   rename 发布 root-owned `0444` start permit。permit 同时绑定 supervisor
   generation、candidate generation 与 unit 名，runtime 用 systemd 自动
   注入的 `$INVOCATION_ID` 验证 permit 后才 exec。这样同时封住了
   supervisor 崩溃时的 restart 传播和 StartTransientUnit 竞态；代码与门禁
   显式禁止重新加入
   `BindsTo`/`PartOf`，且 controller/build/runtime 都显式固定
   `Restart=no`。
   runtime 另用 transient service，只监听 `127.0.0.1:18001`，
   运行 2 个 Uvicorn worker，
   并设置 `DynamicUser=yes` 与 `ProtectSystem=strict`。验证读取 candidate
   cgroup，要求实际存活的 `spawn_main` worker 恰好为 2 个，而不只检查命令行
   声明。
4. runtime 中的 production 数据只读，数据库与 Redis 均关闭；
   `APP_JATO_MONTHLY_ENABLED=false` 和
   `APP_JATO_MONTHLY_EXECUTION_MODE=disabled` 是显式设置。验证必须看到月更
   endpoint 返回结构化 HTTP 423，且 candidate cgroup 中没有
   `jato_monthly_worker.py`。Grouped time series、Dashboard overview、
   metadata 与 advanced analysis 四类启动预热也全部显式关闭，避免 canary
   扫描大数据、写入缓存或击穿 4G 上限。
5. canary 不调用 Nginx installer、`nginx reload`、production unit/env/drop-in
   installer、scheduler reconcile、release GC 或 production checkpoint。
6. 启动前和清理后会比较公网 `/healthz`、`build-meta.json` SHA、`nginx -T`
   hash、active unit PID/命令/6G/8G/2 workers、monthly worker、scheduler
   状态，以及 active-slot、`/opt/jato/active` 和 production unit/env/drop-in
   的存在状态与指纹。旧部署中这些路径为 absent 也属于合法且必须保持的状态。
   现有公网 `/readyz` 可能为 404，所以旧服务 baseline 只使用
   `/healthz + build-meta`；candidate 自身仍必须通过精确 feature SHA
   `/readyz`。
7. 无论成功、build 失败、runtime 失败、TERM、SIGKILL 或 controller 总预算
   到期，都只会按派生 unit 名及精确 FragmentPath、完整 ExecStart argv、
   Environment、supervisor dependency 和 cgroup 身份停止子 unit。controller
   的 EXIT trap 先尝试正常收口并写 draft checkpoint；无论 controller 如何
   结束，仍存活且持锁的
   supervisor 都先证明 controller cgroup 完全静默，再在一个全新 reconcile
   进程中核验，不依赖 controller 的内存 flags。若 supervisor 自身被异常
   终止，`Restart=on-failure` 的新进程读取 checkpoint：已经进入 controller
   的任务只允许 recovery-only，且该恢复合同不要求已经按设计删除的 staged
   archive 再次存在，禁止重新 build/runtime。恢复会确认 18001 已释放，再
   在当前锁与当前 supervisor generation 下重新生成 after snapshot；跨代
   candidate、生产指纹变化、矛盾/重复 marker 或显式 stop 请求都只能生成
   failed receipt。runtime 与 staged archive 先清；
   `/opt/jato-canary` 及其 `runtime/control/sources` 父层固定为
   `root:root 0755`，所有入口都会复验无 symlink、owner/group 和 mode，
   防止 deploy 用户通过 rename 整体替换 root-owned 控制树或 staged source；
   只有按 run 派生的 runtime 叶目录按需交给 deploy 用户。
   极小的 root-owned 只读 control bundle 不由仍受 `Restart=on-failure`
   管理的 supervisor 自删，避免删除自身 ExecStart 后发生不可恢复重启。它会
   保留到 supervisor 已 inactive/collected，再由外部流程在验证 terminal
   receipt、端口和全部子 unit 后清理。持久 run ID 不可复用。`passed` 还必须存在唯一
   `controller_completed/completed` marker；`expected_failure_verified`
   必须存在唯一 `fault_observed/completed` marker；所有 terminal receipt
   都必须以幂等且唯一的 `supervisor_reconciled/completed` 为 checkpoint
   末态。任一 production 指纹、unit 身份、清理、writer generation 或
   evidence 不完整都会失败并保留 control 供检查。

服务器上的调用形式：

```bash
archive="$HOME/.cache/jato-canary/archives/<feature-sha>/<archive-sha256>.tar.gz"

DEPLOY_STATE_DIR="$HOME/.local/state/jato-production-release" \
CANARY_REPOSITORY="tristan419/JATO_Analysis_System" \
CANARY_BRANCH="codex/tencent-bluegreen-release" \
CANARY_COMMIT_SHA="<真实 40 位 feature SHA>" \
CANARY_SOURCE_ARCHIVE="$archive" \
CANARY_SOURCE_BYTES="<stat 得到的字节数>" \
CANARY_SOURCE_SHA256="<sha256sum 得到的摘要>" \
CANARY_RUN_ID="<唯一 canary id>" \
bash 03_Scripts/deploy/tencent_feature_candidate_canary.sh launch
```

命令快速返回后，必须继续轮询 supervisor unit 与 receipt；只有 receipt
终态才能判定结果：

```text
/run/systemd/transient/jato-feature-canary-supervisor-<run-key>.service
/run/systemd/transient/jato-feature-canary-controller-<run-key>.service
/var/lib/jato-canary/checkpoints/<run-key>.json
/var/lib/jato-canary/evidence/<run-key>.json
/var/lib/jato-canary/receipts/<run-key>.json
```

正常成功要求：

- supervisor、controller、build、runtime 四个 transient unit 均已停止/collect；
- `127.0.0.1:18001` 无监听；
- runtime 与 staged source 两个临时路径均不存在；root-owned control
  evidence 仍存在且不可写，待 supervisor collect 后再外部清理；
- receipt 为 `outcome=passed`，`terminalWriter=supervisor_reconcile`，
  candidate evidence/start permit 中的 supervisor generation 与 writer
  `InvocationID` 相同，permit 的 candidate generation 与 candidate
  systemd `InvocationID` 相同，checkpoint 末态为
  `supervisor_reconciled/completed`；
- embedded before/after 除采集时间外完全相同。

受控失败演练增加 `CANARY_FAULT=after_candidate_start` 并使用另一个从未用过
的 run ID。controller 会产生内部故障退出码；只有 cleanup 和 production
before/after comparison 全部通过后，checkpoint/receipt 才收敛为
`expected_failure_verified`。外层 launch 命令的退出码仍只代表 systemd
是否接受 supervisor，不能替代 receipt。公网旧服务必须始终健康。这个演练
不会批准、发布或重试任何 JATO 月更数据任务。

不要用 `systemctl stop` 模拟 supervisor 故障：systemd 的
`Restart=on-failure` 不会把显式 stop 当成失败重启。耐久性预检或故障注入
必须对 supervisor 的 MainPID 使用 `systemctl kill --kill-whom=main -s KILL`
（并使用专门、从未复用的 preflight/canary run ID），随后以 terminal receipt
和全部子 unit 已 collect 为准。显式 stop 仅用于受控运维终止；它会依赖
`TimeoutStopSec` 内的 signal trap 尝试收口，并且即使 candidate marker
已经齐全也只能写 failed receipt，不能作为重启能力或 canary 通过证明。
