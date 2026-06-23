# JATO Fullstack 腾讯云 Ubuntu 部署手册

本文对应当前 Fullstack 方案，不走 Docker。生产形态固定为：

- React + Vite 前端先构建成静态文件
- nginx 负责静态文件和反向代理
- FastAPI 后端通过 systemd 常驻
- 服务器更新通过 SSH 拉代码并执行发布脚本

如果你想尽量少输命令，当前推荐直接使用：`bash 03_Scripts/tencent_fullstack_bootstrap.sh`。

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
curl -# -L --connect-timeout 15 --max-time 600 https://codeload.github.com/tristan419/JATO_Analysis_System/tar.gz/refs/heads/main -o JATO_Analysis_System-main.tar.gz
tar -xzf JATO_Analysis_System-main.tar.gz
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
curl -# -L --connect-timeout 15 --max-time 600 https://codeload.github.com/tristan419/JATO_Analysis_System/tar.gz/refs/heads/main -o JATO_Analysis_System-main.tar.gz
scp JATO_Analysis_System-main.tar.gz root@<你的腾讯云IP>:/opt/

# 在腾讯云服务器上执行
cd /opt
tar -xzf JATO_Analysis_System-main.tar.gz
cd /opt/JATO_Analysis_System-main
```

## 3.1 一键初始化、部署并启动

完成仓库拉取后，直接执行：

```bash
cd /opt/JATO_Analysis_System-main
bash 03_Scripts/tencent_fullstack_bootstrap.sh
```

常用覆盖参数：

```bash
cd /opt/JATO_Analysis_System-main
SERVER_NAME=your.domain.com \
BACKEND_PORT=8000 \
APP_BACKEND_WORKERS=2 \
APP_AUTH_ENABLED=false \
VITE_API_BASE=/v1 \
bash 03_Scripts/tencent_fullstack_bootstrap.sh
```

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

部署脚本每次都会把这些 unit / timer 同步到 `/etc/systemd/system`，并重启 timer 以应用新的时间表。

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
- Environment / deploy script: `REPO_ARCHIVE_URL`

推荐默认值：

- `DEPLOY_REPO_DIR=/opt/JATO_Analysis_System-main`
- `FULLSTACK_BACKEND_SERVICE_NAME=jato-fullstack-backend@8000`
- `DEPLOY_BRANCH=main`
- `DEPLOY_SERVER_NAME=ojeur.cloud www.ojeur.cloud`
- `DEPLOY_ENABLE_HTTPS=true`
- `FULLSTACK_VITE_API_BASE=/v1`
- `VITE_ASSET_BASE_URL=`（未接 CDN 时留空；接海外 CDN 后填 CDN 域名并保留结尾 `/`）
- `REPO_REMOTE_URL=https://gitclone.com/github.com/tristan419/JATO_Analysis_System.git`
- `REPO_ARCHIVE_URL=https://codeload.github.com/tristan419/JATO_Analysis_System/tar.gz/refs/heads/main`

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
