# 腾讯云 Ubuntu 低带宽部署手册（不改首屏策略）

本文针对 12Mbps 带宽场景，聚焦以下优化：

1. Nginx 压缩与缓存
2. CDN 前置
3. 定时数据刷新（预处理）

部署前提：本项目当前只按腾讯云 Ubuntu 服务器部署，不再使用 Docker 作为上线路径；服务端通过 systemd + nginx 运行。

## 1) Nginx 压缩与缓存

仓库已提供模板与安装脚本：

- 03_Scripts/deploy/nginx/jato_dashboard.conf.example
- 03_Scripts/deploy/nginx/install_jato_nginx.sh

在服务器执行：

```bash
cd /opt/JATO_Analysis_System
sudo chmod +x 03_Scripts/deploy/nginx/install_jato_nginx.sh
sudo SERVER_NAME=_ APP_PORT=8501 bash 03_Scripts/deploy/nginx/install_jato_nginx.sh
```

如果你已经有域名，把 `SERVER_NAME` 改成域名并配置 DNS A 记录。

## 2) CDN 前置（推荐）

如果已备案并使用腾讯云 CDN：

1. CDN 控制台新增加速域名，例如 `dash.example.com`
2. 源站填你的服务器公网 IP 或回源域名
3. 协议建议：回源 HTTP（内网/同地域时可选 HTTPS）
4. 缓存规则建议：
   - `/static/*`：缓存 7 天
   - 其余路径：不缓存或极短缓存
5. 回源 Host 头：保持与源站 Nginx `server_name` 一致
6. 开启 Gzip/Brotli（按 CDN 支持）

如果暂未备案，可临时用 Cloudflare（海外访问链路优先）：

1. DNS 托管到 Cloudflare
2. A 记录指向服务器 IP
3. 缓存规则：`/static/*` 缓存，动态路径绕过

## 3) 定时数据刷新（预处理）

### 3.1 创建系统服务

新建 `/etc/systemd/system/jato-refresh.service`：

```ini
[Unit]
Description=JATO nightly data refresh
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/opt/JATO_Analysis_System
ExecStart=/opt/JATO_Analysis_System/.venv/bin/python /opt/JATO_Analysis_System/03_Scripts/run_data_refresh_job.py
```

### 3.2 创建定时器

新建 `/etc/systemd/system/jato-refresh.timer`：

```ini
[Unit]
Description=Run JATO refresh every night

[Timer]
OnCalendar=*-*-* 03:30:00
Persistent=true

[Install]
WantedBy=timers.target
```

### 3.3 启用

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now jato-refresh.timer
sudo systemctl list-timers --all | grep jato-refresh
```

手动触发一次：

```bash
sudo systemctl start jato-refresh.service
sudo journalctl -u jato-refresh.service -n 100 --no-pager
```

## 4) 自动拉取并重启（GitHub Push）

仓库已有工作流：`.github/workflows/deploy-ec2-auto-update.yml`（已改为通用 SSH 部署）。

说明：该工作流对应的目标机器是腾讯云 Ubuntu 实例，部署时直接在主机上更新代码并重启 systemd 服务，不经过 Docker。

确保以下 Secret/Variable 已配置：

- Secrets: `SSH_HOST`, `SSH_USER`, `SSH_PRIVATE_KEY`
- Variables: `DEPLOY_REPO_DIR`, `SYSTEMD_SERVICE_NAME`, `DASHBOARD_PORT`

## 5) 验证项

```bash
curl -I http://127.0.0.1/
curl -I http://127.0.0.1/static/
curl -fsS http://127.0.0.1/healthz
```

浏览器开发者工具检查：

- 静态资源是否命中缓存（`cache-control`）
- websocket `/_stcore/stream` 是否正常 101
- 首屏后网络请求是否显著减少

## 6) 动态聚合灰度（复杂筛选兜底）

当前代码已支持三段路由：`预聚合 -> 动态聚合 -> 原始切片`。

- 开关环境变量：`JATO_DYNAMIC_AGGREGATION_ENABLED=true`
- `restart_dashboard.sh` 与 systemd 模板已默认开启该开关。

验证方法：

1. 打开 Dashboard，在顶部状态行观察 `路径` 字段。
2. 无筛选或低复杂筛选时，应看到 `路径: precomputed`。
3. 多维筛选（2 个及以上维度）时，应优先看到 `路径: dynamic-aggregate`。
4. 若动态聚合不可用，自动回退 `路径: raw`，页面功能不受影响。
