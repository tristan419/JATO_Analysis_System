# Proxy & Scraper Debug — 2026-05-15

## 问题

自 4/21 起，MSRP 自动抓取任务（dryrun + ingest）全部失败，News sync 和 VOC sync 不受影响。

## 诊断过程

### 1. MSRP 任务失败

```bash
sudo systemctl --failed
# ● jato-msrp-sync@dryrun.service  failed
# ● jato-msrp-sync@ingest.service  failed
```

日志显示两个问题：
- **Playwright 浏览器缺失**：`Executable doesn't exist at .../chrome-linux64/chrome`
- **网络不通**：`net::ERR_CONNECTION_CLOSED`

### 2. Playwright 浏览器缺失（主因）

时间线：
- **4/13** — `pip install` 了 `playwright 1.58.0` Python 包
- **从未执行** `playwright install chromium`（下载浏览器二进制）
- `~/.cache/ms-playwright/` 目录为空
- 部署脚本 `03_Scripts/ops/deploy_fullstack_server.sh` 没有 `playwright install` 步骤

修复：
1. 本地下载 Chromium 145.0.7632.6 + headless shell（共 ~280MB）
2. SCP 上传到服务器 `~/.cache/ms-playwright/chromium-1208/` 和 `chromium_headless_shell-1208/`
3. 同步到 `/root/.cache/ms-playwright/`（systemd 以 root 运行）
4. 部署脚本新增自动安装步骤

### 3. 代理失效

服务器上 mihomo (Clash Meta) 运行中但无法代理 HTTPS：
- 旧配置使用 SSR 节点，域名 `dajbxtw.naikonode1.top` 已失效（全球解析不到）
- HTTP 可代理（返回 502 说明 SSR 隧道通了），HTTPS 全部 `ECONNRESET`
- 规则 `MATCH,DIRECT` 导致大部分流量不走代理

修复：
1. 用新订阅 `https://naikosub.com/link/LPFT7gIKqNavBKoe?clash=1` 拉取 43 个 anytls 节点
2. 合并配置：新增 `auto` url-test 组（43 节点，300s 测速）
3. 规则改为 `MATCH,auto`（全量走代理）
4. 重启 mihomo → `github: 200`, `google: 200`

## 修复清单

| 项目 | 状态 | 文件 |
|------|------|------|
| Playwright 浏览器安装 | ✅ | 服务器 `~/.cache/ms-playwright/` + `/root/.cache/` |
| 部署脚本自动安装 Playwright | ✅ | `03_Scripts/ops/deploy_fullstack_server.sh` |
| Mihomo 代理节点更新 | ✅ | `/etc/mihomo/config.yaml` |
| MSRP service 代理环境变量 | 🔲 | `03_Scripts/deploy/systemd/jato-msrp-sync@.service` |

## 服务器关键信息

| 项目 | 值 |
|------|-----|
| SSH | `ssh -i ~/.ssh/tencent_lh.pem ubuntu@150.158.141.14` |
| Mihomo 代理 | `127.0.0.1:7897` (mixed-port) |
| Mihomo 控制台 | `127.0.0.1:9090` |
| Mihomo 配置 | `/etc/mihomo/config.yaml` |
| Playwright 版本 | 1.58.0 |
| Chromium 版本 | 145.0.7632.6 (v1208) |
| Playwright cache (ubuntu) | `~/.cache/ms-playwright/` |
| Playwright cache (root) | `/root/.cache/ms-playwright/` |

## 日常维护

### 更新代理订阅

```bash
# 本地拉取
curl -s 'https://naikosub.com/link/LPFT7gIKqNavBKoe?clash=1' -o /tmp/sub.yaml

# 用 Python 合并（保留 auto url-test 组 + MATCH,auto 规则）
# SCP 上传并重启
scp -i ~/.ssh/tencent_lh.pem /tmp/mihomo_merged.yaml ubuntu@150.158.141.14:/tmp/
ssh -i ~/.ssh/tencent_lh.pem ubuntu@150.158.141.14 "
  sudo cp /tmp/mihomo_merged.yaml /etc/mihomo/config.yaml
  sudo systemctl restart mihomo
"
```

### Playwright 浏览器重装

```bash
ssh -i ~/.ssh/tencent_lh.pem ubuntu@150.158.141.14 "
  export http_proxy=http://127.0.0.1:7897 https_proxy=http://127.0.0.1:7897
  /opt/JATO_Analysis_System-main/.venv/bin/playwright install chromium
  sudo mkdir -p /root/.cache/ms-playwright
  sudo cp -a ~/.cache/ms-playwright/. /root/.cache/ms-playwright/
"
```

### 检查抓取任务

```bash
# 查看 timer 状态
sudo systemctl list-timers | grep jato

# 查看最近日志
sudo journalctl -u jato-msrp-sync@dryrun --no-pager -n 30
sudo journalctl -u jato-country-news-sync --no-pager -n 10
sudo journalctl -u jato-voc-forum-sync --no-pager -n 10

# 失败服务
sudo systemctl --failed
```
