# MSRP 本地同步自动化

## 双击入口

已经提供 Finder 可双击入口：

```bash
03_Scripts/DoubleClick/sync_msrp_db_to_cloud.command
```

它会调用：

```bash
03_Scripts/schedule_msrp_sync_to_cloud.sh
```

后者会负责：

- 补 Homebrew PostgreSQL 客户端 PATH
- 载入本地 `.venv`
- 加 `flock` 锁，避免重复同步
- 把日志写到 `03_Scripts/logs/`
- 调用 `03_Scripts/sync_msrp_db_to_cloud.sh`

> 2026-04-21 补记：本地 MSRP 源库不要再假设是 `5432`。同步脚本现在会优先读取
> `06_AppPlatform/.runtime/postgres.env` 里的 `APP_DATABASE_URL`，也就是本地
> fullstack 当前实际在用的 PostgreSQL（例如 Docker `5433`），以避免把空库同步到腾讯云。

## 本机定时执行

推荐本机用 macOS `launchd`，因为当前 MSRP 源库在本机 PostgreSQL；云端“主动拉取”本地库会涉及暴露数据库端口或反向隧道，稳定性和安全面都更差。

如果仓库本身还放在 `Downloads/` 下，安装脚本会自动切到 `terminal-bridge` 模式：

- `launchd` 只负责按时触发
- 真正执行同步的是 Terminal
- 这是为了绕过 macOS 对后台进程直接访问 `Downloads/` 的限制
- 触发时通常会打开一个 Terminal tab 或 window，这是预期行为

安装每天 03:20 自动同步：

```bash
JATO_MSRP_ALERT_EMAIL='you@example.com' bash 03_Scripts/install_local_msrp_sync_launchd.sh 3 20
```

改成别的时间，直接重跑一次安装脚本即可：

```bash
JATO_MSRP_ALERT_EMAIL='you@example.com' bash 03_Scripts/install_local_msrp_sync_launchd.sh 1 45
```

如果不想发邮件，只保留失败摘要文件和本机通知：

```bash
bash 03_Scripts/install_local_msrp_sync_launchd.sh 3 20
```

立即触发一次：

```bash
launchctl kickstart -k gui/$(id -u)/com.jato.msrp-db-sync
```

查看任务状态：

```bash
launchctl print gui/$(id -u)/com.jato.msrp-db-sync
```

卸载：

```bash
launchctl bootout gui/$(id -u) "$HOME/Library/LaunchAgents/com.jato.msrp-db-sync.plist"
rm -f "$HOME/Library/LaunchAgents/com.jato.msrp-db-sync.plist"
```

## 日志位置

时间戳日志：

```bash
03_Scripts/logs/msrp-db-sync-*.log
```

最近一次日志软链接：

```bash
03_Scripts/logs/msrp-db-sync-latest.log
```

launchd 标准输出/错误：

```bash
$HOME/Library/Logs/JATO_Analysis_System/msrp-db-sync-launchd.out.log
$HOME/Library/Logs/JATO_Analysis_System/msrp-db-sync-launchd.err.log
```

最近一次失败摘要：

```bash
03_Scripts/logs/msrp-db-sync-last-failure.txt
```

## 失败告警行为

`03_Scripts/schedule_msrp_sync_to_cloud.sh` 在失败时会做三件事：

1. 写固定失败摘要到 `03_Scripts/logs/msrp-db-sync-last-failure.txt`
2. 如果配置了 `JATO_MSRP_ALERT_EMAIL`，通过本机 `mail` 命令提交邮件告警
3. 触发一次 macOS 本机通知

注意：邮件发送依赖本机可用的 `/usr/bin/mail` 或你指定的 `JATO_MSRP_ALERT_MAIL_BIN`。脚本会尽力提交邮件，但最终投递仍取决于这台 Mac 的本地邮件能力或 MTA 配置。

## 为什么不默认做云端拉取

当前已经验证稳定的链路是：

1. 本机 `pg_dump`
2. `scp` 到腾讯云
3. 云端 `pg_restore`
4. 后端健康检查

如果改成腾讯云主动拉本机 PostgreSQL，至少还要额外解决：

1. 本机数据库对外可达
2. 动态公网 IP 或 DNS
3. 反向隧道或端口暴露的安全控制
4. 本机离线时的失败重试与告警

所以当前建议是：

- MSRP 数据库同步：本机 `launchd` 定时推送
- MSRP 抓取执行：腾讯云继续用现有 `cron/systemd` 跑 `03_Scripts/run_msrp_low_concurrency.sh`
