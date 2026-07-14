# JATO Production Backup & Restore Runbook

> 2026-05-24 | 生产数据保护方案

## 当前数据分布

| 数据 | 存储 | 备份现状 |
|------|------|---------|
| Order Genius (BOM/FOB/order/lifecycle) | PostgreSQL `ordering` schema | PostgreSQL custom dump |
| Auth (users/roles) | PostgreSQL `auth` schema | PostgreSQL custom dump |
| MSRP source governance | PostgreSQL `msrp` schema | PostgreSQL custom dump |
| MSRP replayable evidence | `04_Processed_data/ops/msrp_source_evidence/` | 内容寻址对象归档 + 完整性报告 |
| Market Scan / JATO | PostgreSQL + Parquet `04_Processed_data/` | Parquet 有文件，PG 无备份 |
| COC Match 历史 | SQLite `04_Processed_data/ops/coc_match/` | runtime ops archive |
| Order Genius 上传文件 | `04_Processed_data/ops/order_genius_uploads/` | runtime ops archive |
| Pipeline 状态 | `hermes/reports/pipeline_status/` | ❌ 无自动备份 |

## 备份策略

### 1. 统一备份脚本

```bash
REPO_DIR=/opt/JATO_Analysis_System-main \
BACKEND_ENV_FILE=/etc/jato-fullstack/backend.env \
/opt/JATO_Analysis_System-main/03_Scripts/ops/backup_production_data.sh
```

脚本会从 `BACKEND_ENV_FILE` 读取 `APP_DATABASE_URL`，不会把数据库密码写在仓库文档或脚本里。

输出：
- PostgreSQL dump: `/opt/backups/jato/pg/jato-*.dump`
- runtime ops artifacts: `/opt/backups/jato/ops/ops-*.tar.gz`
- MSRP evidence archive: `/opt/backups/jato/msrp-evidence/msrp-evidence-*.tar.gz`
- MSRP evidence integrity report: `/opt/backups/jato/integrity/msrp-evidence-*.json`
- manifest: `/opt/backups/jato/manifests/backup-*.json`

同一次备份的四类核心产物使用相同 timestamp。manifest 记录 PostgreSQL
dump、evidence archive 和 integrity report 的绝对路径、SHA-256 与字节数，
并记录可重放 evidence object 数和对象原始总字节数。PostgreSQL dump 固定包含
`auth`、`ordering`、`public` 和 `msrp` schema。evidence 完整性不健康时不会发布
manifest，脚本以非零状态退出。

默认 evidence root 为：

```text
$APP_PROJECT_ROOT/04_Processed_data/ops/msrp_source_evidence
```

可通过 `MSRP_GOVERNANCE_EVIDENCE_ROOT` 覆盖，但该路径必须位于 release
替换目录外。`official_url` 和仅用于视觉佐证的 `screenshot` 不属于可重放对象，
不会进入 evidence archive。

### MSRP evidence 只读完整性检查

```bash
set -a
source /etc/jato-fullstack/backend.env
set +a
/opt/JATO_Analysis_System-main/.venv/bin/python \
  /opt/JATO_Analysis_System-main/03_Scripts/ops/msrp_evidence_integrity.py \
  --output /tmp/msrp-evidence-integrity.json
```

命令只读取 `msrp.source_evidence_assets` 和 evidence root，不修改数据库或对象。
它校验普通文件、可读性、DB size、完整 SHA-256、missing、mismatch、路径越界与
orphan；任一不健康状态返回非零退出码。

默认保留 30 天，可用 `BACKUP_RETENTION_DAYS` 覆盖。

### 2. Cron 安装

```bash
sudo REPO_DIR=/opt/JATO_Analysis_System-main \
BACKEND_ENV_FILE=/etc/jato-fullstack/backend.env \
/opt/JATO_Analysis_System-main/03_Scripts/ops/install_backup_cron.sh
```

默认 cron：

```text
0 2 * * * REPO_DIR=/opt/JATO_Analysis_System-main BACKEND_ENV_FILE=/etc/jato-fullstack/backend.env /opt/JATO_Analysis_System-main/03_Scripts/ops/backup_production_data.sh >> /var/log/jato-backup.log 2>&1
```

### 3. Deploy 前自动备份

`deploy_fullstack_server.sh` 会在 Alembic migration 前调用 `backup_production_data.sh`。

环境变量：
- `RUN_PRE_DEPLOY_BACKUP=auto`：默认，失败时报警但不中断部署
- `RUN_PRE_DEPLOY_BACKUP=true`：失败时中断部署
- `RUN_PRE_DEPLOY_BACKUP=false`：跳过

### 4. 关键事件触发备份

在以下操作前，先执行 pg_dump：
- Order Genius `publish baseline`
- Payment term 勘误 (`PATCH /payment-terms/countries/{id}` with correction=true)
- BOM lifecycle 变更
- Alembic migration (`alembic upgrade head`)

## 恢复流程

### MSRP 一致性恢复顺序

数据库行和 evidence objects 必须使用同一 timestamp 的 manifest 恢复。恢复期间
backend、MSRP scheduler 和其他写入者必须保持停止；不要先启用 API 再补对象。

```bash
# 1. 停止所有可能写入 MSRP 的服务
sudo systemctl stop jato-fullstack-backend@8000
sudo systemctl stop jato-msrp-dryrun.timer jato-msrp-ingest.timer

# 2. 校验 manifest 内三个核心 artifact 的 SHA-256
sha256sum /opt/backups/jato/pg/jato-<timestamp>.dump
sha256sum /opt/backups/jato/msrp-evidence/msrp-evidence-<timestamp>.tar.gz
sha256sum /opt/backups/jato/integrity/msrp-evidence-<timestamp>.json

# 3. 先将对象解压到隔离 staging，不覆盖现存 evidence root
sudo rm -rf /opt/restore/msrp_source_evidence
sudo mkdir -p /opt/restore/msrp_source_evidence
sudo tar xzf /opt/backups/jato/msrp-evidence/msrp-evidence-<timestamp>.tar.gz \
  -C /opt/restore/msrp_source_evidence

# 4. 恢复包含 msrp schema 的同 timestamp PostgreSQL dump
pg_restore -d "postgresql://jato_app:<pwd>@127.0.0.1:5432/jato_app" \
  --clean --if-exists \
  /opt/backups/jato/pg/jato-<timestamp>.dump

# 5. 原子切换 staging evidence root，再运行只读完整性检查
sudo mv /opt/JATO_Analysis_System-main/04_Processed_data/ops/msrp_source_evidence \
  /opt/JATO_Analysis_System-main/04_Processed_data/ops/msrp_source_evidence.pre-restore
sudo mv /opt/restore/msrp_source_evidence \
  /opt/JATO_Analysis_System-main/04_Processed_data/ops/msrp_source_evidence
set -a; source /etc/jato-fullstack/backend.env; set +a
/opt/JATO_Analysis_System-main/.venv/bin/python \
  /opt/JATO_Analysis_System-main/03_Scripts/ops/msrp_evidence_integrity.py

# 6. 仅在 integrity 返回 0 后重新启用写路径
sudo systemctl start jato-fullstack-backend@8000
sudo systemctl start jato-msrp-dryrun.timer jato-msrp-ingest.timer
curl http://127.0.0.1:8000/healthz
```

### PostgreSQL 通用恢复

```bash
# 1. 停止 backend
sudo systemctl stop jato-fullstack-backend@8000

# 2. 恢复
pg_restore -d "postgresql://jato_app:<pwd>@127.0.0.1:5432/jato_app" \
  --clean --if-exists \
  /opt/backups/jato/pg/jato-20260524-020000.dump

# 3. 重启 backend
sudo systemctl start jato-fullstack-backend@8000
curl http://127.0.0.1:8000/healthz
```

### Ops 文件恢复

```bash
cd /opt/JATO_Analysis_System-main/04_Processed_data/ops
tar xzf /opt/backups/jato/ops/ops-20260524-030000.tar.gz
```

### Excel 重新上传恢复

Order Genius BOM 底表可以通过重新上传 Material Excel 恢复基础数据。但以下数据无法通过上传恢复：
- 订单数量 (`order_quantity_cell`)
- FOB 快照
- Payment term 勘误历史
- 物料生命周期记录

这些必须从数据库备份恢复。

## 生产服务器配置步骤

```bash
# 1. 安装 pg_dump (如果未安装)
sudo apt-get install -y postgresql-client-16

# 2. 配置 cron
sudo REPO_DIR=/opt/JATO_Analysis_System-main \
BACKEND_ENV_FILE=/etc/jato-fullstack/backend.env \
/opt/JATO_Analysis_System-main/03_Scripts/ops/install_backup_cron.sh

# 3. 立即执行一次备份
sudo REPO_DIR=/opt/JATO_Analysis_System-main \
BACKEND_ENV_FILE=/etc/jato-fullstack/backend.env \
/opt/JATO_Analysis_System-main/03_Scripts/ops/backup_production_data.sh
```

## 当前状态验证

```
✅ PostgreSQL: 235 SKUs, 1137 FOB rows, 7 order cells, 5 users
✅ Migrations: at head (20260523_0023)
✅ COC: 5 runs in SQLite, 32KB
✅ OG uploads: 7 sessions preserved
✅ Admin: user=admin role=admin active=true
✅ Auth: enabled, token-based with session store
⚠️  DB URL discrepancy: backend.env has port 5433, actual DB on 5432
⚠️  sqlite3 not installed on server → COC queries fail
```
