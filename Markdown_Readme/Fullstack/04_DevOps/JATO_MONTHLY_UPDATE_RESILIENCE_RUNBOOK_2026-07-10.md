# JATO 月更隔离与恢复 Runbook

日期：2026-07-10  
范围：单国家 Excel 上传、OJEUR 可用性、候选 Review/Publish、后台 worker 与本机 SSH 运维。

## 事故边界

已知事实：任务 `jato-update-27342713`（`2026-05-r5`）上传匈牙利 2026-05 数据后，Raw Compare 运行约 4916 秒并以 `-9` 结束；匈牙利 active freshness 仍为 2026-04。`-9` 只证明进程被 SIGKILL，不能单独证明 OOM。当前没有确认的 SSH 主机侧内存证据。

因此本改动不重试、不 publish、不删除该真实任务，也不修改生产 active 数据。

## 新流程

```mermaid
flowchart LR
  A[Browser chunk upload] --> B[Persisted queued job]
  B --> C[isolated systemd worker]
  C --> D{inspect country scope}
  D -- one country --> E[target-only review + refresh]
  D -- multiple countries --> F[prepare + raw compare + refresh]
  E --> G[Review]
  F --> G
  G --> H[Approve bound to candidate fingerprint]
  H --> I[Admin publish guards]
```

- FastAPI 只持久化上传、鉴权和任务状态；不再启动月更 daemon thread。
- worker 以单并发、低 CPU/IO 优先级执行；同一 country scope + month + SHA-256 只允许一个逻辑摄入，显式 Retry 保留为新的 attempt。
- 单国任务只读取目标国家的 active/candidate Parquet 分区用于 Review，并检查 schema、月份、重复键、负销量、回退、疑似翻倍、未上传国家分区签名和 candidate 指纹。
- Publish 必须已有 `approved` Review，且 Review 时的 candidate fingerprint 与当前候选完全一致。Smart Merge 会清除旧批准。

## 部署 worker（变更窗口内）

先在生产主机测量，不要直接套用固定 MemoryMax：

```bash
cd /opt/JATO_Analysis_System-main
.venv/bin/python 03_Scripts/ops/jato_monthly_worker_capacity.py
sudo install -m 0644 03_Scripts/deploy/systemd/jato-monthly-worker.service /etc/systemd/system/
sudo install -m 0644 03_Scripts/deploy/systemd/jato-monthly-worker.env.example /etc/jato-fullstack/monthly-worker.env
sudo mkdir -p /etc/systemd/system/jato-monthly-worker.service.d
sudo cp 03_Scripts/deploy/systemd/jato-monthly-worker.resource.conf.example /etc/systemd/system/jato-monthly-worker.service.d/resources.conf
# 根据 capacity 报告填写经审核的 MemoryHigh / MemoryMax；不要猜测数值。
sudo systemctl daemon-reload
sudo systemctl enable --now jato-monthly-worker.service
sudo systemctl status jato-monthly-worker.service --no-pager
```

建议保留：`CPUWeight=20`、`IOWeight=20`、`Nice=10`、`OOMScoreAdjust=500`、`TasksMax=64`。这样内核必须回收资源时，会优先牺牲 worker，不会优先牺牲 nginx/FastAPI。进程级 RLIMIT 默认不设；若启用，写入 `monthly-worker.env` 并与 systemd cgroup 上限保持一致。

## 服务器本机运维 CLI

所有 mutation 默认 dry-run，必须同时给 `--execute --confirm-job <job-id>`；命令复用同一服务层和审计日志，不提供新的公网 bypass/token。

```bash
.venv/bin/python 03_Scripts/jato_monthly_ops.py --help
.venv/bin/python 03_Scripts/jato_monthly_ops.py doctor
.venv/bin/python 03_Scripts/jato_monthly_ops.py find-upload --sha256 <sha256>
.venv/bin/python 03_Scripts/jato_monthly_ops.py --job-id <job> status
.venv/bin/python 03_Scripts/jato_monthly_ops.py --job-id <job> review
.venv/bin/python 03_Scripts/jato_monthly_ops.py --job-id <job> verify
.venv/bin/python 03_Scripts/jato_monthly_ops.py --execute --job-id <job> --confirm-job <job> --expected-country 匈牙利 --expected-month 2026-05 --expected-sha256 <sha256> approve --decision approve
```

Retry、publish、rollback 均要求同样的 `--execute` 与 job confirm；publish/rollback 可额外使用 expected country/month/SHA 防止选错任务。

## `jato-update-27342713` 的安全处理顺序

1. `doctor` 与 `status`：保留 job、上传副本和日志，记录状态；不要 retry/publish/delete。
2. 在部署独立 worker 后观察 worker heartbeat 和 `jato_etl` pipeline 状态；若仍需要恢复，先通过 `review` 确认 candidate 和匈牙利 2026-05 的目标范围。
3. 仅在人工确认输入文件 SHA、国家、月份和 Review 无 blocker 后，才创建受控的新 Retry attempt；旧 job 永远保留作事故证据。
4. 再由管理员批准 Review 后 publish；发布后检查 `/healthz`、data freshness、分区 manifest 文件数与实际 Parquet 数一致性。

## 下一 PR：原子 publish

本 PR 保留现有备份与 publish guards，不改变真实 active。下一 PR 应实现：

1. 将候选完整复制到同一文件系统下的 publish transaction 目录。
2. 校验 parquet、manifest、partition manifest、fingerprint 与 refresh report 的 checksum/计数。
3. 用版本化 active 目录 + 原子 symlink/rename 切换，而不是逐文件覆盖。
4. 切换后写 publication journal、失效 Redis/local derived cache，并对外暴露新 dataset token。
5. 失败时保留 transaction 目录和证据；rollback 只切换已验证的旧版本。

## 给 Codex / 开发代理的复用提示词

```text
任务：对 JATO 月更做可靠性改造。输入为单个国家的月度 Excel（例如匈牙利 2026-05）。

硬约束：
1) 浏览器上传只负责可续传分片和创建持久化 queued job，绝不能在 FastAPI 进程内启动长任务 thread/daemon。
2) 独立 worker 单并发执行；必须有 worker heartbeat、stale/restart reconciliation、资源被 kill 的显式 phase，并且 worker 的资源优先级低于 Web。
3) 在 worker 中先读取 Excel 表头 + 国家/月列分类：单国走专用 refresh/review，跳过全量 Raw Compare；多国继续复用 prepare/raw_compare/refresh。
4) 单国 Review 只能读取目标国家 active/candidate Parquet 数据，必须校验 schema、country scope、月份、重复键、负销量、latest regression、疑似销量翻倍、未上传国家分区不变、manifest/candidate fingerprint。
5) Publish 必须由服务端检查：success/completed、Review 已 approved、approval fingerprint 等于当前 candidate fingerprint、原有回退/翻倍/产物 guard 全部通过。Smart Merge 或 candidate 重建必须作废 approval。
6) 幂等键为 country scope + month + SHA-256；同一逻辑输入去重，Retry 是新 attempt 但保持上传副本与审计链。
7) 路由仅做鉴权/参数/响应；复用现有 monthly service、Parquet repository、publish guards、rollback、cache invalidation。不要新增公网运维 token 或绕过认证。
8) 前端清晰区分浏览器登录/网络状态和后台 worker/job 状态；流程固定为 Review → Approve Review → Publish；401/403/网络/5xx/resource-killed 要给可执行提示。
9) 为 systemd 提供低优先级 worker unit 和容量测量脚本；不得凭空写生产 MemoryMax。
10) 写单元测试覆盖单国不触发 Raw Compare、Review/approval/fingerprint、幂等、资源 killed/stale、路由权限与 UI 错误；绝不对真实生产任务 retry/publish/delete。

输出：先列事实、推断、未知项；再给改动文件、测试命令、部署步骤、回滚步骤与残余风险。中文沟通，前端布局/交互和后端技术栈/复用点必须说明。
```
