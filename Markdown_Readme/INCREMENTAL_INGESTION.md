# 增量入库方案预研（v1）

> 文档定位：增量入库方案、里程碑与当前落地状态说明。
> 返回总览：[ROADMAP（总览导航）](./ROADMAP.md)

## 目标

在不重跑全量 ETL 的前提下，仅处理新增/变更数据，缩短刷新时延并降低资源消耗。

## 现状

- 当前流程：`Excel -> 全量 Parquet -> 分区重建`
- 优点：实现简单、一致性高
- 问题：数据量上升后，全量重建成本高

## 候选方案

### 方案 A：基于快照对比（推荐起步）

1. 为每次输入快照生成指纹（文件哈希 + 行数 + 时间戳）
2. 若指纹未变，直接跳过 ETL
3. 若变更，按业务键（国家/品牌/Model/Version + 时间列）做增量对比
4. 仅写入变更分区

优点：改造小、风险低；缺点：仍依赖业务键稳定。

### 方案 B：事件日志（CDC 风格）

- 记录新增/更新/删除事件并驱动分区 upsert

优点：实时性高；缺点：实现复杂度较高。

## 推荐里程碑

- M1：先做“输入快照指纹 + 变更检测跳过”（✅ 已完成）
- M2：补“按分区增量重写”（✅ 已完成）
- M3：补“冲突检测与回滚”（🚧 v1 已落地）
- P3：增量细粒度报告（🚧 v1 已落地）

## 最小落地清单

- [x] 新增 `dataset_fingerprint.json`
- [x] 在 `run_data_refresh_job.py` 接入 skip-unchanged
- [x] 增加增量模式参数 `--incremental`
- [x] 增加增量回归报告字段（变更行数、变更分区数）

## 已落地能力（2026-03-08）

- `run_data_refresh_job.py` 新增参数：
  - `--incremental`
  - `--skip-unchanged`
  - `--fingerprint`
  - `--conflict-keys`
  - `--conflict-policy`（`report_only` / `fail` / `last_wins`）
  - `--no-rollback`（默认开启自动回滚，可显式关闭）
  - `--keep-backup`（保留回滚备份目录）
- 当输入源集合的指纹（文件列表+各文件路径/大小/mtime/sheet/分区列）未变化时，
  作业状态返回 `skipped_unchanged`，并跳过 ETL 与分区重建。
- 支持多 Excel 合并场景（`--input-files` 或 `--merge-all-xlsx`），
  当新增/删除补档文件时会自动触发刷新，不会误判为 unchanged。
- 产物报告 `refresh_job_report.json` 已新增 `incremental` 字段，
  记录是否启用、是否命中跳过、指纹文件路径等信息。
- `incremental.regression` 已包含：
  - `changedRows`
  - `changedPartitionCount`
  - `updatedPartitionDirectories`
  - `addedPartitionDirectories` / `removedPartitionDirectories`
- `incremental.regression`（P3-v1）新增：
  - `partitionKeyColumn` / `changedPartitionKeys`
  - `addedPartitionKeys` / `updatedPartitionKeys` / `removedPartitionKeys`
  - `changedCountries`（当分区列为国家时自动产出）
  - `affectedPartitionRowsEstimate`（按分区变化估算受影响行数）
  - `mergeKeyRegression`（dedupe/conflict key 与 dropped/conflict 指标变化）
- `build_partitioned_dataset.py --incremental` 已支持：
  - 基于 `partitionStats(rows+signature)` 识别分区变更
  - 仅重写新增/变更分区，并清理已删除分区

## M3 v1 使用建议（冲突检测与回滚）

先检查冲突，不自动改写：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --conflict-keys "国家,make,model,version name" \
  --conflict-policy fail \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

允许“后文件覆盖前文件”并继续产出：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --conflict-keys "国家,make,model,version name" \
  --conflict-policy last_wins \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

说明：

- 默认会在输出目录写 `conflict_report.json`（可通过 `--conflict-report` 指定路径）。
- 任务失败时默认自动回滚全量产物和分区产物；如需关闭，使用 `--no-rollback`。
