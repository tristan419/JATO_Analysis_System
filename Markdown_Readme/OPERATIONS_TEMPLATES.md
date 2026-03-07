# 运营模板（回归 / 验收 / 发布 / 监控）

> 文档定位：运营与发布过程中的标准模板与执行清单。
> 返回总览：[ROADMAP（总览导航）](./ROADMAP.md)

## A. 回归测试清单模板

### 数据层

- [ ] `python 03_Scripts/elt_worker.py` 成功
- [ ] `python 03_Scripts/build_partitioned_dataset.py --overwrite` 成功
- [ ] `04_Processed_data/manifest.json` 存在
- [ ] `04_Processed_data/partitioned_dataset_v1/manifest.json` 存在

### 应用层

- [ ] `streamlit run 05_DashBoard/app.py` 首屏无报错
- [ ] 侧边栏筛选（国家/细分/动总/品牌/Model/Version）可用
- [ ] 年度/月度两大 tab 正常
- [ ] 增强分析 9 图可逐一打开
- [ ] 明细预览、CSV 下载正常

### 性能层

- [ ] `PYTHONPATH=05_DashBoard python 03_Scripts/benchmark_dashboard_load.py --repeats 3`
- [ ] 投影读取平均耗时低于全列读取
- [ ] 大数据模式打开时，内存估算在阈值内

---

## B. 上线前验收清单

- [ ] 业务关键筛选场景验收（至少 5 个）
- [ ] 关键指标口径验收（销量、MSRP、份额）
- [ ] 权限与密钥检查（不含明文 secrets）
- [ ] 回滚方案准备（上一个稳定版本可一键恢复）
- [ ] 运维联系人与告警接收群已确认

---

## C. 发布说明模板

```markdown
# Release vX.Y.Z

## 1. 本次范围
- 读取层优化：
- 图表改动：
- 文档改动：

## 2. 兼容性
- 数据格式兼容性：
- 配置项变更：

## 3. 性能结果
- 侧边栏读取 avg:
- 分析投影读取 avg:
- 全列读取 avg:
- 投影加速比:

## 4. 风险与回滚
- 主要风险：
- 回滚步骤：

## 5. 验收结论
- 结论：通过 / 有条件通过 / 不通过
```

---

## D. 监控指标与告警清单

### 指标（建议）

- 首屏加载耗时（P50/P95）
- 侧边栏读取耗时
- 分析读取耗时
- 明细加载耗时
- 每会话内存估算
- 错误率（4xx/5xx 或异常数）

### 告警阈值（建议起步）

- 首屏 P95 > 8s（持续 5 分钟）
- 错误率 > 2%（持续 5 分钟）
- 进程内存 > 80%（持续 3 分钟）
- 数据源检查告警（缺 parquet / 缺 manifest）出现即告警

---

## E. 历史归档清理策略（可执行模板）

### 默认策略（建议）

- 归档目录：`01_RAW_DATA/historyDataArchive`
- 时间保留：保留最近 `180` 天
- 数量保留：额外保留最新 `20` 个文件（不受天数约束）
- 执行频率：每周一次（建议低峰时段）

### 执行命令

先 dry-run 预览：

```bash
python 03_Scripts/cleanup_history_archive.py \
  --target-dir 01_RAW_DATA/historyDataArchive \
  --keep-latest 20 \
  --keep-days 180
```

确认后执行删除：

```bash
python 03_Scripts/cleanup_history_archive.py \
  --target-dir 01_RAW_DATA/historyDataArchive \
  --keep-latest 20 \
  --keep-days 180 \
  --apply
```

### 回滚与审计建议

- 清理前先备份文件列表（文件名、大小、mtime）到运维记录。
- 若误删，优先从对象存储/离线备份恢复。
- 每次执行后记录输出摘要：总文件、保留数、删除数。

---

## F. 补档数据合并运行模板（15+5 国家场景）

### 适用场景

- 主包：`JATO-2026.1.xlsx`（例如 15 国）
- 补档：`JATO-2026.1 (1).xlsx`（例如新增 5 国）

### 执行前检查

- [ ] 两个文件 sheet 名一致（默认 `Data Export`）
- [ ] 文件已放入 `01_RAW_DATA/`
- [ ] 去重策略已确认（是否需要 `--dedupe-keys`）

### 推荐执行命令

自动合并 raw 全部 xlsx：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

存在重复记录时：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --dedupe-keys "国家,make,model,version name" \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

### 执行后验证

- [ ] 检查 `04_Processed_data/refresh_job_report.json` 中 `jobStatus=success`
- [ ] 检查 `incremental.fingerprintMatched=false`（有新增补档时应触发刷新）
- [ ] 检查 `04_Processed_data/manifest.json` 的 `mergeSummary.sourceFileCount`
- [ ] Dashboard 侧边栏确认新增国家可见

### 冲突与回滚验收（建议）

- [ ] 先执行一次 `--conflict-policy fail`，确认冲突可被阻断并输出冲突报告
- [ ] 再执行一次 `--conflict-policy last_wins`，确认产物可落盘并可追溯
- [ ] 若任务失败，确认日志出现“已自动回滚”提示
- [ ] 检查回滚后全量/分区产物可正常读取（无半成品目录）
