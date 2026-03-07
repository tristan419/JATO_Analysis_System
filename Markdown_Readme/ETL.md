# JATO 数据处理 Pipeline（四件套落地版）

> 文档定位：数据处理 Pipeline 的标准执行手册（Raw -> ETL -> 分区 -> 刷新）。
> 返回总览：[ROADMAP（总览导航）](./ROADMAP.md)

## 目录分层约定

- 原始数据目录：`01_RAW_DATA/`
- 处理脚本目录：`03_Scripts/`
- 处理结果目录：`04_Processed_data/`

当前关键脚本：

- `03_Scripts/elt_worker.py`
  - Excel -> 全量 Parquet
  - 自动生成全量数据 manifest
- `03_Scripts/build_partitioned_dataset.py`
  - 全量 Parquet -> 分区数据集
  - 自动生成分区数据 manifest
- `03_Scripts/run_data_refresh_job.py`
  - 一键执行 ETL + 分区 + 校验 + 可选 benchmark
- `03_Scripts/style_check.py`
  - 轻量代码风格检查（Tab/行尾空白/行长）
- `03_Scripts/ci_smoke_check.py`
  - 本地/CI smoke：style + py_compile + 核心脚本 `--help`
- `03_Scripts/cleanup_history_archive.py`
  - 历史归档清理（支持 dry-run / apply）


## Pipeline 步骤总览（从原始到可部署）

### Step 1: 原始数据入仓（Raw）

将文件放入：`01_RAW_DATA/`

推荐命名：`JATO-2026.1.xlsx`


### Step 2: 执行 ETL（Excel -> Parquet）

默认执行（优先读取 `01_RAW_DATA/JATO-2026.1.xlsx`，否则自动选最新 xlsx）：

```bash
python 03_Scripts/elt_worker.py
```

可选参数执行：

```bash
python 03_Scripts/elt_worker.py \
  --input 01_RAW_DATA/JATO-2026.1.xlsx \
  --output 04_Processed_data/jato_full_archive.parquet \
  --manifest 04_Processed_data/manifest.json \
  --sheet "Data Export"
```

产物：

- `04_Processed_data/jato_full_archive.parquet`
- `04_Processed_data/manifest.json`


### Step 3: 构建分区数据集（Parquet -> Partitioned Parquet）

默认按“国家”分区：

```bash
python 03_Scripts/build_partitioned_dataset.py --overwrite
```

指定分区列（可多列）：

```bash
python 03_Scripts/build_partitioned_dataset.py \
  --input 04_Processed_data/jato_full_archive.parquet \
  --output-dir 04_Processed_data/partitioned_dataset_v1 \
  --partition-cols "国家,细分市场（按车长）" \
  --overwrite
```

按分区增量重写（仅重写变化分区）：

```bash
python 03_Scripts/build_partitioned_dataset.py \
  --input 04_Processed_data/jato_full_archive.parquet \
  --output-dir 04_Processed_data/partitioned_dataset_v1 \
  --partition-cols "国家" \
  --incremental
```

产物：

- `04_Processed_data/partitioned_dataset_v1/...`
- `04_Processed_data/partitioned_dataset_v1/manifest.json`


### Step 4: 对象存储上传（Object Storage）

将以下目录上传到对象存储（S3/R2/Blob）：

- `04_Processed_data/partitioned_dataset_v1/`

建议路径规范：

- `s3://<bucket>/jato/datasets/partitioned_dataset_v1/...`


### Step 5: 查询下推（Predicate Pushdown）

应用层按筛选条件仅读取所需分区、所需列：

- 读取策略：`SELECT 列子集 + WHERE 过滤`
- 不再全量 `read_parquet` 整包加载


### Step 6: 缓存策略（Cache）

推荐缓存分层：

- 维度选项缓存：长 TTL
- 图表查询缓存：中 TTL
- 明细查询缓存：短 TTL

缓存键建议包含：

- 数据集版本（manifest）
- 筛选条件
- 列选择


### Step 7: 一键数据刷新作业（ETL + 分区 + 校验 + 基准）

快速执行（推荐日常）：

```bash
python 03_Scripts/run_data_refresh_job.py --overwrite-partition
```

仅刷新数据（跳过 benchmark）：

```bash
python 03_Scripts/run_data_refresh_job.py --overwrite-partition --skip-benchmark
```

作业报告输出：

- `04_Processed_data/refresh_job_report.json`

增量模式（输入未变化时跳过 ETL + 分区重建）：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --incremental \
  --skip-unchanged \
  --skip-benchmark \
  --fingerprint 04_Processed_data/dataset_fingerprint.json
```

说明：当输入有变化且启用 `--incremental` 时，
分区构建阶段将执行“按分区增量重写”，不会全量重建全部分区目录。


### Step 8: 扩展数据补档（多 Excel 合并）

场景示例：

- `JATO-2026.1.xlsx` 含 15 个国家
- `JATO-2026.1 (1).xlsx` 补充另外 5 个国家

推荐做法：将两份文件放入 `01_RAW_DATA/`，启用多文件合并。

1) 自动合并 raw 目录下全部 xlsx：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

2) 若存在跨文件重复记录，增加去重键（后出现记录优先）：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --dedupe-keys "国家,make,model,version name" \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

3) 仅合并指定文件（避免误读 raw 目录全部文件）：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --input-files "01_RAW_DATA/JATO-2026.1.xlsx,01_RAW_DATA/JATO-2026.1 (1).xlsx" \
  --dedupe-keys "国家,make,model,version name" \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

说明：

- 去重键大小写不敏感，脚本会自动匹配实际列名。
- 处理结果会写入 manifest 的 `mergeSummary`，包含 sourceFileCount 与 droppedDuplicateRows。
- 建议命名补档文件为 `JATO-2026.1-P1.xlsx` / `JATO-2026.1-P2.xlsx`，避免长期使用 `(1)` 命名。

4) 先做冲突拦截（发现冲突即失败，不落盘）：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --conflict-keys "国家,make,model,version name" \
  --conflict-policy fail \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

5) 冲突按“后文件覆盖前文件”自动处理（last_wins）：

```bash
python 03_Scripts/run_data_refresh_job.py \
  --merge-all-xlsx \
  --conflict-keys "国家,make,model,version name" \
  --conflict-policy last_wins \
  --incremental \
  --skip-unchanged \
  --skip-benchmark
```

6) 回滚行为说明：

- 默认开启“失败自动回滚”（全量 parquet、manifest、分区目录）。
- 如需关闭，追加 `--no-rollback`。
- 如需保留备份目录用于审计，追加 `--keep-backup`。


## 当前状态（截至本次更新）

- ✅ Raw -> 全量 Parquet
- ✅ 全量 Parquet -> 分区数据集
- ✅ 两级 manifest 生成
- ✅ 查询下推改造（Dashboard 读取层）
- ✅ 缓存分层改造（sidebar/analysis/detail）
- ✅ 一键数据刷新作业脚本
- ⏳ 对象存储上传（待接入）


## 日常执行最短路径

```bash
# 1) 放原始文件到 01_RAW_DATA/
# 2) 生成全量 parquet
python 03_Scripts/elt_worker.py

# 3) 生成分区数据
python 03_Scripts/build_partitioned_dataset.py --overwrite
```

可选：一键刷新（含校验）

```bash
python 03_Scripts/run_data_refresh_job.py --overwrite-partition --skip-benchmark
```

可选：上线前 smoke 校验

```bash
python 03_Scripts/ci_smoke_check.py
```

完成后，应用可继续读取 `04_Processed_data` 现有产物；
后续切换对象存储时，只需替换数据读取后端，不改业务图表逻辑。


## Dashboard 运行读取架构（2026-03）

当前 `05_DashBoard/dashboard/runner.py` 已落地“分区优先 + 列裁剪 + 下推 + 版本缓存”：

1. 数据源选择
   - 优先：`04_Processed_data/partitioned_dataset_v1/`
   - 回退：`04_Processed_data/jato_full_archive.parquet`

2. 版本令牌（缓存失效）
   - 分区目录优先读取 `manifest.json` 的 `mtime/size` 作为版本令牌
   - 全量文件使用 Parquet 文件 `mtime/size` 作为版本令牌
   - 版本令牌注入 `load_column_names / load_dataset_slice` 缓存键

3. 两阶段读取
   - 侧边栏阶段：仅加载筛选维度列（国家/细分/动总/品牌/Model/Version）
   - 分析阶段：
     - 大数据模式（默认）：加载“图表所需核心列 + 时间列”，并做 Arrow 过滤下推
     - 全列模式：加载全部列

4. 明细预览按需全列
   - `明细预览按需全列` 开关开启时，额外执行一次“全列 + 当前筛选”读取
   - 仅用于明细预览，不影响图表主读取路径

5. 运行状态可视化
   - 页面状态栏显示：数据源类型、版本令牌、加载模式、读取列数
   - 显示侧边栏/分析/明细读取耗时、行数、分析内存估算


## 故障排查（常见）

- 报错：`未找到数据文件`
  - 检查 `04_Processed_data/jato_full_archive.parquet` 是否存在
  - 若走分区模式，检查 `04_Processed_data/partitioned_dataset_v1/` 是否存在

- 报错：分区目录无数据
  - 确认目录下有 `*.parquet`
  - 重新执行：
    - `python 03_Scripts/build_partitioned_dataset.py --overwrite`

- 警告：分区目录缺少 `manifest.json`
  - 建议重建分区数据集以恢复 manifest
  - 没有 manifest 不会阻断运行，但会影响版本缓存命中质量

- 警告：未识别到 `国家` 字段
  - 检查上游 ETL 字段名是否变化
  - 若字段重命名，需同步更新列映射逻辑

- 运行变慢
  - 优先开启 `大数据模式（列裁剪 + 过滤下推）`
  - 明细预览不需要全字段时，关闭 `明细预览按需全列`
  - 缩小筛选范围（国家/细分/动总/品牌）后再看细粒度图表
