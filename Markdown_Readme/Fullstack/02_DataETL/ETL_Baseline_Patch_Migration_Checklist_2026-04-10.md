# JATO Baseline / Patch 迁移清单 · 2026-04-10

这份清单把当前 2026.1 单整包方式，迁移成 baseline + patch + compare + staging release 的实际执行步骤。

## 1. 迁移目标

执行完成后，数据流程应固定为：

1. baseline 文件单独冻结保存。
2. 新文件先进入 inbox / patch batch。
3. refresh 前必须先做 raw compare。
4. candidate refresh 先落 staging，不直接覆盖 serving 产物。
5. review 通过后再 promotion。

## 2. 当前状态

当前主 raw 文件：

1. [01_RAW_DATA/整合版【已规整】JATO数据-【202301-202601】21国（含奥、芬、斯洛文尼亚、斯洛伐克）-20250126-fina.xlsx](../../01_RAW_DATA/%E6%95%B4%E5%90%88%E7%89%88%E3%80%90%E5%B7%B2%E8%A7%84%E6%95%B4%E3%80%91JATO%E6%95%B0%E6%8D%AE-%E3%80%90202301-202601%E3%80%9121%E5%9B%BD%EF%BC%88%E5%90%AB%E5%A5%A5%E3%80%81%E8%8A%AC%E3%80%81%E6%96%AF%E6%B4%9B%E6%96%87%E5%B0%BC%E4%BA%9A%E3%80%81%E6%96%AF%E6%B4%9B%E4%BC%90%E5%85%8B%EF%BC%89-20250126-fina.xlsx)

当前 canonical 输出：

1. [04_Processed_data/manifest.json](../../04_Processed_data/manifest.json)
2. [04_Processed_data/jato_full_archive.parquet](../../04_Processed_data/jato_full_archive.parquet)
3. [04_Processed_data/partitioned_dataset_v1](../../04_Processed_data/partitioned_dataset_v1)
4. [04_Processed_data/refresh_job_report.json](../../04_Processed_data/refresh_job_report.json)
5. [04_Processed_data/dataset_fingerprint.json](../../04_Processed_data/dataset_fingerprint.json)

## 3. 目标目录结构

### 3.1 Raw 区

    01_RAW_DATA/
      inbox/
        2026-02/
      baseline/
      patch_batches/
        2026-02/
      rejected/
        2026-02/
      historyDataArchive/
        raw/
        compare_reports/
        release_inputs/

### 3.2 Processed 区

    04_Processed_data/
      reviews/
        raw_compare/
      staging/
      releases/
      jato_full_archive.parquet
      manifest.json
      partitioned_dataset_v1/
      refresh_job_report.json
      dataset_fingerprint.json

## 4. 第一次迁移动作

### 4.1 冻结 2026.1 baseline

把当前 2026.1 主文件整理为：

1. `01_RAW_DATA/baseline/JATO-2026.1-full-21countries-baseline.xlsx`

### 4.2 放入 2026.2 patch

新收到的 2026.2 文件按下面规则处理：

1. 原始落地先放 `01_RAW_DATA/inbox/2026-02/`
2. 格式确认后移入 `01_RAW_DATA/patch_batches/2026-02/`
3. 建议命名为 `JATO-2026.2-partial-17countries.xlsx`

### 4.3 不通过的 patch

如果 compare 不通过：

1. 文件移入 `01_RAW_DATA/rejected/2026-02/`
2. compare 报告归档到 `01_RAW_DATA/historyDataArchive/compare_reports/2026-01_vs_2026-02/`

## 5. compare 执行

    python 03_Scripts/raw_compare_review.py \
      --old 01_RAW_DATA/baseline/JATO-2026.1-full-21countries-baseline.xlsx \
      --new 01_RAW_DATA/patch_batches/2026-02/JATO-2026.2-partial-17countries.xlsx \
      --output-dir 04_Processed_data/reviews/raw_compare/2026-01_vs_2026-02

预期结果：

1. 17 个国家 freshnessStatus = advanced。
2. 4 个国家 freshnessStatus = unchanged_latest。
3. 不出现 regressed。
4. 不出现 missing_in_candidate。
5. 不出现 removedMonths。

## 6. candidate refresh 执行

本次候选 release 命名建议：

1. `2026-02-mixed`

候选输出目录：

1. `04_Processed_data/staging/2026-02-mixed/`

    python 03_Scripts/run_data_refresh_job.py \
      --baseline-input 01_RAW_DATA/baseline/JATO-2026.1-full-21countries-baseline.xlsx \
      --patch-input-files 01_RAW_DATA/patch_batches/2026-02/JATO-2026.2-partial-17countries.xlsx \
      --supplement-missing-countries-from-parquet 04_Processed_data/jato_full_archive.parquet \
      --output 04_Processed_data/staging/2026-02-mixed/jato_full_archive.parquet \
      --manifest 04_Processed_data/staging/2026-02-mixed/manifest.json \
      --partition-output 04_Processed_data/staging/2026-02-mixed/partitioned_dataset_v1 \
      --report 04_Processed_data/staging/2026-02-mixed/refresh_job_report.json \
      --fingerprint 04_Processed_data/staging/2026-02-mixed/dataset_fingerprint.json \
      --incremental \
      --skip-benchmark

当 patch 只覆盖部分国家时，`--supplement-missing-countries-from-parquet` 会把 current active 中未出现在 patch 的国家整国补回 candidate，保证 promotion 的语义更接近“current active ∪ 本次 patch”，而不是“历史 baseline + 本次 patch”。

## 7. promotion 清单

promotion 当前按人工流程执行：

1. 把 staging 目录复制到 `04_Processed_data/releases/2026-02-mixed/`
2. 再把批准后的 release 覆盖到 `04_Processed_data/` 根目录 canonical 路径
3. 记录本次 release 使用的 baseline 和 patch 到 `01_RAW_DATA/historyDataArchive/release_inputs/2026-02-mixed/`

## 8. 命名规则

### 8.1 baseline

1. `JATO-2026.1-full-21countries-baseline.xlsx`
2. `JATO-2026.2-full-21countries-baseline.xlsx`

### 8.2 patch

1. `JATO-2026.2-partial-17countries.xlsx`
2. `JATO-2026.2-partial-17countries-P1.xlsx`
3. `JATO-2026.3-partial-4countries-backfill.xlsx`

### 8.3 compare id

1. `2026-01_vs_2026-02`
2. `2026-02-baseline_vs_2026-02-patch17`

### 8.4 release id

1. `2026-01-full`
2. `2026-02-mixed`
3. `2026-02-mixed-r2`

## 9. 执行约束

1. compare 和 refresh 一律使用显式输入路径。
2. 不再依赖 raw 根目录自动扫描决定正式输入。
3. refresh 先写 staging，不直接覆盖 canonical 根目录。
4. compare 报告必须存在，candidate refresh 才能执行。

## 10. 推荐顺序

1. 先跑 raw compare。
2. 再跑 baseline + patch 的 candidate refresh。
3. 最后做 promotion。
