# 2026-03 月度更新计划

- 对比: 2026-01_vs_2026-03
- baseline: 01_RAW_DATA/baseline/JATO-2026.1-full-21countries-baseline.xlsx
- patch: 01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx

## 步骤 1 · Raw Compare

```bash
python 03_Scripts/raw_compare_review.py --old 01_RAW_DATA/baseline/JATO-2026.1-full-21countries-baseline.xlsx --new 01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx --output-dir 04_Processed_data/reviews/raw_compare/2026-01_vs_2026-03
```

## 步骤 2 · Candidate Refresh

```bash
python 03_Scripts/data_pipeline/run_data_refresh_job.py --baseline-input 01_RAW_DATA/baseline/JATO-2026.1-full-21countries-baseline.xlsx --patch-input-files 01_RAW_DATA/patches/2026-03/JATO-2026.3-partial.xlsx --output 04_Processed_data/staging/2026-03-mixed/jato_full_archive.parquet --manifest 04_Processed_data/staging/2026-03-mixed/manifest.json --partition-output 04_Processed_data/staging/2026-03-mixed/partitioned_dataset_v1 --report 04_Processed_data/staging/2026-03-mixed/refresh_job_report.json --fingerprint 04_Processed_data/staging/2026-03-mixed/dataset_fingerprint.json --incremental --skip-benchmark
```

## 步骤 3 · Promotion

1. review raw compare 结果
2. 确认 staging 无异常后复制到 releases/ 和 canonical 根目录
3. 归档到 historyDataArchive/
