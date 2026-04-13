# JATO Raw Compare / Review 规格 · 2026-04-10

这份文档把 raw 对 raw 的 compare / review 流程、字段结构、判定规则、目录结构和最小脚本方案一次性定死，目标是在正式 refresh 之前，先回答三个问题：

1. 新文件到底更新了哪些国家。
2. 这些国家是新增月份、补旧月份，还是修订旧数据。
3. 这批 raw 是否可以进入候选数据集，再进入正式发布。

当前仓库已经具备多文件合并、冲突报告、按国家分区识别变化、按变化国家重算 summary 的基础能力，分别见 [03_Scripts/elt_worker.py](../../03_Scripts/elt_worker.py#L98)、[03_Scripts/elt_worker.py](../../03_Scripts/elt_worker.py#L189)、[03_Scripts/build_partitioned_dataset.py](../../03_Scripts/build_partitioned_dataset.py#L166)、[03_Scripts/run_data_refresh_job.py](../../03_Scripts/run_data_refresh_job.py#L423)、[03_Scripts/precompute_summaries.py](../../03_Scripts/precompute_summaries.py#L67)。

落地迁移清单见：[ETL_Baseline_Patch_Migration_Checklist_2026-04-10.md](./ETL_Baseline_Patch_Migration_Checklist_2026-04-10.md)

当前限制也要明确：现有 raw 自动扫描仍是根目录单层 xlsx，并带有一个 2026.1 的优先读取逻辑，见 [03_Scripts/elt_worker.py](../../03_Scripts/elt_worker.py#L53) 和 [03_Scripts/elt_worker.py](../../03_Scripts/elt_worker.py#L91)。所以本规格的目录结构是目标结构；在真正改脚本前，compare 和 refresh 一律通过显式输入路径工作，不依赖根目录全扫。

## 1. 核心原则

### 1.1 先 compare，再 refresh

任何新 raw 批次都不能直接进入正式 refresh。

固定顺序：

1. raw compare / review
2. candidate refresh
3. review 通过后 promotion
4. 正式 serving 产物覆盖当前 canonical 路径

### 1.2 数据发布按 baseline + patch 理解

从现在开始，不再把“最新数据”理解成“唯一一份总 Excel”。

统一改成：

1. baseline：一个冻结的、可回溯的全量基线。
2. patch batch：后续按国家、按月份推进的增量批次。
3. release：由 baseline 和一个或多个 patch 组合出来的候选或正式数据集。

### 1.3 mixed freshness 是允许的

17 个国家更新到 2026.2、4 个国家仍停在 2026.1 是允许状态，不需要强行等齐。

但必须同时维护两种消费语义：

1. latest available：每个国家看到自己的最新月。
2. common comparable month：跨国横向对比时，只看所有国家共同拥有的最近月份。

### 1.4 merge 语义是 upsert，不是纯 append

后续 patch 不应被理解成“只新增某国某个月”。

patch 可能有三类变化：

1. 新增未来月份。
2. 补录旧月份。
3. 修订同一个国家、同一个月份下已有业务键记录。

因此 compare 要同时识别 freshness 推进和同月修订，不能只看有没有新月份。

## 2. 目录结构

### 2.1 推荐 raw 目录结构

建议把 [01_RAW_DATA](../../01_RAW_DATA) 从“单文件落地目录”改成“原始批次管理区”：

    01_RAW_DATA/
      inbox/
        2026-02/
          JATO-2026.2-partial-17countries.xlsx
      baseline/
        JATO-2026.1-full-21countries-baseline.xlsx
      patch_batches/
        2026-02/
          JATO-2026.2-partial-17countries.xlsx
        2026-03/
          ...
      rejected/
        2026-02/
          JATO-2026.2-partial-17countries-rejected.xlsx
      historyDataArchive/
        raw/
          2026-01/
          2026-02/
        compare_reports/
          2026-01_vs_2026-02/
        release_inputs/
          2026-02-mixed/

约定如下：

1. inbox：新收到但未 review 的原始文件。
2. baseline：冻结全量基线，只保留少量关键基线，不反复覆盖。
3. patch_batches：通过初步格式检查后，作为正式 compare 输入的 patch 文件。
4. rejected：review 不通过的文件，保留但不进入 refresh。
5. historyDataArchive/raw：归档已经处理过或不再活跃的输入。
6. historyDataArchive/compare_reports：归档 compare 结果，便于追溯每次 decision。
7. historyDataArchive/release_inputs：归档某次 release 实际使用了哪些 baseline 和 patch。

### 2.2 推荐 processed 目录结构

建议把 [04_Processed_data](../../04_Processed_data) 分成 compare、staging、release history、canonical 四层：

    04_Processed_data/
      reviews/
        raw_compare/
          2026-01_vs_2026-02/
            raw_compare_report.json
            country_latest_month_diff.csv
            country_month_coverage_diff.csv
            country_change_summary.csv
            conflict_samples.json
            review_checklist.md
      staging/
        2026-02-mixed/
          jato_full_archive.parquet
          manifest.json
          partitioned_dataset_v1/
          summaries/
          refresh_job_report.json
          dataset_fingerprint.json
      releases/
        2026-01-full/
        2026-02-mixed/
      jato_full_archive.parquet
      manifest.json
      partitioned_dataset_v1/
      summaries/
      refresh_job_report.json
      dataset_fingerprint.json

约定如下：

1. reviews/raw_compare：只存 raw 对 raw 的 compare 产物。
2. staging：候选 refresh 产物，不直接 serving。
3. releases：已批准的历史 release 副本，用于审计和回滚。
4. 根目录 canonical 文件：当前系统实际 serving 的路径，短期内保持兼容，不改现有读取代码。

### 2.3 兼容性规则

在 serving 代码没有 release-aware 之前：

1. compare 输出写到 reviews。
2. candidate refresh 输出写到 staging。
3. promotion 时把已批准 release 复制到根目录 canonical 路径。
4. releases 保留一份历史副本，避免“根目录覆盖后无法追溯”。

## 3. 文件命名规范

### 3.1 raw 文件命名

建议统一成：

1. baseline：JATO-2026.1-full-21countries-baseline.xlsx
2. patch：JATO-2026.2-partial-17countries.xlsx
3. patch 多包：JATO-2026.2-partial-17countries-P1.xlsx

命名中至少包含：

1. 数据月份
2. full 或 partial
3. 国家数或批次标识
4. baseline 或 patch 语义

### 3.2 compare 批次命名

建议 compare id 统一成：

1. 2026-01_vs_2026-02
2. 2026-02-baseline_vs_2026-02-patch17

### 3.3 release 命名

建议 release id 统一成：

1. 2026-01-full
2. 2026-02-mixed
3. 2026-03-mixed-r2

其中 r2 表示同一月份的第二次修订版。

## 4. Raw Compare / Review 的输出目标

compare 步骤必须回答下面这些问题：

1. 两个 xlsx 各自包含哪些国家。
2. 每个国家在 old 和 new 中最新到哪个月份。
3. 每个国家有哪些月份是新增、缺失、重叠。
4. 对重叠月份，哪些业务键新增、删除、修订。
5. 有没有 schema 漂移、sheet 错误、国家丢失、月份回退等 blocker。

因此 compare 输出不只是一张国家名单，而是一组结构化产物。

## 5. 报告字段结构

### 5.1 主报告文件

主文件建议固定为：

    04_Processed_data/reviews/raw_compare/<compare_id>/raw_compare_report.json

顶层结构建议如下：

1. compareId：比如 2026-01_vs_2026-02
2. generatedAtUtc：生成时间
3. baselineInput：old 文件信息
4. candidateInput：new 文件信息
5. schemaCheck：schema 校验结果
6. timeAxisCheck：时间列识别结果
7. countryScopeSummary：国家范围摘要
8. countryFreshnessSummary：国家最新月份摘要
9. countryCoverageSummary：国家月份覆盖摘要
10. overlapChangeSummary：重叠范围的记录级变化摘要
11. reviewFindings：按严重级别列出问题
12. decisionSuggestion：建议进入 candidate / reject / manual review

### 5.2 baselineInput / candidateInput 字段

两个输入对象字段一致：

1. path：输入路径
2. fileName：文件名
3. bytes：文件大小
4. mtimeUtc：文件时间
5. sheetName：实际读取 sheet
6. rowCount：原始行数
7. columnCount：列数
8. countryCount：国家数
9. monthColumnCount：月份列数
10. earliestMonth：最早识别月份
11. latestMonth：最晚识别月份

### 5.3 schemaCheck 字段

1. status：pass / review / fail
2. missingColumns：新文件缺少的关键列
3. addedColumns：新文件新增列
4. renamedCandidates：疑似重命名列
5. incompatibleColumns：类型或命名无法自动对齐的列

判定规则：

1. 关键业务列缺失，直接 fail。
2. 只新增非关键列，review。
3. 仅列顺序变化，pass。

### 5.4 timeAxisCheck 字段

1. recognizedMonthColumnsOld
2. recognizedMonthColumnsNew
3. addedMonths
4. removedMonths
5. overlappingMonths
6. latestMonthAdvanced：是否整体出现未来月份

判定规则：

1. 新文件没有识别到时间列，fail。
2. 发生月份回退，review 或 fail，默认按 fail 处理。
3. 新增未来月份，pass。
4. 只修订旧月份但无新增月份，review。

### 5.5 countryScopeSummary 字段

1. oldCountries
2. newCountries
3. addedCountries
4. removedCountries
5. overlappingCountries
6. unchangedCountryCount
7. changedCountryCount

判定规则：

1. baseline 中已有国家在 candidate 中消失，默认 fail，除非本次任务显式允许删国。
2. 新增国家，review。
3. 只有部分国家更新，pass。

### 5.6 countryFreshnessSummary 字段

这部分要同时生成 JSON 摘要和 CSV 明细。

每个国家字段：

1. country
2. oldLatestMonth
3. newLatestMonth
4. freshnessStatus
5. freshnessDeltaMonths
6. oldRowCount
7. newRowCount
8. rowDelta

freshnessStatus 只允许以下枚举：

1. advanced：新文件最新月推进了
2. unchanged_latest：最新月没变
3. regressed：最新月回退
4. new_country：baseline 中没有，candidate 中新增
5. missing_in_candidate：baseline 中有，candidate 中消失

判定规则：

1. advanced：可进入下一步 review。
2. unchanged_latest：允许，但要进一步检查是否有旧月修订。
3. regressed：默认 fail。
4. missing_in_candidate：默认 fail。

### 5.7 countryCoverageSummary 字段

这部分用于回答“是不是只是某些国家新增了 2026.2”以及“有没有补旧月”。

每个国家字段：

1. country
2. oldMonths
3. newMonths
4. addedMonths
5. removedMonths
6. overlappingMonths
7. coverageStatus

coverageStatus 建议枚举：

1. added_future_months：新增未来月份
2. backfill_old_months：补录旧月份
3. revised_overlap_only：只修订重叠月份
4. unchanged_coverage：月份覆盖完全一致
5. regressed_coverage：月份覆盖退化

判定规则：

1. added_future_months：正常 patch。
2. backfill_old_months：允许，但要人工 review。
3. revised_overlap_only：允许，但要看业务键变化规模。
4. regressed_coverage：默认 fail。

### 5.8 overlapChangeSummary 字段

这部分只分析 old 和 new 重叠范围内的数据变化，相当于 git diff 的记录级摘要。

建议按国家输出：

1. country
2. compareMonths
3. compareKeyColumns
4. addedRecordCount
5. removedRecordCount
6. changedRecordCount
7. unchangedRecordCount
8. changeRate
9. sampleAddedKeys
10. sampleRemovedKeys
11. sampleChangedKeys

说明：

1. compareKeyColumns 是业务键，不是时间列。
2. changedRecordCount 指键相同但 payload 不同。
3. sampleChangedKeys 用于 review 抽样，不要求全量展开。

### 5.9 reviewFindings 字段

每条 finding 统一结构：

1. severity：blocker / review / info
2. scope：global / country / country_month / record_key
3. target：目标对象，比如 Germany 或 Germany|2026 Feb
4. ruleId：命中的判定规则编号
5. message：人类可读说明
6. metrics：相关数值
7. suggestedAction：下一步动作

### 5.10 decisionSuggestion 字段

只允许三个值：

1. proceed_to_candidate_refresh
2. manual_review_required
3. reject_input_batch

判定逻辑：

1. 任何 blocker 存在，reject_input_batch。
2. 无 blocker，但存在较多 review 项，manual_review_required。
3. 仅有 info 或少量预期中的 partial advance，proceed_to_candidate_refresh。

## 6. CSV 和附属文件规范

compare 目录下建议固定输出下面几类文件：

1. country_latest_month_diff.csv
2. country_month_coverage_diff.csv
3. country_change_summary.csv
4. conflict_samples.json
5. review_checklist.md

### 6.1 country_latest_month_diff.csv

每行一个国家，核心字段：

1. country
2. old_latest_month
3. new_latest_month
4. freshness_status
5. old_row_count
6. new_row_count
7. row_delta

### 6.2 country_month_coverage_diff.csv

每行一个国家，核心字段：

1. country
2. added_months
3. removed_months
4. overlapping_months
5. coverage_status

### 6.3 country_change_summary.csv

每行一个国家，核心字段：

1. country
2. compare_key_columns
3. added_record_count
4. removed_record_count
5. changed_record_count
6. unchanged_record_count
7. change_rate

### 6.4 conflict_samples.json

结构尽量兼容现有冲突报告能力，复用 [03_Scripts/elt_worker.py](../../03_Scripts/elt_worker.py#L339) 的样式：

1. compareId
2. compareKeyColumns
3. sampledCountries
4. samples

每个 sample 至少包含：

1. country
2. businessKey
3. oldValueDigest
4. newValueDigest
5. changedFields

### 6.5 review_checklist.md

这份文件是给人看的，不代替 JSON。

固定分成五块：

1. 本次 compare 摘要
2. 国家 freshness 结果
3. 需要人工确认的国家
4. blocker 列表
5. promotion 建议

## 7. 判定规则

建议给规则编号，后续 JSON 中直接引用。

### 7.1 Global blocker

1. R001：sheet 不存在或不可读，fail。
2. R002：未识别到月份列，fail。
3. R003：关键业务列缺失，fail。
4. R004：候选文件国家集合明显小于 baseline 且无任务说明，fail。
5. R005：月份整体回退，fail。

### 7.2 Global review

1. R101：只新增非关键列，review。
2. R102：国家总数变化，review。
3. R103：仅修订旧月，无未来月份推进，review。

### 7.3 Country-level review

1. R201：freshness advanced，info。
2. R202：latest month unchanged 但 row delta 明显，review。
3. R203：新增国家，review。
4. R204：某国家 removed months 不为空，review 或 fail，默认 fail。
5. R205：某国家 changedRecordCount 占比异常高，review。

### 7.4 Promotion 通过条件

只有同时满足以下条件才建议进入 candidate refresh：

1. 无 global blocker。
2. 无 country-level regression。
3. baseline 国家没有被意外删掉。
4. 所有 partial lagging 国家都只是 unchanged_latest，不是 missing 或 regressed。
5. overlap change 中没有超阈值异常变动。

## 8. 最小 compare 脚本方案

### 8.1 脚本定位

最小 compare 脚本已经落地，路径固定为：

    03_Scripts/raw_compare_review.py

这不是正式 refresh 脚本，而是 refresh 前置 gate。

### 8.2 输入参数

最小输入：

1. --old：baseline xlsx 路径
2. --new：candidate xlsx 路径
3. --sheet：默认 Data Export
4. --output-dir：compare 输出目录

推荐扩展参数：

1. --country-col：默认 国家
2. --compare-keys：比如 国家,make,model,version name
3. --sample-limit：冲突抽样上限
4. --strict：发现 blocker 时返回非零退出码

### 8.3 脚本处理步骤

1. 读取两个 xlsx。
2. 规范化列名与字符串类型。
3. 识别月份列。
4. 计算国家集合与国家最新月份。
5. 计算国家月份覆盖差异。
6. 对重叠国家和重叠月份，基于 compare keys 做记录级 diff。
7. 生成 JSON、CSV、review checklist。
8. 给出 decisionSuggestion。

### 8.4 输出路径

输出目录建议固定为：

    04_Processed_data/reviews/raw_compare/<compare_id>/

脚本产物固定写入：

1. raw_compare_report.json
2. country_latest_month_diff.csv
3. country_month_coverage_diff.csv
4. country_change_summary.csv
5. conflict_samples.json
6. review_checklist.md

### 8.5 退出码规则

1. 0：compare 成功完成，且无 blocker。
2. 1：compare 运行失败。
3. 2：compare 成功完成，但命中 blocker 或 strict gate 未通过。

## 9. Compare keys 的初始设计

最小 compare 脚本必须支持显式 compare keys，不允许写死。

初始推荐候选：

1. 国家
2. make 或 品牌
3. model
4. version name

如果 version 稳定性差，再补：

1. 动总规整 或 Powertrain
2. 细分市场（按车长）

注意：

1. compare keys 用于识别“同一业务记录”。
2. 月份列属于 payload，不属于 compare keys。
3. compare keys 的最终版本必须在一次真实 compare 后再锁死。

## 10. Candidate Refresh 和 Promotion 规则

### 10.1 candidate refresh 输入

一旦 compare 通过，candidate refresh 的输入应明确记录：

1. 使用哪个 baseline 文件。
2. 使用哪些 patch 文件。
3. compare 报告 id 是什么。

### 10.2 candidate refresh 输出

candidate 产物统一落到：

    04_Processed_data/staging/<release_id>/

最少包含：

1. jato_full_archive.parquet
2. manifest.json
3. partitioned_dataset_v1/
4. summaries/
5. refresh_job_report.json
6. dataset_fingerprint.json

### 10.3 promotion

promotion 不是重新跑一次 refresh，而是把 staging 中已批准的 release：

1. 复制到 04_Processed_data/releases/<release_id>/ 保留历史
2. 再覆盖 04_Processed_data 根目录 canonical 文件

这样可以保证：

1. 当前服务路径不变。
2. 历史 release 可追溯。
3. 回滚时不需要重新拼 raw。

## 11. 2026.1 vs 2026.2 的首个落地模板

当前案例直接按下面的思路执行：

1. 把现有总文件定义为 baseline：2026.1 full 21 countries。
2. 把 2026.2 新文件定义为 patch candidate。
3. 先做 raw compare，输出国家 freshness 和月份覆盖 diff。
4. 预期结果是：17 个国家 advanced，4 个国家 unchanged_latest。
5. 只要没有 regressed 或 missing_in_candidate，就允许进入 candidate refresh。
6. 发布语义定义为 2026-02-mixed，不伪造 4 个国家的 2026.2。

## 12. 短期执行约束

在目录扫描逻辑真正改完之前，先遵守下面这几条：

1. baseline 和 patch 文件可以放在上面定义的子目录里。
2. compare 和 refresh 都通过显式输入路径运行，不依赖 raw 根目录自动发现。
3. 现有 [03_Scripts/elt_worker.py](../../03_Scripts/elt_worker.py#L91) 的 2026.1 优先逻辑视为 legacy 行为，不再当作长期约定。
4. [04_Processed_data/manifest.json](../../04_Processed_data/manifest.json#L9) 和 [04_Processed_data/dataset_fingerprint.json](../../04_Processed_data/dataset_fingerprint.json#L3) 当前仍是单源模式，因此 compare 报告必须先于 refresh 报告存在。

## 13. 后续实施优先级

如果按最小改动推进，实施顺序建议固定为：

1. 先做 raw_compare_review.py。
2. 再做 country freshness 元数据输出。
3. 再让 refresh 支持 baseline + patch 的显式输入组合。
4. 最后再把 serving 层从 canonical 根目录切到 release-aware 模式。

这样可以先把 compare / review 体系建起来，再逐步替换“单整包覆盖”的旧操作方式。
