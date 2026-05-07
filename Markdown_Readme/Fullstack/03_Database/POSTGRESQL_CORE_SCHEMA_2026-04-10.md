# PostgreSQL Core Schema Design

状态：Active

日期：2026-04-10

关联文档：

1. [PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md](PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md)
2. [MSRP_Official_Price_Enrichment_PRD_2026-04-10.md](./MSRP/01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md)
3. [MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md](./MSRP/02_Research_Design/MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md)
4. [ALEMBIC_MIGRATION_PLAN_2026-04-10.md](ALEMBIC_MIGRATION_PLAN_2026-04-10.md)

## 1. 目标

本设计用于定义 PostgreSQL 首批核心表结构，覆盖以下业务域：

1. 工程配置表导入与查询。
2. MSRP 来源、抓取批次与结构化价格结果。
3. 人工 review。
4. manual override。
5. 价格预警。

## 2. 数据库设计原则

### 2.1 主键策略

建议：

1. 业务主表使用 UUID 主键。
2. 批次类和日志类表也使用 UUID。

原因：

1. 便于离线生成。
2. 便于后续扩展到多脚本写入。
3. 避免序列号泄露业务规模。

### 2.2 时间字段

统一使用：

1. timestamptz。

建议字段命名：

1. created_at_utc
2. updated_at_utc
3. observed_at_utc
4. finished_at_utc

### 2.3 金额字段

金额建议使用：

1. numeric(14, 2)

不要用 float 存最终价格。

### 2.4 半结构化字段

以下场景允许使用 JSONB：

1. 工程配置行的非标准列。
2. 官网抓取原始结构化摘要。
3. 候选匹配列表。
4. 匹配解释详情。

### 2.5 大文件处理原则

数据库中只保存：

1. 文件路径。
2. 文件摘要。
3. 文件元数据。

数据库中不保存：

1. xlsx 二进制。
2. HTML 原文。
3. PDF 原文。
4. PNG 截图二进制。

## 3. 推荐 PostgreSQL Schema 划分

建议使用以下 schema：

1. ops
2. engineering
3. msrp
4. review
5. alerting

用途：

1. ops：通用批次、导入、审计。
2. engineering：工程配置表。
3. msrp：来源、抓取、价格结果、当前价格。
4. review：人工复核与 override。
5. alerting：价格波动预警和价格效果验证。

### 3.1 当前已落地的 app-facing PostgreSQL 契约（2026-04-21）

当前仓库里的 PostgreSQL 已经实际承载四类 app-facing 业务域：

1. `engineering.*`
	- `config_projects`
	- `config_import_batches`
	- `config_variants`
	- `base_variants`
	- `market_variants`
	- `market_feature_overrides`
2. `msrp.*`
	- `sources`
	- `scrape_batches`
	- `observations`
	- `current_prices`
	- `price_history`
	- `jato_msrp_links`
3. `review.*`
	- `review_cases`
	- `review_decisions`
	- `match_overrides`
4. `ops.*` 中和 app-facing 内容直接相关的业务表
	- `country_news_articles`
	- `country_news_digests`
	- `voc_source_runs`
	- `voc_raw_documents`

当前边界应固定为：

1. PostgreSQL 负责业务真值、事务、一致性、可索引查询和审计链。
2. Parquet 继续负责 JATO 主分析事实表与大规模分析查询。
3. 文件系统 / artifact 层继续负责 VOC raw/enriched/deck、网页 snapshot 路径和其他大对象。
4. News / VOC 在 PostgreSQL 里当前是 app-facing snapshot / staging，不是全文对象仓库。

## 4. 首批核心表总览

建议第一阶段至少建立以下表：

1. ops.import_batches
2. engineering.config_projects
3. engineering.config_import_batches
4. engineering.config_variants
5. msrp.sources
6. msrp.scrape_batches
7. msrp.observations
8. msrp.current_prices
9. review.review_cases
10. review.review_decisions
11. review.match_overrides
12. alerting.price_alerts
13. alerting.price_sales_effectiveness

## 5. ops.import_batches

用途：统一记录各种导入批次。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| import_batch_id | uuid | 主键 |
| domain | text | engineering / msrp / manual |
| source_file_name | text | 源文件名 |
| source_file_path | text | 源文件路径 |
| source_file_hash | text | 文件摘要 |
| import_status | text | pending / running / success / failed |
| row_count | integer | 导入行数 |
| error_count | integer | 错误数 |
| triggered_by | text | 触发人或脚本 |
| started_at_utc | timestamptz | 开始时间 |
| finished_at_utc | timestamptz | 完成时间 |
| created_at_utc | timestamptz | 创建时间 |

索引建议：

1. domain, created_at_utc desc
2. import_status
3. source_file_hash

## 6. engineering.config_projects

用途：定义一个工程配置项目或车型项目。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| project_id | uuid | 主键 |
| project_code | text | 项目标识，例如 omoda9-cn |
| brand | text | 品牌 |
| model | text | 车型 |
| market_country | text | 市场国家或区域 |
| display_name | text | 显示名称 |
| status | text | active / archived |
| created_at_utc | timestamptz | 创建时间 |
| updated_at_utc | timestamptz | 更新时间 |

约束建议：

1. unique(project_code)

索引建议：

1. brand, model
2. market_country

## 7. engineering.config_import_batches

用途：记录某个工程配置项目的一次 xlsx 导入。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| config_import_batch_id | uuid | 主键 |
| project_id | uuid | FK -> engineering.config_projects |
| import_batch_id | uuid | FK -> ops.import_batches |
| source_schema_version | text | 导入模板版本 |
| replace_mode | text | full_replace / incremental |
| import_status | text | pending / success / failed |
| row_count | integer | 导入行数 |
| valid_from_date | date | 生效起始日期，可空 |
| notes | text | 备注 |
| created_at_utc | timestamptz | 创建时间 |

索引建议：

1. project_id, created_at_utc desc
2. import_status

## 8. engineering.config_variants

用途：保存工程配置表中的每条配置记录。

这是工程配置域最核心的表。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| variant_id | uuid | 主键 |
| project_id | uuid | FK -> engineering.config_projects |
| config_import_batch_id | uuid | FK -> engineering.config_import_batches |
| external_row_key | text | 原始行主键，可空 |
| brand | text | 品牌 |
| model | text | 车型 |
| trim_name | text | 版型名 |
| version_name | text | 版本名 |
| market_country | text | 国家或区域 |
| powertrain | text | 动力 |
| body_style | text | 车身形式 |
| drive_type | text | 驱动形式 |
| battery_kwh | numeric(10, 2) | 电池容量，可空 |
| range_km | numeric(10, 2) | 续航，可空 |
| target_msrp | numeric(14, 2) | 工程配置目标 MSRP，可空 |
| is_active | boolean | 当前是否有效 |
| row_hash | text | 标准化行摘要 |
| attributes_json | jsonb | 非标准扩展字段 |
| source_file_path | text | 源文件路径 |
| created_at_utc | timestamptz | 创建时间 |
| updated_at_utc | timestamptz | 更新时间 |

设计说明：

1. 用固定结构列承接常用查询字段。
2. 用 attributes_json 容纳变化较大的 xlsx 列。
3. 不建议把三百多个配置列全部硬编码成数据库列。

约束建议：

1. unique(project_id, config_import_batch_id, row_hash)

索引建议：

1. project_id, is_active
2. brand, model, market_country
3. trim_name
4. gin(attributes_json)

## 9. msrp.sources

用途：维护国家-品牌级官网来源注册表。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| source_id | uuid | 主键 |
| source_code | text | 业务标识，例如 se-volvo-configurator |
| country | text | 国家代码 |
| brand | text | 品牌 |
| source_url | text | 入口 URL |
| source_type | text | api / html / playwright / pdf |
| extractor_name | text | 抓取器名称 |
| extractor_version | text | 抓取器版本 |
| price_semantics | text | 价格口径，例如 msrp_including_vat |
| requires_location | boolean | 是否依赖邮编或经销商 |
| enabled | boolean | 是否启用 |
| notes | text | 备注 |
| created_at_utc | timestamptz | 创建时间 |
| updated_at_utc | timestamptz | 更新时间 |

约束建议：

1. unique(source_code)

索引建议：

1. country, brand
2. enabled

## 10. msrp.scrape_batches

用途：记录夜间抓取批次。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| scrape_batch_id | uuid | 主键 |
| batch_code | text | 业务批次号 |
| trigger_type | text | scheduled / manual / backfill |
| scope_country | text | 本次抓取国家 |
| scope_brands_json | jsonb | 品牌范围 |
| candidate_count | integer | 候选数 |
| success_count | integer | 成功数 |
| review_required_count | integer | 待 review 数 |
| failed_count | integer | 失败数 |
| status | text | pending / running / partial / success / failed |
| started_at_utc | timestamptz | 开始时间 |
| finished_at_utc | timestamptz | 完成时间 |
| notes | text | 备注 |

约束建议：

1. unique(batch_code)

索引建议：

1. scope_country, started_at_utc desc
2. status

## 11. msrp.observations

用途：保存官网价格抓取后的结构化结果。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| observation_id | uuid | 主键 |
| scrape_batch_id | uuid | FK -> msrp.scrape_batches |
| source_id | uuid | FK -> msrp.sources |
| country | text | 国家代码 |
| brand | text | 品牌 |
| jato_model | text | JATO model |
| jato_trim | text | JATO trim |
| official_model | text | 官网 model |
| official_trim | text | 官网 trim |
| msrp_value | numeric(14, 2) | 标准化 MSRP |
| currency | text | 币种 |
| tax_included | boolean | 是否含税 |
| price_label | text | msrp / campaign / finance / unknown |
| availability_text | text | 官网原始文案 |
| observed_at_utc | timestamptz | 抓取时间 |
| source_url | text | 来源 URL |
| source_snapshot_path | text | 快照路径 |
| source_payload_hash | text | 快照摘要 |
| extraction_version | text | 提取规则版本 |
| match_confidence | numeric(5, 4) | 匹配得分 |
| match_status | text | auto_accepted / review_required / human_approved / rejected |
| match_reason_json | jsonb | 匹配解释 |
| created_at_utc | timestamptz | 创建时间 |
| updated_at_utc | timestamptz | 更新时间 |

设计说明：

1. observation 是事实表，允许按周追加。
2. 原始快照仍在磁盘，仅存路径和摘要。

索引建议：

1. country, brand, jato_model, observed_at_utc desc
2. match_status, observed_at_utc desc
3. source_payload_hash

## 12. msrp.current_prices

用途：保存当前生效的官方价格结果。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| current_price_id | uuid | 主键 |
| country | text | 国家代码 |
| brand | text | 品牌 |
| jato_model | text | JATO model |
| jato_trim | text | JATO trim |
| official_model | text | 官网 model |
| official_trim | text | 官网 trim |
| effective_observation_id | uuid | FK -> msrp.observations |
| current_msrp_value | numeric(14, 2) | 当前生效 MSRP |
| currency | text | 币种 |
| tax_included | boolean | 是否含税 |
| match_confidence | numeric(5, 4) | 当前匹配置信度 |
| match_status | text | auto_accepted / human_approved |
| source_url | text | 来源 URL |
| source_snapshot_path | text | 快照路径 |
| last_price_change_at_utc | timestamptz | 最近变价时间 |
| updated_at_utc | timestamptz | 更新时间 |

约束建议：

1. unique(country, brand, jato_model, jato_trim)

索引建议：

1. country, brand
2. jato_model

## 13. review.review_cases

用途：保存待人工处理的 MSRP 匹配案例。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| review_case_id | uuid | 主键 |
| observation_id | uuid | FK -> msrp.observations |
| country | text | 国家代码 |
| brand | text | 品牌 |
| jato_model | text | JATO model |
| jato_trim | text | JATO trim |
| official_model | text | 官网 model |
| official_trim | text | 官网 trim |
| candidate_matches_json | jsonb | 候选匹配列表 |
| match_confidence | numeric(5, 4) | 自动分数 |
| review_status | text | open / in_review / approved / rejected |
| source_url | text | 来源 URL |
| source_snapshot_path | text | 快照路径 |
| current_assignee | text | 当前处理人，可空 |
| created_at_utc | timestamptz | 创建时间 |
| updated_at_utc | timestamptz | 更新时间 |

索引建议：

1. review_status, created_at_utc desc
2. country, brand
3. current_assignee

## 14. review.review_decisions

用途：保存 review 的审计日志。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| review_decision_id | uuid | 主键 |
| review_case_id | uuid | FK -> review.review_cases |
| observation_id | uuid | FK -> msrp.observations |
| decision | text | approve / reject / remap |
| decided_official_model | text | 人工确认的官网 model，可空 |
| decided_official_trim | text | 人工确认的官网 trim，可空 |
| note | text | 说明 |
| decided_by | text | 操作人 |
| decided_at_utc | timestamptz | 决策时间 |

索引建议：

1. review_case_id, decided_at_utc desc
2. observation_id

## 15. review.match_overrides

用途：保存长期有效的人工匹配白名单规则。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| override_id | uuid | 主键 |
| country | text | 国家代码 |
| brand | text | 品牌 |
| jato_model | text | JATO model |
| jato_trim | text | JATO trim |
| official_model | text | 官网 model |
| official_trim | text | 官网 trim |
| valid_from_date | date | 生效日期 |
| valid_to_date | date | 失效日期，可空 |
| override_reason | text | 原因 |
| created_by | text | 创建人 |
| created_at_utc | timestamptz | 创建时间 |
| updated_at_utc | timestamptz | 更新时间 |

约束建议：

1. unique(country, brand, jato_model, jato_trim, official_model, official_trim, valid_from_date)

索引建议：

1. country, brand, jato_model
2. valid_from_date, valid_to_date

## 16. alerting.price_alerts

用途：保存价格变化预警事件。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| price_alert_id | uuid | 主键 |
| country | text | 国家代码 |
| brand | text | 品牌 |
| jato_model | text | JATO model |
| jato_trim | text | JATO trim |
| official_model | text | 官网 model |
| official_trim | text | 官网 trim |
| previous_observation_id | uuid | FK -> msrp.observations |
| current_observation_id | uuid | FK -> msrp.observations |
| previous_msrp | numeric(14, 2) | 上一价格 |
| current_msrp | numeric(14, 2) | 当前价格 |
| delta_value | numeric(14, 2) | 差值 |
| delta_pct | numeric(8, 4) | 差值百分比 |
| price_change_direction | text | up / down / unchanged |
| alert_level | text | info / warning / critical |
| alert_status | text | open / reviewed / ignored |
| detected_at_utc | timestamptz | 发现时间 |
| created_at_utc | timestamptz | 创建时间 |

索引建议：

1. alert_status, detected_at_utc desc
2. country, brand
3. alert_level

## 17. alerting.price_sales_effectiveness

用途：保存价格变化后销量效果验证结果。

建议字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| effectiveness_id | uuid | 主键 |
| price_alert_id | uuid | FK -> alerting.price_alerts |
| country | text | 国家代码 |
| brand | text | 品牌 |
| jato_model | text | JATO model |
| jato_trim | text | JATO trim |
| price_event_month | text | 价格事件月份 |
| price_change_direction | text | up / down |
| price_change_value | numeric(14, 2) | 价格变化值 |
| baseline_window_months_json | jsonb | 变化前窗口月份 |
| post_window_months_json | jsonb | 变化后窗口月份 |
| baseline_avg_sales | numeric(14, 2) | 前窗口平均销量 |
| post_avg_sales | numeric(14, 2) | 后窗口平均销量 |
| sales_delta | numeric(14, 2) | 销量变化值 |
| sales_delta_pct | numeric(8, 4) | 销量变化百分比 |
| effectiveness_label | text | positive / neutral / negative / insufficient_data |
| confidence_note | text | 结论说明 |
| generated_at_utc | timestamptz | 生成时间 |

索引建议：

1. country, brand, jato_model
2. price_event_month
3. effectiveness_label

## 18. 关键关系说明

主要关系如下：

1. ops.import_batches -> engineering.config_import_batches
2. engineering.config_projects -> engineering.config_import_batches
3. engineering.config_import_batches -> engineering.config_variants
4. msrp.sources -> msrp.observations
5. msrp.scrape_batches -> msrp.observations
6. msrp.observations -> review.review_cases
7. review.review_cases -> review.review_decisions
8. review.match_overrides 影响 msrp.observations 的后续匹配与 msrp.current_prices 的物化
9. msrp.observations -> alerting.price_alerts
10. alerting.price_alerts -> alerting.price_sales_effectiveness

### 18.1 2026-04-21 审计后新增的结构护栏

基于本地 PostgreSQL 实例审计，当前实现需要把下面几条从“应用层约定”升级成“数据库级护栏”：

1. `msrp.price_history` 必须禁止同一业务键出现重叠价格区间。
2. `review.match_overrides` 必须禁止同一业务键出现重叠有效期。
3. `review.review_decisions` 必须和 `review_cases` 里的 `review_case_id + observation_id` 成对一致，不能只靠两条独立外键。
4. `engineering.market_feature_overrides` 的 `bool/number/text/json` 多态值列必须强制“恰好一列有值”。
5. `current_prices / observations / price_history / engineering import` 等真实外键需要补齐索引。

这些约束的目标不是追求“更严格的 schema 好看”，而是把现在已经由服务层隐式维护的事实收回到 PostgreSQL 自己保证：

1. MSRP current truth 的时间线不重叠。
2. dated override 不会产生双重生效窗口。
3. review 审计链不会出现 case / observation 漂移。
4. 多态 feature value 不会写出自相矛盾的数据。

## 19. 与 Parquet 的边界

以下结论必须明确：

1. PostgreSQL 不承载 JATO 主事实表。
2. PostgreSQL 只承载业务库和操作型数据。
3. JATO 分析查询继续走 Parquet。
4. 网站中的后台配置、MSRP、review、alerts 直接走 PostgreSQL。

## 20. 第一阶段实现建议

建议开发顺序：

1. 先建 ops、engineering、msrp、review、alerting schema。
2. 先落 import_batches、config_projects、config_import_batches、config_variants。
3. 再落 sources、scrape_batches、observations、current_prices。
4. 最后落 review_cases、review_decisions、match_overrides、price_alerts、price_sales_effectiveness。

## 21. 暂不纳入第一阶段的表

第一阶段可以先不做：

1. 用户账号体系表。
2. 复杂权限模型。
3. 通用任务队列表。
4. 通知中心表。

如果后续 review 用户数量增加，再补充这些通用平台表。

## 22. 当前推荐的 domain 文档入口（2026-04-21）

为了避免后续把 news / VOC / MSRP 的关系库入口写散，这里把当前推荐入口固定如下：

1. MSRP 数据库架构入口：`Fullstack/MSRP/README.md` + 本文档。
2. VOC 数据库边界入口：`Fullstack/02_DataETL/VOC_FORUM_IMPLEMENTATION_STATUS_2026-04-19.md` + 本文档。
3. News 数据库边界入口：`Fullstack/01_DevWorkflow/COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md` + 本文档。
4. 迁移落地入口：`Fullstack/03_Database/ALEMBIC_MIGRATION_PLAN_2026-04-10.md` + `06_AppPlatform/backend/alembic/versions/`。
