# Official MSRP Enrichment Technical Design

状态：Draft

日期：2026-04-10

关联 PRD：[MSRP_Official_Price_Enrichment_PRD_2026-04-10.md](../01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md)

更新说明：2026-04-10 后续平台架构决策已收敛为"JATO 主分析读模型继续保留 Parquet，工程配置表、MSRP、review、override、alerts 迁入 PostgreSQL，原始 xlsx 与抓取快照保留在磁盘"。涉及平台级数据库边界与 PostgreSQL 表结构时，以 [PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md](./PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md) 和 [POSTGRESQL_CORE_SCHEMA_2026-04-10.md](./POSTGRESQL_CORE_SCHEMA_2026-04-10.md) 为准。

## 1. 设计目标

本设计稿用于把 MSRP 官网价格补全 PRD 收敛成可开发的技术方案，重点定义：

1. 表结构。
2. 存储布局。
3. 周级任务流。
4. 模块边界。
5. 与现有 FastAPI + Parquet 架构的接入方式。

## 2. 现有仓库约束与设计决策

基于当前仓库实现，设计时采用以下约束：

1. 主业务后端是 FastAPI。
2. 现有核心数据读取依赖 04_Processed_data 下的 Parquet 与分区数据集。
3. 现有后端已经有 Parquet read path、manifest 读取、轻量 JSON 写入模式。
4. 当前仓库没有必要在第一阶段直接引入新的 OLTP 数据库。

因此第一阶段的核心设计决策如下：

1. Official MSRP enrichment 采用 sidecar 数据域，不直接修改现有 JATO 主事实表。
2. 原始官网抓取快照按原始证据保留，不覆盖旧版本。
3. 归一化结果、预警结果、销量验证结果统一物化为 Parquet 读模型。
4. 人工 review 决策采用低写入量操作日志，再定期物化到 review 与 serving 表。
5. Dashboard 和 API 只读物化后的 serving 表，不直接驱动抓取。

## 3. 总体架构

整体链路分成五层：

1. Candidate 层：根据当前 JATO 数据生成待抓取对象。
2. Extractor 层：按国家和品牌调用对应官网采集器。
3. Normalize + Match 层：标准化价格并与 JATO model / trim 做置信匹配。
4. Serving 层：输出 latest prices、review queue、alerts、effectiveness 等 Parquet 表。
5. API / UI 层：FastAPI 读取 serving 表，前端消费缓存结果。

## 4. 目录与存储布局

### 4.1 配置目录

建议新增：

    02_Config_MetaData/
      msrp/
        source_registry.yaml
        match_rules.yaml
        alert_rules.yaml
        pilot_scope.yaml

用途：

1. source_registry.yaml：维护国家-品牌来源、入口 URL、采集器类型。
2. match_rules.yaml：维护品牌别名、trim 规则、人工白名单映射。
3. alert_rules.yaml：维护价格波动阈值。
4. pilot_scope.yaml：维护第一阶段国家、品牌、TopN 范围。

### 4.2 原始快照目录

建议新增：

    01_RAW_DATA/
      official_price_snapshots/
        country=SE/
          brand=Volvo/
            scrape_batch_id=20260410T020000Z/
              source_id=se-volvo-configurator/
                snapshot_001.html.gz
                snapshot_002.json.gz

设计原则：

1. 原始抓取快照属于审计证据，保留原文。
2. 快照不可覆盖，只追加。
3. 支持 HTML、JSON、PDF、PNG 截图等多种原始载体。

### 4.3 归一化与 serving 目录

建议新增：

    04_Processed_data/
      msrp_enrichment/
        batches/
        candidates/
        observations/
        latest_prices/
        review_queue/
        alerts/
        effectiveness/
        manifests/
        operational/

目录用途：

1. batches：抓取批次元数据。
2. candidates：候选抓取对象快照。
3. observations：归一化后的价格观测事实表。
4. latest_prices：供 API / Dashboard 直接读取的当前生效价格表。
5. review_queue：低置信度待人工复核表。
6. alerts：价格波动预警表。
7. effectiveness：价格变化与销量验证结果。
8. manifests：每个任务物化后的 manifest。
9. operational：低频可变操作日志，例如人工 review decision log。

## 5. 物理存储策略

### 5.1 读多写少的数据

以下数据采用 Parquet：

1. scrape_batches
2. candidate_models_weekly
3. official_price_observations
4. review_queue
5. current_official_prices
6. price_change_alerts
7. price_sales_effectiveness

### 5.2 低频人工写入的数据

以下数据采用 JSONL 操作日志，再物化：

1. review_decisions.jsonl
2. manual_match_overrides.jsonl

原因：

1. 人工 review 写频低。
2. 当前仓库已有轻量 JSON 写入模式。
3. 可以避免第一阶段引入数据库。

## 6. 表结构定义

### 6.1 source_registry

存储位置建议：

1. 源配置文件：02_Config_MetaData/msrp/source_registry.yaml
2. 运行时解析后也可输出一份镜像表到 04_Processed_data/msrp_enrichment/batches/source_registry_snapshot.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| source_id | string | 主键，示例 se-volvo-configurator |
| country | string | ISO 国家代码，建议用 SE、DE 等 |
| brand | string | 品牌名 |
| source_url | string | 官方入口 URL |
| source_type | string | api / html / playwright / pdf |
| extractor_name | string | 绑定的 extractor 名称 |
| extractor_version | string | 当前 extractor 版本 |
| price_semantics | string | msrp_including_vat 等 |
| requires_location | bool | 是否依赖邮编或经销商选择 |
| request_headers_profile | string | 请求头配置名 |
| enabled | bool | 是否启用 |
| notes | string | 备注 |

约束：

1. source_id 唯一。
2. 同一 country + brand 可允许多个 source_id，但只能有一个 primary source。

### 6.2 scrape_batches

存储位置建议：

1. 04_Processed_data/msrp_enrichment/batches/scrape_batches.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| scrape_batch_id | string | 主键，示例 20260410T020000Z-se-weekly |
| trigger_type | string | scheduled / manual / backfill |
| scope_country | string | 本批次国家 |
| scope_brands_json | string | 品牌列表 JSON |
| candidate_count | int64 | 待抓取候选数 |
| source_count | int64 | 实际使用 source 数 |
| status | string | pending / running / partial / success / failed |
| started_at_utc | timestamp | 开始时间 |
| finished_at_utc | timestamp | 完成时间 |
| error_count | int64 | 失败数 |
| warning_count | int64 | 警告数 |
| manifest_path | string | 任务 manifest 路径 |

### 6.3 candidate_models_weekly

存储位置建议：

1. 04_Processed_data/msrp_enrichment/candidates/observed_week=YYYY-WW/*.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| candidate_id | string | 主键 |
| scrape_batch_id | string | 对应抓取批次 |
| candidate_week | string | ISO week |
| country | string | 国家代码 |
| brand | string | 品牌 |
| jato_model | string | JATO model |
| jato_trim | string | JATO trim |
| sales_month | string | 候选生成时对应销量月份 |
| current_sales | float64 | 当前销量 |
| model_rank_in_country | int64 | 国家内销量排名 |
| selection_reason | string | top_n / manual / retry |
| source_id | string | 命中的官网来源 |
| created_at_utc | timestamp | 创建时间 |

唯一键建议：

1. candidate_week + country + brand + jato_model + coalesce(jato_trim, '')

### 6.4 official_price_observations

存储位置建议：

1. 04_Processed_data/msrp_enrichment/observations/observed_week=YYYY-WW/country=SE/brand=Volvo/*.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| observation_id | string | 主键 |
| scrape_batch_id | string | 抓取批次 |
| source_id | string | 来源配置 |
| country | string | 国家代码 |
| brand | string | 品牌 |
| jato_model | string | JATO model |
| jato_trim | string | JATO trim |
| official_model | string | 官网 model |
| official_trim | string | 官网 trim |
| msrp_value | float64 | 标准化后的 MSRP |
| currency | string | 币种 |
| tax_included | bool | 是否含税 |
| price_label | string | msrp / campaign / finance / unknown |
| availability_text | string | 官网原始在售文本 |
| observed_at_utc | timestamp | 抓取时间 |
| observed_week | string | ISO week |
| source_url | string | 价格来源 URL |
| source_snapshot_path | string | 原始快照路径 |
| source_payload_hash | string | 原始内容摘要 |
| source_type | string | api / html / playwright / pdf |
| extraction_version | string | 提取规则版本 |
| match_confidence | float64 | 自动匹配得分 |
| match_status | string | auto_accepted / review_required / human_approved / rejected |
| match_reason_json | string | 匹配明细 JSON |
| created_at_utc | timestamp | 记录创建时间 |
| updated_at_utc | timestamp | 记录更新时间 |

说明：

1. 这是 append-only 事实表。
2. 同一官网 model / trim 在不同周会有多条 observation。
3. 人工 review 不直接覆盖原 observation，只更新 match_status 并生成 serving 结果。

### 6.5 review_queue

存储位置建议：

1. 04_Processed_data/msrp_enrichment/review_queue/status=open/*.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| review_id | string | 主键 |
| observation_id | string | 对应 observation |
| country | string | 国家代码 |
| brand | string | 品牌 |
| jato_model | string | JATO model |
| jato_trim | string | JATO trim |
| official_model | string | 官网 model |
| official_trim | string | 官网 trim |
| candidate_matches_json | string | 候选匹配列表 JSON |
| match_confidence | float64 | 自动匹配分数 |
| review_status | string | open / in_review / approved / rejected |
| source_url | string | 来源 URL |
| source_snapshot_path | string | 快照路径 |
| created_at_utc | timestamp | 创建时间 |
| updated_at_utc | timestamp | 更新时间 |

数据来源：

1. 由 observations 中 match_status=review_required 的记录物化生成。

### 6.6 current_official_prices

存储位置建议：

1. 04_Processed_data/msrp_enrichment/latest_prices/current_official_prices.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| serving_id | string | 主键 |
| country | string | 国家代码 |
| brand | string | 品牌 |
| jato_model | string | JATO model |
| jato_trim | string | JATO trim |
| official_model | string | 官网 model |
| official_trim | string | 官网 trim |
| current_msrp_value | float64 | 当前生效 MSRP |
| currency | string | 币种 |
| tax_included | bool | 是否含税 |
| effective_observation_id | string | 当前生效 observation |
| effective_observed_at_utc | timestamp | 当前生效抓取时间 |
| match_confidence | float64 | 当前匹配得分 |
| match_status | string | auto_accepted / human_approved |
| source_url | string | 来源 URL |
| source_snapshot_path | string | 快照路径 |
| last_price_change_at_utc | timestamp | 最近一次价格变化时间 |
| updated_at_utc | timestamp | 物化时间 |

说明：

1. current_official_prices 是 API 和 Dashboard 的首选读取表。
2. 只包含可对外展示的 accepted 结果。

### 6.7 price_change_alerts

存储位置建议：

1. 04_Processed_data/msrp_enrichment/alerts/detected_week=YYYY-WW/*.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| alert_id | string | 主键 |
| country | string | 国家代码 |
| brand | string | 品牌 |
| jato_model | string | JATO model |
| jato_trim | string | JATO trim |
| official_model | string | 官网 model |
| official_trim | string | 官网 trim |
| previous_observation_id | string | 上一个 accepted observation |
| current_observation_id | string | 当前 accepted observation |
| previous_msrp | float64 | 上一价格 |
| current_msrp | float64 | 当前价格 |
| delta_value | float64 | 差值 |
| delta_pct | float64 | 差值百分比 |
| price_change_direction | string | up / down / unchanged |
| alert_level | string | info / warning / critical |
| detected_at_utc | timestamp | 发现时间 |
| alert_status | string | open / reviewed / ignored |

### 6.8 price_sales_effectiveness

存储位置建议：

1. 04_Processed_data/msrp_enrichment/effectiveness/price_event_month=YYYY-MM/*.parquet

字段定义：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| analysis_id | string | 主键 |
| alert_id | string | 对应价格变化 alert |
| country | string | 国家代码 |
| brand | string | 品牌 |
| jato_model | string | JATO model |
| jato_trim | string | JATO trim |
| price_event_month | string | 价格变化月份 |
| price_change_direction | string | up / down |
| price_change_value | float64 | 价格变化值 |
| baseline_window_months_json | string | 变化前窗口月份 JSON |
| post_window_months_json | string | 变化后窗口月份 JSON |
| baseline_avg_sales | float64 | 前窗口平均销量 |
| post_avg_sales | float64 | 后窗口平均销量 |
| sales_delta | float64 | 销量变化 |
| sales_delta_pct | float64 | 销量变化百分比 |
| effectiveness_label | string | positive / neutral / negative / insufficient_data |
| confidence_note | string | 置信说明 |
| generated_at_utc | timestamp | 生成时间 |

## 7. 模块边界

### 7.1 配置模块

建议路径：

1. 02_Config_MetaData/msrp/source_registry.yaml
2. 02_Config_MetaData/msrp/match_rules.yaml
3. 02_Config_MetaData/msrp/alert_rules.yaml
4. 02_Config_MetaData/msrp/pilot_scope.yaml

职责：

1. 维护国家、品牌、采集源。
2. 维护匹配规则和人工 override。
3. 维护告警阈值和 pilot 范围。

### 7.2 批处理脚本模块

建议新增脚本：

1. 03_Scripts/msrp_generate_candidates.py
2. 03_Scripts/msrp_run_scrape_batch.py
3. 03_Scripts/msrp_materialize_review_queue.py
4. 03_Scripts/msrp_materialize_current_prices.py
5. 03_Scripts/msrp_generate_price_alerts.py
6. 03_Scripts/msrp_compute_sales_effectiveness.py

职责划分：

1. msrp_generate_candidates.py：从当前 JATO parquet 生成候选池。
2. msrp_run_scrape_batch.py：执行官网抓取、规范化和 observation 写入。
3. msrp_materialize_review_queue.py：从 review_required observation 物化待复核结果。
4. msrp_materialize_current_prices.py：从 accepted observation 物化 current_official_prices。
5. msrp_generate_price_alerts.py：对 accepted observation 做前后 diff。
6. msrp_compute_sales_effectiveness.py：把价格变化事件与 JATO sales 做窗口分析。

### 7.3 Extractor Adapter 模块

建议新增包：

1. 03_Scripts/msrp_extractors/base.py
2. 03_Scripts/msrp_extractors/volvo_se.py
3. 03_Scripts/msrp_extractors/tesla_se.py
4. 03_Scripts/msrp_extractors/bmw_se.py

职责：

1. 一个国家-品牌一个 adapter。
2. 对外统一返回 normalize 前的原始提取记录。
3. 不负责 JATO 匹配，只负责官网提取。

### 7.4 匹配模块

建议新增包：

1. 03_Scripts/msrp_matching/normalize_names.py
2. 03_Scripts/msrp_matching/scoring.py
3. 03_Scripts/msrp_matching/overrides.py

职责：

1. 标准化品牌、model、trim 名称。
2. 计算候选匹配得分。
3. 应用人工 override。

### 7.5 后端读取模块

建议新增：

1. 06_AppPlatform/backend/app/infra/msrp_repository.py
2. 06_AppPlatform/backend/app/services/msrp_service.py
3. 06_AppPlatform/backend/app/api/routes/msrp.py

职责：

1. msrp_repository.py：负责读取 latest_prices、alerts、review_queue、effectiveness。
2. msrp_service.py：负责聚合查询和 review 写入协调。
3. msrp.py：暴露 API 路由。

### 7.6 前端模块

第一阶段不做实时抓取按钮。

建议只接：

1. 当前官方价格视图。
2. 价格波动预警列表。
3. 价格变化后的销量验证视图。
4. 人工 review 后台页。

## 8. 匹配设计

### 8.1 预处理

匹配前统一处理：

1. 大小写标准化。
2. 去掉多余空格、标点、市场营销词。
3. 统一品牌别名和 trim 关键词。
4. 把 AWD、RWD、Long Range、Ultra 等特征词单独抽出。

### 8.2 匹配打分

第一阶段建议采用规则加分制，而不是复杂模型。

建议分数组成：

1. model 名称相似度：0.40
2. trim 名称相似度：0.30
3. 动力类型一致性：0.10
4. 结构化特征一致性：0.10
5. 品牌 / 国家约束加分：0.10

总分范围：0 到 1。

### 8.3 匹配阈值

建议默认阈值：

1. >= 0.92：auto_accepted
2. >= 0.75 且 < 0.92：review_required
3. < 0.75：rejected

阈值必须可配置。

### 8.4 人工 override

人工 review 通过后，建议额外生成 override 规则：

1. country + brand + jato_model + jato_trim
2. official_model + official_trim
3. valid_from
4. override_reason

这样下一周同一案例可以直接命中白名单。

## 9. 任务流定义

### 9.1 周级抓取主流程

触发方式：

1. 每周定时。
2. 人工手动。
3. 某国家 / 品牌 backfill。

主流程：

1. 读取 pilot_scope.yaml。
2. 从当前 JATO canonical parquet 生成候选对象。
3. 创建 scrape_batch 记录。
4. 按 candidate 的 country + brand 分组。
5. 对每组调用对应 extractor adapter。
6. 保存原始官网快照。
7. 归一化提取结果。
8. 执行 JATO 与官网匹配。
9. 写入 official_price_observations。
10. 物化 review_queue。
11. 物化 current_official_prices。
12. 生成 price_change_alerts。
13. 输出 manifest 与批次总结。

### 9.2 人工 review 流程

主流程：

1. API 读取 review_queue。
2. 用户查看候选项、快照和 URL。
3. 用户提交 approve / reject / remap。
4. 后端把 decision 追加写入 operational/review_decisions.jsonl。
5. 定时或手动触发 rematerialize。
6. review_queue、current_official_prices、alerts 根据 decision 重算。

### 9.3 销量验证流程

触发时机：

1. 新的 JATO 月度数据 refresh 完成后。

主流程：

1. 读取最近 N 个月 price_change_alerts。
2. 将 alert 映射到 JATO 的 model / trim。
3. 提取价格变化前窗口销量。
4. 提取价格变化后窗口销量。
5. 计算 baseline_avg_sales、post_avg_sales、sales_delta。
6. 生成 effectiveness_label。
7. 写入 price_sales_effectiveness。

## 10. Serving 读模型设计

### 10.1 API 首选表

API 和前端应优先读取：

1. current_official_prices
2. price_change_alerts
3. price_sales_effectiveness
4. review_queue

不建议前端直接读取：

1. 原始 observation 全表。
2. 原始官网快照文件。

### 10.2 Dashboard 接入方式

Dashboard 接入建议：

1. 在国家和品牌筛选后，展示 current_official_prices 中的官方价格。
2. 对价格变化显著的车型展示 alert badge。
3. 对有价格事件的车型展示销量变化验证卡片。
4. 对 review_required 的结果不在正式分析图中默认展示。

## 11. API 设计建议

建议新增路由：

1. GET /v1/msrp/latest
2. GET /v1/msrp/alerts
3. GET /v1/msrp/effectiveness
4. GET /v1/msrp/reviews
5. POST /v1/msrp/reviews/{review_id}/decision

### 11.1 GET /v1/msrp/latest

用途：

1. 查询当前生效官网价格。

建议过滤参数：

1. country
2. brand
3. model
4. only_accepted

### 11.2 GET /v1/msrp/alerts

用途：

1. 查询价格波动预警。

建议过滤参数：

1. country
2. brand
3. min_alert_level
4. since_week

### 11.3 GET /v1/msrp/effectiveness

用途：

1. 查询价格变化后的销量验证。

建议过滤参数：

1. country
2. brand
3. model
4. event_month

### 11.4 GET /v1/msrp/reviews

用途：

1. 返回待人工 review 的记录。

### 11.5 POST /v1/msrp/reviews/{review_id}/decision

请求字段建议：

1. decision：approve / reject / remap
2. official_model
3. official_trim
4. note

写入行为建议：

1. 只追加写入 review_decisions.jsonl。
2. 不直接在线改写 Parquet 主表。

## 12. 失败处理与幂等

### 12.1 幂等原则

1. scrape_batch_id 唯一。
2. 同一 batch 重跑时允许覆盖该 batch 对应的 manifest，但不覆盖已有快照文件。
3. observation_id 建议由稳定字段哈希生成，避免重复写入。

### 12.2 失败分类

建议分类：

1. source_unreachable
2. parsing_failed
3. price_not_found
4. match_failed
5. snapshot_write_failed

### 12.3 重试策略

建议：

1. 网络失败自动重试 3 次。
2. 解析失败不自动无限重试。
3. 单个 source 失败不应阻塞整个 scrape_batch。

## 13. 观测与审计

每个批次至少输出：

1. 批次级 manifest。
2. 成功 / 失败 source 数。
3. observation 数。
4. auto_accepted / review_required / rejected 数。
5. alert 数。
6. effectiveness 计算数。

每条正式价格至少可追溯到：

1. source_url
2. source_snapshot_path
3. scrape_batch_id
4. extraction_version
5. manual decision log

## 14. 安全与合规

第一阶段必须遵守：

1. 优先使用官网公开页面或官网接口。
2. 控制抓取频率，不做高频并发轰炸。
3. 不绕过登录、验证码或明确禁止访问的机制。
4. 原始快照仅用于内部分析与审计。

## 15. 第一阶段开发切分

建议按以下顺序实施：

1. 建立配置文件与目录结构。
2. 实现 scrape_batches 与 candidate_models_weekly。
3. 实现瑞典 2 到 3 个品牌 extractor。
4. 实现 official_price_observations 落盘。
5. 实现匹配与 review_queue 物化。
6. 实现 current_official_prices serving 表。
7. 实现 alerts 与 effectiveness。
8. 接入 FastAPI 只读接口。

## 16. 明确不做的技术方案

第一阶段不建议做：

1. 前端实时点击后现场抓官网。
2. 通用自由搜索 agent 直接面向生产抓取。
3. 一开始就引入复杂数据库和任务编排平台。
4. 一开始就覆盖全部国家和全部品牌。

## 17. 打开问题

当前仍需业务确认的点：

1. 第一阶段 pilot 品牌最终名单。
2. MSRP 口径是否统一要求含税。
3. 降价有效性验证使用 1 个月、2 个月还是 3 个月销量窗口。
4. 人工 review 是先做后台 API，还是直接做前端页面。
