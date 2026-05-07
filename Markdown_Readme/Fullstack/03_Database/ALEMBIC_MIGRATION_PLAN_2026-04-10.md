# Alembic Migration Plan

状态：Active

日期：2026-04-10

关联文档：

1. [PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md](PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md)
2. [POSTGRESQL_CORE_SCHEMA_2026-04-10.md](POSTGRESQL_CORE_SCHEMA_2026-04-10.md)

## 1. 目标

本计划用于把 PostgreSQL 建表顺序拆成可执行的 Alembic 迁移步骤，避免一次迁移把所有业务表混在一起，后续难以回滚和定位问题。

## 2. 迁移原则

1. 先 schema 和元数据，再业务事实表。
2. 先低耦合表，再高依赖表。
3. 先支撑导入和来源管理，再支撑 observation、review、alerts。
4. 每个 revision 只覆盖一个清晰的业务阶段。

## 3. 推荐迁移顺序

### 3.1 0001 foundation

用途：先把 PostgreSQL 跑道搭起来。

内容：

1. 创建 ops、engineering、msrp、review schema。
2. 创建 ops.import_batches。
3. 创建 engineering.config_projects。
4. 创建 msrp.sources。
5. 创建 review.match_overrides。

为什么先做这一批：

1. 这些表依赖最少。
2. 可以立刻支撑项目管理、官网来源配置、人工 override。
3. 适合作为第一批 API 路由的后端承载表。

当前代码状态：已落地对应 Alembic revision。

### 3.2 0002 engineering imports

用途：让工程配置表可以进入数据库。

内容：

1. 创建 engineering.config_import_batches。
2. 创建 engineering.config_variants。
3. 增加必要索引和 JSONB 索引。

为什么第二步做它：

1. 工程配置表是后续网站 CRUD 的核心业务数据。
2. 它依赖 config_projects 和 import_batches 已先存在。

### 3.3 0003 msrp ingestion

用途：让官网 MSRP 抓取结果能结构化落库。

内容：

1. 创建 msrp.scrape_batches。
2. 创建 msrp.observations。

为什么第三步做它：

1. 需要先有 sources。
2. observation 是后续 current prices、review queue、alerts 的上游事实表。

### 3.4 0004 serving and review queue

用途：生成当前可用价格与人工 review 载体。

内容：

1. 创建 msrp.current_prices。
2. 创建 review.review_cases。
3. 创建 review.review_decisions。

为什么第四步做它：

1. 这些表依赖 observations 已落地。
2. review_cases 和 review_decisions 可以开始支撑人工操作闭环。

### 3.5 0005 alerting

用途：实现价格预警与价格效果分析。

内容：

1. 创建 alerting schema。
2. 创建 alerting.price_alerts。
3. 创建 alerting.price_sales_effectiveness。

为什么最后做：

1. alerts 依赖 current prices 或 observations。
2. effectiveness 又依赖 alerts 和后续 JATO 月度数据。

### 3.6 0012 news auto review + VOC staging

用途：把 app-facing news snapshot 和 VOC raw staging 正式接进 PostgreSQL。

内容：

1. 给 `ops.country_news_articles` / `ops.country_news_digests` 增加 auto-review 字段。
2. 创建 `ops.voc_source_runs`。
3. 创建 `ops.voc_raw_documents`。

为什么这一步要进 PostgreSQL：

1. news 需要 app-facing snapshot / digest store，而不是继续依赖 toolkit-only 文件。
2. VOC 需要可筛选、可审计、可观察的 raw staging 层。
3. 这两类表都属于业务操作型数据，不应该回落到 Parquet。

### 3.7 0013 database guardrails

用途：把本来只在服务层隐式成立的数据库规则收回到 PostgreSQL 自己保证。

内容：

1. 给 `msrp.price_history` 增加时间窗口合法性检查与业务键区间排斥约束。
2. 给 `review.match_overrides` 增加有效期合法性检查与业务键区间排斥约束。
3. 给 `review.review_decisions` 增加 `review_case_id + observation_id` 复合一致性约束。
4. 给 `engineering.market_feature_overrides` 增加多态值列一致性 CHECK。
5. 给 `engineering/msrp` 下真实外键补索引。

为什么这一步必须补：

1. 当前 live 数据虽然干净，但 schema 本身还没防住时态重叠和冗余漂移。
2. 这些问题越到后期越难靠脚本补救。
3. 现在数据量还小，补这类约束的成本最低。

## 4. Revision 命名建议

建议统一格式：

1. 20260410_0001_foundation
2. 20260410_0002_engineering_imports
3. 20260410_0003_msrp_ingestion
4. 20260410_0004_review_and_serving
5. 20260410_0005_alerting
6. 20260419_0012_news_auto_review_and_voc_staging
7. 20260421_0013_database_guardrails

## 5. Alembic 操作约定

建议命令：

    cd 06_AppPlatform/backend
    export APP_DATABASE_ENABLED=true
    export APP_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/jato_app
    alembic upgrade head

新增 revision：

    alembic revision -m "add engineering imports"

如果需要基于 metadata 自动对比：

    alembic revision --autogenerate -m "add msrp ingestion tables"

## 6. 回滚策略

建议原则：

1. 每个 revision 必须有 downgrade。
2. 一次只回滚一个业务阶段。
3. 不要把大量表合并进一个不可控 revision。

## 7. 开发建议

当前最优顺序：

1. 先完成 0001 基础 revision 和配套 API。
2. 再做 0002 工程配置导入链路。
3. 再做 0003 MSRP observation 链路。
4. 最后做 review queue 和 alerting。
