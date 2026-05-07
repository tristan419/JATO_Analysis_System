# 跨源 Join 设计：JATO Sales × MSRP（2026-04-17）

状态：Draft

关联：
- [PRODUCT_DEEPDIVE_2026-04-17.md](../PRODUCT_DEEPDIVE_2026-04-17.md) § Q1
- [PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md](./PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md)
- [POSTGRESQL_CORE_SCHEMA_2026-04-10.md](./POSTGRESQL_CORE_SCHEMA_2026-04-10.md)
- [`02_DataETL/PRECOMPUTE_STRATEGY.md`](../02_DataETL/PRECOMPUTE_STRATEGY.md)

---

## 1. 问题复述

JATO sales 在 Parquet（分区 + 预聚合），MSRP 在 PostgreSQL（事务 + 审核 + 历史）。Positioning / Pricing 页要同时用两者：**一个月销量 × 一条 trim 的 msrp** 才能画"价格–销量象限"。现在的疑问：继续用 Parquet 能 join 得动吗？

## 2. 核心结论

**Parquet 不扔，三路径并行**：

1. **Online federated**（新）：FastAPI 进程内用 **DuckDB** ATTACH Postgres scanner + 读 partition parquet，一条 SQL 完成 join。单国家延迟目标 ≤ 400 ms。
2. **Offline materialized**（新）：夜间 refresh job 追加一步，产出 `04_Processed_data/positioning/positioning_matrix.parquet`，前端 dashboard 直接读，零 join。
3. **Streaming fallback**：Online federated 不可用时，service 层手写"先查 PG → 按 key list 过滤 parquet"，保证可用但不做主路径。

### 2.1 对当前疑问的直接回答

用户现在的核心担忧是：**sales 总来源于 JATO、MSRP 总来源于库以后，是不是就不能再用 Parquet？**

答案是：**还能用，而且应该继续用；只是 Parquet 不再承担"主键契约"。**

- **Parquet 负责 analytics facts**：销量、segment share、历史趋势、预聚合矩阵。
- **PostgreSQL 负责 canonical entities**：trim、price history、source review、override、link contract。
- **Join contract 不能靠裸 `model` 字符串**：必须显式落在 `jato_msrp_link` 这类桥接表，并保留 `country + brand + normalized model + powertrain + year` 粒度。

### 2.2 推荐的 grain 切分

要把"一款车多个 version / 多个动总 / 多个 trim"讲清楚，至少要分三层 grain：

1. **JATO model grain**：`country_code + jato_brand + jato_model + jato_powertrain + model_year`
2. **sellable trim grain**：`trim_id`（本地可售版型，带本币 MSRP、配置、source history）
3. **positioning matrix grain**：`country_code + jato_model grain + trim_id`

这样处理以后：

- JATO sales 仍然可以在 Parquet 里按 model grain 高效扫描。
- trim / feature / source 冲突继续留在 PG 做事务与审核。
- dashboard / 国家助手主查询可以优先读 materialized `positioning_matrix.parquet`，而不是每次在线拼接大量明细。

### 2.3 Repo 现状与下一步

这不是一张白纸：

- `06_AppPlatform/backend/app/services/market_scan_service.py` 已经在用 **DuckDB + Postgres attach** 做 overlay。
- `06_AppPlatform/backend/app/db/models.py` 已经有 `JatoMsrpLink`、`MatchOverride`、`MsrpObservation`、`CurrentPrice`、`PriceHistory`。

因此当前最合理的推进方式不是重写一套，而是：

1. 保留现有 `CurrentPrice` 作为 published/current projection。
2. 把 `JatoMsrpLink` 作为正式 lifecycle object 使用：active 状态、confidence、link source、notes 都进入主流程；`MatchOverride` 只负责带生效期的例外。
3. 让 positioning / 国家助手优先消费 materialized matrix 或 compositions，而不是前端临时 join。

### 2.4 业务可见性要求（2026-04-20）

截至当前实现，`market_scan_service.py` 在运行时已经会把 **PG reviewed price overlay 的元信息** 放进 deck metadata（`priceOverlay`），其中至少会区分：

- 是否真正命中了 `current_prices`
- 命中了多少 row / model
- 是 `link` 还是 `direct` overlay
- 还是已经退回 `parquet-only`

因此接下来的优先级不应再只是“能不能 join”，而是要让业务面明确看到：

1. **这次定位定价页到底有没有用到 reviewed PG MSRP**
2. **如果没有，是部分未命中还是完全 parquet fallback**
3. **Country Copilot 的定位/定价回答有没有使用这层 reviewed truth**

换句话说，`priceOverlay` 不应只停留在 backend metadata；它应该被提升为 Positioning/Pricing 页和 Country Copilot grounding / trust 的**显式状态层**。否则系统虽然已经做了 cross-source join，业务侧仍然看不出“当前结论用了哪层价格真值”。

## 3. 当前 join contract：`jato_msrp_link` + `match_override`（2026-04-18）

现在 backend 里已经落地的桥接契约不是“直接把 JATO rename 成 official”，而是保留两边原始 key，再通过两层 resolver 串起来：

1. **`JatoMsrpLink`**：稳定映射层，表示“这个 JATO key 通常对应哪个 official key”。
2. **`MatchOverride`**：带 `valid_from / valid_to` 的时态例外层，表示“在这个时间窗口里，link 结果要被临时覆盖”。

这两层共同决定 `MsrpObservation -> CurrentPrice` 的 canonical official key；销量 truth 仍挂 JATO，价格 / 配置 truth 仍挂 official / engineering key。

```sql
CREATE TABLE jato_msrp_link (
    link_id              UUID PRIMARY KEY,
    country              VARCHAR(64) NOT NULL,
    brand                VARCHAR(64) NOT NULL,
    jato_model           VARCHAR(255) NOT NULL,
    jato_trim            VARCHAR(255) NOT NULL,
    jato_powertrain      VARCHAR(64) NOT NULL DEFAULT '',
    official_model       VARCHAR(255) NOT NULL,
    official_trim        VARCHAR(255) NOT NULL,
    official_edition     VARCHAR(255),
    official_powertrain  VARCHAR(64),
    confidence           SMALLINT NOT NULL DEFAULT 80,
    link_source          VARCHAR(64) NOT NULL DEFAULT 'manual',
    is_active            BOOLEAN NOT NULL DEFAULT TRUE,
    notes                TEXT,
    created_at_utc       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE match_override (
    override_id          UUID PRIMARY KEY,
    country              VARCHAR(64) NOT NULL,
    brand                VARCHAR(64) NOT NULL,
    jato_model           VARCHAR(255) NOT NULL,
    jato_trim            VARCHAR(255) NOT NULL,
    jato_powertrain      VARCHAR(64) NOT NULL DEFAULT '',
    official_model       VARCHAR(255) NOT NULL,
    official_trim        VARCHAR(255) NOT NULL,
    valid_from_date      DATE NOT NULL,
    valid_to_date        DATE,
    override_reason      TEXT NOT NULL,
    created_by           VARCHAR(128) NOT NULL,
    created_at_utc       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at_utc       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### 3.1 Resolver 优先级

同一个 observation 进入 ingest / materialize 时，按下面顺序解析 canonical mapping：

1. **有效期内的 `MatchOverride`**
2. **active 的 `JatoMsrpLink`**
3. **observation 自带的 official 字段**

这样做的原因：

- `JatoMsrpLink` 负责长期稳定映射，避免同一 JATO trim 每次都重复 review。
- `MatchOverride` 负责时间窗例外，避免把短期命名调整或过渡版本写死成永久 link。
- observation 原字段仍保留，resolver 只是决定 published/current projection 用哪个 official key。

### 3.2 Review 写回规则

review decision 在 2026-04-18 这一批开始不再只写 `MatchOverride`：

1. **approve / remap**：会 upsert 一个 active `JatoMsrpLink`，并把同一 JATO key 下冲突的旧 active link 退役。
2. **persist_override=true**：另外再写一条 `MatchOverride`，只覆盖指定生效期。
3. **ingest / current price materialize**：统一走同一个 canonical mapping resolver，不再一边只看 override、一边直接信 observation 字段。

这意味着“一次人工确认，后续 observation 继承”终于真正进入主链路，而不是停留在孤立 review 记录。

### 3.3 mismatch taxonomy

当前 resolver 和 review decision 统一使用四类 mismatch reason：

1. `naming_mismatch`：同一个 official target，主要差在命名或 trim label。
2. `timing_mismatch`：只在某个时间窗内需要临时覆盖，优先落 `MatchOverride`。
3. `market_mismatch`：JATO key 与 official market key 实际不在同一个市场表达。
4. `granularity_mismatch`：一侧比另一侧更粗或更细，例如 edition / powertrain 颗粒度不一致。

这些分类会进入 observation 的 `match_reason_json.mappingResolver`，供后续 reconcile、assistant answer 和 workbench UI 共用。

## 4. 在线 federated 查询方案（DuckDB）

### 4.1 技术点
- 后端进程内 `import duckdb`；启动时 `con.install_extension('postgres_scanner'); con.load_extension('postgres_scanner')`。
- `con.execute("ATTACH 'dbname=jato user=... password=... host=127.0.0.1' AS pg (TYPE POSTGRES, READ_ONLY true)")`
- Parquet 直接 `FROM read_parquet('04_Processed_data/partitioned_dataset_v1/国家=SE/*.parquet')`。

### 4.2 典型 SQL（positioning 页单国家）
```sql
WITH sales_12m AS (
    SELECT
        "品牌" AS jato_brand,
        "Model" AS jato_model,
        COALESCE("动力总成", '-') AS jato_powertrain,
        SUM("Sales") AS sales_12m
    FROM read_parquet('04_Processed_data/partitioned_dataset_v1/国家=SE/*.parquet')
    WHERE "Year-Month" >= '2025-05'
    GROUP BY 1, 2, 3
),
msrp AS (
    SELECT
        l.country_code,
        l.jato_brand,
        l.jato_model,
        l.jato_powertrain,
        t.trim_id,
        t.local_name AS trim_name,
        p.base_msrp_eur,
        p.effective_from
    FROM pg.jato_msrp_link l
    JOIN pg.msrp_trim t ON t.trim_id = l.msrp_trim_id
    JOIN pg.msrp_current_price p ON p.trim_id = t.trim_id
    WHERE l.country_code = 'SE'
)
SELECT
    s.jato_brand, s.jato_model, s.jato_powertrain, s.sales_12m,
    m.trim_id, m.trim_name, m.base_msrp_eur
FROM sales_12m s
LEFT JOIN msrp m
  ON m.jato_brand = s.jato_brand
 AND m.jato_model = s.jato_model
 AND m.jato_powertrain = s.jato_powertrain
ORDER BY s.sales_12m DESC
LIMIT 200;
```

### 4.3 为什么不是 SQLAlchemy + python 端 merge
- Parquet 90 万行反序列化到 pandas 再 merge，内存峰值约 1.2 GB；DuckDB 在 C++ 侧做 hash join，内存 < 150 MB。
- 单机 16 GB 内存必须走 DuckDB。

### 4.4 安全与权限
- DuckDB 连接 PG 用**只读账号**，禁止 `UPDATE`/`DELETE`/`INSERT`。
- 所有 `read_parquet(...)` 路径白名单在 `app/core/config.py` 里 `ALLOWED_PARQUET_ROOTS` 固定。
- 任何 user-facing SQL 参数继续走 bind parameters，不拼字符串，防 SQL 注入。

## 5. 离线 positioning matrix（主路径）

### 5.1 产物
```
04_Processed_data/positioning/
├── positioning_matrix.parquet        # country × model × trim 宽表
├── positioning_by_segment.parquet    # country × segment 聚合
└── manifest.json
```

### 5.2 字段（positioning_matrix）
| 列 | 类型 | 来源 |
|----|------|------|
| country_code | string | JATO |
| jato_brand | string | JATO |
| jato_model | string | JATO |
| jato_powertrain | string | JATO |
| sales_12m | int64 | JATO 聚合 |
| sales_3m | int64 | JATO 聚合 |
| trim_id | int64 | PG |
| trim_name | string | PG |
| base_msrp_local | float | PG |
| base_msrp_eur | float | PG |
| currency | string | PG |
| link_confidence | int8 | PG `jato_msrp_link` |
| as_of_date | date | job 执行日期 |

### 5.3 何时产出
- `run_data_refresh_job.py` 在"预聚合 summaries"之后追加 `build_positioning_matrix.py` 一步。
- 也在 MSRP reconciliation 完成后**增量刷新**（只更新受影响的 country）。

### 5.4 前端路径
`PositioningPricingPage.tsx` 后端端点 `/api/positioning/scatter?country=SE` 直接返回这张表的切片（按 country 切），不再触发 join。

## 6. Compositions 层（按 ARCHITECTURE_REVIEW P0-1 要求）

```
06_AppPlatform/backend/app/services/compositions/
├── __init__.py
├── positioning.py           # DuckDB federated 查询 / 读 matrix parquet
├── msrp_enriched.py         # MSRP + JATO brand family 维度补全
└── review_enriched.py       # review_decision + MSRP observation + JATO
```

- 约定：**所有跨 PG × Parquet 的 join 只在 `compositions/` 下发生**。
- Route 层只调用 `compositions`，不写 SQL。
- Repository 层（`app/infra/`）只负责单一数据源访问。

## 7. 迁移步骤

1. Alembic `0007_add_jato_msrp_link`：建 `jato_msrp_link` 表 + ETL 从 `model_rules` 派生初始数据。
2. 新建 `app/services/compositions/positioning.py`，实现 DuckDB federated 查询，覆盖 PositioningPricingPage 的主调用。
3. `03_Scripts/build_positioning_matrix.py`：离线产出。
4. `run_data_refresh_job.py` 加入调用；MSRP reconciliation 任务也触发增量刷新。
5. 前端 PositioningPricingPage 切换数据源；保留旧路径 30 天作为 fallback。
6. 观察 1 周后废弃旧路径。

## 8. 验收

- 单国家 (SE) positioning 查询 P95 ≤ 400 ms（federated 在线路径）。
- `positioning_matrix.parquet` 刷新耗时 ≤ 5 min（全 22 国）。
- `jato_msrp_link` 覆盖率：Top 30 SUV / 国家 ≥ 80%。
- 前端切换后，Positioning 页首屏 ≤ 1.5 s（12 Mbps）。

## 9. 回滚

- 去 `00_AppPlatform/backend/app/services/compositions/positioning.py` 的 feature flag `USE_DUCKDB_FEDERATED`，回退到旧 path。
- Alembic `downgrade 0007` 撤销链接表（数据仍在 YAML，可重建）。
