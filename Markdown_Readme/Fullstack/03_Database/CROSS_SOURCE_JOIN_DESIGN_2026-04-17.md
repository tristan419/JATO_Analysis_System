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

## 3. 关键表：`jato_msrp_link`（PG）

把 JATO 的 dimensional key 与 MSRP 的 trim 建立**多对多显式映射**，这是跨源 join 的唯一契约。

```sql
CREATE TABLE jato_msrp_link (
    link_id             BIGSERIAL PRIMARY KEY,
    country_code        VARCHAR(4)  NOT NULL,       -- 'SE'
    jato_brand          VARCHAR(64) NOT NULL,       -- JATO '品牌'
    jato_model          VARCHAR(128) NOT NULL,      -- JATO 'Model'
    jato_powertrain     VARCHAR(32),                -- 'HEV'|'PHEV'|'BEV'|'MHEV'|'ICE'
    jato_model_year     SMALLINT,                   -- 2026, 可空
    msrp_trim_id        BIGINT NOT NULL REFERENCES msrp_trim(trim_id),
    confidence          SMALLINT NOT NULL DEFAULT 80,   -- 0-100
    source              VARCHAR(32) NOT NULL,       -- 'model_rules' | 'override' | 'manual'
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (country_code, jato_brand, jato_model, jato_powertrain, jato_model_year, msrp_trim_id)
);

CREATE INDEX ix_link_jato_key ON jato_msrp_link (country_code, jato_brand, jato_model, jato_powertrain);
CREATE INDEX ix_link_trim ON jato_msrp_link (msrp_trim_id);
```

来源优先级：
1. 从 MSRP source YAML 的 `model_rules` / `fixed_jato_model` / `fixed_jato_powertrain` 自动派生（ETL 时 upsert）。
2. review workbench 里 reviewer 手动 override（source='override'）。
3. 人工补录（source='manual'）。

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
