# 统一抓取流水线（Unified Scraping Pipeline）2026-04-17

状态：Draft（Q6：MSRP + news + policy + incentive + spec 统一抓取平台）

关联：
- [PRODUCT_DEEPDIVE_2026-04-17.md](../PRODUCT_DEEPDIVE_2026-04-17.md) § Q6
- `07_ScrapingToolkit/README.md`
- [MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md](../MSRP/03_Implementation/MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md)
- [COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md](../01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md)

---

## 1. 目标

把现在 `07_ScrapingToolkit` 里以 MSRP 为中心的 pipeline 扩展为**多 domain 统一抓取平台**，覆盖：
- `msrp` — 官方 / 经销商价格（已在跑）
- `news` — 汽车 / 财经 / 政策新闻（Phase 3 进行中）
- `policy` — 碳税、排放、Malus/Bonus、路权、城市限制
- `incentive` — BEV / PHEV 补贴、企业税优惠、充电补贴
- `spec` — 官方 configuration 表（trim feature catalog 的源头）

同时不打破现有架构的边界约束：
- 仍然定位为**离线制品生产者**（见 ARCHITECTURE_REVIEW P1-1）。
- 单机运行，不引入 Redis / Celery / Airflow / K8s。
- FastAPI 进程不 import toolkit 的 fetcher / extractor 运行时模块。

## 2. 工具分工

| 工具 | 主战场 | 用例 | 层 |
|------|--------|------|---|
| **Scrapling**（已用） | 规则化 CSS 抽取；结构稳定的官网 | MSRP brand-family extractors | Extractor |
| **Playwright**（已用） | JS 渲染、反爬、登录态、动态加载 | Volvo / VW / Audi / 某些 dealer 站 | Fetcher |
| **httpx / requests** | 静态 HTML、RSS/Atom、JSON API | 简单站点 / 媒体 RSS | Fetcher |
| **Firecrawl** | 长尾站点、PDF、LLM 结构化、深度 crawl | 政策原文 PDF、无 CSS 规律页、Tier 3/4 媒体 | Fetcher + Extractor |
| **Crawlee** | 队列驱动的大规模 crawl | 全站 news / 全站 spec 目录 | Fetcher |
| **pdfplumber + camelot** | PDF 表格解析 | 官方季度 price list / 法规原文表 | Extractor |

**原则**：能用 Scrapling 就用 Scrapling；Scrapling 搞不定的再升 Playwright；还搞不定的再上 Firecrawl。Firecrawl 是贵的兜底，不是默认。

## 3. 三层架构

### 3.1 顶层：`ScrapeJob` 契约

所有抓取动作都被抽象成 `ScrapeJob`：

```python
# 07_ScrapingToolkit/jato_scraper/core/job.py
from pydantic import BaseModel, Field
from typing import Literal

JobKind = Literal['msrp', 'news', 'policy', 'incentive', 'spec']
FetcherKind = Literal['httpx', 'playwright', 'firecrawl_scrape', 'firecrawl_crawl', 'crawlee']
ExtractorKind = Literal['css_rules', 'schema_org', 'pdf_table', 'llm_extract', 'rss', 'json_path']

class FreshnessPolicy(BaseModel):
    max_age_hours: int        # 超过这个时间要重抓
    skip_if_fresh: bool = True

class ScrapeJob(BaseModel):
    job_id: str               # 'msrp:se:volvo:xc60' 一类的 slug
    kind: JobKind
    url: str
    fetcher: FetcherKind
    extractor: ExtractorKind
    extractor_config: dict    # css rules / schema / pdf options / llm schema
    schema_ref: str           # 'RawMsrpObservation' | 'RawNewsArticle' | ...
    freshness: FreshnessPolicy
    priority: int = 50        # 0-100
    allow_domains: list[str] = Field(default_factory=list)  # 抓取 link 时的域名白名单
    metadata: dict = Field(default_factory=dict)            # country, brand, source_id, ...
```

Job 的定义可以来自：
1. **YAML 源**（msrp 的 `sources/*.yaml`，news 的 `news_sources/*.yaml`，policy 的 `policy_sources/*.yaml`）。
2. **PG `scrape_job_queue`**（动态生成的 job，例如 MSRP 新 trim 触发 spec 抓取）。

### 3.2 中层：Fetcher → Extractor → Normalizer 三步

```
┌────────────────────────────────┐
│ Fetcher layer                  │
│  httpx / playwright /          │
│  firecrawl / crawlee           │
│                                │
│  → RawDocument                 │
│    - content_type: html|pdf|json│
│    - body (bytes or text)      │
│    - meta: final_url, status,  │
│            fetched_at, headers │
└──────────────┬─────────────────┘
               ▼
┌────────────────────────────────┐
│ Extractor layer                │
│  css_rules | schema_org |      │
│  pdf_table | llm_extract | rss │
│                                │
│  → StructuredObservation       │
│    (Pydantic per JobKind)      │
└──────────────┬─────────────────┘
               ▼
┌────────────────────────────────┐
│ Normalizer + Sink layer        │
│  - currency converter (shared) │
│  - unit / lang normalizer      │
│  - dedup by canonical key      │
│  - confidence scoring          │
│                                │
│  → PG sink                     │
│    msrp_observation |          │
│    news_article |              │
│    policy_snippet |            │
│    incentive_program |         │
│    spec_feature_observation    │
└────────────────────────────────┘
```

### 3.3 关键 Pydantic 输出模型（每个 kind 一个）

```python
class RawMsrpObservation(BaseModel):     # 已存在，保留
    ...

class RawNewsArticle(BaseModel):
    url: str
    title: str
    summary: str | None
    body_text: str | None
    published_at: datetime | None
    country_code: str
    source_domain: str
    lang: str | None
    topics: list[str] = []                # ['carbon_tax','bev_subsidy']

class PolicySnippet(BaseModel):
    country_code: str
    topic: str                            # 'malus_bonus' | 'co2_tax' | ...
    effective_from: date | None
    effective_to: date | None
    title: str
    body_markdown: str                    # 归一后的 markdown
    source_url: str
    source_kind: str                      # 'government' | 'eur_lex' | 'media'
    citation_tier: int                    # 1=官方公报，2=部委，3=媒体解读

class IncentiveProgram(BaseModel):
    country_code: str
    program_name: str
    applies_to: list[str]                 # ['BEV','PHEV']
    amount_eur_max: float | None
    amount_local: float | None
    currency: str | None
    effective_from: date | None
    effective_to: date | None
    conditions: str
    source_url: str

class SpecFeatureObservation(BaseModel):
    trim_slug: str
    country_code: str
    feature_items: list[dict]             # [{feature_key, kind, option_price}, ...]
    source_url: str
```

## 4. PG schema 扩展（Alembic 0012）

```sql
-- 抓取任务队列（轻量，不是全 Airflow）
CREATE TABLE scrape_job_queue (
    job_id        VARCHAR(96) PRIMARY KEY,        -- 'policy:se:malus_2026'
    kind          VARCHAR(16) NOT NULL,
    payload       JSONB NOT NULL,                 -- ScrapeJob 序列化
    priority      SMALLINT NOT NULL DEFAULT 50,
    status        VARCHAR(16) NOT NULL DEFAULT 'pending',  -- pending|running|ok|failed|skipped
    enqueued_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at    TIMESTAMPTZ,
    finished_at   TIMESTAMPTZ,
    last_error    TEXT,
    retry_count   SMALLINT NOT NULL DEFAULT 0
);
CREATE INDEX ix_sjq_status_priority ON scrape_job_queue (status, priority DESC);

-- 抓取运行日志（可观察性）
CREATE TABLE scrape_run_log (
    run_id        BIGSERIAL PRIMARY KEY,
    job_id        VARCHAR(96) NOT NULL,
    kind          VARCHAR(16) NOT NULL,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ,
    status        VARCHAR(16) NOT NULL,
    fetcher       VARCHAR(32),
    extractor     VARCHAR(32),
    http_status   INT,
    bytes_fetched INT,
    rows_out      INT,
    error_class   VARCHAR(64),
    error_detail  TEXT
);
CREATE INDEX ix_srl_job_time ON scrape_run_log (job_id, started_at DESC);

-- policy / incentive / news 各自的 observation 表（news 在 Copilot 文档定义过）
CREATE TABLE policy_snippet (
    snippet_id    BIGSERIAL PRIMARY KEY,
    country_code  VARCHAR(4) NOT NULL,
    topic         VARCHAR(32) NOT NULL,
    effective_from DATE, effective_to DATE,
    title         TEXT NOT NULL,
    body_markdown TEXT NOT NULL,
    source_url    TEXT NOT NULL UNIQUE,
    source_kind   VARCHAR(32) NOT NULL,
    citation_tier SMALLINT NOT NULL DEFAULT 3,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_policy_country_topic ON policy_snippet (country_code, topic);

CREATE TABLE incentive_program (
    program_id     BIGSERIAL PRIMARY KEY,
    country_code   VARCHAR(4) NOT NULL,
    program_name   VARCHAR(256) NOT NULL,
    applies_to     VARCHAR(16)[] NOT NULL,
    amount_eur_max NUMERIC(12, 2),
    amount_local   NUMERIC(12, 2),
    currency       VARCHAR(8),
    effective_from DATE, effective_to DATE,
    conditions     TEXT,
    source_url     TEXT NOT NULL,
    fetched_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (country_code, program_name, effective_from)
);
```

## 5. 调度（systemd timer，不升级到 Airflow）

在 `04_DevOps/SINGLE_NODE_SCHEDULING_2026-04-17.md`（待建，ARCHITECTURE_REVIEW P0-5）里统一排布：

| Timer | Kind | 节奏 | 时间窗 | 并发 |
|-------|------|------|--------|------|
| `jato-msrp-sync` | msrp | 每日 | 01:00–03:00 | 2 |
| `jato-news-sync` | news | 每 6 小时 | `0 */6 * * *` | 1 |
| `jato-policy-sync` | policy | 每周一 | 04:00 | 1 |
| `jato-incentive-sync` | incentive | 每周三 | 04:00 | 1 |
| `jato-spec-sync` | spec | 触发式 | MSRP 发现新 trim 后入队 | 1 |
| `jato-refresh-job` | ETL | 每月 | 指定日 05:00 | 1 |

**并发协调**：Fetcher 层共享一个 `concurrency_limiter`（以 domain 为 key），同一域名并发 ≤ 2，全局并发 ≤ 6。Playwright 浏览器池全局 ≤ 1 个（内存预算 ~2 GB）。

## 6. 代码目录重构

```
07_ScrapingToolkit/
├── jato_scraper/
│   ├── core/
│   │   ├── job.py              # ScrapeJob / FreshnessPolicy
│   │   ├── raw_document.py     # RawDocument
│   │   ├── concurrency.py      # 域名级限流
│   │   └── observability.py    # run log 写入
│   ├── fetchers/
│   │   ├── httpx_fetcher.py
│   │   ├── playwright_fetcher.py
│   │   ├── firecrawl_fetcher.py
│   │   └── crawlee_fetcher.py
│   ├── extractors/
│   │   ├── css_rules.py        # Scrapling 封装
│   │   ├── schema_org.py       # 已有
│   │   ├── pdf_table.py        # 新：pdfplumber + camelot
│   │   ├── llm_extract.py      # 新：Firecrawl extract / local LLM
│   │   └── rss.py              # 已有
│   ├── schemas/                # Pydantic per kind
│   │   ├── msrp.py
│   │   ├── news.py
│   │   ├── policy.py
│   │   ├── incentive.py
│   │   └── spec.py
│   ├── sinks/
│   │   ├── pg_sink.py          # 统一 PG upsert（每 kind 一个 upsert）
│   │   └── chroma_sink.py      # news / policy 同步到 Chroma
│   ├── pipelines/
│   │   ├── msrp_pipeline.py    # 现有流程包装
│   │   ├── news_pipeline.py
│   │   ├── policy_pipeline.py
│   │   ├── incentive_pipeline.py
│   │   └── spec_pipeline.py
│   ├── currency_converter.py   # 已有，保留
│   └── runner.py               # 统一 entry point：scrape --kind=<k> --jobs=<path>
├── sources/                    # MSRP YAML
├── news_sources/               # News YAML
├── policy_sources/             # 新：政策源 YAML
├── incentive_sources/          # 新：补贴源 YAML
├── spec_sources/               # 新：spec 源 YAML
└── run.py                      # CLI 入口
```

CLI 统一入口：
```bash
python 07_ScrapingToolkit/run.py scrape --kind msrp --source sources/volvo-xc60-se.yaml
python 07_ScrapingToolkit/run.py scrape --kind news --batch batch_a
python 07_ScrapingToolkit/run.py scrape --kind policy --country SE --topics malus_bonus,co2_tax
```

## 7. 与平台的边界

- **`06_AppPlatform` 禁止 import** `07_ScrapingToolkit/jato_scraper/{fetchers,extractors,sinks,pipelines}`。
- **允许共享**：`currency_converter.py` 作为纯函数，迁到 `07_ScrapingToolkit/jato_scraper/shared/currency.py`；平台通过 `pip install -e 07_ScrapingToolkit` 安装 wheel 后只 import `jato_scraper.shared.*`。
- **数据交换**：toolkit → PG（通过 sink）→ 平台从 PG 读；toolkit → Chroma（通过 chroma_sink）→ 平台 Copilot 从 Chroma 读。

## 8. 错误处理与可观察性

- 每个 job 写 `scrape_run_log` 一条记录。
- `error_class` 标准化：`NETWORK` / `PARSING` / `SCHEMA` / `CAPTCHA` / `RATE_LIMIT` / `EMPTY`。
- 3 次连续 failure 的 job 自动降低 priority，第 7 次 failure 后 `status='skipped'`，进人审清单。
- 每日 04:55 生成 `04_Processed_data/ops/scrape_digest_YYYYMMDD.md`（成功数 / 失败数 / 分布），人工早查。

## 9. 测试点

- 每个 Fetcher 都有 fixture 测试（httpx 用 respx，playwright 用 page.route，firecrawl mock）。
- 每个 Extractor 都有 unit test（喂本地 HTML fixture → 期望 Pydantic 输出）。
- 端到端 smoke：`make smoke-scrape-msrp` / `make smoke-scrape-news`。
- Schema 契约测试：每个 kind 的 Pydantic 模型必须与 PG DDL 对齐（`tests/toolkit/test_schema_parity.py`）。

## 10. 迁移步骤（从现状到统一平台）

| W | 动作 |
|---|------|
| W1 | `ScrapeJob` / `RawDocument` / `RunLog` 抽象落地；MSRP pipeline 包装一层不改行为 |
| W1 | news_pipeline + Alembic 0011 news_article 落库 + sink=pg（同 Copilot 文档） |
| W2 | Alembic 0012 scrape_job_queue + scrape_run_log + policy_snippet + incentive_program |
| W2 | policy_pipeline MVP（只接 SE/NO 两国碳税 / Malus，手工 YAML） |
| W3 | Firecrawl fetcher + llm_extract extractor（低置信 MSRP 兜底） |
| W3 | pdf_table extractor（BMW/Mercedes 季度 PDF） |
| W4 | Crawlee 集成（news 全站 crawl，解决 RSS 时效性差的站） |
| W5 | incentive_pipeline + spec_pipeline |
| W5 | `SINGLE_NODE_SCHEDULING_2026-04-17.md` 定稿，所有 systemd timer 落地 |

## 11. 验收

- 5 个 kind 全部能跑 smoke。
- MSRP dry-run 通过率 ≥ 70%（Firecrawl 兜底后）。
- news 最新时间 T-6h 内有 ≥ 70% 国家。
- policy 覆盖 7 国（Batch A + B）的 Malus/Bonus + CO2 tax + BEV incentive 三个 topic。
- scrape_run_log 24h 可查，错误分类正确率 ≥ 95%。

## 12. 明确不做

- **不引入**任务队列中间件（Redis Streams / RabbitMQ）。`scrape_job_queue` 就在 PG 里，`SELECT ... FOR UPDATE SKIP LOCKED` 足够。
- **不做分布式 crawl**。单机 12 Mbps + 4 CPU 的上限远在"分布式"之前。
- **不做自动 CAPTCHA 破解**。遇到验证码的源标 `skipped`，走人工。
- **不做翻译**。news/policy 保留原文语言，LLM 侧做 on-the-fly 翻译（Copilot 回复时）。

## 13. 附录：Firecrawl 选型理由

- 对长尾 / 无规律站点，Firecrawl 的 `extract` API 能用 LLM 按 JSON schema 输出，省掉 CSS 调试工时。
- 月度成本估算：`low_confidence_rate × monthly_job_count × $0.003`。当前 44% 通过率 → 约 $0.5–1/月（完全可接受）。
- 可自托管（firecrawl-self-host），如未来隐私要求升级再切换。
