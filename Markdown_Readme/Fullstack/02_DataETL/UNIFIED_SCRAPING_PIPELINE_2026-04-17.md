# 统一抓取流水线（Unified Scraping Pipeline）2026-04-17

状态：Active（当前实现 + 后续扩展位并存）

关联：
- [PRODUCT_DEEPDIVE_2026-04-17.md](../PRODUCT_DEEPDIVE_2026-04-17.md) § Q6
- `07_ScrapingToolkit/README.md`
- [MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md](../MSRP/03_Implementation/MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md)
- [COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md](../01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md)

---

## 0. 2026-04-18 实现快照

当前 repo 已经有的部分：

- MSRP 抓取主链路（Scrapling / Playwright / httpx / JSON / currency converter）
- news RSS/Atom foundation（`07_ScrapingToolkit/news_sources/*.yaml` + `news_runner`）
- 本地 `/data-management` 页面上的 **Airflow start / stop / open-UI controls**，用于单机环境下的可视化 orchestration 辅助

当前**还没统一抽象好**的部分：

- 通用 `ScrapeJob` / `scrape_job_queue`
- policy / incentive / spec 的稳定 sink
- Firecrawl / Crawlee 的正式接入

因此本文要读成：**前半是当前能力边界，后半是统一平台的目标形态**。

## 0.1 无人工 review 的自动质检策略

对 `news` / `VOC` 不走人工逐条 review，而是走 **auto review + confidence gate**：

1. **Deterministic checks**
   - URL / host 合法性
   - 标题 / 摘要 / 正文长度阈值
   - 发布时间有效性 / 新鲜度
   - 页面类型是否符合 source 预期（thread / article / landing fallback）
   - country/source metadata 是否完整

2. **Source tier**
   - `official`
   - `association`
   - `trusted_media`
   - `local_media`
   - `aggregator`

3. **Cross-source / cross-document consistency**
   - `news`：同一 country refresh 内，跨 publisher 的标题/摘要 token overlap 作为 corroboration 信号
   - `VOC`：按文档质量和 source-level document coverage 做 publish-ready 统计，不因单个 source error 拖垮整批

4. **Evidence-grounded output**
   - 每个 observation / document 都带 `autoReview`
   - 至少包含：`score`、`publishTier`、`publishDecision`、`signals`、`warnings`

5. **Publish tiers**
   - `high` → `auto_publish`
   - `medium` → `candidate_publish`
   - `low` → `holdout` / `hold_raw`

原则不是“让 LLM 单点审批”，而是：

> **规则校验 + source tier + corroboration + confidence gate**

这样 `assistant` / `deck` / `digest` 默认只消费通过自动闸门的数据，raw layer 则保留全量证据，便于后续回放和调试。

## 1. 目标

把现在 `07_ScrapingToolkit` 里以 MSRP 为中心的 pipeline 扩展为**多 domain 统一抓取平台**，覆盖：
- `msrp` — 官方 / 经销商价格（已在跑）
- `news` — 汽车 / 财经 / 政策新闻（Phase 3 进行中）
- `policy` — 碳税、排放、Malus/Bonus、路权、城市限制
- `incentive` — BEV / PHEV 补贴、企业税优惠、充电补贴
- `spec` — 官方 configuration 表（trim feature catalog 的源头）

同时不打破现有架构的边界约束：
- 仍然定位为**离线制品生产者**（见 ARCHITECTURE_REVIEW P1-1）。
- 默认仍可单机直接运行；**Airflow 只作为可选的本地 orchestration / 可视化层**，不改变核心抓取代码结构，也不是线上请求依赖。
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

## 5. 调度（默认 systemd timer；Airflow 仅本地可选）

默认调度主链路仍以 systemd timer 为准；Airflow 目前只作为 **local-only orchestration / 可视化控制层** 出现在 `/data-management`，不替代核心抓取代码与发布流程。

当前腾讯云单机调度默认统一排布如下：

| Timer | Kind | 节奏 | 时间窗 | 线上目标形态 |
|-------|------|------|--------|--------------|
| `jato-country-news-sync.timer` | news | 每日 | 23:15 | **DB-first**：写 PostgreSQL snapshot，不要求保留 `04_Processed_data/news` |
| `jato-voc-forum-sync.timer` | voc | 每日 | 01:45 | **artifact-first**：保留 `04_Processed_data/voc` raw / enriched / deck，并同步 raw 到 PG staging |
| `jato-msrp-dryrun.timer` | msrp | 每日 | 03:30 | **backend/API-first**：低并发 dry-run |
| `jato-msrp-ingest.timer` | msrp | 每周六 | 05:30 | **backend/API-first**：低并发 ingest |

这组 timer 故意全部落在 `23:00-07:00` 窗口，避免和白天的 dashboard / copilot 交互流量争资源。

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

## 14. 2026-04-18 补充：Scrapling + OpenCLI + Firecrawl 的协同策略

为了把 MSRP、news、finance、碳税法规这些抓取都收进同一套平台，推荐把抓取器能力再分一层：

| 能力 | 优先级 | 适用场景 | 产物 |
|---|---|---|---|
| **Scrapling / httpx** | 默认 | 结构稳定的官网、RSS、JSON API、schema.org 页面 | 低成本 canonical observation |
| **Playwright / OpenCLI** | 次级 | 需要点击流程、cookie、配置器、登录态、人机交互录制的站点 | 可复用交互脚本 + page snapshot |
| **Firecrawl** | 兜底 | PDF、深层目录、长尾媒体、规则性很差的政策/新闻页 | 结构化 JSON / markdown 抽取 |

这里的 **OpenCLI** 更适合被定位成"交互编排层"：当站点必须点开 trim、切换 engine、展开 finance 条款时，用 OpenCLI/浏览器脚本把动作固化成 step sequence；真正的结构化抽取仍回到 extractor schema，而不是把 CLI 输出直接当最终数据。

> 注意：**OpenCLI 目前还不在 repo 内**。这里记录的是推荐扩展位；当前已存在的是 Scrapling、Playwright、httpx/JSON、news runner，以及文档里规划中的 Firecrawl/Crawlee。

## 15. 推荐的六阶段总流程

```text
Source discovery
  -> Probe & classify
  -> Fetch
  -> Extract
  -> Normalize & reconcile
  -> Publish & sync
```

### 15.1 Source discovery
- MSRP：brand family source、dealer page、官方 PDF
- News：RSS / 媒体站 / 行业协会
- Policy / finance / tax：政府公报、税务局、交通部、EU 法规页面

### 15.2 Probe & classify
- 先判断站点属于 `static_html` / `dynamic_flow` / `pdf_table` / `long_tail_unstructured`
- 产出推荐 fetcher：Scrapling、OpenCLI/Playwright、Firecrawl、Crawlee

### 15.3 Fetch
- 原始 HTML / PDF / JSON 一律先落硬盘缓存：`04_Processed_data/raw_documents/{kind}/{country}/{date}/...`
- 每次抓取都保留 `final_url / fetched_at / http_status / content_hash`

### 15.4 Extract
- MSRP / spec / policy / news 各自有独立 schema
- LLM 只负责把"难抽的网页"转成 schema，不负责最终 truth 决策

### 15.5 Normalize & reconcile
- 货币、日期、生效期、country code、brand/model/trim alias 统一在这里收口
- MSRP 进入 `observation -> reconciliation -> published price`
- policy/news 进入 `observation -> dedup/topic tagging -> country digest`

### 15.6 Publish & sync
- PG：transactional truth（observation、published、review、run log）
- Parquet：分析宽表 / 预聚合产物
- Vector store：给 Country Copilot 做 local retrieval
- Chart deck snapshots：给前端/国家助手做 deterministic answer context

## 16. 为什么现在 news 只能抓到旧时间

当前痛点本质上不是"源不够多"，而是**抓到的内容还没有稳定 sink 成国家助手可检索的本地 truth**。要让 2026-04 的新闻和法规真的能回答出来，至少要补齐：

1. `news_article` / `policy_snippet` 落 PG，并带 `published_at` / `fetched_at`
2. 定时增量同步到 vector store
3. 国家助手只在问到 freshness / policy / latest 时再触发 live fetch
4. live fetch 结果必须回写，不然下一轮还会继续"只知道旧快照"

## 17. 面向国家助手的落地要求

抓取平台的产物最终不是为了"抓到页面"，而是为了让 Country Copilot 能精准回答。因此要求：

1. **每种抓取都必须能落到可查询实体**：model、trim、feature、policy topic、news topic。
2. **每条记录都要有 source tier 和 freshness**：否则回答无法解释"为什么信这个价格/这条法规"。
3. **图表和回答共用同一套 sink**：不要出现聊天回答来自 live 页面、图表来自旧 snapshot 的口径撕裂。
