# AI News & VOC Intelligence Pipeline Roadmap（Crawler 主干 + LLM 增强）

状态：Draft（建议新增到 `Markdown_Readme/Fullstack/02_DataETL/`）  
建议文件名：`AI_NEWS_VOC_INTELLIGENCE_PIPELINE_2026-05-09.md`  
适用范围：News / VOC / Policy / Customer Insights / Country Copilot  
关联模块：`07_ScrapingToolkit`、`06_AppPlatform/backend`、`CustomerInsightsPage`、`Data Management`、`Country Copilot`  
核心结论：**crawler 负责稳定采集，大模型负责理解、结构化、翻译、证据摘要与报告生成。**

---

## 0. TL;DR

当前 JATO Analysis System 已经具备比较完整的：

```text
React + FastAPI + PostgreSQL + Parquet + Scraping Toolkit + Country Copilot
```

同时 `07_ScrapingToolkit` 已经落地：

```text
news_runner / run_news.py
voc_sources
jato-voc-fetch
jato-voc-enrich
raw -> enriched -> deck artifact
PostgreSQL staging
Customer Insights forum_live
Data Management VOC 观察台
```

因此，下一步不应该推翻 crawler，也不应该让大模型直接替代采集层。

推荐新增的是一条 **AI Intelligence Pipeline**：

```text
Crawler / API / RSS
  -> Raw Artifact
  -> Main Text / Content Units
  -> LLM Enrichment
  -> Evidence-grounded AI Observations
  -> PostgreSQL lightweight index
  -> Customer Insights / Country Copilot / Weekly Digest
```

一句话：

> **crawler 决定系统能不能长期稳定跑；LLM 决定系统能不能把 raw text 变成业务洞察。**

---

## 1. 为什么不是“crawler vs 大模型”二选一

### 1.1 crawler 更适合做主采集层

crawler / API / RSS 更适合长期做：

- 固定 source 的定时抓取
- RSS / Atom 新闻流
- 官方网站 / 媒体网站 / 论坛页面
- YouTube Data API 等合规 API
- 原始网页 / JSON / HTML artifact 保存
- 失败重试、限流、日志、调度
- source-level coverage tracking
- dashboard 趋势复算

原因是 crawler 具备四个关键优势：

```text
可复现
可调度
可审计
低成本
```

如果用大模型直接“帮我找并分析北欧 VOC”，短期很方便，但长期会出现：

- 每次返回 source 不稳定
- 难以判断漏抓了哪些论坛 / 评论区
- 结果难以复算
- 成本不可控
- 与 Data Management / PostgreSQL / dashboard 的数据资产链路断开

### 1.2 大模型更适合做理解层

LLM / AI API 更适合做：

- 多语言翻译
- brand / model / trim / powertrain entity normalization
- pain point 多标签分类
- aspect-level sentiment
- ownership stage 判断
- purchase impact 判断
- 原文 evidence sentence 提取
- 中文 / 英文摘要
- customer insight deck 生成
- weekly market digest
- Country Copilot grounded answer

LLM 的价值不在“抓网页”，而在：

```text
把 raw text 变成结构化 observation
把 observation 变成 evidence-grounded insight
把 insight 变成 PM / management 能读懂的结论
```

### 1.3 最终判断

| 任务 | crawler / API | LLM / AI API | 推荐主导 |
|---|---:|---:|---|
| 固定 source 定时抓取 | 强 | 弱 | crawler |
| RSS / News feed | 强 | 弱 | crawler |
| YouTube comments API | 强 | 中 | crawler / API |
| 论坛正文抓取 | 强 | 中 | crawler |
| 新 source discovery | 中 | 强 | Exa / AI search |
| boilerplate removal | 中 | 强 | crawler + AI extractor |
| 多语言翻译 | 弱 | 强 | LLM |
| 车型 / 品牌 / 动力归一 | 中 | 强 | LLM + alias dictionary |
| sentiment / pain point | 弱 | 强 | LLM |
| evidence card | 中 | 强 | LLM |
| 周报 / 国家报告 | 弱 | 强 | Perplexity / report model |
| dashboard 趋势 | 强 | 中 | crawler + DB |
| 证据回放 | 强 | 弱 | artifact + URL |

---

## 2. 目标功能定义

新增功能名称建议：

```text
AI News & VOC Intelligence Pipeline
```

中文名称：

```text
新闻与客户原声 AI 洞察增强链路
```

### 2.1 功能目标

在现有 `news` 和 `VOC` 抓取链路上，新增 AI enrichment 层，实现：

1. **VOC 从 raw text 升级为 observation**
   - comment / sentence / content unit 级别
   - aspect-level sentiment
   - pain point / product signal / scenario tags
   - model / brand / powertrain mentions
   - evidence quote + translation

2. **News 从 article 升级为 market event**
   - event type
   - related country
   - related brand / model
   - policy / incentive / regulation / launch / recall / pricing event
   - market impact
   - confidence / source tier

3. **Country Copilot 能回答动态问题**
   - “瑞典 Tucson HEV 用户主要抱怨什么？”
   - “挪威最近 BEV 充电相关负面新闻有哪些？”
   - “芬兰 PHEV 用户最关心价格还是冬季能耗？”
   - “丹麦 SUV 用户讨论最多的家庭用车场景是什么？”

4. **Customer Insights 页面增加 AI Signals**
   - Top pain points
   - Model mention ranking
   - Powertrain mention ranking
   - Aspect sentiment matrix
   - Evidence cards
   - Original language / CN summary / EN summary

5. **Data Management 页面可监控 AI 成本与质量**
   - AI run status
   - processed units
   - token usage
   - estimated cost
   - confidence distribution
   - failed units
   - replay path

---

## 3. 两阶段范围

## 3.1 Stage 1 — MVP：Nordic Four

### 国家范围

```text
SE = Sweden
FI = Finland
NO = Norway
DK = Denmark
```

### MVP 定位

MVP 阶段不追求“15 国全覆盖”，而是先把北欧四国的 AI 增强链路做稳。

核心目标：

```text
source 稳定
contentUnits 稳定
AI observation 稳定
evidence 可回放
成本可控
前端可展示
Copilot 可调用
```

### MVP 数据源范围

优先接入：

- 现有 `voc_sources` 北欧国家源
- 现有 `news_sources` 北欧国家源
- Google News RSS / local media RSS
- 本地汽车媒体
- 公开论坛 / 车主社区
- YouTube comments API（后续可选）
- Exa source discovery（只做候选 source，不做主采集）

谨慎或暂不做：

- Facebook / Instagram 大规模评论抓取
- Reddit 大规模抓取
- 登录墙 / 付费墙内容
- 自动 CAPTCHA 破解
- 无授权社媒批量爬取

### MVP 成果

MVP 结束时应至少产出：

```text
04_Processed_data/voc/SE/ai_enriched/
04_Processed_data/voc/FI/ai_enriched/
04_Processed_data/voc/NO/ai_enriched/
04_Processed_data/voc/DK/ai_enriched/

04_Processed_data/news/ai_enriched/

PostgreSQL:
ops.ai_enrichment_runs
ops.ai_observations
```

前端至少展示：

- `CustomerInsightsPage`：AI Signals tab
- `/data-management`：AI Enrichment run status card
- `Country Copilot`：可读取 AI VOC observations

---

## 3.2 Stage 2 — Stage2 Countries 扩展

### 国家范围

Stage 2 国家以仓库后续 `stage2` 配置文件为准，建议新增：

```text
07_ScrapingToolkit/voc_sources/stage2.yaml
07_ScrapingToolkit/news_sources/stage2.yaml
```

如沿用当前 15 国扩展目标，建议范围为：

```text
SE / FI / NO / DK
HU / HR / AT / CZ
DE / FR / ES / IT / NL / BE / PL
```

说明：

- Stage 1 的 SE / FI / NO / DK 是基础国家，不重复开发，只扩容 source 和调度。
- Stage 2 新增国家主要挑战是 language taxonomy、source registry、失败重试和成本监控。
- Stage 2 不应一开始就全部开启高频 AI enrichment，应先按 source coverage 和市场优先级分批打开。

### Stage 2 成果

Stage 2 完成后，应实现：

- 15 国 news / VOC AI enrichment 批处理
- 每国 source coverage dashboard
- 每国 AI observation coverage
- Country Copilot 支持跨国家 VOC / News 对比
- weekly digest 自动生成
- AI cost dashboard
- 每月人工 QA 抽检机制

---

## 4. 推荐架构

## 4.1 总体链路

```text
┌─────────────────────────────────────────────────────────────┐
│ Source Registry                                             │
│ voc_sources/*.yaml / news_sources/*.yaml / source discovery │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Crawler / API Layer                                         │
│ jato-voc-fetch / run_news.py / RSS / YouTube API / httpx    │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Raw Artifact Layer                                          │
│ 04_Processed_data/voc/<country>/raw/*.json                  │
│ 04_Processed_data/news/raw/*.json                           │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Pre-AI Cleaning Layer                                       │
│ Trafilatura / lxml fallback / contentUnits / sentence split │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ AI Enrichment Layer                                         │
│ language / translation / entities / sentiment / evidence    │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Confidence Gate                                             │
│ deterministic checks + source tier + evidence grounding     │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ Storage Layer                                               │
│ JSON artifact + PostgreSQL lightweight index                │
└──────────────────────┬──────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────────┐
│ App Layer                                                   │
│ FastAPI / Customer Insights / Data Management / Copilot     │
└─────────────────────────────────────────────────────────────┘
```

---

## 4.2 模块分工

| 模块 | 职责 | 是否新增 |
|---|---|---|
| `jato-voc-fetch` | 公开 VOC 页面抓取 | 已有，增强 |
| `jato-voc-enrich` | heuristic enrichment | 已有，保留 |
| `jato-voc-ai-enrich` | LLM enrichment | 新增 |
| `jato-news-ai-enrich` | news event AI extraction | 新增 |
| `source_discovery_job` | Exa / search-based source candidate discovery | 新增 |
| `ai_cost_tracker` | token / request / provider cost 记录 | 新增 |
| `ai_quality_gate` | confidence / evidence / source tier 检查 | 新增 |
| `ops.ai_enrichment_runs` | AI run metadata | 新增 |
| `ops.ai_observations` | AI observation searchable index | 新增 |
| `CustomerInsightsPage AI tab` | 前端展示 | 新增 |
| `Country Copilot voc_query tool` | Copilot 调用 | 新增 |

---

## 5. Provider 选型与职责

### 5.1 Exa

职责：

```text
source discovery
long-tail page discovery
high-quality forum / media / review page candidate finding
```

不做：

```text
不替代 jato-voc-fetch
不作为 daily full crawl 主通道
不直接决定最终 insight
```

推荐使用方式：

```text
weekly source discovery
  -> candidate source list
  -> deterministic source validation
  -> write candidate_sources.yaml
  -> promote selected source to voc_sources/stage1.yaml or stage2.yaml
```

### 5.2 DeepSeek / Gemini / OpenAI-compatible LLM

职责：

```text
language detection
translation
brand/model/powertrain mentions
pain point extraction
aspect sentiment
purchase impact
evidence sentence extraction
structured JSON output
```

推荐默认：

```text
MVP 默认：DeepSeek V4 Flash / deepseek-chat compatible model
高价值报告或复杂归因：DeepSeek Pro / GPT / Gemini Pro
```

原则：

```text
低成本模型处理 bulk units
强模型只处理 low-confidence 或 report-level reasoning
```

### 5.3 Perplexity Sonar Deep Research

职责：

```text
weekly / monthly report
external validation
citation-backed country memo
management-ready report generation
```

不做：

```text
不做 daily raw collection
不做每条 comment 级别 enrichment
不进入主数据采集链路
```

### 5.4 NewsAPI.ai / Newsdata.io

职责：

```text
Stage 2 新闻源稳定化
历史新闻回溯
实体 / topic / sentiment 补充
跨国家 news coverage
```

MVP 阶段：

```text
不强制接入
优先继续用 RSS / Google News RSS / local source
```

Stage 2 阶段：

```text
如 RSS coverage 不足，再接 NewsAPI.ai
```

---

## 6. 新增目录建议

```text
07_ScrapingToolkit/
├── jato_scraper/
│   ├── ai/
│   │   ├── __init__.py
│   │   ├── cost_tracker.py
│   │   ├── quality_gate.py
│   │   ├── source_discovery.py
│   │   ├── providers/
│   │   │   ├── base.py
│   │   │   ├── deepseek_client.py
│   │   │   ├── gemini_client.py
│   │   │   ├── perplexity_client.py
│   │   │   └── exa_client.py
│   │   ├── prompts/
│   │   │   ├── voc_aspect_extraction.md
│   │   │   ├── voc_translation_and_entity_grounding.md
│   │   │   ├── news_event_extraction.md
│   │   │   └── evidence_card_summary.md
│   │   ├── schemas/
│   │   │   ├── voc_ai_observation.py
│   │   │   ├── news_ai_event.py
│   │   │   ├── ai_run_summary.py
│   │   │   └── source_candidate.py
│   │   └── runners/
│   │       ├── run_voc_ai_enrich.py
│   │       ├── run_news_ai_enrich.py
│   │       └── run_source_discovery.py
│   └── ...
├── voc_sources/
│   ├── stage1_nordic.yaml
│   ├── stage2.yaml
│   └── taxonomy/
│       ├── nordic_core.yaml
│       ├── stage2_core.yaml
│       ├── brand_aliases.yaml
│       ├── model_aliases.yaml
│       ├── powertrain_aliases.yaml
│       └── scenario_aliases.yaml
└── news_sources/
    ├── stage1_nordic.yaml
    └── stage2.yaml
```

---

## 7. Output Artifact 设计

### 7.1 VOC AI enriched artifact

路径：

```text
04_Processed_data/voc/<country>/ai_enriched/
├── ai_observations_<YYYYMMDD_HHMMSS>.json
├── ai_evidence_cards_<YYYYMMDD_HHMMSS>.json
├── ai_run_summary_<YYYYMMDD_HHMMSS>.json
└── latest.json
```

### 7.2 `ai_observations` JSON schema

```json
{
  "runId": "voc_ai_SE_20260509_010000",
  "countryCode": "SE",
  "sourceUrl": "https://example.com/thread",
  "sourceCode": "se_ev_forum_xxx",
  "sourceType": "forum",
  "sourceTier": "local_forum",
  "unitId": "se_ev_forum_xxx:doc001:unit003",
  "unitType": "comment",
  "languageDetected": "sv",
  "originalText": "...",
  "translatedTextZh": "...",
  "translatedTextEn": "...",
  "brandMentions": ["Toyota", "Kia"],
  "modelMentions": ["RAV4", "Sportage"],
  "powertrainMentions": ["HEV", "PHEV"],
  "scenarioTags": ["winter_driving", "family_use"],
  "painPoints": ["winter_range", "fuel_consumption"],
  "productSignals": ["awd", "charging", "reliability"],
  "aspectSentiments": [
    {
      "aspect": "winter_range",
      "sentiment": "negative",
      "severity": "medium",
      "evidenceSentence": "...",
      "confidence": 0.82
    }
  ],
  "ownershipStage": "owner",
  "purchaseImpact": "hesitation",
  "summaryZh": "用户主要抱怨冬季真实续航下降。",
  "summaryEn": "The user mainly complains about reduced real-world winter range.",
  "confidence": 0.81,
  "qualityDecision": "publish_ready",
  "warnings": []
}
```

---

## 8. PostgreSQL 设计

原则：

```text
artifact-first + PG lightweight index
```

也就是：

- 大文本继续留在 `04_Processed_data`
- PostgreSQL 只存可筛选、可审计、可回放的轻量字段
- 不把完整 raw HTML / long text 全部塞进关系库

### 8.1 Alembic migration 建议

建议新增 migration：

```text
06_AppPlatform/backend/alembic/versions/00xx_ai_news_voc_enrichment.py
```

### 8.2 新表：`ops.ai_enrichment_runs`

```sql
CREATE TABLE IF NOT EXISTS ops.ai_enrichment_runs (
    run_id              BIGSERIAL PRIMARY KEY,
    run_key             VARCHAR(128) NOT NULL UNIQUE,
    domain              VARCHAR(32) NOT NULL, -- voc | news | policy
    stage               VARCHAR(32) NOT NULL, -- stage1_nordic | stage2
    country_code        VARCHAR(4),
    provider            VARCHAR(32) NOT NULL,
    model_name          VARCHAR(128) NOT NULL,
    input_units         INT NOT NULL DEFAULT 0,
    output_units        INT NOT NULL DEFAULT 0,
    publish_ready_units INT NOT NULL DEFAULT 0,
    failed_units        INT NOT NULL DEFAULT 0,
    total_input_tokens  INT NOT NULL DEFAULT 0,
    total_output_tokens INT NOT NULL DEFAULT 0,
    estimated_cost_usd  NUMERIC(12, 4),
    status              VARCHAR(16) NOT NULL, -- running | ok | failed | partial
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    artifact_path       TEXT,
    error_summary       TEXT
);

CREATE INDEX IF NOT EXISTS ix_ai_runs_domain_stage
ON ops.ai_enrichment_runs (domain, stage, started_at DESC);

CREATE INDEX IF NOT EXISTS ix_ai_runs_country_time
ON ops.ai_enrichment_runs (country_code, started_at DESC);
```

### 8.3 新表：`ops.ai_observations`

```sql
CREATE TABLE IF NOT EXISTS ops.ai_observations (
    observation_id          BIGSERIAL PRIMARY KEY,
    run_id                  BIGINT REFERENCES ops.ai_enrichment_runs(run_id),
    domain                  VARCHAR(32) NOT NULL, -- voc | news
    country_code            VARCHAR(4) NOT NULL,
    source_code             VARCHAR(128),
    source_type             VARCHAR(64),
    source_tier             VARCHAR(64),
    source_url              TEXT NOT NULL,
    unit_id                 TEXT,
    unit_type               VARCHAR(64),
    language_detected       VARCHAR(16),
    brand_mentions          TEXT[],
    model_mentions          TEXT[],
    powertrain_mentions     TEXT[],
    scenario_tags           TEXT[],
    pain_points             TEXT[],
    product_signals         TEXT[],
    aspect                  VARCHAR(64),
    sentiment               VARCHAR(16),
    severity                VARCHAR(16),
    ownership_stage         VARCHAR(32),
    purchase_impact         VARCHAR(32),
    evidence_text           TEXT NOT NULL,
    evidence_translation_zh TEXT,
    evidence_translation_en TEXT,
    summary_zh              TEXT,
    summary_en              TEXT,
    confidence              NUMERIC(4, 3),
    quality_decision        VARCHAR(32), -- publish_ready | candidate | hold_raw
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_ai_obs_country_time
ON ops.ai_observations (country_code, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_ai_obs_sentiment
ON ops.ai_observations (sentiment);

CREATE INDEX IF NOT EXISTS ix_ai_obs_quality
ON ops.ai_observations (quality_decision);

CREATE INDEX IF NOT EXISTS ix_ai_obs_models_gin
ON ops.ai_observations USING GIN (model_mentions);

CREATE INDEX IF NOT EXISTS ix_ai_obs_pain_points_gin
ON ops.ai_observations USING GIN (pain_points);

CREATE INDEX IF NOT EXISTS ix_ai_obs_powertrain_gin
ON ops.ai_observations USING GIN (powertrain_mentions);
```

---

## 9. CLI 设计

### 9.1 VOC AI enrichment

```bash
cd 07_ScrapingToolkit

python -m jato_scraper.ai.runners.run_voc_ai_enrich \
  --countries SE FI NO DK \
  --stage stage1_nordic \
  --provider deepseek \
  --model deepseek-v4-flash \
  --max-units 500 \
  --output-root ../04_Processed_data/voc \
  --dry-run
```

正式运行：

```bash
python -m jato_scraper.ai.runners.run_voc_ai_enrich \
  --countries SE FI NO DK \
  --stage stage1_nordic \
  --provider deepseek \
  --model deepseek-v4-flash \
  --max-units 5000 \
  --sink pg
```

### 9.2 News AI enrichment

```bash
python -m jato_scraper.ai.runners.run_news_ai_enrich \
  --countries SE FI NO DK \
  --stage stage1_nordic \
  --provider deepseek \
  --model deepseek-v4-flash \
  --since-days 14 \
  --sink pg
```

### 9.3 Source discovery

```bash
python -m jato_scraper.ai.runners.run_source_discovery \
  --countries SE FI NO DK \
  --stage stage1_nordic \
  --provider exa \
  --topics winter_range charging hev_consumption dealer_service \
  --output 07_ScrapingToolkit/voc_sources/candidates/stage1_nordic_candidates.yaml
```

### 9.4 Stage 2

```bash
python -m jato_scraper.ai.runners.run_voc_ai_enrich \
  --stage stage2 \
  --countries HU HR AT CZ DE FR ES IT NL BE PL \
  --provider deepseek \
  --model deepseek-v4-flash \
  --max-units-per-country 3000 \
  --sink pg
```

---

## 10. Prompt 设计

### 10.1 `voc_aspect_extraction.md`

```text
You are an automotive VOC analyst.

Task:
Extract structured customer voice observations from the provided content unit.

Rules:
1. Only use evidence explicitly present in the input text.
2. Do not infer demographics such as age, income, gender, family size unless explicitly stated.
3. Preserve the original evidence sentence.
4. Translate the key evidence into Simplified Chinese and English.
5. Identify automotive entities:
   - brand
   - model
   - trim if available
   - powertrain: ICE / MHEV / HEV / PHEV / BEV
6. Extract aspect-level sentiment, not only document-level sentiment.
7. If evidence is weak, set qualityDecision = "hold_raw".
8. Output strict JSON only.

Allowed pain point labels:
- winter_range
- real_fuel_consumption
- charging_speed
- charging_infrastructure
- software_infotainment
- adas_safety
- reliability
- service_aftersales
- delivery_waiting
- price_tco
- interior_space
- family_practicality
- awd_winter_traction
- towing
- residual_value
- brand_trust
- unknown

Allowed sentiment:
- positive
- neutral
- negative
- mixed

Allowed severity:
- low
- medium
- high

Allowed purchaseImpact:
- would_buy
- hesitation
- reject
- retention_risk
- unknown
```

---

## 11. Confidence Gate 规则

AI 输出不能直接进入 deck，必须通过 quality gate。

### 11.1 Deterministic checks

```text
URL exists
source_code exists
country_code exists
originalText length >= 80 chars
evidenceText must be substring or near-substring of originalText
languageDetected exists
confidence exists
model_mentions must either match alias dictionary or stay empty
pain_points must be from allowed taxonomy
```

### 11.2 Publish decision

| Decision | 条件 | 用途 |
|---|---|---|
| `publish_ready` | confidence ≥ 0.70 且 evidence 可回放 | 进入 deck / Copilot |
| `candidate` | 0.55 ≤ confidence < 0.70 | 进入 Data Management，不进入默认 deck |
| `hold_raw` | confidence < 0.55 或 evidence 不可回放 | 只保留 artifact |
| `failed` | API / parsing / schema error | 进入 error log |

### 11.3 Evidence-grounded 原则

任何进入 Customer Insights 或 Copilot 的 AI observation 必须具备：

```text
source_url
source_code
country_code
unit_id
original evidence sentence
translated evidence
confidence
quality_decision
created_at
```

---

## 12. FastAPI API 设计

### 12.1 AI run status

```http
GET /v1/ai/enrichment/runs?domain=voc&stage=stage1_nordic&country=SE
```

返回：

```json
{
  "items": [
    {
      "runKey": "voc_ai_SE_20260509_010000",
      "domain": "voc",
      "stage": "stage1_nordic",
      "countryCode": "SE",
      "provider": "deepseek",
      "modelName": "deepseek-v4-flash",
      "inputUnits": 500,
      "publishReadyUnits": 381,
      "failedUnits": 12,
      "estimatedCostUsd": 0.08,
      "status": "ok",
      "startedAt": "2026-05-09T01:00:00Z",
      "finishedAt": "2026-05-09T01:12:00Z"
    }
  ]
}
```

### 12.2 AI observations search

```http
GET /v1/ai/observations?country=SE&domain=voc&model=RAV4&painPoint=winter_range
```

返回：

```json
{
  "countryCode": "SE",
  "domain": "voc",
  "total": 128,
  "items": [
    {
      "sourceUrl": "...",
      "sourceType": "forum",
      "modelMentions": ["RAV4"],
      "powertrainMentions": ["HEV"],
      "painPoints": ["winter_range"],
      "sentiment": "negative",
      "evidenceText": "...",
      "evidenceTranslationZh": "...",
      "confidence": 0.82,
      "qualityDecision": "publish_ready"
    }
  ]
}
```

### 12.3 Customer Insights AI summary

```http
GET /v1/customer-insights/forum-live/ai-summary?countries=SE,FI,NO,DK
```

返回：

```json
{
  "countries": ["SE", "FI", "NO", "DK"],
  "topPainPoints": [],
  "modelMentionRanking": [],
  "powertrainMentionRanking": [],
  "aspectSentimentMatrix": [],
  "evidenceCards": [],
  "coverage": {
    "observationCount": 1200,
    "publishReadyCount": 860,
    "sourceCount": 24,
    "lastRunAt": "2026-05-09T01:12:00Z"
  }
}
```

---

## 13. React 页面改动

### 13.1 CustomerInsightsPage

新增模式：

```text
Benchmark
Forum VOC Live
AI Signals
```

AI Signals 展示：

- Top Pain Points
- Model Mentions
- Powertrain Mentions
- Aspect Sentiment Matrix
- Evidence Cards
- Source Coverage
- Confidence Distribution

### 13.2 Data Management

新增卡片：

```text
AI News & VOC Enrichment
```

字段：

- Last Run
- Stage
- Countries
- Provider
- Model
- Processed Units
- Publish-ready Units
- Failed Units
- Estimated Cost
- Confidence Distribution
- Artifact Path
- Error Summary

### 13.3 Country Copilot

新增 tool：

```text
customer_voc_ai_search
country_news_ai_search
```

典型 query：

```text
瑞典 Tucson HEV 用户主要抱怨什么？
挪威 BEV 车主冬季续航讨论最多的问题是什么？
芬兰 HEV 用户更关注油耗还是价格？
丹麦家庭 SUV 用户最常提到哪些场景？
```

回答结构：

```text
直接回答
关键结论
证据卡片
来源与时间
置信度提示
```

---

## 14. 调度策略

### 14.1 MVP 调度

建议 systemd timer：

| Timer | 频率 | 时间 | 说明 |
|---|---:|---:|---|
| `jato-voc-fetch.timer` | daily | 01:00 | 抓北欧 VOC |
| `jato-voc-enrich.timer` | daily | 01:45 | heuristic enrichment |
| `jato-voc-ai-enrich.timer` | daily | 02:30 | AI enrichment |
| `jato-news-sync.timer` | daily | 23:15 | news RSS / feed |
| `jato-news-ai-enrich.timer` | daily | 00:15 | news AI event extraction |
| `jato-source-discovery.timer` | weekly | Sunday 04:30 | Exa source discovery |
| `jato-weekly-digest.timer` | weekly | Monday 06:00 | Perplexity / report generation |

### 14.2 Stage 2 调度

Stage 2 不建议所有国家每天全量 AI enrichment。

建议分层：

| 国家组 | 频率 | 说明 |
|---|---:|---|
| Nordic core | daily | SE/FI/NO/DK，保持高频 |
| High-priority Stage2 | every 2 days | DE/FR/ES/IT/AT/CZ 等 |
| Low-priority Stage2 | weekly | 根据 source coverage 决定 |
| Source discovery | weekly | 全部国家 |
| Weekly digest | weekly | 重点国家生成，非重点国家汇总 |

---

## 15. 成本模型

### 15.1 MVP 北欧四国估算

假设：

```text
4 countries
每国每月 300-800 analysis units
总计 1,200-3,200 units/month
每 unit 平均 input 800 tokens
每 unit 平均 output 200 tokens
```

粗略 token：

```text
input = 0.96M - 2.56M tokens
output = 0.24M - 0.64M tokens
```

如果用低成本 LLM，MVP AI enrichment 成本通常应控制在：

```text
$1 - $10 / month
```

Exa source discovery：

```text
4 countries × 10 queries × 4 weeks = 160 search requests/month
```

估算：

```text
$3 - $10 / month
```

Perplexity weekly digest：

```text
4 country reports/week × 4 weeks = 16 reports/month
```

估算：

```text
$8 - $30 / month
```

MVP 总预算建议：

```text
不接 NewsAPI.ai：$15 - $50 / month
接 NewsAPI.ai：$105 - $150 / month
```

### 15.2 Stage 2 估算

假设：

```text
15 countries
30,000 - 80,000 units/month
```

建议预算：

```text
LLM enrichment: $20 - $80 / month
Exa: $15 - $30 / month
NewsAPI.ai: $90+ / month
Perplexity digest: $30 - $90 / month
```

Stage 2 总预算建议：

```text
$155 - $300 / month 起步
```

注意：

- 真实成本取决于 unit 数、prompt 长度、模型选择、cache 命中率和失败重试次数。
- MVP 必须实现 `ai_cost_tracker`，不要等 Stage 2 才补成本记录。

---

## 16. 质量评估指标

### 16.1 MVP 验收指标

| 指标 | 目标 |
|---|---:|
| 国家覆盖 | SE / FI / NO / DK |
| 每国有效 source | ≥ 5 |
| 每国 monthly raw docs | ≥ 50 |
| 每国 monthly analysis units | ≥ 200 |
| publish-ready ratio | ≥ 60% |
| evidence 可回放率 | 100% |
| model mention precision | ≥ 85% |
| pain point 人工抽检准确率 | ≥ 80% |
| AI observation confidence ≥ 0.70 占比 | ≥ 70% |
| 月成本 | ≤ $50，不含服务器 |
| Copilot VOC query 可回答率 | ≥ 70% |

### 16.2 Stage 2 验收指标

| 指标 | 目标 |
|---|---:|
| 国家覆盖 | stage2 config 全覆盖 |
| 每国 source registry | ≥ 3 active sources |
| 每国 AI observations | ≥ 100/month |
| high-priority 国家 | ≥ 500 observations/month |
| publish-ready ratio | ≥ 55% |
| evidence 可回放率 | 100% |
| source error rate | ≤ 20% |
| AI schema parse success | ≥ 95% |
| monthly cost | ≤ $300 起步预算 |
| weekly digest 生成成功率 | ≥ 90% |

---

## 17. 风险与限制

### 17.1 数据合规风险

风险：

- Reddit、Facebook、Instagram 等平台对数据使用限制较多
- 商业用途、AI 训练用途、批量抓取用途需要谨慎
- 登录墙 / 付费墙不应绕过
- 不做 CAPTCHA 破解

策略：

```text
优先官方 API
优先公开网页
保留 source_url
不保存不必要个人信息
不输出可识别个人身份信息
不把 raw user content 用作训练集
```

### 17.2 多语言误判

风险：

- 北欧语言、德语、法语、西语、意大利语中的汽车术语不同
- 讽刺、反问、俚语容易导致 sentiment 错误
- 模型可能把品牌名、车型名误识别

策略：

```text
taxonomy alias dictionary
LLM + deterministic validation
人工 QA sample set
confidence gate
low confidence 不进入 deck
```

### 17.3 证据污染

风险：

- 导航栏、广告、cookie banner、论坛 member meta 污染正文
- document-level sentiment 过粗
- sentence window 误把不相关句子归入 observation

策略：

```text
Trafilatura-first
lxml fallback
contentUnits 优先
boilerplate-aware cleaning
evidence sentence 必须可回放
```

### 17.4 成本失控

风险：

- Stage 2 国家数量扩张
- prompt 太长
- 重复处理同一 content unit
- Perplexity report 过度调用

策略：

```text
content_hash 去重
unit-level cache
prompt versioning
cost_tracker
per-stage quota
weekly digest limit
high-value country only
```

---

## 18. PR 实施计划

## 18.1 PR-1：AI schemas + provider base

目标：

```text
建立 AI enrichment 的基础类型、provider 抽象和 prompt 目录。
```

改动：

- [ ] 新增 `07_ScrapingToolkit/jato_scraper/ai/__init__.py`
- [ ] 新增 `ai/providers/base.py`
- [ ] 新增 `ai/schemas/voc_ai_observation.py`
- [ ] 新增 `ai/schemas/news_ai_event.py`
- [ ] 新增 `ai/schemas/ai_run_summary.py`
- [ ] 新增 `ai/prompts/voc_aspect_extraction.md`
- [ ] 新增 `ai/prompts/news_event_extraction.md`
- [ ] 新增 unit tests

验收：

- [ ] Pydantic schema 可 validate example JSON
- [ ] provider base interface 有 mock test
- [ ] prompt 文件进入 repo
- [ ] 不影响现有 `jato-voc-fetch` / `jato-voc-enrich`

---

## 18.2 PR-2：DeepSeek / LLM provider + cost tracker

目标：

```text
接入低成本 LLM provider，并能记录 token 和 estimated cost。
```

改动：

- [ ] 新增 `ai/providers/deepseek_client.py`
- [ ] 新增 `ai/cost_tracker.py`
- [ ] 支持 env var：`DEEPSEEK_API_KEY`
- [ ] 支持 model config：`deepseek-v4-flash` / compatible alias
- [ ] 记录 input_tokens / output_tokens
- [ ] 输出 estimated_cost_usd
- [ ] 增加 retry / timeout / JSON parse error handling

验收：

- [ ] dry-run 不调用真实 API
- [ ] mock API test 通过
- [ ] schema parse success 可统计
- [ ] cost summary 可输出 JSON

---

## 18.3 PR-3：VOC AI enrichment runner

目标：

```text
读取现有 VOC raw / contentUnits，生成 AI observations。
```

改动：

- [ ] 新增 `ai/runners/run_voc_ai_enrich.py`
- [ ] 支持 `--countries`
- [ ] 支持 `--stage`
- [ ] 支持 `--max-units`
- [ ] 支持 `--dry-run`
- [ ] 支持 `--output-root`
- [ ] 输出 `ai_observations_<timestamp>.json`
- [ ] 输出 `ai_run_summary_<timestamp>.json`
- [ ] 写 `latest.json`

验收：

- [ ] SE/FI/NO/DK dry-run 可跑
- [ ] 每个 observation 有 source_url / unit_id / evidence
- [ ] confidence gate 生效
- [ ] failed unit 不拖垮整批
- [ ] artifact 路径符合规范

---

## 18.4 PR-4：PostgreSQL AI tables + sink

目标：

```text
把 AI run 和轻量 observation index 同步到 PostgreSQL。
```

改动：

- [ ] Alembic migration 新增 `ops.ai_enrichment_runs`
- [ ] Alembic migration 新增 `ops.ai_observations`
- [ ] 新增 `ai/sinks/pg_ai_sink.py`
- [ ] runner 支持 `--sink pg`
- [ ] 增加 DB integration test

验收：

- [ ] migration upgrade 成功
- [ ] migration downgrade 成功
- [ ] run summary 可写入 PG
- [ ] observations 可按 country / model / pain point 查询
- [ ] 不存完整 HTML / raw long text

---

## 18.5 PR-5：FastAPI AI endpoints

目标：

```text
平台侧能读取 AI run 和 AI observations。
```

改动：

- [ ] 新增 `/v1/ai/enrichment/runs`
- [ ] 新增 `/v1/ai/observations`
- [ ] 新增 `/v1/customer-insights/forum-live/ai-summary`
- [ ] 增加 service：`ai_observation_service.py`
- [ ] 增加 contract tests

验收：

- [ ] API 支持 country filter
- [ ] API 支持 model / painPoint / sentiment filter
- [ ] API 返回 evidence cards
- [ ] API 返回 coverage summary
- [ ] 前后端 type contract 通过

---

## 18.6 PR-6：Customer Insights AI Signals UI

目标：

```text
在 CustomerInsightsPage 增加 AI Signals 视图。
```

改动：

- [ ] 新增 AI Signals tab
- [ ] 新增 Top Pain Points card
- [ ] 新增 Model Mentions ranking
- [ ] 新增 Powertrain Mentions ranking
- [ ] 新增 Aspect Sentiment Matrix
- [ ] 新增 Evidence Cards with expand
- [ ] 新增 Confidence Distribution
- [ ] 新增 Source Coverage card

验收：

- [ ] 无 AI 数据时显示 empty state
- [ ] evidence card 可展开原文 / 翻译
- [ ] 图表走项目标准 chart 组件
- [ ] 移动端不破版
- [ ] 与 benchmark / forum_live 模式边界清晰

---

## 18.7 PR-7：Data Management AI Enrichment 观察台

目标：

```text
在 /data-management 观察 AI run、成本和失败情况。
```

改动：

- [ ] 新增 AI Enrichment status card
- [ ] 新增 run history table
- [ ] 新增 cost summary
- [ ] 新增 failed units summary
- [ ] 新增 artifact path display
- [ ] 新增 stage filter

验收：

- [ ] 可按 stage1_nordic / stage2 过滤
- [ ] 可按 country 过滤
- [ ] 可查看 last run status
- [ ] 可查看 estimated cost
- [ ] error summary 可读

---

## 18.8 PR-8：Country Copilot VOC AI tool

目标：

```text
让 Country Copilot 可以调用 AI observations 回答 VOC 问题。
```

改动：

- [ ] 新增 tool：`customer_voc_ai_search`
- [ ] 新增 intent：`voc_query`
- [ ] Copilot route 支持 `country + model + pain_point`
- [ ] 回答结构加入 evidence cards
- [ ] grounding 中显示 source_url / confidence / observed_at

验收：

- [ ] “瑞典 Tucson HEV 用户主要抱怨什么？”可回答
- [ ] “挪威 BEV 冬季续航问题有哪些？”可回答
- [ ] 无数据时能说明 coverage gap
- [ ] 不编造没有 evidence 的结论
- [ ] 回答能显示 source freshness

---

## 18.9 PR-9：Exa source discovery

目标：

```text
用 Exa 辅助发现新 VOC / News source candidate。
```

改动：

- [ ] 新增 `ai/providers/exa_client.py`
- [ ] 新增 `ai/source_discovery.py`
- [ ] 新增 `ai/runners/run_source_discovery.py`
- [ ] 输出 `voc_sources/candidates/*.yaml`
- [ ] 增加 source validation
- [ ] 增加 dedup by domain / URL

验收：

- [ ] Nordic 四国每国可产出 candidate source list
- [ ] candidate 不直接进入 production sources
- [ ] source tier / country / language / topic 有字段
- [ ] Data Management 可后续接入 review

---

## 18.10 PR-10：Stage 2 rollout

目标：

```text
把 MVP 能力扩展到 stage2 国家。
```

改动：

- [ ] 新增 `voc_sources/stage2.yaml`
- [ ] 新增 `news_sources/stage2.yaml`
- [ ] 新增 `taxonomy/stage2_core.yaml`
- [ ] Stage 2 国家 source coverage baseline
- [ ] Stage 2 AI enrichment schedule
- [ ] Stage 2 cost quota

验收：

- [ ] stage2 config 国家全部可跑 dry-run
- [ ] 每国至少 3 个 active sources
- [ ] 每国至少 100 AI observations/month
- [ ] Stage 2 月成本进入预算
- [ ] Copilot 支持 stage2 国家查询

---

## 19. PR Checklist 总表

### 19.1 Backend / Data

- [ ] 新增 AI schema
- [ ] 新增 AI provider abstraction
- [ ] 新增 LLM provider client
- [ ] 新增 cost tracker
- [ ] 新增 quality gate
- [ ] 新增 VOC AI runner
- [ ] 新增 News AI runner
- [ ] 新增 source discovery runner
- [ ] 新增 artifact output
- [ ] 新增 PostgreSQL migration
- [ ] 新增 PG sink
- [ ] 新增 FastAPI endpoints
- [ ] 新增 backend unit tests
- [ ] 新增 backend contract tests

### 19.2 Frontend

- [ ] Customer Insights AI Signals tab
- [ ] Evidence Card expandable view
- [ ] Data Management AI status card
- [ ] Cost summary UI
- [ ] Confidence distribution UI
- [ ] Source coverage UI
- [ ] Empty state
- [ ] Mobile responsive check
- [ ] TypeScript API types
- [ ] Frontend tests

### 19.3 Copilot

- [ ] 新增 `voc_query` intent
- [ ] 新增 `customer_voc_ai_search` tool
- [ ] 新增 `country_news_ai_search` tool
- [ ] Grounding 显示 source / confidence / freshness
- [ ] 无数据时返回 coverage gap
- [ ] 不使用 LLM 编造 evidence
- [ ] Copilot regression prompts

### 19.4 DevOps

- [ ] env vars documented
- [ ] systemd timer documented
- [ ] manual CLI documented
- [ ] dry-run mode documented
- [ ] cost quota documented
- [ ] failure retry documented
- [ ] Tencent Cloud deployment notes updated
- [ ] ROADMAP.md 更新
- [ ] README link 更新

### 19.5 Quality / QA

- [ ] 50 条 Nordic VOC 人工标注测试集
- [ ] model mention precision check
- [ ] pain point classification QA
- [ ] evidence replay QA
- [ ] translation spot check
- [ ] cost estimate check
- [ ] failed-unit sample review
- [ ] Stage 2 language sample review

---

## 20. 推荐开发顺序

### MVP：建议 6 周

| Week | 目标 | 交付 |
|---|---|---|
| W1 | AI schema + provider + cost tracker | PR-1 / PR-2 |
| W2 | VOC AI runner | PR-3 |
| W3 | PG sink + FastAPI endpoints | PR-4 / PR-5 |
| W4 | Customer Insights + Data Management UI | PR-6 / PR-7 |
| W5 | Country Copilot VOC tool | PR-8 |
| W6 | Exa source discovery + QA set | PR-9 |

### Stage 2：建议 4–6 周

| Week | 目标 | 交付 |
|---|---|---|
| S2-W1 | stage2 source config | stage2.yaml |
| S2-W2 | taxonomy / alias 扩展 | stage2_core.yaml |
| S2-W3 | stage2 dry-run | source coverage report |
| S2-W4 | stage2 AI enrichment | observations + cost report |
| S2-W5 | Copilot + UI 国家扩展 | stage2 visible |
| S2-W6 | QA / cost / performance stabilization | rollout report |

---

## 21. 明确不做

MVP 不做：

- 不重写现有 crawler
- 不让 LLM 直接替代 `jato-voc-fetch`
- 不做 Facebook / Instagram 大规模抓取
- 不绕过登录墙 / 付费墙
- 不做 CAPTCHA 破解
- 不把 forum VOC 当成代表性 survey
- 不把人口统计字段从论坛里强行推断出来
- 不把所有 AI 输出直接进入 deck
- 不把完整 raw HTML 塞进 PostgreSQL
- 不做分布式 crawling / Kubernetes 化
- 不在 Stage 1 接入复杂消息队列

Stage 2 暂不做：

- 不做全国家每日全量高频 AI enrichment
- 不做全平台社媒抓取
- 不把 Perplexity 当主采集层
- 不把 NewsAPI.ai 作为唯一新闻源

---

## 22. 环境变量

```bash
# LLM
export DEEPSEEK_API_KEY=...
export GEMINI_API_KEY=...
export OPENAI_API_KEY=...

# Search / discovery
export EXA_API_KEY=...
export PERPLEXITY_API_KEY=...

# Optional news provider
export NEWSAPI_AI_KEY=...

# Runtime flags
export JATO_AI_ENRICHMENT_ENABLED=true
export JATO_AI_DEFAULT_PROVIDER=deepseek
export JATO_AI_DEFAULT_MODEL=deepseek-v4-flash
export JATO_AI_MONTHLY_BUDGET_USD=50
export JATO_AI_STAGE2_MONTHLY_BUDGET_USD=300
```

---

## 23. ROADMAP.md 更新建议

在 `Markdown_Readme/Fullstack/ROADMAP.md` 的 `02_DataETL/` 活跃文档清单新增：

```markdown
| `02_DataETL/AI_NEWS_VOC_INTELLIGENCE_PIPELINE_2026-05-09.md` | 架构/AI/VOC/News | Draft | Crawler 主干 + LLM 增强的 News/VOC 两阶段实施方案；Stage 1 北欧四国 MVP，Stage 2 扩展到 stage2 国家 |
```

在当前关注点新增：

```markdown
- News / VOC 下一阶段将采用 **crawler 主采集 + LLM enrichment** 的混合架构：crawler 继续负责可复现抓取和 artifact，LLM 负责多语言理解、实体归一、aspect sentiment、evidence card 与周报生成。MVP 先覆盖 SE / FI / NO / DK，Stage 2 再扩展到 stage2 国家。
```

---

## 24. 成功标准

这个功能成功，不是因为“用了大模型”，而是因为它能稳定回答：

```text
某国某车型用户在抱怨什么？
这些抱怨来自哪些 source？
原文证据是什么？
置信度是多少？
这个痛点是否在多个 source 复现？
这个趋势比上周增强还是减弱？
能否支撑产品定位 / 定价 / 配置建议？
```

最终目标：

> 把 News / VOC 从“抓到一堆网页”升级成“可审计、可回放、可对比、可进入 Country Copilot 的汽车市场洞察资产”。

---

## 25. References

- Exa API Pricing: https://exa.ai/pricing
- DeepSeek API Models & Pricing: https://api-docs.deepseek.com/quick_start/pricing
- Perplexity Sonar Deep Research Docs: https://docs.perplexity.ai/docs/sonar/models/sonar-deep-research
- NewsAPI.ai Pricing: https://newsapi.ai/plans
- Firecrawl Extract API: https://docs.firecrawl.dev/api-reference/v2-endpoint/extract
- Reddit Data API Terms: https://redditinc.com/policies/data-api-terms
- Google robots.txt crawler documentation: https://developers.google.com/search/reference/robots_txt
- YouTube Data API commentThreads.list: https://developers.google.com/youtube/v3/docs/commentThreads/list
