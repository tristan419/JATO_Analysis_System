# PRD：Governed Multi-Source Analytical Copilot for JATO Analysis System

> 建议保存路径：
>
> `Markdown_Readme/Fullstack/01_DevWorkflow/COUNTRY_COPILOT_GOVERNED_ANALYTICAL_COPILOT_PRD_2026-05-11.md`
>
> 目标：交给 Claude Code 实施。
>
> 重要说明：本 PRD 不是要重写 Country Copilot，也不是单纯做 Markdown / HTML 渲染。它是在现有 FastAPI + React + TypeScript 代码快照基础上，把 Country Copilot 从“结构化报告聊天助手”升级为“受治理的多源分析 Copilot”。

---

## 0. 文档信息

| 字段 | 内容 |
|---|---|
| 文档名称 | Governed Multi-Source Analytical Copilot PRD |
| 项目 | JATO Analysis System / OJ Europe App Platform |
| 当前阶段 | Phase 4 — Fullstack 主线 |
| 当前架构 | FastAPI + React + TypeScript |
| 当前核心页面 | `/copilot` Country Copilot 页面、CountryChatWidget、Dashboard、Data Management、MSRP、Review Workbench |
| 当前核心后端 | `/v1/assistant/country/chat`、`/v1/assistant/country/chart-deck`、MSRP APIs、Review APIs、Engineering APIs、Data Management APIs |
| 数据来源 | JATO Parquet、PostgreSQL CurrentPrice / PriceHistory / MSRP Link / MatchOverride、Engineering normalized variants、VOC artifacts、News digest、Policy / tax snippets、未来 live web |
| 本 PRD 目标 | 定义一条从用户问题到多源证据计划、受控查询、结果验证、证据包、答案生成、渲染、审计反馈的完整 governed copilot pipeline |
| 核心原则 | 不是 NL2SQL，而是 NL2EvidencePlan |
| Claude Code 目标 | 基于现有代码渐进实施，不破坏旧链路，不重启服务，不推倒重写 |
| MVP 优先级 | Metadata Catalog + Source Planner + QueryPlan Contract + Validator + EvidencePack + Adapter to existing CountryChatResponse |
| 非目标 | 不引入 Hermes Agent 主链路；不把所有问题都翻译成 SQL；不让 LLM 直接查询数据库；不删除现有 CountryChatGroundedAnswer / CountryChatAnalysisDeck |

---

## 1. 一句话结论

本 PRD 的目标不是把自然语言直接翻译成 SQL，而是把用户问题翻译成一个受治理的多源证据计划：

```text
User Question
→ Intent Recognition
→ Semantic Layer
→ Metadata Catalog
→ Source Planner
→ SQL / Tool / Evidence Planner
→ Validator
→ Governed Query Engine
→ Result Verifier
→ Evidence Pack Builder
→ Answer Composer
→ Structured Renderer
→ Audit & Feedback
```

对于单独 BI 系统，流程可以是：

```text
User Question → SQL Planner → Parquet Query → Chart / Answer
```

但 JATO Analysis System 不是单独 BI。它既有结构化 JATO sales 数据，也有 PostgreSQL 中的官方价格、MSRP reconciliation、CurrentPrice、PriceHistory、JatoMsrpLink、MatchOverride，又有 Engineering variant diff、VOC forum evidence、新闻、政策、碳税、补贴、法规和未来 live web。因此正确定位不是 **Natural Language to SQL Copilot**，而是：

```text
Governed Evidence Copilot
```

也就是说，系统不是先问“我要写什么 SQL”，而是先问：

```text
这个问题需要什么证据？
证据来自哪些 source lane？
哪些是必须源？
哪些是补充源？
哪些可以结构化查询？
哪些必须走 deterministic service？
哪些需要非结构化 evidence retrieval？
哪些证据过期、缺失或置信度不足？
最终能支持什么结论？
```

---

## 2. 当前代码快照理解

Claude Code 执行前必须先理解当前代码状态，不允许按空项目思路重建。

### 2.1 当前项目主线

当前项目已经处于 Fullstack 主线，Streamlit 已归档。主要技术栈是：

```text
Frontend: React + TypeScript
Backend: FastAPI
Data: Parquet + PostgreSQL
Deployment: Tencent Cloud + nginx + systemd + GitHub Actions
LLM: DeepSeek V4-Flash / other configured chat providers
```

Country Copilot 当前已经具备：

```text
snapshot → report
cross-dimension causal analysis
drive × fuel
registration × fuel
segment × fuel
6-section report prompt
streaming SSE
Token cost display
answerMode
grounding
contextSnapshot
executionPlan
renderHints
visible answer path
single-round evidence + final answer fallback
```

这意味着本 PRD 应该基于现有结构升级，而不是推翻重写。

### 2.2 当前前端关键文件

Claude Code 必须先检查：

```text
06_AppPlatform/frontend/src/types/countryChat.ts
06_AppPlatform/frontend/src/contexts/CountryChatContext.tsx
06_AppPlatform/frontend/src/pages/CountryChatPage.tsx
06_AppPlatform/frontend/src/components/CountryChatGroundedAnswer.tsx
06_AppPlatform/frontend/src/components/CountryChatAnalysisDeck.tsx
06_AppPlatform/frontend/src/components/ChatInlineCharts.tsx
06_AppPlatform/frontend/src/components/CountryChatWidget.tsx
06_AppPlatform/frontend/src/api/client.ts
06_AppPlatform/frontend/src/countryCopilot.css
```

### 2.3 当前后端关键文件

Claude Code 必须先检查：

```text
06_AppPlatform/backend/app/main.py
06_AppPlatform/backend/app/services/
06_AppPlatform/backend/app/repositories/
06_AppPlatform/backend/app/schemas/
06_AppPlatform/backend/tests/unit/
06_AppPlatform/backend/README.md
```

并搜索以下关键词定位实际实现：

```text
country_chat
country_chat_service
assistant/country/chat
answerMode
grounding
executionPlan
renderHints
CurrentPrice
JatoMsrpLink
MatchOverride
variantDiff
peerCorridor
priceOverlay
newsDigest
marketEvents
```

### 2.4 当前已完成能力不应被破坏

本 PRD 明确要求保留以下能力：

```text
1. /copilot 页面正常问答
2. CountryChatWidget 正常问答
3. CountryChatContext session cache / draft cache / selected country / selected model
4. refresh news / retry with fresh news
5. CountryChatGroundedAnswer fallback
6. CountryChatAnalysisDeck / ChatInlineCharts
7. CurrentPrice MSRP lookup
8. engineering variant diff
9. peerCorridor / price stance
10. news ops status
11. Token cost display
12. answerMode + grounding + contextSnapshot
13. visible answer path
```

---

## 3. 为什么这不是纯 BI Copilot

### 3.1 纯 BI 的边界

纯 BI 助手通常只需要：

```text
用户问题
→ 识别指标 / 维度 / 过滤条件
→ 生成 SQL
→ 查询数仓 / Parquet / DuckDB
→ 返回表格或图表
```

例如：

```text
“瑞典 2025 年 BEV 占比是多少？”
```

这种问题确实可以走 BI lane：

```text
JATO Parquet
→ country = Sweden
→ year = 2025
→ group by powertrain
→ sum sales
→ calculate share
```

### 3.2 JATO Copilot 的边界

但你当前的问题大量不是纯 BI：

```text
J7 SHS 在瑞典能不能卖？
O9 PHEV 4WD 的 70kW 快充能不能打公寓用户？
Tucson HEV 在捷克用户抱怨什么？
瑞典 malus 对 PHEV 定价有什么影响？
芬兰碳税 / car tax / vehicle tax 怎么进入 MSRP？
某个 trim 的官方 MSRP 是否可信？
某个竞品的配置差异对应哪个 JATO version？
最近北欧 PHEV 政策有什么变化？
```

这些问题需要：

| 问题类型 | 需要数据 |
|---|---|
| 市场结构 | JATO Parquet |
| 车型销量 | JATO Parquet |
| MSRP / 官方价格 | PostgreSQL CurrentPrice / PriceHistory |
| 价格对账 | JatoMsrpLink + MatchOverride |
| 配置差异 | Engineering normalized variants / variantDiff |
| 政策 / 碳税 | Policy / tax digest |
| 新闻 | News article / news digest |
| VOC | Forum raw / enriched / customer insight deck |
| 策略判断 | 多源 evidence pack |
| PPT / report | Structured renderer |

所以不能用单一 NL2SQL 架构。正确架构是 **multi-source governed evidence planning**。

---

## 4. 目标架构

### 4.1 总体链路

```text
┌────────────────────────────────────────────────────┐
│ User Question                                       │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 1. Intent Recognition                               │
│ - fact_lookup / metric_query / comparison           │
│ - trend / distribution / correlation                │
│ - pricing_strategy / product_strategy               │
│ - policy_tax / voc_insight / news_intelligence      │
│ - country_report / deck_generation                  │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 2. Semantic Layer                                   │
│ - country / brand / model / trim / version          │
│ - powertrain / segment / body / drive               │
│ - metric / dimension / time / business goal         │
│ - canonical aliases                                 │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 3. Metadata Catalog Lookup                          │
│ - dataset availability                              │
│ - field semantics                                   │
│ - join rules                                        │
│ - source freshness                                  │
│ - governance policy                                 │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 4. Source Planner                                   │
│ - structured BI lane                                │
│ - canonical entity lane                             │
│ - VOC lane                                          │
│ - policy / tax lane                                 │
│ - news lane                                         │
│ - live web lane                                     │
└──────────────────────┬─────────────────────────────┘
                       ↓
        ┌──────────────┼────────────────┬───────────────┐
        ↓              ↓                ↓
┌─────────────┐ ┌───────────────┐ ┌────────────────────┐
│ SQL Planner │ │ Tool Planner  │ │ Evidence Planner   │
└──────┬──────┘ └───────┬───────┘ └─────────┬──────────┘
       ↓                ↓                   ↓
┌─────────────┐ ┌───────────────┐ ┌────────────────────┐
│ SQL         │ │ Tool          │ │ Source / Freshness  │
│ Validator   │ │ Validator     │ │ Validator           │
└──────┬──────┘ └───────┬───────┘ └─────────┬──────────┘
       ↓                ↓                   ↓
┌────────────────────────────────────────────────────┐
│ 5. Governed Query Engine                            │
│ - StructuredQueryEngine                             │
│ - CanonicalServiceEngine                            │
│ - EvidenceRetrievalEngine                           │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 6. Result Verifier                                  │
│ - row count / null / share sum / freshness          │
│ - unit / currency / alias / source coverage         │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 7. Evidence Pack Builder                            │
│ - normalized claims                                 │
│ - supporting sources                                │
│ - confidence                                        │
│ - limitations                                       │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 8. Answer Composer                                  │
│ - CountryAnswerV1                                   │
│ - Evidence-grounded answer                          │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 9. Renderer                                         │
│ - existing GroundedAnswer fallback                  │
│ - Markdown / HTML / Table / Chart / Report blocks   │
└──────────────────────┬─────────────────────────────┘
                       ↓
┌────────────────────────────────────────────────────┐
│ 10. Audit & Feedback                                │
│ - trace                                             │
│ - query plan                                        │
│ - evidence pack                                     │
│ - user feedback                                     │
│ - eval harness                                      │
└────────────────────────────────────────────────────┘
```

### 4.2 设计原则

1. **不是所有问题都进入 SQL Planner。**
2. **LLM 不直接执行 SQL。**
3. **LLM 不直接访问数据库。**
4. **LLM 只生成受限 QueryPlan JSON 或 SourcePlan JSON。**
5. **所有 SQL 必须由 deterministic compiler 生成。**
6. **所有 SQL 必须经过 SQL Validator。**
7. **所有结果必须经过 Result Verifier。**
8. **所有策略判断必须基于 Evidence Pack。**
9. **Renderer 只负责展示，不负责业务判断。**
10. **Audit log 必须记录每次回答的 plan、sources、verification、answer mode。**

---

## 5. Source Lanes 数据通道设计

### 5.1 Structured BI Lane

用于处理结构化市场数据。

数据来源：

```text
JATO Parquet
precomputed aggregates
PostgreSQL analytical tables if available
```

典型问题：

```text
瑞典 2025 年 BEV 占比是多少？
芬兰 SUV-B PHEV 销量趋势如何？
捷克 HEV 市场 2022-2025 怎么变化？
西班牙各价格带销量分布？
```

允许操作：

```text
filter
group_by
aggregate
rank
trend
share
distribution
correlation
```

禁止操作：

```text
write
delete
unbounded full scan
unvalidated joins
SELECT *
```

### 5.2 Canonical Entity Lane

用于处理价格、车型、配置、实体映射。

数据来源：

```text
CurrentPrice
PriceHistory
JatoMsrpLink
MatchOverride
Engineering normalized variants
variantDiff
peerCorridor
price stance
```

典型问题：

```text
瑞典 RAV4 HEV 2026 版型价格是多少？
XC60 B5 和 B6 配置差异是什么？
J7 SHS 应该放在哪个 peer corridor？
某个官方 trim 对应哪个 JATO version？
```

这里不一定需要 SQL Planner，优先使用 deterministic service：

```text
msrp_lookup_service
engineering_variant_diff_service
positioning / peer corridor service
review / link service
```

### 5.3 VOC Lane

用于处理论坛、社交媒体、用户评论、试驾反馈。

数据来源：

```text
raw VOC artifact
heuristic enriched VOC artifact
customer_insight_deck.json
forum contentUnits
theme tags
persona cohorts
product matching
competitor matching
observation sentences
```

典型问题：

```text
捷克 Tucson HEV 用户抱怨什么？
北欧用户对 PHEV 快充有什么反馈？
瑞典 SUV 用户最关注什么配置？
用户对 AWD 和续航 trade-off 怎么看？
```

VOC Lane 不能返回“统计事实”冒充销量数据。  
它只能返回：

```text
observed feedback
theme frequency
qualitative signal
example quote / observation sentence
confidence / source limitation
```

### 5.4 Policy / Tax Lane

用于政策、碳税、补贴、法规、CO₂、malus、car tax、vehicle tax。

数据来源：

```text
policy snippets
tax rule config
official source digest
manual policy notes
news policy digest
future policy table
```

典型问题：

```text
瑞典 malus 怎么算？
芬兰 car tax 和 vehicle tax 怎么进入 MSRP？
Euro 6e-bis 对 PHEV 有什么影响？
125g CO₂ 在瑞典会产生多少税？
```

输出必须包含：

```text
jurisdiction
effective date
formula
threshold
unit
source
confidence
limitation
```

### 5.5 News Lane

用于最新新闻、市场事件、政策变化、品牌动态。

数据来源：

```text
news_article
news_digest
marketEvents
RSS / Atom snapshot
Gemini summarized digest
future live search
```

典型问题：

```text
最近瑞典汽车市场有什么政策新闻？
捷克 HEV 市场最近有什么变化？
Tucson HEV 最近有没有召回或口碑新闻？
```

News Lane 必须显示 freshness：

```text
published_at
fetched_at
syncTimestamp
stale flag
source domain
summary provider
```

### 5.6 Live Web Lane

MVP 不默认实现，只保留扩展位。

触发条件：

```text
用户明确说“联网查 / latest / 最新”
本地 source coverage 不足
policy/news freshness 过期
关键事实缺失
```

必须限制：

```text
domain allowlist
quota
no arbitrary browsing
no executable download
write-back as normalized evidence before answer
```

---

## 6. Intent Recognition

### 6.1 Intent 类型

新增统一 intent taxonomy：

```python
CountryCopilotIntent = Literal[
    "fact_lookup",
    "metric_query",
    "comparison",
    "trend",
    "distribution",
    "correlation",
    "pricing_strategy",
    "product_strategy",
    "policy_tax",
    "voc_insight",
    "news_intelligence",
    "country_report",
    "deck_generation",
    "chitchat"
]
```

### 6.2 Intent 输出契约

新增：

```text
06_AppPlatform/backend/app/copilot_governance/intent.py
```

```python
from pydantic import BaseModel
from typing import Literal

class IntentRecognitionResult(BaseModel):
    intent: str
    confidence: float
    question_type: Literal["direct", "analytical", "strategic", "report", "chitchat"]
    requires_structured_data: bool
    requires_canonical_entity_data: bool
    requires_unstructured_evidence: bool
    requires_policy_context: bool
    requires_news_context: bool
    requires_chart: bool
    requires_table: bool
    requires_report: bool
    rationale: str
```

### 6.3 Intent 示例

输入：

```text
J7 SHS 在瑞典能不能卖？
```

输出：

```json
{
  "intent": "product_strategy",
  "confidence": 0.88,
  "question_type": "strategic",
  "requires_structured_data": true,
  "requires_canonical_entity_data": true,
  "requires_unstructured_evidence": true,
  "requires_policy_context": true,
  "requires_news_context": false,
  "requires_chart": true,
  "requires_table": true,
  "requires_report": false,
  "rationale": "The question asks for market feasibility and requires sales structure, competitor price corridor, policy/tax context and optional VOC evidence."
}
```

---

## 7. Semantic Layer

### 7.1 目标

Semantic Layer 负责把用户自然语言转为业务语义对象。  
它不是 SQL，也不是最终 answer。它是 query planning 的中间层。

### 7.2 新增文件

```text
06_AppPlatform/backend/app/copilot_governance/semantic_layer.py
06_AppPlatform/backend/app/copilot_governance/entity_resolution.py
```

### 7.3 Semantic Parse Contract

```python
from pydantic import BaseModel
from typing import Any

class SemanticEntity(BaseModel):
    raw_text: str
    entity_type: str
    canonical_value: str | None = None
    aliases: list[str] = []
    confidence: float = 0.0

class SemanticMetric(BaseModel):
    name: str
    aggregation: str | None = None
    unit: str | None = None

class SemanticDimension(BaseModel):
    name: str
    role: str

class SemanticTimeRange(BaseModel):
    start: str | None = None
    end: str | None = None
    granularity: str | None = None

class SemanticParseResult(BaseModel):
    country: str | None = None
    country_code: str | None = None
    brands: list[str] = []
    models: list[str] = []
    trims: list[str] = []
    powertrains: list[str] = []
    segments: list[str] = []
    body_types: list[str] = []
    drive_types: list[str] = []
    metrics: list[SemanticMetric] = []
    dimensions: list[SemanticDimension] = []
    time_range: SemanticTimeRange | None = None
    business_goal: str | None = None
    entities: list[SemanticEntity] = []
    assumptions: list[str] = []
    missing_slots: list[str] = []
```

### 7.4 Semantic Layer 示例

输入：

```text
瑞典市场，J7 SHS 对比 Tucson HEV 应该怎么定价？
```

输出：

```json
{
  "country": "Sweden",
  "country_code": "SE",
  "brands": ["JAECOO", "Hyundai"],
  "models": ["JAECOO 7 SHS", "Tucson HEV"],
  "powertrains": ["PHEV", "HEV"],
  "segments": ["SUV-B", "SUV-C"],
  "metrics": [
    {"name": "sales_volume", "aggregation": "sum", "unit": "units"},
    {"name": "msrp", "aggregation": "median", "unit": "SEK"},
    {"name": "peer_corridor", "aggregation": null, "unit": "SEK"}
  ],
  "dimensions": [
    {"name": "model", "role": "competitor"},
    {"name": "powertrain", "role": "structure"},
    {"name": "price_band", "role": "positioning"}
  ],
  "business_goal": "pricing_strategy",
  "assumptions": [
    "J7 SHS is treated as JAECOO 7 PHEV / SHS hybrid product for planning."
  ],
  "missing_slots": []
}
```

---

## 8. Metadata Catalog

### 8.1 目标

Metadata Catalog 是系统知道“有哪些数据、在哪里、能怎么查、可信度如何”的中枢。  
没有 Metadata Catalog，LLM 会乱猜字段、乱查表、乱 join。

### 8.2 新增目录

```text
06_AppPlatform/backend/app/copilot_governance/catalog/
  __init__.py
  models.py
  registry.py
  datasets/
    jato_sales_parquet.yaml
    current_price_postgres.yaml
    price_history_postgres.yaml
    engineering_variants.yaml
    voc_forum_artifacts.yaml
    news_digest.yaml
    policy_tax_digest.yaml
```

### 8.3 Dataset Catalog Contract

```python
class FieldCatalogItem(BaseModel):
    name: str
    semantic_role: str
    data_type: str
    filterable: bool = False
    aggregatable: bool = False
    groupable: bool = False
    description: str | None = None

class DatasetCatalogItem(BaseModel):
    dataset_id: str
    display_name: str
    source_lane: str
    storage_type: str
    grain: str
    owner: str | None = None
    freshness_field: str | None = None
    latest_period: str | None = None
    fields: list[FieldCatalogItem]
    allowed_intents: list[str]
    required_filters: list[str] = []
    join_keys: list[str] = []
    governance: dict = {}
```

### 8.4 jato_sales_parquet 示例

```yaml
dataset_id: jato_sales_parquet
display_name: JATO Sales Parquet
source_lane: structured_bi
storage_type: parquet
grain: country_month_model_version
freshness_field: year_month
latest_period: "2025-12"
allowed_intents:
  - metric_query
  - comparison
  - trend
  - distribution
  - correlation
  - pricing_strategy
  - product_strategy
  - country_report
required_filters:
  - country
governance:
  max_rows: 5000
  allow_full_scan: false
  readonly: true
fields:
  - name: Country
    semantic_role: country
    data_type: string
    filterable: true
    groupable: true
  - name: Model
    semantic_role: model
    data_type: string
    filterable: true
    groupable: true
  - name: Powertrain
    semantic_role: powertrain
    data_type: string
    filterable: true
    groupable: true
  - name: Sales
    semantic_role: measure.sales_volume
    data_type: number
    aggregatable: true
  - name: Segment
    semantic_role: segment
    data_type: string
    filterable: true
    groupable: true
```

### 8.5 VOC Catalog 示例

```yaml
dataset_id: voc_forum_artifacts
display_name: VOC Forum Artifacts
source_lane: voc
storage_type: artifact_json
grain: country_source_document_content_unit
freshness_field: fetched_at
allowed_intents:
  - voc_insight
  - product_strategy
  - pricing_strategy
  - country_report
governance:
  qualitative_only: true
  cannot_claim_market_share: true
  require_observation_sentence: true
fields:
  - name: country
    semantic_role: country
    data_type: string
    filterable: true
  - name: theme_tags
    semantic_role: voc.theme
    data_type: array
    filterable: true
  - name: persona_cohorts
    semantic_role: voc.persona
    data_type: array
    filterable: true
  - name: observation_sentence
    semantic_role: evidence.quote
    data_type: string
  - name: source_url
    semantic_role: source.url
    data_type: string
```

---

## 9. Source Planner

### 9.1 目标

Source Planner 决定一个问题需要哪些数据源。  
这是整个系统最重要的新增模块。

新增文件：

```text
06_AppPlatform/backend/app/copilot_governance/source_planner.py
```

### 9.2 Source Plan Contract

```python
class SourcePlanItem(BaseModel):
    source_id: str
    source_lane: str
    required: bool
    reason: str
    expected_output: str
    fallback_source_id: str | None = None
    freshness_required: bool = False
    confidence_weight: float = 1.0

class SourcePlan(BaseModel):
    question: str
    intent: str
    execution_mode: Literal["structured_only", "canonical_only", "evidence_only", "hybrid"]
    items: list[SourcePlanItem]
    answer_mode: str
    requires_sql_planner: bool
    requires_tool_planner: bool
    requires_evidence_retrieval: bool
    max_execution_steps: int
```

### 9.3 示例：纯 BI 问题

输入：

```text
瑞典 2025 年 SUV 各动力类型销量分布？
```

输出：

```json
{
  "execution_mode": "structured_only",
  "requires_sql_planner": true,
  "requires_tool_planner": false,
  "requires_evidence_retrieval": false,
  "answer_mode": "chart_insight",
  "items": [
    {
      "source_id": "jato_sales_parquet",
      "source_lane": "structured_bi",
      "required": true,
      "reason": "Need sales distribution by SUV and powertrain.",
      "expected_output": "aggregated sales by powertrain"
    }
  ]
}
```

### 9.4 示例：策略问题

输入：

```text
J7 SHS 在瑞典能不能卖？
```

输出：

```json
{
  "execution_mode": "hybrid",
  "requires_sql_planner": true,
  "requires_tool_planner": true,
  "requires_evidence_retrieval": true,
  "answer_mode": "strategy_brief",
  "items": [
    {
      "source_id": "jato_sales_parquet",
      "source_lane": "structured_bi",
      "required": true,
      "reason": "Need segment and powertrain sales structure.",
      "expected_output": "SUV segment / powertrain mix / competitor volume"
    },
    {
      "source_id": "current_price_postgres",
      "source_lane": "canonical_entity",
      "required": true,
      "reason": "Need reviewed MSRP and competitor price corridor.",
      "expected_output": "current MSRP and price stance"
    },
    {
      "source_id": "policy_tax_digest",
      "source_lane": "policy_tax",
      "required": false,
      "reason": "Need Swedish malus / CO2 threshold context if available.",
      "expected_output": "tax risk and policy explanation"
    },
    {
      "source_id": "voc_forum_artifacts",
      "source_lane": "voc",
      "required": false,
      "reason": "Need user pain points around hybrid SUV, AWD and winter use.",
      "expected_output": "qualitative customer concerns"
    }
  ]
}
```

---

## 10. QueryPlan / SQL Planner

### 10.1 原则

LLM 不直接输出 SQL。  
LLM 或规则系统只能输出 QueryPlan JSON。  
SQL 由 deterministic compiler 生成。

新增文件：

```text
06_AppPlatform/backend/app/copilot_governance/query_plan.py
06_AppPlatform/backend/app/copilot_governance/sql_compiler.py
```

### 10.2 QueryPlan Contract

```python
class QueryMetric(BaseModel):
    field: str
    aggregation: Literal["sum", "avg", "min", "max", "count", "count_distinct", "median"]
    alias: str

class QueryFilter(BaseModel):
    field: str
    operator: Literal["=", "!=", "in", "not_in", ">=", "<=", ">", "<", "between", "contains"]
    value: Any

class QueryPlan(BaseModel):
    query_id: str
    dataset_id: str
    query_type: Literal["aggregate", "detail", "ranking", "trend", "distribution", "correlation"]
    filters: list[QueryFilter]
    group_by: list[str] = []
    metrics: list[QueryMetric] = []
    order_by: list[dict] = []
    limit: int = 1000
    time_grain: str | None = None
    rationale: str
```

### 10.3 QueryPlan 示例

```json
{
  "query_id": "qp_se_suv_powertrain_2025",
  "dataset_id": "jato_sales_parquet",
  "query_type": "aggregate",
  "filters": [
    {"field": "Country", "operator": "=", "value": "Sweden"},
    {"field": "Year", "operator": "=", "value": 2025},
    {"field": "BodyType", "operator": "=", "value": "SUV"}
  ],
  "group_by": ["Powertrain"],
  "metrics": [
    {"field": "Sales", "aggregation": "sum", "alias": "sales_volume"}
  ],
  "order_by": [{"field": "sales_volume", "direction": "desc"}],
  "limit": 100,
  "rationale": "User asks for SUV powertrain distribution in Sweden 2025."
}
```

---

## 11. SQL Validator

### 11.1 目标

SQL Validator 确保所有结构化查询：

```text
安全
只读
可控
有过滤
不全表扫描
不越权
不乱 join
不超时
```

新增文件：

```text
06_AppPlatform/backend/app/copilot_governance/sql_validator.py
```

### 11.2 Validator Rules

必须实现：

```text
1. readonly only
2. dataset whitelist
3. field whitelist
4. no SELECT *
5. no DDL / DML
6. no DELETE / UPDATE / INSERT / DROP / ALTER / CREATE
7. max limit
8. max scan rows if known
9. required filters
10. country filter required for large datasets
11. time filter recommended for time-series datasets
12. join whitelist
13. no arbitrary raw SQL from LLM
14. timeout
15. aggregation whitelist
```

### 11.3 Validator Output

```python
class ValidationIssue(BaseModel):
    severity: Literal["error", "warning", "info"]
    code: str
    message: str
    suggested_fix: str | None = None

class QueryValidationResult(BaseModel):
    valid: bool
    risk_level: Literal["low", "medium", "high"]
    issues: list[ValidationIssue]
    normalized_query_plan: QueryPlan | None = None
```

---

## 12. Governed Query Engine

### 12.1 目标

统一执行不同 source lane，但不同 lane 使用不同治理规则。

新增目录：

```text
06_AppPlatform/backend/app/copilot_governance/engines/
  __init__.py
  structured_query_engine.py
  canonical_service_engine.py
  evidence_retrieval_engine.py
  governed_query_engine.py
```

### 12.2 StructuredQueryEngine

负责：

```text
Parquet / DuckDB / Polars query
PostgreSQL read-only analytical query
precomputed aggregate access
```

输入：

```text
Validated QueryPlan
```

输出：

```text
QueryResult
```

### 12.3 CanonicalServiceEngine

负责调用 deterministic services：

```text
CurrentPrice lookup
PriceHistory lookup
MSRP link / MatchOverride
Engineering variant diff
peerCorridor
price stance
reviewed price overlay
```

输入：

```text
ToolPlan
```

输出：

```text
CanonicalResult
```

### 12.4 EvidenceRetrievalEngine

负责：

```text
VOC artifact retrieval
news digest retrieval
policy / tax snippets
carbon tax rule lookup
future live web retrieval
```

输入：

```text
EvidenceRetrievalPlan
```

输出：

```text
EvidenceResult
```

### 12.5 Unified Result

```python
class GovernedResult(BaseModel):
    result_id: str
    source_id: str
    source_lane: str
    status: Literal["ok", "partial", "empty", "error"]
    rows: list[dict] = []
    summary: dict = {}
    evidence_items: list[dict] = []
    freshness: str | None = None
    confidence: Literal["high", "medium", "low"] = "medium"
    limitations: list[str] = []
    debug: dict = {}
```

---

## 13. Result Verifier

### 13.1 目标

Result Verifier 在把结果交给 LLM 之前做质量验证。

新增文件：

```text
06_AppPlatform/backend/app/copilot_governance/result_verifier.py
```

### 13.2 Verification Checks

必须支持：

```text
row_count_check
empty_result_check
null_rate_check
date_freshness_check
share_sum_check
currency_check
unit_check
country_match_check
model_alias_match_check
source_coverage_check
outlier_check
confidence_downgrade
```

### 13.3 示例：share sum check

如果 powertrain share 合计不是 100%，可能是未知燃料类型或过滤条件问题：

```json
{
  "verification_status": "warning",
  "issues": [
    {
      "type": "share_sum_mismatch",
      "message": "Powertrain share sums to 93.4%. Unknown or missing fuel categories may exist.",
      "severity": "warning"
    }
  ],
  "confidence_adjustment": "medium"
}
```

### 13.4 Verification Output

```python
class ResultVerificationIssue(BaseModel):
    severity: Literal["error", "warning", "info"]
    code: str
    message: str
    affected_source_id: str | None = None

class ResultVerificationReport(BaseModel):
    status: Literal["pass", "warning", "fail"]
    confidence: Literal["high", "medium", "low"]
    issues: list[ResultVerificationIssue]
    limitations: list[str]
    verified_results: list[GovernedResult]
```

---

## 14. Evidence Pack Builder

### 14.1 目标

Evidence Pack 是 Answer Composer 的唯一可信输入。  
LLM 不应该直接拿原始 SQL rows、raw VOC、raw news 拼答案。

新增文件：

```text
06_AppPlatform/backend/app/copilot_governance/evidence_pack.py
```

### 14.2 Evidence Pack Contract

```python
class EvidenceSource(BaseModel):
    source_id: str
    source_lane: str
    source_name: str
    freshness: str | None = None
    confidence: Literal["high", "medium", "low"]
    coverage: Literal["strong", "partial", "thin", "missing"]
    limitations: list[str] = []

class EvidenceClaim(BaseModel):
    claim_id: str
    claim: str
    claim_type: Literal["fact", "metric", "trend", "comparison", "risk", "recommendation", "qualitative_signal"]
    supporting_source_ids: list[str]
    confidence: Literal["high", "medium", "low"]
    limitations: list[str] = []

class EvidencePack(BaseModel):
    evidence_pack_id: str
    question: str
    country: str | None = None
    intent: str
    answer_mode: str
    sources: list[EvidenceSource]
    claims: list[EvidenceClaim]
    tables: list[dict] = []
    charts: list[dict] = []
    limitations: list[str] = []
    next_data_needed: list[str] = []
    verification_report: ResultVerificationReport
```

### 14.3 示例

```json
{
  "evidence_pack_id": "evp_se_j7_20260511",
  "question": "J7 SHS 在瑞典能不能卖？",
  "country": "Sweden",
  "intent": "product_strategy",
  "answer_mode": "strategy_brief",
  "sources": [
    {
      "source_id": "jato_sales_parquet",
      "source_lane": "structured_bi",
      "source_name": "JATO Sales Parquet",
      "freshness": "2025-12",
      "confidence": "high",
      "coverage": "strong",
      "limitations": []
    },
    {
      "source_id": "current_price_postgres",
      "source_lane": "canonical_entity",
      "source_name": "CurrentPrice",
      "freshness": "2026-04",
      "confidence": "medium",
      "coverage": "partial",
      "limitations": ["Some competitor trims may not be fully reviewed."]
    },
    {
      "source_id": "voc_forum_artifacts",
      "source_lane": "voc",
      "source_name": "VOC Forum Artifacts",
      "freshness": "2026-04",
      "confidence": "medium",
      "coverage": "partial",
      "limitations": ["VOC is observed-only and not representative of full market demand."]
    }
  ],
  "claims": [
    {
      "claim_id": "claim_001",
      "claim": "J7 SHS should be positioned as a value-led electrified SUV rather than a premium challenger.",
      "claim_type": "recommendation",
      "supporting_source_ids": ["jato_sales_parquet", "current_price_postgres"],
      "confidence": "medium",
      "limitations": ["Trim-level sales truth is not fully joined to version diff answer."]
    }
  ],
  "limitations": [
    "Policy/tax evidence is partial and should be verified before external presentation."
  ],
  "next_data_needed": [
    "Reviewed competitor MSRP coverage",
    "Trim-level sales truth join",
    "Nordic hybrid SUV VOC coverage"
  ]
}
```

---

## 15. Answer Composer

### 15.1 目标

Answer Composer 基于 Evidence Pack 生成答案。  
它不负责查数据，不负责验证数据，只负责表达。

新增：

```text
06_AppPlatform/backend/app/copilot_governance/answer_composer.py
```

### 15.2 输出 CountryAnswerV1

沿用之前的 `CountryAnswerV1`，但本 PRD 中它作为 Evidence Pack 后的 output layer。

建议保留：

```text
summary
dataAvailability
blocks
evidence
recommendedActions
risks
nextDataNeeded
renderMeta
debug
```

### 15.3 答案模式

```text
quick_answer
kpi_answer
comparison_table
chart_insight
strategy_brief
markdown_report
html_report
ppt_slide
```

### 15.4 约束

Answer Composer 必须遵守：

```text
1. 不使用 Evidence Pack 之外的数据
2. 不编造具体数值
3. 缺失数据必须写入 limitations
4. 定性 VOC 不能表达为全市场事实
5. 新闻必须显示 freshness
6. 政策必须显示 effective date 或 unknown
7. 策略建议必须绑定 supporting claim
```

---

## 16. Renderer 关系

之前的 `COUNTRY_COPILOT_STRUCTURED_RENDERING_PRD_2026-05-11.md` 仍然有效，但它是 output layer 子 PRD。

本 PRD 和渲染 PRD 的关系：

```text
Governed Analytical Copilot PRD
  ├── Intent / Semantic / Metadata / Source / Query / Evidence Governance
  └── Structured Rendering PRD
       ├── CountryAnswerV1
       ├── Markdown Renderer
       ├── HTML Renderer
       ├── Table Block
       ├── Chart Block
       └── Evidence Ledger UI
```

Claude Code 可以先实施本 PRD 的 governance contracts，再接入已有 structured rendering 方案。

---

## 17. Audit & Feedback

### 17.1 目标

每一次 Copilot 回答都应可审计。

新增目录：

```text
06_AppPlatform/backend/app/copilot_governance/audit.py
06_AppPlatform/backend/app/copilot_governance/feedback.py
```

### 17.2 Audit Record

```python
class CopilotAuditRecord(BaseModel):
    audit_id: str
    created_at: str
    user_name: str | None = None
    country: str | None = None
    question: str
    intent_result: dict
    semantic_parse: dict
    source_plan: dict
    query_plans: list[dict] = []
    validation_reports: list[dict] = []
    result_verification: dict | None = None
    evidence_pack_id: str | None = None
    answer_mode: str
    provider: str | None = None
    model: str | None = None
    latency_ms: int | None = None
    token_usage: dict | None = None
    cost_rmb: float | None = None
    status: str
    error: str | None = None
```

### 17.3 Feedback

前端增加轻量反馈：

```text
👍 有用
👎 不准
⚠️ 数据不对
📊 图表不合适
🧾 证据不足
```

Feedback contract：

```python
class CopilotFeedback(BaseModel):
    audit_id: str
    rating: Literal["up", "down", "issue"]
    issue_type: str | None = None
    comment: str | None = None
    created_at: str
```

---

## 18. Eval Harness

### 18.1 目标

本系统必须有 eval harness，不然无法知道改动是否导致退化。

新增目录：

```text
06_AppPlatform/backend/evals/governed_copilot/
  cases.yaml
  run_eval.py
  metrics.py
  README.md
```

### 18.2 Eval Case

```yaml
- id: se_bev_share_bi
  question: "瑞典 2025 年 BEV 占比是多少？"
  country: "Sweden"
  expected_intent: "metric_query"
  expected_execution_mode: "structured_only"
  expected_sources:
    - jato_sales_parquet
  must_have_query_plan: true
  must_not_use_sources:
    - voc_forum_artifacts
    - news_digest
  expected_answer_mode: "kpi_answer"

- id: se_j7_strategy_hybrid
  question: "J7 SHS 在瑞典能不能卖？"
  country: "Sweden"
  expected_intent: "product_strategy"
  expected_execution_mode: "hybrid"
  expected_sources:
    - jato_sales_parquet
    - current_price_postgres
  optional_sources:
    - voc_forum_artifacts
    - policy_tax_digest
  expected_answer_mode: "strategy_brief"
  must_have_evidence_pack: true
  must_have_limitations: true

- id: cz_tucson_voc
  question: "捷克 Tucson HEV 用户主要抱怨什么？"
  country: "Czech Republic"
  expected_intent: "voc_insight"
  expected_execution_mode: "evidence_only"
  expected_sources:
    - voc_forum_artifacts
  must_not_have_query_plan: true
  expected_answer_mode: "comparison_table"
  must_have_voc_limitation: true

- id: sweden_malus_policy
  question: "瑞典 malus 125g CO2 怎么算？"
  country: "Sweden"
  expected_intent: "policy_tax"
  expected_execution_mode: "evidence_only"
  expected_sources:
    - policy_tax_digest
  must_have_formula: true
  must_have_effective_date_or_limitation: true
```

### 18.3 Metrics

```text
intent_accuracy
semantic_parse_accuracy
source_plan_accuracy
query_plan_validity
validator_block_rate
result_verifier_coverage
evidence_pack_completeness
answer_mode_accuracy
citation_or_evidence_presence
limitation_presence
no_fake_data_rate
latency
cost
fallback_safety
```

### 18.4 CLI

```bash
cd 06_AppPlatform/backend
python evals/governed_copilot/run_eval.py
```

---

## 19. Implementation Plan for Claude Code

## 实施状态快照（2026-05-12，最终版）

### 已完成 ✅

| Phase | 内容 | 状态 |
|-------|------|------|
| Phase 1 | Governance schemas (intent, source_plan, query_plan, evidence_pack, result_verifier) | ✅ |
| Phase 2 | Metadata Catalog: 5 dataset YAMLs + registry + 10-country policy tax YAMLs | ✅ |
| Phase 3 | Rule-based Source Planner with Chinese/English keywords + intent-based mapping (9 intents) | ✅ |
| Phase 4 | QueryPlan schema + SQL Validator (13 rules: whitelist, forbidden keywords, row limits, SELECT *) | ✅ |
| Phase 5 | Governed Query Engine adapter: 5 source types (jato_sales, msrp, voc, policy, news) | ✅ |
| Phase 6 | Result Verifier: 5 checks (row count, freshness, share sum, source coverage, empty) | ✅ |
| Phase 7 | Evidence Pack Builder from snapshot + source plan + tax estimate tables | ✅ |
| Phase 8 | Answer Composer: StructuredAnswer with 7 block types, rule-driven | ✅ |
| Phase 9 | Wired sourcePlan, evidencePack, governanceTrace, structuredAnswer, auditId into CountryChatResponse | ✅ |
| Phase 10 | Frontend CopilotGovernancePanel (collapsed, shows intent/sources/blocks) + StructuredAnswerView | ✅ |
| Phase 11 | Eval Harness: 39 cases, CLI runner, 100% pass | ✅ |
| Extra | 10-country tax calculators (SE/NO/FI/DK/DE/NL/HU/HR/AT/CZ) with EUR unification | ✅ |
| Extra | Semantic layer: entity resolution for powertrain, segment, metrics, business goals (中英文别名) | ✅ |
| Extra | Audit logging: in-memory ring buffer (200 records), auditId in every response | ✅ |
| Extra | User feedback: up/down/issue ratings contract | ✅ |
| Extra | Price Alert service: EUR-unified, MSRP repository integration, monthly payment estimates | ✅ |
| Extra | VOC data wiring: query_nordic_customer_deck(mode=forum_live) for qualitative evidence | ✅ |
| Extra | SSE streaming progress events (loading → cross-tabs → planning → generating) | ✅ |
| Extra | Token cost display (RMB, collapsed by default) | ✅ |

### 最终统计

| 指标 | 值 |
|------|-----|
| Unit tests | **528 passed** |
| Eval cases | **39/39 (100%)** |
| Governance files | **37** |
| Supported countries (tax) | **10** (Batch A full: SE/NO/FI/DK/HU/HR/AT/CZ + DE/NL) |
| Source lanes wired | **5/6** (structured_bi, canonical_entity, voc, policy_tax, news; live_web reserved) |
| PRD phases complete | **11/11** |

### 每个 Chat 响应包含的治理字段

```json
{
  "sourcePlan":        { "execution_mode": "hybrid", "items": [...] },
  "evidencePack":      { "sources": [...], "limitations": [...] },
  "governanceTrace":   { "intent": "...", "planGenerated": true },
  "structuredAnswer":  { "summary": "...", "blocks": [...], "recommendations": [...] },
  "auditId":           "a1b2c3d4"
}
```

### 完整治理栈文件树

```
copilot_governance/
  __init__.py
  intent.py              ← 14 intents + legacy mapping
  semantic_layer.py      ← entity resolution (中英文)
  source_plan.py         ← rule + intent Source Planner (10 rules)
  query_plan.py          ← QueryPlan schema
  sql_validator.py       ← 13 safety rules
  result_verifier.py     ← 5 quality checks
  evidence_pack.py       ← EvidencePack builder
  answer_composer.py     ← StructuredAnswer (7 block types)
  policy_service.py      ← policy query + search
  tax_calculator.py      ← 10-country CO2/weight/purchase tax
  price_alert_service.py ← EUR price alerts + monthly payments
  audit.py               ← audit logging (200 records)
  feedback.py            ← user feedback contract
  catalog/               ← 5 dataset YAMLs + 10 policy YAMLs
  engines/               ← Governed Query Engine adapter
```

---

## 20. Backend File-Level Task List

### 20.1 新增文件

```text
06_AppPlatform/backend/app/copilot_governance/__init__.py
06_AppPlatform/backend/app/copilot_governance/intent.py
06_AppPlatform/backend/app/copilot_governance/semantic_layer.py
06_AppPlatform/backend/app/copilot_governance/entity_resolution.py
06_AppPlatform/backend/app/copilot_governance/source_planner.py
06_AppPlatform/backend/app/copilot_governance/query_plan.py
06_AppPlatform/backend/app/copilot_governance/sql_validator.py
06_AppPlatform/backend/app/copilot_governance/result_verifier.py
06_AppPlatform/backend/app/copilot_governance/evidence_pack.py
06_AppPlatform/backend/app/copilot_governance/answer_composer.py
06_AppPlatform/backend/app/copilot_governance/audit.py
06_AppPlatform/backend/app/copilot_governance/feedback.py
06_AppPlatform/backend/app/copilot_governance/catalog/models.py
06_AppPlatform/backend/app/copilot_governance/catalog/registry.py
```

### 20.2 新增测试

```text
06_AppPlatform/backend/tests/unit/test_copilot_metadata_catalog.py
06_AppPlatform/backend/tests/unit/test_copilot_source_planner.py
06_AppPlatform/backend/tests/unit/test_copilot_query_plan.py
06_AppPlatform/backend/tests/unit/test_copilot_sql_validator.py
06_AppPlatform/backend/tests/unit/test_copilot_result_verifier.py
06_AppPlatform/backend/tests/unit/test_copilot_evidence_pack.py
```

### 20.3 修改现有文件

根据实际代码定位，可能修改：

```text
country_chat_service.py
country_chat schemas
assistant country router
```

要求：

```text
新增 optional fields
不删除旧字段
不改变旧 response 必填 contract
异常时 fallback
```

---

## 21. Frontend File-Level Task List

### 21.1 类型扩展

修改：

```text
06_AppPlatform/frontend/src/types/countryChat.ts
```

新增：

```ts
governanceTrace?: CountryCopilotGovernanceTrace | null;
sourcePlan?: CountryCopilotSourcePlan | null;
evidencePack?: CountryCopilotEvidencePack | null;
structuredAnswer?: CountryAnswerV1 | null;
```

新增：

```text
06_AppPlatform/frontend/src/types/copilotGovernance.ts
```

### 21.2 UI 新增组件

```text
06_AppPlatform/frontend/src/components/copilot-governance/
  CopilotGovernancePanel.tsx
  SourcePlanView.tsx
  EvidencePackView.tsx
  VerificationWarnings.tsx
  MetadataCoverageBadges.tsx
```

### 21.3 集成点

在 `CountryChatGroundedAnswer` 中增加：

```tsx
{message.evidencePack ? (
  <CopilotGovernancePanel
    sourcePlan={message.sourcePlan}
    evidencePack={message.evidencePack}
    governanceTrace={message.governanceTrace}
    compact={compact}
  />
) : null}
```

默认折叠，不干扰主回答。

---

## 22. Acceptance Criteria

### 22.1 P0 技术验收

1. 所有新增 schema 可 import。
2. Metadata catalog 可加载。
3. Source Planner 对 30 个问题输出合理 source plan。
4. SQL Validator 能阻断危险 query plan。
5. Result Verifier 能输出 pass / warning / fail。
6. Evidence Pack Builder 能把 governed results 转成 evidence pack。
7. `/v1/assistant/country/chat` 可返回 optional governance fields。
8. 前端旧页面不崩。
9. 旧 CountryChatGroundedAnswer fallback 仍可用。
10. Backend unit tests pass。

### 22.2 P0 产品验收

1. 纯 BI 问题只走 structured source。
2. VOC 问题不生成 SQL。
3. policy/tax 问题不错误查询 JATO Parquet。
4. strategy 问题生成 hybrid source plan。
5. 数据不足时 evidence pack 有 limitations。
6. Answer Composer 不编造 evidence pack 之外的具体数据。

### 22.3 P1 验收

1. QueryPlan 可以编译到受控 SQL / DuckDB / repository query。
2. Source Planner 可区分 required source 和 optional source。
3. Result Verifier 能检查 share sum / freshness / coverage。
4. EvidencePack 可在前端折叠展示。
5. Eval Harness 可运行并输出报告。

### 22.4 P2 验收

1. Audit log 可持久化。
2. Feedback 可提交。
3. Evaluation cases ≥ 100。
4. Structured Renderer 与 Evidence Pack 完整结合。
5. HTML / Markdown report 可以导出。

---

## 23. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| 过度架构化导致开发慢 | MVP 迟迟不能用 | 先做 contracts + adapter，不一次性重构 |
| LLM 生成 query plan 不稳定 | 查询错误 | MVP 先用规则 Source Planner，LLM 仅后续增强 |
| SQL 安全风险 | 高 | 不允许 raw SQL from LLM，只允许 QueryPlan 编译 |
| 非结构化 VOC 被误读为市场事实 | 高 | VOC governance 标记 qualitative_only |
| 政策过期 | 高 | policy lane 必须 freshness/effective date |
| 前端过度复杂 | 中 | governance panel 默认折叠 |
| 旧 Copilot 受影响 | 高 | optional fields + fallback |
| Eval 不覆盖真实问题 | 中 | 从用户历史高频问题建立 golden set |

---

## 24. Do / Don't for Claude Code

### Do

```text
Do read existing files first.
Do add optional fields.
Do keep fallback.
Do write unit tests.
Do implement deterministic rules first.
Do keep governance panel collapsed by default.
Do preserve existing page/widget behavior.
Do use existing contextSnapshot / grounding / renderHints where possible.
```

### Don't

```text
Do not rewrite CountryChatContext.
Do not delete CountryChatGroundedAnswer.
Do not delete CountryChatAnalysisDeck.
Do not make LLM output raw SQL.
Do not execute write SQL.
Do not start/stop/restart services without explicit approval.
Do not run docker compose up/down unless explicitly approved.
Do not change production config.
Do not replace crawler with LLM.
Do not make VOC claims look like market-wide statistics.
```

---

## 25. Recommended Implementation Prompt for Claude Code

下面这段可以直接发给 Claude Code：

```text
Implement the Governed Multi-Source Analytical Copilot PRD.

First inspect:
- Markdown_Readme/Fullstack/ROADMAP.md
- 06_AppPlatform/backend/README.md
- 06_AppPlatform/frontend/src/types/countryChat.ts
- 06_AppPlatform/frontend/src/contexts/CountryChatContext.tsx
- 06_AppPlatform/frontend/src/pages/CountryChatPage.tsx
- 06_AppPlatform/frontend/src/components/CountryChatGroundedAnswer.tsx
- 06_AppPlatform/frontend/src/components/CountryChatAnalysisDeck.tsx
- 06_AppPlatform/frontend/src/api/client.ts
- backend country chat service/router files

Do not rewrite the existing Copilot.
Do not delete existing components.
Do not change required response fields.
Add optional governance fields only.
Keep fallback to existing grounding rendering.
Do not start/stop/restart services unless explicitly approved.

Implement in phases:
1. Add backend copilot_governance schemas and catalog registry.
2. Add metadata catalog YAML files.
3. Add rule-based source planner.
4. Add QueryPlan schema and SQL validator.
5. Add ResultVerifier.
6. Add EvidencePack builder.
7. Add optional fields to CountryChatResponse.
8. Add frontend governance types and collapsed governance panel.
9. Add unit tests.
10. Add eval harness skeleton.

MVP should compile and pass existing tests.
```

---

## 26. 最终总结

本 PRD 的核心判断是：

```text
你的系统不是 BI chatbot。
你的系统是汽车市场多源证据 Copilot。
```

所以合理顺序不是：

```text
NL → SQL → Parquet → Answer
```

而是：

```text
NL → Intent → Semantic Layer → Metadata Catalog → Source Plan
→ Query / Tool / Evidence Plan → Validator → Governed Engine
→ Result Verifier → Evidence Pack → Answer → Render → Audit / Feedback
```

渲染型 Markdown / HTML 仍然重要，但它是最后一层。  
真正决定系统专业性的，是中间这四个模块：

```text
Metadata Catalog
Source Planner
Result Verifier
Evidence Pack Builder
```

这四个模块建起来以后，Country Copilot 才能从“能写报告的聊天助手”升级为真正支撑：

```text
市场分析
车型定位
定价策略
竞品比较
MSRP 对账
配置差异
VOC 洞察
政策 / 碳税解释
新闻追踪
管理层汇报
```

的 **Governed Multi-Source Analytical Copilot**。
