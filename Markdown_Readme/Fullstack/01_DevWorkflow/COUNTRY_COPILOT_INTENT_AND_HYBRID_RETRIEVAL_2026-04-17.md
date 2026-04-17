# Country Copilot：Intent Routing + Tiered Retrieval（2026-04-17）

状态：Draft（Q3 + Q4 合写；supersedes 根目录 `country_assistant_upgrade_plan.md`）

关联：
- [PRODUCT_DEEPDIVE_2026-04-17.md](../PRODUCT_DEEPDIVE_2026-04-17.md) § Q3+Q4
- [COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md](./COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md)

---

## 1. 现状诊断

Country Copilot 目前存在两个真实痛点：

1. **一股脑扔**：前端 `CountryChatAnalysisDeck` 一次渲染 5–10 张图表，后端 `country_chat_service` 预加载所有 snapshot + wiki + 榜单。不论问什么，UI 都塞一套卡片。
2. **时效性差**：news 抓取的 RSS 数据还没持久化到 PG（Phase 3 Step 3.3 未完成），Copilot 检索不到 2026-04 的新闻；也没有"联网兜底"通道。

**根因**不是 LLM 不够聪明，而是架构是"先倒出所有 context → 让 LLM 收口"，而不是"先分类意图 → 精确取数 → LLM 组装"。

## 2. 目标架构

```
┌──────────────────────────────────────────────────────────────────┐
│                        用户 query                                │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 1. Intent Router（一次小 LLM 调用，< 300 ms）              │
│  → intent: precise_lookup | market_trend | spec_compare          │
│            | policy_regulation | news_query | chitchat           │
│  → suggested_tools: [tool_name, ...]                             │
│  → required_entities: {country?, model?, trim?, topic?, since?}  │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 2. Tool Execution（并行，Tiered Retrieval）                │
│                                                                  │
│  Tier 0  PG/Parquet canonical     → <50 ms                       │
│  Tier 1  Chroma local RAG         → <300 ms                      │
│  Tier 2  fresh_web_lookup (live)  → 1–5 s，按需触发              │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 3. Composer LLM（主推理，基于工具结果）                    │
│  → markdown narrative                                            │
│  → render_hints[]（前端按需渲染表格/图表）                       │
└──────────────┬───────────────────────────────────────────────────┘
               ▼
┌──────────────────────────────────────────────────────────────────┐
│  Step 4. 前端按 render_hints 精准渲染（不再预渲染全部卡片）     │
└──────────────────────────────────────────────────────────────────┘
```

## 3. Intent 分类表

| Intent | 典型 query | 工具白名单 | 默认 render |
|--------|-----------|-----------|------------|
| `precise_lookup` | "瑞典 RAV4 Hybrid 2026 各版型价格" | current_msrp_lookup, trim_feature_compare | 表格 |
| `market_trend` | "挪威 BEV 过去 24 个月销量走势" | sales_trend, segment_share | 折线图 + stat card |
| `spec_compare` | "XC60 B5 vs B6 差在哪" | trim_feature_compare, current_msrp_lookup | diff matrix 表 |
| `policy_regulation` | "2026 瑞典 Malus/Bonus 有什么变化" | country_policy_digest, fresh_web_lookup | 文字 + 引用卡 |
| `news_query` | "最近有什么关于碳税的新闻" | country_news_search, fresh_web_lookup | 卡片列表 |
| `chitchat` | "你好 / 你能做什么" | — | 纯文字 |

分类实现：小模型（Qwen-2.5 / gemini-flash）+ JSON 输出模式，系统 prompt 中列出 intent 定义和少量 few-shot 例子。

## 4. Tool 契约（强制规则）

每个 tool = 现有 service 的只读入口。**不允许在 tool 内写新的查询逻辑**。

```python
# app/copilot/tools/current_msrp_lookup.py
from pydantic import BaseModel
from app.services.compositions import msrp_enriched

class CurrentMsrpLookupIn(BaseModel):
    country: str
    brand: str | None = None
    model: str | None = None
    powertrain: str | None = None     # 'HEV' | 'PHEV' | ...
    model_year: int | None = None

class CurrentMsrpLookupOut(BaseModel):
    trims: list[dict]                 # ≤ 20 条，已排序
    source_summary: dict              # {tier1: n, tier2: n, ...}
    freshness_days_max: int

def run(req: CurrentMsrpLookupIn) -> CurrentMsrpLookupOut:
    return msrp_enriched.lookup_current_msrp(**req.model_dump())
```

**硬约束**：
- 返回 payload ≤ 2 KB；超出要分页或裁剪。
- schema 字段名与前端已消费字段对齐（`FULLSTACK_DEVELOPMENT_SPEC`）。
- 任何 tool 必须有单测（`tests/copilot/test_tools_*.py`）。

## 5. Tiered Retrieval 实现

### 5.1 Tier 0：canonical
直接走 `compositions/`（Q1 方案里建好的）。tool 返回纯结构化数据。

### 5.2 Tier 1：local RAG
- 向量库：继续用 `04_Processed_data/chroma_db/`（已在用）。
- 索引内容：
  - `spec_sheet`：每个 trim 一个 doc，文字化的 features + price。
  - `country_profile`：每个国家一个 doc，policy / incentive 要点。
  - `news_article`：PG `news_article` 增量同步（Q6 unified pipeline 产物）。
  - `policy_snippet`：PG `policy_snippet` 增量同步。
- 检索：mmr + metadata filter（country, kind, date_range）。

### 5.3 Tier 2：fresh_web_lookup（新）
- 实现：Firecrawl `search` + `scrape` 组合；或 Bing grounding API。
- **触发条件**（由 Composer LLM 决定，不是硬规则）：
  - T0/T1 返回置信 < 0.6
  - query 含 freshness 信号（"最新" / "latest" / 年份 ≥ 当前年）
  - 用户显式说 "联网查"
- 结果必须归一化后**写回 T0/T1**：
  - news 结果 → `news_article` 表 + Chroma upsert
  - policy 结果 → `policy_snippet` 表 + Chroma upsert
- domain allowlist（避免乱抓）：每个国家有一份白名单，存 `07_ScrapingToolkit/news_sources/*.yaml`。

### 5.4 成本与安全
- fresh_web_lookup 每次调用计费，设日度 quota（`copilot_quota_config`）。
- 所有 T2 返回的外链都必须在 allowlist 域名内；否则丢弃。
- 不执行任何 T2 返回的 JS / 不写磁盘可执行文件。

## 6. Composer LLM 输出契约

```json
{
  "markdown": "### 瑞典 RAV4 HEV 2026 款主要版型\n...",
  "citations": [
    {"source": "msrp_trim_price", "trim_id": 12345, "observed_at": "2026-03-28"},
    {"source": "country_policy_digest", "country": "SE"}
  ],
  "render_hints": [
    {"kind": "trim_table", "title": "瑞典 HEV 版型价格", "trim_ids": [12345, 12346, 12347]},
    {"kind": "bar_chart", "metric": "base_msrp_eur", "group_by": "trim_name"},
    {"kind": "stat_card", "label": "瑞典 HEV 销量占比", "value": "42%"}
  ],
  "followups": [
    "对比 2025 款价格",
    "推荐 Tier 1 经销商"
  ]
}
```

**前端硬规则**（纳入 UI_SPECIFICATION_V1）：
- 只渲染 `render_hints` 列表中的卡片，不再拉取"全量 country deck"。
- 每个 hint kind 对应唯一组件 + `LazyPlotlyChart` 渲染路径。
- 每张卡必须显示 citation 的 observed_at / source tier（可折叠）。

## 7. News 时效性解决（Phase 3 Step 3.3 收尾）

1. **新增 PG 表**（Alembic 0011）：
```sql
CREATE TABLE news_batch_run (
    run_id       BIGSERIAL PRIMARY KEY,
    started_at   TIMESTAMPTZ NOT NULL,
    finished_at  TIMESTAMPTZ,
    batch_key    VARCHAR(32) NOT NULL,    -- 'batch_a' | 'batch_b'
    status       VARCHAR(16) NOT NULL,    -- 'running' | 'ok' | 'failed'
    articles_ok  INT NOT NULL DEFAULT 0,
    articles_new INT NOT NULL DEFAULT 0
);
CREATE TABLE news_article (
    article_id    BIGSERIAL PRIMARY KEY,
    country_code  VARCHAR(4) NOT NULL,
    source_domain VARCHAR(128) NOT NULL,
    url           TEXT NOT NULL UNIQUE,
    title         TEXT NOT NULL,
    summary       TEXT,
    body_text     TEXT,
    published_at  TIMESTAMPTZ NOT NULL,
    fetched_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    topics        VARCHAR(32)[],           -- ['carbon_tax','bev_subsidy']
    lang          VARCHAR(8),
    run_id        BIGINT REFERENCES news_batch_run(run_id)
);
CREATE INDEX ix_news_country_time ON news_article (country_code, published_at DESC);
CREATE INDEX ix_news_topics_gin ON news_article USING GIN (topics);
```

2. **调度**：systemd timer `jato-news-sync.timer` 每 6 小时跑 `07_ScrapingToolkit/run_news.py --sink pg`。
3. **Chroma 同步**：`sync_news_to_chroma.py` 每晚 03:00 跑增量 upsert（基于 `fetched_at`）。
4. **Copilot tool `country_news_search`** 直接查 PG + Chroma 组合。

## 8. 迁移步骤

| W | 动作 |
|---|------|
| W1 | Intent Router 小 LLM 调用 + 意图分类（单元测试 ≥ 20 条） |
| W1 | `app/copilot/tools/` 骨架 + 前端 render_hints 契约 |
| W1 | 把现有 `CountryChatAnalysisDeck` 改为按 hints 渲染，保留旧全量路径做 A/B |
| W2 | 3 个核心 tool（current_msrp_lookup / sales_trend / country_policy_digest）上线 |
| W2 | Alembic 0011 news_article 落库 + news_runner sink=pg |
| W3 | Chroma 增量同步 + `country_news_search` tool |
| W3 | `fresh_web_lookup` tool（Firecrawl）+ quota + allowlist |
| W4 | `trim_feature_compare` tool（依赖 Q2 的 trim_feature_catalog） |

## 9. 验收

- Intent 分类 P1 ≥ 0.85（基于 50 条人工标注的 query）。
- 端到端 latency（precise_lookup）P95 ≤ 1.5 s（不触发 T2 时）。
- 一次对话的 render_hints 平均 ≤ 3 张，不再出现 5–10 张卡一股脑。
- news 最新时间 T-24h 内有 ≥ 80% 国家。
- T2 触发率 ≤ 15%（即 85% 的 query 在本地闭环解决）。

## 10. 回滚

- 在 `country_chat_service` 保留 `USE_INTENT_ROUTER` feature flag，出问题直接关掉回旧链路。
- 前端 `CountryChatAnalysisDeck` 保留"fallback 全量渲染"路径 30 天。

## 11. 废弃标记
根目录 `Markdown_Readme/country_assistant_upgrade_plan.md` 在本文档落地后改为 History 状态，内容被本文 + `COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md` 共同承接。
