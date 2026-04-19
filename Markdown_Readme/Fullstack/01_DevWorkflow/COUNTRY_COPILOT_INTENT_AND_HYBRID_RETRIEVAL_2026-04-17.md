# Country Copilot：Intent Routing + Tiered Retrieval（2026-04-17）

状态：Active（Q3 + Q4 合写；已落地 direct-answer / grounding 主链路，Live/news sink 仍未闭环）

关联：
- [PRODUCT_DEEPDIVE_2026-04-17.md](../PRODUCT_DEEPDIVE_2026-04-17.md) § Q3+Q4
- [COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md](./COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md)

---

## 0. 2026-04-18 实现快照

这份文档最初写的是目标架构；截至 2026-04-18，下面这些能力已经进入代码主链路：

1. **窄问题 direct-answer 路由**：`positioning-focus`、`segment-fuel-focus`、`precise-lookup` 已经在 `country_chat_service.py` 中生效，不再所有问题都回退到宽泛 market overview。
2. **grounded answer payload**：后端现在统一返回 `answerMode + grounding + contextSnapshot`，前端 page / widget 先显示答案，再显示证据层。
3. **related news 变成相关证据层**：news 不再默认整块铺开，而是按当前问题相关性进入 grounding。
4. **current MSRP 直查**：`msrp_lookup_service.py` 会从 `CurrentPrice` 直接产出 trim/price/source-tier 表。
5. **version / trim diff 直查**：`engineering_variant_diff_service.py` 基于 engineering normalized variants 输出 `variantDiff`，国家助手可以直接回答“配置差异”。
6. **多轮追问继承**：segment / powertrain / route 会沿会话 history 继承，`SUV-B 里卖得最好的 PHEV` 这类追问不再直接掉回国家总览。

仍然**没有完全落地**的部分：

- 通用 tool registry（文档里的 `app/copilot/tools/*` 还是目标形态，不是当前 repo 结构）。
- T2 `fresh_web_lookup` 联网写回闭环。
- news / policy 的稳定 PG sink 与向量同步闭环。

因此，下面各章节应理解为：**前半已经部分实现，后半仍是后续目标态**。

## 1. 最初诊断（已部分修复）

这份设计起草时，Country Copilot 存在两个核心痛点：

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

## 12. 2026-04-18 补充：Local + Live 的标准回答栈

围绕用户现在最关心的"**JATO 销量 + 本地 MSRP/trim/spec + 最新 news/policy** 怎么同时服务国家助手"，统一成下面这套约束：

| 层 | 角色 | 典型数据 | 是否默认进入回答 |
|---|---|---|---|
| **Snapshot** | 市场底图 | JATO sales 聚合、segment/brand/model 大盘、已审核本地 wiki 摘要 | 是，但只取回答当前问题所需的最小切片 |
| **Dynamic** | 这次问题的答案 | `CurrentPrice`、`JatoMsrpLink`、engineering normalized variants（`ConfigBaseVariant` + `ConfigMarketFeatureOverride`）、`variantDiff`、当前国家的 trim/version 表 | 是，**版型/价格/配置问题必须优先命中这里** |
| **Live** | 时效性补充 / 缺口补足 | 相关 news/policy evidence、后续规划中的 Firecrawl / 搜索兜底结果 | 否；仅当用户问最新、法规、新闻、或本地缺失时才触发 |
| **LLM** | 组织表达 | 结论、表格标题、风险提示、follow-up 建议 | 最后一步，不能替代取数 |

### 12.1 回答硬规则

1. **先答问题，再补背景**：如问"瑞典 2026 款 RAV4 / Sportage / Kona HEV 的具体版型和价格"，默认先返回 trim/price 表，而不是先吐国家快照。
2. **图表按 render hint 精准拉取**：只有用户问销量结构、segment 对位、价格带时才带对应图表，不再默认上 5–10 张。
3. **news / policy 只作为相关证据层**：例如回答 HEV trim 价格时，只有当税制/补贴会直接影响结论时，才补 Malus/Bonus 一条 evidence card。
4. **联网是增量，不是主路径**：本地 PG / Parquet / review 审核后的结果能回答，就不要触发 live web。

### 12.2 典型 query 应怎么走

以 `"瑞典市场，2026 款的 RAV4 Hybrid、Sportage HEV、Kona Hybrid 的具体版型和价格呢？"` 为例：

1. Router 抽出 `country=SE`、`models=[RAV4, Sportage, Kona]`、`powertrain=HEV`、`model_year=2026`、intent=`precise_lookup`。
2. Tool 只打 `current_msrp_lookup` + `trim_feature_compare` +（可选）`sales_rank_lookup`。
3. Composer 先出一个 trim table，再补一句销量/税制背景；如果本地 `model_year=2026` 不全，再触发 live lookup 去官网或 allowlist 第三方补缺。
4. 前端只渲染 trim 表、价格对比条形图、最多一张"相关政策"证据卡。

## 13. 和 Google AI 模式相比，目前还缺什么

Google AI Overview 看起来像"模型更聪明"，但真正差距主要在**取数组织**而不是模型参数量：

1. **实体级 canonical data 还不完整**：现在国家助手已经能做 segment/length、current MSRP、variant diff 直答，但还缺更统一的 `country + model + trim + year + powertrain` 查询面和 sales-truth 对位层。
2. **工具契约还不够窄**：已经有 `msrp_lookup_service` 和 `engineering_variant_diff_service` 这样的 deterministic 服务，但还没有收束成统一 tool registry。
3. **时效数据尚未形成闭环**：news 相关 evidence 已能进入 grounding，但稳定 sink 到 PG + vector store 仍未完成，所以最新性仍受限。
4. **citations / freshness 还不够显式**：grounding 已把证据层拆出来，但 `source tier / observed_at / official vs third-party` 仍需继续强化。
5. **前端仍有部分 route 级而非 entity 级渲染**：route-aware 已收紧很多，但还需要继续从“路由收口”升级到“实体查询结果驱动渲染”。

## 14. 是否需要换模型

**结论：现阶段不应把换模型当主解。**

先做好的顺序应该是：

1. **先补 canonical tool 面**：继续把已经落地的价格 lookup、variant diff、segment-fuel direct path，收束成更统一的 deterministic tool contract。
2. **再做 router / composer 分离**：router 用小模型或 JSON mode（Gemini Flash / Qwen 2.5 级别即可），composer 继续用现有回答模型。
3. **最后才评估主回答模型升级**：只有当多语种表述、长上下文压缩或 citation 整理依旧不稳时，才考虑换更强的 answer model。

换句话说，用户现在感受到的"Google AI 更像在精准查询"并不是因为它默认用了更大的模型，而是因为它的 retrieval contract 更窄、source hierarchy 更清晰、UI 只渲染和问题直接相关的结果。
