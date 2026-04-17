# JATO 平台：六问答卷（2026-04-17）

状态：Draft（主答卷，供讨论与落地）
关联：[ROADMAP.md](./ROADMAP.md) · [ARCHITECTURE_REVIEW_2026-04-17.md](./ARCHITECTURE_REVIEW_2026-04-17.md)

> 定位：这是针对你 2026-04-17 提出的六个核心难点的系统性答卷。每一问给出"结论 + 架构方案 + 动作清单 + 对应专题文档链接"。细节全部落到专题 md，本文只做总览与导航。

---

## 概览：六个问题一张图

```
  ┌─────────────────────────────────────────────────────────────────────┐
  │                  JATO Fullstack 下一个 3 个月的主战场                │
  └─────────────────────────────────────────────────────────────────────┘

  Q1 跨源 join       Q2 抓取 + version matrix   Q5 多源对账
  JATO × MSRP         MSRP pipeline 优化          官网 vs 第三方
  ──────────────      ──────────────────────      ─────────────
  不扔 Parquet。       LLM 辅助选择器 + Firecrawl    保留多源，
  DuckDB ATTACH PG,   fallback + review 置信分级。  tier 优先级 +
  夜间产出 positioning  PG 上建 version/feature      reconciliation
  matrix parquet。    matrix，支持差异对比。         选值 + 差异可视。
                                                      (与 Q2 同一 PG 设计)

  Q3+Q4 Copilot 精准化与本地×联网
  ─────────────────────────────
  真正的 Function Calling：先分类再查询。
  Tiered retrieval：Canonical → Local RAG → Live Web。
  前端按 render_hints 渲染，不再一股脑。

  Q6 统一抓取流水线
  ────────────────
  三层抽象：Fetcher(scrapling/playwright/firecrawl/crawlee)
          → Extractor(css/schema_org/pdf/llm)
          → Normalizer(currency/unit/schema/sink)
  按 domain 分 kind：msrp / news / policy / incentive / spec
```

---

## Q1. Sales 来自 JATO（Parquet），MSRP 来自 PG，join 时 Parquet 还能用吗？

### 结论
**能用，不扔。** Parquet 仍是 sales 读层。跨源 join 用两条并行路径解决：线上请求走 DuckDB ATTACH Postgres 做 federated query；高频 dashboard 走夜间预聚合产出的 positioning matrix parquet。

### 为什么不迁 JATO 进 PG
JATO 90 万行 × 月度追加是典型读多写少；PG 16 GB 内存下做全量分析 join 会被 sort/hash 打爆。**分析 vs 业务**的边界已经在 [`03_Database/PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md`](./03_Database/PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md) 定下来，这条结论不动。

### 方案三段式
1. **JATO × MSRP 链接表**（PG 新表 `jato_msrp_link`）：把 JATO 的 `(country, brand, model, powertrain)` 与 MSRP 的 `(trim_id)` 建立多对多映射，由 MSRP 侧的 `model_rules` / override 回流维护。
2. **在线 federated 查询**：DuckDB `ATTACH postgres_scanner` + 读 partition parquet，一次 SQL 完成 `sales_12m LEFT JOIN current_msrp ON link`。延迟目标 ≤ 400 ms（单国家）。
3. **离线 positioning matrix**：refresh job 的最后一步产出 `04_Processed_data/positioning/positioning_matrix.parquet`（country × model × trim × last_12m_sales × current_msrp_eur × tier）。Dashboard 的 Positioning/Pricing 页直接读，零 join。

### 专题文档
[`03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md`](./03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md)

---

## Q2. MSRP 抓取能怎么优化？以及"同一款车多 version / 多动总 / 配置差异"怎么落库？

### 结论
抓取侧走**置信分级 + LLM 辅助选择器 + Firecrawl 兜底**三板斧，把目前 44% 的 dry-run 通过率逐步推到 70%+。配置差异侧建**三张 PG 表**（trim_feature_catalog / feature_dictionary / trim_feature_diff_view），Parquet 做不了这件事，必须是 PG。

### 抓取优化 5 条（按优先级）
1. **AggregateOffer / Schema.org 优先扫一遍**（已在 Scrapling 做过，继续扩到所有新 source）。
2. **LLM 辅助 selector 草稿**：给一个新 URL，让小模型（本地或云端）先给 CSS 草稿 → 再人工 diff。从 0 到 60% 的工作让 LLM 做。
3. **Firecrawl extract API 作为 fallback**：JSON schema 声明要的字段，Firecrawl 直接返回结构化 JSON。用于"静态网页但 DOM 太乱"的第三方媒体。
4. **PDF price sheet 抽取**：pdfplumber + camelot，官网发布季度 price list PDF 的品牌（BMW/Mercedes）走这条。
5. **Review 置信分级**：每条 observation 计算 `confidence_score`（CSS 命中 + 字段完整度 + 与历史价格偏差）：
   - ≥ 0.90 自动通过
   - 0.60–0.90 进 review 队列
   - < 0.60 直接 drop + flag

### 配置差异表（Version / Feature Matrix）落库设计
```
msrp_trim                 (trim_id PK, brand, model, local_name, country, model_year, ...)
msrp_trim_price           (trim_id FK, currency, base_msrp, base_msrp_eur, effective_from, effective_to)
trim_feature_catalog      (trim_id FK, feature_key, feature_kind:'standard'|'optional'|'package', option_price, source_id)
feature_dictionary        (feature_key PK, zh_name, en_name, category, is_premium)
trim_feature_diff_view    (materialized view：两两 trim 差集)
```

前端 `SpecificationPage` / `VersionComparisonPage` 已有骨架，扩展成：选 A / B 两个 trim → 显示"A 有 B 无 / B 有 A 无 / 两者共有但值不同"三栏。

### 专题文档
[`MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md`](./MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md)（与 Q5 合写，同一张 PG 设计）

---

## Q3 + Q4. Copilot 为什么"一股脑扔"？怎么做到 Google AI 那种精准回答 + 本地和联网混合？

### 根因
当前 `country_chat_service` 是"**先取所有数据，再让 LLM 收口**"——意图识别→加载 country snapshot + local wiki + 销量榜 + MSRP + news 全部塞进 prompt。这不是 Function Calling，是"context 倾倒"。LLM 再聪明也只能照着倾倒物编故事。

### 目标架构：真·Function Calling + 分层检索

**分层 1：意图分类（router，便宜快）**
- 用一次 LLM 把 query 分到：`precise_lookup` / `market_trend` / `spec_compare` / `policy_regulation` / `news_query` / `chitchat`。
- 每个意图对应一组工具白名单，绝不互串。

**分层 2：工具白名单（每个 tool = 现有 service 的只读入口）**
```
tool_current_msrp_lookup(country, brand, model)
tool_trim_feature_compare(trim_ids[])
tool_sales_trend(country, model, window)
tool_country_policy_digest(country, topics[])
tool_country_news_search(country, query, since_date)
tool_fresh_web_lookup(query, domain_allowlist, freshness_days)   ← 联网兜底
```
每个 tool 返回**精简结构化 JSON**（≤ 2 KB），不返回叙述文本。

**分层 3：Tiered retrieval（本地 × 联网）**
| Tier | 延迟 | 来源 | 何时命中 |
|------|------|------|---------|
| T0 | <50 ms | PG / Parquet canonical | 结构化查询（价格、销量、配置） |
| T1 | <300 ms | Chroma local RAG（spec sheets、news cache、policy digest） | 半结构化问答 |
| T2 | 1–5 s | Firecrawl / Bing grounding `fresh_web_lookup` | T0/T1 置信 < 阈值 **或** query 含"最新 / latest / 2026 新政"关键字 |

**回填机制**：T2 的返回必须归一化后写回 PG `news_article` / `policy_snippet` + upsert Chroma，下次同类 query 直接 T1 命中。

**分层 4：输出端 render hints**
LLM 回复不再是 markdown+已渲染的所有卡片。后端返回：
```json
{
  "markdown": "...",
  "render_hints": [
    {"kind": "table", "title": "瑞典 HEV 版型价格对比", "data": {...}},
    {"kind": "chart", "kind_detail": "bar_price_by_trim", "data": {...}}
  ]
}
```
前端按 hints 渲染需要的那一两个图表，不预渲染全部卡片。

### "RAV4 / Sportage / Kona HEV 瑞典 2026 价格"这种 query 的真实执行路径
1. Router → `precise_lookup`
2. `tool_current_msrp_lookup(country=SE, models=[RAV4, Sportage, Kona], powertrain=HEV)` → 返回 3 组 trim+price（T0）
3. `tool_country_policy_digest(country=SE, topics=[Malus, Bonus])` → 返回瑞典车税要点（T1）
4. 若 MSRP 缺 2026 版本 → 触发 `tool_fresh_web_lookup(..., freshness_days=30)` 取最新官网（T2），回填 T0/T1
5. LLM 组装成 3 张 trim 表 + 税制说明 + 1 张价格对比条形图的 render hints

### 专题文档
[`01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md`](./01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md)

### News 时效性（2025 年底 vs 2026-04 最新）
根因有二：(1) `news_runner` 的产物还没落库（`COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md` Phase 3 Step 3.3 未完成）；(2) 没有 "fresh_web_lookup" 兜底。解决路径在上面的 T1 回填 + T2 兜底，加一个 systemd timer 每 6 小时跑 `news_runner`。

---

## Q5. 官网和第三方都给 MSRP，怎么处理？要不要保留多个数据源？

### 结论
**保留多源，永远不覆盖历史观测值。** 上层暴露的 `current_msrp` 是经过 reconciliation 的"当选值"，观测层保留全部来源供追溯、对比、置信度计算。

### Source Tier 定义
| Tier | 类别 | 典型例子 | 置信度 |
|------|------|---------|--------|
| 1 | 品牌官网 / 官方购车页 | volvocars.com/se | 0.95 |
| 2 | 官方经销商网 | volvoforhandlare.se | 0.85 |
| 3 | 权威汽车媒体 | vibilagare.se | 0.65 |
| 4 | 其他第三方 / 比价站 | - | 0.40 |
| 5 | 租赁月供页（非 MSRP） | 大量 offers 页 | 不采 |

### Reconciliation 规则
给定同一 `(country, trim_id, price_period)`：
1. 如果存在 Tier 1 且新鲜度 ≤ 30 天 → 直接采用 Tier 1。
2. 如果只有 Tier 2+，且各源差异 ≤ 3% → 取加权中位数，flag `reconciled`。
3. 如果差异 > 3% → 进入 review 队列，不自动发布。
4. 历史 observation 永不修改，所有 reconciliation 决策写 `reconciliation_log`。

### 前端呈现
MSRP detail drawer 增加"Sources (n)"折叠项：并排显示每个来源的价格 + tier + 观测日期，差异 > 3% 时红色高亮。

### 专题文档
[`MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md`](./MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md)

---

## Q6. 金融 / 碳税 / 补贴 / 新闻 / 政策大抓取：怎么统一？

### 结论
把 `07_ScrapingToolkit` 升级成**"统一抓取平台"**：Scrapling + Playwright + Firecrawl + Crawlee 各司其职，靠一层统一抽象（`ScrapeJob` 契约）拼在一起，落到 PG 分 kind 的 observation 表。

### 工具职责划分（不重复造轮子）

| 工具 | 主战场 | 放进哪层 |
|------|--------|---------|
| **Scrapling**（已用） | 规则化 CSS 结构抽取，品牌官网 MSRP 主力 | Extractor |
| **Playwright**（已用） | 重 JS 渲染官网（Volvo / VW / Audi） | Fetcher |
| **Firecrawl** | 长尾站点 / PDF / LLM extract / 政策法规原文 | Fetcher + Extractor |
| **Crawlee** | 队列驱动大规模 crawl，如全站 news / 全站 spec | Fetcher |
| **httpx** | 简单静态 HTML / RSS / JSON API | Fetcher |

### 三层架构
```
┌────────────────────────────────────────────────────────────┐
│              ScrapeJob (queue, PG table)                   │
│   kind: msrp | news | policy | incentive | spec            │
│   url, extractor_kind, schema_ref, freshness_policy        │
└────────────┬───────────────────────────────────────────────┘
             ▼
┌────────────────────────────────────────────────────────────┐
│  Fetcher layer                                             │
│   httpx / playwright / firecrawl / crawlee                 │
│   → RawDocument (html | pdf | json | rss)                  │
└────────────┬───────────────────────────────────────────────┘
             ▼
┌────────────────────────────────────────────────────────────┐
│  Extractor layer                                           │
│   css_rules | schema_org | pdf_parser | llm_extract | rss  │
│   → StructuredObservation (pydantic model per kind)        │
└────────────┬───────────────────────────────────────────────┘
             ▼
┌────────────────────────────────────────────────────────────┐
│  Normalizer + Sink layer                                   │
│   currency / unit / language / dedup → PG observation      │
│   msrp_observation | news_article | policy_snippet | ...   │
└────────────────────────────────────────────────────────────┘
```

### 调度（仍旧 systemd timer，不上 Airflow）
| kind | 节奏 | 理由 |
|------|------|------|
| msrp | 每日夜 01:00–03:00 | 低优先；官方价格更新慢 |
| news | 每 6 小时 | 保证国家助手能答"最近" |
| policy | 每周一 04:00 | 变更频率低 |
| incentive | 每周三 04:00 | 同上 |
| spec | 按 MSRP 触发（有新 trim 才抓） | 避免空跑 |

### 专题文档
[`02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md`](./02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md)

---

## 现有 MD 的归类结论

看完全部 MD 后：**现有结构基本好，不用推翻重来**。唯一建议：

1. `country_assistant_upgrade_plan.md` 目前**独立放在 `Markdown_Readme/` 根目录**，与其它文档分层错位 → 已在 Q3/Q4 专题中 superseded，建议移入 `Fullstack/_archived/` 作为历史规划留档。
2. `Streamlit/` 目录继续冻结，新增文档一律写进 `Fullstack/`。
3. 本答卷产出的 5 份新 md 均登记入 `ROADMAP.md § 4.1`。
4. MSRP 抓取 / 配置差异 / 多源对账属于同一 PG schema 演进，合并写进同一份 md（Q2+Q5），避免再拆。
5. Copilot 精准化 / 本地联网混合属于同一执行链路（router → tools → retrieval → render hints），合并成一份（Q3+Q4）。

---

## 建议落地顺序（按"最快能让用户感知"排序）

| 周 | 动作 | 专题 |
|----|------|------|
| W1 | Q3/Q4 Router + render_hints 最小实现（意图分类即可上线，tools 分期补） | Copilot |
| W1 | Q1 DuckDB ATTACH PG POC（单国家 positioning 页改造） | 跨源 join |
| W2 | Q5 reconciliation 表 + tier 字段 Alembic 0007 | MSRP |
| W2–3 | Q6 ScrapeJob 抽象 + news 落库（解决时效性） | 统一抓取 |
| W3–4 | Q2 trim_feature_catalog 表 + version diff 前端 | MSRP 配置差异 |
| W4+ | Firecrawl / Crawlee 接入；policy / incentive kind 开跑 | 统一抓取 |

---

## 下一步

这份答卷只做方向、不做实现。每一节指向的专题文档都会给：
- 最小数据模型（Pydantic / SQLAlchemy / Alembic）
- 关键函数签名
- 测试点与验收样例
- 回滚路径

如果你认可整体方向，就逐份评审专题文档，确认后我按 W1 → W4 的顺序开工。
