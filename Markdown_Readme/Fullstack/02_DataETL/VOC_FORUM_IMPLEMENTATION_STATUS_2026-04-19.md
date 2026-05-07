# VOC Forum Pipeline 研究结论、已完成项与下一步（2026-04-19）

状态：Active（research 已沉淀进 repo；raw/enriched/deck 主链路已跑通；enrichment 已补 taxonomy-driven 自动分析层；Customer Insights 已补 benchmark / forum_live 双模式；Data Management 已补 VOC 观察台）

## 1. 这份文档解决什么问题

这份文档把此前散落在 session research、`VOC_FORUM_SCRAPING_FEASIBILITY_2026-04-17.md`、`07_ScrapingToolkit/README.md` 和代码实现里的 VOC 信息合并成一份正式仓库文档，回答四件事：

1. 论坛 VOC 在这个仓库里最合理的目标是什么。
2. 当前代码已经做到哪一步。
3. 哪些事情已经验证过，哪些还没做。
4. 后续应该按什么顺序继续推进。

## 2. 研究结论：最好的方向不是“论坛直接复刻 Excel”

### 2.1 最优目标

当前最优解不是把公开论坛直接爬成 `VOC_Nordic_SUV_Users_100.xlsx` 那种“问卷式样本表”，而是明确拆成三层：

1. **Raw VOC**：公开页面抓取后的可审计原始记录。
2. **Enriched observations**：围绕证据句、信号命中、主题和情绪做结构化归因。
3. **Insight deck**：只聚合 forum 可直接支持的 observed insight。

同时要保留一条业务边界：

- **Excel benchmark 继续保留**：承载人口画像、样本化展示、研究整理后的字段。
- **Forum VOC live deck 单独成长**：承载公开论坛里可直接观测到的声音证据、主题、抱怨点、决策因素和竞品对比。

### 2.2 为什么不能直接把论坛结果当成样本事实

论坛内容非常适合抽：

- 痛点与抱怨
- 决策因素
- 续航 / 充电 / 冬季能力 / 空间 / 软件 / 服务体验
- 使用场景
- 竞品比较
- 真实语言表达和证据片段

但论坛天然不适合作为可靠 sample fact 的字段包括：

- 年龄 / 性别 / 家庭结构
- 收入 / 职业
- 通勤里程
- 样本代表性统计

所以 forum deck 必须显式区分：

- **Observed from forum VOC**
- **Not treated as sample facts / inferred only**

### 2.3 业内最佳实践对本仓库的启发

从已有 research 和公开资料看，最重要的不是先上更复杂的 LLM，而是先把输入层做对：

1. **先做 main-text extraction / boilerplate removal**  
   页眉、导航、论坛 member meta、媒体站壳子如果不先清掉，后面的 sentiment / topic / summary 都会被污染。

2. **不要只停在 document-level sentiment**  
   VOC 更适合按 aspect / target / observation 抽取，而不是整篇帖子只给一个情绪。

3. **优先 multilabel / multi-signal**  
   一篇帖子经常同时谈 winter range、charging、software、service、price，单标签不够用。

结论就是：**先把 observation layer 做扎实，再决定哪些环节要接翻译、弱监督模型或 LLM。**

## 3. 当前 repo 已经做到的事情

### 3.1 Source registry 和 raw capture 已落地

`07_ScrapingToolkit` 现在已经具备 VOC 的基础抓取链路：

- `voc_sources/` 按 batch → countries → sources 组织公开来源
- `jato-voc-plan` 用于输出 source registry / taxonomy / collection strategy
- `jato-voc-fetch` 用于抓取 source landing page 和同站候选 thread/article
- 输出路径：
  - `04_Processed_data/voc/<country>/raw/<source_code>.json`

当前 raw artifact 已包含：

- source 元数据
- taxonomy / collection strategy
- landing page 摘要
- document-level `url` / `title` / `publishedAt` / `rawText` / `excerpt`
- document-level `autoReview`
- document-level `contentUnits`
- source-level `autoReview` 汇总
- 抓取错误列表

### 3.2 fetch-time 正文抽取已经升级

`jato_scraper.voc_fetcher` 目前不再只是轻量 XPath 抽正文，而是：

- 优先走 **Trafilatura-first main-text extraction**
- 当抽取结果过薄时自动退回 **lxml XPath fallback**
- 在 raw artifact 中补写 `textExtraction.method`
- 在 raw artifact 中补写 fetch-time `contentUnits`

当前 fetch-time `contentUnits` 的策略是：

- 优先保留 DOM block（`p / li / blockquote`）
- 对 comment / reply / post-like container 尝试映射为 `comment` / `reply_post` / `discussion_post`
- 结构过薄时再退回 `fetch_sentence_window`

这一步的意义是把“正文提取质量”前移到 fetch 层，而不是把所有脏文本都扔给 downstream enrichment。

### 3.3 staging sync 已存在

后端已经有 `sync_voc_raw_to_store()`，会把 raw artifact 同步到 PostgreSQL staging，包括：

- `ops.voc_source_runs`
- `ops.voc_raw_documents`

这意味着 forum VOC 不只是落本地 JSON，而是已经具备：

- 可审计
- 可回放
- 可做 review/filter
- 可进一步接后端 API

### 3.3.1 PostgreSQL 在 VOC 里的职责边界（2026-04-21）

当前 VOC 已明确采用“artifact-first + PG staging”双层形态：

1. `04_Processed_data/voc/<country>/raw/*.json` 继续保留原始抓取产物，承担回放与 enrichment 输入。
2. PostgreSQL `ops.voc_source_runs` / `ops.voc_raw_documents` 承担 app-facing staging、后台观察台、筛选查询和后续 review/filter 的关系型入口。

这意味着 PostgreSQL 在 VOC 里当前不是最终 deck store，而是：

1. raw run / raw document 的结构化 staging 层。
2. 供 `/data-management`、运营排查、后续 review/filter 使用的业务库入口。
3. 和 Customer Insights forum live deck 并行存在的“可审计关系层”，而不是替代 deck artifact。

对应的 PostgreSQL contract 当前固定为：

1. `ops.voc_source_runs` 以 `source_code + collected_at_utc` 去重同一次 source run。
2. `ops.voc_raw_documents` 以 `voc_source_run_id + source_url` 去重同一 run 内的页面。
3. PostgreSQL 只存路径、摘要、review 与统计字段，不回存 HTML / screenshot 二进制。

### 3.4 enriched signals 和 country deck 已落地

`jato-voc-enrich` 现在已经能把 raw 继续聚合成：

- `04_Processed_data/voc/<country>/enriched/customer_insight_signals.json`
- `04_Processed_data/voc/<country>/deck/customer_insight_deck.json`

当前 heuristic enrichment 已覆盖：

- document-level sentiment
- ownership stage
- pain points
- product signals
- powertrain mentions
- decision factors
- evidence cards

当前又继续补了一层 **taxonomy-driven automatic analysis**：

- theme tags / multi-label themes
- persona cohorts / persona tags
- product matching / primary product / competitor mentions
- heuristic auto scores（relevance / persona / match confidence / overall）
- cross analysis（product × pain point、persona × decision factor、theme × source type）
- association graph / filter suggestions（基于标签共现做 support / 双向 confidence / lift / jaccard / NPMI / phi，可复算可复现）
- 当前 association layer 已继续补 statistical validation：
  - one-tailed Fisher exact test
  - Benjamini-Hochberg FDR
  - sourceCode / siteType / monthBucket replication axes
- 当前 association layer 又继续把 transaction unit 从 document-level 往下推成 **content-unit**：
  - 优先吃显式 `contentUnits / comments / replyPosts / readerComments / publicComments`
  - 没有结构化评论时退回 derived sentence windows
  - country artifact 会额外暴露 `analysisUnitCount`
- persona summaries / score bands / matched products

### 3.5 已补的 observation-first 改造

这轮实现已经把 enrichment 往 research 推荐的方向推进了一步，新增了：

- boilerplate-aware cleaning
- sentence-level observations
- token-boundary matching（避免 `Toyota` 误命中 `OTA`）
- mention counting
- `signalObservationCount`
- deck metric `Signal observations`

这意味着当前 deck 不再只是“多少篇文档提到过什么”，而是开始区分：

- 有多少文档
- 有多少 observation-level signal hits

## 4. 当前已经验证过的状态

### 4.1 当前主链路

当前 VOC 主链路已经是：

```text
source registry
  -> raw fetch
  -> raw artifact
  -> PostgreSQL staging
  -> enriched signals
  -> country deck
```

### 4.2 已验证项

已经验证过的点包括：

- `07_ScrapingToolkit` 测试通过
- `jato-voc-fetch` 和 `jato-voc-enrich` 能跑通
- NO live artifact 已重新生成
- raw artifact 已记录 `textExtraction.method`
- deck artifact 已包含 `Signal observations`
- `OTA` / `Toyota` 的误命中边界问题已修复

### 4.2.1 batch_a 八国 enrichment 实跑（2026-04-21）

这轮又直接对 `voc_sources/batch_a.yaml` 对应的 8 个国家（SE / FI / NO / DK / HU / HR / AT / CZ）跑了一轮本地 enrichment：

```bash
cd 07_ScrapingToolkit
jato-voc-enrich --countries SE FI NO DK HU HR AT CZ --output tmp/voc_batch_a_summary.json
```

本地 raw snapshot 的汇总结果：

- **43 docs**
- **43 publish-ready docs**
- **68 analysis units**
- **24 configured sources**

按国家看：

| Country | Docs | Ready | Units | Sources | Top association | Validation | Unit sources |
| --- | ---: | ---: | ---: | ---: | --- | --- | --- |
| SE | 3 | 3 | 1 | 3 | Range / charging / winter usability + Charging convenience | src:N / type:N / month:N / fdr:N | derived_sentence_window:1 |
| FI | 6 | 6 | 11 | 3 | - | - | derived_sentence_window:11 |
| NO | 6 | 6 | 6 | 3 | Charging convenience + Charging experience | src:N / type:N / month:N / fdr:N | derived_sentence_window:5, document_fallback:1 |
| DK | 6 | 6 | 3 | 3 | Service / aftersales + Service / reliability | src:N / type:N / month:N / fdr:N | derived_sentence_window:3 |
| HU | 6 | 6 | 20 | 3 | infrastructure_cautious_driver + charging_gap | src:N / type:N / month:N / fdr:Y | derived_sentence_window:20 |
| HR | 6 | 6 | 5 | 3 | Software / OTA / reliability + Software / OTA bugs | src:N / type:N / month:N / fdr:N | derived_sentence_window:4, document_fallback:1 |
| AT | 4 | 4 | 16 | 3 | Service / delivery / dealer experience + Service / aftersales | src:N / type:N / month:N / fdr:N | derived_sentence_window:16 |
| CZ | 6 | 6 | 6 | 3 | HEV + PHEV | src:N / type:N / month:N / fdr:N | derived_sentence_window:6 |

这轮结果说明了三件事：

1. **batch_a 当前已能在 8 国全量产出 enriched / deck artifact**，主链路可跑通。
2. **content-unit transaction 已经有效提高 transaction 数**：43 篇文档被扩成 68 个 analysis units。
3. **但当前 raw snapshot 仍主要依赖 derived sentence windows**，说明 fetch/raw 还没有稳定保留 comment / reply 结构；要让 association graph 真正进入“更科学、更稳”的阶段，下一步最值钱的不是继续堆国家，而是让 raw 层显式落 `contentUnits/comments/replyPosts`。

### 4.2.2 fetch-time `contentUnits` 隔离 refetch 验证（2026-04-21）

在 fetcher 补完 raw `contentUnits` 之后，又做了一轮**隔离的 batch_a refetch + enrich smoke run**（每个 source 仅抓 1 个链接，输出到独立 session 目录，不覆盖主数据）：

```bash
cd 07_ScrapingToolkit
python -m jato_scraper.voc_fetcher \
  --batch-files voc_sources/batch_a.yaml \
  --countries SE FI NO DK HU HR AT CZ \
  --max-links-per-source 1 \
  --output-root <isolated_output_root>

python -m jato_scraper.voc_enricher \
  --countries SE FI NO DK HU HR AT CZ \
  --output-root <isolated_output_root>
```

这轮隔离验证看到：

- SE：`fetch_lxml_block:5`
- NO：`fetch_lxml_block:8`
- HU：`fetch_lxml_block:12`
- HR：`fetch_lxml_block:6`
- AT：`fetch_lxml_block:13`
- CZ：`fetch_lxml_block:2`
- FI / DK：当前这轮 refetch 仍未产出可分析 unit

这说明新的 fetch-time content-unit preservation 已经开始把 transaction source 从 downstream `derived_sentence_window` 前移到 raw/fetch 层，但离“各国 source 都稳定保住 comment / reply 结构”还有距离。

### 4.3 当前已经接到业务展示层，但仍保留 benchmark 边界

当前状态已经从 artifact-ready 往前推进了一步：

- 后端 `customer_insight_service` 已支持：
  - `benchmark`
  - `forum_live`
- `CustomerInsightsPage` 已补 **Benchmark Excel / Forum VOC Live** 模式切换
- live 模式已继续补 **country-focus filter**，可按单一已生成 country deck 聚焦查看
- live 模式当前会聚合已生成的 Nordic country deck，并展示：
  - source mix
  - site type / language / publish tier
  - sentiment
  - ownership-stage hits
  - pain points / product signals / decision factors
  - evidence cards

但业务边界仍然保留：

- benchmark Excel 继续承载人口画像、样本化结论和 persona 展示
- forum live 仍然只承载 observed evidence layer

也就是说：**现在已经是 UI-ready 的双模式，不是“forum 直接替换 benchmark”**。

### 4.4 `/data-management` 已补 VOC 观察台

这一轮又把 VOC 从“只在 Customer Insights 可见”继续推进到“在运维页可直接观察”：

- `/data-management` 现在新增 **VOC 观察台**
- 支持按 country 切换查看当前 VOC 状态
- 可直接查看 selected country 的：
  - raw / enriched / deck artifact 路径与更新时间
  - source runs、document / publish-ready / error 计数
  - `textExtraction.method` 聚合结果
  - PostgreSQL staging source-runs / documents / publish-ready 同步状态
  - deck high-level signals / evidence cards
  - VOC 对应仓库文档路径

这一步的意义是把 VOC 从“研究链路 / 展示链路”再往前推进一层，变成**可运营、可追踪、可切国家回看的数据资产入口**。

### 4.5 `CustomerInsightsPage` 的 live deck 已可直接展开看抓取内容

之前 forum live 只保留 evidence card 的 source / signal / snippet，用来快速回放。

现在 live deck 的 evidence card 已继续补成 **可直接展开查看抓取正文预览**：

- 页面摘录（excerpt）
- 抓取后的正文预览（cleaned text preview）
- 命中的 observation 句子、signal label 与 matched tokens

这样用户在网站里就能直接看到“这条 VOC 实际抓到了什么内容”，不必只靠 snippet 或跳到原始链接。

### 4.6 PostgreSQL 结构护栏（2026-04-21）

基于当前 PostgreSQL 审计，VOC 当前最重要的数据库边界不是“再扩更多表”，而是持续守住 staging contract：

1. `ops.voc_*` 继续作为 staging / ops 入口，而不是把 enrichment / deck 全量搬进关系库。
2. 外键列和唯一业务键要保持可直接回放 source run -> raw document 的路径，不让 staging 退化成松散 JSON 仓。
3. 和 MSRP / news 一样，PostgreSQL 侧只保存 app-facing 关系真值与索引友好的审计字段；原始正文与大对象继续留在 artifact 层。

## 5. 已做完的事情清单

### 已完成

1. 完成 VOC feasibility / architecture research。
2. 明确 `raw -> enriched observations -> deck` 的三层路线。
3. 在 `07_ScrapingToolkit` 中落地 `jato-voc-enrich`。
4. 产出 country-level enriched artifact 和 deck artifact。
5. 增补 boilerplate-aware cleaning、sentence-level observations、mention-aware aggregation。
6. 修复 token-boundary 误匹配问题。
7. 升级 `voc_fetcher` 为 Trafilatura-first + fallback。
8. 在 raw artifact 中记录 `textExtraction.method`。
9. 更新 toolkit README 与 VOC feasibility 文档。
10. 用 live country 数据重新生成 raw/deck artifact 并回看结果。
11. 后端补 `forum_live` customer insight API 聚合层。
12. `CustomerInsightsPage` 补 benchmark / forum_live 双模式与 evidence-card 面板。
13. live 模式补 country-level filter，可聚焦单一 country deck，而不只看全部已生成 coverage。
14. `/data-management` 补 VOC 观察台，支持按 country 查看 artifact / source runs / staging / docs。
15. `CustomerInsightsPage` 的 forum live evidence card 已支持直接展开查看抓取内容与 observation 句子。
16. `voc_taxonomy` / `voc_enricher` 已补 taxonomy-driven 自动分析层，`voc_sources` 当前可直接继承 theme/persona/product/scoring/cross-analysis 默认能力。
17. VOC 文档现在已把 PostgreSQL staging 明确写成正式架构边界：`ops.voc_source_runs` / `ops.voc_raw_documents` 负责 app-facing staging，artifact 层继续保留 raw/enriched/deck。

## 6. 还没做、但应该继续做的事情

### 6.1 P0：已完成主展示与运维观察入口；继续把 live 模式做得更可控

这一层已经做完的部分：

- 后端已补 forum VOC deck 聚合 API
- 前端已补 **benchmark / live 双模式**
- 当前 live 模式已经可直接在 `CustomerInsightsPage` 查看
- 当前 live 模式也已支持按 country deck 聚焦查看
- `/data-management` 已可按 country 查看 VOC artifact / source runs / staging / docs

下一步不再是“有没有 live 模式”，而是“让 live 模式更可控、更可用”，优先包括：

- 更稳定的 source coverage 提示
- 后端 API contract 继续往 product-friendly 方向收敛
- side-by-side 或 benchmark/live 对照增强

### 6.2 P1：把 raw extraction 从“能用”继续提升到“结构更稳”

虽然 fetch 层已经升级成 Trafilatura-first，但还可以继续补：

- 更好的 thread/comment DOM 拆解
- comment-level extraction，而不只是 page-level text
- engagement / reply count / thread depth
- author hash / content unit id
- raw HTML 的短 TTL 策略

### 6.3 P1：继续把 observation layer 的实体与跨语言能力做深

这轮已经先把 taxonomy-driven 自动分析补到 heuristic/config-driven 层，当前已具备：

- `themeTags`
- `personaTags`
- `personaCohorts`
- `productMentions`
- `primaryProduct`
- `competitorMentions`
- `autoScores`
- `scoreBands`
- `crossAnalysis`

下一轮建议继续补这些字段：

- `language_detected`
- `translated_text`
- `brand_mentions`
- `model_mentions`
- `scenario_tags`
- comment-level entity grounding

这样后续 deck、Copilot、对比分析都更容易复用。

### 6.4 P2：在 heuristic 自动分析之上再接模型

当前顺序已经变成：

1. 抽正文
2. observation contract
3. taxonomy-driven multi-label / matching / scoring
4. translation / entity normalization
5. model-assisted tagging
6. frontend productization

等输入层和当前 heuristic 层再稳定一轮以后，再考虑：

- multilabel taxonomy classifier
- LLM-assisted structured extraction
- fixed evaluation set / regression set
- benchmark 对照和人工抽检

## 7. 推荐的后续执行顺序

1. **继续补 comment-level extraction、scenario tags 和细粒度 entity grounding**  
   让 observation 粒度更细、product/persona 匹配更稳。

2. **在现有 taxonomy-driven heuristic 层之上补 translation / normalization**  
   先把跨语言实体归一、品牌/车型别名和场景标签做稳。

3. **最后再接 multilabel classifier / LLM extraction**  
   这时模型输入、回归集和人工抽检路径都会更可控。

## 8. 当前建议如何使用这套 VOC 链路

### 当下最适合的定位

当前 forum VOC 最适合用于：

- 看近期用户讨论主题
- 看痛点和决策因素
- 看真实证据句和 source mix
- 做 benchmark Excel 的补充和动态更新层

当前不适合用于：

- 直接替代样本画像页
- 直接输出人口统计事实
- 把 forum 讨论包装成代表性 survey 结论

## 9. 关联文档

- `02_DataETL/VOC_FORUM_SCRAPING_FEASIBILITY_2026-04-17.md`
- `02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md`
- `07_ScrapingToolkit/README.md`

如果要继续做 VOC，这份文档可以作为**当前状态入口**；feasibility 文档保留“为什么做、边界是什么”，而这份文档承接“已经做到哪、接下来怎么做”。
