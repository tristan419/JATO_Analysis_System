# JATO Scraping Toolkit

独立的抓取工具包，当前主战场仍是 MSRP 官方价格抓取，同时逐步扩展到 news / policy / incentive / spec 等结构化采集。

## 相关设计文档

- `../Markdown_Readme/Fullstack/02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md` — 统一抓取平台蓝图
- `../Markdown_Readme/Fullstack/MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md` — MSRP 多源对账与 feature diff
- `../Markdown_Readme/Fullstack/03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md` — JATO sales × MSRP 联表方案

## 目录结构

```text
07_ScrapingToolkit/
├── jato_scraper/                 # 核心 Python 包
│   ├── extractors/
│   │   ├── scrapling_web.py      # 主提取器（Scrapling headless browser）
│   │   └── http_json.py          # HTTP JSON API 提取器（备用）
│   │   └── playwright_card_flow.py # 动态卡片流提取器（VW configurator 等）
│   ├── base.py                   # 基础类型定义（RawObservation 等）
│   ├── news_base.py              # 新闻批次和文章结构定义
│   ├── news_config_loader.py     # 新闻批次 YAML 加载器
│   ├── news_runner.py            # RSS/Atom 新闻抓取入口
│   ├── config_loader.py          # YAML source 配置加载器
│   ├── currency_converter.py     # 汇率自动转换（open.er-api.com → EUR）
│   ├── llm/                      # LLM provider clients + page analyzer
│   ├── registry.py               # Source 注册表
│   ├── runner.py                 # 主运行入口（run_scrape / main）
│   ├── source_bootstrap.py       # Draft YAML scaffold 自动生成
│   ├── validation.py             # 价格验证（范围检查、货币匹配）
│   └── candidate_scope.py        # 候选 source 覆盖范围分析
├── source_drafts/                # 草稿 YAML（开发中）
│   ├── suv_only_country_model_top30/   # ← 当前执行重点
│   │   ├── {country_code}/             # 每国 ~30 个 YAML
│   │   ├── draft_batch_summary.md      # 批次索引
│   │   └── draft_batch_summary.json    # 机器可读索引
│   └── _archived/                      # 旧口径存档
│       ├── country_brand_priority_top30/   # 旧口径（已归档）
│       └── all_market_country_model_top30/ # 全市场口径（备用参考）
├── msrp_batches/                 # MSRP 国家批次入口（如 batch_a）
├── sources/                      # 生产 source YAML
│   ├── volvo_se_xc60.yaml        # 已上线 source
│   ├── bmw_de.yaml               # 样例 source
│   └── _template.yaml            # YAML 模板
├── news_sources/                 # 国家新闻批次配置（Batch A / Batch B，Batch A 已接入多源试点）
├── run.py                        # CLI 入口
├── run_news.py                   # 新闻 CLI 入口
├── pyproject.toml                # 包定义
└── requirements.txt
```

## 快速使用

### 安装

```bash
cd 07_ScrapingToolkit
pip install -e .
playwright install chromium
```

`pdf_text` source 遇到字体编码导致 `pypdf` 无法取字的官方价目 PDF 时，会回退到 Poppler 的 `pdftotext`。生产环境需安装 Poppler 并确保 `pdftotext` 在 `PATH`；若二进制位于非标准位置，设置 `JATO_PDFTOTEXT_BIN=/absolute/path/to/pdftotext`。该回退只负责文本解码，价格仍必须由 source profile 明确匹配官方 `Listaár` / list-price 字段。

如果后续需要用 NVIDIA NIM 做页面分析或 source 辅助判定，先在仓库根目录执行：

```bash
bash 03_Scripts/store_nvidia_api_key.sh
source 03_Scripts/load_nvidia_api_key.sh
```

### 单个 source dry-run

```bash
# 方式 1：CLI，--sources 同时支持 source_code、单个 YAML 文件路径、或目录路径
python run.py --sources source_drafts/suv_only_country_model_top30/se/01_kia_sportage_se.yaml --dry-run -v
python run.py --sources kia_sportage_se_draft_scrapling --dry-run -v
python run.py --sources source_drafts/suv_only_country_model_top30/be --dry-run -v

# 方式 2：Python API
from jato_scraper.runner import run_scrape
result = run_scrape(source_codes=['kia_sportage_se_draft_scrapling'], dry_run=True)
print(result['sources']['kia_sportage_se_draft_scrapling'])
# → {'status': 'dry_run', 'valid': 1, 'extracted': 1, 'rejected': 0}
```

### 批量 dry-run（按国家）

```bash
cd 03_Scripts
python batch_dryrun.py se    # 瑞典全部 29 个 source
python batch_dryrun.py cz    # 捷克全部 30 个 source
python batch_dryrun.py all   # 所有已填充 keyword 的国家
```

### MSRP 批次执行（Batch A）

```bash
cd 07_ScrapingToolkit
python run.py --batch-files msrp_batches/batch_a.yaml --countries SE FI --dry-run -v
python run.py --batch-files msrp_batches/batch_a.yaml --dry-run -v
```

当前 `msrp_batches/batch_a.yaml` 先对齐第一批 8 个国家（SE / FI / NO / DK / HU / HR / AT / CZ），
每个国家映射到现有的 `source_drafts/suv_only_country_model_top30/<country>/` country pack。

### 新闻批次抓取

```bash
cd 07_ScrapingToolkit
python run_news.py --batch-files news_sources/batch_a.yaml --limit-per-feed 5
python run_news.py --batch-files news_sources/batch_a.yaml news_sources/batch_b.yaml --output tmp/news_batch.json
```

> 当前 `run_news.py` 更接近 feed-level ingest；如果要支撑国家助手的"最新政策 / 最新碳税 / 最新市场新闻"问答，还需要把结果稳定 sink 到 PG + vector store，再由 Copilot 只在 freshness query 时按需调用。

当前 news runner 也会默认额外保留一份 raw batch artifact 到仓库根目录：

```text
04_Processed_data/news/raw/news_batch_<timestamp>.json
```

如果显式传了 `--output`，则写到指定文件；不传时仍会把 JSON 打到终端，但同时落一份默认 artifact，方便后续在数据总览页统计最近抓取批次、文章量和错误数。

当前 `news` 还开始附带第一版 **auto review / publish gate** 思路，目标不是人工 review，而是自动质量闸门：

- 先做 deterministic checks：URL、标题长度、摘要长度、发布时间、标签存在性
- 再做 source tier 识别：aggregator / local_media / trusted_media / association / official
- 再做 batch 内 corroboration：不同 publisher 标题/摘要高度重合时加分
- 最终产出 `autoReview`：
  - `score`
  - `publishTier`（high / medium / low）
  - `publishDecision`（auto_publish / candidate_publish / holdout）
  - `signals` / `warnings`

当前 public payload 默认只把 `publishDecision != holdout` 的事件放进 `marketEvents`，也就是先做自动闸门，再给国家助手和 digest 层消费。

### 国家化 VOC batch 规划

```bash
cd 07_ScrapingToolkit
jato-voc-plan --batch-files voc_sources/batch_a.yaml
jato-voc-plan --batch-files voc_sources/batch_a.yaml --countries SE FI NO DK
```

当前这批 `voc_sources/` 已经开始从 scaffold 进入第一波真实公开源，先按现有 news 一样组织成：

- batch
- countries
- sources

当前 `batch_a.yaml` 先落地 8 个国家（SE / FI / NO / DK / HU / HR / AT / CZ），每国 3 个公开源，组合是：

- 公开论坛 / 车主社区
- 本地汽车媒体评论页
- EV 社区或消费者媒体

也就是说，先固化国家级 source registry、语言和 taxonomy 的承载方式，并把第一波真实公开源写进 country-pack；后续再继续扩到更多国家与更细的评论页 / 车型论坛。

当前 `jato_scraper.voc_runner` 输出的 collection plan 也会一并带上：

- `taxonomy`：每个 `taxonomy_profile` 对应的 sentiment labels、ownership stages、pain points、focus themes、extraction fields
- `collection_strategy`：每个 source 的 primary unit、content targets、extraction targets

这样后续做 crawler、清洗、翻译、deck 抽取时，可以统一围绕同一套 VOC schema 推进，而不是每个国家各做各的字段。

### VOC raw 抓取（第一版）

```bash
cd 07_ScrapingToolkit
jato-voc-fetch --batch-files voc_sources/batch_a.yaml --countries SE NO --max-links-per-source 4
jato-voc-fetch --batch-files voc_sources/batch_a.yaml --output tmp/voc_raw_summary.json
```

当前 `jato_scraper.voc_fetcher` 是一个轻量 public-page collector，职责是：

- 先抓每个 source 的公开入口页
- 按 `site_type`（forum / ev_community / media_comments ...）挑选同站 article / thread 候选链接
- 再抓这些公开页面正文，统一写入 `04_Processed_data/voc/<country>/raw/<source_code>.json`

正文抽取现在改成 **Trafilatura-first + safe fallback**：

- 优先用 `trafilatura.extract(...)` 做 main-text / boilerplate removal
- 如果抽出来的正文过薄，自动退回现有 `lxml` XPath 提取
- raw artifact 会额外写 `textExtraction.method`，方便回看每篇文档用了哪条抽取路径

第一版输出以 raw layer 为主，默认带：

- source 元数据
- taxonomy / collection strategy
- landing page 摘要
- 文档级 `url` / `title` / `publishedAt` / `rawText` / `excerpt`
- 文档级 `textExtraction.method`
- 文档级 `contentUnits`（`unitId` / `unitType` / `unitSource` / `text` / `author?` / `publishedAt?`）
- 抓取错误列表（单个 source 失败不会拖垮整批）

当前 `VOC raw` 也开始附带第一版 **auto review**：

- 文档级 `autoReview`：按正文长度、标题质量、发布时间、page kind、source/site_type 匹配度给分
- source 级 `autoReview` 汇总：`candidateCount` / `reviewedCount` / `publishReadyCount` / `heldRawCount` / `tierCounts`
- 输出层级：
  - `high` → `auto_publish`
  - `medium` → `candidate_publish`
  - `low` → `hold_raw`

fetch 层现在又继续往前补了一步：**把 fetch-time content units 直接落进 raw artifact**。

- 优先从页面 DOM 里保留 `p / li / blockquote` 这类 block 作为 `contentUnits`
- 对 comment / reply / post-like 容器会尽量保留 `comment` / `reply_post` / `discussion_post` 这类 unitType
- 如果页面结构过薄，再退回 `fetch_sentence_window`

这一步先解决“raw 层就开始保住 thread/comment block”，后续如果要继续做更稳的 comment DOM 解析、作者字段、翻译和 sentiment enrichment，就不必全靠 downstream 再从整页正文二次切句。

### VOC enriched signals + country deck

```bash
cd 07_ScrapingToolkit
jato-voc-enrich --countries SE NO
jato-voc-enrich --output tmp/voc_enriched_summary.json
jato-voc-enrich --countries SE FI NO DK HU HR AT CZ --output tmp/voc_batch_a_summary.json
```

当前 `jato_scraper.voc_enricher` 会把已有的 raw artifact 继续聚合成：

- `04_Processed_data/voc/<country>/enriched/customer_insight_signals.json`
- `04_Processed_data/voc/<country>/deck/customer_insight_deck.json`

第一版 enrichment 先走 **heuristic signal extraction**，补出：

- document-level sentiment / ownership stage
- pain points（winter range / charging / software / service / price / delivery）
- product signals（range / charging / software / reliability / service / family practicality）
- powertrain mentions（BEV / PHEV / HEV / ICE）
- evidence cards（标题 + URL + 命中信号 + excerpt）

当前又往前补了一层 **boilerplate-aware cleaning + sentence-level observations**：

- 先清掉论坛 chrome / navigation / repeated member meta 这类正文噪声
- 再按 sentence 产出 observation 级 signal hits（signal kind / key / matched tokens / sentence / sentiment）
- deck 层额外暴露 `Signal observations`，方便区分“有多少文档”与“有多少可回放证据命中”

当前这条 enrichment 链又继续补成一层 **taxonomy-driven automatic analysis**，目标就是把 `voc_sources/` 从“只定义抓取入口”推进成“也定义自动分析维度”的入口：

- **自动归类 / 多标签**：
  - `themeTags`
  - `personaCohorts`
  - `painPoints`
  - `productSignals`
- **自动匹配**：
  - `productMentions`
  - `primaryProduct`
  - `competitorMentions`
  - 当前先按 taxonomy profile 里的 alias catalog 做 heuristic matching
- **自动打分**：
  - `relevanceScore`
  - `personaScore`
  - `matchConfidence`
  - `overallScore`
  - `scoreBand`
- **cross analysis**：
  - `productPainPoints`
  - `personaDecisionFactors`
  - `themeBySourceType`
- **协同矩阵 / 推荐筛选**：
  - `associationGraph`
  - `associationRecommendations`
  - `synergyMatrix`
  - `filterSuggestions`
  - 通过同一批 publish-ready VOC 文档里的标签共现，按 transaction 方式计算：
    - `count`
    - `supportPct`
    - `confidenceForwardPct`
    - `confidenceReversePct`
    - `lift`
    - `jaccard`
    - `npmi`
    - `phiCoefficient`
    - `expectedCount`
    - `fisherPValue`
    - `fdrAdjustedPValue`
  - 当前还会记录 replication 轴：
    - `sourceCodes`
    - `siteTypes`
    - `monthBuckets`
  - 显著性用 **one-tailed Fisher exact test**，多重比较校正用 **Benjamini-Hochberg FDR**
  - transaction unit 现已优先使用显式 `contentUnits / comments / replyPosts / readerComments / publicComments`，没有结构化评论时退回 **derived sentence windows**
  - deck / enrichment 现在会额外给出 `Analysis units`，用于判断当前 country VOC 的有效样本量是否足够支撑关联筛选
  - 适合做“选了长续航的人，也常继续看性价比”这类 guided filter，不把它当作因果结论
  - `synergyMatrix` / `filterSuggestions` 当前保留为兼容别名；更通用、更可复现实质上是 `associationGraph` / `associationRecommendations`
- **产品 / 人群筛选基础层**：
  - `matchedProducts`
  - `personaSummaries`
  - `scoreBands`

这些新维度当前由 `taxonomy_profile` 驱动，所以只要 `voc_sources/countries/*.yaml` 继续引用 profile（如 `nordic_core` / `cee_core` / `dach_core`），后续加国家时就能直接继承同一套多标签、打分、匹配和 cross-analysis 逻辑。

同时 deck artifact 会显式区分：

- **observed from forum VOC**：source mix、site type、语言、情绪、ownership stage、pain points、product signals、decision factors、evidence cards
- **not treated as sample facts**：年龄、家庭结构、通勤等 profile 字段

这样 raw → enriched → deck 三层就先跑通了；后续如果要接翻译、LLM tagging 或前端消费，可以直接围绕这份 country deck 再接下一层。

### Batch A 实跑快照（2026-04-21）

用当前 repo 内已有 raw artifact 直接跑：

```bash
cd 07_ScrapingToolkit
jato-voc-enrich --countries SE FI NO DK HU HR AT CZ --output tmp/voc_batch_a_summary.json
```

本地这轮 batch_a 结果是：

- 8 个国家全部成功产出 enriched + deck artifact
- 总计 **43 docs / 43 publish-ready docs / 68 analysis units / 24 configured sources**
- 当前 `analysisUnitCount` 已明显高于 document 数，但 raw snapshot 里还几乎没有显式 `contentUnits/comments/replyPosts`，所以大部分 unit 仍来自 **derived sentence windows**
- 这意味着 content-unit association graph 已经能跑，但下一阶段最有价值的提升仍然是：让 fetch/raw 层真正保留 comment/reply 结构，而不是主要依赖 sentence-window fallback

在 fetcher 补完 `contentUnits` 之后，又做了一轮**隔离的 batch_a refetch smoke run**（每 source 抓 1 个链接，输出到独立目录，不覆盖主数据）：

- SE / NO / HU / HR / AT / CZ 已开始出现 `fetch_lxml_block`
- FI / DK 这轮 refetch 仍是 0 analysis units，说明当前抓到的页面虽然能落 raw，但还没有稳定产出可分析 block / signal

这说明 fetch-time content-unit preservation 已经开始起作用，但“各国都稳定拿到 comment/reply-like unit”还没完成，后续仍要继续前推 source-specific comment DOM/分页策略。

### EVKX BEV 参数 + MSRP 抓取

```bash
cd 07_ScrapingToolkit
python run_evkx.py --pricing-country UnitedStates --limit 20
python run_evkx.py --pricing-country Germany --page-size 100
```

默认会输出到：

```text
04_Processed_data/msrp_candidate_scope/evkx/evkx_bev_<pricing_country>_<availability>.json
```

输出包含：

- EVKX 搜索接口返回的 BEV 列表与起售价
- 每辆车详情页里的多市场 pricing 列表
- `/specifications/` 页里的分 section 规格表

### EVKX JSON 导入 MSRP review

抓完 EVKX JSON 后，可以直接走现有 MSRP review batch ingest：

```bash
python 03_Scripts/import_evkx_catalog.py \
  04_Processed_data/msrp_candidate_scope/evkx/evkx_bev_unitedstates_current.json

python 03_Scripts/import_evkx_catalog.py \
  04_Processed_data/msrp_candidate_scope/evkx/evkx_bev_unitedstates_current.json \
  --dry-run
```

当前导入策略：

- 默认写入 `review_required`，不直接 auto-accept
- 会为每条 EVKX 记录保存 `selectedMarketPrice`、`specHighlights`、完整 `specifications`
- 会基于同国家 current prices 生成一组 `candidateMatches` 供 ReviewCasesPage 展示

## 提取策略

`scrapling_web.py` 按优先级依次尝试三种提取方式：

| 优先级 | 策略 | 适用场景 | 示例品牌 |
| ------ | ------ | --------- | --------- |
| 1 | `attr_json` | YAML 指定 JSON 属性路径 | — |
| 2 | `json_script_selector` (ld+json) | 页面含 `<script type="application/ld+json">` | Toyota, Volvo, Skoda, Hyundai, KIA, Dacia |
| 3 | `css` | 指定 CSS 选择器提取 DOM 元素 | Ford, Peugeot, Opel |

动态 configurator 场景可以直接使用 `playwright_card_flow.py`。这类 source 适合：

- 先展示 trim card，再进入下一步展示 engine 或 powertrain card
- 价格只在第二步或更深一步才可见
- 同一 trim 会拆成多个动力版本，必须逐卡点击并展开

Volkswagen 品牌家族现在优先按这条路径复用，而不是为每个国家单独写一次站点逻辑。复用时先验证三件事：

- 页面给出的是真正 MSRP，而不是 finance / leasing 月付
- 第二步动力卡的价格语义和币种一致
- 需要 engine-level 粒度时，把 trim 和 powertrain 组合成一个 trim key，避免 current_prices 唯一键冲突

2026-04-12 起，draft source 还支持 `profile_preset`。这意味着 Volkswagen 这类品牌家族可以把公共 profile 抽到 `_shared/presets/*.yaml`，各国 source 只保留 URL、币种、价格标签和本地 powertrain 关键词。

但 SI / SK 是当前例外：live probe 显示它们落在 Porsche configurator family（`konfigurator.volkswagen.si` / `konfigurator.vw.sk`），不要直接套用现有 Volkswagen card-flow preset；后续应单独抽 shared preset 或 extractor。

### ld+json 支持的 @type

`Product`, `Car`, `Vehicle`, `IndividualProduct`, `ProductModel`, `AggregateOffer`

### 价格验证范围

| 货币 | 最低 | 最高 |
| ------ | -----: | -----: |
| SEK | 50,000 | 5,000,000 |
| EUR | 5,000 | 500,000 |
| NOK / DKK | 50,000 | 5,000,000 |
| CHF | 10,000 | 1,500,000 |
| HUF | 1,000,000 | 50,000,000 |
| CZK | 200,000 | 10,000,000 |

## YAML Source 配置示例

```yaml
source_code: toyota_rav4_cz_draft_scrapling
brand: TOYOTA
country: CZ
source_url: https://www.toyota.cz/nova-auta/rav4
profile_url: https://www.toyota.cz/nova-auta/rav4
currency: CZK
extractor: scrapling
# ld+json 自动提取，无需额外 CSS 配置
```

带 CSS 选择器的配置：

```yaml
source_code: ford_puma_at_draft_scrapling
brand: FORD
country: AT
source_url: https://www.ford.at/autos/puma
profile_url: https://www.ford.at/autos/puma
currency: EUR
extractor: scrapling
css:
  vehicle_container: '.dse-price-module'
  price: '.dse-price-module__price'
```

Playwright trim -> powertrain card flow 示例：

```yaml
source_code: volkswagen_tiguan_se_draft_scrapling
brand: VOLKSWAGEN
country: 瑞典
source_url: https://www.volkswagen.se/sv/bygg-din-bil.html/__app/31106.app
extractor_type: playwright
profile:
  url: https://www.volkswagen.se/sv/bygg-din-bil.html/__app/31106.app
  browser: chromium
  headless: true
  initial_ready_selector: '[data-testid="trimcard"]'
  cookie_reject_text: Enbart nödvändiga
  trim_card_selector: '[data-testid="trimcard"]'
  trim_name_selector: h3
  next_step_selector: '[data-testid="smart-offer-trim-next-step-cta"]'
  detail_ready_selector: '[data-testid="engine-card"]'
  detail_card_selector: '[data-testid="engine-card"]'
  detail_price_selector: '[data-testid="price-container"]'
  combine_trim_and_powertrain: true
  combined_trim_separator: ' | '
  default_currency: SEK
  fixed_model: TIGUAN
  fixed_jato_model: TIGUAN
  structured_fields:
    powertrain_rules:
      - powertrain: PHEV
        keywords: [eHybrid, laddhybrid]
      - powertrain: MHEV
        keywords: [eTSI, mild hybrid]
      - powertrain: ICE
        keywords: [TSI, TDI, bensin, diesel]
```

如果要复用共享 preset，可以这样写：

```yaml
source_code: volkswagen_tiguan_de_draft_scrapling
brand: VOLKSWAGEN
country: 德国
source_url: https://www.volkswagen.de/de/modelle/tiguan.html
extractor_type: scrapling
profile_preset: ../_shared/presets/volkswagen_model_page_scrapling.yaml
profile:
  url: https://www.volkswagen.de/de/modelle/tiguan.html
  default_currency: EUR
  default_price_label: Unverbindliche Preisempfehlung des Herstellers
  fixed_model: TIGUAN
  fixed_jato_model: TIGUAN
  structured_fields:
    powertrain_rules:
      - powertrain: PHEV
        keywords: [ehybrid, plug-in-hybrid]
      - powertrain: MHEV
        keywords: [mild-hybrid, mild hybrid]
      - powertrain: ICE
        keywords: [benzin, diesel, tsi, tdi]
```

如果后续要扩展 Volkswagen 其它国家，先复制这个 profile 结构，再只替换：

- `source_code`
- `country`
- `source_url` / `profile.url`
- `default_currency`
- `cookie_reject_text`
- `structured_fields.powertrain_rules` 里的本地语言关键词

## 新闻抓取基础设施（Phase 3 foundation）

当前新增的是与 MSRP 抓取解耦的轻量新闻入口，先把国家新闻流按批次标准化输出：

- `news_sources/batch_a.yaml`：第一批国家
- `news_sources/batch_b.yaml`：剩余当前 JATO 数据国家
- `jato_scraper/news_runner.py`：按批次抓取并输出标准 JSON

当前推荐的新闻职责拆分是：

- Feed 层负责召回：保留 `Google News RSS`，同时补充本地媒体 / 行业协会 / OEM newsroom 等 RSS
- Digest 层负责理解：Gemini 只做摘要、标签、排序、事件提炼，不替代检索

当前 `batch_a.yaml` 已经升级到四层组合：国家定向 `Google News RSS` + `ACEA` + `Transport & Environment` + 每国一条本地汽车 / 行业 RSS。其中 `ACEA` 与 `Transport & Environment` 继续通过 `include_keywords` 按国家名过滤，本地 RSS 则负责补足更高密度的市场新闻。

输出字段包括：

- `country_code`
- `country_label`
- `publisher`
- `title`
- `url`
- `summary`
- `published_at`
- `tags`

这一步先解决“国家新闻能批量抓到并标准化输出”，后续再决定是写入 PostgreSQL、回灌到 country chat snapshot，还是做成独立事件表。

## LLM 辅助分析

工具包已经内置 Hugging Face / NVIDIA 的 OpenAI-compatible client，并新增了一个 MSRP 页面分析 CLI。它不会直接替代 extractor，而是用于在建 source 前快速判断：

- 这个页面更像真实 MSRP，还是 finance / leasing 落地页
- 更适合 `scrapling`、`playwright` 还是 `http_json`
- 有没有明显的 `ld+json`、`data-testid`、configurator 链接线索
- 是否值得在 pipeline 里引入 LLM 做语义过滤、selector 建议或失败兜底

现在这个 CLI 默认会先跑一层本地 heuristics，所以即使腾讯云机器上暂时没配密钥，也能先得到一份 extractor 建议和风险摘要：

```bash
python 03_Scripts/analyze_msrp_page_with_llm.py \
  --url https://www.volkswagen.de/de/modelle/tiguan.html
```

示例：

```bash
source 03_Scripts/load_nvidia_api_key.sh
python 03_Scripts/analyze_msrp_page_with_llm.py \
  --provider nvidia \
  --model meta/llama-3.3-70b-instruct \
  --url https://www.volkswagen.de/de/modelle/tiguan.html
```

腾讯云部署建议：

- 腾讯云上不需要 macOS Keychain；直接注入 `NVIDIA_API_KEY` 或 `NVAPI_KEY` 环境变量即可，provider client 会自动读取
- 优先把 LLM 用在 source 起草、页面语义判断、失败样本复盘，不要放在每次常规 scrape 的热路径里
- 对动态页面先让规则提取器跑，LLM 只处理 ambiguous / failed cases，控制成本和时延
- 生产机仍按低并发运行 Playwright；LLM 分析更适合手工触发或离线诊断

## 当前进度（2026-04-12）

| 市场 | Dry-run 通过 | 总数 | 通过率 | 主要通过品牌 |
| ------ | ----------: | -----: | ------: | ------------ |
| SE | 20 | 29 | 69.0% | Toyota, Volvo, KIA, Skoda, Peugeot, VW |
| CZ | 19 | 30 | 63.3% | Skoda, Toyota, Dacia, Ford, Hyundai, KIA |
| HU | 15 | 30 | 50.0% | Toyota, Dacia, Ford, Nissan, Volvo, Opel |
| NO | 13 | 30 | 43.3% | Volvo, Skoda, Hyundai, Toyota, KIA, Ford |
| CH | 13 | 30 | 43.3% | Skoda, Dacia, Hyundai, Toyota, Volvo, Mercedes |
| AT | 7 | 30 | 23.3% | Dacia, Ford, Peugeot, Toyota, VW, Volvo |
| HR | 5 | 30 | 16.7% | Toyota, Nissan, Opel |
| **合计** | **92** | **209** | **44.0%** | |

> Batch 3-4（14 国 420 文件）尚未进行 keyword filling。
>
> 2026-04-13 更新：Volkswagen France / Italy / Finland 的 10 个 draft 已切到通用 Playwright configurator preset，并完成定向 dry-run（FR 17/17，IT 41/41，FI 34/34 valid observations）。Slovenia / Slovakia 属于 Porsche configurator family，需要单独 shared preset 或 extractor；Poland 需先把总价和 finance 月供做干净分离后再转。

## 相关文档

- 执行计划：`Markdown_Readme/Fullstack/MSRP/05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md`
- 技术流程：`Markdown_Readme/Fullstack/MSRP/03_Implementation/MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md`
- Keyword 填充记录：`Markdown_Readme/Fullstack/MSRP/04_Execution/MSRP_KEYWORD_FILLING_AND_POC_RESULTS_2026-04-12.md`
- CSS 工具链：`Markdown_Readme/Fullstack/MSRP/03_Implementation/CSS_SELECTOR_TOOLCHAIN_2026-04-11.md`
