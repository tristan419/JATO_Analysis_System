# JATO Scraping Toolkit

独立的 MSRP 官方价格抓取工具包，从各国汽车品牌官网提取结构化价格数据。

## 目录结构

```text
07_ScrapingToolkit/
├── jato_scraper/                 # 核心 Python 包
│   ├── extractors/
│   │   ├── scrapling_web.py      # 主提取器（Scrapling headless browser）
│   │   └── http_json.py          # HTTP JSON API 提取器（备用）
│   │   └── playwright_card_flow.py # 动态卡片流提取器（VW configurator 等）
│   ├── base.py                   # 基础类型定义（RawObservation 等）
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
├── sources/                      # 生产 source YAML
│   ├── volvo_se_xc60.yaml        # 已上线 source
│   ├── bmw_de.yaml               # 样例 source
│   └── _template.yaml            # YAML 模板
├── run.py                        # CLI 入口
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
