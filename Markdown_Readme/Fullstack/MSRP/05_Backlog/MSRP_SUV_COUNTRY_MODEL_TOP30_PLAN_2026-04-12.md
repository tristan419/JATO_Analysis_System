# MSRP SUV-Only Country Model Top 30 执行计划

Date: 2026-04-12

Status: 执行中 — 执行口径已统一为 Batch A / Batch B，当前按 Batch A 优先推进。

Update: 2026-04-13 Volkswagen configurator 专项补充已记录；BE/DK/FR 子集已 promotion 到 sources，IT/FI 子集验证完成，SI/SK 与 PL 进入定向挂起。2026-04-21 补记：本页的批次口径已改为 Batch A / Batch B；其中 Batch A 与 `07_ScrapingToolkit/msrp_batches/batch_a.yaml` 对齐，CZ Skoda 5 个 source 已完成 production promotion；`03_Scripts/batch_ingest.py` 现已支持 `--auto-review --materialize` 串联自动链路；SK Touareg 已通过官方 `stock-cennik` PDF 收口并 promotion 到 production source；同日已完成 **CZ live ingest** 首轮实跑，review queue 已真实落地到本地 PostgreSQL。

---

## 0.1 2026-04-13 Volkswagen 暂存说明

### 已完成

- BE: Volkswagen ID.4 / T-Roc / Tiguan 已完成 production promotion to `07_ScrapingToolkit/sources/`，对应 draft 不再作为 batch_dryrun 待跑项。
- DK: Volkswagen ID.4 / T-Roc 已完成 production promotion to `07_ScrapingToolkit/sources/`，定向 dry-run 结果为 7/7、4/4。
- FR: Volkswagen T-Roc / Tiguan / T-Cross 已切到 trim-overview Playwright preset，定向 dry-run 结果为 4/4、7/7、6/6。
- FR: Volkswagen T-Roc / Tiguan / T-Cross 已完成 production promotion to `07_ScrapingToolkit/sources/`。
- IT: Volkswagen T-Roc / Tiguan / T-Cross 已切到 trim -> engine Playwright preset，定向 dry-run 结果为 5/5、24/24、12/12。
- FI: Volkswagen ID.4 / Tiguan / T-Cross / Taigo 已切到 numeric __app Playwright preset，并补上 OneTrust dismiss 支持，定向 dry-run 结果为 9/9、9/9、7/7、7/7。
- CZ: Skoda Kamiq / Karoq / Kodiaq / Elroq / Enyaq 的 draft 已在官方 `skoda-auto.cz/modely/*/*` 页面完成定向 dry-run，均为 **2/2 valid**，并已 promotion 到 `07_ScrapingToolkit/sources/`。

### 暂存 / 后续

- SI / SK: 已补 `porsche_holding_volkswagen_model_overview_scrapling` shared preset，并把 SI / SK Volkswagen drafts 从 `todo.invalid` 切到 Porsche Holding all-model overview 入口；SI 的 Tiguan / T-Roc / Tayron 已定向 dry-run 1/1。
- SK: live validation 已定位到当前真实入口 `https://www.vw.sk/modely`，不是旧的 `volkswagen.sk/modely-a-konfigurator/vsetky-modely`。T-Cross / Tiguan / Taigo / T-Roc / Tayron 已切到新入口并定向 dry-run **1/1**。
- SK: Touareg 已从 blocker 转为 production source：`/touareg/touareg/cenniky-a-katalogy` 公共页稳定暴露 `stock-cennik` / `katalog` / `technické údaje` 三个 PDF，其中 `stock-cennik` 已通过新 `pdf_text` extractor 接入 `07_ScrapingToolkit/sources/volkswagen_touareg_sk.yaml`，live dry-run 为 **11/11 valid**。
- PL: Volkswagen Tiguan / T-Roc 已补 `volkswagen_pl_special_price_scrapling` shared preset，直接落在 `car-technical-data-section-v2` 的首条 `Cena specjalna`，避开后续 finance 月供与 leasing 百分比。
- PL: Touareg 主站模型页不直接给 MSRP，但会跳到 `https://cenniki.volkswagen.pl/Touareg.html`；其前端实际再拉 `pricelists/<uuid>/pricelist.json`，其中 `sections.0.groups[*].items[*].price` 已给出正式价目。现已补 `volkswagen_pl_pricelist_http_json` shared preset，并定向 dry-run **7/7**。
- PL: Tiguan Allspace 虽然主站独立车型页已失效/回跳 Tiguan，但 `cenniki.volkswagen.pl` 的官方价目索引仍保留 `Tiguan-Allspace.html` 与对应 `pricelist.json`。现已切到同一 JSON preset，并定向 dry-run **14/14**。

> 当前 Volkswagen handoff 中，SK Touareg 的单独 source 已经收口。PL 的 Tiguan / T-Roc finance 文案分离、Touareg / Tiguan Allspace 的 `pricelist.json` 接入，以及 SK 的 T-Cross / Tiguan / Taigo / T-Roc / Tayron live validation 现在都已有定向 dry-run 或 production source 落地；后续应回到 Batch A 优先推进。

---

## 1. 策略概述

### 1.1 范围定义

> **只覆盖 SUV 细分市场，每国 Top 30 畅销车型**，
> 用最小工作量拿到最有分析价值的价格数据。

**依据**: JATO 数据中 SUV segment 占各国新车销量 40-60%。覆盖 Top 30 即可代表大部分市场体量。

### 1.2 文件规模

| 类别          |                                            数量 |
| ------------- | ----------------------------------------------: |
| 国家          |                                              21 |
| 每国 YAML     |             ~30（Top 30 车型，少数国家不足 30） |
| 总 draft YAML |                                             629 |
| 存放目录      | `source_drafts/suv_only_country_model_top30/` |

### 1.3 每个 YAML 的标准化流程

```
scaffold 生成 → keyword filling → CSS selector 填充 → dry-run 测试 → promotion to sources/
```

| 阶段              | 自动化程度  | 说明                                              |
| ----------------- | ----------- | ------------------------------------------------- |
| scaffold 生成     | 全自动      | `source_bootstrap.py`，已完成全部 626 个        |
| keyword filling   | 半自动      | 按国家执行脚本，需要人工提供本地化关键词          |
| CSS selector 填充 | 手动/半自动 | Playwright 检查页面结构，同品牌跨国可复用         |
| dry-run 测试      | 全自动      | `run.py --dry-run`，validation pipeline         |
| promotion         | 手动        | 验证通过后从 `source_drafts/` 移入 `sources/` |

---

## 2. 国家批次划分

### Batch A — 当前优先波次

> 与 `07_ScrapingToolkit/msrp_batches/batch_a.yaml` 对齐，后续 scraper 修复、promotion、ingest 都以这 8 国优先。

| ISO | 国家     | 文件数 | 货币 | 当前状态 |
| --- | -------- | -----: | ---- | -------- |
| SE  | 瑞典     |     29 | SEK  | ✅ Keyword / CSS / dry-run 已完成首轮，当前可继续做 promotion / ingest |
| FI  | 芬兰     |     30 | EUR  | 进行中；VW 子集已验证，其余品牌继续补 URL / selector |
| NO  | 挪威     |     30 | NOK  | ✅ Keyword / CSS / dry-run 已完成首轮 |
| DK  | 丹麦     |     30 | DKK  | 进行中；VW 子集已 promotion，其余品牌继续补 URL / selector |
| HU  | 匈牙利   |     31 | HUF  | ✅ Keyword / CSS / dry-run 已完成首轮 |
| HR  | 克罗地亚 |     30 | EUR  | 进行中；Keyword 已完成，继续补稳定品牌 |
| AT  | 奥地利   |     31 | EUR  | ✅ Keyword / CSS / dry-run 已完成首轮 |
| CZ  | 捷克     |     30 | CZK  | ✅ Keyword / CSS / dry-run 已完成首轮；Skoda 5 个 source 已 promotion |

### Batch B — 后续波次

> Batch B 作为后续波次排队，待 Batch A 的高确定性品牌、promotion、ingest 路径稳定后再整体推进。当前文档先按这组国家管理；如后续新增 `msrp_batches/batch_b.yaml`，应以此分组为准。

| ISO | 国家       | 文件数 | 货币 | 当前状态 |
| --- | ---------- | -----: | ---- | -------- |
| CH  | 瑞士       |     31 | CHF  | 已有首轮 keyword / dry-run 基线，归入 Batch B 统一排期 |
| SI  | 斯洛文尼亚 |     30 | EUR  | 进行中；VW 子集已定向补充 |
| RO  | 罗马尼亚   |     30 | RON  | 待启动 |
| DE  | 德国       |     30 | EUR  | 待启动 |
| FR  | 法国       |     30 | EUR  | 待启动；VW 子集已 promotion |
| IT  | 意大利     |     30 | EUR  | 待启动；VW 子集已验证 |
| ES  | 西班牙     |     30 | EUR  | 待启动 |
| NL  | 荷兰       |     30 | EUR  | 待启动 |
| BE  | 比利时     |     30 | EUR  | 待启动；VW 子集已 promotion |
| PL  | 波兰       |     30 | PLN  | 进行中；VW 专项定向推进 |
| PT  | 葡萄牙     |     30 | EUR  | 待启动 |
| IE  | 爱尔兰     |     30 | EUR  | 待启动 |
| GB  | 英国       |     30 | GBP  | 待启动 |

---

## 3. 进度汇总

| 波次 | 范围 | 当前目标 | 当前状态 |
| ---- | ---- | -------- | -------- |
| Batch A | SE / FI / NO / DK / HU / HR / AT / CZ（241 文件） | 继续补高确定性品牌，优先做 promotion / ingest / review 闭环 | **优先推进中** |
| Batch B | CH / SI / RO / DE / FR / IT / ES / NL / BE / PL / PT / IE / GB（391 文件） | 维持排队，等待 Batch A 的模式复用与执行路径稳定 | **后续波次** |
| 总计 | 21 国 / 632 文件 | 先拉起 Batch A 的系统可用覆盖，再扩 Batch B | 执行中 |

> 注：历史上已完成的首轮 keyword filling / dry-run 基线仍然有效，但本页后续执行、汇报、排期都统一使用 Batch A / Batch B 口径。

---

## 4. 汇率自动转换

| 项目                   | 说明                                                |
| ---------------------- | --------------------------------------------------- |
| 模块                   | `jato_scraper/currency_converter.py`              |
| 主 API                 | `open.er-api.com/v6/latest/EUR`（ECB 数据，免费） |
| 备用 API               | `api.exchangerate-api.com/v4/latest/EUR`          |
| 缓存                   | 单次 run 只请求一次，会话级缓存                     |
| 新 RawObservation 字段 | `msrp_value_eur`, `fx_rate_to_eur`              |
| EUR 区国家             | rate=1.0，无需 API 调用                             |
| 已验证货币             | SEK (10.8680), CZK, HUF, NOK, CHF, EUR              |

---

## 5. CSS Selector 填充策略（计划中）

### 5.0 提取管线全流程图

```mermaid
flowchart TD
    A["加载 YAML"] --> B["抓取页面"]

    B --> B1{"tier"}
    B1 -->|http| B2["Fetcher.get"]
    B1 -->|stealth| B3["StealthyFetcher.fetch"]
    B1 -->|dynamic| B4["DynamicFetcher.fetch"]
    B2 --> C{"HTTP OK"}
    B3 --> C
    B4 --> C

    C -->|no| X1{"有 PDF"}
    C -->|yes| D{"extract strategy"}

    D -->|attr_json| E["extract attr_json"]
    E -->|hit| K["RawObservation"]
    E -->|miss| F["extract json_script_selector"]

    F --> F2{"@type matched"}
    F2 -->|yes| F3["read offers.price or lowPrice"]
    F2 -->|no| G["extract css"]
    F3 -->|hit| K
    F3 -->|miss| G

    G --> G1{"vehicle_container exists"}
    G1 -->|yes| G2["parse model trim price"]
    G1 -->|no| X2{"有 PDF"}
    G2 --> G3{"price valid"}
    G3 -->|yes| K
    G3 -->|no| X2

    D -->|none| X2

    X1 -->|yes| P1["find PDF link"]
    X1 -->|no| X1a["fail: no alternate source"]
    X2 -->|yes| P1
    X2 -->|no| X2a["fail: no extractable content"]

    P1 --> P2["download PDF"]
    P2 --> P3{"PDF type"}
    P3 -->|text| P4["pdfplumber or PyMuPDF"]
    P3 -->|scan| P5["OCR: Tesseract or EasyOCR"]
    P4 --> P6["extract model trim price"]
    P5 --> P6
    P6 --> K

    K --> L["build observation"]
    L --> L1["map to jato_model"]
    L1 --> L2["derive powertrain and edition"]
    L2 --> L3["score confidence"]
    L3 --> L4{"match_status"}
    L4 -->|auto_accepted| M1["auto_accepted"]
    L4 -->|review_required| M2["review_required"]
    L4 -->|force_review| M3["force_review"]

    M1 --> N1["POST /v1/msrp/batches"]
    M2 --> N1
    M3 --> N1
    N1 --> N2["write MsrpObservation"]

    N2 --> R1{"needs review"}
    R1 -->|no| CP1["materialize current price"]
    R1 -->|yes| R2{"override matched"}
    R2 -->|yes| R2a["apply MatchOverride"]
    R2 -->|no| R3["create ReviewCase"]
    R2a --> CP1
    R3 --> R4["review in /review/cases"]
    R4 --> R5{"decision"}
    R5 -->|approve| CP1
    R5 -->|reject| R5b["stop"]
    R5 -->|remap| R6["save MatchOverride"]
    R6 --> CP1

    CP1 --> CP2["POST /msrp/current-prices/materialize"]
    CP2 --> CP3["write CurrentPrice"]
    CP3 --> CP4["write PriceHistory"]

    CP4 --> AL1{"change >= 5%"}
    AL1 -->|yes| AL2["flag for review"]
    AL1 -->|no| AL3["normal change"]

    CP3 --> J1["map MSRP to JATO model"]
    J1 --> J2["join sales_12m_sum"]
    J2 --> J3["dashboard and analysis"]
```

### 5.1 核心思路

同品牌不同国家市场的官网结构通常高度相似。利用这一点，可以按品牌模板 "一次适配、跨国复用"：

```
适配 Volvo SE selector → 复用到 Volvo NO/HR/AT/... → 微调差异
适配 BMW DE selector  → 复用到 BMW AT/CH/NL/...  → 微调差异
```

### 5.2 品牌可行性（基于 dry-run 验证）

| 可行性    | 品牌         | 提取方式                  | 覆盖市场             | 备注                                                                      |
| --------- | ------------ | ------------------------- | -------------------- | ------------------------------------------------------------------------- |
| ✅ 已验证 | Toyota       | ld+json (Vehicle)         | SE/CZ/HU/NO/CH/HR/AT | 最可靠，跨市场一致                                                        |
| ✅ 已验证 | Volvo        | ld+json (Product)         | SE/CZ/HU/NO/CH/AT    | CSS `[data-testid]`                                                     |
| ✅ 已验证 | Skoda        | ld+json (Vehicle)         | SE/CZ/CH/NO          | URL 已重构 `/modely/X/X`                                                |
| ✅ 已验证 | KIA          | ld+json (Product)         | SE/CZ/NO             | `hyundai.com` 子域                                                      |
| ✅ 已验证 | Hyundai      | ld+json (Product)         | SE/CZ/NO/CH          | `hyundai.com/{cc}`                                                      |
| ✅ 已验证 | Dacia        | ld+json (Product)         | AT/CZ/HU/CH          | `de.dacia.ch` 等                                                        |
| ✅ 已验证 | Ford         | CSS `.dse-price-module` | AT/CZ/HU/NO          | 跨国一致                                                                  |
| ✅ 已验证 | Peugeot      | CSS `.font-21`          | SE                   | 仅 SE 验证                                                                |
| ✅ 已验证 | Nissan       | ld+json / CSS             | HU/NO                | 部分市场                                                                  |
| ✅ 已验证 | Opel         | CSS（两种模式）           | HU/HR                | HU `.q-value`，HR `.wl-font-36`                                       |
| ✅ 已验证 | Mercedes     | ld+json (Product)         | SE/HU/AT/CH          | SE EQA, AT GLC/EQA, CH GLA/GLE/EQA; HR/NO/CZ 不可用                       |
| ✅ 已验证 | Renault      | ld+json                   | CZ/HU                | 仅部分市场                                                                |
| ❌ 不可用 | Tesla        | 403 Forbidden             | 全部                 | 反爬严格                                                                  |
| ❌ 不可用 | VW           | styled-components         | 大部分               | 仅 SE Tiguan 有 ld+json; ID.4/T-Roc/Tayron 仅 FAQPage                     |
| ❌ 不可用 | BMW          | 无价格 ld+json            | 全部                 | 页面200但仅 VideoObject; URL `/sv/alla-modeller/x-serie/x1/bmw-x1.html` |
| ❌ 不可用 | Audi         | 404 / 无价格              | 大部分               | 同上                                                                      |
| ❌ 不可用 | CUPRA/SEAT   | 404                       | 大部分               | 站点结构不稳定                                                            |
| ❌ 不可用 | MG/Geely/BYD | 连接失败/SSL              | 大部分               | 中国品牌海外站不稳定                                                      |

### 5.2.1 PDF 价目表回退通道（VW Group）

网页爬取失败的品牌，可通过官方 PDF 价目表获取 MSRP。已发现 VW Group（Porsche Holding）在奥地利市场提供统一的 PDF CDN：

| 品牌    | 型号  | 获取方式                             | PDF 文件                                         |
| ------- | ----- | ------------------------------------ | ------------------------------------------------ |
| Audi AT | Q8    | `/q8/q8/preisliste` 页面           | `2145_audi_q8_pa_katalog_inkl__preisliste.pdf` |
| Audi AT | Q3    | `/q3/q3/preisliste` 页面           | `2503_q3_preisliste_final.pdf`                 |
| SEAT AT | Arona | `/kataloge-preislisten/arona` 页面 | `arona_neu_dupl.pdf`                           |
| SEAT AT | Ateca | `/kataloge-preislisten/ateca` 页面 | `ateca_dupl.pdf`                               |

**CDN 模式**:

```
https://gpt-live.porsche.co.at/at/brand/{BRAND_CODE}/pricelist/{FILENAME}.pdf
https://porschegpt-prod.etn.cz/at/brand/{BRAND_CODE}/pricelist/{FILENAME}.pdf
```

- 品牌代码: `A`=Audi, `S`=SEAT, 推测 `V`=VW, `C`=CUPRA, `K`=Skoda
- Porsche Holding = VW Group 奥地利分销商，统一承载所有 VW 集团品牌价目表
- **提取方案**: pdfplumber 文本型 PDF → 表格解析 → 提取 model/trim/price → RawObservation

**未发现 PDF 的品牌**: BMW（404）、Tesla（403）、Hyundai（404）、BYD/MG/Geely（无价目表页面）

### 5.3 工具链

- **CSS Selector Toolchain**: `jato_scraper/css_toolchain/` 提供交互式 Playwright inspector 脚本
- **Playwright codegen**: 可录制浏览器操作自动生成 selector
- **dry-run 验证**: `python run.py --sources <yaml> --dry-run -v` 即时验证

---

## 6. 与旧计划的关系

本文档 **替代** 之前的 `MSRP_COUNTRY_BRAND_SOURCE_PRIORITY_PLAN_2026-04-11.md`。

| 维度       | 旧计划 (04-11)                 | 新计划 (04-12)                   |
| ---------- | ------------------------------ | -------------------------------- |
| 范围       | 国家 × 品牌系族               | SUV-only × country model top 30 |
| 批次单位   | 10 个 brand-family source 每批 | 按 Batch A / Batch B 两波推进，波内按国家 pack 执行 |
| draft 数量 | 估算 ~1000+                    | 实际 626                         |
| 汇率       | Frankfurter API（未集成）      | open.er-api.com（已集成并验证）  |
| 状态       | 已归档                         | 当前执行中                       |

---

## 7. Dry-run 详细结果（2026-04-12）

### 7.1 各市场通过清单

| 市场 | 通过率                  | 通过品牌                                                                           |
| ---- | ----------------------- | ---------------------------------------------------------------------------------- |
| SE   | **21/29** (72.4%) | KIA×4, Skoda×2, Toyota×4, Volvo×6, VW Tiguan, Peugeot×3, Mercedes EQA         |
| CZ   | **19/30** (63.3%) | Dacia×2, Ford×2, Hyundai×2, KIA×2, Renault, Skoda×5, Toyota×3, Volvo×2      |
| HU   | **16/31** (51.6%) | Dacia×2, Ford×2, Mercedes EQA, Nissan×2, Opel, Renault, Toyota×5, Volvo×2     |
| NO   | **13/30** (43.3%) | Hyundai×2, KIA, Nissan, Peugeot, Skoda×2, Toyota, Volvo×3, Ford×2              |
| CH   | **15/31** (48.4%) | Dacia×2, Hyundai×2, Mercedes EQA/GLE, Skoda×4, Toyota×2, Volvo×2              |
| AT   | **8/31** (25.8%)  | Dacia×2, Ford Puma, Mercedes EQA/GLC, Peugeot 3008, Toyota Yaris Cross, VW Tiguan |
| HR   | **5/30** (16.7%)  | Nissan, Opel, Toyota×3                                                            |

### 7.2 修复手段总结

| 修复类型           | 数量 | 示例                                                                |
| ------------------ | ---: | ------------------------------------------------------------------- |
| URL 域名/路径修正  |  ~25 | Dacia `www→de.dacia.ch`，Skoda `/modely/X→/modely/X/X`        |
| CSS 提取策略新增   |  ~12 | Ford `.dse-price-module`，Peugeot `.font-21`，Opel `.q-value` |
| ld+json @type 扩展 |    1 | 支持 Vehicle/AggregateOffer/IndividualProduct                       |
| lowPrice fallback  |    1 | VW AggregateOffer `offers.lowPrice`                               |
| HTML 标签清洗      |    1 | CSS `.get()` 返回 HTML，需 `_HTML_TAG_RE.sub()`                 |

### 7.3 确认不可用的品牌

> 以下品牌经 2026-04-12 多轮 StealthyFetcher 探测确认不可用，已尝试所有已知 URL 模式。

| 品牌                   | 失败原因           | 探测详情                                                                                              |
| ---------------------- | ------------------ | ----------------------------------------------------------------------------------------------------- |
| Tesla                  | 403 Forbidden      | 官网+库存 API 均返回 403                                                                              |
| BMW                    | 页面无价格 ld+json | 页面 200 OK 但 ld+json 仅有 VideoObject(5-8块); URL 路径 `/sv/alla-modeller/x-serie/x1/bmw-x1.html` |
| Audi                   | 301 重定向         | q4-e-tron 等页面 301 到非价格页                                                                       |
| CUPRA                  | 404/308            | formentor/terramar 均 308 重定向或 404                                                                |
| SEAT                   | 404                | 所有尝试的 URL 均 404                                                                                 |
| Polestar               | 无价格 ld+json     | 页面 200 OK, ld+json 仅 VideoObject/BreadcrumbList; h2 有租赁价 "5 495 kr/mån" 但非 MSRP             |
| Mazda                  | 无可提取价格       | SE/CZ 页面 200 OK 但无 ld+json, CSS 仅有配置器按钮                                                    |
| VW (非 Tiguan)         | 无 Vehicle ld+json | ID.4/T-Roc/Tayron 页面 200 OK 但 ld+json 仅 FAQPage; body 中 "kr" 均为租赁价                          |
| MG/Geely/BYD           | 连接失败/SSL       | 中国品牌海外站不稳定                                                                                  |
| Porsche                | 无价格             | 配置器模式，无标准 DOM 价格                                                                           |
| Xpeng/Jaecoo/KGM/OMODA | 连接失败           | 新品牌海外站点不稳定                                                                                  |

### 7.4 DynamicFetcher (Playwright 全渲染) 实验结果

现有 dry-run 使用 `tier: stealth` (StealthyFetcher)。对标记为"不可用"的品牌，切换 `tier: dynamic` (DynamicFetcher, Playwright 全渲染) 重新探测：

#### VW — 已发现价格

| 车型     | 市场 | stealth 结果         | dynamic 结果                    | 发现价格         |
| -------- | ---- | -------------------- | ------------------------------- | ---------------- |
| VW ID.4  | SE   | ❌ 仅 FAQPage ld+json | ✅ DOM 含 PriceVatIncluded 属性 | **538,700 SEK**  |
| VW T-Roc | SE   | ❌ 仅 FAQPage ld+json | ✅ DOM 含 PriceVatIncluded 属性 | **389,900 SEK**  |

价格藏在 JS 渲染后的 DOM data 属性结构中：

```json
{"PriceVatIncluded", "label": "Pris från", "value": "538 700"}
```

**操作计划**：
1. VW SE 全系 YAML 配置 `tier: dynamic`
2. 新增提取规则匹配 `PriceVatIncluded` data 属性 → `attr_json` 或 CSS 策略
3. 验证后可推广至 VW 其他市场 (NO/CZ/HU/CH)

#### 其他品牌 — DynamicFetcher 仍无法突破

| 品牌  | 市场 | dynamic 结果               | 判定          |
| ----- | ---- | -------------------------- | ------------- |
| BMW   | SE   | 200 OK 但仅通用数字无 MSRP | ❌ 不可突破   |
| Tesla | SE   | 403 Forbidden              | ❌ 反爬严格   |
| VW    | AT   | 404 页面不存在             | ❌ URL 失效   |
| BMW   | AT   | 404 页面不存在             | ❌ URL 失效   |
| Audi  | AT   | 404 页面不存在             | ❌ URL 失效   |

#### 预期提升

VW 切换 dynamic 后，理论上可补回一部分此前失败的 YAML（重点是 ID.4、T-Roc、Tayron、T-Cross 一类页面），但具体增幅必须以复跑结果为准，不能直接把实验样本外推成正式通过率。

### 7.5 LLM 集成评估

当前工具箱中 **无任何 LLM 集成代码**。以下为各场景评估：

| 应用场景                 | 方案                                    | 可行性  | ROI  | 备注                                          |
| ------------------------ | --------------------------------------- | ------- | ---- | --------------------------------------------- |
| PDF 表格理解             | LLM 解析 OCR输出 → 结构化价格表        | ✅ 高   | ⭐⭐⭐ | VW Group AT 大量 PDF，人工 pdfplumber 成本高  |
| 非结构化 HTML 提取       | LLM 分析渲染后 DOM → 定位价格节点       | ⚠️ 中等 | ⭐⭐  | BMW/Mazda 等有页面但无结构化数据的品牌        |
| CSS Selector 自动建议    | LLM 分析页面 → 推荐 selector            | ✅ 高   | ⭐⭐  | 加速新品牌适配，减少手工 Playwright 调试      |
| 品牌/车型名模糊对齐      | LLM 匹配 source name → JATO model code | ⚠️ 中等 | ⭐   | confidence 低时辅助 human review              |

**建议优先级**：PDF 解析 (P1) → DynamicFetcher VW 适配 (P1) → LLM HTML 提取 (P2) → CSS 自动建议 (P3)

### 7.6 Batch A 优先落地策略

Batch A 不必等到 dry-run 全绿后再入系统。当前后端已经支持“先落 observation，再走 review”的链路，和 XC60 的处理思路一致。

#### 路径 A：现有 valid observation 直接入库

- 入口脚本：`03_Scripts/batch_ingest.py`
- 行为：对指定国家重新执行非 dry-run 抓取；使用 `--auto-review --materialize` 时，会在 ingest 后继续触发 `POST /v1/review/cases/auto-resolve` 与 `POST /v1/msrp/current-prices/materialize`
- 条件：只要某个 source 产出 `report.valid`，就会 `POST /v1/msrp/batches`
- 结果：
    - `auto_accepted` 直接写入 `CurrentPrice`
    - `review_required` 自动创建 `ReviewCase`
    - `override_applied` 自动复用历史规则
    - open `ReviewCase` 若后续已能命中 active link / override，可被 auto-resolve 自动 approve 并 materialize

这条路适合已经能抽到价格、但匹配信心不足的样本。它们不需要继续卡在 scraper 侧，可以先进系统审核。

#### 路径 B：人工整理价格后手工入批次

对网页抓不到、但 PDF 或人工核验能拿到价格的品牌，可跳过 scraper DOM 提取，直接构造 observation payload 提交到 `POST /v1/msrp/batches`。

适用对象：

- VW Group AT 的 PDF 价目表
- 其他能人工确认 MSRP、但网页结构不稳定的样本

建议做法：

1. 先把 PDF 或人工采集结果整理成结构化表
2. 将 `match_status` 统一打成 `review_required`
3. 批量入库后在 `review/cases` 页面集中审核

这条路本质上是把“抓取问题”与“业务审核”拆开，先保证数据能进系统。

#### 路径 C：按品牌白名单推进国家落地

Batch A 建议不要按“国家全量是否完成”推进，而是按“品牌白名单”推进：

- 第一组：Toyota / Dacia / Skoda / Volvo / Ford
- 第二组：Mercedes / Hyundai / KIA / Peugeot / Nissan / Opel
- 第三组：VW dynamic 试点

这样可以先把确定性高的 observation 批量导入，尽快让 Batch A 各国在系统里出现可审核数据，而不是被 Tesla、BMW、Audi 这类难点品牌拖住整波进度。Batch B 保持排队，不与 Batch A 抢执行优先级。

#### 推荐执行顺序

1. 先用 `batch_ingest.py batch_a --auto-review --materialize` 导入 Batch A 已经有 valid observation 的国家和品牌
2. 再把 VW dynamic 试点补进去，观察能否新增一批 review case
3. 对 PDF 可得品牌，补一条“人工整理后入库”的旁路
4. 在前端 review workbench 中集中做 remap / approve / reject

#### 2026-04-21 CZ live ingest 实际结果

- 已执行：`python 03_Scripts/batch_ingest.py cz --auto-review --materialize`
- 首轮 country-pack 结果：**14/30 OK, 11 empty, 5 failed**
- 空结果/失败主要来自旧 draft 的 404、连接关闭、或未配置 extractor 的站点；不是 batch 脚本本身中断
- 首轮结束时：`auto-review approved=0`、`materialize candidates=0`
- 随后已补跑 5 个 promoted `Skoda CZ` production sources，当前 `review/cases?country=CZ` 总量为 **146**
- `current-prices?country=Czechia` 仍为 **0**，说明当前 observation 主要仍处于 `review_required` 队列，尚未通过 active link / override 自动出队，这与现有 resolver 设计一致
- 这次实跑已经证明 **抓取 → ingest → review case 创建 → auto-review/materialize 调用** 链路在本地后端 + Docker DB 上可真实执行；下一步重点应转到 review workbench 的批量 approve/remap，而不是继续只做 dry-run

---

## 8. 近期里程碑

| 里程碑 | 目标                                      | 状态                       |
| ------ | ----------------------------------------- | -------------------------- |
| M1     | Batch A country pack 对齐（8 国）         | ✅ 已完成（与 `msrp_batches/batch_a.yaml` 对齐） |
| M2     | Batch A 高确定性品牌 dry-run / promotion  | 进行中（DK VW、CZ Skoda 等子集已落地；Batch B 的 SK Touareg PDF blocker 已额外收口） |
| M3     | Batch A valid observation ingest          | 进行中（CZ 已完成 live ingest 实跑；review queue 已落地，待批量审核/映射） |
| M4     | Batch B 首轮 keyword / CSS / dry-run      | 待启动                     |
| M5     | Batch B promotion / ingest                | 待启动                     |
| M6     | 全部 626 个 draft YAML dry-run pass       | 长期目标                   |
