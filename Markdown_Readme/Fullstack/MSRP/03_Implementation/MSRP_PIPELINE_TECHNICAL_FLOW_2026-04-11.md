# MSRP Pipeline — Technical Flow & PR Summary

Date: 2026-04-11

Status: Implemented & Tested (79 backend + 16 frontend tests passing)

关联文档：
- PRD: [MSRP_Official_Price_Enrichment_PRD_2026-04-10.md](../01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md)
- 技术设计: [MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md](../02_Research_Design/MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md)
- XC60 执行记录: [MSRP_XC60_EXECUTION_RESULT_2026-04-11.md](../04_Execution/MSRP_XC60_EXECUTION_RESULT_2026-04-11.md)

---

## 1. 端到端技术流程图

```
┌──────────────────────────────────────────────────────────────────┐
│                     JATO Sales Data (Parquet)                    │
│         04_Processed_data/partitioned_dataset_v1/                │
│                                                                  │
│   ┌─ 按 (国家, 品牌, Model) 聚合前 12 个月销量 ──┐              │
│   │   SELECT country, brand, model,               │              │
│   │          SUM(sales) as total_sales             │              │
│   │   GROUP BY country, brand, model               │              │
│   │   ORDER BY total_sales DESC                    │              │
│   │   LIMIT top_n                                  │              │
│   └────────────────────┬───────────────────────────┘              │
└────────────────────────┼─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│              Candidate Selection (scope by sales)                │
│                                                                  │
│  每个国家：取前 12 个月 sales 排名 Top N 车型                    │
│  → 生成 (country, brand, model) 抓取候选列表                    │
│  → 匹配 07_ScrapingToolkit/sources/*.yaml 已有源                │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│               Scheduling (夜间执行窗口)                          │
│                                                                  │
│  YAML 配置:                                                      │
│    schedule:                                                     │
│      frequency: weekly                                           │
│      run_window_start: "00:00"                                   │
│      run_window_end:   "05:00"                                   │
│      preferred_weekday: monday                                   │
│                                                                  │
│  服务器 cron / systemd timer 在窗口内触发                        │
│  runner.py --source volvo_se_xc60_build_scrapling                │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                   Web Extraction Layer                            │
│                                                                  │
│  07_ScrapingToolkit/jato_scraper/extractors/scrapling_web.py     │
│                                                                  │
│  1. Fetcher (http / stealth / dynamic)                           │
│     └─ Scrapling library → 拿到 HTML DOM                        │
│                                                                  │
│  2. CSS Selector 提取                                            │
│     └─ vehicle_container → trim / price / model                  │
│     └─ exclude_if_selector 过滤非 trim 卡片                     │
│     └─ exclude_price_prefixes 过滤 "Från" 等家族价格            │
│                                                                  │
│  3. Powertrain Evidence Engine (2-pass)                           │
│     Pass 1: trim text only → 精确匹配 (B5→MHEV, T8→PHEV)       │
│     Pass 2: full text incl. URL → 兜底匹配                      │
│     → 返回 (powertrain, source, ambiguous)                       │
│                                                                  │
│  4. Edition Detection                                            │
│     └─ keyword 匹配 → special flag (Black Edition = special)    │
│     └─ Nordic Edition = non-special                              │
│                                                                  │
│  5. Confidence Scoring (weighted_profile_v1)                     │
│     base_score + Σ bonuses → clamp(0, 1)                        │
│                                                                  │
│  6. 3-Tier Auto-Accept Gate                                      │
│     ┌─ score < review_threshold (0.95) ────→ review_required     │
│     │  tier: "below_threshold"                                   │
│     ├─ 0.95 ≤ score < 0.98 ──→ check constraints:               │
│     │  ├─ powertrain match?                                      │
│     │  ├─ powertrain ambiguous?                                  │
│     │  ├─ special edition?                                       │
│     │  └─ all pass → auto_accepted (tier: "semi_auto")           │
│     │     any fail → review_required (tier: "constraint_failed") │
│     └─ score ≥ semi_auto_threshold (0.98) + constraints pass     │
│        → auto_accepted (tier: "full_auto")                       │
│                                                                  │
│  → 输出: List[RawObservation]                                    │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                   Validation Layer                                │
│                                                                  │
│  jato_scraper/validation.py                                      │
│  - 价格 > 0 且在合理范围内                                       │
│  - 币种存在且合法                                                │
│  - model / trim 非空                                             │
│  → 输出: BatchValidationReport (valid + rejected)                │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                FX Normalization Layer                             │
│                                                                  │
│  两阶段汇率转换:                                                 │
│                                                                  │
│  ── 阶段 1: Scraping Toolkit 内即时转换 ──                       │
│  jato_scraper/currency_converter.py (2026-04-12 新增)            │
│                                                                  │
│  extract → validate → enrich_observations_with_eur()             │
│  - 调用免费汇率 API: open.er-api.com (ECB 数据源, 无需 key)     │
│  - 备用 API: exchangerate-api.com                                │
│  - 会话级缓存: 一次 run 只请求一次汇率表(166 种货币)            │
│  - 写入 RawObservation:                                          │
│    msrp_value_eur = msrp_value / fx_rate                         │
│    fx_rate_to_eur = 1 EUR 兑多少本地货币                         │
│  - EUR 币种时 rate=1.0, 不调 API                                 │
│                                                                  │
│  ── 阶段 2: Backend Ingest 时冻结 ──                             │
│  backend/app/services/fx_service.py                              │
│                                                                  │
│  策略: FX 汇率随抓取时间冻结                                     │
│  - 首次抓取时, 用 observed_at_utc 日期向 API 获取                │
│  - 汇率记录在 observation 上: fx_rate_to_eur / fx_rate_as_of_date│
│  - 后续同 source_currency + source_msrp_value 未变时,            │
│    不因汇率浮动更新 EUR 价格                                     │
│                                                                  │
│  关键原则:                                                        │
│  ⚠️  价格变动检测只比较 source_msrp_value (原币)                 │
│  ⚠️  不比较 current_msrp_value (EUR) 避免汇率涨跌误判           │
│                                                                  │
│  Fallback: 当 API 不可达时, 使用 STATIC_RATES_PER_EUR           │
│  Source 标记: "open.er-api.com" / "static-fallback" / "identity" │
└────────────────────────┬─────────────────────────────────────────┘
                         │
                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                Backend Ingest Pipeline                            │
│                                                                  │
│  POST /v1/msrp/batches                                           │
│  backend/app/services/msrp_workflow_service.py                   │
│                                                                  │
│  1. Create ScrapeBatch record                                    │
│  2. For each observation:                                        │
│     a. Create MsrpObservation in msrp.observations               │
│     b. If match_status = "auto_accepted":                        │
│        → materialize_current_price_from_observation()            │
│     c. If match_status = "review_required":                      │
│        → ★ Override Feedback Lookup (2026-04-11 新增)            │
│          查询 review.match_overrides:                            │
│          WHERE (country, brand, jato_model, jato_trim)           │
│          AND valid_from ≤ observation_date                       │
│          AND (valid_to IS NULL OR valid_to ≥ observation_date)   │
│          → 匹配到 → 应用 override 的 official_model/trim        │
│              设 match_status = "override_applied"                │
│              → materialize_current_price_from_observation()      │
│          → 未匹配 → _ensure_review_case()                       │
│  3. Commit transaction                                           │
│                                                                  │
│  ★ Price History Recording (2026-04-11 新增)                     │
│  materialize_current_price 检测到原币价格变动时:                  │
│  → 关闭 msrp.price_history 当前开放区间 (设置 valid_to_utc)     │
│  → 开启新区间 (valid_from_utc = observed_at_utc)                │
│  输出: "(country,brand,model,trim) 从 X 到 Y 时间段价格为 Z"    │
└──────────────┬────────────────────┬──────────────────────────────┘
               │                    │
               ▼                    ▼
┌──────────────────────┐ ┌─────────────────────────┐
│  current_prices      │ │  review_cases           │
│                      │ │                         │
│  Unique key:         │ │  Human review queue     │
│  (country, brand,    │ │  - open / approved /    │
│   jato_model,        │ │    rejected /           │
│   jato_trim)         │ │    closed_superseded    │
│                      │ │                         │
│  价格变动检测:        │ │  一键 approve → 触发    │
│  比较 source_msrp    │ │  materialize            │
│  + source_currency   │ │                         │
│  (NOT EUR converted) │ │                         │
└──────────┬───────────┘ └─────────┬───────────────┘
           │                       │
           ▼                       ▼
┌──────────────────────────────────────────────────────────────────┐
│                    Frontend (React + Vite)                        │
│                                                                  │
│  06_AppPlatform/frontend/src/pages/MsrpPage.tsx                  │
│                                                                  │
│  Current Prices 表格:                                            │
│  ┌──────────────────────────────────────────────────────────────┐ │
│  │Country│Brand│JATO Model│JATO Trim│JATO PT│Official│MSRP(EUR)│ │
│  │       │     │          │         │       │ Model  │(整数)    │ │
│  ├───────┼─────┼──────────┼─────────┼───────┼────────┼─────────┤ │
│  │Sweden │Volvo│XC60      │Ultra    │PHEV   │XC60    │67,217   │ │
│  │Sweden │Volvo│XC60      │Core NE  │PHEV   │XC60    │49,557   │ │
│  └──────────────────────────────────────────────────────────────┘ │
│                                                                  │
│  + 「详情」按钮 → Detail Drawer:                                 │
│    - Edition / Powertrain / FX / Confidence / Source URL 等      │
│                                                                  │
│  06_AppPlatform/frontend/src/pages/ReviewCasesPage.tsx           │
│  - 低置信度人工复核                                               │
│  - approve / reject → 触发 materialize                           │
└──────────────────────────────────────────────────────────────────┘
```

---

## 2. 数据补充通道: OCR / API → Markdown

当价格源不是标准 HTML 页面时，可用两种补充通道:

| 通道 | 工具 | 场景 | 集成方式 |
|:---|:---|:---|:---|
| **PDF OCR** | [Chandra OCR 2](https://github.com/datalab-to/chandra) | 品牌发布 PDF 价格表 | 新建 `PdfOcrExtractor`，输出 Markdown → 解析为 `RawObservation` |
| **API → Markdown** | 品牌官方 API (JSON) | 有些品牌对外暴露配置器 API | `http_json` extractor 拿 JSON → 可选转 Markdown 归档为可读证据 |

两种通道最终都进入同一个 validation → FX → ingest → gate pipeline：

```
PDF / API 数据源
      │
      ▼
PdfOcrExtractor / HttpJsonExtractor
      │
      ▼
List[RawObservation]  ←── 与 scrapling_web 输出格式一致
      │
      ▼
(同一套 validation + FX + ingest + gate pipeline)
```

---

## 3. Scraping Scope 策略: 按 JATO 销量排名

### 原则

不是盲目 "每个国家 Top 15 品牌"，而是:

1. 读取 JATO Parquet 数据 (04_Processed_data)
2. 按 (国家, 品牌, Model) 聚合**前 12 个月**的 Sales 销量
3. 每个国家取 **Top N 热销车型**（不是品牌）
4. 匹配已有 YAML 源 → 缺失的标记为待开发

### 时间窗定义

```
scope_window = [max_month - 11, max_month]
```

其中 `max_month` 是该国家 JATO 数据中最新的月份。

### 示例

假设 JATO Sweden 最新月为 2026-03:
- 窗口 = 2025-04 ~ 2026-03
- Top 5 销量: XC60, Model Y, Golf, 3-Series, XC40
- XC60 已有 YAML → 自动抓取
- Model Y / Golf / 3-Series / XC40 → 待开发 extractor

---

## 4. 夜间调度配置

### YAML 声明式调度

每个 source YAML 新增 `schedule` 段:

```yaml
schedule:
  frequency: weekly           # weekly / daily / manual_only
  run_window_start: "00:00"   # 服务器本地时间
  run_window_end: "05:00"
  preferred_weekday: monday
```

### 服务器执行

在生产服务器上通过 systemd timer 或 cron 在 00:00-05:00 窗口触发:

```cron
0 0 * * 1  cd /opt/jato && python 07_ScrapingToolkit/run.py --all --trigger scheduled 2>&1 | tee -a logs/scrape.log
```

Runner 在执行时检查 `schedule.run_window_*`，只在窗口内执行该源。

---

## 5. FX 汇率策略（2026-04-12 更新）

### 两阶段转换

#### 阶段 1: Scraping Toolkit 即时转换（新增）

在 `jato_scraper/currency_converter.py` 中完成，extract → validate 之后、ingest 之前：

```python
# runner.py 中调用链
report = validate_observations(observations, country=...)
enrich_observations_with_eur(report.valid)   # ← 新增
```

实现细节：

- 免费汇率 API：`https://open.er-api.com/v6/latest/EUR`（ECB 数据源，无需注册）
- 备用 API：`https://api.exchangerate-api.com/v4/latest/EUR`
- 会话级缓存：一次 run 只请求一次 API，获取 166 种货币的 EUR 汇率
- 写入 `RawObservation.msrp_value_eur` 和 `RawObservation.fx_rate_to_eur`
- EUR 币种时 rate = 1.0，不调 API

已验证结果：

| 国家 | 车型 | 本地价格 | 货币 | EUR 价格 | 汇率 |
|------|------|---------|------|---------|------|
| SE | XC60 Core Nordic Edition | 569,900 | SEK | 52,438 | 10.868 |
| SE | XC60 Plus Nordic Edition | 599,900 | SEK | 55,199 | 10.868 |
| SE | XC60 Ultra | 773,000 | SEK | 71,126 | 10.868 |

#### 阶段 2: Backend Ingest 时冻结

在 `backend/app/services/fx_service.py` 中完成：

- 首次 ingest 时，如 Toolkit 已提供 `fx_rate_to_eur` 则直接采用
- 汇率随该次 observation 冻结，不再变动
- 价格变动检测只比较 `source_msrp_value`（原币）

### 问题与解决

如果每次 materialize 都用最新汇率重算 EUR 价格，会出现:
- 官网 SEK 价格没变（569,900 SEK）
- 但 EUR/SEK 汇率从 10.87 变成 10.92
- 系统误判为"涨价" → 生成错误预警

解决方案：**只有原币涨跌才触发预警，EUR 转换值仅用于展示和跨国对比。**

---

## 6. Canonical Powertrain Taxonomy

8 种 JATO 标准动力总成分类:

| Code | Full Name | 典型关键词 |
|:---|:---|:---|
| BEV | Battery Electric Vehicle | electric, bev, ev |
| FCV | Fuel Cell Vehicle | fcv, fcev, fuel cell, hydrogen |
| HEV | Hybrid Electric Vehicle | hybrid (alone) |
| ICE | Internal Combustion Engine | petrol, diesel, gasoline |
| LPG | Liquefied Petroleum Gas | lpg, autogas |
| MHEV | Mild Hybrid Electric Vehicle | mild hybrid, mhev, b5, b6 |
| PHEV | Plug-in Hybrid Electric Vehicle | plug-in, phev, recharge, t6, t8 |
| REEV | Range-Extended Electric Vehicle | reev, erev, range extender |

检测引擎: 2-pass, trim text 优先 → 消除 URL "xc60-hybrid" 歧义。

---

## 7. PR Change Summary

### 改动文件清单

| 文件 | 改动说明 |
|:---|:---|
| `07_ScrapingToolkit/jato_scraper/extractors/scrapling_web.py` | 8 种 powertrain 默认规则 + powertrain_ambiguous 字段 + 2-pass 检测 + 3-tier gate |
| `07_ScrapingToolkit/sources/volvo_se_xc60.yaml` | MHEV 规则 + 收紧 gate (0.95/0.98) + schedule 段 |
| `07_ScrapingToolkit/sources/_template.yaml` | 新增 schedule 段模板 |
| `06_AppPlatform/backend/app/services/msrp_workflow_service.py` | 价格变动检测改为比较 source_msrp_value 原币 |
| `06_AppPlatform/frontend/src/pages/MsrpPage.tsx` | 新增 JATO Powertrain 列 + 详情 Drawer |
| `06_AppPlatform/frontend/src/utils/msrpCurrentPrice.ts` | MSRP 显示为整数 (Math.round) |
| `06_AppPlatform/backend/tests/unit/test_scraper_scrapling.py` | 新增 5 个 gate 测试 + MHEV 检测测试 |
| `03_Scripts/backfill_xc60_structured_fields.py` | 历史数据 backfill + review case 清理脚本 |

### 核心改动逻辑

1. **Powertrain Evidence Engine**: 从 "固定写死 PHEV" 改为 "trim 文本优先的 2-pass 证据匹配"，B5→MHEV, T8→PHEV
2. **3-Tier Gate**: 从单阈值 0.90 改为 `review_threshold=0.95` + `semi_auto_threshold=0.98` + 约束检查
3. **FX 原币检测**: 价格变动只比较 `source_msrp_value` + `source_currency`，不比较 EUR 转换值
4. **前端**: Current Prices 表新增 JATO Powertrain 列 + MSRP 显示整数 + 详情抽屉

### 测试覆盖

- Backend: 79 tests passing (含 25 scraper tests)
- Frontend: 16 tests passing
- 无 lint errors
