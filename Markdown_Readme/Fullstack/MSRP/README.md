# MSRP 文档索引

日期：2026-04-12

这个目录把 Fullstack 下所有 MSRP 功能开发文档按功能阶段归档，避免再散落在根目录。

## 目录结构

| 阶段 | 目录 | 说明 |
| --- | --- | --- |
| 需求 | `01_Requirements/` | 产品目标、范围、验收口径 |
| 预研 / 设计 | `02_Research_Design/` | 技术路径、实现设计、路线选择 |
| 实现 | `03_Implementation/` | 已落地的 pipeline、currency converter、review loop、price history |
| 执行验证 | `04_Execution/` | 真实站点抓取 POC 测试、keyword filling 批次记录 |
| Backlog | `05_Backlog/` | SUV country model top30 批次计划与后续开发入口 |

## 当前进度快照

| 市场 | 通过 | 总数 | 通过率 |
| ------ | -----: | -----: | -------: |
| SE | 20 | 29 | 69.0% |
| CZ | 19 | 30 | 63.3% |
| HU | 15 | 30 | 50.0% |
| NO | 13 | 30 | 43.3% |
| CH | 13 | 30 | 43.3% |
| AT | 7 | 30 | 23.3% |
| HR | 5 | 30 | 16.7% |
| **合计** | **92** | **209** | **44.0%** |

> Batch 1+2（7 国）已完成 keyword filling + CSS/URL 修复 + dry-run 验证。
> 详见 `05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md` § 7。

## 推荐阅读顺序

1. `01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md` — 产品需求
2. `02_Research_Design/TECH_PATH_SCRAPE_AND_UPLOAD.md` — 技术路径选型
3. `02_Research_Design/MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md` — 技术设计
4. `03_Implementation/MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md` — 端到端技术流（核心）
5. `03_Implementation/MSRP_OVERRIDE_AND_PRICE_HISTORY_2026-04-11.md` — Override 与价格时间序列
6. `03_Implementation/MSRP_BRAND_FAMILY_MODEL_RULES_2026-04-11.md` — 多车型映射规则
7. `03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md` — **多 version / 多动总 / 配置 diff / 多源对账（当前架构主入口）**
8. `../03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md` — JATO sales × MSRP 的跨源 join 设计
9. `03_Implementation/CSS_SELECTOR_TOOLCHAIN_2026-04-11.md` — CSS Selector 填充工具链
10. `05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md` — 批量执行入口
11. `04_Execution/MSRP_KEYWORD_FILLING_AND_POC_RESULTS_2026-04-12.md` — Keyword 填充记录
12. `04_Execution/MSRP_SWEDEN_VOLKSWAGEN_BATCH_RESULT_2026-04-18.md` — Sweden Volkswagen ID.4 + Tayron rollout 收口结果
13. `04_Execution/MSRP_XC60_EXECUTION_RESULT_2026-04-11.md` — XC60 pilot 结果
14. `04_Execution/MSRP_XC60_SCRAPE_PREVIEW_2026-04-11.md` — XC60 预览评估（已执行）
15. `04_Execution/MSRP_BATCH1_SOURCE_RESEARCH_2026-04-11.md` — Batch-1 品牌调研
16. ~~`05_Backlog/MSRP_COUNTRY_BRAND_SOURCE_PRIORITY_PLAN_2026-04-11.md`~~ — 已归档，被 #10 替代

## 当前推荐入口（2026-04-18）

如果你当前关注的是：

| 主题 | 优先看 |
| --- | --- |
| MSRP 抓取通过率、人审优化 | `03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md` |
| JATO 销量 × MSRP 定位定价联表 | `../03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md` |
| 多 version / 多动总 / 配置差异 | `03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md` § 3 |
| 批量 source 执行现状 | `05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md` |
| 抓取平台扩展到 news/policy/spec | `../02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md` |

## 当前开发入口

- 文档入口：`05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md`
- SUV 候选覆盖报告：`../../../04_Processed_data/msrp_candidate_scope/suv_only/candidate_scope_report.md`
- SUV source 草稿产物：`../../../07_ScrapingToolkit/source_drafts/suv_only_country_model_top30/`
- 生产 source：`../../../07_ScrapingToolkit/sources/`
- 汇率转换模块：`../../../07_ScrapingToolkit/jato_scraper/currency_converter.py`

## 当前策略

1. **SUV-only 口径优先**：当前执行目标聚焦 `suv_only_country_model_top30`，每个国家按 JATO 12 个月销量排名取 Top 30 SUV 车型生成 source draft YAML。全市场口径 (`all_market`) 仅作为参考保留，不参与当前执行。
2. **按国家批次推进 keyword filling**：每批国家的 draft YAML 按标准化流程填充本地化关键词（动力总成、版本、价格带），填完后零 `TODO_KEYWORD` 残留才算可进入 CSS selector 填充阶段。
3. **pipeline 内自动汇率转换**：scraping toolkit 的 `currency_converter.py` 在 extract → validate 之后自动调用免费汇率 API (`open.er-api.com`) 将本地货币转 EUR，写入 `msrp_value_eur` / `fx_rate_to_eur`，无需手工换算。
4. brand-family 级 source 优先，规则化配置 `model_rules`、`fixed_model`、`fixed_jato_model`、`fixed_jato_powertrain`、`copy_trim_to_jato_trim`、`edition_rules`、`powertrain_rules`、`price_band_bonuses`。
5. review override 已回流到 future matching；重复 case 优先靠规则与 override 消灭，而不是先引入 LLM。
6. price history 负责价格区间时间线，override 负责 trim 命名映射，两者分离。
7. 生产 source 只接收真实 MSRP / 官方购车价页面；月供 / leasing offer 页面只能作为 research 线索，不能直接接入 MSRP pipeline。
8. 历史 `top20_batch1`、split brand backlog 草稿、旧 `all_market` country×brand 优先级排名均已废弃；后续只保留 `suv_only_country_model_top30` 目录。

## 变更日志

### 2026-04-18

1. **Sweden Volkswagen configurator batch 收口完成**：ID.4 与 Tayron 已完成 apply、DB 核验、Pricing/MSRP 前端核验，并补 execution 记录到 `04_Execution/MSRP_SWEDEN_VOLKSWAGEN_BATCH_RESULT_2026-04-18.md`。
2. **Sweden Volkswagen top30 覆盖闭环**：`suv_only_country_model_top30/se/` 下的 Volkswagen 四个候选（Tiguan / ID.4 / T-Roc / Tayron）当前都已进入 configurator-capable draft 范围。

### 2026-04-12（第二次更新）

1. **Batch 1+2 dry-run 验证完成**：92/209 sources 通过（44.0%），从 26.3% 提升至 44.0%。
2. **Scrapling 提取器增强**：新增 `AggregateOffer.lowPrice` fallback、`Vehicle` @type 支持、HTML 标签清洗、空 CSS 策略容错。
3. **42 个 YAML 源文件修复**：URL 域名/路径修正（Dacia ×8、Renault ×2、Nissan ×2、Skoda ×13）、CSS 提取策略新增（Ford ×7、Peugeot ×3、Opel ×2）、Volvo CSS 选择器更新 ×2、Hyundai URL ×2。
4. **品牌可行性全面验证**：12 个品牌确认可提取（Toyota、Volvo、Skoda、KIA、Hyundai、Dacia、Ford、Peugeot、Nissan、Opel、Mercedes、Renault），19 个品牌确认不可用（Tesla、VW、BMW、Audi 等）。
5. **MSRP 执行计划更新**：`MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md` 加入完整 dry-run 结果表和品牌可行性矩阵。

### 2026-04-12

1. **Keyword filling Batch 1 完成**：SE（26 文件）+ HR（30 文件）= 56 文件，零 placeholder 残留。
2. **Keyword filling Batch 2 完成**：HU（30）+ NO（30）+ AT（30）+ CZ（30）+ CH（30）= 150 文件，零 placeholder 残留。另有 `batch2_residual_cleanup.py` 修复 32 个边缘 case（LPG/Ford Puma Gen-E BEV/Audi Q3 PHEV 等）。
3. **206/626 draft YAML keyword 填充完成**（Batch 1+2 共 7 国，~33%）。
4. **Pipeline 内汇率转换模块上线**：`jato_scraper/currency_converter.py`。使用 `open.er-api.com`（免费、ECB 数据源、无需 API key），备用 `exchangerate-api.com`。调用链：extract → validate → `enrich_observations_with_eur()` → `msrp_value_eur` + `fx_rate_to_eur` 写入 `RawObservation`。
5. **POC 测试 1 通过**：Volvo SE XC60 抓取 3 个 trim，SEK→EUR 自动转换正常（569,900 SEK → 52,438 EUR，rate=10.868）。
6. **Backlog 文档从 `country×brand` 切换为 `SUV country model top30`**，批次定义从 Batch 1-3 × 10 brand-family source 改为 Batch 1-4 × 国家维度。

### 2026-04-11

1. Scrapling brand-family extractor 已支持 `model_rules`，可以把官网车型文案映射回 `jato_model`，并支持 `skip_if_model_unmapped`。
2. candidate scope 现已把 `model_rules` 展开为多车型覆盖，不再只识别 `fixed_jato_model`。
3. MSRP 前端 detail drawer 已展示 `price history`，并显式标识 `override_applied`。
4. batch-1 官网调研结果：Renault / Peugeot 公开 offers 页主要暴露租赁月供，不是 MSRP；Volkswagen 当前 offers 内容主要嵌在 `script#spaModel` 中，且仍需进一步确认真实购车价入口。
