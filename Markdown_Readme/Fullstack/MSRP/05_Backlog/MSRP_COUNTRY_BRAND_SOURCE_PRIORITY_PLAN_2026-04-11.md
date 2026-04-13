# MSRP Country×Brand 扩 Source 优先级计划

日期：2026-04-11

状态：**已归档 (Archived)** — 2026-04-12 起由新计划替代

> **⚠️ 本文档已归档。**
> 当前执行计划请参阅：[`MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md`](MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md)
>
> 变更原因：范围从 country×brand 切换为 SUV-only country model top30，批次按国家划分而非按品牌系族，
> 汇率由 Frankfurter API 改为 open.er-api.com（已集成到 pipeline 中）。

说明：本页保留旧的 `all_market` country×brand 优先级计划作为历史参考。当前执行口径已切换为按国家的 model top30；`all_market` 与 `suv_only` 请查看新生成目录下的汇总文件。

产物来源：

- 全市场候选覆盖报告：`04_Processed_data/msrp_candidate_scope/all_market/candidate_scope_report.json`
- SUV 候选覆盖报告：`04_Processed_data/msrp_candidate_scope/suv_only/candidate_scope_report.json`
- 双口径生成器：`03_Scripts/regenerate_msrp_dual_backlogs.py`
- 全市场 source draft 目录：`07_ScrapingToolkit/source_drafts/_archived/all_market_country_model_top30/`（已归档）
- SUV source draft 目录：`07_ScrapingToolkit/source_drafts/suv_only_country_model_top30/`

## 策略

按 country×brand 价值优先扩 source，不再按单车型推进。

当前优先级分数：

`priority_score = sales_12m_sum + candidate_model_count * 1000`

排序意图：

1. 先拿销量高的 country×brand。
2. 同销量下，优先覆盖一个 source 能吃掉更多 model 的 brand-family 页面。
3. 以 10 个 source 为一个 rollout batch，先打完 batch 1，再做 batch 2 和 batch 3。

## Batch 1

| Rank | Country | Brand | Scope | Missing Models | Sales 12M | Draft |
| ---: | --- | --- | --- | ---: | ---: | --- |
| 1 | 德国 | VOLKSWAGEN | brand_family | 9 | 419,012 | `batch_01/de/01_volkswagen_de.yaml` |
| 2 | 法国 | RENAULT | brand_family | 5 | 229,152 | `batch_01/fr/01_renault_fr.yaml` |
| 3 | 法国 | PEUGEOT | brand_family | 5 | 211,332 | `batch_01/fr/02_peugeot_fr.yaml` |
| 4 | 德国 | SKODA | brand_family | 4 | 131,844 | `batch_01/de/02_skoda_de.yaml` |
| 5 | 法国 | DACIA | brand_family | 3 | 115,093 | `batch_01/fr/03_dacia_fr.yaml` |
| 6 | 意大利 | FIAT | brand_family | 2 | 107,090 | `batch_01/it/01_fiat_it.yaml` |
| 7 | 法国 | CITROEN | brand_family | 3 | 95,748 | `batch_01/fr/04_citroen_fr.yaml` |
| 8 | 意大利 | TOYOTA | brand_family | 3 | 95,648 | `batch_01/it/02_toyota_it.yaml` |
| 9 | 捷克 | SKODA | brand_family | 8 | 81,578 | `batch_01/cz/01_skoda_cz.yaml` |
| 10 | 德国 | OPEL | brand_family | 2 | 84,612 | `batch_01/de/03_opel_de.yaml` |

## Batch 2

| Rank | Country | Brand | Scope | Missing Models | Sales 12M | Draft |
| ---: | --- | --- | --- | ---: | ---: | --- |
| 11 | 意大利 | DACIA | brand_family | 2 | 77,028 | `batch_02/it/03_dacia_it.yaml` |
| 12 | 西班牙 | TOYOTA | brand_family | 4 | 74,803 | `batch_02/es/01_toyota_es.yaml` |
| 13 | 意大利 | VOLKSWAGEN | brand_family | 3 | 73,248 | `batch_02/it/04_volkswagen_it.yaml` |
| 14 | 法国 | TOYOTA | brand_family | 2 | 64,481 | `batch_02/fr/05_toyota_fr.yaml` |
| 15 | 西班牙 | SEAT | brand_family | 3 | 61,308 | `batch_02/es/02_seat_es.yaml` |
| 16 | 德国 | AUDI | brand_family | 2 | 61,277 | `batch_02/de/04_audi_de.yaml` |
| 17 | 意大利 | RENAULT | brand_family | 2 | 53,585 | `batch_02/it/05_renault_it.yaml` |
| 18 | 意大利 | JEEP | single_model | 1 | 48,786 | `batch_02/it/06_jeep_it.yaml` |
| 19 | 西班牙 | RENAULT | brand_family | 2 | 45,608 | `batch_02/es/03_renault_es.yaml` |
| 20 | 罗马尼亚 | DACIA | brand_family | 5 | 42,152 | `batch_02/ro/01_dacia_ro.yaml` |

## Batch 3

| Rank | Country | Brand | Scope | Missing Models | Sales 12M | Draft |
| ---: | --- | --- | --- | ---: | ---: | --- |
| 21 | 西班牙 | PEUGEOT | brand_family | 2 | 41,841 | `batch_03/es/04_peugeot_es.yaml` |
| 22 | 意大利 | CITROEN | single_model | 1 | 37,509 | `batch_03/it/07_citroen_it.yaml` |
| 23 | 西班牙 | DACIA | single_model | 1 | 37,378 | `batch_03/es/05_dacia_es.yaml` |
| 24 | 西班牙 | HYUNDAI | brand_family | 2 | 35,698 | `batch_03/es/06_hyundai_es.yaml` |
| 25 | 挪威 | TESLA | brand_family | 2 | 33,626 | `batch_03/no/01_tesla_no.yaml` |
| 26 | 西班牙 | VOLKSWAGEN | brand_family | 2 | 33,201 | `batch_03/es/07_volkswagen_es.yaml` |
| 27 | 瑞典 | VOLKSWAGEN | brand_family | 6 | 27,756 | `batch_03/se/01_volkswagen_se.yaml` |
| 28 | 德国 | FIAT | single_model | 1 | 31,712 | `batch_03/de/05_fiat_de.yaml` |
| 29 | 意大利 | PEUGEOT | single_model | 1 | 30,158 | `batch_03/it/08_peugeot_it.yaml` |
| 30 | 波兰 | HYUNDAI | brand_family | 3 | 27,297 | `batch_03/pl/01_hyundai_pl.yaml` |

## 每个 Source 草稿现在包含什么

每个 draft YAML 现在已经预置了这些待填骨架：

1. `fixed_model`
2. `fixed_jato_model`
3. `fixed_jato_powertrain`
4. `copy_trim_to_jato_trim`
5. `confidence_rules`
6. `structured_fields.edition_rules`
7. `structured_fields.powertrain_rules`
8. `price_band_bonuses`
9. `auto_accept_gates`

## 推荐执行顺序

1. 先做 batch 1 的 10 个 source。
2. 每做完一个 source，就跑 scrape preview，确认 trim card 粒度、币种、powertrain 规则和 edition 规则。
3. 命中稳定后再把 draft 从 `source_drafts/` 提升到 `sources/`。
4. promotion 后重跑 candidate scope，确认 brand-level coverage 是否真正吃掉 backlog。

## 当前判断

- 最高 ROI 依然是 brand-family 规则化 + override 回流。
- LLM 仍然不进主提取链路。
- 单车型页面只在 batch 中作为补洞项，优先级低于 brand-family 页面。
