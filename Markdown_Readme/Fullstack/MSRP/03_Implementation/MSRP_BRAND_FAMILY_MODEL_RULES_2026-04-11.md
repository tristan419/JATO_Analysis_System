# MSRP Brand-Family Model Rules — 实现记录

Date: 2026-04-11

Status: Implemented

关联文档：
- `MSRP_OVERRIDE_AND_PRICE_HISTORY_2026-04-11.md`
- `../05_Backlog/MSRP_COUNTRY_BRAND_SOURCE_PRIORITY_PLAN_2026-04-11.md`

---

## 1. 为什么要补这个能力

brand-family 级官网 source 最大的问题不是抓不到车型卡片，而是抓到了之后 `jato_model` 为空。

在旧实现里：

- `jato_model` 只能来自 `fixed_jato_model`
- 这意味着单页面只能稳定服务一个车型
- 一旦一个页面承载多个车型，observation 会落成 `official_model` 有值、`jato_model` 为空
- 后续 `override`、`auto-accept`、`current_prices` materialize 都无法稳定工作

因此本次实现的目标不是“多抓一点文本”，而是让 brand-family 页面第一次具备可落库、可回流、可覆盖 candidate scope 的基础能力。

---

## 2. 已落地能力

### 2.1 Scrapling profile 新增字段

`07_ScrapingToolkit/jato_scraper/extractors/scrapling_web.py`

- `model_rules`
  - 规则列表，按官网车型文本把 observation 映射到 `jato_model`
- `skip_if_model_unmapped`
  - brand-family 页面启用后，未命中规则的 observation 直接跳过，避免空 `jato_model` 污染 review / current price

规则示例：

```yaml
model_rules:
  - key: model_clio
    jato_model: Clio
    official_model: Clio
    keywords: ["clio", "nouvelle clio"]
skip_if_model_unmapped: true
```

### 2.2 extractor 行为变化

- `_build_model_search_text()`
  - 从 `official_model`、`official_trim`、`trimText`、`name`、`model` 等字段拼搜索文本
- `_resolve_model_mapping()`
  - `fixed_jato_model` 仍优先
  - 若无 fixed model，则按 `model_rules` 命中 `jato_model`
  - 可选把 `official_model` 覆写为规则中更稳定的 canonical label
- `_build_observation()`
  - 若开启 `skip_if_model_unmapped` 且没有命中规则，则直接返回 `None`

### 2.3 confidence / reason 增强

- `confidence_rules` 新增 `model_rule_bonus`
- `match_reason.structuredFields` 新增：
  - `modelMappingSource`
  - `modelMappingKeywords`
- `match_reason.evidence` 也会记录 model mapping 证据

这样 review 时可以看出是哪个规则把官网车型文案映射成了 JATO 模型。

### 2.4 candidate scope 识别增强

`07_ScrapingToolkit/jato_scraper/candidate_scope.py`

旧逻辑只认识：

- `fixed_jato_model`
- `fixed_model`

新逻辑还会展开：

- `model_rules[].jato_model`

结果是一个 brand-family source 可以在 coverage report 里对多个车型产生 `model_source` 覆盖，而不再永远只是 `brand_source`。

### 2.5 source scaffold 同步增强

`07_ScrapingToolkit/jato_scraper/source_bootstrap.py`

- draft scaffold 现在会自动生成 `model_rules` 占位
- brand-family draft 默认打开 `skip_if_model_unmapped`
- confidence scaffold 新增 `model_rule_bonus`

`07_ScrapingToolkit/sources/_template.yaml` 也补充了 `model_rules` 示例注释。

---

## 3. 前端可见性

`06_AppPlatform/frontend/src/pages/MsrpPage.tsx`

- current prices 表新增 `Match` 列
- `override_applied` 不再被当成未知状态
- detail drawer 新增 `price history` 表格

这让规则映射、override 回流、价格时间序列这三条链路第一次可以在同一页面联动观察。

---

## 4. 验证结果

后端定向测试：

- `test_scraper_scrapling.py`
  - 新增 brand-family `model_rules` 命中测试
  - 新增 unmapped skip 测试
- `test_candidate_scope.py`
  - 新增 `model_rules` 展开覆盖测试

执行结果：`32 passed`

前端验证：

- `vitest` 全量 unit tests 通过
- `tsc --noEmit` 通过

---

## 5. 当前边界

这个能力解决的是“多车型页面如何映射回 JATO 模型”。

它没有自动解决两个更上游的问题：

1. 官网页面暴露的是不是 MSRP，而不是 leasing / finance 月供
2. 页面里是否存在稳定可抓的价格字段，而不是只剩 cookie wall 或高度定制 JSON

因此 model rules 已经让 brand-family source “技术上可行”，但是否能直接升到生产，还要看每个国家品牌页面暴露的价格语义。