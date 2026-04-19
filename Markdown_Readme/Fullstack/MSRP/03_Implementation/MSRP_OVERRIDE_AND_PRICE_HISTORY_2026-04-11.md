# MSRP Override 回流 + 价格时间序列 — 技术设计

Date: 2026-04-11

Status: Active（price history 已实现；2026-04-18 补齐 link/override lifecycle）

关联文档：
- Pipeline 全流程: [MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md](./MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md)
- PRD: [MSRP_Official_Price_Enrichment_PRD_2026-04-10.md](../01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md)

---

## 0. 2026-04-18 更新说明

这份文档最初记录的是“override auto-apply + price history”设计与实现。到 2026-04-18，实际代码已经继续往前走了一步：

1. **`MatchOverride` 不再是唯一映射层**：现在还有长期稳定映射对象 `JatoMsrpLink`。
2. **ingest / materialize 已共用 canonical mapping resolver**：优先级固定为 `valid MatchOverride > active JatoMsrpLink > raw observation`。
3. **review approve/remap 会 upsert active link**：人工确认不再只写 dated override；`persist_override=true` 时才额外写 `MatchOverride`。
4. **mismatch reason 已统一分类**：`naming_mismatch / timing_mismatch / market_mismatch / granularity_mismatch` 会写入 `match_reason_json.mappingResolver`。
5. **新增 `/v1/msrp/links` CRUD API**：后续 workbench UI 不必再把所有 link 管理都塞进 override 列表。

因此，下面原始章节里凡是把 override 描述成“唯一映射手段”的地方，都应理解为**早期阶段背景**；当前主流程已经升级为 `link + override + price history` 三层并行。

---

## 1. 问题陈述

### 1.1 早期痛点：Override 能存但还没真正进入主链路

最初的问题是：`review.match_overrides` 在人工 approve/remap 时可以存入，但后续 scraper 抓取同一个 trim 时不会自动复用，导致重复进入 `review_required` 队列、人工重复批准。

> 现状：这个问题已经解决，而且进一步升级成了 **`MatchOverride + JatoMsrpLink` 共用 resolver** 的主链路。

### 1.2 价格时间序列丢失

`msrp.current_prices` 是快照表（每个 business key 只保留最新价格）。当官网降价时，`materialize_current_price_from_observation()` 直接覆盖旧价格。旧的价格值和持续时间段**在 current_prices 表中丢失**（虽然 `msrp.observations` 保留了所有历史观测值，但按时间段查询需要全表扫描）。

用户需求：**"什么时候到什么时候是这个价格"** — 压缩的价格区间段。

---

## 2. 设计决策

### 2.1 Override / Link Lookup Key: (country, brand, jato_model, jato_trim[, jato_powertrain])

关键观察：人工 review 只修改 `official_model` / `official_trim`，**不修改** `jato_model` / `jato_trim`。而 `jato_model` 来自 YAML 配置的 `fixed_jato_model`，`jato_trim` 来自 `copy_trim_to_jato_trim`（= 网页上的 official_trim）。这两个字段在同一个 scraper 的重复执行中是稳定的。

因此：
- Lookup key = `(country, brand, jato_model, jato_trim)` + 日期范围
- Override 提供 = `(official_model, official_trim)` 作为映射结果
- 无需新增 `scraper_official_*` 列

| 场景 | jato_trim (lookup key) | override.official_trim (映射值) | 效果 |
|:---|:---|:---|:---|
| approve | "B5 AWD Momentum" | "B5 AWD Momentum" (不变) | 下次同 trim 直接 auto-apply |
| remap | "B5" (原始) | "B5 Mild Hybrid" (修正) | 下次 "B5" 来了自动映射为 "B5 Mild Hybrid" |

### 2.2 新增 match_status: "override_applied"

在 `ELIGIBLE_CURRENT_PRICE_STATUSES` 中新增 `"override_applied"`，使得 override 命中的 observation 跳过 ReviewCase 创建，直接进入 `materialize_current_price_from_observation()`。

### 2.3 Override 时效性（当前仍有效）

`MatchOverride` 已有 `valid_from_date` / `valid_to_date` 字段。Lookup 自动过滤：

```sql
WHERE valid_from_date <= :observation_date
  AND (valid_to_date IS NULL OR valid_to_date >= :observation_date)
```

当品牌改款换代时，设置旧 override 的 `valid_to_date` 即可让系统回退到 review_required。

### 2.4 价格时间序列：`msrp.price_history`

压缩存储：每行 = 一个价格区间段。

```
price_history
├─ country, brand, jato_model, jato_trim  (业务 key)
├─ msrp_value (EUR), source_msrp_value, source_currency  (价格)
├─ valid_from_utc  (本区间开始时间 = 首次观测到此价格)
├─ valid_to_utc    (本区间结束时间 = 下一个不同价格的观测时间，NULL = 当前)
├─ started_by_observation_id  (哪个 observation 开启了本区间)
└─ ended_by_observation_id    (哪个 observation 结束了本区间)
```

**触发逻辑**（在 `materialize_current_price_from_observation` 内）：

```
if 价格变动 (source_msrp_value 或 source_currency 变了):
    close open_period (设 valid_to_utc, ended_by_observation_id)
    create new_period (valid_from_utc = observed_at_utc)
if 首次观测 (no existing current_price):
    create first_period
if 价格未变:
    不动 price_history (区间继续开放)
```

**查询示例**：

```sql
-- "Sweden VOLVO XC60 B5 AWD 近半年价格变动"
SELECT valid_from_utc, valid_to_utc,
       source_msrp_value, source_currency,
       msrp_value AS eur_value
FROM msrp.price_history
WHERE country = '瑞典' AND brand = 'VOLVO'
  AND jato_model = 'XC60' AND jato_trim = 'B5 AWD'
ORDER BY valid_from_utc DESC;
```

---

## 3. 实现变更清单

### 3.1 Alembic Migration `20260411_0006`

| 操作 | 表 | 说明 |
|:---|:---|:---|
| CREATE TABLE | `msrp.price_history` | 价格区间表 |
| CREATE INDEX | `ix_msrp_price_history_business_key` | (country, brand, jato_model, jato_trim) |
| CREATE INDEX | `ix_msrp_price_history_open_period` | 含 valid_to_utc 用于快速查开放区间 |
| CREATE INDEX | `ix_review_match_overrides_lookup` | (country, brand, jato_model, jato_trim, valid_from_date) |

### 3.2 Model 变更 (`app/db/models.py`)

新增 `PriceHistory` class (schema=msrp)。

### 3.3 Repository 变更

| 文件 | 新增函数 | 说明 |
|:---|:---|:---|
| `review_repository.py` | `find_applicable_override()` | 按 business key + 日期查找有效 override |
| `msrp_repository.py` | `get_open_price_period()` | 查找当前开放的价格区间 |
| `msrp_repository.py` | `add_price_history()` | 写入新区间 |
| `msrp_repository.py` | `list_price_history()` | 按条件查询区间 |

### 3.4 Service 变更 (`msrp_workflow_service.py` + `msrp_mapping_service.py`)

| 位置 | 变更 | 说明 |
|:---|:---|:---|
| `ELIGIBLE_CURRENT_PRICE_STATUSES` | 新增 `"override_applied"` | override 命中的 observation 可 materialize |
| `msrp_mapping_service.py` | 新增 canonical mapping resolver | resolver 顺序：`valid override > active link > raw observation` |
| `create_scrape_batch_ingest()` | ingest 循环中统一走 resolver | review_required → 先查 override/link → 命中则 auto-apply / materialize |
| `materialize_current_price_from_observation()` | 价格变动时调用 `_record_price_period()` | 记录压缩时间序列 |
| 新增 `_record_price_period()` | 关闭旧区间 + 开放新区间 | 内部 helper |
| 新增 `list_price_history()` | service layer 查询 | 调用 repo + payload 序列化 |
| `review_service.create_review_decision()` | approve/remap 时 upsert active `JatoMsrpLink` | 人工确认会被后续 observation 继承 |

### 3.5 API 变更 (`msrp_workflow.py`)

| Endpoint | Method | 说明 |
|:---|:---|:---|
| `/v1/msrp/price-history` | GET | 查询价格时间序列，支持 country/brand/jato_model/jato_trim 过滤 |

### 3.6 Payload Serializer (`payload_serializers.py`)

新增 `price_history_payload()` → camelCase JSON。

---

## 4. Override 与价格变动的交互

场景：半年价格稳定，然后降价。

```
Jan-Jun:  scraper 抓到 source_msrp = 569,900 SEK
          override 匹配 → auto-apply → current_price 更新 (价格不变，区间继续)
          price_history: [Jan → NULL, 569900 SEK]

Jul:      scraper 抓到 source_msrp = 549,900 SEK (降价)
          override 仍匹配 → auto-apply (mapping 没变)
          → materialize 检测价格变动
          → price_history:
            [Jan → Jul, 569900 SEK] (closed)
            [Jul → NULL, 549900 SEK] (open)
```

关键：**Link / Override 管 naming/mapping（谁对应谁），Price history 管价格（什么时候是多少钱）。三者独立运作。**

Override 有 `valid_to_date`：当品牌换代/停产时设置截止日期，系统自动回退到 active link 或 manual review。

---

## 5. LLM 的合理位置

LLM **不应**进入核心提取和匹配主链路。合理定位：

| 窄位置 | 输入 | 输出 | 场景 |
|:---|:---|:---|:---|
| **Source 发现助手** | 品牌名 + 国家 + "MSRP configurator" | 候选 URL + draft YAML | 辅助人工查找官网定价入口 |
| **低置信度证据摘要** | observation 的 match_reason_json | 简洁中文摘要 | 帮助人工更快 approve/remap |

ROI 顺序：**brand-family 规则化 > override 回流 > Source 发现助手 > 证据摘要 > (远期) LLM 报告**

---

## 6. 下一步

1. 给 top 20-30 个高价值 country×brand 编写 brand-family 级 YAML source，优先使用已落地的 `model_rules`、`skip_if_model_unmapped`、`fixed_jato_powertrain`、`copy_trim_to_jato_trim`、`edition_rules`、`powertrain_rules`、`price_band_bonuses`
2. 继续筛选真正的 MSRP / 官方购车价入口；`€/mois` 这类 leasing offer 页面不能直接接入 MSRP pipeline
3. 前端 ReviewCasesPage 增加 persist_override 默认开启，减少人工遗忘
4. MsrpPage 已支持 `price history` 列表展示，后续如有需要再升级为时序图
5. 定时清理过期 override（`valid_to_date < today`）→ 归档而非删除
