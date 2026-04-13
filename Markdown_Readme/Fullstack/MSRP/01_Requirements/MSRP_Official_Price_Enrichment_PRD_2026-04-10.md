# Official MSRP Enrichment PRD

状态：Draft

日期：2026-04-10

关联技术设计：[MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md](../02_Research_Design/MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md)

## 1. 背景

当前 JATO 数据可以稳定提供国家、品牌、Model、版型和销量等核心字段，销量口径相对可信，但 MSRP 在部分国家或部分版型上不够准确，难以直接用于：

1. 市场对标分析。
2. 价格带监控。
3. 降价与销量变化联动分析。
4. Dashboard 中基于官方价格的版本对比。

同时，业务已经具备按国家筛选、按销量筛选 Top Model 的能力，例如：

1. 筛选国家为瑞典。
2. 识别销量 Top 3 车型，如 XC60、Model Y。
3. 希望进一步补全这些车型在该国家官网上的不同版型 MSRP。

当前已经跑通 BMW Germany 的真实官网抓取链路，但这只是 extractor 可用性的验证样本，不代表产品范围只聚焦 BMW。产品长期方向仍然是围绕 JATO 国家、品牌、Model、版型体系，逐步把官网 MSRP 能力扩展成 21 国、多品牌、可持续运行的 enrichment 基座。

因此需要建设一个官方 MSRP 补全系统，从各国品牌官网或官网后端接口中获取价格，并与 JATO model / trim 做结构化匹配，最终形成可持续维护的价格快照、预警、人工复核和销量验证能力。

## 2. 产品目标

本功能的目标不是一次性抓取，而是建设一个可持续运行的 MSRP enrichment 能力。

核心目标：

1. 用官方来源补全国家级、车型级、版型级 MSRP。
2. 每周抓取一次价格快照，保留历史时间戳。
3. 识别价格波动并生成预警事件。
4. 将价格变化与后续 JATO 销量数据联动，验证降价是否有效。
5. 尽量全自动运行，低置信度结果进入人工 review。

补充定位：

1. 第一阶段可以用单个国家和少量品牌做 pilot，但产品蓝图不是停留在 pilot。
2. 长期目标是覆盖 21 个国家，并在每个国家优先维护 Top 10 品牌的 source registry。
3. 对进入 source registry 的品牌，目标是逐步覆盖官网公开可抓取的全部车型，而不是长期只抓 Top N 单车型。

## 3. 非目标

以下内容不在第一阶段范围内：

1. 一次性覆盖全部 21 国、全部品牌、全部车型。
2. 替代人工做所有最终价格口径判断。
3. 实时请求官网后直接返回前端结果。
4. ~~覆盖金融方案月供、补贴后净价、经销商落地价等全部价格体系。~~ → 已列入第三阶段计划，见 §9.9。

第一阶段应优先聚焦：

1. 少数国家。
2. 少数品牌。
3. Top 销量车型。
4. 官方 MSRP 的结构化留存与预警。

第三阶段扩展方向（已纳入 PRD backlog）：

1. 金融方案月供、首付比例、分期期数、金融利率。
2. 国家级补贴后净价。
3. 租赁月租（Leasing / LLD / LOA）。
4. 经销商落地价（如可获取）。

## 4. 业务问题定义

系统要回答以下问题：

1. 某国家当前销量 Top Model 的官网 MSRP 是多少。
2. 某 Model 在该国家官网有哪些 trim / version。
3. JATO 的 trim 与官网 trim 是否匹配，匹配置信度多高。
4. 本周与上周相比，官网 MSRP 是否变化。
5. 若 3 月出现降价，后续拿到 4 月或 5 月的 JATO sales 后，销量是否提升。

## 5. 用户画像

主要用户：

1. 业务分析师：希望看到更可信的 MSRP 和价格变动。
2. 运营或研究人员：希望快速 review 低置信度匹配。
3. 数据团队：希望维护少量国家 / 品牌采集器，而不是手工补价格。

## 6. 核心使用场景

### 6.1 国家热销车型价格补全

1. 用户在 Dashboard 中筛选国家为瑞典。
2. 系统基于 JATO sales 找到销量 Top N 车型。
3. 后台自动查询这些车型在瑞典官网的官方 MSRP。
4. 前端读取缓存好的价格结果，而不是实时爬官网。
5. 当用户进一步点到 XC60 及其不同动总或版型时，系统应能返回瑞典 XC60 在官网抓到的各 powertrain / trim 价格，例如 MHEV、PHEV 等版本，而不是只返回一个 model 级单值。

### 6.2 低置信度人工复核

1. 系统完成 JATO model / trim 与官网 model / trim 的自动匹配。
2. 当匹配置信度低于阈值时，不直接进入正式结果。
3. 系统保留来源 URL、抓取快照、候选 trim 列表，供人工 review。

### 6.3 价格波动预警

1. 系统每周抓取一次官网价格快照。
2. 当某国家、品牌、model、trim 的 MSRP 发生变化时，生成价格变动事件。
3. 若降幅或涨幅超过阈值，生成高优先级 alert。

### 6.4 价格变化效果验证

1. 3 月官网 MSRP 降价。
2. 后续拿到 4 月、5 月的 JATO sales 数据。
3. 系统分析价格变化前后销量变化。
4. 形成“降价是否有效”的验证视图。

## 7. 产品原则

1. Official source first：优先使用品牌官网或官网后端接口，不依赖第三方价格站。
2. Snapshot first：每次抓取都保留带时间戳的历史快照，不覆盖旧值。
3. Match with confidence：所有自动匹配都必须输出置信度。
4. Reviewable by design：低置信度和异常结果必须可人工复核。
5. Async over realtime：后台异步抓取和缓存，避免前端实时等待官网请求。

## 8. 范围设计

### 8.1 当前执行范围（2026-04-12 更新）

当前执行口径已从最初的"少数国家 × 少数品牌"演进为 **SUV-only country model top30**：

1. 数据粒度：每个国家按 JATO 12 个月 SUV 销量排名取 Top 30 车型。
2. 覆盖国家：21 个欧洲国家（DE/FR/IT/ES/SE/NO/DK/FI/AT/CH/NL/BE/PL/CZ/HU/HR/SI/RO/SK/GR/PT）。
3. 总计草稿 YAML：626 个 source draft，均已按标准化骨架生成。
4. 按国家批次推进 keyword filling（本地化动力总成/版本关键词），再按站推进 CSS selector 填充。
5. 价格口径：官网公开可见 MSRP，pipeline 内自动转换为 EUR。

#### 批次定义（keyword filling）

| 批次 | 国家 | 文件数 | 状态 |
| --- | --- | ---: | --- |
| Batch 1 | SE（26）+ HR（30） | 56 | 完成，零 placeholder |
| Batch 2 | HU（30）+ NO（30）+ AT（30）+ CZ（30）+ CH（30） | 150 | 完成，零 placeholder |
| Batch 3 | SI + RO | ~60 | 未开始 |
| Batch 4 | DK/DE/FR/ES/IT/NL/BE/PL/PT/FI/GR/SK | ~360 | 未开始 |

当前进度：206/626 文件 keyword 填充完成（~33%）。

### 8.2 第二阶段范围

1. CSS selector 逐站填充（需 Playwright 交互式检查）。
2. 填充完成的 draft 升级到 `sources/` 成为生产 source。
3. 按站 dry-run 验证 → 确认价格语义为真实 MSRP 后入库。
4. 支持 PDF price list、配置器 API、动态页面等多种官网来源。

### 8.3 长期覆盖蓝图

在 SUV pilot 跑通后，目标范围按以下顺序扩展：

1. 覆盖 21 个国家的 SUV Top 30 车型（当前口径）。
2. 扩展到 all_market Top 30 车型（含轿车、MPV 等）。
3. 每个品牌逐步覆盖官网公开可抓取的全部车型。
4. 每个 model 下保留各 powertrain / trim / version 的价格记录。
5. 非 EUR 国家的价格自动转换为 EUR 用于跨国对比。

## 9. 功能需求

### 9.1 候选抓取对象生成

系统需要根据已有 JATO 数据生成每周待抓取对象。

要求：

1. 支持按国家筛选。
2. 支持按销量排序选取 Top N model。
3. 支持按品牌白名单过滤。
4. 支持人工指定待抓取 model 列表。

输出应至少包含：

1. 国家。
2. 品牌。
3. JATO model。
4. JATO trim 描述。
5. JATO sales month。
6. 当前销量值。

### 9.2 官网来源注册与采集策略

系统需要维护国家-品牌级别的 source registry。

每个来源至少定义：

1. 国家。
2. 品牌。
3. 官网首页或车型页 URL。
4. 采集类型：API、HTML、Playwright、PDF。
5. 反爬或额外交互要求。
6. 价格口径说明。

### 9.3 官网价格抓取

系统需要从官网或官网后端接口抓取价格信息。

支持的抓取方式：

1. 直接请求官方接口。
2. 解析静态 HTML。
3. 用浏览器自动化处理动态页面。
4. 解析官网 price list PDF。

抓取结果必须保留：

1. 原始 URL。
2. 原始页面或接口响应快照。
3. 抓取时间戳。
4. 币种。
5. 是否含税。

### 9.4 价格归一化

官网抓回的价格必须标准化，至少处理：

1. 本地数字格式（如匈牙利 `9.922.500`、瑞典 `569 900`）。
2. 币种识别与记录（EUR/SEK/NOK/DKK/CHF/CZK/HUF/PLN/RON 等）。
3. **自动 EUR 转换**：pipeline 内 `currency_converter.py` 在 validation 后自动调用免费汇率 API（`open.er-api.com`，ECB 数据源）将本地货币转为 EUR，写入 `msrp_value_eur` 和 `fx_rate_to_eur` 字段。备用 API：`exchangerate-api.com`。
4. 含税 / 不含税。
5. 单次促销价与常规 MSRP 区分。
6. 金融方案价格与 MSRP 区分。

### 9.9 金融方案数据采集（第三阶段 — 计划中）

#### 9.9.1 背景

欧洲多国品牌官网除了展示 MSRP 之外，还会在同一页面或相邻页面上展示金融方案。典型国家如法国（LOA/LLD）、德国（Finanzierung）、意大利（Anticipo + Rata）、西班牙（Financiación）等。这些金融数据与 MSRP 共同构成消费者实际购车决策的完整价格画面。

在 MSRP 抓取基座稳定后，应复用同一 extractor 框架扩展金融字段提取能力。

#### 9.9.2 目标金融字段

官网金融方案通常包含以下字段组合：

| 字段 | 英文名 | 说明 | 示例 |
| --- | --- | --- | --- |
| 月供金额 | monthly_payment | 每月还款金额（含税） | €289/mois |
| 首付金额 | down_payment | 首付金额（绝对值） | €5,000 |
| 首付比例 | down_payment_pct | 首付占车价百分比 | 15% |
| 分期期数 | term_months | 还款总月数 | 48 |
| 年利率 | apr | 名义年利率（TAN/TAEG） | 3.99% |
| 实际年利率 | effective_apr | 包含费用的实际年利率（TAEG/effektiver Jahreszins） | 4.52% |
| 尾款/气球付款 | balloon_payment | 合同期末一次性尾款 | €12,500 |
| 金融方案类型 | finance_type | 方案类别 | loan / balloon_loan / lease_loa / lease_lld / pcp |
| 总融资成本 | total_credit_cost | 全部利息 + 手续费总额 | €3,200 |
| 总支付金额 | total_amount_payable | 首付 + 月供 × 期数 + 尾款 | €42,200 |
| 年行驶里程限制 | annual_mileage_limit | 租赁方案的里程上限 | 15,000 km/an |
| 金融方案有效期 | offer_valid_until | 方案截止日期 | 2026-06-30 |
| 政府补贴/环保奖金 | subsidy_amount | 已扣除的国家补贴金额 | -€5,000 (Bonus écologique) |
| 补贴后净价 | net_price_after_subsidy | MSRP 减去补贴后的参考价 | €34,990 |

#### 9.9.3 各国金融方案类型分布

| 国家 | 常见金融类型 | 官网展示特征 |
| --- | --- | --- |
| 法国 (FR) | LOA（带回购租赁）、LLD（长期租赁）、Crédit classique | 月供 + 首付 + 期数 + TAEG，常有 Bonus écologique 扣减 |
| 德国 (DE) | Finanzierung（分期贷款）、Leasing | 月供 + Anzahlung + Laufzeit + effektiver Jahreszins |
| 意大利 (IT) | Finanziamento、Leasing | Anticipo + Rata mensile + TAN/TAEG + Maxirata finale |
| 西班牙 (ES) | Financiación、Renting | Cuota mensual + Entrada + Plazo + TAE |
| 英国 (GB) | PCP（Personal Contract Purchase）、HP（Hire Purchase）、PCH（Lease） | Monthly payment + Deposit + Term + APR + Optional final payment |
| 荷兰 (NL) | Financial Lease、Private Lease | Maandbedrag + Looptijd + JKP |
| 挪威 (NO) | Lånefinansiering | Månedlig betaling + Egenkapital + Nominell rente / Effektiv rente |
| 瑞典 (SE) | Billån、Privatleasing | Månadskostnad + Kontantinsats + Löptid + Ränta |
| 其他 EUR 区 | 各国变体 | 通常包含月供 + 首付 + 期数 + APR 四要素 |

#### 9.9.4 价格语义扩展

当前 `price_semantics` 字段只支持 `base_msrp`。第三阶段需扩展为：

| price_semantics 值 | 含义 |
| --- | --- |
| `base_msrp` | 官方建议零售价（现有） |
| `finance_monthly` | 金融方案月供 |
| `lease_monthly` | 租赁月租（LOA/LLD/PCH） |
| `net_after_subsidy` | 补贴后净价 |
| `campaign_price` | 限时促销价 |

一个 trim 可以同时产出多条 observation：一条 `base_msrp` + 一条 `finance_monthly` + 一条 `lease_monthly`，通过 `price_semantics` 区分。

#### 9.9.5 RawObservation 扩展字段

在 `base.py` 的 `RawObservation` 中新增可选字段：

```python
# --- Phase 3: Finance fields (planned) ---
monthly_payment: float | None = None
down_payment: float | None = None
down_payment_pct: float | None = None
term_months: int | None = None
apr: float | None = None
effective_apr: float | None = None
balloon_payment: float | None = None
finance_type: str | None = None          # loan / balloon_loan / lease_loa / lease_lld / pcp
total_credit_cost: float | None = None
total_amount_payable: float | None = None
annual_mileage_limit: int | None = None
offer_valid_until: str | None = None
subsidy_amount: float | None = None
net_price_after_subsidy: float | None = None
```

#### 9.9.6 CSS Selector 扩展

金融数据通常在 MSRP 页面的同一 vehicle card 或附近 section 中展示。CSS profile 需扩展：

```yaml
css:
  # existing MSRP selectors
  vehicle_container: '...'
  trim: '...'
  price: '...'
  # Phase 3: finance selectors
  monthly_payment: '.finance-offer .monthly-amount::text'
  down_payment: '.finance-offer .deposit::text'
  term_months: '.finance-offer .duration::text'
  apr: '.finance-offer .rate::text'
  finance_type: '.finance-offer .type::text'
  subsidy_badge: '.eco-bonus .amount::text'
```

#### 9.9.7 实施前提

1. **MSRP 基座稳定**：至少 5 个国家的 MSRP dry-run 通过后再启动金融字段。
2. **Validation 扩展**：`validation.py` 需新增金融字段合理性校验（月供 < MSRP、APR ∈ [0, 30%]、期数 ∈ [12, 84] 等）。
3. **数据表扩展**：`official_price_observations` 表新增金融字段列，或创建独立的 `finance_observations` 表（见 §10.5）。
4. **Dashboard 展示**：前端需支持 MSRP / 月供 / 租赁 三种视图切换。

#### 9.9.8 优先级排序

| 优先级 | 金融类型 | 理由 |
| --- | --- | --- |
| P0 | LOA/LLD 月供（法国） | 法国市场金融方案渗透率极高，消费者主要看月供 |
| P1 | PCP 月供（英国） | 英国 PCP 占新车销售 80%+ |
| P1 | Finanzierung/Leasing（德国） | 德国高端品牌金融比例高 |
| P2 | 政府补贴净价 | 法国 Bonus écologique、德国 Umweltbonus 等直接影响成交价 |
| P3 | 其他国家金融方案 | 跟随 MSRP 覆盖进度逐步扩展 |

---

### 9.5 JATO 与官网的匹配

系统需要对 JATO model / trim 和官网 model / trim 做匹配。

匹配输出必须包含：

1. 匹配结果。
2. 匹配置信度。
3. 匹配理由或规则来源。
4. 原始候选项。

匹配优先级建议：

1. 品牌完全一致。
2. 国家完全一致。
3. model 名称标准化后高相似度。
4. trim 描述高相似度。
5. 车身、动力、续航、电池等结构化特征辅助比对。

业务联动口径建议：

1. 第一层关联以 country + brand + normalized jato_model 对应官网 model bucket 为主。
2. 第二层关联再用 jato_trim、powertrain、drive type、battery/range 等特征去细分到官网 trim / version。
3. 当前价格查询应支持“同一国家、同一 JATO model 下的不同 powertrain / trim 同时展示”，避免把 model 级价格误当成版型级价格。

### 9.6 低置信度人工 review

当匹配置信度不足时，系统必须进入 review 阶段。

review 页面或 review 数据输出应包含：

1. JATO model / trim。
2. 官网 model / trim 候选。
3. 来源 URL。
4. 抓取快照。
5. 自动匹配分数。
6. 人工确认结果。

### 9.7 周级快照与价格预警

系统每周至少执行一次抓取。

每次抓取必须生成：

1. 本周价格快照。
2. 与上周快照的 diff。
3. 涨价 / 降价 / 无变化事件。
4. 超阈值价格预警。

### 9.8 降价有效性验证

系统需要把价格变化与后续销量数据联动。

至少支持：

1. 记录价格变化发生月份。
2. 记录后续 JATO sales 月份。
3. 比较价格变化前后销量。
4. 输出简单结论，例如：销量上升、销量无明显变化、销量下降。

## 10. 核心数据表设计

以下为第一阶段建议的关键表。

### 10.1 official_price_observations

这是最核心的价格快照事实表。

建议字段：

| 字段 | 含义 |
| --- | --- |
| observation_id | 主键 |
| scrape_batch_id | 本次抓取批次 |
| country | 国家 |
| brand | 品牌 |
| jato_model | JATO model |
| jato_trim | JATO trim 或版型描述 |
| official_model | 官网 model |
| official_trim | 官网 trim |
| msrp_value | MSRP 数值（本地货币） |
| msrp_value_eur | MSRP 数值（EUR 转换后） |
| fx_rate_to_eur | 汇率（1 EUR = X 本地货币） |
| currency | 币种 |
| tax_included | 是否含税 |
| observed_at_utc | 抓取时间戳 |
| observed_week | 抓取所属周 |
| source_url | 来源 URL |
| source_type | API / HTML / Playwright / PDF |
| match_confidence | 匹配置信度 |
| match_status | auto_accepted / review_required / human_approved / rejected |
| source_snapshot_path | 原始抓取快照路径 |
| source_payload_hash | 原始内容摘要 |
| price_label | MSRP / campaign price / finance price |
| availability_text | 官网原始在售说明 |
| extraction_version | 提取规则版本 |
| created_at_utc | 记录创建时间 |
| updated_at_utc | 记录更新时间 |

### 10.2 source_registry

维护品牌官网入口与采集方式。

建议字段：

| 字段 | 含义 |
| --- | --- |
| source_id | 主键 |
| country | 国家 |
| brand | 品牌 |
| source_url | 官方入口 URL |
| source_type | API / HTML / Playwright / PDF |
| price_semantics | 价格口径说明 |
| requires_location | 是否需要邮编 / 经销商上下文 |
| enabled | 是否启用 |
| extractor_name | 采集器名称 |
| extractor_version | 采集器版本 |
| notes | 备注 |

### 10.3 price_change_alerts

用于记录周级价格变化事件。

建议字段：

| 字段 | 含义 |
| --- | --- |
| alert_id | 主键 |
| country | 国家 |
| brand | 品牌 |
| official_model | 官网 model |
| official_trim | 官网 trim |
| previous_observation_id | 上一条价格快照 |
| current_observation_id | 当前价格快照 |
| previous_msrp | 上一价格 |
| current_msrp | 当前价格 |
| delta_value | 价格变化值 |
| delta_pct | 价格变化百分比 |
| alert_level | info / warning / critical |
| detected_at_utc | 预警生成时间 |
| alert_status | open / reviewed / ignored |

### 10.4 price_sales_effectiveness

用于验证价格变化是否带来销量变化。

建议字段：

| 字段 | 含义 |
| --- | --- |
| analysis_id | 主键 |
| country | 国家 |
| brand | 品牌 |
| jato_model | JATO model |
| jato_trim | JATO trim |
| price_event_month | 价格变化月份 |
| price_change_direction | up / down |
| price_change_value | 价格变化值 |
| baseline_sales_months | 变化前销量月份集合 |
| post_sales_months | 变化后销量月份集合 |
| baseline_avg_sales | 变化前平均销量 |
| post_avg_sales | 变化后平均销量 |
| sales_delta | 销量变化值 |
| sales_delta_pct | 销量变化百分比 |
| effectiveness_label | positive / neutral / negative / insufficient_data |
| confidence_note | 结论置信说明 |
| generated_at_utc | 生成时间 |

### 10.5 finance_observations（第三阶段 — 计划中）

独立存储金融方案数据，与 `official_price_observations` 通过 `observation_id` / `scrape_batch_id` 关联。

| 字段 | 含义 |
| --- | --- |
| finance_observation_id | 主键 |
| observation_id | 关联的 MSRP observation（FK） |
| scrape_batch_id | 抓取批次 |
| country | 国家 |
| brand | 品牌 |
| official_model | 官网 model |
| official_trim | 官网 trim |
| finance_type | loan / balloon_loan / lease_loa / lease_lld / pcp / hp |
| monthly_payment | 月供金额（本地货币） |
| monthly_payment_eur | 月供金额（EUR） |
| down_payment | 首付金额 |
| down_payment_pct | 首付比例 |
| term_months | 分期期数 |
| apr | 名义年利率 (TAN) |
| effective_apr | 实际年利率 (TAEG) |
| balloon_payment | 尾款/气球付款 |
| total_credit_cost | 总融资成本 |
| total_amount_payable | 总支付金额 |
| annual_mileage_limit | 年里程限制（租赁） |
| offer_valid_until | 方案有效期 |
| subsidy_amount | 补贴金额 |
| net_price_after_subsidy | 补贴后净价 |
| currency | 币种 |
| source_url | 来源 URL |
| observed_at_utc | 抓取时间戳 |
| created_at_utc | 创建时间 |

---

## 11. 匹配机制设计

### 11.1 匹配等级

建议把匹配结果分为三档：

1. 高置信度：自动接收。
2. 中置信度：进入人工 review。
3. 低置信度：默认拒绝或等待人工指定。

### 11.2 匹配输入特征

匹配算法建议至少使用：

1. 标准化品牌名。
2. 标准化 model 名。
3. trim 文本相似度。
4. 动力类型。
5. 电池容量、续航、驱动形式等结构化特征。
6. 车型页上下文。

### 11.3 人工 review 输出

人工 review 至少要能看到：

1. 自动匹配到的候选项。
2. 候选项置信度。
3. 官网链接。
4. 快照内容。
5. 人工最终确认结果。

## 12. 系统流程

建议整体流程如下：

1. 从 JATO 数据生成待抓取候选池。
2. 按国家和品牌调用对应采集器。
3. 抓取官网价格并保存原始快照。
4. 归一化价格和元数据。
5. 执行 JATO 与官网匹配，输出置信度。
6. 高置信度结果写入正式 observation 表。
7. 中低置信度结果进入人工 review 队列。
8. 对通过的 observation 做周级 diff，生成价格预警。
9. 当新的 JATO sales 到达时，计算价格变化后的销量验证结果。

## 13. 架构建议

### 13.1 运行模式

建议采用异步批处理，不建议前端实时抓官网。

原因：

1. 官网请求慢且不稳定。
2. 动态页面可能依赖浏览器环境。
3. 同一车型价格一周内通常不会频繁变化。
4. Dashboard 更适合读取缓存结果。

### 13.2 组件划分

建议至少拆成以下模块：

1. candidate generator：根据 JATO 生成待抓取对象。
2. source registry：管理各国品牌来源。
3. extractor adapters：每个国家-品牌的抓取器。
4. normalization layer：统一价格、币种、含税口径。
5. matching engine：JATO 与官网的匹配。
6. review queue：低置信度复核。
7. alert engine：价格变化预警。
8. effectiveness engine：价格变化与销量验证。

## 14. 自动化与人工协同

系统设计目标应是“大部分自动，少量人工复核”。

自动化部分：

1. 候选对象生成。
2. 官网抓取。
3. 价格归一化。
4. 高置信度自动匹配。
5. 周级价格预警生成。
6. 销量验证计算。

人工部分：

1. 低置信度 trim 匹配。
2. 官网结构变化后的采集规则修正。
3. 价格口径争议判断。
4. 异常价格 review。

## 15. 预警规则建议

第一阶段至少支持：

1. 任意价格变化都记录 event。
2. 绝对金额变化超过阈值时生成 warning。
3. 百分比变化超过阈值时生成 critical。
4. 同一周内重复波动只保留最终状态和完整快照。

阈值建议作为配置项，而不是硬编码。

## 16. 验收标准

第一阶段验收建议：

1. 能在单一国家稳定跑通至少 2 到 3 个品牌。
2. 能对 Top N model 生成结构化 MSRP observation。
3. observation 保留时间戳、URL、快照和匹配置信度。
4. 周级 diff 能正确识别涨价 / 降价 / 无变化。
5. 低置信度结果能进入 review 队列。
6. 至少能在一个价格变化案例中，完成后续销量验证分析。

## 17. 风险与缓解

### 17.1 官网结构变化

风险：采集器失效。

缓解：

1. 每个来源独立 adapter。
2. 保留快照与提取版本。
3. 失败时自动告警。

### 17.2 价格口径不一致

风险：抓到的不是标准 MSRP。

缓解：

1. 显式记录是否含税。
2. 显式记录 price_label。
3. review 流程允许人工修正。

### 17.3 JATO 与官网 trim 无法稳定一一映射

风险：自动匹配误判。

缓解：

1. 输出置信度。
2. 中低置信度不自动入正式结果。
3. 提供人工 review 与白名单映射。

### 17.4 价格变化与销量变化存在时间滞后

风险：错误归因。

缓解：

1. 保留价格变化月份。
2. 使用后续 1 到 3 个月销量窗口做验证。
3. 输出 insufficient_data 标签，避免过度解读。

## 18. 推荐开发顺序

1. 建立 source registry 和瑞典 pilot 品牌列表。
2. 完成瑞典 2 到 3 个品牌的 extractor。
3. 建立 official_price_observations 表和快照存储。
4. 建立基础 matching engine 和 review 队列。
5. 建立周级 diff 与 price_change_alerts。
6. 接入后续 JATO sales，生成 effectiveness 分析。

## 19. 后续实现建议

建议后续开发时先以“国家-品牌 adapter”模式落地，而不是做一个通用的自由搜索 agent。

原因：

1. 更可控。
2. 更容易维护。
3. 更适合长期自动运行。
4. 更方便把低置信度案例导入人工 review。

Agent 更适合做：

1. 首次探索官网结构。
2. 辅助生成 extractor 草稿。
3. 辅助定位页面里的价格字段。
4. 协助人工 review 边界案例。

正式生产链路应以确定性抓取器和规则匹配为主。

## 20. 后续扩展方向（非第一阶段）

在抓取、归一化、匹配和 review 主链路稳定之后，后续分析路径可以继续扩展，但不应前置于采集链路建设：

1. 把 MSRP observation 与月度 JATO sales 组成统一的国家-品牌-model-trim 时间序列。
2. 在价格和销量之外，引入新闻抓取、当地政策、补贴、宏观或市场事件等 event 数据。
3. 把价格、销量、新闻、当地事件聚合为国家分析数据集。
4. 在结构化数据稳定后，再引入 LLM 生成国家级分析报告。
5. 第一阶段仍应优先把官网抓取链路、review 闭环和 current price materialization 打稳。
