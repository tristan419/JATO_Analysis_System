# Product Deck — Lease Comparison / Privatleasing & Fleet Leasing PRD

> Version: v0.1  
> Scope: Product Deck → Pricing & Positioning → Lease Comparison  
> Status: Product requirement / implementation guide only. No code changes included.

---

## 1. 背景与目标

当前 Pricing & Positioning 模块主要用于车型价格、定位和竞品对比。新增 **Lease Comparison** 的目标是把不同国家、不同品牌、不同车型的租赁方案标准化为可比较的结构，尤其服务于 Nordic / Europe 市场下的 **Privatleasing（私人租赁）**、**Fleet Leasing（车队/企业租赁）**、**Finansiell Leasing（金融租赁）** 等业务场景。

该模块不是一个单纯的“月供计算器”，而是一个面向汽车产品、销售支持和市场对标的 **租赁报价结构分析面板**。

核心能力：

1. 统一展示不同国家、品牌、车型、金融机构的租赁方案卡片。
2. 支持 Private Leasing、Fleet Leasing 等不同 lease type。
3. 所有金额统一换算为 EUR（Euro，欧元），同时保留原始币种。
4. 复用 MSRP（Manufacturer Suggested Retail Price，厂家建议零售价）模块中的汇率来源。
5. 支持任意已知参数反推未知参数，例如反推 APR（Annual Percentage Rate，年化百分比利率）、MF（Money Factor，资金因子）、RV（Residual Value，残值）、Cap Cost（Capitalized Cost，资本化成本）。
6. 支持活动方案的手工录入、编辑、保存、版本记录和跨方案对比。
7. 支持 AI Summary Panel，自动解释报价结构差异、风险点和业务结论。
8. 支持折叠展示公式、计算过程和可视化图表。

---

## 2. 信息架构

### 2.1 Product Deck 菜单位置

目标菜单层级：

```text
Product Deck
└── Pricing & Positioning
    ├── Pricing
    ├── Lease Comparison
    ├── Compare
    ├── Customer Insight
    └── Current MSRP
```

说明：

- **Lease Comparison** 建议放在 **Pricing** 下面，因为业务路径是先看定价，再看租赁月供结构。
- Compare 是配置/版型层面对比，Lease Comparison 是金融报价结构对比，二者应并列但 Lease Comparison 更贴近 Pricing。
- Current MSRP 保持作为原始价格数据入口。

### 2.2 推荐路由

```text
/product/pricing/leasing
```

### 2.3 页面标题

```text
Lease Comparison
Privatleasing & Fleet Leasing Offer Simulator
```

中文说明：

```text
用于对比不同国家、品牌和车型的租赁月供、首付、残值、利率、里程、服务包、汇率换算和报价风险。
```

---

## 3. 模块边界

### 3.1 本模块负责

- Private Leasing / Privatleasing 方案管理。
- Fleet Leasing 方案管理。
- Finansiell Leasing / Financial Leasing 结构标记。
- 月供、Cap Cost、RV、APR、MF、首付、合同周期、里程的统一计算。
- 原始币种金额与 EUR 金额双轨展示。
- 方案卡片保存、编辑、版本记录。
- 多国家、多品牌、多车型方案对比。
- AI Summary 和公式解释。

### 3.2 本模块不负责

- 不直接替代完整金融系统。
- 不直接生成正式合同。
- 不直接处理税务合规和会计处理。
- 不直接判断某国 Private Leasing / Fleet Leasing 的法律定义，只做业务标签和计算标准化。
- 不强制替代 MSRP 模块内已有汇率逻辑，只复用或引用其汇率数据。

---

## 4. 核心业务对象

### 4.1 Lease Offer 租赁方案

一个 Lease Offer 是一张可以保存、编辑、对比的方案卡片。

每张卡片最少应包含：

| 字段 | 英文全称 | 说明 |
|---|---|---|
| Country | Country | 国家，例如 Sweden、Finland、Norway |
| Currency | Currency | 原始币种，例如 SEK、EUR、NOK、DKK |
| FX Rate to EUR | Foreign Exchange Rate to Euro | 原始币种换算 EUR 汇率 |
| Brand | Brand | 品牌，例如 Toyota、Jaecoo、Omoda |
| Model | Model | 车型，例如 RAV4、Jaecoo 7 |
| Version / Trim | Version / Trim | 版型 |
| Powertrain | Powertrain | HEV、PHEV、BEV、ICE |
| Segment | Segment | A-SUV、B-SUV、C-SUV 等 |
| Lease Type | Lease Type | Private Leasing、Fleet Leasing、Financial Leasing |
| Provider | Leasing Provider | TFS、Drivalia、ALD、Arval、LeasePlan 等 |
| Monthly Payment | Monthly Payment | 月供 |
| Down Payment | Down Payment | 首付 / 初始付款 |
| Deposit | Deposit | 押金，需区分是否可退 |
| Term Months | Contract Term in Months | 合同期，例如 36 个月 |
| Mileage | Mileage Allowance | 年里程或总里程 |
| Cap Cost | Capitalized Cost | 资本化成本 / 计价基础 |
| RV | Residual Value | 残值 |
| RV % | Residual Value Percentage | 残值率 |
| APR | Annual Percentage Rate | 年化百分比利率 |
| MF | Money Factor | 资金因子 |
| Service Included | Service Included | 是否含保养 |
| Insurance Included | Insurance Included | 是否含保险 |
| Tyre Included | Tyre Package Included | 是否含轮胎包 |
| RV Guaranteed | Residual Value Guaranteed | 残值是否保证 |
| VAT Included | Value Added Tax Included | 是否含 VAT / moms |
| Source | Source | 来源链接、截图、人工录入 |
| Effective Date | Effective Date | 生效日期 |
| Expiry Date | Expiry Date | 活动截止日期 |
| Status | Status | Draft、Active、Expired、Archived |
| Notes | Notes | 业务备注 |

---

## 5. Lease Type 设计

### 5.1 Private Leasing / Privatleasing

业务特征：

- 面向个人客户。
- 通常强调 **0 down payment / 0 kr kontantinsats**。
- 月供通常包含 VAT / moms。
- 可能包含 service，也可能不包含。
- 合同期通常 24、36、48 个月。
- 里程通常按年或总里程限制。
- 对客户最敏感的是月供、首付、服务包和交付成本。

页面显示重点：

```text
Monthly Payment
0 kr Down Payment
Term + Mileage
Service Included
VAT Included
Residual Value Risk
```

### 5.2 Fleet Leasing

业务特征：

- 面向企业客户、租赁公司、大客户和车队。
- 可能存在首付、保证金、advance rental、管理费、服务包、保险包。
- 月供可能不含 VAT，或者企业可抵扣部分 VAT。
- 重点不是单车最低月供，而是 TCO（Total Cost of Ownership，总拥有成本）和批量采购支持。
- 和 Private Leasing 的 MVP 差异可以先集中在 **首付结构 / 押金 / advance rental / 是否可退 / 是否计入 Cap Cost**。

页面显示重点：

```text
Fleet Customer Type
Volume Tier
Down Payment / Advance Rental
Monthly Payment ex. VAT or incl. VAT
Service / Insurance / Tyre Package
Effective Monthly Equivalent
Total Contract Cost
```

### 5.3 Financial Leasing / Finansiell Leasing

业务特征：

- 更接近融资租赁。
- 客户可能承担 RV 风险。
- 报价看起来月供低，但风险可能转移给客户。
- 需要明确标记“RV not guaranteed”。

页面显示重点：

```text
RV Guaranteed: No
Customer RV Risk: Yes
Monthly Payment may not be directly comparable with operating lease
```

---

## 6. 汇率与 EUR 标准化

### 6.1 原则

所有原始金额必须保留原始币种，同时计算 EUR 标准化金额。

```text
Original Amount + Original Currency + FX Rate + EUR Amount
```

示例：

```text
Monthly Payment: 5,944 SEK / month
FX Rate: 1 SEK = 0.087 EUR
Monthly Payment EUR: 517 EUR / month
```

### 6.2 汇率来源

优先级：

1. 复用 MSRP 模块已有汇率表或汇率服务。
2. 如果 MSRP 模块没有该日期汇率，使用最近一个有效汇率。
3. 如果没有可用汇率，允许用户手动输入 FX Rate。
4. 所有保存的方案必须保存当时使用的 FX Rate，而不是每次动态刷新后覆盖历史记录。

### 6.3 汇率字段

| 字段 | 说明 |
|---|---|
| originalCurrency | 原始币种 |
| targetCurrency | 默认 EUR |
| fxRateToEur | 原始币种到 EUR 汇率 |
| fxRateDate | 汇率日期 |
| fxRateSource | MSRP module / manual / imported |
| fxLocked | 是否锁定历史汇率 |
| amountOriginal | 原始金额 |
| amountEur | EUR 换算金额 |

### 6.4 为什么要锁定汇率

Lease Campaign 是某个时间点的市场活动记录，不能因为后续汇率波动导致历史月供、折扣、竞争力判断变化。

因此：

```text
保存方案时锁定 FX Rate。
重新分析时可选择 Refresh FX，但必须生成新版本记录。
```

---

## 7. 页面整体结构

页面建议分为 5 个 Block：

```text
Block A: Parameter Solver / 参数反推器
Block B: Campaign Offer Manager / 活动方案管理
Block C: Comparison Board / 方案卡片对比区
Block D: AI Summary Panel / AI 业务总结
Block E: Formula & Calculation Drawer / 公式、过程和图表
```

---

# Block A — Parameter Solver 参数反推器

## A.1 目标

用于在已知任意关键参数的情况下，自动反推缺失参数。

典型场景：

- 已知 Cap Cost、RV、Monthly Payment，反推 APR / MF。
- 已知 Target Monthly Payment、RV、APR，反推所需 Cap Cost。
- 已知 Cap Cost、APR、Monthly Payment，反推 RV。
- 已知 Cap Cost、RV、APR，计算 Monthly Payment。

## A.2 输入字段

| 字段 | 说明 |
|---|---|
| Lease Type | Private Leasing / Fleet Leasing / Financial Leasing |
| Country | 国家 |
| Currency | 原始币种 |
| Months | 合同期，默认 36 |
| Cap Cost | Capitalized Cost |
| RV | Residual Value |
| RV % | Residual Value Percentage |
| APR | Annual Percentage Rate |
| MF | Money Factor |
| Monthly Payment | 月供 |
| Down Payment | 首付 |
| Upfront Treatment | 首付处理方式 |
| VAT Included | 是否含 VAT |

## A.3 待求值选择

用户选择一个字段作为 unknown：

```text
Solve for:
- Monthly Payment
- Cap Cost
- Residual Value
- Residual Value %
- APR / MF
- Required Discount
```

## A.4 首付处理逻辑

首付不能简单粗暴地全部从 Cap Cost 里扣，需要区分三种情况：

| 类型 | 英文 | 处理方式 |
|---|---|---|
| 降低融资本金 | Cap Cost Reduction | 从 Cap Cost 中扣减 |
| 签约应付费用 | Due at Signing | 不影响 Cap Cost，但计入总成本 |
| 可退押金 | Refundable Deposit | 不计入真实费用，但影响现金流 |

推荐字段：

```text
upfrontAmount
upfrontTreatment:
  - CAP_COST_REDUCTION
  - DUE_AT_SIGNING
  - REFUNDABLE_DEPOSIT
  - UNKNOWN
```

## A.5 Effective Monthly Equivalent 有效月供

为了公平对比有首付和无首付方案，必须计算有效月供。

```text
Effective Monthly Equivalent =
(Monthly Payment × Months + Non-refundable Upfront Amount) / Months
```

如果是 refundable deposit，可单独展示，不计入费用型月供。

---

# Block B — Campaign Offer Manager 活动方案管理

## B.1 目标

支持业务人员按国家、品牌、车型保存不同租赁活动方案。

一个国家可以保存多个品牌；一个品牌可以保存多个 model；一个 model 可以保存多个 lease offer。

示例：

```text
Sweden
├── Toyota
│   ├── C-HR 1.8 HEV
│   ├── Corolla Cross HEV
│   └── RAV4 HEV
├── Jaecoo
│   ├── Jaecoo 5 HEV
│   └── Jaecoo 7 HEV
└── Omoda
    └── Omoda 9
```

## B.2 活动卡片状态

| 状态 | 说明 |
|---|---|
| Draft | 草稿 |
| Active | 当前有效 |
| Expired | 已过期 |
| Archived | 归档 |
| Scenario | 试算方案，不作为真实市场记录 |

## B.3 编辑与保存

每次修改活动方案时，必须保留版本记录。

记录内容：

```text
offerId
versionNo
changedBy
changedAt
changeReason
previousValue
newValue
fxRateUsed
sourceUpdated
```

业务目的：

- 可追踪某个报价从哪里来。
- 可追踪为什么修改 APR、RV、月供或汇率。
- 支持后续复盘活动效果。

## B.4 数据来源

支持三种来源：

1. Manual Input：人工录入。
2. Excel Import：从市场调研表导入。
3. Screenshot / Source Link：截图或网页来源。

## B.5 对比池

用户可以将方案加入 Compare Basket：

```text
Add to Compare
Remove from Compare
Compare Selected
```

最大建议：

```text
最多同时对比 6 张卡片
桌面端每行 3 张
移动端每行 1 张
```

---

# Block C — Comparison Board 方案卡片对比区

## C.1 卡片分组

推荐默认分组：

```text
Group 1: OEM Official / Captive Finance
Group 2: Third-party Leasing / Drivalia / ALD / Arval
Group 3: Chinese Brand Offers
Group 4: Fleet Offers
Group 5: Scenario Offers
```

也可按国家分组：

```text
Sweden
Finland
Norway
Denmark
Germany
Netherlands
```

## C.2 卡片展示字段

卡片顶部：

```text
Badge: HEV · A-SUV
Brand + Model
Version / Trim
Provider
Lease Type
```

核心价格：

```text
Monthly Payment
Monthly Payment EUR
Effective Monthly Equivalent
```

基础条款：

```text
Down Payment
Term
Mileage
VAT included / excluded
Service included / excluded
```

金融参数：

```text
Cap Cost
RV
RV %
APR
MF
RV Guaranteed
```

风险与说明：

```text
Customer RV Risk
Service Need Confirm
Insurance Need Confirm
Tyre Package Need Confirm
```

## C.3 卡片视觉规则

| 情况 | UI 表达 |
|---|---|
| 当前重点对标车型 | 绿色高亮边框 |
| RV 不保证 | Warning badge |
| Service 不含 | 灰色或橙色提示 |
| APR 为反推值 | 使用 ≈ 符号 |
| 汇率为手动输入 | Manual FX badge |
| 数据过期 | Expired badge |
| 参数缺失 | Need Confirm |

## C.4 对比表

除卡片外，需要提供 Table View。

推荐列：

```text
Country
Brand
Model
Lease Type
Provider
Monthly Payment Original
Monthly Payment EUR
Effective Monthly EUR
Down Payment EUR
Term
Mileage
Cap Cost EUR
RV EUR
RV %
APR
RV Guaranteed
Service Included
TCO EUR
Risk Level
```

---

# Block D — AI Summary Panel

## D.1 目标

AI Summary Panel 不是复述数据，而是输出业务结论。

重点回答：

```text
谁的月供最低？
谁的真实有效月供最低？
谁的 APR 更高？
谁用了更高 RV 拉低月供？
哪个方案风险被转移给客户？
Private Leasing 和 Fleet Leasing 是否可直接比较？
是否需要销售支持 / campaign support？
```

## D.2 Summary 输出结构

建议固定四段：

### 1. Executive Summary

一句话结论。

示例：

```text
Drivalia 方案表面月供更低，但主要来自更高 RV、较低里程和 service 不含；Toyota TFS 月供较高但包含 service、0 down payment 和 guaranteed RV，客户风险更低。
```

### 2. Key Drivers

用条目说明月供差异来源：

```text
- RV assumption
- APR / MF
- Down payment treatment
- Mileage allowance
- Service package
- VAT treatment
- Currency FX impact
```

### 3. Risk Notes

风险提示：

```text
- RV not guaranteed
- APR is reverse-calculated, not official
- Service package needs confirmation
- Fleet lease and private lease are not fully comparable if VAT or down payment treatment differs
```

### 4. Recommended Action

业务动作：

```text
- Confirm whether service is included
- Confirm whether RV is guaranteed
- Align mileage to 15,000 km/year before final comparison
- Recalculate all offers in EUR with locked FX rate
- If target monthly payment is required, run Target Cap Cost scenario
```

## D.3 AI Summary 输入数据

AI Summary 不直接读取页面截图，而是读取标准化后的 Lease Offer JSON：

```text
selectedOffers[]
normalizedCurrency = EUR
fxRateUsed
missingFields[]
riskFlags[]
calculationResults
```

## D.4 AI Summary 风险等级

建议计算一个 riskLevel：

| Risk Level | 条件 |
|---|---|
| Low | RV guaranteed + service included + clear VAT + official provider |
| Medium | 部分字段需确认，但主参数完整 |
| High | RV not guaranteed / APR reverse only / service unknown / upfront treatment unknown |

---

# Block E — Formula & Calculation Drawer 公式、过程和图表

## E.1 折叠区域

页面底部或右侧抽屉：

```text
Show Formula & Calculation Steps
```

展开后显示：

1. 基础月供公式。
2. MF 与 APR 转换。
3. Cap Cost 反推公式。
4. RV 反推公式。
5. APR / MF 反推公式。
6. 有效月供公式。
7. 汇率换算公式。
8. 图表解释。

---

## E.2 基础公式

### Money Factor 与 APR

```text
MF = APR / 2400

APR = MF × 2400
```

说明：

```text
APR 是 Annual Percentage Rate，年化百分比利率。
MF 是 Money Factor，租赁计算中常用的资金因子。
```

### 月折旧

```text
Monthly Depreciation = (Cap Cost - RV) / Months
```

### 月利息

```text
Monthly Interest = (Cap Cost + RV) × MF
```

### 总月供

```text
Monthly Payment = Monthly Depreciation + Monthly Interest
```

完整展开：

```text
Monthly Payment =
(Cap Cost - RV) / Months + (Cap Cost + RV) × MF
```

---

## E.3 反推公式

设：

```text
P = Monthly Payment
C = Cap Cost
R = Residual Value
M = Months
F = Money Factor
```

### 已知 C、R、F，求 P

```text
P = (C - R) / M + (C + R) × F
```

### 已知 P、R、F，求 C

```text
C = [P - R × (F - 1/M)] / (1/M + F)
```

### 已知 P、C、F，求 R

```text
R = [P - C × (1/M + F)] / (F - 1/M)
```

### 已知 P、C、R，求 F

```text
F = [P - (C - R) / M] / (C + R)

APR = F × 2400
```

### 已知 C、R，求 RV %

```text
RV % = RV / Cap Cost
```

---

## E.4 目标月供反推 Cap Cost

用于回答：

```text
客户目标月供是 4,000 SEK，现在报价是 4,500 SEK，需要降多少 Cap Cost？
```

公式：

```text
Target Cap Cost =
[Target Monthly Payment - RV × (MF - 1/Months)] / (1/Months + MF)
```

降价需求：

```text
Discount Needed = Current Cap Cost - Target Cap Cost
Discount % = Discount Needed / Current Cap Cost
```

---

## E.5 汇率换算公式

```text
Amount EUR = Amount Original × FX Rate to EUR
```

示例：

```text
Monthly Payment EUR = Monthly Payment SEK × SEK_to_EUR
Cap Cost EUR = Cap Cost SEK × SEK_to_EUR
RV EUR = RV SEK × SEK_to_EUR
```

---

## E.6 有效月供公式

```text
Effective Monthly Equivalent =
(Monthly Payment × Months + Non-refundable Upfront Amount) / Months
```

如果首付是 Cap Cost Reduction：

```text
Adjusted Cap Cost = Gross Cap Cost - Cap Cost Reduction
```

如果是 refundable deposit：

```text
Deposit is shown as cash flow, but excluded from cost-based effective monthly payment.
```

---

## E.7 可视化图表

推荐图表：

### Chart 1: Monthly Payment Breakdown

堆叠柱状图：

```text
Monthly Depreciation
Monthly Interest
Service / Insurance / Other Fee
```

用途：

```text
看月供是由折旧驱动，还是由利息驱动。
```

### Chart 2: Effective Monthly Comparison

柱状图：

```text
Offer A Effective Monthly EUR
Offer B Effective Monthly EUR
Offer C Effective Monthly EUR
```

用途：

```text
将不同币种、不同首付结构统一到 EUR 月均成本。
```

### Chart 3: RV % vs APR Scatter

散点图：

```text
X-axis: RV %
Y-axis: APR %
Bubble size: Monthly Payment EUR
```

用途：

```text
判断某个低月供是否来自过高 RV 或低 APR 活动。
```

### Chart 4: Total Contract Cost

柱状图：

```text
Total Contract Cost EUR =
Monthly Payment EUR × Months + Non-refundable Upfront EUR
```

用途：

```text
看完整合同期成本，而不是只看月供。
```

---

## 8. 数据保存与版本记录

### 8.1 推荐数据表

#### lease_offers

| 字段 | 说明 |
|---|---|
| id | Offer ID |
| country | 国家 |
| brand | 品牌 |
| model | 车型 |
| version | 版型 |
| leaseType | Private / Fleet / Financial |
| provider | 金融机构 |
| currency | 原始币种 |
| fxRateToEur | 汇率 |
| fxRateDate | 汇率日期 |
| monthlyPayment | 原始月供 |
| monthlyPaymentEur | EUR 月供 |
| effectiveMonthlyEur | EUR 有效月供 |
| downPayment | 首付 |
| downPaymentEur | EUR 首付 |
| upfrontTreatment | 首付处理方式 |
| months | 合同期 |
| mileagePerYear | 年里程 |
| capCost | Cap Cost |
| capCostEur | EUR Cap Cost |
| residualValue | RV |
| residualValueEur | EUR RV |
| residualValuePercent | RV % |
| aprPercent | APR |
| moneyFactor | MF |
| aprSource | official / reverse_calculated / manual |
| rvGuaranteed | 是否残值保证 |
| serviceIncluded | 是否含服务 |
| vatIncluded | 是否含 VAT |
| status | 状态 |
| sourceType | 来源类型 |
| sourceUrl | 来源链接 |
| notes | 备注 |
| createdBy | 创建人 |
| updatedBy | 更新人 |
| createdAt | 创建时间 |
| updatedAt | 更新时间 |

#### lease_offer_versions

| 字段 | 说明 |
|---|---|
| id | Version ID |
| offerId | 对应方案 |
| versionNo | 版本号 |
| snapshotJson | 修改后的完整快照 |
| changeReason | 修改原因 |
| changedBy | 修改人 |
| changedAt | 修改时间 |

#### lease_compare_sets

| 字段 | 说明 |
|---|---|
| id | Compare Set ID |
| name | 对比集名称 |
| country | 国家 |
| selectedOfferIds | 被选中的 offer |
| createdBy | 创建人 |
| createdAt | 创建时间 |

---

## 9. 权限与协作

### 9.1 推荐权限

| Role | 权限 |
|---|---|
| Viewer | 查看方案和对比 |
| Editor | 新增、编辑、归档方案 |
| Admin | 删除方案、管理汇率、管理字段字典 |

### 9.2 审核建议

对于关键金融字段：

```text
APR
RV
RV Guaranteed
Service Included
VAT Included
Upfront Treatment
```

建议支持 `Needs Review` 状态，防止未经确认的方案被当作正式市场报价。

---

## 10. MVP 范围

第一版建议只做以下能力：

1. Product Deck 下新增 Lease Comparison 入口。
2. 支持 Private Leasing、Fleet Leasing、Financial Leasing 标签。
3. 支持手工新增和编辑 offer card。
4. 支持原始币种 + EUR 标准化展示。
5. 汇率复用 MSRP 模块来源或允许手动录入。
6. 支持参数反推 APR / MF。
7. 支持选中多张卡片对比。
8. 支持 AI Summary Panel。
9. 支持公式折叠面板。
10. 保存方案修改记录。

暂不做：

```text
自动爬取官网报价
正式合同生成
复杂税务处理
多用户审批流
自动同步金融机构 API
```

---

## 11. Phase 2 扩展

后续可以扩展：

1. Target Monthly Payment 目标月供逆算。
2. 自动计算 required discount / campaign support。
3. 批量导入 Excel 竞品租赁方案。
4. 自动识别官网截图中的月供、首付、里程、合同期。
5. AI 自动生成销售话术。
6. 按国家输出 lease competitiveness score。
7. 接入 TCO 模型，加入能耗、保险、保养、轮胎、残值风险。
8. 支持 B2B fleet volume tier，例如 10 台、50 台、100 台不同支持政策。
9. 支持不同 VAT 场景，例如 incl. VAT、excl. VAT、partially deductible VAT。
10. 支持地区汇率敏感性分析。

---

## 12. 业务判断规则

### 12.1 月供不能直接比较的情况

以下情况不能只看 Monthly Payment：

```text
不同币种
不同首付
不同合同周期
不同年里程
service 是否包含不同
insurance 是否包含不同
RV 是否保证不同
VAT included / excluded 不同
Private Leasing 和 Fleet Leasing 混比
Financial Leasing 和 Operating Leasing 混比
```

必须先标准化：

```text
Currency → EUR
Upfront → Effective Monthly Equivalent
Term → Total Contract Cost
Mileage → Cost per km
Service / insurance → inclusion flag
RV risk → risk badge
```

### 12.2 低月供的常见来源

低月供可能来自：

```text
更高 RV
更低 APR
更低里程
不含 service
不含 insurance
高首付
活动补贴
厂家金融支持
残值风险转移给客户
汇率影响
```

AI Summary 必须解释低月供的来源，避免误判“月供低 = 更有竞争力”。

---

## 13. 推荐页面文案

### 页面顶部

```text
Lease Comparison
Compare private leasing, fleet leasing and financial leasing offers across countries, brands and models. All offers are normalized to EUR for cross-market comparison.
```

中文：

```text
对比不同国家、品牌和车型的私人租赁、车队租赁和金融租赁方案，并统一换算为 EUR 进行跨市场比较。
```

### 风险提示

```text
Reverse-calculated APR is an estimate based on Cap Cost, Residual Value, term and monthly payment. Actual offers may include fees, service packages, insurance, VAT treatment, campaign support and rounding.
```

中文：

```text
反推 APR 仅基于 Cap Cost、RV、合同期和月供估算。真实报价可能包含手续费、服务包、保险、VAT 处理、活动支持和四舍五入。
```

### 汇率提示

```text
EUR amounts are calculated using the locked FX rate at the time the offer is saved.
```

中文：

```text
EUR 金额按照方案保存时锁定的汇率计算。
```

---

## 14. 示例用户流程

### 流程 1：录入 Drivalia 方案并反推 APR

1. 进入 Product Deck → Pricing & Positioning → Lease Comparison。
2. 选择 Country = Sweden。
3. 新增 Offer。
4. Lease Type 选择 Financial Leasing。
5. 输入 Brand、Model、Monthly Payment、Cap Cost、RV、Months。
6. 系统自动反推 MF 和 APR。
7. 选择 RV Guaranteed = No。
8. Service Included = Need Confirm。
9. 保存为 Active 或 Draft。
10. 加入 Compare Basket，与 Toyota TFS 方案对比。

### 流程 2：对比 Private Leasing 与 Fleet Leasing

1. 选择同一国家、同一车型或同级别车型。
2. 勾选 Private Leasing offer 和 Fleet Leasing offer。
3. 系统将原始币种统一换算为 EUR。
4. 系统计算 Effective Monthly Equivalent。
5. AI Summary 输出：Fleet 是否因为首付/advance rental 导致表面月供较低。
6. 用户展开 Formula Drawer 查看计算过程。

### 流程 3：保存活动变更记录

1. 编辑某个 Omoda 9 Leasing offer。
2. 将 Monthly Payment 从 5,944 SEK 改为 5,699 SEK。
3. 填写 Change Reason：June campaign update。
4. 系统保存新版本。
5. 历史版本仍可查看和恢复。

---

## 15. 交付物清单

需要给开发的交付物：

```text
1. Lease Comparison PRD
2. Lease Offer 字段字典
3. 公式和计算规则
4. 页面 wireframe
5. AI Summary prompt 规则
6. 汇率复用说明
7. MVP / Phase 2 范围
8. 测试用例
```

---

## 16. 测试用例

### Case 1: 反推 APR

输入：

```text
Monthly Payment = 5,944 SEK
Cap Cost = 412,000 SEK
RV = 274,950 SEK
Months = 36
```

预期：

```text
系统输出 MF
系统输出 APR ≈ x%
APR 标记为 reverse calculated
```

### Case 2: 汇率换算

输入：

```text
Monthly Payment = 5,944 SEK
FX Rate SEK to EUR = 0.087
```

预期：

```text
Monthly Payment EUR = 517.13 EUR
保存 offer 时锁定 FX Rate
```

### Case 3: 首付有效月供

输入：

```text
Monthly Payment = 400 EUR
Months = 36
Down Payment = 3,600 EUR
Upfront Treatment = DUE_AT_SIGNING
```

预期：

```text
Effective Monthly Equivalent = 500 EUR
```

### Case 4: RV 不保证

输入：

```text
Lease Type = Financial Leasing
RV Guaranteed = No
```

预期：

```text
卡片显示 RV risk warning
AI Summary 提示客户承担残值风险
```

### Case 5: Private Leasing vs Fleet Leasing

输入：

```text
Private Leasing: 0 down payment, 500 EUR/month
Fleet Leasing: 3,600 EUR upfront, 420 EUR/month
Months = 36
```

预期：

```text
Fleet Effective Monthly Equivalent = 520 EUR
AI Summary 提示 fleet 表面月供低，但考虑 upfront 后不一定更低
```

---

## 17. 关键结论

Lease Comparison 应定位为 **Pricing 下面的租赁报价结构分析工具**，而不是单纯的计算器。

第一版最重要的不是做复杂金融，而是把不同报价统一成：

```text
同一币种 EUR
同一合同期
同一首付处理逻辑
同一里程口径
同一 RV 风险标签
同一 service / VAT 标识
```

这样 Product Deck 才能真正回答业务问题：

```text
竞品月供为什么低？
我们需要降 Cap Cost 还是调 RV？
Private Leasing 和 Fleet Leasing 是否能直接对比？
某个国家某个品牌的活动是否真的有竞争力？
```
