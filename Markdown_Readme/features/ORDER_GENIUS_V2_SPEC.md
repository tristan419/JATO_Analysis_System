# Order Genius V2 — 业务规格与实施路线

> 2026-05-23 | 状态: 规格冻结，待实施

## 产品目标

将 Order Genius 从「单国家 SKU 月度数量矩阵」升级为「多国选品订单工作台」：

```
Order Genius V1                         Order Genius V2
──────────────────────────────────      ────────────────────────────────
单国家 matrix                           多国家并列选品表
物料号直接上传                            BOM 底表 → 国家物料生命周期 → 订单
FOB 写死在 cell 里                        FOB 按国家/付款条件/有效期管理
没有历史追溯                              物料切换 / FOB 变更 / 历史数量全可查
上传直接覆盖主数据                         上传生成候选 → Admin 审核 → 发布
```

## 当前代码定位

V1 已有 MVP 基础，但职责混在一起：

| 文件 | V1 职责 | V2 变化 |
|------|---------|---------|
| `OrderGeniusPage.tsx` | 单页承载全部功能 | 拆为 Toolbar + Table + 多个 Panel |
| `material_master_parser.py` | 解析 Excel → SKU rows | 改为生成 digest candidate |
| `order_genius_service.py` | publish / FOB / matrix / quantity | 拆为 BOM / lifecycle / order-sheet 三个 service |
| `material_sku_master` | 同时当 BOM、SKU、生命周期来源 | 演进为 BOM 明细表 |
| `country_sku_fob_resolved` | 存国家 FOB | 迁移到 `country_material_price_history` |
| `order_quantity_cell` | 按 country+year+month+material 存数量 | 升级为快照表 |
| `material_lifecycle` | 已有雏形 | 成为订单表核心驱动 |

**核心问题**：当前是「上传后生成单国家订单 matrix」，目标应是「上传生成候选，Admin 维护主数据，订单表读取主数据」。

---

## V2 模块划分（4 个页面/区域）

### 1. BOM Master / BOM 底表维护
- Admin 维护车型、动总、规格、BOM 模板、物料号、外饰、内饰、备注
- 上传 digest 只辅助生成候选，不直接覆盖正式底表
- 网页维护为最高权威

### 2. Digest Review / 上传差异审核
- 上传 Excel 后生成 BOM / FOB / lifecycle 候选变更
- 不直接覆盖正式底表
- 支持逐项 **Accept / Reject / Mark & Edit**
- Mark 模式：用户标记需人工处理 → 在 review 页面直接编辑候选值 → 保存 patched candidate → 提交 publish

### 3. Lifecycle & Price / 国家生命周期与 FOB
- Admin 维护国家级物料生效期、替代关系、FOB 有效期
- 同一物料在不同国家可以不同时间生效

### 4. Monthly Order Sheet / 月度选品订单表
- 按月份、国家组、模板生成订单表
- 横向展示多个国家数量/金额
- 保存数量、FOB 快照、金额快照

---

## 数据库演进

保留现有表，降低「万能表」职责。

### BOM 层

```text
bom_product
  brand, model, product_family, powertrain_family

bom_variant
  product_id, variant_spec, version/config, battery_type, drive_type

bom_material_option
  variant_id, bom_template, material_code,
  exterior_color_name, exterior_color_code,
  interior_color_name, interior_color_code,
  lifecycle_status, admin_locked, source_upload_id
```

### 价格与生命周期层

```text
country_material_lifecycle
  country_code, variant_id, material_code,
  valid_from_month, valid_to_month,
  replacement_of_material_code, replacement_reason, status

country_material_price_history
  country_code, material_code, payment_term_code,
  fob_eur, valid_from_month, valid_to_month, source
```

### 订单事实层

```text
order_sheet
  order_month, country_group_id, template_id, status

order_sheet_line
  sheet_id, country_code, material_code,
  quantity, fob_snapshot, amount_snapshot, payment_term_snapshot,
  lifecycle_id, price_id, row_version
```

---

## P0：业务口径

### 付款条件（系统维护，Excel 不能覆盖）

| Country | Code |
|---------|------|
| Croatia | TT |
| Sweden | LC90 |
| Greece | LC90 |
| Hungary | LC120 |
| Bulgaria | LC120 |
| Romania | LC120 |

现有 `seed_order_genius_rules.py` 须修正。后续 FOB 解析和订单金额都按 `country + month` 找当时有效 payment term。

### 动总标准化

当前解析器按字符串包含识别动总，容易把 `HEV/PHEV` 误判成 `EV`。V2 统一 powertrain normalizer：

| Family | 包含 | 颜色 |
|--------|------|------|
| EV | EV, BEV | 绿 `#16a34a` |
| HEV | HEV | 黄 `#d97706` |
| PHEV | PHEV, SHS | 蓝 `#2563eb` |
| MHEV | MHEV | 琥珀 `#ca8a04` |
| ICE | ICE | 深灰 `#4b5563` |
| LPG | LPG | `#6b7280` |

后端解析、筛选、前端颜色、产品块分组全部用 family。显示时可保留原始动总。

### FOB 计算

- Key: `country + payment_term_code + material_code + valid_from/to`
- FOB 变更不追溯历史订单金额
- 定制色价格规则独立维护（`colour_price_rule`）
- 上传发布时选择 pricing mode: `fob_includes_surcharge` 或 `fob_excludes_surcharge`

### 物料生命周期

- 按国家维护，同一物料在不同国家可不同时间生效
- 历史物料只能结束有效期，不能删除
- 切换时记录 `replaced_by_code`

### 订单事实

- Key: `order_month + country + material_code`
- 保存时快照：`fob_snapshot`, `amount_snapshot`, `payment_term_snapshot`, `lifecycle_id`
- 即使后续 FOB 变了、物料号换了，历史订单金额不变

---

## P1：修付款条件 + 动总

1. 修正 seed 和 DB 里的国家付款条件
2. 付款条件表加 `valid_from_month` / `valid_to_month`
3. 统一 powertrain normalizer（后端 + 前端共用）

---

## P2：Material 上传改为 Digest Candidate

- 上传不再直接覆盖 `material_sku_master` 和 `country_sku_fob_resolved`
- 改为生成 digest 候选表（新增/变更/删除）
- 保留 `source_sheet` / `source_row` 溯源

---

## P3：Digest Review（Accept / Reject / Mark & Edit）

每条 diff 三种操作：

| 操作 | 含义 |
|------|------|
| Accept Excel | 采用 Excel 候选值 |
| Reject / Keep Current | 保留当前网页值 |
| Mark & Edit | 标记需人工处理，在 review 页面编辑候选值，保存 patched candidate |

### Diff 粒度（字段级）

每个字段显示：
- 当前网页值 vs Excel 候选值 vs 用户修补值
- 字段来源（上传 / 网页 / patched）
- 上次维护人
- 影响国家/月度/订单

### 锁机制

- `admin_locked=true` 的字段不能被 digest 覆盖
- 只能生成冲突项让 Admin 处理

---

## P4：BOM Admin 页面

- 网页维护 BOM 底表（product → variant → material_option）
- 支持增删改，记录变更历史
- 上传 digest 只做候选，不直接覆盖

---

## P5：Lifecycle & Price 页面

- 国家维度维护物料生效期 + FOB 有效期
- 同一个产品在不同国家的切换时间可不同
- 物料替代关系可见（`replaced_by_code`）

### 示例

```
J7 ICE / Romania:
  2025-01 ~ 2025-07  material A  FOB 15,000
  2025-08 ~ 至今     material B  FOB 14,500

J7 ICE / Sweden:
  2025-01 ~ 至今     material A  FOB 15,200

J7 ICE / Croatia:
  2025-03 ~ 至今     material B  FOB 14,800
```

---

## P6：重做 Monthly Order Sheet

### 顶部筛选

```
[月份 ▼] [国家组/区域模板 ▼] [订单模板 ▼]
[品牌 ▼] [车型 ▼] [动总 ▼] [配置 ▼] [外饰 ▼] [内饰 ▼]
```

### 主体表格

```
┌─ 产品块 (动总颜色条) ───────────────────────────────────────┐
│ 配置 │ 外饰+色码 │ 内饰 │ 物料号+有效期 │ FOB │             │
│      │           │      │               │     │ RO qty│amt  │
│      │           │      │               │     │ SE qty│amt  │
│      │           │      │               │     │ TTL   │     │
│ 备注                                                       │
└────────────────────────────────────────────────────────────┘
```

- 国家列支持 Czech、Slovakia 横向并列
- 支持国家组合模板（如「巴尔干组」= Croatia + Bulgaria + Romania）
- 每国两列：数量 | 金额

---

## P7：订单快照 + 定制色规则

### 订单快照

保存时记录：
- `fob_snapshot` / `amount_snapshot` / `payment_term_snapshot`
- `lifecycle_id` / `price_id`
- 后续主数据变更不追溯

### 定制色价格规则

```text
colour_price_rule
  brand, colour_name, colour_code, colour_type,
  surcharge_eur, valid_from_month, valid_to_month
```

上传发布时选择 pricing mode：
| Mode | 含义 |
|------|------|
| `fob_includes_surcharge` | 上传 FOB 已含定制色价格 |
| `fob_excludes_surcharge` | 上传 FOB 是基础价，系统额外 + surcharge |

---

## P8：历史追溯和导出

- 旧物料号：浅灰 + 删除线，显示 `有效期 → 历史 FOB`
- 新物料号：黑色，显示 `active`
- 切换点在月份维度上可见
- 导出按选品订单表格式（多国列 + TTL + 备注），不是旧 matrix

---

## 前端改造路线

现有 `OrderGeniusPage.tsx` 拆为：

```
OrderGeniusPage/
  OrderSheetToolbar       — 月份/国家组/模板/品牌/车型/动总筛选
  OrderSheetTable         — 产品块 + 多国数量/金额主体
  BomMasterPanel          — BOM 底表维护
  DigestReviewPanel       — 上传差异审核 (Accept/Reject/Mark)
  LifecycleTimelinePanel  — 国家物料生命周期
  UploadMaterialPanel     — 上传 Material Excel
```

## 后端改造路线

保留现有 endpoint，新增 V2 endpoint，不打断现有生产：

```
/order-genius/bom/*            — BOM CRUD
/order-genius/digest/*         — 上传 + diff + accept/reject/mark
/order-genius/lifecycle/*      — 国家物料生命周期 + FOB 历史
/order-genius/order-sheets/*   — 月度选品订单表
```

现有 `/matrix` 继续跑，V2 稳定后降级为 legacy。

---

## 实施顺序

| 阶段 | 内容 | 依赖 |
|------|------|------|
| **S1** | 本文档（规格冻结） | — |
| **S2** | P1 修付款条件 seed + powertrain normalizer | S1 |
| **S3** | P2 digest candidate 机制 | S2 |
| **S4** | P3 Digest Review (Accept/Reject/Mark) | S3 |
| **S5** | P4 BOM Admin 页面 | S4 |
| **S6** | P5 Lifecycle & Price 页面 | S5 |
| **S7** | P6 Monthly Order Sheet（多国） | S6 |
| **S8** | P7 订单快照 + 定制色规则 | S7 |
| **S9** | P8 历史追溯 + 导出 | S8 |

## 最终原则

```
Excel 负责导入候选。
Admin 网页负责最终主数据。
BOM 存所有合法物料。
国家生命周期决定某国某月用哪个物料。
价格历史决定当时 FOB。
订单表保存当时事实快照。
Digest Review 支持选择性修补，不接受全量覆盖。
```
