# Order Genius V2 — 业务规格与实施路线

> 2026-05-23 | 状态: 规格冻结，待实施

## 产品目标

将 Order Genius 从「单国家 SKU 月度数量矩阵」升级为「多国选品订单工作台」：

```
Order Genius V1                    Order Genius V2
─────────────────────────────      ─────────────────────────────────
单国家 matrix                      多国家并列选品表
物料号直接上传                      BOM 底表 → 国家物料生命周期 → 订单
FOB 写死在 cell 里                  FOB 按国家/付款条件/有效期管理
没有历史追溯                        物料切换 / FOB 变更 / 历史数量全可查
```


## P0：业务口径（先冻结，后写代码）

### 0.1 国家付款条件

| 规则 | 说明 |
|------|------|
| 维护方 | 系统后台维护，Excel 上传 **不能** 决定 LC/TT |
| 生效逻辑 | 按 `country + payment_term_code + valid_from/to` 匹配订单月份 |
| 变更 | 一个国家可以从 LC90 改为 LC120，历史订单不变 |

初始值（须修正当前 DB）：

| Country | Code |
|---------|------|
| Croatia | TT |
| Sweden | LC90 |
| Greece | LC90 |
| Hungary | LC120 |
| Bulgaria | LC120 |
| Romania | LC120 |

### 0.2 BOM 物料底表

- 维护维度：`brand / model / powertrain / version / exterior_color / interior_color / bom_template`
- 一个配置行 = 一个 `material_code`
- 底表是后续所有国家 FOB 和订单的基础
- 可以在网页上直接增删改

### 0.3 国家 FOB

- Key: `country + payment_term_code + material_code + valid_from/to`
- 不同国家可以给同一物料号不同 FOB
- FOB 变更不追溯历史订单金额

### 0.4 物料生命周期

- 按国家维护
- 同一物料号在不同国家的生效/失效时间可以不同
- 历史物料只能结束有效期，不能删除
- 切换时记录 `replaced_by_code`

### 0.5 订单事实

- Key: `order_month + country + material_code`
- 保存时快照：`fob_snapshot`, `amount_snapshot`, `payment_term_snapshot`, `lifecycle_id`
- 即使后续 FOB 变了、物料号换了，历史订单金额不变


## P1：修国家付款条件主数据

### 数据修正
更新 `ordering.country_payment_term_master` 为正确值。

### 新增字段
```sql
ALTER TABLE ordering.country_payment_term_master
  ADD COLUMN valid_from_month TEXT,   -- '2025-01'
  ADD COLUMN valid_to_month TEXT,     -- NULL = 当前有效
  ADD COLUMN is_active BOOLEAN DEFAULT true;
```

### 消费规则
- 计算 FOB 时：按订单月份匹配付款条件（不按"当前"）
- Excel 里的付款条件仅用于反查，不覆盖系统主数据


## P2：修动总标准化

### 当前问题
`HEV` 被错误显示为绿色，大概率是 `EV` 子串匹配先于 `HEV`。

### 统一规则

| Family | 包含 |
|--------|------|
| EV | EV, BEV |
| HEV | HEV |
| PHEV | PHEV, SHS |
| ICE | ICE |
| MHEV | MHEV |
| LPG | LPG |

### 要求
- 后端解析、后端筛选、前端颜色、前端筛选共用同一套语义
- 筛选 `EV` 只能返回 EV/BEV，不能混入 HEV/PHEV
- 颜色映射：`BEV=#16a34a, PHEV=#2563eb, HEV=#d97706, MHEV=#ca8a04, ICE=#4b5563`


## P3：重构 Material 上传为 BOM digest

### 当前问题
上传 Material Excel → 直接写入 `material_sku_master` + `country_sku_fob_resolved`。FOB 和物料号混在一起。

### 目标
上传后生成结构化 BOM 底表，FOB 独立管理。

### BOM 底表字段
```
product              — brand + model + version
powertrain_family    — 标准化后（EV/HEV/PHEV/ICE/MHEV）
powertrain_raw       — 原始值
version / config
exterior_color_name
exterior_color_code
interior_color_name
bom_template
material_code
source_sheet         — Excel sheet 名
source_row           — Excel 行号
raw_payload          — JSONB 原始行
remark
```

### 前端布局
BOM 维护页尽量贴近原始 Excel 表结构，因为用户后续在这页上改底表。


## P4：国家级物料 + FOB 生命周期

### 新表 `ordering.country_material_fob`
```
country_code
product_identity     — brand|model|version|powertrain
material_code
payment_term_code
fob_eur
valid_from_month     — '2025-01'
valid_to_month       — NULL = 当前
source_baseline_id   — 来源上传
replaced_by_code     — 替代物料号
lifecycle_status     — active | phased_out | replaced
```

### 业务示例
```
J7 ICE / Romania:
  2025-01 ~ 2025-07  material A  FOB 15,000
  2025-08 ~ 至今     material B  FOB 14,500

J7 ICE / Sweden:
  2025-01 ~ 至今     material A  FOB 15,200

J7 ICE / Croatia:
  2025-03 ~ 至今     material B  FOB 14,800
```

同一产品在不同国家可以有不同切换时间。历史物料只结束有效期，不删除。


## P5：重做主页面为月度选品订单表

### 顶部筛选
```
[月份 ▼] [国家组/区域模板 ▼] [订单模板 ▼]
[品牌 ▼] [车型 ▼] [动总 ▼] [配置 ▼] [外饰 ▼] [内饰 ▼]
```

### 主体表格
```
┌─ 产品块 (动总颜色条) ──────────────────────────────────────┐
│ 配置 │ 外饰+色码 │ 内饰 │ 物料号+有效期 │ FOB │ RO qty/amt │ SE qty/amt │ TTL qty/amt │ 备注 │
│ 版型 │ Blue(A51)│ Black│ A-001(→至今) │15000│  50/750k   │  30/450k   │  80/1.2M    │      │
│ 版型 │ Grey(B12)│ Black│ A-001(→至今) │15000│  20/300k   │  10/150k   │  30/450k    │      │
└────────────────────────────────────────────────────────────┘
```

### 产品块颜色
| Powertrain | 颜色条 |
|-----------|--------|
| BEV/EV | 绿色 |
| PHEV/SHS | 蓝色 |
| HEV | 黄色 |
| MHEV | 琥珀色 |
| ICE | 深灰 |

### 国家列
- 支持 Czech、Slovakia 横向并列
- 支持 2-3 个国家组合模板（如「巴尔干组」= Croatia + Bulgaria + Romania）
- 每国两列：数量 | 金额


## P6：订单事实保存快照

### 当前问题
`order_quantity_cell` 只保存 `quantity + fob_eur`。一旦物料号或 FOB 变更，历史金额丢失。

### 目标
保存时快照：

```
order_month
country_code
material_code
quantity
fob_snapshot           — 保存时的 FOB
amount_snapshot        — quantity × fob_snapshot
payment_term_snapshot  — 保存时的付款条件
lifecycle_id           — 保存时的物料生命周期记录 ID
row_version
updated_by
```

### 消费
- 当前订单：显示最新快照
- 历史订单：显示当时快照（不可编辑）


## P7：定制色价格规则

### 新表 `ordering.colour_price_rule`
```
brand
colour_name / colour_code / colour_type
surcharge_eur
valid_from_month
valid_to_month
```

### Pricing mode（上传发布时选择）
| Mode | 含义 |
|------|------|
| `fob_includes_surcharge` | 上传 FOB 已含定制色价格 |
| `fob_excludes_surcharge` | 上传 FOB 是基础价，系统额外加 surcharge |

不能混用。上传发布时明确选择。


## P8：历史追溯和导出

### 页面显示
- 旧物料号：浅灰 + 删除线，显示 `有效期 → 历史 FOB`
- 新物料号：黑色，显示 `active`
- 切换点：在月份维度上可见分界

### 导出
- 按选品订单表格式导出（不是旧 matrix）
- 包含所有国家列、TTL 列、备注列
- 历史月份：显示当时快照值


## 实施顺序

| 阶段 | 内容 | 依赖 |
|------|------|------|
| **S1** | 写 V2 业务规格文档（本文档） | — |
| **S2** | P1 修付款条件 + P2 修动总标准化 | S1 |
| **S3** | P3 BOM digest + P4 国家物料/FOB 生命周期 | S2 |
| **S4** | 数据迁移：V1 → V2 模型 | S3 |
| **S5** | P5 重做主页面 | S4 |
| **S6** | P6 订单快照 + P7 定制色规则 | S5 |
| **S7** | P8 历史追溯 + 导出 | S6 |

## 技术路线

- 后端：FastAPI + SQLAlchemy + PostgreSQL，现有 `ordering` schema
- 数据迁移兼容 V1 表（`material_sku_master`, `country_sku_fob_resolved`, `order_quantity_cell`）
- 前端：现有 TSX 页面，主交互从「选国家看 matrix」改为「选月份和国家组维护订单表」
- JATO 月更不走这条线，Order Genius 数据完全独立


## 与 V1 的兼容

| V1 表 | V2 处理 |
|-------|---------|
| `material_sku_master` | 保留 → 升级为 BOM 底表 |
| `country_sku_fob_resolved` | 迁移到 `country_material_fob` |
| `order_quantity_cell` | 升级为快照表（加 snapshot 列） |
| `material_lifecycle` (V1 P5) | 升级为 `country_material_fob` 的一部分 |
