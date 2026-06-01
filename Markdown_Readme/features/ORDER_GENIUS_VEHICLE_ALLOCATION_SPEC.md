# Order Genius — PI Vehicle Allocation & Delivery Tracker 最终规格

> 目标：在现有 Order Genius 选品/PI 导出能力后，新增一个“PI 车辆池 + 虚拟车号 + VIN 绑定 + 物流交付查询”页面。
>
> 范围：MVP 优先落地，不重构现有选品矩阵；新增独立数据表、API、页面、Excel 导入导出。

---

## 1. 背景与现状

现有 Order Genius 主要解决：

- 物料主数据上传与发布。
- 国家 + 物料号 + 付款条件定位 FOB。
- 按国家、年月、车型、版型、颜色、物料号填写订购数量。
- 导出 PI / 选品订单表。

现有数据粒度是“数量格”：

```text
country + year + month + material_code -> quantity + fob
```

但业务现在需要追踪 PI 之后的单车交付：

- 一笔 PI 可能有多款车、多个 BOM、多个物料号。
- 一个月可能有多笔 PI。
- PI 刚下单时通常没有 VIN。
- 总代需要先用虚拟车号 Car Code 把车分给代理/消费者。
- 后续 VIN、船期、到港时间、可提车时间逐步回填。
- 物流同事需要通过 PI Code、Car Code、VIN 三种方式快速查询车辆。

因此新增模块不应塞进原选品矩阵，而应放在 PI 之后。

---

## 2. 模块定位

新增页面：

```text
/product/order-genius/vehicle-allocation
```

中文名：

```text
车辆分配与交付查询
```

英文名：

```text
Vehicle Allocation & Delivery Tracker
```

导航位置建议：

```text
Product Deck / Order Genius
├── Selection Matrix 选品表
├── PI Export PI导出
└── Vehicle Allocation 车辆分配与交付查询
```

模块不替代现有 Order Genius，而是承接 PI 后的车辆池管理。

---

## 3. 核心业务对象

采用三层结构：

```text
PI Header 一笔 PI
  └── PI Line 这笔 PI 里的某个 BOM / 物料号 / 配置行
        └── Vehicle Unit 单台车，绑定 Car Code / VIN
```

### 3.1 PI Header

表示一笔 PI 订单批次。

一笔 PI 可以包含多款车、多条物料行。

### 3.2 PI Line

表示 PI 内的一条配置/物料行。

一条 PI Line 对应一个 BOM 或 material_code，并带数量。

### 3.3 Vehicle Unit

表示单台车。

单台车一开始可能没有 VIN，但必须有系统可识别的 Car Code。

---

## 4. 编号规则

### 4.1 PI Code

系统内部标准 PI 编号：

```text
PI-{COUNTRY}-{YYYYMM}-{PI_SEQ}
```

示例：

```text
PI-RO-202607-001
PI-RO-202607-002
PI-SE-202607-001
```

含义：

| 段位 | 含义 |
|---|---|
| PI | Proforma Invoice，形式发票 |
| RO | 国家代码 |
| 202607 | PI 创建月份 / 下单月份 |
| 001 | 该国家该月份第几笔 PI |

重要原则：

- PI Code 只表示“一笔 PI 批次”。
- 不放车型。
- 不放 BOM。
- 不放物料号。
- 不放船名。
- 不放 ETD / ETA。
- 不放 VIN。

原因：一笔 PI 可能有多款车、多物料号，物流信息也可能后续变化。

### 4.2 Official PI No

外部真实 PI 号，原样保存，不强制格式。

字段名：

```text
official_pi_no
```

用途：对接财务、总代原始单据。

### 4.3 PI Line Code

PI 内部行号：

```text
{PI_CODE}-L{LINE_SEQ}
```

示例：

```text
PI-RO-202607-001-L01
PI-RO-202607-001-L02
PI-RO-202607-001-L03
```

### 4.4 Car Code

虚拟车号：

```text
CAR-{COUNTRY}-{YYMM}-{PI_SEQ}-L{LINE_SEQ}-{UNIT_SEQ}
```

示例：

```text
CAR-RO-2607-001-L01-0001
CAR-RO-2607-001-L01-0002
CAR-RO-2607-001-L02-0001
```

含义：

```text
CAR-RO-2607-001-L02-0012
= 罗马尼亚 / 2026年7月 / 第1笔PI / 第2条配置行 / 第12台车
```

Car Code 用于在无 VIN 阶段进行车辆分配和查询。

---

## 5. 字段命名规范

### 5.1 时间字段

注意：原始需求里写了两个 ETD，第二个应改为 ETA。

| 字段 | 全称 | 中文 |
|---|---|---|
| etd | Estimated Time of Departure | 预计离港时间 |
| eta | Estimated Time of Arrival | 预计到港时间 |
| actual_departure_date | Actual Departure Date | 实际离港时间 |
| actual_arrival_date | Actual Arrival Date | 实际到港时间 |
| ready_for_pickup_date | Ready for Pickup Date | 可提车时间 |

### 5.2 VIN

VIN = Vehicle Identification Number，车辆识别代码 / 车架号。

### 5.3 BOM

BOM = Bill of Materials，物料清单 / 配置物料模板。

### 5.4 PI

PI = Proforma Invoice，形式发票。

---

## 6. 数据库设计

建议继续使用 `ordering` schema。

### 6.1 表：ordering.pi_order_header

用途：记录 PI 主信息。

```sql
CREATE TABLE ordering.pi_order_header (
    pi_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pi_code TEXT NOT NULL,
    official_pi_no TEXT NULL,

    country_code TEXT NOT NULL,
    country_name TEXT NULL,

    order_date DATE NULL,
    order_month TEXT NOT NULL,
    pi_sequence_no INTEGER NOT NULL,

    shipping_schedule_url TEXT NULL,
    feishu_tracking_url TEXT NULL,
    ship_name TEXT NULL,

    etd DATE NULL,
    eta DATE NULL,
    actual_departure_date DATE NULL,
    actual_arrival_date DATE NULL,
    ready_for_pickup_date DATE NULL,

    status TEXT NOT NULL DEFAULT 'draft',
    remark TEXT NULL,

    row_version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NULL,
    updated_by TEXT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_pi_order_header_pi_code UNIQUE (pi_code),
    CONSTRAINT uq_pi_order_header_country_month_seq UNIQUE (country_code, order_month, pi_sequence_no)
);
```

推荐 status：

```text
draft
ordered
in_production
shipped
arrived
ready_for_pickup
closed
cancelled
```

### 6.2 表：ordering.pi_order_line

用途：记录 PI 中每个 BOM / 物料号配置行。

```sql
CREATE TABLE ordering.pi_order_line (
    pi_line_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pi_id UUID NOT NULL REFERENCES ordering.pi_order_header(pi_id) ON DELETE CASCADE,

    pi_code TEXT NOT NULL,
    pi_line_code TEXT NOT NULL,
    line_sequence_no INTEGER NOT NULL,

    material_code TEXT NULL,
    bom TEXT NULL,

    brand TEXT NULL,
    model_name TEXT NULL,
    version TEXT NULL,
    powertrain TEXT NULL,
    exterior_color_name TEXT NULL,
    exterior_color_code TEXT NULL,
    interior_color_name TEXT NULL,
    interior_colour_code TEXT NULL,

    quantity INTEGER NOT NULL DEFAULT 0,
    fob_eur NUMERIC(12, 2) NULL,
    amount_eur NUMERIC(14, 2) NULL,

    remark TEXT NULL,

    row_version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NULL,
    updated_by TEXT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_pi_order_line_code UNIQUE (pi_line_code),
    CONSTRAINT uq_pi_order_line_pi_line_seq UNIQUE (pi_id, line_sequence_no),
    CONSTRAINT ck_pi_order_line_quantity_non_negative CHECK (quantity >= 0)
);
```

### 6.3 表：ordering.pi_vehicle_unit

用途：记录单台车。

```sql
CREATE TABLE ordering.pi_vehicle_unit (
    vehicle_unit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    pi_id UUID NOT NULL REFERENCES ordering.pi_order_header(pi_id) ON DELETE CASCADE,
    pi_line_id UUID NOT NULL REFERENCES ordering.pi_order_line(pi_line_id) ON DELETE CASCADE,

    pi_code TEXT NOT NULL,
    pi_line_code TEXT NOT NULL,
    car_code TEXT NOT NULL,
    vin TEXT NULL,

    material_code TEXT NULL,
    bom TEXT NULL,

    brand TEXT NULL,
    model_name TEXT NULL,
    version TEXT NULL,
    powertrain TEXT NULL,
    exterior_color_name TEXT NULL,
    exterior_color_code TEXT NULL,
    interior_color_name TEXT NULL,
    interior_colour_code TEXT NULL,

    production_date DATE NULL,
    etd DATE NULL,
    eta DATE NULL,
    actual_departure_date DATE NULL,
    actual_arrival_date DATE NULL,
    ready_for_pickup_date DATE NULL,
    ship_name TEXT NULL,
    country_code TEXT NOT NULL,

    dealer_code TEXT NULL,
    dealer_name TEXT NULL,
    customer_ref TEXT NULL,

    allocation_status TEXT NOT NULL DEFAULT 'unallocated',
    logistics_status TEXT NOT NULL DEFAULT 'pending',

    remark TEXT NULL,

    row_version INTEGER NOT NULL DEFAULT 1,
    created_by TEXT NULL,
    updated_by TEXT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT uq_pi_vehicle_unit_car_code UNIQUE (car_code),
    CONSTRAINT uq_pi_vehicle_unit_pi_car UNIQUE (pi_code, car_code)
);

CREATE UNIQUE INDEX uq_pi_vehicle_unit_vin_not_null
ON ordering.pi_vehicle_unit(vin)
WHERE vin IS NOT NULL AND vin <> '';
```

推荐 allocation_status：

```text
unallocated
reserved
allocated
delivered
cancelled
```

推荐 logistics_status：

```text
pending
in_production
ready_for_shipping
on_vessel
arrived_at_port
in_warehouse
ready_for_pickup
delivered
```

### 6.4 可选表：ordering.vehicle_logistics_event

MVP 可不做，V2 再做。

```sql
CREATE TABLE ordering.vehicle_logistics_event (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vehicle_unit_id UUID NOT NULL REFERENCES ordering.pi_vehicle_unit(vehicle_unit_id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    event_date DATE NULL,
    source TEXT NULL,
    remark TEXT NULL,
    created_by TEXT NULL,
    created_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 7. 从现有 Order Genius 生成 PI 车辆池

### 7.1 生成逻辑

从现有选品矩阵中选择某国家、某年月、若干物料号后：

```text
order_quantity_cell rows
    -> create pi_order_header
    -> create pi_order_line by material_code
    -> expand quantity into pi_vehicle_unit rows
```

示例：

```text
PI Code: PI-RO-202607-001
L01: OMODA9 Exclusive AWD Black/Red, quantity = 40
L02: J7 SHS Premium White, quantity = 35
L03: JAECOO5 Black, quantity = 25
```

生成：

```text
PI-RO-202607-001-L01 -> 40台 Vehicle Unit
PI-RO-202607-001-L02 -> 35台 Vehicle Unit
PI-RO-202607-001-L03 -> 25台 Vehicle Unit
```

每台车获得 Car Code。

### 7.2 数量修改原则

PI 生成后，不建议直接联动修改原选品矩阵数量。

MVP 规则：

- 选品矩阵负责订单意向数量。
- PI Header / PI Line / Vehicle Unit 负责 PI 后单车池。
- 一旦 PI 状态不是 draft，不允许随意减少已经生成的 Vehicle Unit。
- 如果需要调整数量，走 add line / cancel unit / remark，不直接硬删历史。

---

## 8. Excel 导入设计

### 8.1 导入模板

MVP 必须支持 Excel 导入维护：

```text
PI Code
Official PI No
Car Code
VIN
BOM
Material Code
Brand
Model
Version
Powertrain
Exterior Colour
Interior Colour
Order Date
Production Date
ETD
ETA
Ship Name
Country
Dealer Code
Dealer Name
Customer Ref
Allocation Status
Logistics Status
Ready for Pickup Date
Shipping Schedule URL
Feishu Tracking URL
Remark
```

### 8.2 导入匹配规则

按优先级匹配：

1. `vin` 非空：优先用 VIN 匹配单车。
2. `pi_code + car_code`：匹配单车。
3. `car_code` 全局唯一：匹配单车。
4. 如果都不存在，则新增。

### 8.3 导入校验

必须校验：

- 同一个 VIN 不可绑定多台车。
- 同一个 Car Code 不可重复。
- 同一个 PI 内 `pi_line_code` 不可重复。
- ETA 不应早于 ETD。
- ready_for_pickup_date 不应早于 ETA，除非用户确认或仅 warning。
- allocation_status 必须在枚举范围内。
- logistics_status 必须在枚举范围内。

### 8.4 导入模式

支持两个模式：

```text
preview
apply
```

Preview 返回：

- total_rows
- new_headers
- new_lines
- new_units
- updated_units
- warnings
- errors
- preview_rows

Apply 只有在没有 blocking error 时执行。

---

## 9. API 设计

建议新增 route 文件：

```text
06_AppPlatform/backend/app/api/routes/order_genius_vehicle_allocation.py
```

或者放在现有 `order_genius.py` 内，但建议独立文件，避免当前 route 过长。

### 9.1 PI Header

```http
GET /api/order-genius/vehicle-allocation/pi
```

Query：

```text
country
month
status
keyword
page
page_size
```

返回 PI 列表和汇总。

```http
POST /api/order-genius/vehicle-allocation/pi
```

创建 PI Header。

```http
GET /api/order-genius/vehicle-allocation/pi/{pi_code}
```

返回 PI Header + Lines + Summary。

```http
PATCH /api/order-genius/vehicle-allocation/pi/{pi_code}
```

更新船期、状态、链接、备注。

### 9.2 PI Line

```http
POST /api/order-genius/vehicle-allocation/pi/{pi_code}/lines
PATCH /api/order-genius/vehicle-allocation/lines/{pi_line_code}
```

创建/更新 PI Line。

### 9.3 Vehicle Unit

```http
GET /api/order-genius/vehicle-allocation/vehicles
```

Query：

```text
keyword
pi_code
pi_line_code
car_code
vin
material_code
bom
country
ship_name
allocation_status
logistics_status
eta_from
eta_to
ready_from
ready_to
page
page_size
```

```http
GET /api/order-genius/vehicle-allocation/vehicles/{car_code}
PATCH /api/order-genius/vehicle-allocation/vehicles/{car_code}
```

更新 VIN、状态、代理、客户代号、物流节点。

### 9.4 三项主检索

```http
GET /api/order-genius/vehicle-allocation/search?keyword={keyword}
```

自动判断：

- 命中 PI Code：返回 PI 级详情。
- 命中 Car Code：返回单车详情。
- 命中 VIN：返回单车详情。
- 都不命中：返回空结果。

### 9.5 Excel 导入导出

```http
POST /api/order-genius/vehicle-allocation/import/preview
POST /api/order-genius/vehicle-allocation/import/apply
POST /api/order-genius/vehicle-allocation/export
```

### 9.6 从选品矩阵生成 PI

```http
POST /api/order-genius/vehicle-allocation/generate-from-order-matrix
```

Body：

```json
{
  "countryCode": "RO",
  "countryName": "Romania",
  "orderYear": 2026,
  "orderMonth": 7,
  "officialPiNo": "optional external PI no",
  "lineItems": [
    {
      "materialCode": "MATERIAL001",
      "quantity": 40
    }
  ]
}
```

返回：

```json
{
  "piCode": "PI-RO-202607-001",
  "lineCount": 3,
  "vehicleCount": 100
}
```

---

## 10. 前端页面设计

### 10.1 页面布局

页面：

```text
Vehicle Allocation & Delivery Tracker
```

分为 4 块：

```text
1. 顶部搜索区
2. PI 汇总卡片
3. 筛选工具栏
4. 车辆明细表
```

### 10.2 顶部搜索区

一个主搜索框：

```text
Search by PI Code / Car Code / VIN
```

用户输入后自动识别。

示例：

```text
PI-RO-202607-001
CAR-RO-2607-001-L02-0012
LVVDB21B1RT000001
```

### 10.3 PI 汇总卡片

当搜索 PI Code 时显示：

| 指标 | 含义 |
|---|---|
| Total Units | 车辆总数 |
| Allocated | 已分配 |
| Reserved | 已预留 |
| Unallocated | 未分配 |
| VIN Assigned | 已绑定 VIN |
| VIN Missing | 未绑定 VIN |
| On Vessel | 海运中 |
| Arrived | 已到港 |
| Ready for Pickup | 可提车 |

同时展示：

- PI Code
- Official PI No
- Country
- Order Date
- Ship Name
- ETD
- ETA
- Ready for Pickup Date
- Shipping Schedule URL
- Feishu Tracking URL

### 10.4 筛选工具栏

字段：

- Country
- Month
- Ship Name
- Model
- Version
- Material Code
- BOM
- Allocation Status
- Logistics Status
- ETA Range
- Ready Date Range
- VIN Missing only
- Unallocated only

### 10.5 明细表字段

MVP 表格列：

```text
PI Code
PI Line Code
Car Code
VIN
BOM
Material Code
Brand
Model
Version
Exterior Colour
Interior Colour
Production Date
ETD
ETA
Ready for Pickup Date
Ship Name
Country
Dealer
Allocation Status
Logistics Status
Remark
```

### 10.6 表格交互

- 点击行打开右侧 Drawer。
- Drawer 内编辑单车详情。
- VIN 可后补。
- allocation_status 可修改。
- logistics_status 可修改。
- ETD / ETA / Ready Date 可修改。
- 支持批量选择车辆后批量更新：
  - Ship Name
  - ETD
  - ETA
  - Ready Date
  - Dealer
  - Allocation Status
  - Logistics Status

### 10.7 导出

支持导出当前筛选结果：

```text
Export Current View
Export PI Vehicle List
Export VIN Missing List
Export Ready for Pickup List
```

---

## 11. 权限设计

沿用现有角色体系。

推荐权限：

| 角色 | 权限 |
|---|---|
| admin | 全部读写、导入、导出、删除/cancel |
| editor | 创建/编辑 PI、Line、Vehicle，导入导出 |
| logistics_editor | 更新 VIN、船期、到港、可提车、物流状态 |
| order_filler | 查看/维护自己国家范围内 PI 与车辆 |
| viewer | 只读 |
| dealer_viewer | 只能看授权国家/代理的数据，只读 |

公开查询端口不要直接开放全库。

如果要做开放查询，建议另做：

```text
/public/vehicle-lookup?token=xxx
```

公开查询只返回：

- PI Code
- Car Code
- VIN
- Model
- Version
- Colour
- ETA
- Ready for Pickup Date
- Logistics Status

不返回 FOB、金额、付款条件、其他国家数据。

---

## 12. CoC 边界

CoC = Certificate of Conformity，一致性证书。

CoC 不适合作为订单分配识别依据。

原因：

- CoC 证明车辆合规，不代表订单归属。
- CoC 不天然包含 PI 分配关系。
- CoC 不知道车辆是否已被消费者订购。
- CoC 不知道代理分配、船期、到港、可提车时间。

订单和交付识别应使用：

```text
PI Code + PI Line Code + Car Code + VIN
```

---

## 13. MVP 开发任务清单

### Phase 1：数据库与模型

- [ ] 新增 Alembic migration：
  - `ordering.pi_order_header`
  - `ordering.pi_order_line`
  - `ordering.pi_vehicle_unit`
- [ ] 新增 SQLAlchemy models。
- [ ] 添加唯一约束和索引。
- [ ] 添加枚举校验或服务层校验。

### Phase 2：Service / Repository

- [ ] 生成 PI Code。
- [ ] 生成 PI Line Code。
- [ ] 生成 Car Code。
- [ ] 从 order matrix 生成 PI Header / Line / Vehicle Unit。
- [ ] 查询 PI Summary。
- [ ] 搜索 PI / Car Code / VIN。
- [ ] 更新单车 VIN / 物流 / 分配状态。
- [ ] Excel 导入 preview / apply。
- [ ] Excel 导出。

### Phase 3：API

- [ ] 新增 route。
- [ ] PI CRUD。
- [ ] PI Line CRUD。
- [ ] Vehicle query / update。
- [ ] Search endpoint。
- [ ] Import preview / apply。
- [ ] Export endpoint。
- [ ] 权限校验和国家访问校验。

### Phase 4：前端页面

- [ ] 新增页面 `/product/order-genius/vehicle-allocation`。
- [ ] 顶部三项搜索框。
- [ ] PI Summary cards。
- [ ] 筛选栏。
- [ ] 车辆明细表。
- [ ] 行 Drawer 编辑。
- [ ] 批量更新。
- [ ] Excel 导入面板。
- [ ] Excel 导出按钮。

### Phase 5：测试

- [ ] PI Code 生成测试。
- [ ] 一个月多个 PI 测试。
- [ ] 一笔 PI 多个 PI Line 测试。
- [ ] 数量展开为 Vehicle Unit 测试。
- [ ] Car Code 唯一性测试。
- [ ] VIN 唯一性测试。
- [ ] 导入重复 VIN 报错测试。
- [ ] 导入更新已有 Car Code 测试。
- [ ] 搜索 PI / Car Code / VIN 测试。
- [ ] 权限和国家隔离测试。

---

## 14. 推荐文件结构

Backend：

```text
06_AppPlatform/backend/app/api/routes/order_genius_vehicle_allocation.py
06_AppPlatform/backend/app/api/order_genius_vehicle_schemas.py
06_AppPlatform/backend/app/services/order_genius_vehicle_service.py
06_AppPlatform/backend/app/infra/order_genius_vehicle_repository.py
06_AppPlatform/backend/app/services/order_genius_vehicle_import_parser.py
06_AppPlatform/backend/app/services/order_genius_vehicle_exporter.py
06_AppPlatform/backend/alembic/versions/20260601_00xx_order_genius_vehicle_allocation.py
```

Frontend：

```text
06_AppPlatform/frontend/src/pages/OrderGeniusVehicleAllocationPage.tsx
06_AppPlatform/frontend/src/api/orderGeniusVehicleAllocation.ts
06_AppPlatform/frontend/src/components/order-genius/vehicle/VehicleSearchBar.tsx
06_AppPlatform/frontend/src/components/order-genius/vehicle/PiSummaryCards.tsx
06_AppPlatform/frontend/src/components/order-genius/vehicle/VehicleAllocationTable.tsx
06_AppPlatform/frontend/src/components/order-genius/vehicle/VehicleDetailDrawer.tsx
06_AppPlatform/frontend/src/components/order-genius/vehicle/VehicleImportPanel.tsx
```

Tests：

```text
06_AppPlatform/backend/tests/unit/test_order_genius_vehicle_code_generation.py
06_AppPlatform/backend/tests/unit/test_order_genius_vehicle_service.py
06_AppPlatform/backend/tests/unit/test_order_genius_vehicle_import.py
06_AppPlatform/backend/tests/api/test_order_genius_vehicle_routes.py
```

---

## 15. 验收标准

### 15.1 编号验收

- [ ] Romania 2026-07 第一笔 PI 生成 `PI-RO-202607-001`。
- [ ] 同月第二笔生成 `PI-RO-202607-002`。
- [ ] Sweden 2026-07 第一笔生成 `PI-SE-202607-001`。
- [ ] 一笔 PI 三个物料号，生成 `L01/L02/L03`。
- [ ] L02 第 12 台生成类似 `CAR-RO-2607-001-L02-0012`。

### 15.2 单车池验收

- [ ] 一个 PI Line quantity=40 时，生成 40 台 Vehicle Unit。
- [ ] 没有 VIN 时仍可通过 Car Code 查询。
- [ ] 后补 VIN 后可通过 VIN 查询。
- [ ] 同一个 VIN 不能绑定两台车。

### 15.3 查询验收

- [ ] 输入 PI Code，显示整笔 PI 和所有车辆。
- [ ] 输入 Car Code，显示单台车。
- [ ] 输入 VIN，显示单台车。
- [ ] 按 Allocation Status 可筛选未分配车辆。
- [ ] 按 Ready for Pickup Date 可筛选可提车车辆。

### 15.4 物流验收

- [ ] 可维护 Ship Name。
- [ ] 可维护 ETD。
- [ ] 可维护 ETA。
- [ ] 可维护 Ready for Pickup Date。
- [ ] 可维护 Shipping Schedule URL。
- [ ] 可维护 Feishu Tracking URL。

### 15.5 导入导出验收

- [ ] Excel 导入可新增车辆。
- [ ] Excel 导入可更新已有 Car Code。
- [ ] Excel 导入可后补 VIN。
- [ ] Excel 导出包含当前筛选结果。

---

## 16. 不做范围 / 后续 V2

MVP 暂不做：

- 不接飞书 API，只保存飞书查询链接。
- 不做 CoC 自动识别订购关系。
- 不做复杂客户实名信息。
- 不做实时船司 API。
- 不做完整物流事件时间轴。
- 不做公开无鉴权查询。

V2 可扩展：

- 飞书物流 API 同步。
- 物流事件表和时间轴。
- 船期自动更新。
- Dealer Portal。
- 公共查询 token。
- VIN 批量 OCR / PDF 导入。
- CoC 文件归档，但不作为订单识别主键。

---

## 17. 最终结论

最终落地结构：

```text
PI Code      = 一笔 PI 批次
PI Line Code = PI 里的某个 BOM / 物料号 / 配置行
Car Code     = 无 VIN 阶段用于分配和查询的虚拟单车编号
VIN          = 后续真实车架号
```

推荐编号：

```text
PI Code:      PI-RO-202607-001
PI Line Code: PI-RO-202607-001-L02
Car Code:     CAR-RO-2607-001-L02-0012
```

主页面目标：

```text
通过 PI Code / Car Code / VIN 三种方式，快速查询车辆属于哪笔 PI、什么配置、是否已分配、船什么时候到、什么时候可以提车。
```

这就是 Order Genius 从“选品表”扩展到“PI 后车辆交付管理”的最小闭环。
