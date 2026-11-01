# Order Genius V2 — 业务规格与实施路线

## 2026-11-01 Material Assets 按月批量下载补充契约

- Order Filler及以上在原Material Assets标题栏使用Download，选择YYYY-MM后预览、勾选、下载；权限仍为获授权国家×品牌，不授予维护或回收站权限。下载勾选与Compare及Create PI状态独立，不改变选品数量。
- 月份按BOM物料/颜色真实生效区间与该月的交集筛选，月中更替的旧/新物料都可出现并显示实际日期；日期缺失沿用已有生命周期规则，日期冲突明确标示。无有效物料时不回退全库，不以FOB有无作为资料下载门禁。
- 文件取预览时当前明确关联且未移入回收站的配置版本、外饰图片组、内饰图片组；不是历史文件回放，不按上传日期或文件名倒推旧绑定。缺失资料在预览及下载索引中明确列出，不编造文件或静默忽略。
- 勾选可按物料或资料组进行；零显式勾选表示当前月份预览范围全部可下载资料，主操作显示Download all及实际文件数，有勾选则显示Download selected及实际文件数。切月清掉旧下载勾选与旧结果，旧请求不得覆盖新月；不能把Compare的8物料限制或旧单组50文件限制套到批量预览。
- ZIP固定按Model → Trim → 含**的基准物料 → Configuration / Exterior / Interior组织，外/内饰继续按色码/标识分组，多角度照片全部保留。每个所选物料包独立完整，共用文件可在各物料目录重复，但不在数据库复制。目录中的**转换为__，清单保留原始物料号、配置UUID、图片组/文件标识、有效日期及相对路径；同名文件不得覆盖，路径须兼容Windows且不能穿越目录。
- 下载由服务端再次核验月份、选择和权限；预览后关联变化须提示重读，不能悄悄切换版本。单批最多512MiB，按ZIP中实际重复打包的文件字节计数；超容量明确要求按Model/物料分组，不静默截断。ZIP逐文件写入受控临时文件，成功/失败/取消响应均释放，不能全量图片字节常驻内存。已有单组下载继续复用同一归档实现。
- 扩展现有资料目录、BOM日期解析、授权文件读取和ZIP下载，不新增数据库表、迁移、后台任务或通用下载平台；不修改BOM、FOB、PI、时间轴、文件或绑定。

## 2026-11-01 Material Assets 上传与回收站补充契约

- 一个含`**`基准号绑定一套配置，其展开颜色SKU共用该配置。基准号不同不代表配置内容必然不同：同Model不同基准可显式复用同一配置版本UUID，内饰/外饰仍分别读取各自BOM及图片绑定；不因配置相同自动合并物料。更新仅改变明确目标基准，不连带切换其他引用。对比区分配置内容与BOM内外饰差异，保留原表来源，不用BOM字段偷偷改写文件中的值。
- 配置资料本轮仅处理 XLSX，不扩展 PDF/Word。继续明确选择配置列，保留不可变版本、单元格来源及原文件。
- 图片支持 JPEG/PNG/GIF/WebP/BMP/TIFF/AVIF/HEIC/HEIF；按实际解码结果验证，不以扩展名伪装兼容。输入最多100MiB/8000万像素，覆盖50–70MB照片；超过6MB保存不超过6,000,000字节的优化版（长边最多4096px），不同时保存大原图；小图不强制重压。另生成480px缩略图、2048px展示图；EXIF方向纠正、ICC转sRGB，派生图去除EXIF。配置XLSX仍保留原文件，不压缩改写。
- 复用 `upload_toolkit` 分片、校验及恢复；中断后选择同一文件继续，暂停不清分片，取消清理未提交上传。完成并提交与取消须明确区分，不能把已经保存的资料报告为取消成功。
- 大图不直接进入列表：缩略图懒加载，图库按需加载展示图且只预载相邻图。Blob URL和图片请求在离屏/关闭/授权变化时释放或中止，不常驻缓存全量原图；下载才请求原文件。
- Material Assets标题栏右侧提供Recycle bin入口，不在选品页增加常驻区块。回收站分Photos和Configurations两个目录，合计上限2GB（2,000,000,000字节，含原文件及派生图片）；不自动清空或淘汰旧项。容量满时拒绝本次移入，提示先清理；并发写入由后端统一校验。
- 图片/配置卡片右上角×为移入回收站，保留可访问名称。解除关联与移除共享图片分开；共享删除保留影响范围确认。配置可移入回收站，关联对比暂不可用，版本快照及既有绑定保留；恢复不覆盖后来新选的配置。永久删除仅移除所选文件，仍有后续/绑定引用的版本快照不物理删除。被引用的整图片组仍须先解除关联。
- Editor及以上可在授权资料范围内移入和恢复；永久删除另需明确确认不可恢复。Order Filler不获得维护/回收站权限。恢复不覆盖新的物料绑定或配置版本。
- 本补充扩展原Material Assets模型/路由，不新建文件平台、后台队列或恢复控制面；不改变BOM、FOB、数量、PI或时间轴关联。实施与交付状态以进度文档为准。

## 2026-10-31 Notice Board / Material Assets 当前契约

本文件是唯一业务规则，较新日期的补充契约优先；实现与发布状态仅记在 `ORDER_GENIUS_IMPLEMENTATION_ROADMAP.md`。

- BOM Admin 仍唯一维护物料号、Product fields、生命周期日期、国家与报价。本轮不改变它们，也不改数量、旧 PI、颜色释放及成交快照。
- BOM Admin 搜索同时匹配完整颜色SKU和含 `**` 的基准物料号；二者属于同一物料条件组，同类取并集、与车型/动力等条件取交集。`*` 是基准号的字面字符，不扩展成任意通配符；保留原国家/品牌权限、历史物料及清空行为。
- 替换现有 Notice Board / 泳道：按 Model、Trim 和含 `**` 的基准物料展示轻量线性日历；共用一个双柄时间窗口，默认最近四个月，可调整到今天前后三年。日期继续读取原 `material-timeline`，不建第二套日期或推断更替。保留授权范围、个人已读和明确的缺失/冲突状态；不增加“跳转编辑 BOM”入口。
- Material Assets 入口位于选品页 Filters & Actions 的 BOM / Assets 工具区，与 BOM Admin 使用同类近全屏大面板；Mega Menu 不提供该入口。复用现有资料内容，不离开选品页、不重建选品状态。管理单位是品牌＋Model＋含 `**` 的 `bomTemplate`，不是展开颜色后的完整 SKU，也不修改物料号。配置版本使用 UUID，与该基准关联；读取 BOM Product fields 而不是另造小电池/FWD等描述。
- 选品页移除标题下说明和常驻的国家/年/月/品牌/车型/动力标签；筛选功能及工具区内摘要保留。Enter new batch 与 Order Genius 标题同排，原有余量/保存/PI门禁不变。Notice Board 折叠后只保留紧凑单行，提醒和日历随面板折叠，未读数量仍可见。
- Notice Board 与配置对比弹层使用各自独立的组件标识；对比开关、国家范围及授权变化后，页面最多保留一个 Notice Board，不残留旧面板。授权变化仍重置各自受限状态。
- 时间轴更替仅以已保存的 `material-timeline.links` 为准，按品牌、Model及国家范围显示物料链或分支关系，并与实际日期条对应。不能根据日期接近推断替换；不同国家的链不能拼接，重叠和缺失日期不被改写。关系维护仍留在原BOM编辑流程。
- Order Filler 继续在选品表修改数量；Notice Board 解释更替，Assets 维护资料。已确认的更替可共用一条日期演进线，旧/新段保留真实日期和重叠，邻接关系提供 Compare，顺序固定旧→新。基准物料行可加入2–8个同/跨Model物料对比；按品牌＋Model＋`**`基准去重，不使用PI勾选，不重复加入颜色SKU。
- 对比选择由选品页统一持有，按加入顺序编号并用浅→深色阶高亮物料号标签；日历、选品表及对比列头顺序一致，不覆盖动力色。关闭弹层保留选择、数量和表格位置，清空才移除高亮。复用配置比较/原文件接口；默认仅差异、可看全部，并展示Model/Trim/日期/UUID来源。无配置或不可访问须明确失败，不能静默删去目标或重排。
- 选品表外饰及内饰图例在hover或键盘聚焦时预览已关联图片组的第一张，点击进入现有图库；按已保存的物料/色码关联取图，不从HEX猜图库，不进入管理页，不新增封面或图片处理系统。
- 数量正常成功保存仅局部更新数字及相关金额/合计，不整表刷新、不重置列宽/展开/滚动/焦点；Enter/Tab可连续填格。同格请求按顺序保存，晚返回不能覆盖较新的输入。PI余量后台刷新不切换整表编辑状态，但Create PI须等待所有保存和最新余量核验；保留原rowVersion/expectedAllocatedQuantity、失败留输入及409明确核对规则。
- 每个 Model 有共用图片库。外饰色码和 HEX 来自该基准的既有 BOM 颜色；每个色码可关联含多张照片的图片组。同 Model 显式复用图片组引用，不复制文件，不按 HEX 或名称猜色码，不新增目标物料没有的颜色。内饰复用限定同 Model 和对应内饰标识，不能把棕内饰误换成黑色。
- 配置上传复用原解析器，必须明确选择文件中的配置列；不按名称猜物料。更新创建新 `ConfigVersion` 与不可变配置值/来源快照，原文件可下载、来源可定位；不会用可变 Trim 当前值冒充历史版本。不能解析的文件明确报错，不返回伪对比结果。
- 可选择同 Model 或跨 Model 多个基准比较配置；缺值不等于不配/降配，保留文件、单元格、版本 UUID 等来源。原始文件、图片和关联持久化，刷新不丢失。配置更新/解除关联和删除分开；删除被引用版本或图片组必须拒绝，移除共享图片须提示影响并确认。
- Editor 及以上可维护配置、图片及关联；Order Filler 仅获授权国家×品牌的预览、下载、Compare，无上传、修改、复用或删除权。所有列表、原文件下载、图片读取、比较及写入均在服务端校验，不能只隐藏按钮。
- 默认英文，可切中文；紧凑布局、必要字段标签、tooltip补充信息，不常驻教学段落。Historical 在资料页最下默认折叠，展开仍按 Model 分组。选品 HEX 可就地预览关联图片，不跳转、不改数量。
- 复用 FastAPI＋SQLAlchemy/PostgreSQL、现有配置解析/比较和时间轴/通知工作流；仅增加必要的资料持久化。不另建 BOM、BI、文件平台、解析器或长期恢复系统。
- 本轮流程：MD → 实现 → 自动测试与真实浏览器 → Candidate验收；不直接推广 Active/intl。AI自然语言解释、未有来源的规格推断不作为本批已完成能力。

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
