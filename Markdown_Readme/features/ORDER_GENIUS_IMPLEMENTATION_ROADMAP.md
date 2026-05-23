# Order Genius — 当前状态与待办

> 当前版本: V1.5 | 目标版本: V2 | V2 spec: [ORDER_GENIUS_V2_SPEC.md](./ORDER_GENIUS_V2_SPEC.md)
> 更新: 2026-05-23

## V1.5 已实现

### 数据
- Material 上传（分片 5MB）、解析、publish baseline
- `material_sku_master`：品牌/车型/版型/外饰/内饰/BOM 模板/物料号/生命周期状态
- `country_sku_fob_resolved`：国家 + 物料号 + 付款条件 → FOB
- `country_payment_term_master`：国家付款条件（seed 仍需修正）
- `order_quantity_cell`：country + year + month + material_code → quantity + fob
- `material_lifecycle`：表已建，API 已有，数据待填充
- FOB 字段 `base_fob_eur` / `payment_term_adjustment_eur` / `colour_surcharge_eur` 已改 nullable

### 页面
- 单国家 matrix：品牌/车型/动总/版型/颜色/物料号筛选
- 月份筛选（All months / 单月）
- 列显隐面板（Months / Amount / TTL Qty / TTL Amt / FOB / Material / Remark）
- 产品块分组（品牌+车型+版型+动总），动总颜色条
- 数量编辑（点击格 → 输入 → Enter/Blur 保存）
- TTL Qty / TTL Amt 保存后本地即时更新
- 历史物料浅灰 + 删除线，不可编辑
- 物料号 datalist 自动补全
- 颜色名 + 色码展示
- 物料有效期显示（`effective_from → effective_to`）
- 备注列

### 并发保护
- `order_quantity_cell` 有 `row_version` 乐观锁
- 首次编辑成功 → 返回新 rowVersion → 前端本地更新
- 二次编辑用旧 rowVersion → 后端返回 409 `Concurrent update conflict`
- 唯一键 `country + year + month + material_code`，不同国家不冲突

---

## V1.5 未实现

### BOM 层
- [ ] BOM Admin 网页维护（product / variant / material_option）
- [ ] BOM 行级 `row_version` 并发保护
- [ ] BOM 增删改 + 变更历史

### Digest Review
- [ ] 上传生成 candidate diff 而非直接覆盖
- [ ] Accept / Reject / Mark & Edit 三种操作
- [ ] 字段级 diff（当前值 vs Excel 候选 vs 修补值）
- [ ] `admin_locked` 字段保护

### 国家生命周期与 FOB
- [ ] 国家级 lifecycle 编辑页
- [ ] FOB history 编辑页（含 `valid_from/to`）
- [ ] Lifecycle / FOB 行级 `row_version`
- [ ] 付款条件有效期（`valid_from_month` / `valid_to_month`）
- [ ] 付款条件 seed 修正（Croatia=TT, Sweden=LC90, Greece=LC90, Hungary=LC120, Bulgaria=LC120, Romania=LC120）

### 动总标准化
- [ ] 统一 powertrain normalizer（EV/BEV→EV, HEV→HEV, PHEV/SHS→PHEV, ICE→ICE）
- [ ] 后端解析 + 筛选 + 前端颜色 + 产品块分组共用

### 多国订单表
- [ ] 国家组/区域模板
- [ ] 横向多国列（数量 + 金额）
- [ ] 订单模板
- [ ] 订单快照（fob_snapshot / amount_snapshot / payment_term_snapshot / lifecycle_id）

### 定制色
- [ ] 颜色价格规则表（brand + colour + surcharge + valid_from/to）
- [ ] Pricing mode 选择（`fob_includes_surcharge` vs `fob_excludes_surcharge`）

### 历史追溯
- [ ] 物料切换点在月份维度可见
- [ ] 历史 FOB 可见
- [ ] 历史订单数量可查（只读）

### 导出
- [ ] 按选品订单表格式导出（多国列 + TTL + 备注）

### 多人协同
- [ ] 实时同步（polling / WebSocket）
- [ ] FOB 变更时提示"价格已更新"并刷新金额

---

## 并发模型（现状与规划）

### 已实现：订单数量格
```
用户 A 编辑 Romania / Jan / material X → 保存成功，rowVersion++
用户 B 编辑 Romania / Jan / material X（旧 rowVersion）→ 409 Conflict → 提示刷新
用户 A 编辑 Romania / Jan / material X → 正常
用户 B 编辑 Croatia / Jan / material X → 正常（不同行）
```

### 待实现：底表编辑
```
BOM material → row_version
FOB price → row_version
Lifecycle → row_version
Payment term → row_version
```

### 待实现：FOB 变更联动
```
用户 A 打开订单表（FOB=15000）
Admin 在后台改 FOB → 14500
用户 A 不刷新就保存数量 → 后端用当前 DB FOB 写入，返回 fobSnapshot + amountSnapshot
前端用返回值刷新格内金额，并提示 "价格已更新: 15000 → 14500"
```

---

## 测试验证方法

### 基础编辑
1. 打开 `/product/order-genius` → 选 Romania → 选年月
2. 点物料号某月数量格 → 输入数字 → Enter/Blur
3. 验证：格内数字变化、TTL Qty/Amt 即时更新、刷新后保留

### 并发冲突
1. 两个浏览器窗口打开同一国家、同年、同物料、同月
2. A 改 `1` 保存 → 成功
3. B 不改，直接改 `2` 保存 → 预期 409 错误
4. B 刷新 → 再改 → 成功

### 不同国家不冲突
1. A 编辑 Romania / Jan / material X
2. B 编辑 Croatia / Jan / material X
3. 两者独立保存，不互相影响
