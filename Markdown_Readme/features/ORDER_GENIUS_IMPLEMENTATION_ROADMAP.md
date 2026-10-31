# Order Genius — 当前状态与待办

## 2026-10-31 选品对比与连续填量（#315 后续）

- 基线 `main@b7cf227c`（GitHub main已核验），分支 `codex/order-material-compare-entry`；原混合目录只读。
- [x] 复用现有配置对比表：日历新旧Compare、选品基准行加入、按加入顺序统一高亮；关闭不丢数量/位置。
- [x] 显式更替日期演进线；国家/Model隔离、并行型号和真实重叠不改写。
- [x] 外/内饰首图hover与聚焦预览，点击复用现有图库。
- [x] 数量局部保存、同格请求顺序、连续输入、列宽/滚动/展开稳定；Create PI保留保存/余量门禁。
- [x] 类型/自动回归/本地Chrome交互验证通过。
- 交付回执：[PR #316](https://github.com/tristan419/JATO_Analysis_System/pull/316) 集中记录最终CI与Candidate版本/部署身份；无成功部署回执前不视为已交付，Active不动。
- 本地验证：类型检查、669项前端测试、生产构建、路由回归通过。Chrome156使用本地隔离API夹具验证旧→新和跨Model对比顺序/关闭保留、外内饰hover首图/多角度图库、连续输入时焦点/滚动/列宽不变、同格快速改写版本串行、窄屏横向表格与Escape；未改生产数量，未冒称Candidate登录后的真实资料CRUD已验收。
- 后端继续复用FastAPI＋SQLAlchemy/PostgreSQL的原资料、数量及PI核验接口，不增加表或迁移，不改变报价、订单快照和关联规则。

## 2026-10-31 资料面板与选品页精简（#314 后续）

- 业务规则仍以 V2 Spec 顶部当前契约为准；基线 `main@5662a215`，分支 `codex/material-assets-layout`。
- [x] Material Assets 放进选品页工具区，与 BOM Admin 共用98%屏宽面板，移除 Mega Menu 入口，保留原资料/权限/确认流程。
- [x] 移除标题说明与重复筛选标签，新批次与标题齐平；Notice Board 折叠态压为38px单行（含窄屏未读数）。
- [x] 显式展示已保存的国家/Model更替链，不根据日期推断，不拼接跨国家的关系。
- [x] 本地类型检查、657项前端全量回归、生产构建、路由回归通过；补充键盘焦点测试3项通过。1920px/390px浏览器布局、嵌套确认、折叠、关闭返回及Order Filler入口验证通过。浏览器使用本地API fixtures，仅验证本轮前端，不冒充线上资料/上传验收。
- 发布回执：[PR #315](https://github.com/tristan419/JATO_Analysis_System/pull/315) 集中记录CI与Candidate实际版本/部署身份；无成功部署回执前不视为已交付。Active/intl 不动。
- 后端、数据库和迁移无变动；不重复实现资料、日期或关系保存能力。

## 2026-10-31 Notice Board / Material Assets

- 业务规则：`ORDER_GENIUS_V2_SPEC.md` 顶部当前契约；旧阶段说明不能覆盖本节。
- 分支：`codex/material-assets-notice-board`，基线 `main@ea8d7cf9`；使用独立 worktree，不改混合观察目录。
- [x] UI Demo已获确认：删除“Edit in BOM Admin”跳转，其余按基准物料/HEX/共用图库与角色权限实现。
- [x] 现有代码审计：复用原时间轴与通知接口；配置解析与版本元数据存在，但缺不可变值快照；图片及绑定未有持久化。
- [x] 替换现有 Notice Board 为轻量日历、四月默认窗口/双柄/三年范围、Model分组和个人通知。
- [x] 独立 Material Assets、基准配置版本、同Model图片复用、选品HEX预览、跨Model来源对比及下载。
- [x] 必要迁移、前后端权限/引用与并发校验、自动测试、浏览器真实文件验证（本地隔离库）。
- [x] PR #314、CI及Candidate部署：`5662a215`，release run `41154016628`；用户已提交实际页面反馈，进入上方布局收口。登录后完整业务验收仍待用户确认。
- Active / intl：本轮未授权推广，保持不动。
- 后续任务：有来源的AI差异解释仍需另行落实；不以确定性字段表冒充大模型结果。
- 验证证据：前端全量647项；后端相关362项通过/1项跳过；新资料与迁移链28项通过；TypeScript与生产构建通过。独立本地PostgreSQL已从空库执行完整Alembic到0062；浏览器走真实API完成XLSX解析/UUID保存、原图上传/下载/同Model复用、跨Model单元格来源对比、390px窄屏。均未使用生产业务数据，Candidate尚待下方发布步骤。
- 上传口径：本批复用现有“在售可控资源表”XLSX配置矩阵；须选择配置列。不支持的格式明确422，PDF/任意供应商表解析不计为完成。

> 当前版本: V1.5 | 目标版本: V2 | V2 spec: [ORDER_GENIUS_V2_SPEC.md](./ORDER_GENIUS_V2_SPEC.md)
> 更新: 2026-08-07

## 当前开发基线

- BOM Admin / Order Genius 的新开发必须从当时最新的远端 `main` 创建独立
  worktree、branch 和 PR，不再沿用固定的长期 BOM 分支。
- 2026-08-07 的过渡分支是
  `codex/bom-admin-edit-scope-current-main`，基于
  `main@37a9905cebd0ccaf5147314f6cedb639f95a25ca`，只重放旧 PR #173 中已确认
  仍缺失的行级编辑修复及其测试。
- `codex/bom-colour-rule-library`、`codex/bom-admin-followup` 和旧
  `JATO_Analysis_System_bom_admin` worktree 仅用于历史审计；禁止把整支历史
  branch 合回新开发线。
- 当前过渡 PR 合并后，下一个 BOM 功能仍应从届时最新 `main` 新建分支，不能
  把本节记录的过渡分支继续当作永久开发入口。

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

### CBU / Finance
- [ ] 真正 CBU 明细成本拆分表：国家可切换，最小粒度为包含 `**` 的 BOM template，成本项支持 Excel 粘贴/导入和网页维护
- [ ] NL price CBU 保持独立格式；其它国家走通用 CBU 成本明细/Excel 粘贴，不复用 NL 边际矩阵
- [ ] 完整 edit history / audit log：记录字段级前后值、操作人、操作时间、来源模式、source payload，可按国家/物料/BOM template 查询

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
