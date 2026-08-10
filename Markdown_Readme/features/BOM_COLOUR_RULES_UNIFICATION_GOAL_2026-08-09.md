# BOM Colour Rules 统一目标与实施契约

> Goal ID: `bom-colour-rules-unified`
>
> 状态：`PLANNED_READY_FOR_CLEAN_IMPLEMENTATION_PR`
>
> 当前契约基线：`main@2dea140f328c1d7077ca0792979b47bcca4dca8e`
>
> 规划 PR：`#218`（Goal-only；不承载业务实现）
>
> 独立范围：只处理颜色名称、swatch、颜色规则报告、Add/Edit Colour 和选品页取色一致性。

当前依赖状态：

- BOM Admin 行编辑 PR `#215` 已合并到 `main@40ae32112927b3e138a88e42cd43ccc611f4ba0f`。
- Candidate 可写 FIFO 沙箱代码 PR `#219` 已合并到 `main@2dea140f328c1d7077ca0792979b47bcca4dca8e`。
- `#219` 只完成代码合并；Candidate PostgreSQL role/ACL、8001 drop-in 和首次 `prepare-candidate` 仍需分别授权，尚未在服务器执行。
- 颜色业务实现必须从包含 `#215+#219+#218` 的届时最新远端 `main` 新建 worktree、branch 和独立 Draft PR。

## 1. 现有 Goal 达成审计

结论：**Order Genius 的基础能力已经上线，但本文件定义的颜色统一目标尚未达成。**

现有文档之间也存在状态偏差：

- `order_genius.md` 把 Order Genius 总体标为 `implemented`，指的是基础模块已经存在。
- `ORDER_GENIUS_IMPLEMENTATION_ROADMAP.md` 仍把 BOM Admin、定制色等列为未实现，已落后于当前代码。
- 当前代码已经具有 BOM Admin、颜色加价、单 SKU swatch 编辑和临时推导的 Colour Swatch Rules。
- 这些能力仍未形成“同一品牌颜色代码在 BOM 与选品页一致显示”的完整闭环。

| 目标 | 当前状态 | 是否达成 |
|---|---|---|
| BOM Admin 显示数据库 `colourHex` | 已实现，支持 `#HEX1\|#HEX2` | 是 |
| 颜色加价按品牌与 tier 重算 | 已实现，人工 FOB 会跳过 | 是 |
| 同一品牌+代码复用标准名称与 swatch | 规则被名称再次拆组 | 否 |
| Refresh 准确展示缺失/冲突/可填充 | UI 只显示冲突并把无冲突误报为 clean | 否 |
| Add Colour 输入已知代码自动带出规则 | 未实现 | 否 |
| Edit Colour 同时保存代码、名称、swatch | 前端提交，后端只保存代码 | 否 |
| 选品 Matrix 使用数据库 swatch | API 不返回 `colourHex`，前端走硬编码名称表 | 否 |
| BOM 与选品共享同一 swatch 渲染逻辑 | 两套路径 | 否 |
| tier 变化给出逐国价格影响报告 | 后端有统计，交互报告不完整 | 部分达成 |

因此本 Goal 不是重复建设，而是收口现有半成品。

## 2. 已确认的当前事实

以下为本次只读核验记录，作为实施前基线，而不是写死在代码中的常量：

- 当前规则由 274 条 active SKU 临时推导，不是独立数据库规则表。
- 旧分组口径得到 51 个分组。
- 其中 16 个有标准 swatch、35 个没有 swatch、0 个 swatch 冲突。
- 当前 UI 只筛选 `status === "conflict"`，因此显示 `no conflicts / Colour rules clean`。
- “无冲突”不等于“数据完整”，35 个缺失分组当前被隐藏。
- BOM Admin 读取 `material_sku_master.colour_hex`。
- 选品 Matrix 当前未返回 `colourHex`，`OrderGeniusGrid.tsx` 使用英文名称硬编码并在未知时回退 `#94a3b8`。
- 当前 BOM 与选品查询以 `is_active` 为主要可见性条件；OMODA5 FL 相关行虽为 `is_published=false` 仍会显示。

### OMODA5 BEV FL 示例

| 代码 | 当前名称 | 当前数据库 swatch | 目标处理 |
|---|---|---|---|
| W3 | `water blue`，另有占位名称 `W3` | `#B6D3FB` | 将占位名称补为 `water blue`，复用 swatch |
| X4 | OMODA5 FL 内为 `X4` | `#1A1A1A\|#C8C0B8` | 从同品牌其他车型补规范名称和一致 swatch |
| ZE | OMODA5 FL 内为 `ZE` | `#1A1A1A\|#F0ECE0` | 从同品牌其他车型补规范名称和一致 swatch |
| ZU | OMODA5 FL 内为 `ZU` | `#1A1A1A\|#1ABC9C` | 从同品牌其他车型补规范名称和一致 swatch |
| ZF | OMODA5 FL 内为 `ZF` | `#19191A\|#8B0000` | 名称可补；黑色存在 `#1A1A1A`/`#19191A` 差异，必须人工选择 |

这些示例用于验收，不允许在代码里为 W3/X4/ZE/ZF/ZU 建专项分支。

## 3. 价格规则口径

颜色名称/swatch 规则和颜色加价 tier 是两个独立维度。

当前默认加价按品牌行、tier 列交叉：

| 品牌 | Dual | Special |
|---|---:|---:|
| OMODA | +200 EUR | +200 EUR |
| JAECOO | +300 EUR | +300 EUR |

补充规则：

- 不是“所有 Dual 固定 +300、所有 Special 固定 +200”。
- Special 可以被车型+颜色代码规则覆盖。
- 当前已知 Special 覆盖是 `OMODA9 SHS / UE / +300`，不涉及本 Goal 的 W3/X4/ZE/ZF/ZU。
- 手工 FOB 来源继续跳过自动重算，不能被 tier 拖拽静默覆盖。
- 名称/swatch 自动补齐不得修改 `single/dual/special`。
- tier 拖拽仍是独立的价格操作，必须显示实际重算报告。

## 4. 根因与现有责任边界

### 4.1 规则键过窄

当前规则键是：

```text
品牌 + 颜色代码 + 规范化颜色名称
```

因此下面两行被拆成两个规则：

```text
OMODA + W3 + W3
OMODA + W3 + water blue
```

目标规则主键应为：

```text
规范化品牌 + 大写颜色代码
```

名称和 swatch 都是该键下需要分析、补齐或判冲突的属性，不再是主键的一部分。

### 4.2 现有报告只展示冲突

后端已经能推导 `standard/missing/conflict`，但前端只显示 conflict。目标是继续复用这条推导链，扩充分类和展示，不建立新规则平台。

### 4.3 创建与编辑未共用规则解析

- Add Colour 留空名称时，前端把代码当名称写入。
- 创建接口只有在“品牌+代码+名称完全匹配”时才复用 swatch。
- Edit Colour 前端提交 `colourName/colourHex`，后端接口忽略这两个字段。

目标是让创建、编辑、批量填充调用同一个品牌+代码规则解析函数。

### 4.4 BOM 与选品渲染分叉

- BOM 直接读取 `colourHex`。
- Matrix payload 没有 `colourHex`。
- 选品页使用组件内部的硬编码英文名称映射。

目标是 Matrix 返回数据库字段，两个页面复用一个小型 swatch formatter/renderer；硬编码名称表只能作为旧数据的最后降级，不再是主路径。

## 5. 实施范围

### 5.1 后端：复用现有 repository 和 route

1. 将现有规则推导改为按 `brand + colourCode` 聚合。
2. 将空名称或 `normalized(name) == normalized(code)` 识别为占位名称。
3. 对每个规则返回明确分类和计数：
   - `fillable`：存在唯一规范名称与唯一 swatch，且有待补 SKU；
   - `missing`：没有足够来源确定名称或 swatch；
   - `name_conflict`：同一品牌+代码存在多个非占位名称；
   - `swatch_conflict`：存在多个有效 swatch；
   - `complete`：所有目标行已具备一致名称与 swatch。
4. Preview 返回将更新的 material code、旧/新名称、旧/新 swatch；只包含确定项。
5. Apply 只更新 preview 中确定项，冲突项必须拒绝自动写入。
6. 创建 SKU 时，若品牌+代码规则唯一，则后端补齐缺失名称和 swatch；不能只依赖前端。
7. 编辑颜色代码时，保存提交的名称和 swatch，并保留现有物料号重生成逻辑。
8. Matrix 的 active 与 historical 行都返回 `colourCode`、`colourTier`、`colourHex`。
9. 所有写操作保持单事务，返回实际更新/跳过/冲突数量和 material codes。

### 5.2 前端：交互与布局反馈

本 Goal 不重做 BOM Admin 布局，只扩展现有工具卡和两个现有弹窗：

- Refresh 期间按钮显示 loading 并防止重复点击。
- 工具卡显示五类摘要，不再用 `no conflicts` 代替完整性。
- 点击某一类打开可滚动明细；大列表不塞回小卡片。
- `预览确定项` 先展示逐 SKU 旧值/新值。
- 确认按钮文案为 `填充 N 个确定项`，提交时锁定。
- 完成后展示更新、未变化、冲突、缺规则数量，并重新加载 BOM。
- API 失败在面板/弹窗内展示结构化原因，不使用原生 alert。
- Add Colour 输入已知 code 后自动带出名称和单色/双色 swatch，并标注来源为 `Brand + Code rule`。
- 未知 code 保持人工输入；首次保存后由现有 SKU 数据成为后续规则来源。
- 用户手工修改自动带出的值时，页面明确标记“将创建规则差异”，不能悄悄制造冲突。

### 5.3 共享 swatch 渲染

- 抽出一个小型、纯函数式 swatch 工具，解析 `#RRGGBB` 和 `#RRGGBB|#RRGGBB`。
- BOM chip 和选品 Colour cell 使用同一 background 计算。
- 显示名称来自 API；数据库有 `colourHex` 时不得再按英文名称猜色。
- 数据库确实无 swatch 时才显示中性灰，并给出“缺少 swatch”提示。

### 5.4 tier 拖拽 Review

拖拽 Single/Dual/Special 后沿用现有重算函数，并把返回报告展示为：

```text
X4：Single → Dual
应用规则：OMODA / Dual / +200 EUR
扫描国家：9
已更新：7
跳过人工 FOB：1
缺少 Single 基准价：1
```

明细至少包含国家、旧 `finalFobEur`、新 `finalFobEur`、实际 `colourSurchargeEur` 和跳过原因。

## 6. 明确非范围

- 不新建颜色规则数据库表或迁移。
- 不建立第二套 Colour Library/管理平台。
- 不为 W3/X4/ZE/ZF/ZU 写专项映射。
- 不自动改变 `single/dual/special` tier。
- 不覆盖人工 FOB。
- 不改变历史订单快照。
- 不处理 `is_published=false` 仍显示的发布语义；该问题需要独立业务确认。
- 不包含 Candidate 数据库沙箱、发布、Active 或 intl 改动。
- 不把 Candidate 测试数据复制回 Active。

## 7. 验收矩阵

| 场景 | 预期结果 |
|---|---|
| W3 存在 `water blue/#B6D3FB` 与占位 `W3` | Preview 将占位名称和缺失 swatch列为确定填充 |
| X4/ZE/ZU 有唯一同品牌标准 | 自动补名称与对应双色 swatch |
| ZF 有两个黑色色值 | 进入 swatch conflict，不自动覆盖 |
| 同代码存在两个真实名称 | 进入 name conflict，不自动覆盖 |
| 代码完全未知 | Add Colour 允许人工名称/swatch，保存后可被 Refresh 收集 |
| Add Colour 输入已知代码 | UI 自动带出，后端二次解析，刷新后仍一致 |
| 编辑同时改代码、名称、swatch | 三项都落库，物料号按现有规则更新 |
| Matrix 展示 X4/ZE/ZF/ZU | 使用数据库双色 swatch，不再统一灰色 |
| BOM 与 Matrix 同一 material code | 名称和 swatch 一致 |
| OMODA Single → Dual | 默认实际加价 +200，报告逐国变化 |
| JAECOO Single → Dual/Special | 默认实际加价 +300，报告逐国变化 |
| 人工 FOB 行 | 标为 skippedManual，价格不变 |
| 无基准价行 | 标为 skippedNoBase，价格不变 |
| 自动填充名称/swatch | tier 与 FOB 均不变化 |

## 8. 测试契约

后端至少覆盖：

- 品牌规范化和大小写不同仍合并到同一 brand+code；
- name==code 被识别为占位；
- 唯一名称/唯一 swatch 可填；
- 名称冲突与 swatch 冲突分别阻断；
- Preview 与 Apply material code 集合一致；
- Create/Edit 共用解析并真实保存 name+hex；
- Matrix active/historical 都携带 colour fields；
- tier 重算继续保护 manual/no-base。

前端至少覆盖：

- 五类摘要与明细；
- preview/confirm/loading/error/result 状态；
- Add Colour 已知与未知 code；
- 单色/双色 renderer；
- Matrix 优先使用 `colourHex`，缺失时才降级；
- tier Review 报告的规则、数量和逐国结果。

Candidate 验收必须在独立可写沙箱上完成：填充、创建、编辑、拖拽后检查页面与数据库结果；测试数据不进入 Active。

## 9. 对 PR #215/#219 的承接和门禁

历史只读比较曾确认：

- #215 与颜色实现会实质重叠 `order_genius.py`、`test_ordering_bom_admin.py` 和 `OrderGeniusPage.tsx`。
- 该 owner 冲突已通过先同步、验证并合并 #215 解决；#215 最终进入 `main@40ae32112927b3e138a88e42cd43ccc611f4ba0f`。
- #219 与 #215 的 changed-file overlap 为 0，并已在 post-#215 main 上完成组合测试后合并到 `main@2dea140f328c1d7077ca0792979b47bcca4dca8e`。
- #218 当前及最终都只承载本 Goal Markdown，不承载颜色 TypeScript/Python 实现。

| #215 文件 | 本 Goal 是否可能修改 | 重叠性质 |
|---|---|---|
| `backend/app/api/routes/order_genius.py` | 是 | **实质重叠**：#215 修改 `create_material_sku` 的规则输入，本 Goal 也要修改创建解析 |
| `backend/tests/unit/test_ordering_bom_admin.py` | 是 | 同一测试文件，需要保留 #215 新测试并扩展颜色用例 |
| `frontend/src/pages/OrderGeniusPage.tsx` | 是 | 同文件不同主要区域；仍属于共享 owner 冲突 |
| `frontend/src/tests/unit/orderGeniusBomAdmin.test.ts` | 可能 | 可复用或另建颜色测试，避免覆盖 |
| `frontend/src/utils/orderGeniusBomAdmin.ts` | 可能 | 优先复用，不复制近似 helper |
| `.github/RELEASE_COORDINATION.md` | 否 | 不在颜色功能范围 |
| `ORDER_GENIUS_IMPLEMENTATION_ROADMAP.md` | 否 | 本 Goal 使用独立文档，不争用该文件 |

当前硬门禁：

1. #215 的共享 owner 阻塞已经解除，不再是颜色实现的 `Depends-On`。
2. #218 保持 Goal-only；不得在已审阅的一文件规划 PR 中追加业务实现。
3. #218 合并后，颜色实现必须从届时最新 remote main 重新建立干净 worktree/branch/PR，不得复用本规划分支。
4. 实现必须继承 #215 已合并的创建接口、行级编辑 helper 和测试，不复制相似逻辑。
5. 不从旧 `bom-colour-rule-library`、`bom-admin-followup` 或其他历史 BOM branch 恢复整棵代码。
6. 实现 PR description 必须列出最终共享文件、owner、#215 继承点，以及 #219 Candidate 验收的前置服务器状态。
7. Candidate 服务器配置和首次 `prepare-candidate` 必须另行授权；代码合并本身不等于 Candidate 可写沙箱已上线。

## 10. 预计实现文件与规模

在 post-#215/#219 main 上重新核准后，当前预计：

- 后端：现有 repository、route、matrix service、对应单测；
- 前端：现有 page、API types/client、grid、一个共享 swatch helper 及单测；
- 文档：本 Goal 随进度更新。

预计 9–11 个代码/测试文件，净改动约 350–650 行，不含测试夹具。若实现开始引入新数据表、第二套规则 service 或专项迁移，则视为范围失控，必须停止。

> 当前方案正在从修复 bug 演变为专项系统，已停止扩大修改。建议退回最小根因修复。

上句是本 Goal 的强制停止条件：仅当出现上述范围失控时启用；当前方案尚未触发。

## 11. 进度清单

- [x] 只读核验现有颜色规则、Matrix 与价格重算路径。
- [x] 审计现有 Goal 是否覆盖并达成颜色统一。
- [x] 从 `main@619466e8` 建立独立规划 worktree/branch。
- [x] 只读比较 #215 与当前 main，确认共享 owner 冲突。
- [x] 写入本目标、范围、交互和验收契约。
- [x] #215 同步最新 main、通过并合并（`main@40ae3211`）。
- [x] #219 在 post-#215 main 上通过组合测试并合并（`main@2dea140f`）；服务器配置仍未执行。
- [x] #218 保持 Goal-only，并按 `main@2dea140f` 回写依赖与实施边界。
- [ ] 合并 #218 设计契约；不触发部署。
- [ ] 从包含 #215+#219+#218 的最新 main 重建颜色实现 worktree/branch/PR。
- [ ] 后端规则、Preview/Apply、Create/Edit 与 Matrix 实现。
- [ ] 前端报告、自动带出和共享 renderer 实现。
- [ ] focused tests、TypeScript no-emit、Vite build 与 backend tests 通过。
- [ ] 创建独立 Draft PR；不从 feature branch 生产部署。
- [ ] Candidate 可写沙箱人工验收。
- [ ] 用户明确批准后才进入 Active 发布流程。

## 12. Definition of Done

只有同时满足以下条件才可把本 Goal 标为完成：

1. Refresh 不再把 35 个缺失组误报为 clean。
2. 品牌+代码是唯一规则边界，占位名称能被确定性补齐。
3. W3/X4/ZE/ZU 自动填充通过，ZF 冲突被拦截并可人工选择。
4. Add/Edit Colour 与批量填充共用同一后端规则解析。
5. BOM 与选品显示相同名称和单/双色 swatch。
6. 名称/swatch 补齐不改变 tier 或 FOB。
7. tier 变化报告准确展示规则、逐国价格、manual/no-base 跳过。
8. 所有 focused tests、类型检查和构建通过。
9. 在 Candidate 可写沙箱完成真实页面验收，Active 数据无变化。
10. 独立 PR 合入 main；生产发布仍需用户另行明确授权。
