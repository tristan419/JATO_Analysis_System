# BOM Admin / Order Genius：Candidate 验收与问题梳理

> 2026-09-24 新需求另见 [国家日期选品、月内多批 PI 与 WVTA](ORDER_GENIUS_DATED_SELECTION_PI_BATCH_WVTA_2026-09-24.md)。时间价格建立在模板基准修复之上；PI 可见性故障优先独立处理，不能把本清单现有修复完成等同新需求完成。

> 创建：2026-09-15；最新需求与状态修订：2026-09-24，Asia/Shanghai。状态：C/B/A/D 已按 C → B → A → D 合并到 main；手动基准重算修复已通过 PR #233 合入，Candidate 已重新准备。当前 Candidate 只代表部署可验收，不代表 Special/manual、共享色卡、BOM/Matrix 双入口与 D 的真实失效场景已全部收口。
> 用户目标：稳定填写订单、稳定列布局、跨物料共享颜色名称/色卡、正确计算颜色加价。暂不优先建设多人编辑、row_version 扩展或新门禁。
> 初次调研只做验收和文档；之后 A 已有本地代码提交 `260f839a`，尚未部署验收。B/C（含 E）/D 的代码批次随后已各自在独立 worktree 完成，整个 BOM 修复仍未完成 Candidate 验收。
> **最新执行口径（2026-09-25）：先读第 13 节[最终确认方案与 Luna Max 任务](#bom-pricing-final-20260925)。#236 必须修订后重新审阅，不能直接合并。** 最新已记录 Candidate 为 #235 的 `abd38688d35eec9d771d8a9fc5fa599545b172eb`，仅部分样本通过。下方 9 月 24 日至 #235 的状态、实现和测试是历史记录，不是当前执行指令；其中“Matte 一律 Special”“付款条件参与基准”“明确最终价永久跳过”均被本方案替代。本次仅更新文档，未修改代码或任何环境数据。

### 2026-09-24 · PR #233 合入后重新准备 Candidate（当前待验构件）

- PR [#233](https://github.com/tristan419/JATO_Analysis_System/pull/233) `fix(bom): reprice manual base rows with colour surcharge` 已合入；合并提交：`cf10438fdeed1573a13fc1f2a533c18623d87476`。该提交只修复有可信 Single 基准的 `manual_edit/manual_country_adjust` 行参与当前 tier surcharge 重算；没有基准的明确最终价继续保护。
- `production-release` run [#35971888377](https://github.com/tristan419/JATO_Analysis_System/actions/runs/35971888377) 的 `prepare-candidate` 已成功。Candidate 地址仍为 `https://candidate.ojeur.cloud/product/order-genius`；操作 ID：`2026-09-24T080357833Z-prepare-candidate-8f651be3`。
- 当前构件身份：commit `cf10438fdeed1573a13fc1f2a533c18623d87476`；archive SHA-256 `ea4d7e9ab0a5d9e0fdfa74a4627e2e2dc711d379f7452cdac012e4bc26fff9b3`；manifest SHA-256 `566d1cfe6039a1ffcdc7e147dad06a0374e4a312b665d04aba3e5d1e2df5f57b`。部署报告通过 Candidate sandbox provision/database isolation/backend/monthly-disabled/preview checks，并记录 `active_unchanged`；本次没有 update-active、sync-intl 或正式数据写入。
- 验收重点：重复 OMODA9/UE `dual → special` 样本时，若 CH 的 Single 基准仍为 `25,200`，应从旧 `25,400` 改为 `25,500`（Special `+300`），而不是再显示 `manual_fob` 跳过。没有可信基准的手动最终价仍应说明并保护。

#### Candidate 色块没有颜色：当前证据与边界

- PR #233 没有修改色卡数据或渲染器，因此它不会修复也不会造成色块颜色缺失。BOM 行渲染器读取 SKU 的 `colourHex`；值缺失时使用中性占位 `#94A3B8`，Matrix 渲染器同样对颜色名称查不到映射时回退到 `#94A3B8`。对应源码为 `06_AppPlatform/frontend/src/pages/OrderGeniusPage.tsx` 的 `renderColourChip` 和 `06_AppPlatform/frontend/src/components/OrderGeniusGrid.tsx` 的颜色映射。
- 后端列表返回的是每个 SKU 的 `colourHex`（来自 `colour_hex`）。因此“色块灰、看起来没有颜色”首先表示该 SKU 没有可用的规范 hex，或共享规则汇总处于 `missing/conflict`，不是 CSS 失效。此前 Candidate 规则卡已经记录 `24 missing rules`，并观察到 JAECOO5 的部分列表/Matrix 显示 `missing swatch`；J5 空品牌规范化只能修正查找 key，不能凭空生成颜色值。
- 蓝边也不是“有颜色”的证明：当前 BOM chip 对有 `colourHex` 的自定义值使用 `2px solid #3b82f6`，它是编辑/自定义视觉标记，不是规则命中或保存状态。应把“缺 hex”“规则冲突”“自定义 hex”分开验收，不用随意填默认颜色掩盖数据缺口。
- 本次 Candidate 先用于验证 #233 的基准重算和回归；共享颜色库应继续以 Refresh/Preview 的 `missing/conflict/fill` 明细定位，后续只补真实缺失或冲突的共享规则，不批量把 `#94A3B8` 写回正式数据。

#### 颜色值回填口径：查已有映射，不把名称自动“算成”真实车漆色

颜色名称、颜色代码、色块值和价格 tier 必须分开保存。已有颜色库映射时取库中的标准名称与 HEX；未知颜色才进入用户确认流程，不能用一个通用 RGB 猜测真实车漆。

1. 优先按 **规范化品牌 + Colour Code** 查询共享颜色库。命中唯一记录时直接复用其标准名称和已保存的 `#RRGGBB`；车型、版本和物料号只引用这份映射，不各自复制一套颜色定义。
2. 色码未命中时，再按颜色名称、别名和历史写法找候选。只有唯一候选可自动回填；多个候选必须让用户选择，不能静默覆盖。
3. 仍未命中时，用户可以使用浏览器选色器或直接输入 HEX。保存前应明确这是用户确认的新共享值；保存后相同品牌＋色码的 BOM 与 Matrix 统一读取它。
4. 不能把 `Alpine green` 等名称自动换算成所谓真实车漆 HEX：同名车漆可能存在金属、珠光、哑光等差异。若提供近似色，只能标记为“建议色”，等待用户确认后才能成为共享标准。
5. 双色保存两份 HEX，由前端画对角双色块；Matte 可以只有一份 HEX。价格 tier 只认 BOM Admin 已保存分类，不从色名或色块数量推断；OMODA9 Matte black 独立模板属于 Single。

验收必须分别证明：已有映射会复用原 HEX；未知色码可选色并保存；同一映射在 BOM/Matrix 两个入口一致；名称修改不会生成新色值或覆盖另一色码；Dual/Special 的价格规则只读取 tier，不从色块数量推导价格。

### 2026-09-24 · 四批合并后的 Candidate 准备结果（业务验收待登录）

- 合并顺序已执行：C → B → A → D；四个 PR 均已通过 required checks 并合入远端 `main`。最终 main：`efe5ac0f5b11f547ca94942bd2a0ad0708822bb0`。
- 自动 `production-release` run：[#35960030314](https://github.com/tristan419/JATO_Analysis_System/actions/runs/35960030314)，`prepare-candidate` 成功。
- 实际构件身份：archive SHA-256 `2810a3510337ab7980ff1d19e9ac64f9276b23c9d4aaf619d363d747673c962b`；manifest SHA-256 `c7a49e45c7b4b6ce80f03b1948474af2f64b2b2c3c23bf10a0a9433257e14af9`；操作 ID `2026-09-24T053732954Z-prepare-candidate-91b3be18`。
- Candidate 地址：`https://candidate.ojeur.cloud/product/order-genius`。部署报告通过 `candidate_sandbox_provisioned`、`candidate_database_isolation_verified`、`candidate_backend_verified`、`candidate_monthly_disabled_verified`、`candidate_preview_verified`、`active_unchanged`；本次没有 update-active、sync-intl 或正式数据写入。
- 报告另记 `release_gc_deferred:pointer_target_outside_store`：旧 Active 指针仍在 legacy store，发布回收被延后；这是清理诊断，不是 Candidate 部署失败，也没有改变 Active 流量。
- 浏览器实际打开后页面显示 Candidate commit `efe5ac0f5b11`、artifact `2810a3510337`。使用此前约定的 `candidateAdmin` 测试账号登录返回 `Invalid credentials`，没有继续尝试未知密码，也没有写入 BOM 数据。必须先取得/恢复正确的 Candidate 测试凭据，再执行第 15 节统一验收；不能把部署成功写成业务验收通过。

### 2026-09-24 · Candidate 浏览器验收批次 1（管理员登录后）

- 用户提供的 Candidate 账号登录成功；凭据不写入文档。页面确认 commit `efe5ac0f5b11`、artifact `2810a3510337`、Candidate 沙箱尾号 `889b4d88`。本轮没有 update-active、sync-intl 或正式数据写入。
- 颜色规则 Preview/Apply 真实通过：初始 `38 rules / 8 SKU fields can be filled`，Preview 8 项；Apply 后页面显示 `Filled 8 SKUs; 0 unchanged, 9 conflicts, 24 missing rules`，再次 Preview 为 0 项。8 行颜色名称写入 Candidate 沙箱，价格/tier 未因 Apply 改变；冲突和缺规则仍未解决。
- Model 列在 50%、100%、150% 缩放截图中均在最左，`Reset order grid column widths` 可见；拖拽列宽、刷新/数量保存后的持久化、真实窄窗口尺寸尚未完成，不能标记 B 全通过。
- #215 交互证据：JAECOO5 HEV Exclusive-FWD 首行进入 `DONE`，Select-FWD 另一版本行仍是 `EDIT`。这证明不会整组同时编辑，但尚未取得“同 BOM Template 跨不同车型/版本”的专门样本。
- JAECOO5 HEV 的 BOM 色卡按钮现在显示规范化 `JAECOO`，BW 规则可打开并读到 `#94A3B8`；列表和 Matrix 仍有大量 `missing swatch`（规则卡显示 24 个），共享颜色库双入口同步仍未通过。
- OMODA9 SHS 在 Candidate 中将 `UE` 从 `dual` 移到 `special` 后，价格复核弹窗显示 `9 countries scanned · 8 updated · 1 manual FOB skipped · 0 missing Single base`；AT、RO、BG、CZ、HR、GR、HU、SK 均按 Special `+300 EUR` 重算，只有 CH 保持 `25,400 → 25,400 · surcharge 200 · manual_fob`。这证明车型＋色码的 Special `+300` 规则和优先级已经生效，未更新的是被 `manual_edit` 标记的瑞士行。
- 本次确认的业务语义是：BOM Admin 中手动 Copy 和 Manual Edit 都是在修改国家基准 FOB，不是锁定某个颜色的最终 FOB；最终价始终是 `base_fob_eur + 当前 tier surcharge`。真正的车型＋品牌＋色码特殊价优先于品牌色码特殊价和品牌 Special 默认，Matte 只走 Special、不叠加 Dual。
- 当前剩余根因是 `manual_edit` 同时表示“人工改过基准”和“不可重算的最终价”。重算函数在读取 `base_fob_eur` 前直接跳过该行，所以复制后再手动调整的 CH 不能跟随 Special 规则。下一批只需收窄保护：有可信基准的手动行允许重算；没有基准的明确最终价继续报告并保护，不批量回填历史正式数据。
- 管理员登录成功不等于 D 已验收：有效低权限 403、过期 token、断网重登及数量/BOM 草稿跨标签保留本批未执行。

## 13. 实施记录

<a id="bom-pricing-final-20260925"></a>

### 2026-09-25 最终确认方案与 Luna Max 任务（最高优先级；文档已确认，代码待修订）

本节替代旧 C5–C8、旧 #236 方案中冲突的定价要求；历史测试和部署记录不代表此方案已实现。保留 A/B/C/D 已合并成果，不重做整页、认证或共享色库，不增加新门禁/定价框架。#236 尚未合并，必须先修订并重新审阅。

#### 1. 唯一业务口径

基准按 **BOM 模板＋国家** 维护；物料＋国家的最终 FOB = 当前模板国家基准 + 当前已保存档位的适用加价。付款条件只展示和记录，不筛选基准、不决定价格、不影响重算范围。

| 场景 | BOM Admin 档位 | 定价 |
|---|---|---|
| 普通 Single | Single | 当前模板国家基准＋0 |
| OMODA 双色 | Dual | 基准＋200（无适用定制时） |
| JAECOO 双色 | Dual | 基准＋300（无适用定制时） |
| OMODA7 Matte gray | Special | 基准＋200 |
| OMODA9 UE / Matte gray | Special | 基准＋300，车型色码特例替代默认＋200 |
| OMODA9 CP / Matte black 独立模板 | Single | 自身模板国家基准＋0 |

Matte black 的＋500属于独立模板的基础定价，不另加颜色费，也不建立与其他模板自动联动。名称、色码、HEX 仅识别/展示；不根据 matte 字样、双色色块或历史价差覆盖已保存 tier。旧 tier 为空时可读取已有合法 exterior_color_type；仍缺失则报告待确认，不猜档位。

Single 无条件加价 0，不触发颜色特例。Dual/Special 在**同一适用档位**内按“品牌＋车型＋色码 → 品牌＋色码 → 品牌当前档位默认”取一个金额，不累加。显式 0 是免加价；没有规则不是 0，须报告无法定价。

#### 2. 保留定制能力，收拢现有管理入口

- 将独立 Special Colour Overrides 面板收进 Colour Surcharges 下方，默认折叠“定制颜色加价（N 条）”；保留新增、编辑及停用/删除后回退能力，复用已有存储、接口和预览流程。
- 字段：品牌、可选车型、色码、适用档位（Dual 或 Special）、加价 EUR；名称用于识别，不能充当匹配键或档位推断依据。
- 现有规则是 Special-only。扩展现有模型/请求/唯一约束以支持 tier，旧记录明确迁移为 Special；不得把旧 Special 规则无差别用于 Dual。以最小必要迁移实现，不新建平行规则系统。
- 色卡展示实际命中来源，如“Special · +300 EUR · OMODA9 定制”；Single 显示“Single · +0”。这仍不是某国实际 FOB 更新成功的证明；保存反馈须展示逐国结果。
- 拖动 Special → Dual 后只匹配 Dual 规则；移回 Single 加价归零。名称/色块修改不改变 tier、基准或加价配置。

#### 3. 基准、历史数据与付款条件去耦

- 优先复用现有模板基准保存/读取函数和 base_fob_eur，不假设已有独立模板价格表。人工编辑、复制、调整都修改基准，再派生颜色最终价。
- 同模板同国家 Single 为 1,000 和 1,200 时，必须报告“基准歧义”；旧 Dual/manual 行存有 base=1,000 不能使歧义变为可自动重算。禁止 MIN、任意首行、跨模板/车型回退来掩盖冲突。
- 没有 Single SKU 时（如最后一个 Single 被拖走），只有可验证、同模板国家且一致的已保存模板基准才可继续使用；不能用某一条旧派生价或旧 manual base 独自证明可信。来源或数值冲突仍阻止写入并列出证据。
- manual/copy/import 等 source 仅保留来源和审计意义，不作为永久不重算的锁。有可信基准、明确 tier、适用规则就按统一模型重算；原始导入最终价保留作证据，不能当作 Single 基准再叠加。
- 缺基准、真实冲突、缺 tier、缺规则必须逐项说明且不自动改写。不能继续按 explicit_final 来源整类豁免。规则变更不篡改用户已确认基准。
- 当前数据库物料＋国家可能按付款条件有多行：先只读列出重复及差异。相同值可按明确迁移方案归一，不同值要用户确认统一基准，不任意选择。审计、Apply、查询和导出都必须使用一致国家价格；不允许按付款条件审计多项却以物料＋国家重复写入。保留付款条件参考信息与历史，不直接删除记录或盲改唯一键。
- Country Copy 复制基准到目标国后按目标模板规则派生，不复制已加价的最终值再加一次。新增颜色、Copy Material、批量调整、导入遵循同一原则。

#### 4. #236 根因修订与执行批次

执行前核对最新 origin/main 和 #236 diff；已记录 main 为 abd38688d35eec9d771d8a9fc5fa599545b172eb，不能把它当成永远最新。代码工作区为 /Users/litristan/Downloads/JATO_Analysis_System_bom_reprice_audit，分支 codex/bom-colour-reprice-audit；先确认实际 HEAD、dirty 和归属，禁止在原始混合区开发。

1. **统一计算与审计/写入**：复用 repository 中现有基准解析、get_colour_surcharge_amount_for_sku、reprice_sku_colour_surcharge_fobs 和 update_bom_template_base_fob。让 audit 和 Apply 共用同一份基准/档位/规则/金额/跳过原因决策，修掉歧义后回退旧 manual base 的放行、审计和 writer 来源保护不一致，以及 skippedAmbiguous 从不增加。移除旧 MIN/跨模板选基准路径；避免新增另一套重算器。
2. **规则与 UI**：最小扩展现有 SpecialColourSurchargeRule 的适用档位及迁移/接口/测试，实现折叠管理入口与真实命中反馈。已有 Special 记录继续只适用于 Special，Single 提前返回 0。前后端不得各写一份规则优先级。
3. **入口与重复数据衔接**：检查 repository、service、routes、导入解析以及 BOM/Matrix/export 的国家价格读写；完成付款条件去耦和重复记录的只读报告，明确安全归一策略。需要业务选择的冲突先报告，不让迁移猜价格。不把前述 source-mode 永久冻结逻辑保留在旁路。
4. **验证与交付**：每批更新本节下方的提交、文件、实际命令、结果和未验项。审阅修订后的 #236，再获授权走 PR → main → 现有 Candidate 流程；Candidate 先只读 audit，用户看清范围后另行授权 Apply。此文档不授权合并、部署、批量写入或 Active/www/intl 发布。

预计涉及现有 repository/service/routes、规则模型与必要 migration/request schema、OrderGeniusPage/现有规则组件、API/types 和对应测试。**执行者在写代码前必须基于最新 diff 报告具体根因、负责函数、预计文件和行数，以及为何需要扩展；不能以本方案为由无边界重构。**优先修改已有流程，不复制能力。依赖未变化复用既有环境，不新装依赖。

只读 audit 至少区分可重算、已正确、缺基准、基准歧义、缺档位、缺规则，并返回基准证据、规则来源、旧/新价及 fingerprint。Apply 在同一受保护事务中重新验证读集和规则，再写计算结果，不能在校验后留出旧预览覆盖新值的窗口。写后重新审计应 0 待更新；不能要求过期 fingerprint 再提交也成功。

#### 5. 必须通过的验证，不以单车型特例替代

- OMODA7 Special＋200、OMODA9 UE Special＋300；OMODA9 CP Single 保持自身基准，名称含 matte 也不加费。同品牌其他车型不误命中特例。
- 非 J5 的 JAECOO7 和 J5 ICE/HEV、多个模板国家均测试实际 Single/Dual/Special 差额；BOM、选品表、Matrix、导出一致，不能只验 tooltip。
- 同档位优先级、显式 0、缺规则、停用回退、Special/Dual 来回拖动、Single 跳过特例；重复重算不叠加。
- manual/copy/import 都从可信基准得到相同结果；Single 1,000/1,200＋旧 manual base 1,000 必须歧义且无写入，统计必须增加。无基准不写。
- 修改付款条件不影响任何 FOB；多付款条件相同/冲突记录覆盖查询与 Apply 测试，物料＋国家不重复应用。Country Copy 验目标国基准。
- 真数据库事务/路由级 Apply 测试验证预览与写入同结果、旧 fingerprint 拒绝、跳过行不写、失败回滚及历史记录；不能仅 mock 返回值。
- UI 验折叠规则、预览/取消/保存、搜索和输入焦点保持；BOM/Matrix 双入口与原 A/B/D 不回退。
- Excel 已确认两处规则差异：OMODA7 Hungary I25 22,050 应按 21,950＋200＝22,150；OMODA9 Sweden L12 26,260 应按 26,260＋300＝26,560。这是规则验收样本，不是授权导入文件覆盖网站。JAECOO7 工作簿 19,350 样本属 CZ，不得据此替换 CH 基准。
- 工作簿存在无缓存公式及单格多模板码：检查现有导入器，不把 data_only 空值当成真实缺价，不按名称猜 CP tier；解析问题另给最小修复，禁止任意 eval/盲扩展旧新模板。

已记录的 5 个 country-column / sync_missing_template_fobs 基线失败必须在同一最新基线核对；记录实际结果，不把历史 8 passed 或 CI 绿等同本方案验收通过。本次文档修改未跑代码测试、未验证新 Candidate。

#### 历史实施记录（以下按发生时记录，冲突设计以本节为准）

### 2026-09-25 · 共享颜色库查找与未知色保护（本地已实现，待 PR/Candidate）

- 新 worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_shared_colour_mapping`；分支：`codex/bom-shared-colour-mapping`；基线：最终 main `cf10438fdeed1573a13fc1f2a533c18623d87476`。本批只处理共享颜色映射和色块缺失的真实根因，不改价格规则、认证、Active/www/intl 或正式数据。
- 后端复用现有 `lookup_colour_rule`、`resolve_colour_attributes`、`build_colour_hex_rules_from_skus` 和 SKU 的 `colourHex`：先按规范化品牌＋Colour Code 查唯一规则；色码无可复用规则时，才按 Unicode/标点/空格归一化后的颜色名查现有候选。唯一无冲突候选可回填标准名称与 HEX；多个候选返回选择列表；未知值不猜色、不写默认灰色。
- 前端 Add/Edit Colour 两个入口均传入 `colourName`，保留约 1.2 秒延迟、requestId 和已手改字段保护。多候选时显示现有规则选择按钮；用户选择后才把名称/HEX填入当前编辑目标。BOM/Matrix 仍读取同一 SKU/共享映射；双色保存两份 HEX，Matte 只保存一份，色块数量不推导价格 tier。
- 冲突规则不会被名称候选静默覆盖。按品牌＋代码已存在但处于 conflict 的规则仍要求用户明确处理；只有 `brand_code_rule` 或唯一 `name_candidate` 才允许自动回填。没有候选时用户可选色或输入 HEX 后确认保存。

本批已实跑：

- 后端 `tests/unit/test_ordering_bom_admin.py -k 'colour_rule or colour_attributes or colour_surcharge and not sync_template'`：`19 passed, 36 deselected`；`compileall` 与 `git diff --check` 通过。完整文件中仍有 5 个既有 country-column / `sync_missing_template_fobs` 基线失败，本批不处理。
- 前端已完成 `npm run test:unit`（`75 files / 411 tests passed`）、`npm run check:types`、`npm run build`、`npm run check:router-regression`；构建仅保留既有 chunk-size warning。临时依赖 symlink 已删除。

进入 PR 后 Candidate 验收必须逐项记录：

1. 已有品牌＋色码映射（如 JAECOO＋ZE）在 Add/Edit、BOM 和 Matrix 复用同一名称与 HEX。
2. 色码未命中但名称唯一时只回填唯一候选；名称多候选时出现选择项且不自动写入；完全未知时不产生灰色持久化值。
3. 用户手动选色/输入 HEX 并保存后，两个入口显示相同色值；双色两个 HEX、Matte 一个 HEX，均不改变 tier 或 FOB。
4. 改颜色名称、改色码、异步回填后二次输入不会抢焦点或覆盖手改值；1.2 秒等待后旧响应不能覆盖新编辑目标。
5. 与既有 C7/PR #233 验收合并检查：普通 Dual、OMODA7/9 Matte Special、JAECOO/J5、manual 基准和 no-base 保护不回退；本批不能被解释为价格规则或登录失效已全部完成。

本批尚未创建/合并 PR，也尚未准备新 Candidate；取得 PR 与构件身份后再补写本节，不把本地测试当作 Candidate 验收或 Active 发布。

### 2026-09-25 · PR #234 合入并准备共享颜色映射 Candidate（待浏览器验收）

- PR [#234](https://github.com/tristan419/JATO_Analysis_System/pull/234) 已通过 required CI（backend、frontend、smoke、release contract、Cloudflare Pages）并合入 main；合并提交：`fd50e5a1b3222aec96d1b5dcb34d8d884018ffae`。
- `production-release` run [#36099775361](https://github.com/tristan419/JATO_Analysis_System/actions/runs/36099775361) 的 `prepare-candidate` 已成功；操作 ID：`2026-09-25T055411284Z-prepare-candidate-f3855f8b`。Candidate：`https://candidate.ojeur.cloud/product/order-genius`。
- 构件身份：archive SHA-256 `0067a7c84c7f9ddbbf41ad567bb35784fde27861d28cbe8d3d4f2d5585fbc171`；manifest SHA-256 `9eaf85a81401a4912bec5bd9c6c94670bf3e703af5765dc0c9cc7111de39fef8`；公网 `build-meta.json` 显示 commit/appCommit/deployCommit 均为 `fd50e5a1b3222aec96d1b5dcb34d8d884018ffae`，healthz 返回 200。
- 部署报告通过 `candidate_sandbox_provisioned`、`candidate_database_isolation_verified`、`candidate_backend_verified`、`candidate_monthly_disabled_verified`、`candidate_preview_verified`、`active_unchanged`；没有 update-active、sync-intl 或正式数据写入。`release_gc_deferred:pointer_target_outside_store` 仍是既有 legacy Active 指针清理诊断，不是 Candidate 失败。
- 现在只待真实浏览器验收，不把 Candidate 构件身份等同业务通过。验收按本节清单记录前后值、截图/响应和未通过项；测试写入仅限 Candidate 沙箱。

### 2026-09-25 · Candidate 共享颜色与 Dual 价格复核：三项暂不通过

本批根据 Candidate commit `fd50e5a1b322` 的实际 API 只读核对，否决“Dual +300、色块共享、TE 创建后复用”三项的通过标记；没有手填颜色、修改 FOB 或写入 Candidate。

#### 1. JAECOO Dual +300 目前只在规则层成立

- `/v1/order-genius/colour-surcharges` 当前确认 JAECOO `dual=300`、`special=300`，规则金额本身正确。
- CH 的模板 `T7160RG**MH0001` 当前返回：Single `BW/CL/KU/UD` 都是 `19,350`；Dual `ZK`、`ZN` 也都是 `19,350`。Dual 行还带 `baseFobEur=20,150`、`colourSurchargeEur=300`、`finalFobEur=19,350`；CZ 对应行是 `manual_edit`，CH 是 `copied_from_country`。
- 因此实际结果不是 `19,350 + 300 = 19,650`。这证明当前行的 surcharge 元数据与最终 FOB 不一致，不能只看规则 tooltip 或 `M` 标记归因于手动保护。需要继续区分：现有 tier 已经是 Dual 时是否没有触发重算、Copy Country 是否复制了旧最终价、以及可信 Single 基准读取为何落成 `20,150`。

#### 2. 共享颜色没有覆盖显示路径

- 当前 `/v1/order-genius/colour-hex-rules` 汇总为 `39 rules / 5 fillable / 25 missing / 7 name conflicts / 2 swatch conflicts`。
- 代表性规则：JAECOO+`ZE` 为 `name_conflict` 且 12 个 SKU 中 9 个缺 swatch；JAECOO+`UE` 为 `missing` 且 3 个 SKU 全缺；OMODA+`TE` 为 `missing` 且当前唯一 SKU 缺 swatch。
- `/v1/order-genius/bom-admin?search=T7160RG&country=CH` 与 `/v1/order-genius/matrix?...&material_code_search=T7160RG` 都返回大部分 `colourHex=null`；只有现有 `ZM` 的占位双色 `#1A1A1A|#94A3B8` 和 `SP` 的 `#3A7D44` 有值。代码核对也确认 `renderColourChip`/Matrix 读取 SKU 自身 `colourHex`，PR #234 的 lookup 只服务 Add/Edit 输入，不是显示时的共享回退。
- 因而“一个入口有色、另一个入口灰”仍然可能发生。当前灰色只能解释为缺配置占位，不能当成真实车漆；后续要么在唯一无冲突共享映射时由服务层统一解析，要么显示明确 `missing swatch`，不能静默套灰色。

#### 3. TE 当前 Candidate 没有可复用标准，旧沙箱无法证明已保存

- 当前 `/v1/order-genius/bom-admin?search=TE` 只有 `OMODA / T7000NHTEMY0001 / TE / blacked`，`colourHex=null`；`/colour-hex-rules/lookup?brand=OMODA&colourCode=TE&colourName=blacked` 返回 `source=none`、空 `nameCandidates`。
- 本次 prepare-candidate 报告含 `previous_candidate_sandbox_removed:1`，且 Candidate 页面显示新的数据库快照开始时间。因此如果 TE 是在上一个 Candidate 沙箱保存，旧记录已随沙箱替换消失；如果从未成功提交，当前证据也会相同。不能凭当前结果断言上次保存失败，也不能要求用户反复手填来掩盖这个区别。
- 完整验收必须在**当前**沙箱重新做一次：创建 `OMODA+TE`（名称和单/双色 HEX）→ 刷新页面 → 在同品牌另一个模板只输入 `TE` → 唯一映射自动回填 → 保存 → BOM 与 Matrix 读取同一名称/色值 → 检查该颜色的实际 FOB/tier。过程中记录 material code、当前数据库快照时间、前后 API 响应。

#### 当前结论与下一步边界

- 三项均为“Candidate 已部署、业务未通过”，不更新 Active/www/intl，也不建议继续手工逐色补值或手工给每行加 `300`。
- Dual 价格需要先修正现有可信 Single 基准与重算触发/复制语义；颜色共享需要解决“只从 active SKU 汇总、没有独立可持久化标准”的根因。后者不是继续扩展一次性 lookup 就能完成，是否增加最小持久化颜色标准记录及迁移，应在实施前单独确认。

### 2026-09-24 · 用户澄清：模板基准价与派生颜色价格（历史批次；冲突要求已由 9 月 25 日最终方案替代）

本节覆盖下文将 BOM 页面输入理解为“各颜色手动最终 FOB”的旧口径。用户明确：在含 `**` 的 BOM 模板行按国家维护基准价；Single = 基准 + 0，Dual = 基准 + 品牌 Dual 规则，Special = 基准 + 命中特殊价规则。颜色区域继续支持拖动分类、增删改查；颜色加价由现有统一工具维护，Matte 为 Special。

- 已核查的错位：`BomAdminPanel` 模板行编辑传入 `allCodes`，`handleFobSave` 逐个调用 `updateSkuFob`，把同一输入写成各颜色最终价；repository 标为 `manual_edit` 后，规则重算跳过这些派生行。这不代表用户主动逐颜色锁定了最终价。
- 修改模板基准价应在后端同一事务内保存基准并计算该模板各颜色最终价；前端提交模板作用域、国家和基准金额，复用现有规则解析。禁止继续把同一最终价逐颜色覆盖。保护的是人工维护的基准，不能因基准由人工输入而冻结派生颜色。
- 拖动 Single/Dual/Special 或修改 surcharge 仅改变适用加价，基准保持不变；最后一个 Single 移走时也不能丢失已维护基准。新增颜色、Copy Material、模板批量调整、国家复制/调整均须核对并遵守同一基准语义。Copy Material 页面手填金额按模板基准处理，不再当各颜色最终价。
- 先核查现有基准存储与读取，优先复用 `base_fob_eur` 和已有模板作用域。`**` 当前可以只是分组标识，不能假设数据库已有独立同名基准记录；不得简单禁止非 `**` SKU 请求，否则会堵住现有模板保存流程。若确需数据模型调整，先说明现有存储为何不足及最小改动范围，不新增定价框架。
- `manual_edit` 历史标签本身不能证明颜色最终价是人工特例。旧数据仍需只读核对，不批量解锁或回填正式数据；明确最终价导入保留原值、不二次加价，其导入语义不能套用到 BOM 模板编辑。
- 用户进一步明确：BOM Admin 的手动 Copy 和 Manual Edit 都应改国家 Single 基准；Dual/Special 颜色加价必须覆盖在该基准上。`manual_edit` 不能继续作为模板派生颜色的永久锁价语义；只有没有可信基准的明确最终价才继续保护。
- 验收必须覆盖：OMODA 基准 15000→15500，Single=15500、Dual(+200)=15700；Dual 规则改 +300 后为15800且基准仍15500。再覆盖拖动分类、移走最后一个 Single、Special 特例、复制与批量调整、多国家不同基准、无基准、重复计算、备注单独保存不改变定价语义，以及 BOM/Matrix 一致。
- 模板基准批次已在同一 C worktree 实现，提交见下一节；`eb10c0e`、`be2b85e`、`282cd121` 的新增颜色/导入成果继续保留。代码只改独立 worktree，没有合并远端、部署 Candidate、更新 Active/www/intl 或写正式数据。合并、Candidate 部署和 Active 发布仍按已有授权边界执行。

### 2026-09-24 · C7 第三批：模板基准 FOB 与派生颜色重算（记录时本地实现；后续已随 PR #229 合入）

- worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_colour`；分支：`codex/bom-colour-followup`；提交：`83a45c63 fix(bom): preserve template base FOB semantics`。本批以已有 `282cd121` 为工作树基础，保留用户澄清的模板语义：含 `**` 的模板按国家保存 Single 基准，Single=基准，Dual/Special=基准＋统一颜色规则；没有新增独立 `**` 数据模型，也没有禁止非 `**` SKU 请求。
- 后端新增 `PATCH /order-genius/bom-templates/fob` 和 repository 同事务更新：读取/写入 `base_fob_eur`，按模板所有 active SKU 计算 `colour_surcharge_eur`/`final_fob_eur`，记录变化历史；清空基准会清理该模板国家行。模板编辑不再逐个 `updateSkuFob` 把同一输入标成 `manual_edit`。
- 拖动 Single/Dual/Special 会沿用已有 reprice 流程：`template_base` 与 `template_base_country_adjust` 可重算；本次新证据要求带可信基准的历史 `manual_edit`/复制行也可重算，只有无基准的明确最终价继续保护。最后一个 Single 移走不丢失其他颜色行上的 `base_fob_eur`。模板批量调整、Copy Material、国家复制/调整均传递基准和 surcharge 元数据；明确最终价导入仍不写 Single 基准、不二次加价。
- 前端模板 FOB 编辑、批量调价、Copy Material 改为模板接口；显示基准价和 surcharge，复制后使用接口返回的派生结果更新本地状态。普通无 `**` SKU 保留原逐 SKU 最终价路径。

验证（2026-09-24）：

- 后端模板/规则聚焦用例 `6 passed`；完整 `tests/unit/test_ordering_bom_admin.py` 为 `46 passed, 5 failed`。5 个失败仍是本分支既有基线缺口：`list_bom_admin_country_columns` 缺失/旧 NL 预期 3 项，`sync_missing_template_fobs` 缺失 2 项；没有把它们写成此次通过。
- `compileall` 通过；前端 `npm run check:types` 通过；全量 Vitest `73 files / 399 tests passed`；构建和路由回归通过（构建仅既有大 chunk warning）。
- 当前未验：Candidate 浏览器中的 15000→15500→15800 数值链路、不同国家基准、OMODA/JAECOO/Special 规则、无基准与手动保护、BOM/Matrix 双入口、真实 Copy/拖动交互。模板批次已随 PR #229 合入并部署到当前 Candidate，浏览器验收待正确登录凭据。

### 2026-09-24 · Candidate 浏览器验收批次 2：Special 规则正确，manual 基准语义仍需修正

- Candidate 实测样本为 OMODA9 SHS 的 `UE`，执行 `dual → special`。规则解析为 `OMODA / special / +300 EUR`；9 个国家中 8 个按 `+300` 更新，0 个缺 Single 基准。
- CH 显示 `25,400 → 25,400 · surcharge 200 · manual_fob`，不是 Special 规则没有命中，而是当前重算函数先按 `fob_source_mode in {manual_edit, manual_country_adjust}` 跳过，尚未读取该行的基准元数据。
- 业务修复口径已确定：Copy/Manual Edit 写的是国家基准，颜色 surcharge 重新覆盖计算；车型＋品牌＋色码特殊价为最高优先级。若 CH 保留 `25,200` Single 基准，预期 Special +300 后为 `25,500`。本次只做了 Candidate 沙箱验收，没有修改 Active 或正式数据。
- 下一批代码只需拆开“手动基准”和“手动最终价”语义：有可信 `base_fob_eur` 的行应重算并记录历史；没有基准的明确最终价继续跳过并说明原因。应补有基准 manual 行、无基准 manual 行、特殊价优先级和重复执行幂等测试。

### 2026-09-24 · manual 基准重算修复（本地已实现，尚未合入 Candidate）

- 新 worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_manual_base_reprice`；分支：`codex/bom-manual-base-reprice`；提交：`8ae4a0c7 fix(bom): reprice manual base rows with colour surcharge`。基于最终 main `efe5ac0f5b11`，没有修改旧 C 分支。
- 后端 `reprice_sku_colour_surcharge_fobs` 现在先寻找行内 `base_fob_eur` 或同模板可信 Single 基准，再应用当前 tier surcharge。带基准的 `manual_edit/manual_country_adjust` 行会转成可重算的模板基准来源；无基准手动行仍显示 `manual_fob` 并保护；显式最终价导入的其他 source mode 不受影响。
- 回归证据：颜色重算聚焦测试 `3 passed`；`tests/unit/test_ordering_bom_admin.py` 为 `47 passed, 5 failed`，5 项仍是该最终 main 已知的 country-column / `sync_missing_template_fobs` 基线缺口；compileall 和 diff 检查通过。
- 新增 OMODA9/UE 样本断言：手动行保留 Single 基准 `25,200`，Special 车型色码规则 `+300` 后得到 `25,500`，不再被旧 `+200` manual 标记冻结。该分支尚未创建 PR、合入 main 或重新准备 Candidate；Active/www/intl 和正式数据均未修改。

### 2026-09-24 · C7 第一批：新增颜色自动 FOB 初始化与特殊价页面初接（记录时本地实现；后续已随 PR #229 合入）

本批在 `/Users/litristan/Downloads/JATO_Analysis_System_bom_colour` 的 `codex/bom-colour-followup` 上实施，基线为已对齐的 `579ea01e`，业务提交 `eb10c0ead4a49e4d7f4152dabb63d04143b0676e`。没有合并远端、部署 Candidate、更新 Active/www/intl 或写正式数据。

- `create_material_sku` 接受 `sourceMaterialCode` + `automaticFobs`；在同一事务中创建新 SKU，并由 repository 按来源可用国家/付款条件寻找目标 BOM 模板下的 Single 基准，再调用统一 `get_colour_surcharge_amount_for_sku` 计算 Dual/Special 加价。保存 `base_fob_eur`、`colour_surcharge_eur`、`final_fob_eur` 和自动来源；缺 Single 基准逐国跳过并返回原因，不把自动行标成 `manual_edit`。
- 前端 Add Colour 不再“先重算空价格、再逐国调用手动 FOB 接口复制来源最终价”，改为把来源物料交给后端初始化；页面显示自动创建数量和缺基准国家。`automaticFobs` 与显式 `fobs` 互斥，避免同一请求重复写价。
- 现有 Special Colour Surcharge 页面初接：Colour Surcharges 工具中可加载、编辑、保存品牌＋可选车型＋色码/名称＋金额规则；列表点击可回填编辑。色卡 tooltip 对 Special 显示命中的车型色码/品牌色码来源，0 EUR 作为明确免加价保留。后端原有优先级继续生效：车型＋色码 → 品牌＋色码 → 品牌 Special 默认；Matte 只走 Special，不叠加 Dual。
- 本批没有宣称 C5/C6 完成：Special 色卡快捷入口、真正的预览→确认→应用、停用/删除后的回退预览、完整 reprice 明细和其他自动路径（包括 service 中的 `uploaded_base_plus_colour`）仍待后续批次核对。

验证（2026-09-24）：

- 后端新用例：自动创建路由、同模板 Single 基准＋加价、Special 特例覆盖品牌默认，共 `3 passed`；`compileall` 通过。
- 后端 `tests/unit/test_ordering_bom_admin.py`：`41 passed, 5 failed`。5 个失败是本分支原有基线缺口：缺少 `list_bom_admin_country_columns`（2 项）、空 FOB 列预期仍保留 NL（1 项）、缺少 `sync_missing_template_fobs`（2 项），不由本批变更引入；不能写成全量通过。
- 前端：`npm run check:types` 通过；全量 Vitest `73 files / 399 tests passed`；`npm run build` 通过（仅既有大 chunk warning）；`npm run check:router-regression` 通过。
- worktree 仅保留自动生成的 `hermes/dev_events/dev_events.jsonl` 未提交修改；没有把它 stage 到业务提交。

未验项与下一批：在 Candidate 真实浏览器验证普通 Dual、OMODA7/9 Matte 特例、J5 及非 J5、多国家不同 Single 基准、manual/no-base 保护、BOM/Matrix 一致；补 C5/C6 预览/应用/停用和 C7 其他自动入口。D 继续按原计划，不与本批混合。

### 2026-09-24 · C7 第二批：导入解析复用统一颜色规则（记录时本地实现；后续已随 PR #229 合入）

- 在同一 C worktree 继续提交 `be2b85eb8be8b01979b3ee4672c3e03f4a052575`，随后提交 `282cd1218dd4235ca40392f3c91387e66aa0cad8` 收紧明确最终价语义。`app/services/order_genius_service.py::_resolve_fob_for_sku` 的 `uploaded_base_plus_colour` 现在使用 `_effective_colour_tier` 和 `repo.get_colour_surcharge_amount_for_sku`，因此导入路径也遵守车型＋色码 → 品牌＋色码 → 品牌 Special/ Dual 默认的解析；明确最终价模式仍不叠加且不写入 Single base 元数据。
- 自动解析行现在写入 `base_fob_eur`、`colour_surcharge_eur` 和 `final_fob_eur`，后续重算能识别基准与加价；没有改手动编辑保护。
- 新增回归测试：Special 导入基准 15000、特例 +200 时得到 15200，并保留 base/surcharge 元数据。聚焦 4 项新用例通过；完整后端文件当前 `43 passed, 5 failed`，5 个失败仍为上一批记录的既有基线缺口。
- 本批已随 PR #229 合入并部署到当前 Candidate，尚未做 Candidate 浏览器验收；Copy Material、基准价导入的明确最终价分支和 C5/C6 预览/应用/停用仍待验。

### 2026-09-24 · 通用颜色加价与特殊价管理（已确认方案，C7 前为未实现口径）

本节优先于下文“C/D 已完成”“下一步只需合并”的历史表述；其中 C7 第一批已由上一个小节记录。本节其余 C5/C6/C8 仍是待实施方案，未部署或写正式数据。

- 用户确认：OMODA 普通 Dual 默认 +200 EUR，JAECOO 普通 Dual 默认 +300 EUR；Matte 属于 Special，不是双色，不叠加 Dual。OMODA7 Matte 特例 +200，OMODA9 Matte 特例 +300。金额由页面规则管理，不硬编码车型金额或根据名称包含 matte 自动分类；实际色码须从现有颜色库选择。
- C 的通用根因已由源码确认并在 C7 第一批修正：`handleAddColourEditorSubmit` 原先先创建 SKU、调用 `updateColourTier`，此时没有 FOB 可重算；随后 `copyPositiveFobsToMaterial` 原样复制来源最终 FOB，并经手动改价接口落成 `manual_edit`。后续规则重算因手动保护而跳过。现在新增颜色走后端事务内的 Single 基准＋统一规则初始化；Copy Material 和 service 其他自动入口仍需继续核对。复制已有正确双色可能碰巧金额正确，但来源仍错误。
- 现有特殊价表、GET/PATCH 接口、前端 API/types 及优先级查询已存在；C7 第一批已接入规则列表/编辑和 Special tooltip 来源显示，但快捷入口、预览/应用和停用仍未完成。详细实施见第 12 节 C5–C8。
- D@34656898 尚未收口：`window.open(..., 'noopener,noreferrer')` 返回 null 不能证明被拦截；旧核验响应缺少失效保护，可能覆盖较新认证状态。重登前后 quantity/BOM 草稿仅有不完整证据，需真实组件/浏览器测试，不以源码字符串断言代替。
- 状态日期纠正：D 提交 34656898 的 Git 时间是 2026-09-15；9 月 23 日是后续文档更新日期，不能当作代码实现或重跑测试日期。
- 用户希望已修功能先进入 Candidate 亲测：优先补齐 A/B 必要交互验证并检查组合结果；C7 第一批可在审阅后与既有 C/E 分批准备，但必须标注 C5/C6 其他入口和浏览器验收仍未完成。D 先补上述缺口。之后按现有 PR→main→Candidate 流程交付，不要求所有后续功能一起等齐；Candidate 验收不等于 Active 发布。本节原始方案记录不执行合并或部署。

### 9 月 15–23 日执行记录（当前状态以 9 月 24 日修订为准）

- A 已有本地提交 `260f839a`、`6eeddf3c`；实际新增组件测试仅覆盖两项搜索场景。输入焦点、1200ms、关闭/切换目标、FOB 保存刷新等完整行为验证尚未补齐，因此不能称 A 已全部验收。
- `9011da6a` 是 9 月 15 日历史 Candidate；当前 Candidate 为 `efe5ac0f5b11`，已包含下述修复。400 个测试是历史实跑结果，不代表本清单所有业务场景通过。
- 后续五项对应：Model/列宽 → B；J5 空品牌/漏统计/加价 → C1/C2/C4；颜色双入口同步 → C3；登录提示 → D；蓝边框 → E。五项实施说明均在第 12 节，B/C（含 E）/D 的本地批次已完成。
- B 已随 PR #230、C（含 E）已随 PR #229 合入最终 main，并部署到当前 Candidate；上述 worktree 提交是历史审阅证据，不是当前发布状态。没有更新 Active/www/intl 或写入正式数据。
- D 已在独立 worktree 对齐同一 `main@9011da6a`（对齐提交 `f6887fb3`），并完成衔接修正提交 `34656898`；原业务提交 `1a64194a` 保留。修正仅涉及认证核验结果分类、启动异常处理、页面内 403 核验和新标签登录，不改变后端权限或 #228 的 Candidate token/OAuth 隔离。
- 本地衔接批次复核（2026-09-23）：B/C/D 对齐后相对 `main@9011da6a` 均为 0 behind；B/C 无文本冲突，D 修正只在其自身改动范围内继续。共同修改的 `OrderGeniusPage.tsx` 与规则测试仍须在合并后的组合构件做人工验收，Git 无冲突不等于业务通过。
- D 衔接修正已实跑：无 token 返回 `false`；200 返回 `true`；保留模式下 401 返回 `false` 但不清理 Candidate token/身份；网络异常和 5xx 抛出可区分错误且不清理身份。启动/后台刷新调用者已显式吞掉异常，避免未处理 Promise；登录按钮打开带原路径的同源新标签，原页通过 focus 事件复核身份并提示用户主动重试，数量/BOM 草稿不自动提交。
- 发布顺序：本地行为验证 → 可审阅 PR → 获授权合并 main → 现有 CI 自动 prepare Candidate → 在新构件做业务验收 → 单独决定 Active 发布。不要要求“Candidate 验收通过才允许代码进入 main”，这会与现有 main-only Candidate 流程形成循环。

### 2026-09-15 · A 第一版（待补齐交互验证和异步边界）

- 分支：`codex/bom-input-search-continuity`；基线：远端 `main@9011da6ac1f5592e37b7cba80c455e30e0b0b48e`。代码 worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_input_search_continuity`。
- 已修改：`06_AppPlatform/frontend/src/pages/OrderGeniusPage.tsx`、`06_AppPlatform/frontend/src/tests/unit/orderGeniusColourRulesPage.test.ts`。没有修改后端、价格规则、认证或部署。
- 已实现：颜色编辑 focus 只依赖稳定编辑目标；Add/Edit lookup 延迟统一为 `1200ms`；保存/刷新复用当前已生效搜索，显式 Clear 保持为空查询，加载中的空查询不会被转换成全量刷新。
- 已验证：`npm run check:types` 通过；`npm run test:unit` 通过（73 files / 398 tests）；`npm run build` 通过；`npm run check:router-regression` 通过。
- 尚未完成：新构件尚未部署 Candidate；需在 Candidate 浏览器实测名称连续输入、代码查库回填不抢焦点，以及 `MH0032 → FOB 保存 → 仍保留搜索`。因此本记录不是 A 的线上验收或发布批准。
- 下一步：在现有分支补齐下述 A 收尾，形成可审阅代码和行为证据，再走现有 PR→main→自动 prepare；B/C/D 保持各自明确范围。

### 给 Luna Max：当前交接状态与顺序

| 工作 | 当前状态 | 具体改法入口 | 完成证据 |
|---|---|---|---|
| A 输入/自动填充/搜索 | 已在 Candidate 开始验收，仍未全通过 | 第 13 节 A 收口记录 + 第 12 节 A | 已观察 1.2 秒搜索入口；逐字输入、自动回填后二次修改、FOB 保存后搜索保持仍待验 |
| B Model/列宽 | Candidate 部分通过 | `codex/bom-model-layout` / `53976fc0`；Grid 默认 280px、优先固定 Model、用户列宽初始化与持久化 | 50/100/150% 缩放下 Model 保持最左；拖拽列宽、刷新/数量保存持久化、真实窄窗口仍待验 |
| C J5 品牌、颜色共享、加价 | Preview/Apply 通过，价格和共享色卡未收口 | `codex/bom-colour-followup` / `2218c0df` + `eb10c0e` + `be2b85e`；新增颜色与导入按 Single 基准初始化，Special 规则列表/编辑和来源 tooltip 初接 | 8 个确定性填充已写入 Candidate；仍有 24 missing、9 conflicts；OMODA9 Matte 行显示 Dual default，未证明 +300 已生效 |
| D 登录失效 | 管理员登录通过，真实失效场景未验 | `codex/bom-auth-feedback`：`f6887fb3` + `34656898`；保留 #228 认证逻辑，区分 401/403、网络/5xx，登录新标签保留原页草稿 | 类型检查、全量单测、构建、路由回归已通过；低权限 403、过期 token、断网和草稿保留仍待 Candidate 验收 |
| #215 同模板跨车型/版本 | 专项验收未完成 | 第 12.2 节：构造两个真实渲染行并点击 Edit | 只有被点的目标行打开；不是仅比较 key 字符串 |
| #220 Matrix/保护逻辑 | Preview/Apply 及读取部分通过，其他待验 | 第 3 节未验项 + C 的用例 | 8 行颜色名称 Apply 成功；BOM/Matrix 双入口、tier/manual/no-base 和 Special 数值仍需真实样本 |

当前顺序（9 月 24 日修订）：保留现有 A/B/C/E 成果并核对组合行为，准备分批 Candidate 验收；C 按 C5–C8 补通用新增定价和特殊价管理，D 补弹窗误判、旧响应保护和真实草稿测试。上表的本地完成仅指原批次，不代表这些新增缺口已修复。进入 main/Candidate 前核对实际 PR 与构件，不能直接按原始 D 或旧文档宣称可合并。

### 给 Luna Max：D 衔接修正（最新任务，优先于原“本地完成”记录）

目标：BOM/数量保存遇到认证失败时，用户能分清登录失效、权限不足和网络故障，并能在重新认证后保留输入、主动重试。继续现有 B/C/D worktree，不重做已有业务功能。

1. 先检查三个 worktree 的 status、HEAD、当前远端 main；保留未提交的 Hermes 事件文件，不 stage 到业务提交。B/C 在各自原分支正常对齐 main；D 同样续接原分支。不要整文件覆盖 main，不需要重开 D，也不在混合观察区开发。
2. `contexts/AuthContext.tsx` 的 `refreshUser`：所有正常返回路径满足声明类型；无 token/确证认证失效和无法核验必须可区分。若保留 boolean，可用 true 表示有效、false 表示确证无效，网络/服务错误通过异常传播给调用者；检查启动时和后台调用者如何处理异常，避免未处理 Promise。200、认证拒绝、网络异常、5xx 分别处理，不把所有非 200 都解释为过期。
3. 保留 #228 的 Candidate 真实 token 来源、OAuth 回调隔离、未登录访问限制。复用现有认证函数，明确页面内 403 核验与启动认证的副作用：瞬时网络失败不能清登录身份或导航，确证失效仍须阻止业务请求。不要仅为保留页面而放宽后端权限或恢复匿名 Candidate 身份。
4. `pages/OrderGeniusPage.tsx`：403 核验 200 显示权限不足；确证失效才显示重新登录；网络/5xx 显示无法核验。并发核验复用现有去重，旧核验结果不能覆盖较新的认证状态；不自动重试写请求。
5. 重新登录与草稿：当前直接跳 `/login` 会卸载页面，不能只改提示文字就宣称保留成功。优先验证现有登录页能否由用户在新标签完成登录，原页复用身份刷新后继续编辑；要同时检查 RequireRole 清 token 时的跳转。若现有流程无法保留页面，再说明最小草稿恢复方案及所需文件后实施，限制为当前用户/当前编辑上下文，不保存密码/token、不跨账号回填，不建设通用恢复系统。必须实际证明数量与未保存 BOM 表单仍在，登录成功后由用户主动重试，继续保留 409 冲突处理。
6. 预计主改 `AuthContext.tsx`、`OrderGeniusPage.tsx` 与现有认证/交互测试；只有证据表明确有需要才涉及 `RequireRole.tsx`、`LoginPage.tsx`。`api/client.ts` 事件入口优先复用。写代码前报告具体根因、负责函数、文件和预计行数；不要扩大成全站认证重构。

验证与交付：

- 先用真实 AuthProvider/路由及页面行为测试覆盖：无 token 返回契约；有效身份 403；坏/过期 token；`/auth/me` 网络异常与 5xx；重新登录前后 quantity/BOM 草稿；并发失败与旧核验响应。不以源码字符串断言代替交互测试。
- 在对齐后的 Vite 前端运行 `npm run check:types`、相关测试、`npm run test:unit`、`npm run build`、`npm run check:router-regression`。单独检查 #228 Candidate 登录回归，以及 B/C/D 累积合并结果；Git 无文本冲突不等于行为通过。
- 复用已验证依赖，构建前检查磁盘；不自动安装依赖。历史 C 后端 5 个失败如被引用，标为历史记录，需以基线对照确认其归属，不写成此次已重跑通过。
- 每批更新本节实际基线、提交、实跑结果和未验项，保留原历史记录。A 的 `260f839a`、`6eeddf3c` 仍独立保留，不能被 B/C/D 的整文件覆盖。
- 本轮交接任务是本地对齐、最小修正和验证。远端合并、Candidate 部署及 Active/www/intl 操作仍按用户明确授权执行；目前未取得这些新授权。

### 2026-09-15 · B Model 紧凑布局与列宽（历史实现记录；已随 PR #230 合入）

- 基线：`JATO_Analysis_System/main@f29cf509`（远端跟踪 ref）；独立 worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_model_layout`；分支：`codex/bom-model-layout`。
- 根因：`getModelColumnWidth(rows)` 将分组标题和完整元信息估宽，并把结果放进随数据依赖重建的 `columnDefs`；固定区同时包含多列，AG Grid 在窄视口会自动解除固定，Model 因此可能跑到右侧。此前没有用户列宽持久化。
- 实现：Model 改为稳定 `initialWidth=280`、`minWidth=220`、`maxWidth=760`，设置稳定 `colId`、`lockPinned` 和 `lockPosition`；Version/Colour/Interior/Material/Note/FOB/Country 不再强制 pinned，保留 Model（以及 PI 选择列）为固定优先级，其余列横向滚动。分组标题和元信息在固定宽度内截断并提供 tooltip。
- 实现：使用 AG Grid Column State API 只恢复/保存 `{colId,width}`；按 `user.username` 生成 `localStorage` scope，校验宽度范围，保留显隐列的历史宽度，不保存自动 unpin 的顺序/位置。新增显式 `Reset column widths`，只清除列宽偏好并恢复初始宽度，Refresh 不触发重置。
- 变更文件：`06_AppPlatform/frontend/src/components/OrderGeniusGrid.tsx`、`06_AppPlatform/frontend/src/pages/OrderGeniusPage.tsx`、`06_AppPlatform/frontend/src/tests/unit/orderGeniusGridLayout.test.ts`。提交：`53976fc0 fix(bom): keep model column stable across grid refreshes`。
- 本地验证：`npm run check:types` 通过；`npm run test:unit -- --run src/tests/unit/orderGeniusGridLayout.test.ts` 实际执行全目录 74 files / 393 tests 通过；`npm run build` 通过；`npm run check:router-regression` 通过。构建仅有既有大 chunk warning。
- 尚未完成：当时未创建/合并 PR；后续已随 PR #230 合入并部署到当前 Candidate，仍未做真实浏览器窄屏/缩放、拖拽列宽、Refresh/数量保存/页面重载及显隐列恢复验收。临时依赖 symlink 已删除；worktree 仅保留自动生成的 `hermes/dev_events/dev_events.jsonl` 修改，未纳入提交。

### 2026-09-15 · C（含 E）共享颜色、J5 品牌归一化与色卡边框（历史实现记录；已随 PR #229 合入）

- 基线：`JATO_Analysis_System/main@f29cf509`；独立 worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_colour`；分支：`codex/bom-colour-followup`。
- 根因：部分 J5 ICE/HEV SKU 的存储 `brand` 为空；规则 key、色卡入口、surcharge 查询和 Matrix 筛选都因此漏行或返回 0。两个编辑入口的标准色卡写入范围也不一致。普通色卡硬编码蓝边并不代表冲突或未保存。
- 实现：新增 `resolve_material_brand`，仅用已知的 JAECOO/OMODA 身份从 `brand`、`model_name`、`bom_template` 归一化，不猜未知品牌；BOM、Matrix、规则汇总、surcharge 查询和逐国重算复用该结果。规则摘要新增无效身份计数与最多 5 个样例，前端 Colour Swatch Rules 卡显式告警；无效 SKU 不进入 Preview/Apply。
- 实现：Edit Colour Code 在同一 Brand + Code 下修改名称或有效色卡时复用 `setOrderGeniusColourHexRuleStandard`，与小色卡入口使用同一 active-SKU 作用域；改 code 仍走现有单 SKU 迁移逻辑。色卡普通值改为中性细边框，missing 保留虚线，可点击色卡保留 `:focus-visible`。
- 变更文件：`06_AppPlatform/backend/app/api/routes/order_genius.py`、`06_AppPlatform/backend/app/infra/order_genius_repository.py`、`06_AppPlatform/backend/app/services/order_genius_service.py`、`06_AppPlatform/backend/app/services/ordering_normalization.py`、对应后端测试；`06_AppPlatform/frontend/src/index.css`、`OrderGeniusPage.tsx`、类型与规则契约测试。提交：`2218c0df fix(bom): unify legacy colour identities and shared swatches`。
- 本地验证：前端 `npm run check:types`、`npm run test:unit`（73 files / 393 tests）、`npm run build`、`npm run check:router-regression` 全部通过；后端 Python compileall 通过，相关规则/品牌测试 9 passed，Matrix/Options 4 passed。完整 `test_ordering_bom_admin.py` 为 39 passed / 5 failed，5 个失败均为该基线已有的 `list_bom_admin_country_columns` / `sync_missing_template_fobs` 缺失或旧测试契约，不是本批改动引入；需在后续基线对齐时单独处理。
- 尚未完成：当时未合并/部署；后续已随 PR #229 合入并部署到当前 Candidate，仍未做真实 J5 ICE/HEV 浏览器验收；未写 Active 数据，未验证实际 +300 重算、重复重算幂等、manual FOB 与缺 Single 基准跳过、BOM/Matrix 双入口真实写入后 UI 一致。worktree 仅保留自动生成的 `hermes/dev_events/dev_events.jsonl` 修改，未纳入提交；临时依赖 symlink 已删除。

### 2026-09-15 · D 登录失效提示与数量草稿保留（原始实现；已由下方衔接批次修正）

- 基线：`JATO_Analysis_System/main@f29cf509`；独立 worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_auth_feedback`；分支：`codex/bom-auth-feedback`。
- 根因：`api/client.ts` 的 `request` 已把 HTTP status 放进 Error，但页面只显示原始错误；401/403 没有统一可见提示。数量单元格失败路径会清掉 draft 或回滚显示值，用户无法判断是登录失效、权限不足还是普通冲突。
- 实现：`request`/`requestBlob` 对受保护路径的 401/403 发出轻量 `AUTH_FAILURE_EVENT`（登录接口不触发）；Order Genius 复用 `AuthContext.refreshUser()` 核验 403，区分“当前账号无权限”“登录已失效”“无法核验网络”。不修改后端 `require_min_role`，不自动重试或自动提交。
- 实现：数量保存收到 401/403 时保留当前草稿和值，并显示原始错误；其他 409 冲突和网络失败继续走原有处理。原始版本的重新登录动作会清理 token 并跳转；该导航会卸载内存草稿，已在下方衔接批次改为同源新标签 + 原页复核。
- 变更文件：`06_AppPlatform/frontend/src/api/client.ts`、`contexts/AuthContext.tsx`、`pages/OrderGeniusPage.tsx` 及两份规则/认证契约测试。提交：`1a64194a fix(bom): explain auth failures and preserve quantity drafts`。
- 本地验证：`npm run check:types`、`npm run test:unit`（73 files / 393 tests）、`npm run build`、`npm run check:router-regression` 全部通过；新增受保护 401 事件测试通过。构建仅有既有大 chunk warning。
- 尚未完成（原始版本）：当时未合并/部署；后续已随 PR #232 合入并部署到当前 Candidate，仍未用真实过期 token、有效低权限账号和网络断开做浏览器验收；未证明正式站历史红字一定由会话失效引起。衔接后的跨标签草稿保留仍需真实浏览器验收，不能在未实测前宣称已跨页恢复。

### 2026-09-23 · B/C/D 对齐与 D 衔接修正（历史实现记录；已随 PR #229–#232 合入）

- 统一比较基线：当前远端 `main@9011da6ac1f5592e37b7cba80c455e30e0b0b48e`。
- B `codex/bom-model-layout` 已通过合并提交 `ac59a832` 对齐 `main`；C（含 E）`codex/bom-colour-followup` 已通过 `579ea01e` 对齐；D `codex/bom-auth-feedback` 已通过 `f6887fb3` 对齐。三批原业务提交 `53976fc0`、`2218c0df`、`1a64194a` 均保留，没有整文件覆盖 #228。
- D 衔接修正提交：`34656898 fix(bom): align auth feedback with candidate session flow`。主改 `AuthContext.tsx`、`OrderGeniusPage.tsx` 及现有认证/规则测试，未改后端权限、Candidate token 来源、OAuth 隔离或正式数据。
- `refreshUser` 现在统一返回契约：无 token/确证 401 或 403 返回 `false`；200 返回 `true`；网络异常和 5xx 抛出异常。默认启动校验仍会在确证 Candidate 认证失效时清理身份；页面 403 核验使用 `preserveSession`，不会因瞬时失败清理身份或导航。
- 401/403 业务请求仍通过现有 `AUTH_FAILURE_EVENT` 提示；页面只在 403 核验确认失效时给“重新登录”。重新登录打开同源新标签并带回原路径，原页面保留数量与 BOM 内存草稿，用户完成登录后回到原页由 focus 事件复核并提示主动重试；不自动重放写请求，不保存密码或 token 副本。
- 实跑结果（最终提交 `34656898`）：`npm run check:types` 通过；`npm run test:unit -- --run ...` 实际运行全目录 73 files / 403 tests，全部通过；`npm run build` 通过（仅既有大 chunk warning，build meta commit=`3465689`）；`npm run check:router-regression` 通过。D 新增无 token、网络异常、保留模式 401、5xx 测试；依赖只读复用主 worktree，临时 symlink 已删除。
- 尚未完成：当时尚未创建/更新远端 PR；后续已按 C → B → A → D 合入并部署 Candidate。仍尚未在真实浏览器验证新标签登录前后数量/BOM 表单保持、有效低权限 403、真实过期 token、网络断开，以及 B/C/D 组合后的 Model/J5/Matrix/manual/no-base 业务场景；当前先解决 Candidate 登录凭据。

### A 收尾：本地代码已收口，Candidate 仍待验收

复核 `260f839a` 后，以下并发和编辑目标边界已在同一 worktree 收口；A 已随 PR #231 合入最终 main 并部署到当前 Candidate，但浏览器验收仍因登录凭据无效而未开始。

1. **搜索队列已收口。** `latestLoadKeyRef` 和 `pendingLoadKeyRef` 现在始终代表最后一次请求意图；A→B→A 不会在 A 完成后错误执行旧 B。显式空字符串仍表示 Clear，`null` 仍表示无待办；保存触发的同查询刷新仍会执行。
2. **旧结果/错误已隔离。** `getBomAdmin` 返回或失败时，只有仍对应最新查询的请求才能更新行、国家、notice 和错误；过时请求只结束自己的 loading 流程。
3. **定时衔接已改为直接执行最新 pending。** finally 不再用捕获旧值的零延迟 timer，释放当前请求后直接提交最新 pending key，避免旧查询重新排队。
4. **lookup 生命周期已绑定目标。** Edit/Add 的 lookup effect 纳入稳定编辑目标 key；切换同品牌同代码的另一材料会清理上一结果/错误，回填前同时核对目标身份、当前代码和 requestId；已有 touched 字段保护保留。
5. **实际组件交互测试已补。** 新增 `bomAdminAConcurrency.test.tsx`，用真实 `BomAdminPanel`、延迟 Promise 和模拟 API 覆盖 A→B→A 最终刷新、旧响应不得覆盖新查询；现有源码契约测试同步改为检查目标 key 和最新查询守卫。类型检查、全量单测、构建和路由回归均已通过（74 files / 400 tests）。

本地收口提交：`6eeddf3c fix(bom): close search and lookup continuity gaps`（前一版本：`260f839a`）。2026-09-15 旧 Candidate 中观察到 Code `BW` 被重新聚焦并全选（activeElement=Code，selection=0–2），未点击保存、未写入沙箱；该旧构件证据不能代表当前 main。当前仍需在新 Candidate 实测逐字 code、名称焦点、约 1.2 秒回填、回填后手改、切换/关闭目标、加载中 Clear、FOB 保存刷新和国家筛选；取得正确凭据后再做这些验收，Candidate 验收通过后再决定 Active 发布。

首要续接路径：已完成合并；后续只需在当前 Candidate 做浏览器复验。提交钩子留下的 `hermes/dev_events/dev_events.jsonl` 修改需保留并识别，不混入无关业务修改；不能回到旧 Candidate 页面验证新代码。

## 1. 基线与证据边界

> 本节大部分数字是 2026-09-15 旧 Candidate 的只读基线；当前待验构件是 main `efe5ac0f5b11`，部署 run `35960030314`，archive `2810a3510337`。旧基线中的登录成功、规则数量和沙箱标识不能替代当前构件验收；当前构件首次登录尝试 `candidateAdmin` 已返回 `Invalid credentials`。

- 实测入口：https://candidate.ojeur.cloud/product/order-genius；正确测试凭据待恢复，不在文档记录密码。
- 页面显示 commit `9011da6ac1f5592e37b7cba80c455e30e0b0b48e`，artifact `f55a70fea05f3ba983db1f9e7f83e4fd67a6d1285b2a43373265a1109c3981b2`。
- 沙箱 `jato_candidate_20260915t011310z_3972d27f5b654a45`，页面显示 Active 快照开始时间 2026-09-15 09:13:10。所有本轮业务写入均经 Candidate 同源 API。
- #226、#228 已合并，Candidate 已使用应用登录，不再需要 Basic Auth。本轮重验真实登录，以及 `/v1/auth/me` 匿名 401、坏 token 401、有效身份 200。
- 用户截图里可见的地址是 `ojeur.cloud`，不能把截图当成当前 Candidate #220 的表现。当前 Candidate 报告已经不同于截图中的“50 rules / clean”。
- 源码审查以 `JATO_Analysis_System_candidate_readiness` 的 `fa55a8f91ef0ab299d3794393a18052cc4709c2f` 为本地读取基线；不是旧 handover worktree 中的认证代码。线上构件以页面 SHA 为准。
- 本轮未重跑历史单元测试，未完成所有写入/并发/价格重算验收。以下明确区分实测、源码证据、待验证假设。

## 2. 结论：不是一个 bug，但有三条清晰的关联链

| 关联链 | 已取得的证据 | 影响 |
|---|---|---|
| J5 SKU 品牌为空 → 规则 key 无效、加价查不到 | Candidate 310 个 SKU 中 35 个空品牌：ICE 21、HEV 14 | 色卡入口拒绝、共享规则漏统计、品牌 surcharge 返回 0 |
| 保存/输入触发错误的状态更新 | 输入 effect 依赖整个编辑对象；保存后 `load()` 未携带搜索 | 焦点全选、搜索结果恢复全量；不是数据真的被删除 |
| Model 自动扩宽 + 多列固定 + 未保存列状态 | 宽屏 Model 740px 固定左侧；缩窄后变为非固定列 | Model 跑到其他固定列后；用户列宽不能可靠保留 |

登录失效是另一条链，不能直接与颜色库或布局合并为同一根因。红字也不能一律理解为认证失败。

## 3. #215 / #220 验收结果

| 项目 | 结果 | 证据或剩余工作 |
|---|---|---|
| #215 普通行 Edit | 部分通过 | J5 ICE 展开后点一行，只有 1 个 Done；其他行仍为 Edit |
| #215 同模板跨车型/版本不串行 | 待专项样本验收 | 当前 310 SKU 中没有同模板跨 modelName/version 的样本；源码 `buildBomEditScopeKey(modelGroupKey, versionKey, bomTemplate)` 与对应测试已存在，不能用普通行测试代替 |
| #220 Refresh 五类状态 | 通过，但有统计盲区 | 初始 36 rules：fillable 5、missing 22、nameConflict 7、swatchConflict 2、complete 0；空品牌 SKU 被排除 |
| #220 Lookup | API 通过 | OMODA/W3 → water blue、`#B6D3FB`，source=brand_code_rule，无冲突 |
| #220 Preview → Apply | 真实页面写入通过 | Preview 8 项，点击 Fill 8 deterministic items，HTTP 200 / updated=8；再次 Preview 为 0 项 |
| Apply 不改价格和 tier | 本次样本通过 | 比较前后 310 SKU 的 BOM API 全字段，仅 8 行 `colour` 改变；FOB、tier、swatch、rowVersion 等返回字段均未变；数据库内部 updated_at 不在该 API 内 |
| Add/Edit 自动填充与二次修改 | 已有实现，完整 UI 验收未完成 | 现有 180ms debounce、requestId 防旧响应、touched 标记；焦点 bug 阻碍正常输入 |
| J5 色卡入口 | 失败，实测复现 | 展开 ICE 后有 21 个 missing-brand/code/name 按钮；点击显示 required 提示，不打开色卡 |
| BOM / Matrix 同步 | 数据读取样本通过，双入口写入未验完 | Candidate SE/2026 Matrix 93 行全部匹配 BOM materialCode，colourHex 零差异；仍需验证两种编辑入口更新后的 UI 与历史行边界 |
| tier 重算 / manual FOB / no Single base | 待真实价格写入验收 | 源码有 skippedManual / skippedNoBase 与逐国明细；本轮未执行 tier 修改，不能标记通过 |

### 本轮沙箱写入记录（保留在 Candidate，未恢复）

仅通过现有 Preview/Apply 改了以下颜色名称，未改价格。它们是验收数据，不是对正式站名称的业务批准；例如 “Model green” 仍应由业务确认标准名称。

| materialCode | 原名称 | 应用规则后的名称 |
|---|---|---|
| T7000NHW3MY0001 | W3 | water blue |
| T7000NHX4MY0002 | X4 | Carbon crystal black&Aviation silver |
| T7000NHZEMY0002 | ZE | Carbon crystal black&Khaki white |
| T7000NHZUMY0002 | ZU | Carbon crystal black&Aquatic green |
| T71604NSPMH0031 | SP | Model green |
| T71604NSPMH0032 | SP | Model green |
| T71604NSPMH0035 | SP | Model green |
| T7160RGSPMH0001 | SP | Model green |

输入焦点测试的 `audit unsaved` 已取消，没有保存。下一次 prepare 会替换一次性沙箱，不要把本轮测试结果误认作 Active 已修复。

## 4. J5 ICE / HEV：空品牌是共同根因，但修品牌不等于自动补完历史价格

### 已证实

1. BOM API 返回 JAECOO5 ICE 21 行、JAECOO5 HEV 14 行 `brand=""`，不是页面仅仅漏显示标题。
2. ICE 样本 `T71611CBWMM0013` 有色码 BW、名称 Khaki white，但品牌为空、colourHex=null。其来源元数据是 `OMODA&JAECOO - Order Material Codes.xlsx` / `JAECOO5 ICE` / row 23。
3. `renderColourChip` 要求 brand、code、name 都非空才打开色卡，故 21 个 ICE 入口均被挡住。
4. `_colour_rule_key` 要求规范化品牌+色码。`build_colour_hex_rules_from_skus` 对无效 key 直接 continue，所以这些 SKU 不计入 missing，也不参加 Preview。
5. 前后端加价查找依赖品牌。后端 `get_colour_surcharge_amount_for_sku` 没有命中规则就返回 0；品牌默认查找目前是字段精确匹配，还应检查非空品牌的规范化一致性。
6. Candidate 当前规则：OMODA dual/special = 200 EUR，JAECOO dual/special = 300 EUR。J5 应按 JAECOO 规则讨论 +300，而不是默认写死 +200；若业务要求 J5 +200，那是另一个价格规则决定。
7. HEV 样本 `T71516FZ6MH0012` 的 BG final=15500、uploaded=15800、source=copied_from_country；`T71516FZ6MH0011` 为 13700/14000。差值与 300 一致，但不能仅凭差值断言当年哪次操作覆盖价格。

### 最小处理方向

- 先做一次精确范围的数据修正：核实这 35 行品牌应为 JAECOO，复用已有 metadata 更新逻辑；不重导整个 Excel，不搬旧分支。
- 追溯历史写入来源尚未完成。当前 `material_master_parser.py` 已从 model_name + sheet_name 检测品牌，`ordering_normalization.py` 也已能识别 JAECOO；不能未经验证再写一个全新的品牌推断器。
- 规则报告应把无效品牌/色码计为“未纳入规则的 SKU”，不能静默消失；无需另建治理平台。
- 修品牌后重新 Refresh/Preview；颜色库仍可能缺失/冲突，不保证一键都可填。
- 再逐国列出 Single 基准、现有 FOB 来源、tier、适用 surcharge。调用现有重算逻辑只处理明确可重算行，保留 manual FOB 和 missing-base 跳过原因。不对所有颜色一律 +300。
- 色卡是显示属性，tier 是计价属性：双色 swatch 不等价于必须改成 dual tier；本次 J5 确有 dual 行但品牌失效。

## 5. “所有物料共用颜色库”目前实现到了哪里

当前不是按 Material 建库，而是从所有 active SKU 汇总 **规范化 brand + colourCode**，不同物料/车型共用；它也不是一个完全独立、权威的颜色主表。名称/swatch 仍保存在 SKU 上，汇总可能发生冲突。不能把“共享”理解为不分品牌同色码必然一致。

- #220 已把名字从规则 key 中移除；Refresh 分五类，Preview 只填确定项，冲突不会随意覆盖。
- 小色卡弹窗走 `setOrderGeniusColourHexRuleStandard` → `set_standard_colour_hex_for_rule`，更新同品牌同代码的 active SKU。
- Edit Colour Code / Add Colour 复用 `resolve_colour_attributes`，但手工覆盖仍能作为 SKU 值保存并造成规则差异；它不是自动批准整个品牌的标准变更。
- 因而用户要求“两个入口修改色卡同步”，与目前两个入口的写入作用域仍有差距。最小方案是明确普通代码迁移与共享标准编辑的差别；同样的“改标准色卡”入口复用现有 standard 更新函数，不另建第二个颜色库。改代码时不要顺手把不相关旧 code 的全局标准覆盖掉。
- Matrix 缺色时不应凭本地名称表画出一张看似正确的色卡、BOM 却另存一套；继续复用 #220 的 colourHex 数据和公共 renderer。对历史 SKU 是否追随新标准必须明确，现有 standard 更新限定 active。

## 6. 输入焦点、蓝框与 1.2 秒自动填充

### 输入 bug：已实测

在 Candidate J5 ICE 的 Edit Colour Code 窗口修改 Colour name，150ms 后 activeElement 已变回 Code，值 BW，selectionStart=0 / selectionEnd=2。取消退出，未提交。

原因：`OrderGeniusPage.tsx` 两个 focus effect 分别依赖整个 `colourCodeEditor` / `addColourEditor` 对象；任一 onChange 或查库回填都会生成新对象，再次 focus/select。不是已证明的浏览器 Insert 覆盖模式。

修复方向：仅弹窗打开或切换明确编辑目标时聚焦一次；输入、改名称、改 swatch 和异步回填均不得抢焦点。复用现有弹窗，不加另一套输入状态机。

### 蓝框不止一种

- 输入框蓝边是焦点提示；反复抢焦点使它看起来不断跳动。保留可访问的键盘焦点提示，不简单删掉所有 outline。
- 小色卡蓝边在当前 `renderColourChip` 中是有 swatch 时硬编码 `2px solid #3b82f6`；不是“未保存”或“冲突”的可靠信号。建议普通色卡用中性边框，focus-visible 再突出，冲突使用有文字解释的状态。

### 自动填充不用重做

现有 #220 Add/Edit 查库是 180ms debounce，不是用户希望的约 1200ms；已经有 requestId 和 `colourNameTouched/colourHexTouched` 保护。优先修焦点，再把现有等待时间调到约 1.2 秒：

- 有有效品牌/代码且无冲突，才回填未手工修改的名称/色卡。
- 同一代码回填后允许二次编辑，等待/刷新查询响应不反跳；换代码才重置该代码的 touched 状态。
- 冲突、无规则、缺品牌显示原因，不能把灰色默认值当成已命中的标准色卡。

## 7. 搜索后保存 FOB，为什么变成全量结果

源码闭环已明确：`saveFobEdit` 保存并 patch 当前行，然后 `scheduleLoad(1200)`；`scheduleLoad` 延迟执行的是不带参数的 `load()`；`load(s)` 只有在 s 非空才提交 search/country。输入框文字还在，返回列表已无筛选。

同类路径还包括 metadata 保存、颜色标准保存、Preview Apply、Copy Country、Adjust Country、模板改名等直接 `load()`，所以只修 FOB 一个按钮不够。

最小方向：让现有重载流程默认使用当前已生效搜索条件，显式 Clear 才加载全量；统一调用者语义，保留现有本地 patch，避免手动输入字段与列表条件分离。后续验收记录 search 请求参数，不只观察输入框。此次未改 FOB 来复现该写入路径。

## 8. Model 列和用户列宽

实测步骤：2200px 宽刷新页面 → Model 固定左侧、740px；缩到 1400px → Model pinned=false，前面留下 Version/Colour/Interior/Material/Note/FOB。再放大不自动恢复，刷新宽屏才重新固定。实测是 viewport 宽度变化，尚未逐档测试浏览器原生缩放。

源码 `getModelColumnWidth(rows)` 把分组标题与完整 groupMeta 拼接估宽；没有紧凑最大值。其他信息列也都 pinned:left，固定区太宽，窄屏解除固定时未保护 Model 的优先级。

建议交互：

1. 默认 Model 约 260–300 CSS px，以用户截图 #3 为视觉目标；主体显示车型/版本，重复说明截断或放 tooltip，不按整段元信息无限扩宽。
2. Model 始终第一信息列并优先固定；窄屏其余信息列进入横向滚动区域，不把 Model 释放到右侧。PI 选择框的固定位置另按现有功能保留。
3. 用现有 Grid column state 保存用户宽度，稳定 colId；宽度默认值只用于首次初始化，不在数据/数量刷新时强制应用。只保存所需宽度等偏好，不恢复过去自动 unpin 产生的错误列顺序。
4. 区分“刷新数据”和“重置列布局”。前者保留宽度、筛选、展开与滚动；后者才恢复默认。页面重载后也恢复用户偏好。

当前 columnDefs 会随 rows 派生宽度/其他依赖变化重建，显式 width/pinned 会再次参与更新，未发现列宽持久化接入。刷新页面丢宽度的原因明确；每次数字保存是否都触发 width 重置仍需回归实测，不能一概而论。

## 9. 红字保存失败 / BOM 403：可能同类，但还不能定案

- Candidate 已验证匿名/坏 token=401，有效用户=200。
- `api/client.ts` 的 request 目前抛带 status 的 Error，没有在该入口把失效会话统一反馈给 AuthContext。
- 后端在 AUTH_REQUIRED=false 且 AUTH_ENABLED=true 时，无效 token 可退为 anonymous viewer，再由写权限校验返回 403。该代码路径可以解释“界面仍显示登录但写入 403、重登恢复”；尚未取得用户出错当时的网络响应和正式站配置来证明就是这次原因。
- 数量红字也可能是 409 版本冲突、校验或网络失败；数量保存已有单独 conflict 处理，不能都引导重新登录。

建议复用现有 AuthContext + 请求错误 status：401 提示“登录已失效，草稿未保存，请重新登录”；403 可先验证当前身份，身份失效才同样提示，有效身份则显示权限不足。保留草稿，不无限重试、不自动提交；登录后让用户确认重试。不要为了避免 403 放宽后端权限。

## 10. 接下来从哪里开发

不用先开发 row_version，也不用再做发布门禁。下一项是把现有 BOM 用顺，而不是重新搬 #173。

1. **输入/保存连续性**：修焦点、保留搜索、识别登录失效与保留草稿；复用现有编辑对象/重载函数/认证上下文。
2. **J5 和共享规则收口**：核实并修正 35 行品牌，报告无效 key；验证标准编辑两个入口作用域；逐国审核后再重算非人工价格。
3. **表格布局**：紧凑 Model、优先固定、列宽持久化；不在数据刷新时重置布局。
4. **完成验收清单**：#215 同模板专项样本；Add/Edit 二次输入；BOM/Matrix 双入口同步；tier、manual FOB/no-base；1400/1100px 与浏览器缩放；搜索后保存、重载保持列宽；过期登录草稿恢复。

每个实际开发任务从届时最新远端 main 新建 `codex/` branch + worktree + PR；不要在混合观察区写代码，不复用 #173/#215/#220 分支。不要为了凑一个大 PR 把认证、价格、布局连成整页重写。允许按用户最痛的问题先选一个小范围收口。

预计责任文件（是方向估计，不是已实施 diff）：

| 范围 | 复用位置 | 预估生产改动 |
|---|---|---|
| 焦点/查库等待/搜索保持 | frontend/src/pages/OrderGeniusPage.tsx | 约 20–70 行修改，另加针对性测试 |
| Model 布局/列宽 | frontend/src/components/OrderGeniusGrid.tsx、现有样式/测试 | 约 50–120 行，避免新表格框架 |
| 失效登录提示 | frontend/src/api/client.ts、contexts/AuthContext.tsx、现有提示组件 | 约 40–100 行，待确认错误传播入口 |
| 空品牌/规则统计/价格核对 | material_master_parser.py、ordering_normalization.py、order_genius_repository.py、现有 metadata/重算路由 | 已有能力优先；先列精确数据修正范围，生产代码行数待追溯入口后确定 |

上述路径均相对 `06_AppPlatform/`。实施前重新报告根因、负责函数、具体文件与行数；目前没有理由建设永久恢复系统或第二套颜色库。

## 11. 主要源码定位

以本次读取基线为准，未来修改后行号可能变化：

- `frontend/src/components/OrderGeniusGrid.tsx:456` getModelColumnWidth；`:544` columnDefs 依赖。
- `frontend/src/pages/OrderGeniusPage.tsx:3552` load；`:3640` scheduleLoad；`:4063` FOB 保存后重载。
- 同文件 `:3751` / `:3794` Add/Edit 查库，`:5194` / `:5203` focus effects，`:5330` 色卡入口，`:5397` 色卡蓝边；`:4817` 标准色卡保存。
- `frontend/src/utils/orderGeniusBomAdmin.ts:1` 行编辑 scope key；对应 `tests/unit/orderGeniusBomAdmin.test.ts`。
- `backend/app/infra/order_genius_repository.py:129` 规则 key；`:1279` 规则汇总；`:1569` Apply；`:1629` 标准更新；`:2430` surcharge lookup；`:2499` 重算。
- `backend/app/services/material_master_parser.py:527` 已有品牌后处理；`backend/app/core/security.py:69` 认证分支。

关联：[平台交接](../Fullstack/JATO_PLATFORM_HANDOVER_2026-09-14.md)、[颜色 Goal](BOM_COLOUR_RULES_UNIFICATION_GOAL_2026-08-09.md)、[Candidate Goal](../Fullstack/04_DevOps/SIMPLE_CANDIDATE_RELEASE_V2_2026-08-06.md)。本文件是本轮验收证据入口，不能把历史 Goal 的未勾选项直接解释成代码未实现。

## 12. 执行任务书：按这里改，不重新设计系统

### 12.1 开工约定与第一批范围

本节为下一轮实施准备，不表示已经实施或已授权正式站数据修正/发布。

1. 读取本文件、当前项目 AGENTS.md 和 TypeScript skill；按最新远端 main 核对下列函数是否仍存在。行号只作定位提示，以函数语义和实际 diff 为准。
2. 原混合区只读；本 handover worktree 只承载文档。本文件尚未提交，不能假定新 worktree 自带它。开工时显式读取这个绝对路径：`/Users/litristan/Downloads/JATO_Analysis_System_handover_20260914/Markdown_Readme/features/BOM_ADMIN_CANDIDATE_ACCEPTANCE_2026-09-15.md`。
3. A 已有 `codex/bom-input-search-continuity` / `260f839a`，先在其现有 worktree 续接并核对最新 main 差异；不要重新创建同名分支。B/C/D 新任务才从届时最新 main 建独立 worktree/branch。不要改用旧 #173/#215/#220 分支。
4. 第一批 A 已有两个本地提交，按第 13 节记录补未验场景。后续按 B → C（包含 E）→ D 分批推进；A 尚未部署不阻塞后续本地开发。每批保持明确业务范围。
5. 写代码前报告根因、现有负责函数、具体文件、预计 diff；优先修改现有逻辑，明确没有另建系统的必要。前端描述用户看到的交互，后端描述现有 FastAPI/SQLAlchemy/PostgreSQL 链路与复用函数。
6. 构建前查磁盘与依赖；不默认 npm install/npm ci 或建 venv。沿用项目允许的已验证依赖。只 stage 当前 PR 文件；不提交临时依赖链接、浏览器产物、认证信息或别的任务改动。

### A. 输入与搜索连续性（已证实根因，可以直接实施）

主要文件：`OrderGeniusPage.tsx`，现有 `orderGeniusColourRulesPage.test.ts` / `orderGeniusColourRules.test.ts`，必要时同目录新增一个针对性的交互测试文件。

**A1 — 只在打开/切换目标时聚焦：**

- 修改依赖整个 `colourCodeEditor` / `addColourEditor` 的两个 effect。Edit 依赖稳定的原始 materialCode 或明确的目标身份；Add 依赖打开状态与稳定的模板/车型/版本/tier 身份。实际字段先读类型，不能拿“正在输入的 colourCode”当弹窗身份。
- null → 对象、切换目标才允许 focus/select；对象内 name/code/hex/touched/saving 变化都不允许重新 focus/select。
- 可直接在现有打开入口安排一次聚焦，或用稳定身份 effect 二选一；不要两种并存。保留已有 requestAnimationFrame 清理，关闭后不聚焦已卸载节点。
- 不为绕过此 bug 把输入改成不可控 defaultValue，不通过强制重挂载弹窗解决；不删正常键盘焦点样式。

**A2 — 复用查库，改等待时间，不重写查库：**

- Add/Edit 两个现有 180ms timer 改成共享的本地常量 1200ms；不影响其他保存/搜索 timer。
- 保留品牌+代码查找、requestId、当前代码检查、名称与 swatch 各自 touched 标记；关闭、换目标、改代码时确保旧响应失效。特别补查“两个不同目标恰好同品牌同代码”的旧响应，不要只比较代码。
- 名称被手工改过而 swatch 没改过时，仅允许回填 swatch；反过来也一样。异步返回不得抹掉用户已输入内容。
- 无品牌/代码不发查库；冲突不自动选标准；无规则显示提示且保持可编辑。手工 name/hex onChange 不能重新触发同代码查库或重新抢焦点。

**A3 — 重载默认保持已生效搜索：**

- 先梳理 `load` / `scheduleLoad` / debouncedSearch / Enter / Clear / cache 的现有职责，统一一个“已提交查询”来源，不新增另一套筛选状态。
- 明确函数语义：不传新查询 = 刷新当前查询；显式空字符串 = 清空查询；非空字符串 = 提交该查询。禁止用 truthy `||` 把显式空字符串变回旧查询。
- 输入还在 1.2 秒 debounce 中时，保存重载使用已生效查询；Enter 或 debounce 提交时再更新它。未保存的搜索输入不能被程序清掉。
- 修改 `scheduleLoad` 读取最新已生效查询，避免闭包捕获旧值；所有直接 `load()` 调用都遵守相同语义。Clear 必须显式 `load("")`，并同步清除输入/已生效条件。
- 保留已有 loading/pendingLoadKey 调度；保证 loading 中发生 Clear 时空字符串也是合法待执行请求，不与“没有 pending”混同。过期查询响应不得成为最后展示结果。
- 保留原有国家两字码分流与“目标国家无 FOB”提示，不把国家筛选改成普通文字搜索。保存成功继续用现有 patch 更新当前行，随后只刷新当前查询。

**A 必测（需要真实组件行为或浏览器测试，不只搜索源代码字符串）：**

- Edit/Add 打开时可聚焦；逐字输入 2–4 位 code 不出现第二字替换第一字；名称连续输入后焦点仍在名称，选择范围不变。
- 1199ms 尚未查库，1200ms 后仅最后有效代码发起；回填后再改名称/hex，延迟返回不覆盖；快速换代码/关窗/换目标不串值。使用 fake timer 时也处理 RAF。
- 搜索 `MH0032` → 保存 FOB → 等待重载：输入不变、API 仍含 search、结果仍匹配。同测颜色保存、metadata 保存、Apply、Copy/Adjust 重载。
- Clear 后确实全量；加载中 Clear、快速 A→B、国家码搜索也正确；失败保存保留输入。
- 不为制造测试证据给正式站写 FOB。浏览器写入仅 Candidate，记录材料号、国家、前后值及清理情况。

### B. Model 紧凑布局与列宽（独立任务）

主要文件：`OrderGeniusGrid.tsx`、现有 Order Genius 样式、针对 Grid 的测试。

1. 将 Model 首次默认宽度设为约 280px，合理最小宽度约 180–220px；不再用整段 groupMeta 决定宽度。分组主标题保留，附加说明用截断/tooltip，不能把展开按钮挤掉。
2. 先检查锁定的 AG Grid 版本及现有 ColumnState API。优先只固定 Model（以及当前功能必要的 PI 选择列），其余信息列保持稳定顺序并横向滚动；这样比宽屏固定一批、窄屏反复搬列更直接。若保留其他固定列，必须明确解除固定的优先级，Model 最后也不能跑到右侧。
3. 用 initialWidth 或该版本支持的初始化语义替代反复施加 width；稳定 colId。不要在 rows/数量变化或 onGridSizeChanged 中 sizeColumnsToFit、autoSizeAllColumns、resetColumnState。
4. 复用 Grid 的用户 resize 事件，只在拖动完成时保存有限的 `{colId: width}` 映射；按用户隔离本地偏好，校验有限数值、合理范围及当前有效 colId。读写 localStorage 失败不影响业务。
5. 不保存/恢复自动解除固定后的 order/pinned；同一月份字段使用稳定 colId，显隐列恢复宽度。不要把订单行、价格或 token 放入列宽偏好。
6. 显式“重置列宽”清除该偏好并恢复默认；现有 Refresh 只刷新数据。若已有同等入口，复用而不重复新增。

验收：2200/1400/1100px，浏览器 80/100/125% 缩放，横向滚动、组展开、数量保存、Refresh、页面 reload、切月份/显隐列均不搬 Model、不覆盖手调宽度；PI 和汇总行仍对齐。极窄设备至少保持 Model 可见及其余区域可滚动，不为了固定宽度把操作区压成 0。

### C. 共享颜色、通用新增定价与特殊色规则（含 J5 品牌修复）

**C1 — 数据核实，不全表修复：**

- 重新获取 Candidate 的当前样本；35 是本轮快照计数，不写成程序常量。导出精确 materialCode、brand、model、来源信息、tier、各国 FOB/source/base/surcharge。
- 对 ICE/HEV 的空品牌提出 JAECOO 修正清单；不要把所有空品牌都猜成 JAECOO。现有 parser 已会识别 JAECOO，先找到导入/复制/metadata 的实际写入口；没有复现入口 bug 就不改 parser。
- Candidate 可用现有 metadata API 做精确验证；正式站数据修正另行取得用户批准。禁止重新上传全表、重建数据库或以新 Candidate 数据覆盖 Active。

**C2 — 规则不能静默漏行：**

- 在已有规则汇总响应中增加无效身份 SKU 的计数/有限样本或复用已有问题摘要；不要为无品牌 SKU 伪造可共享的 key。
- 前端同一 Colour Swatch Rules 卡显示“有 N 个 SKU 缺品牌/代码，未纳入规则”；complete/clean 的文案必须考虑该项。
- 保留现有五类规则状态和 Preview fingerprint；缺身份不进入 Apply 自动填充。同步已有 API 类型与测试。

**C3 — 两个色卡入口统一作用域：**

- 复用 `set_standard_colour_hex_for_rule` 作为“修改共享标准名称/色卡”的唯一写入逻辑；弹窗明确提示同品牌同代码影响多少 active SKU。
- Add/Edit 继续调用 `resolve_colour_attributes`；输入时的二次修改先作为表单草稿，不能边打字边更新全局。
- 保存时区分“当前 SKU 代码迁移”与“确认共享标准改变”。单纯新增/改 code 不覆盖旧 code 的规则；手工标准与库冲突时展示当前值/新值与作用范围，不能静默变成另一份 SKU 私有色卡。
- 若需要从 Add/Edit 同时保存标准，优先在现有后端事务中复用标准函数；不要前端连续两个写请求导致一半成功。是否需要扩展现有请求字段由实际接口决定，别先造通用控制面。
- 修改前确认当前 Edit 名称/色卡的业务语义与本段一致；若仍有跨品牌共用或历史 SKU 自动跟随等新要求，单独问清，不扩大默认范围。

**C4 — 加价复用现有重算，不批量盲加：**

- 修正品牌后重新查询适用规则；当前 JAECOO +300，若要 +200 需明确业务决定，不能据截图口述偷偷改配置。
- 复用 `reprice_sku_colour_surcharge_fobs`，确认调用覆盖相关实际入口。缺品牌/规则必须报告“无法定价”，不能把未命中与明确 0 加价混同；明确 0 规则合法。
- 逐国检查同模板/内饰等现有匹配条件下的可信基准；不得取任意车型最低价格补基准。按第 13 节最新澄清保护人工基准、重算派生颜色；不能仅凭 manual source 冻结模板编辑生成的颜色行。missing-base 保留并给原因；已有 surcharge 不能重复叠加。
- 色卡标准变更不应自动改变 tier 或触发改价。价格重算、品牌修正、标准修改分别给出结果，不把三件事塞进 Refresh。

验收：J5 ICE/HEV 品牌修正样本色卡可打开且被统计；跨两个材料同代码的标准同步；冲突不静默吞掉；dual 基准 15000 + 规则 300 = 15300，重复执行不变；manual 有可信基准参与重算，缺基准/歧义不写；规则明确 0 与规则缺失有不同结果。价格测试用可恢复的 Candidate 测试样本，记录操作影响。

**C5 — 复用已有特殊价规则，接入颜色管理页面：**

- 在现有 Colour Surcharges 品牌 Dual/Special 默认表下增加“特殊色加价”列表，支持新增、编辑、移除特例。字段为品牌、车型（可选“该品牌全部车型”）、色码/名称、适用档位（Dual/Special）、加价 EUR；默认折叠在 Colour Surcharges 下方；从现有物料/颜色库选择，名称仅辅助识别，匹配使用品牌、车型和色码。
- Special 色卡入口增加“设置加价”，自动带入当前品牌、车型、色码，打开同一规则编辑交互，不建立另一份私有规则。名称/swatch 仍共享，价格可限定车型；单纯编辑色卡不改变 tier 或价格。
- 定价先看已保存档位：Single 固定＋0；Dual/Special 在同档位内按品牌＋车型＋色码 → 品牌＋色码 → 品牌该档位默认取一个金额。Matte gray 的 Special 与独立模板 Matte black 的 Single 分开处理，不根据名称改档。定制替代默认而不叠加；规则改动不改变基准。有可信基准的 manual/copy/import 都重算，不永久冻结导入最终价。
- 规则 0 合法，代表免加价；删除/停用特例才回退下一级。规则完全缺失必须返回可识别的未配置结果，不能当作明确 0。
- 复用 `SpecialColourSurchargeRule`（`app/db/models.py`）和 repository 中 `get_special_colour_surcharge_for_sku`、`get_colour_surcharge_amount_for_sku`、`upsert_special_colour_surcharge`。现有字段/索引只支持原 Special 范围；支持 Dual 必须最小扩展适用档位与约束，旧规则迁移为 Special，不新建平行表；保留车型精确优先和零金额命中逻辑。
- 复用 `GET/PATCH /special-colour-surcharges`、前端 `getOrderGeniusSpecialColourSurcharges` / `updateOrderGeniusSpecialColourSurcharge` 与现有 types。新增最小停用操作，按规则 ID 精确处理，复用 `is_active`；停用后按剩余规则重算适用的自动价格，不删除历史价格记录。

**C6 — 实际命中规则、价格预览与保存反馈：**

- 当前 `renderColourChip` 只通过品牌默认计算 tooltip，改为展示后端统一解析的规则金额与来源，如“Special · +300 EUR · OMODA9 颜色特例”。区分规则应加金额与某国家实际 FOB；缺基准/歧义/缺规则时不能暗示价格已经更新。
- 当前 PATCH 会立即保存并重算，没有价格预览。扩展现有计算/重算函数，使预览与应用共享同一算法；预览不能写规则、FOB 或历史。展示受影响物料、国家、旧价、新价、基准、加价及 缺基准/歧义/缺档位/规则缺失原因，再由用户确认应用。应用时重新读取当前数据计算，数据变化须反馈，不盲写前端旧预览值，不另建通用审批系统。
- 现有规则保存只显示 Saved，并忽略 reprice 返回值；需接收明细/汇总，显示 updated/unchanged/skipped，成功后刷新 BOM、Matrix 与规则数据，保留搜索条件。只提交用户修改的规则，避免无关默认规则重复重算。
- 移除特例同样预览回退效果；品牌级特例变更不能压过仍存在的车型特例。

**C7 — 新增颜色一次初始化正确价格，并统一其他自动路径：**

- 在现有 `create_material_sku` 后端事务中创建 SKU 并初始化各国自动价格；前端传目标 tier 与来源物料/所需上下文，后端验证来源范围。取消新增颜色中“先重算空价格，再逐国手动写 FOB”的调用链。异常整体回滚；合法缺基准国家按明确策略跳过并返回原因，其余国家正常生成。
- 自动价格从同 BOM 模板、同国家的可信 Single 基准（付款条件仅参考，不参与计算）计算；核查现有匹配是否足够，不取无关车型最低价。使用统一规则解析函数，不在前端重写优先级，不直接复制来源双色最终价后再加一次。
- 正确保存 base、surcharge、final 与来源；区分人工模板基准和派生颜色最终价，不能把模板改价派生行标成不可重算的 manual_edit。复用现有来源语义，只有确实缺失时才增加最小值；不能批量重标记历史行。
- Copy Material 自动继承可信模板基准；页面手填金额作为目标模板基准，按各颜色规则派生最终价。原始来源是 Dual/Special 时不得将其最终价当作基准。导入最终价保留为来源证据，不作永久锁；有可信基准和明确档位时按统一规则重算，不能在已加价最终值上再次加价。
- 核对最新 order_genius_service 的现有统一规则解析；各导入模式不能绕过统一计算，也不能把已加价最终值当基准。去掉 uploaded_final_fob 等来源的永久重算豁免，缺基准/歧义仍阻止写入。
- 全量只读审计历史 manual/copy/import；是否可重算由可信基准、档位、规则决定，不由来源标签决定。批量写入另行授权，不能直接回填正式数据。

**C8 — 文件范围、验收与分批交付：**

- 主要文件：`06_AppPlatform/frontend/src/pages/OrderGeniusPage.tsx`（现有 BomAdminPanel）、`src/api/client.ts`、`src/types/orderGenius.ts`；后端 `app/api/routes/order_genius.py`、对应请求 schema、`app/infra/order_genius_repository.py`、`app/services/order_genius_service.py` 及相关测试。按实际实现需要拆出局部 UI 组件，不重写整页或新增定价框架。实施前报告每批预计文件和行数；本轮未给出已实施行数。
- 规则测试：车型特例覆盖不限车型特例；不限车型特例覆盖品牌 Special 默认；OMODA7/9 相同色码不同金额互不影响；0 命中、缺规则、停用回退；Matte gray Special 不叠加 Dual；Matte black 独立模板 Single 加价为 0。
- 定价测试：品牌完整的非 J5 首次新增 Dual（15000+200 或 +300）；从已有 Dual 新增不重复叠加；新增 Special 命中特例；多国家不同基准；缺基准有原因；人工模板基准在规则重算时不变、派生价格正确更新；重复计算幂等；创建失败回滚；Copy Material 和基准价导入遵守同一规则，最终价导入不二次加价。必须执行第 13 节最新澄清中的模板改价/拖动/复制/调整验收。
- 真实组件测试：选择品牌/车型/色码→预览→确认→BOM/Matrix 显示一致并保留搜索；取消预览无写入；保存失败保留输入；移除特例后正确回退。不能只断言代码字符串。
- 可分为“修复新增价格初始化”“接入特殊价页面/预览/移除”“自动定价其他入口对齐”小批次，但共享同一个后端规则解析。每批先在原业务 worktree 核对基线和未提交内容，避免覆盖 A/B/D；记录提交、实际测试和未验项。已稳定的 A/B/C/E 可独立准备 Candidate 供用户验收，不因此宣称 C5–C8 完成。

### D. 登录失效提示（先补证据，再最小实现）

1. 读取当前 `request`、`requestBlob`、AuthContext、登录路由和现有错误提示，确定是否已有通知机制；复用现有机制，不另建认证服务。
2. 用真实过期/无效凭据模拟 BOM 与 quantity 写请求；单独测试有效身份但无权限的 403、409、网络错误。不要把“重新登录能恢复”当成已经确定根因。
3. 受保护请求 401 才通知“会话失效”；登录失败本身的 401 不触发过期 banner。多个同时失败请求去重，避免十几个 banner 或循环 `/auth/me`。
4. 403 只在身份核查确认失效后提示重新登录；`/auth/me` 200 但权限不够显示权限不足，核查网络失败显示无法验证。不能全局把 403 转 401，更不能放宽 require_roles。
5. 明确保留哪些草稿：现有 quantityDrafts、正在编辑的 BOM 表单、搜索/列宽。若登录导航会卸载页面，先验证现有登录是否可在新标签完成并让当前页重验身份；若不能，给出最小恢复方案后再实现，不默默宣称内存草稿能跨导航保存。
6. 重新登录后只恢复可编辑状态，让用户确认重试；不自动提交旧草稿，不覆盖另一人已更新数据。保留现有 409 处理。

验收：token 无效时有明确 banner 和重新登录动作，未保存值可见；有效低权限用户不被要求反复登录；网络失败不清空身份；重新认证后用户可重试保存；不会产生后台重试风暴。

### E. 色卡蓝边框（可随 C3 实施）

- 位置：`OrderGeniusPage.tsx` 的 `renderColourChip`、`renderDraftColourChip`，以及现有 Order Genius 样式。先检索这些入口中硬编码的 `2px solid #3b82f6`，只修改色卡边框，不全局替换蓝色。
- 普通已存色卡用中性细边框，保留原背景颜色/双色划分；缺色卡保持虚线和 missing 提示。草稿如果确需区分，用明确文字标记，不把蓝框暗示为未保存或冲突。
- 可点击色卡保留键盘 `:focus-visible` 轮廓；必要时把内联边框移入现有 CSS 类，让焦点样式能生效。不要删除输入框的正常焦点提示；输入框反复蓝框的根因由 A 修复。
- 冲突仍由现有规则状态和文字提示表达，不以蓝边框代替。BOM 与 Matrix 保留各自必要交互，不为外观统一强行改写公共渲染器。
- 验收：普通/双色/缺色/历史行/草稿逐类查看；普通色卡无固定蓝框，Tab 聚焦仍清楚可见，点击仍打开正确编辑器，颜色值、tier 和 FOB 均不因样式修改而改变。

### 12.2 测试与交付方式

- 本前端是 Vite 项目，实际脚本见 `06_AppPlatform/frontend/package.json`：`npm run check:types`、`npm run test:unit`、`npm run build`、`npm run check:router-regression`。先运行本次影响的 Vitest 测试，再按风险运行完整脚本；不要照搬别的项目 `apps/web` / Next 构建路径。
- 后端改动使用现有可用 Python/pytest 环境，先通过文件检索定位相关 colour rules、surcharge、parser/auth 测试，再运行实际存在的测试文件；不要编造测试通过数量。
- #215 缺跨车型同模板样本：优先在组件集成测试中构造两个不同车型/版本、相同模板的行，实际点击 Edit 验证只有目标行打开；若 Candidate 建样本，再明确记录创建与清理，不能改真实模板来凑样本。
- 每批交付写：基线 SHA、变更文件、实跑命令和结果、仍未验证项、Candidate SHA、写入记录。更新本文件新增“实施记录”，不抹掉第 3 节原始验收证据。
- 未经独立授权不 merge/update-active/sync-intl/discard-candidate。需要新 Candidate 时遵循现有 PR→main→自动 prepare 流程，不自建旁路；文档也不授予正式站写入权限。
- 如果方案超出本批范围或需要整页重构，停止扩大并报告：“当前方案正在从修复 bug 演变为专项系统，已停止扩大修改。建议退回最小根因修复。”

### 12.3 可直接交给下一轮的起始指令

> 按第 13 节「2026-09-25 最终确认方案与 Luna Max 任务」修订 #236。先核对最新 origin/main、现有分支和 dirty，再分批统一基准解析与 audit/Apply、扩展按档位定制规则并折叠入口、去除付款条件定价与来源永久锁、对齐所有读写入口及测试。不要重做 A/B/C/D，不按单车型硬编码。每批更新文档和实际测试；修订后先审阅，合并、Candidate 准备及批量 Apply 等待相应明确授权，Active/www/intl 不动。

### 2026-09-25 · 两个根因修复批次（PR #235 已合并，Candidate 已准备）

用户授权先修两个根因，禁止在 Candidate 前手工逐行补色或逐行加价：

1. **已有 Dual/Special 行的最终 FOB**：`_find_colour_surcharge_base_fob` 现在优先读取同 BOM template、同国家的可信 Single `final_fob_eur`；兼容旧导入中 `colour_tier` 为空但 `exterior_color_type=single` 的行，模板没有可用 Single 时才按同车型/版本/动力回退。`reprice_sku_colour_surcharge_fobs` 增加按国家重算范围；Copy Country 完成复制后，对目标国已有 Dual/Special 行按 Single 基准 + 当前 surcharge 重算，避免复制来源国的旧最终价。带可信基准的 `manual_edit`/复制行不再被冻结；没有可信基准的明确最终价仍跳过并报告原因。
2. **共享颜色标准与双入口显示**：新增 `ordering.brand_colour_swatch_rule` 持久记录（规范化 brand + colour code 唯一、标准名称、单色/双色 HEX、active 状态），迁移 `20260925_0047`。现有标准保存、创建 SKU、显式编辑颜色 HEX 都复用同一 upsert；解析优先读持久标准，旧 SKU 汇总仅作为兼容回退。BOM Admin 与 Order Genius Matrix 都使用同一个标准 map 解析名称/色值，缺失仍明确为缺失，不生成默认灰色。

本批变更文件：`app/db/models.py`、`alembic/versions/20260925_0047_brand_colour_swatch_rule.py`、`app/infra/order_genius_repository.py`、`app/services/order_genius_service.py`、`app/api/routes/order_genius.py`、`frontend/src/api/client.ts`、`frontend/src/pages/OrderGeniusPage.tsx` 及 BOM 单元测试。原始提交：`7d2880a86fff7d6f079f5a50f294b37019b294d5`；PR [#235](https://github.com/tristan419/JATO_Analysis_System/pull/235) 已合并到 `main`，当前 Candidate 发布提交为 `abd38688d35eec9d771d8a9fc5fa599545b172eb`。没有改 Active/www/intl；Candidate 写入仅限本节记录的沙箱验收数据。

验证结果：

- 前端 TypeScript：通过；Vitest 全量 `75 files / 411 tests passed`；Vite build 通过；router regression 通过。
- 后端定向颜色/价格/迁移测试：`23 passed`；BOM 单元文件整体为 `54 passed, 5 failed`，5 项仍是最终 main 已存在的 country-column / `sync_missing_template_fobs` 基线缺口，不由本批引入。
- 新增回归明确要求：同模板 Single `19,350`、JAECOO Dual `+300` 的旧复制行重算为 `19,650`；持久 `OMODA+TE` 标准即使 SKU 自身无 HEX 也能被 lookup、BOM 和 Matrix 共用。

验收边界：Candidate 验收必须以选品表实际 FOB 为准，而不是色卡 tooltip 的规则金额：同模板 Dual/Special 的实际 FOB 应高于 Single；还要证明复制国家后按目标国 Single 基准重算、创建 TE 后刷新并在另一个模板自动回填名称与单/双色色值、BOM/Matrix 一致。Candidate 可用不等于 Active 发布。

### 2026-09-25 · PR #235 合入后的 Candidate 实测（仅 Candidate 沙箱）

Candidate：`https://candidate.ojeur.cloud`；发布提交：`abd38688d35eec9d771d8a9fc5fa599545b172eb`。本轮没有 update-active、sync-intl 或正式站写入。

**实际选品表 FOB（不是 tooltip 规则金额）：**

- OMODA9 SHS、模板 `T6480J1**LX0017`、CH：同模板 Single（BX/CM/GX）均为 `25,400`；UE 从 `dual` 改为 `special` 后由接口返回 `25,400 → 25,700`，`colourSurchargeEur=300`，`skippedManual=0`。BOM Admin 读取 `25,700`，Matrix 同样读取 `25,700`。
- OMODA9 SHS、模板 `T6480J1**LX0018`、CH：UE 从 `dual` 改为 `special` 时为 `25,900 → 26,200`（Special +300），再改回 `dual` 为 `26,200 → 26,100`（Dual +200）。恢复 Dual 后选品表实际 FOB `26,100`，仍高于同模板 Single `25,900`。
- 这证明“手动复制/手动编辑形成的旧最终价”在执行受支持的 tier 变更时，会以可信 Single 模板基准重算，而不是继续沿用旧的 `manual_edit` 最终价。部署本身不会批量回填历史行；验收操作触发后才会重算，不能把部署完成误认为历史数据已全部修正。

**共享颜色标准与 BOM/Matrix：**

- 在 Candidate 沙箱为 `OMODA + UE` 保存共享标准：名称 `Matte gray`、色值 `#8A8A8A`。接口返回 `source=persistent_rule`、`updated=2`，同步到 `T6480J1UELX0017` 和 `T6480J1UELX0018`。
- 保存后重新 lookup 返回 `status=complete`，刷新规则摘要后该规则为 `complete`；BOM Admin 和 Matrix 对两个模板均读取同一 `#8A8A8A`，没有再显示默认灰色占位。
- 当前 Candidate 快照没有 `OMODA + TE` SKU，因此不能在本轮声称完成“TE 创建后跨模板复用”；本轮用同一品牌、同一色码 UE 的两个真实模板证明了持久规则和双入口共用路径。TE 专项仍需在包含 TE 的 Candidate 数据中按“创建→刷新→另一模板输入→保存”重复一次。

**本批结论：**

- 已通过：实际 Dual/Special FOB 高于同模板 Single；Dual 与 Special 金额区分正确；BOM/Matrix 色值和 FOB 一致；共享标准保存后可复用。
- 认证抽查：Candidate 有效 `admin/admin123` 可读取上述接口；匿名请求和无效 token 均返回 `401 Authentication required`，没有把匿名请求误判成业务功能状态。
- 仍未通过/未覆盖：真实浏览器拖动分类的稳定性、TE 专项跨模板回填、Copy Country 的目标国基准专项、J5 ICE/HEV 数据样本、BOM/Matrix 完整交互回归、D 的真实失效会话与草稿保留。Candidate 可供继续验收，不等于 Active 已发布。

### 2026-09-25 · PR #236 初版审阅：未合并，必须修订

初版新增只读 audit 和 fingerprint Apply，但尚不满足[最终确认方案](#bom-pricing-final-20260925)。记录分支 codex/bom-colour-reprice-audit，worktree /Users/litristan/Downloads/JATO_Analysis_System_bom_reprice_audit；初始提交 3b2bc15b，后续审阅 HEAD 53d00351c070dffd02e16f40cf2be035b5433b94（执行前重新核对）。PR [#236](https://github.com/tristan419/JATO_Analysis_System/pull/236) 不能按原样合并。

已发现问题与作废口径：

- 基准歧义错误放行：Single 1,000/1,200 时，旧 manual base=1,000 使审计误判可改成 1,300；必须保留歧义，不回退旧行。
- audit 与 writer 分别判断，明确最终价保护不一致。新要求不是给 writer 补永久保护，而是共用计算器且不以 import/manual/copy 来源冻结派生价。
- skippedAmbiguous 初始化后未递增，0 不证明没有歧义；补真实计数与测试。
- 按付款条件审计、Apply 却按物料＋国家写入，范围不一致。修订为国家基准口径，不是给 Apply 增加付款条件过滤。
- “Matte 一律 Special”、跨模板寻找基准、按 source 排除 explicit_final 的旧设计作废；旧 Special 特例不能自动用于 Dual。
- 初版测试历史记录为定向 8 passed、完整 BOM 56 passed/5 failed，不是最新方案的验证结论；缺真实 Apply 联动测试，需补齐后重跑并核对基线失败。

下一步：按第 13 节最终方案分批修订和测试 → 审阅/获授权合并 → 现有流程 Candidate → 全量只读 audit → 用户审阅后授权 Candidate Apply。记录实际 Candidate SHA、审计分类/明细及写入范围，写后新审计应为 0 待更新；旧 fingerprint 应拒绝。不得把少数 OMODA9 样本通过说成全部历史行已修复，不执行 Active/www/intl 迁移。

### 2026-09-25 · #236 第一批修订：统一 audit/Apply 基准决策（本地已提交，待审阅）

- 代码 worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_reprice_audit`；分支 `codex/bom-colour-reprice-audit`；业务提交 `944bcc03` 已推送到现有 PR #236 分支，等待审阅。未合并、未准备 Candidate、未写入任何环境数据。
- 在现有 `order_genius_repository.py` 中新增共享 `_resolve_colour_surcharge_reprice_base()`；audit 的 `_colour_surcharge_reprice_item()` 与 writer 的 `reprice_sku_colour_surcharge_fobs()` 共用同一基准决策，消除两套来源保护逻辑。
- Single 基准只按同一 BOM template＋国家寻找；付款条件只保留参考信息，不参与选择。多个不同 Single 值保持 `ambiguous_base`，旧 manual base 不能消除歧义；没有 Single 时只有明确记录为 template/base edit 的行可使用已保存基准。
- imported final source 不再因标签永久跳过：存在可信 Single 时按当前 tier surcharge 重算；无基准时单独归类 `explicit_final`，不自动写入。`skippedAmbiguous` 已接入真实 writer 路径。
- Apply 对 audit 结果按物料＋国家去重；一个调用负责该国家下所有付款条件记录，避免重复重算。
- 新增/调整回归场景：audit/writer 决策一致、歧义 manual 不放行、明确最终价无基准单列、付款条件目标去重、legacy tier 回归等。

本批实跑结果：

- 定向 BOM 重算/audit 测试：`10 passed`。
- `tests/unit/test_ordering_bom_admin.py`：`61 passed, 5 failed`；5 个失败仍是最终 main 已有的 `list_bom_admin_country_columns` / `sync_missing_template_fobs` 基线缺口，不由本批引入。
- `python3 -m compileall -q app/infra/order_genius_repository.py`、`git diff --check` 通过。

本批未处理：按档位扩展 SpecialColourSurchargeRule/迁移及折叠 UI、国家价格重复记录只读盘点、BOM/Matrix 浏览器验收、Candidate audit/apply。下一批先审阅 `944bcc03`，再继续最小的 tier-qualified 定制规则改动；不直接合并或部署。

### 2026-09-25 · #236 第二批：定制颜色规则绑定 Dual/Special（本地已提交，待审阅）

- 业务提交 `95ba8941` 已推送到现有 PR #236 分支；未合并、未准备 Candidate、未写入环境数据。
- `SpecialColourSurchargeRule` 增加 `colour_tier`，旧记录 migration 默认迁移为 `special`；唯一约束和查询键同时包含 tier，避免同品牌/车型/色码的 Special 特例误用于 Dual。新增 migration `20260925_0048_tiered_special_colour_surcharge`，降级遇到 Dual 规则会停止并报错，不删除规则。
- `get_colour_surcharge_amount_for_sku`、upsert、按特例重算和路由/schema 均传递同一 tier。Single 不查询定制规则；Dual/Special 只命中相同档位的车型色码→品牌色码→品牌该档位默认。旧客户端不传 `colourTier` 时兼容为 Special。
- 本批仍未改前端面板；下一批接入 Colour Surcharges 下方折叠管理入口和实际命中反馈，继续复用现有 API/storage，不新建规则系统。

实跑结果：`13 passed`（tier-qualified surcharge、重算/audit 回归与 Alembic 单头检查），compileall 和 diff check 通过；完整 BOM 测试的 5 个既有基线缺口仍需单独记录。

### 2026-09-25 · #236 第三批：前端按档位管理定制加价（本地已提交，待审阅）

- 业务提交 `75758ea6` 已推送到现有 PR #236 分支；未合并、未准备 Candidate、未写入环境数据。
- 前端 API/type 现在显式传递 `colourTier`（`dual` 或 `special`）；色卡命中定制规则时要求与当前 BOM Admin 档位相同，Dual 不再误读 Special 特例，Single 永远不查定制加价。
- 原“Special Colour Overrides”入口改为颜色加价区域下方默认折叠的“Custom colour surcharge (N)”；展开后可编辑品牌、车型、色码、适用档位、名称和金额，已有规则列表也显示档位。优先级说明改为同档位内“车型＋色码 → 品牌＋色码 → 品牌当前档位默认”，不累加。
- 改动复用现有 API、规则存储和色卡渲染器，没有新增规则系统；UI 未做整页重构。

实跑结果：前端 TypeScript `tsc --noEmit` 通过；Vitest `75 files / 411 tests passed`；Vite production build 通过；`git diff --check` 通过。

本批仍未处理：重复付款条件价格的只读盘点、BOM/Matrix 浏览器验收、Candidate 只读 audit/apply、旧历史行迁移；这些必须在审阅并合入后另行按流程验证，不把本地构建当作 Candidate 通过。
