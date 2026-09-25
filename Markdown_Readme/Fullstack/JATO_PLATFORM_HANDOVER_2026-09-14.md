# JATO Platform 开发交接与恢复入口

## 2026-09-25 最新 BOM 执行入口（文档确认，待代码修订）

**Luna Max 先读 [BOM 第 13 节最终确认方案](../features/BOM_ADMIN_CANDIDATE_ACCEPTANCE_2026-09-15.md#bom-pricing-final-20260925)。#236 不可直接合并；本轮只改文档。**

- 基准是 BOM 模板＋国家；付款条件只参考。已保存 tier 决定加价；Single 永远＋0，名称不决定分类。
- OMODA7 Matte gray Special＋200；OMODA9 UE Special＋300；CP / Matte black 独立模板 Single 使用自身较高基准，不另加颜色费。
- 保留定制能力，规则绑定 Dual/Special 档位，同档位内车型色码→品牌色码→品牌默认。旧规则保留为 Special；管理入口折叠到 Colour Surcharges 下方。
- manual/copy/import 不永久锁价；可信基准＋明确档位统一派生。缺基准、歧义、缺档位/规则不写；不能以旧 manual base 消除冲突。
- audit/Apply 共用决策，修复歧义放行、空计数和付款条件审计/国家写入不一致。重复付款条件价格先只读盘点，不猜价、不直接删数据。
- 最新已记录 Candidate：#235 的 abd38688d35eec9d771d8a9fc5fa599545b172eb，仅 OMODA9/UE 部分数值与色库样本通过；JAECOO7 等历史价差仍未收口。实施前重新核对远端 main/PR。
- 下一步修订 #236 并补真实 Apply/读写联动测试，每批更新实施文档；审阅后才请求合并及 Candidate 授权，Candidate 批量重算须先审阅只读清单。Active/www/intl 不动。

**以下日期段落保留历史实现/验收事实，不作最新实施指令。**旧“付款条件参与定价”“导入最终价永久保护”“所有 Matte 是 Special”等要求已被上述口径替代；旧 SHA 不是当前版本。

本轮统一进度入口：[Order Genius 六项大任务进度](../features/ORDER_GENIUS_PROGRESS_2026-09-24.md)。选品表批次栏已确认，见日期选品/PI需求第3.1节；每批交付按该进度口径报告，已合并/部署不等于业务验收或Active发布。

## 2026-09-24 新增：PI 批次不可见、余量下单与日期选品

需求与只读源码证据已整理到 [日期选品、月内多批 PI 与 WVTA](../features/ORDER_GENIUS_DATED_SELECTION_PI_BATCH_WVTA_2026-09-24.md)。优先检查 PI 列表国家/月混合状态和请求失败被吞成空列表；现有 allocation plan 可复用为“总量/已进 PI/余量”，继续下单不清零原始订单。模板基准收口后另批增加国家日级价格期间，再接生产日期与 WVTA 匹配。尚未查正式数据库、实施业务代码或部署，不能将截图 409 视为本次 PI 创建成功。

## 2026-09-24 当前状态：四批 BOM 已合入，Candidate 浏览器验收进行中

最高优先级：BOM 含 `**` 的模板行输入是按国家维护的基准价；Single=基准+0，Dual/Special=基准+适用规则。人工基准应保留，派生颜色仍随基准、分类及规则变化重算。旧文档“保护手动最终 FOB”不能用于冻结模板编辑生成的颜色价格。

现有 `handleFobSave` 将模板下所有颜色写入同一最终价并标成 `manual_edit` 的根因，已由 C 批次修复并随 PR #229 合入；新增颜色/导入与模板基准语义现在都在最终 main。不能假定数据库存在独立 `**` 价格记录，也不能简单禁止非 `**` 请求。明确最终价导入与历史数据单独识别，不自动回填正式数据。

完整实施及数值验收见 [BOM 实施文档第 13 节“用户澄清：模板基准价与派生颜色价格”](../features/BOM_ADMIN_CANDIDATE_ACCEPTANCE_2026-09-15.md)。模板基准批次已随 C PR #229 合入；最终 main 为 `efe5ac0f5b11`，Candidate 已由 run [35960030314](https://github.com/tristan419/JATO_Analysis_System/actions/runs/35960030314) 准备成功。当前进入浏览器业务验收，Active/www/intl 未更新。

### 2026-09-24 Candidate 浏览器验收实绩（覆盖上句“登录暂时无效”）

- 使用用户提供的 Candidate 测试账号登录成功；账号信息不写入交接文档。页面显示 Candidate commit `efe5ac0f5b11`、artifact `2810a3510337`、沙箱尾号 `889b4d88`。本轮仍未执行 update-active、sync-intl 或正式数据写入。
- Colour Swatch Rules 的真实流程已走通：初始页面显示 `38 rules / 8 SKU fields can be filled`；Preview 显示 8 个确定性填充项；点击 Apply 后页面显示 `Filled 8 SKUs; 0 unchanged, 9 conflicts, 24 missing rules`，再次 Preview 为 `0 deterministic fills`。这证明 Candidate 沙箱写入链路工作，但不代表 9 个冲突或 24 个缺规则已经解决。
- Model 列在 50%、100%、150% 浏览器缩放截图中均保持最左；页面提供 `Reset order grid column widths`。本轮尚未完成拖拽列宽、刷新/数量保存后的持久化和真实窄窗口尺寸测试，因此只标记“缩放下位置通过”，不标记列宽持久化通过。
- #215 行编辑实测：JAECOO5 HEV 的 Exclusive-FWD 首个 BOM 行进入 `DONE` 编辑态，同时 Select-FWD 另一版本行仍为 `EDIT`，没有整组一起进入编辑。尚未找到可证明“同 BOM Template 跨不同车型/版本”的专门样本，不能扩大结论。
- JAECOO5 HEV 的颜色入口不再因空品牌而完全打不开：BOM 行显示规范化品牌 `JAECOO`，点击 BW 色卡可打开规则编辑并读到 `#94A3B8`。但列表仍有 24 个 `missing swatch` 规则，Matrix 也仍显示 Missing swatch；共享库/历史 SKU 同步尚未验收通过。
- OMODA9 SHS 在 Candidate 中将 `UE` 从 `dual` 移到 `special` 后，价格复核弹窗显示 `9 countries scanned · 8 updated · 1 manual FOB skipped · 0 missing Single base`；AT、RO、BG、CZ、HR、GR、HU、SK 均按 Special `+300 EUR` 重算，只有 CH 保持 `25,400 → 25,400 · surcharge 200 · manual_fob`。这证明车型＋色码的 Special `+300` 规则和优先级已经生效，未更新的是被 `manual_edit` 标记的瑞士行。
- 本次确认的业务语义是：BOM Admin 中手动 Copy 和 Manual Edit 都是在修改国家基准 FOB，不是锁定某个颜色的最终 FOB；最终价始终是 `base_fob_eur + 当前 tier surcharge`。真正的车型＋品牌＋色码特殊价优先于品牌色码特殊价和品牌 Special 默认，Matte 只走 Special、不叠加 Dual。
- 当前剩余根因是 `manual_edit` 同时表示“人工改过基准”和“不可重算的最终价”。重算函数在读取 `base_fob_eur` 前直接跳过该行，所以复制后再手动调整的 CH 不能跟随 Special 规则。下一批只需收窄保护：有可信基准的手动行允许重算；没有基准的明确最终价继续报告并保护，不批量回填历史正式数据。
- 最小修复已在独立分支 `codex/bom-manual-base-reprice` 实现，提交 `8ae4a0c7`；当前只通过本地测试，尚未创建 PR、合入 main 或重新准备 Candidate。该分支不改变 Active/www/intl，也不回填历史正式数据。
- 真实低权限 403、过期 token、断网重登和数量/BOM 草稿跨标签保留仍未执行；本轮只有管理员登录成功，不把 D 标成浏览器全验收通过。

### 2026-09-24 模板基准批次实绩（已合入，Candidate 待数值验收）

- C worktree `/Users/litristan/Downloads/JATO_Analysis_System_bom_colour`、分支 `codex/bom-colour-followup` 已修正 `handleFobSave(allCodes)` 的根因：`**` 模板行按国家保存 `base_fob_eur`，后端同事务按 Single/Dual/Special 规则派生最终价，不再把同一输入逐颜色写成 `manual_edit`。
- 同批覆盖模板读取、拖动 tier 后重算、最后一个 Single 移走保留基准、Copy Material、模板批量调价、国家复制/调整和前端本地派生结果；普通非 `**` SKU 仍走原逐 SKU 最终价路径，明确最终价导入保持不二次加价。
- 本地验证：模板/规则聚焦 6 项通过；`test_ordering_bom_admin.py` 为 `46 passed, 5 failed`，5 项均为该分支此前已知的 country-column / `sync_missing_template_fobs` 基线缺口；Python compileall 通过；前端类型检查、73 files / 399 tests、构建、路由回归通过。构建只有既有大 chunk warning。
- 未完成：尚未在 Candidate 实测 15000→15500→15800、多国家基准、Special 特例、无基准/manual 保护、BOM/Matrix 双入口、Copy/拖动交互。Candidate 已准备但 `candidateAdmin` 测试账号返回 `Invalid credentials`，需先取得正确测试凭据。Hermes 自动事件文件继续保持 dirty，不纳入业务提交。

## 2026-09-24 更新：颜色定价需求与收口状态（代码已合入，业务验收待登录）

本节覆盖下方“C/D 已完成、只需合并”的旧结论。C/B/A/D 已按 C → B → A → D 合入最终 main 并准备一次 Candidate；以下颜色规则与模板代码已部署到 Candidate，但尚未完成浏览器业务验收。本轮没有写 Active/www/intl 或正式业务数据。

- 新增双色不加价不限于 J5：旧流程先在无 FOB 时重算，再复制来源最终价并落为 manual_edit，后续加价又被手动保护跳过。C7 现有四个业务提交已在 `codex/bom-colour-followup` 修正新增颜色、导入和模板入口：后端按同模板/国家/付款条件 Single 基准＋统一规则初始化，模板编辑/Copy Material/批量调价/国家复制调整也保存 base/surcharge 元数据；明确最终价不伪装成 Single 基准。C5/C6 的预览、应用和停用回退仍待后续。
- 用户确认品牌默认 Dual：OMODA +200 EUR、JAECOO +300 EUR；Matte 是 Special，不叠加 Dual。特例 OMODA7 Matte +200、OMODA9 Matte +300，由用户在页面指定品牌、色码和可选车型维护，不硬编码。
- 特殊价表、API 和后端匹配已存在；本批已在现有 Colour Surcharges 接入列表/编辑并在 Special 色卡 tooltip 展示实际命中来源。优先级为车型色码特例→品牌色码特例→品牌 Special 默认；0 表示免加价，停用才回退。真正的预览→应用、停用和完整回退明细仍待后续批次。详见 [BOM 实施文档 C5–C8](../features/BOM_ADMIN_CANDIDATE_ACCEPTANCE_2026-09-15.md)。
- D@34656898 已修复新标签登录误报、旧核验响应覆盖和 401/403/网络错误分类；真实浏览器的低权限 403、过期 token、网络断开及重登前后数量/BOM 草稿仍待验收。历史 403 个测试通过不能替代这些验收。该代码提交日期为 9 月 15 日，9 月 23 日是文档更新日期。
- Candidate 实际身份、部署报告和登录阻塞见 BOM 验收文档第 13 节；取得正确凭据后统一验收 A/B/C/D 组合，不把 Candidate 验收等同于 Active 发布。
- 最新文档位于 handover worktree；上次远端核对 #227 尚未包含本地修订，后续应重新核对远端，不能直接合并旧版。

### C7 第三批实绩（2026-09-24；记录时未合入，后续已随 PR #229 合入）

- C worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_colour`，分支 `codex/bom-colour-followup`，业务提交 `83a45c63`；后续随 PR #229 合入并部署到当前 Candidate，未更新 Active/www/intl。
- 已改：模板 `PATCH /order-genius/bom-templates/fob`、`base_fob_eur` 派生保存与历史记录；模板编辑、批量调价、Copy Material、国家复制/调整遵守基准＋颜色规则；tier 重算不冻结模板派生价。
- 已验：后端聚焦 6 passed，完整文件 46 passed / 5 known baseline failures；前端 73 files / 399 tests、类型、构建、路由回归通过。Candidate 数值与双入口浏览器验收仍待进行。

### C7 第一批实绩（2026-09-24；记录时未合入，后续已随 PR #229 合入）

- C worktree：`/Users/litristan/Downloads/JATO_Analysis_System_bom_colour`，分支 `codex/bom-colour-followup`，业务提交 `eb10c0ead4a49e4d7f4152dabb63d04143b0676e`、`be2b85eb8be8b01979b3ee4672c3e03f4a052575`、`282cd1218dd4235ca40392f3c91387e66aa0cad8`；基线 `579ea01e`。后续随 PR #229 合入并部署到当前 Candidate，未更新 Active/www/intl。
- 已改：`create_material_sku` 的自动 FOB 初始化、同模板 Single 基准和统一 surcharge 解析；Add Colour 移除旧的逐国 manual 复制；导入 `uploaded_base_plus_colour` 复用同一解析并保存 base/surcharge；现有 Special surcharge 列表/编辑 UI 与来源 tooltip 初接；对应后端/前端测试。
- 已验：后端新增用例 `4 passed`；后端该文件总计 `43 passed, 5 failed`，5 项均为此前已知的 `list_bom_admin_country_columns` / `sync_missing_template_fobs` 基线缺口；前端类型检查、73 files / 399 tests、构建、路由回归均通过。
- 未验：Candidate 浏览器、普通 Dual 与 OMODA7/9 Matte 特例、BOM/Matrix 双入口一致、manual/no-base、多国家基准、C5/C6 预览/应用/停用、Copy Material 和其他自动入口。保留 Hermes 自动事件文件 dirty，不纳入业务提交。

## 2026-09-15 更新：历史快照（已被上方 2026-09-24 状态覆盖）

此节取代下文 9 月 14 日的 Candidate 待切换结论；其他业务线仍是原日期快照，未在本轮重新盘点。

- #226 / #228 已合并；新 Candidate 页面实测 commit `9011da6ac1f5`、artifact `f55a70fea05f`，沙箱尾号 `5b654a45`，快照开始于 2026-09-15 09:13:10。
- 四批 BOM PR 已按 C → B → A → D 合入；最终 main 为 `efe5ac0f5b11f547ca94942bd2a0ad0708822bb0`。自动 Candidate prepare run [35960030314](https://github.com/tristan419/JATO_Analysis_System/actions/runs/35960030314) 成功，Candidate 当前构件 archive `2810a3510337`、manifest `c7a49e45c7b4`，Active/www/intl 未更新。
- Candidate 部署报告已确认沙箱/数据库隔离、Candidate backend、月更禁用、preview 和 `active_unchanged`；但本次浏览器使用此前约定的 `candidateAdmin` 测试账号返回 `Invalid credentials`，因此不要把本轮写成“登录成功”或“BOM 验收通过”。取得正确测试凭据后再继续统一验收，不要再次按旧步骤 discard 或合并 #226/#228。
- BOM #220 已完成一次真实 Preview/Apply：仅 Candidate 的 8 行颜色名称被填充，前后 BOM API 比较 FOB/tier 等其他返回字段未变。完整验收尚未完成，不能标记可发布。
- 新发现：J5 ICE 21 行、HEV 14 行品牌为空，联动影响色卡、规则统计和加价；Model 窄屏移位、输入抢焦点已复现；搜索保存后的无条件 reload 已定位。
- B Model、C 颜色规则和 D 登录失效提示均已在后续 PR #229–#232 合入并部署到当前 Candidate；本节列出的 `main@9011da6a`、未合并和旧 Candidate 版本仅是 9 月 15 日历史快照，不能当作当前状态。
- 本地衔接批次已实跑：D `npm run check:types`、全量 Vitest（73 files / 403 tests）、`npm run build`、`npm run check:router-regression` 均通过；构建仅有既有大 chunk warning。尚未做真实浏览器的有效低权限 403、过期 token、网络断开及新标签登录前后草稿验收。
- 后续不再重复审阅/合并 B/C/D；取得正确 Candidate 凭据后直接做一次统一浏览器验收。不优先扩展多人编辑 row_version，不新增门禁。
- 完整证据、沙箱写入清单和 TODO：[BOM Candidate 验收与问题梳理](../features/BOM_ADMIN_CANDIDATE_ACCEPTANCE_2026-09-15.md)。本轮没有更新 Active/www/intl，也没有合并文档 PR。

## 以下为 2026-09-14 历史盘点（Candidate 当前状态以上节为准）

> 状态快照：2026-09-14（Asia/Shanghai），已按二次实证审计修订，替代本文初稿判断。
>
> 目的：长时间暂停开发后，从一个可信入口重新判断“已经完成什么、什么仍在搁置、下一步从哪里开始”。
>
> 本文不是永久真相。开始任何新任务前，必须重新读取远端 `main`、开放 PR、目标 worktree 状态和线上构件版本。
>
> 本次证据范围：Git 文件差异、GitHub PR/规则/发布记录、可读取的历史任务和公开前端构件；未登录核验线上后端/数据库，未重跑历史全量测试，也未逐 hunk 证明混合区迁移完成。

## 1. 先看结论

当前项目并不是所有功能都停在半成品状态，而是四类状态混在一起：

1. BOM Admin、COC、MarketScan、Hermes 主体已经进入 `main`；
2. Config、AstrBot 有大量真实成果，但远端 PR 与本地未提交进展已经分叉；
3. MSRP 的安全与治理主干已经合并，剩余失败分类和真实来源覆盖工作；
4. Candidate 登录切换在本次历史盘点时未完成，现已于 9 月 15 日进入业务验收，见页首更新。

推荐个人执行顺序：利用现已可用的 Candidate 验收并修复具体 BOM 问题，从最新 `main` 继续开发。Config、AstrBot、MSRP 的选择随后按产品优先级决定，不构成全项目串行依赖。

Candidate 影响在线验收，不是其他功能本地开发的硬前置。先做它是为了便于用户看见和验证成果，不是要求重建部署系统或增加门禁。

当前正式站前端仍是 7 月 23 日版本，BOM #215/#220 已进入 8 月 10 日的 main，却尚未进入这两个正式站构件。现有 AstrBot #157 明确要求先合并 Config #179；后续 JATO 页面 MCP 成果可以先整理和验证，但不能据此跳过旧 PR 的合并约束。具体待办见第 15 节。

## 2. 恢复开发时的第一轮只读核验

不要直接相信本文记录的 SHA 和 ahead/behind 数字。下一次开始时先完成：

```bash
git -C /Users/litristan/Downloads/JATO_Analysis_System \
  -c http.proxy= -c https.proxy= fetch JATO_Analysis_System main

git -C /Users/litristan/Downloads/JATO_Analysis_System \
  rev-parse JATO_Analysis_System/main

gh pr list \
  --repo tristan419/JATO_Analysis_System \
  --state open \
  --limit 100

git -C /Users/litristan/Downloads/JATO_Analysis_System worktree list
```

随后把 `TARGET_WORKTREE` 替换成明确的业务 worktree 绝对路径，再只读检查；不要在混合区误跑目标分支命令：

```bash
git -C TARGET_WORKTREE branch --show-current
git -C TARGET_WORKTREE status --short --untracked-files=all
git -C TARGET_WORKTREE rev-parse HEAD
git -C TARGET_WORKTREE rev-list --left-right --count JATO_Analysis_System/main...HEAD
```

原始目录 `/Users/litristan/Downloads/JATO_Analysis_System` 是混合观察区，不是新开发目录。

## 3. 截至本次交接的基线

- 远端 `main`：`f29cf5096b528e2c0350047f2bc462cc8bfc8696`
- 最后一次 `main` 提交时间：2026-08-10
- 最后合并 PR：[#224](https://github.com/tristan419/JATO_Analysis_System/pull/224)，自动准备 current-main Candidate
- 当前开放 PR：12 个，全部为 Draft，包含本交接文档 #227
- 本机磁盘可用空间：约 235 GiB，不构成恢复阻塞

线上只读检查结果：

- [www build-meta](https://www.ojeur.cloud/build-meta.json) 与 [intl build-meta](https://intl.ojeur.cloud/build-meta.json) 均指向 `cd4557cb932374a0fefb6c80a5fac9fb75a67d62`，构建于 2026-07-23；www 初次 DNS 超时后的重试已成功；
- `candidate.ojeur.cloud` 返回 Nginx Basic Auth `401`，未取得当前内部构件及运行环境；
- 因此不能把“代码已在 main”自动等同于“当前线上已运行”。

交接入口尚未进入 main：本文件及本轮关联文档修订位于 `codex/jato-platform-handover-20260914` / [Draft #227](https://github.com/tristan419/JATO_Analysis_System/pull/227)。合并前从 main 新建 worktree 不会带入本交接文件，应显式读取本地 handover worktree。原混合区 `AGENTS.md` 也是未跟踪文件，不能假设它会随新 worktree 自动复制。本次未合并 PR 或触发部署。

## 4. Candidate / 部署：历史过程，勿重放旧切换顺序

### 9 月 14 日历史进展（#226/#228 后续已合并）

PR：[#226 fix(candidate): require application login](https://github.com/tristan419/JATO_Analysis_System/pull/226)

- 分支：`codex/candidate-app-login`
- 相对本次 `main`：0 behind
- required GitHub CI 全部通过；
- 唯一红项是非 required 的 Cloudflare Pages preview；
- 本地产品代码已 clean，只剩一条提交钩子生成的 Hermes event 未提交。
- 远端 head `5f73caff` 与本地 `c4cc388b` 的 tree 都是 `d5376f21d341eec02071fb00c066474daa776868`；SHA 不同不代表遗漏代码，后续不要盲目重复提交或强推。

已实现：

- Candidate 复用现有 OJEUR `/login`，没有另建登录系统；
- Candidate 使用独立沙箱用户数据和独立 JWT secret；
- 除 login/health 外，未认证 Candidate API 严格拒绝；
- Candidate 禁止接收 Active OAuth callback 凭据；
- 前端测试、TypeScript、构建和后端相关测试已经通过。

### 为什么停下

Candidate 并非从未搭好：2026-08-10 的 [自动 prepare run 31372523132](https://github.com/tristan419/JATO_Analysis_System/actions/runs/31372523132) 已成功。随后 #226 处理旧 Basic Auth 弹窗与应用匿名身份问题。

`jato月更` 任务于 2026-08-11 在 required CI 已通过、继续检查 Cloudflare 分支预览失败时因额度限制中断。Cloudflare 只返回 `Deploy failed`，详细原因尚未取得；它不在 main 的 required checks 列表中。

旧 `APP_AUTH_ENABLED=false` 来自 #226 当时的运行记录，不是本次登录服务器复查的结果。本次只确认 Basic Auth 外层仍存在。下一次执行前先重读 live 状态，再按 #226 已有顺序收口；以下操作不是本文授予的部署权限：

1. 保留现有 Basic Auth；
2. 取得明确授权后废弃旧 Candidate，确认 Active/www 不受影响；
3. 另行获准合并 #226，由 current `main` 自动重建严格认证 Candidate；
4. 在 Basic Auth 仍存在时验证真实 OJEUR 登录、401 和沙箱隔离；
5. 最后原子移除 Candidate vhost 的 Basic Auth。

### 当前恢复建议

不要重放本节历史切换步骤。新 Candidate 已可登录，#228 已解决认证开启后旧匿名 monthly 探针期待 423 的不匹配；继续页首链接中的 BOM 验收。Cloudflare 旧分支预览日志的调查不等于 Candidate 仍不可用。更新 Active/同步 intl 仍须分别授权。

[#225](https://github.com/tristan419/JATO_Analysis_System/pull/225) 的自动 Candidate 证据已经被 #226 吸收，原则上不应单独继续合并。

## 5. BOM Admin：最适合恢复新产品开发

### 已进入 main

- [#215](https://github.com/tristan419/JATO_Analysis_System/pull/215)：相同 BOM Template 只编辑当前车型/版本行；
- [#218](https://github.com/tristan419/JATO_Analysis_System/pull/218)：颜色规则统一目标；
- [#220](https://github.com/tristan419/JATO_Analysis_System/pull/220)：颜色规则与 swatch 统一实现；
- 更早的 Copy SKU、Copy Country、Batch Material、颜色 surcharge、Father Note、CBU Finance 已进入 `main`。

### 当前状态

- 没有开放的 BOM PR；
- 当前有两个未发布的本地 BOM 分支：`codex/bom-model-layout`（`53976fc0`）和 `codex/bom-colour-followup`（`2218c0df`）；它们应分别审阅/提交 PR，不要在旧 #173/#215/#220 分支上继续开发；
- `/Users/litristan/Downloads/JATO_Analysis_System_bom_admin_current` 为 clean；
- 本地 `58ce8b9f` 与 #215 已合并 head `554639ac` 文件树完全一致；这个旧 worktree 不是最新 main，也没有另一份独有的后续功能；
- 旧 BOM worktree/branch 只作为事故与迁移审计源；
- 不需要再次迁移 #173 或整支合并旧 BOM branch。

### 恢复建议

先在可用 Candidate 上验收 main 已有的 #215 行编辑和 #220 颜色规则，不重新实现它们。BOM 新功能从届时最新远端 `main` 新建 worktree/branch/PR，不复用已合并分支；本地开发可以在 Candidate 收口前开始，在线验收待入口可用后进行。正式站前端仍是 `cd4557cb`，不能把“main 已合并”写成“正式站已验收/上线”。

## 6. Config 配置对比：大型已提交成果与本地后续待收口

### 远端已提交部分

PR：[#179 feat(config): govern published product evidence](https://github.com/tristan419/JATO_Analysis_System/pull/179)

- Draft、Mergeable、历史 CI 全绿；
- 67 个文件，`+87,905/-983`（包含源码、测试、样式和文档，不等于功能数量）；
- 截至本次审计落后 `main` 约 135 个提交；
- worktree：`/Users/litristan/Downloads/JATO_Analysis_System_config_compare_migration`。

PR 中已经实现：

- 配置文件上传、原生文本提取与 OCR fallback；
- Digest、审核、发布、immutable snapshot；
- Config Comparison workspace；
- 简易/专家模式、差异筛选、证据抽屉、导出与 AI 摘要；
- 两个只读 Product Evidence MCP：`search_product_evidence` 和 `compare_published_product_configs`；
- Config Alembic `0047 → 0048`；
- 完整后端、前端和迁移测试。
- Published 对比列身份按选中的 ConfigVersion 读取、身份编辑只同步 Draft：已存在于 #179 的 route/测试，不属于未提交 follow-up。

### 本地未提交的后续进展

当前还有 9 个 tracked 修改 `+1606/-32`，另有一个未跟踪新 service `published_snapshot_augmentation_service.py` 共 745 行；后者不包含在前面的 diff 数字中。这批后续不在 #179 中：

- 从 Published snapshot 创建经审核的 augmentation child draft；
- Product Evidence schema preflight；
- schema 未部署时明确返回 `schema_unavailable`；
- 更完整的 Digest、Evidence 和 API 回归测试。

### 为什么停下

- 可读取的 `配置对比功能` 任务最后于 2026-07-23 网络断流；当时的共享入口阻塞记录早于 #179 后续更新，不能当作今天仍未解除的完整阻塞清单。
- 当前确实未收口的是 #179 已提交版本与上述 10 文件后续；不能仅凭未跑完的旧任务推断全部功能状态。
- #179 改动路径中，main 自分叉点以来只修改了 3 个：Alembic 链测试、API client、AuthContext。GitHub 为 `MERGEABLE / behind`；main 规则要求更新基线后检查，但没有“因落后必须重开 PR”的规则。
- Config 与待合并 #226 另有 4 个重叠路径：config.py、security.py、test_security.py、AuthContext；后续对齐时需要同时保留 Config actor mapping 与 Candidate 严格认证。

### 恢复建议

先保护并逐项确认本地 10 文件，再决定纳入当前范围或拆成后续 PR；更新现有 #179 基线是有效选项，只有实际范围/冲突需要时才重建。更新前不得丢弃未提交成果，不因 behind 数字一律重做迁移。

现有 #157 的 PR 说明明确要求 #179 先合并，并完成最新 main 对齐。该约束针对现有 PR 及 Product Evidence/迁移集成，不阻止先整理和验证后来的 JATO-only MCP 本地成果。

## 7. AstrBot：成果最多，恢复风险也最高

### 远端已提交部分

PR：[#157 feat(astrbot): migrate governed agent workspace onto latest main](https://github.com/tristan419/JATO_Analysis_System/pull/157)

- Draft、历史 CI 全绿；
- 85 个已提交文件，`+125,012/-0`（含大量测试）；
- 截至本次审计落后 `main` 约 140 个提交；
- worktree：`/Users/litristan/Downloads/JATO_Analysis_System_astrbot_migration`。

#157 已提交 AstrBot workspace、Agent planning、Evidence、SSE、Follow-up、Memory、Usage、Eval、MCP 治理和前端工作台。

### 本地未提交的后续进展

- 71 个 tracked 修改；
- 展开后 52 个 untracked 文件，总计 123 条状态；默认折叠目录时为 50 条 untracked、总计 121 条；
- tracked diff 约 `+23,181/-12,889`；
- staged 为 0，尚未形成正式 checkpoint。

历史任务记录与本地代码中的重要进展（不等于整棵工作区已验收）：

- 唯一 Capability Catalog；
- 12 个 JATO 页面 Capability；
- DPV4 Native Tool Calls；
- 每轮动态暴露 3–6 个相关工具；
- Query 参数由后端校验并冻结；
- 页面、MCP、Evidence 和图表逐步统一到同一事实源；
- 删除旧文本工具协议、固定中文摘要和基于问题文字猜图的路径；
- 最近一次 Composer/Visual/Evidence 收缩净删约 699 行；
- 2026-08-05 最近一批核心回归 `318 passed`；
- 同批 AstrBot/JATO 广泛回归 `1190 passed`，本次未重跑。

### 仍未完成

- 非 JATO legacy 文本工具协议尚有残留；
- MCP Tools、EvidencePackage、Visual Artifact 仍有重复兼容分支；
- Agent Graph、附件、Access Control、Config Evidence 等多批工作混在同一未提交 tree；
- 本地迁移链 `20260723_0047_astrbot → 20260730_0048_llm_access` 与 Config 的 `20260722_0047 → 20260723_0048` 都从 `20260715_0046` 分叉；不是完全相同 revision ID，但合并时须按现有单链约定重新接续；
- 整体仍是巨大净新增，不能把最近一批测试通过理解成整棵 worktree 可提交。

### 恢复建议

当前先保护未提交成果，不整体 stage 或继续扩大范围。以下是候选拆分，不代表已经验证每批可独立提交：

1. JATO Capability Catalog 和 Native Tool 主路径；
2. Composer/Visual/Evidence 删除与契约测试；
3. Workspace attachments；
4. Access Control / LLM entitlement；
5. Config Product Evidence 集成；
6. 明确排除的运行产物与跨业务文件。

最近一批任务于 2026-08-05 正常完成，未 commit/push，下一步是拆薄 MCP Tools 总路由与 Evidence coverage 的 Catalog 工具枚举。当前目标是 JATO 页面 MCP，明确暂不扩展 Config/MSRP/BOM 等领域。可以先整理这一批；现有 #157 的合并仍须遵守先 #179、再最新 main 对齐及迁移接续的约束。

本地当前范围与下一步见下列 worktree 文档；长期目标以同目录 `astrbot_master_goal.md` 为准：

`/Users/litristan/Downloads/JATO_Analysis_System_astrbot_migration/docs/astrbot_current_goal.md`

## 8. MSRP：治理主干已完成，继续做真实覆盖

### 已进入 main

- #136 Source Governance Console；
- #141 Materialization Approval Gate；
- #149 Evidence Durability；
- #150 Evidence Verification；
- #158 draft extractor dry-run coverage；
- #143 官方来源修复；
- #151 dry-run active-state truth；
- #152 immutable frontend artifact parity。

当前已存在的主链是：

```text
官方来源 → Evidence → Dry-run/Review → 审批 → Materialize
```

### 未完成部分

PR：[#183 fix(msrp): classify source failures consistently](https://github.com/tristan419/JATO_Analysis_System/pull/183)

- 7 个文件、2 个提交；
- worktree clean；
- 历史 CI 全绿；
- 截至本次审计落后 `main` 约 134 个提交。

它统一 homepage redirect、403、official model unavailable 和 extractor failure 的分类。逐文件核对确认：这 7 个路径在 main 从分叉点到本次 SHA 之间均未变化。本地 `56c19cd9` 与远端 `aa06581c` 文件树完全一致（`67949d464d65ed5cf0957b255543fbb05d99dd5a`）。GitHub 为 `MERGEABLE / behind`；更新现有 PR 基线并重新验证即可作为选项，不必仅因落后重开 PR，也不要因 SHA 不同重复搬运。

真实来源覆盖是另一项工作；以下是待复核事项，不是本次已确认仍存在的缺陷：

- 21 国 canonical roster；
- `batch_a` / `all` alias；
- `todo.invalid` 应归类为 `source_not_configured`；
- Scrapling 和 HTTP JSON draft 是否都进入 scheduled/local dry-run；
- promoted/draft 去重；
- 当前真实 no-ingest 覆盖率与 backlog。

候选 dry-run 成功不等于数据已 ingest、materialize 或上线。恢复验证必须继续保持这三层状态分离。

旧 `/Users/litristan/Downloads/JATO_Analysis_System_msrp` 有大量历史提交、报告、截图和运行记录，只能作为审计源。

## 9. JATO 月更

已经进入 `main`：

- [#174](https://github.com/tristan419/JATO_Analysis_System/pull/174)：自动识别上传月份和默认安全历史策略；
- #178：未解决 Review blocker 时禁止 Candidate rebuild；
- #180：原生 prompt 替换为受控对话框。

仍开放的 [#164](https://github.com/tristan419/JATO_Analysis_System/pull/164) 是历史修正策略文档，原临时 worktree 已不存在但分支仍在。先对照 #174 后的实际流程审查内容，再决定更新原 PR、替代或关闭；worktree 缺失本身不要求重开 PR。

原始混合目录目前仍在 `codex/jatomonthly`，但它同时包含 MSRP、Config、BOM、COC、Hermes 和运行产物，不能视为 JATO 月更的未完成 branch。

## 10. 已基本完成或仅剩零散尾项

| 业务线 | 状态 | 建议 |
| --- | --- | --- |
| COC Fill / Match | 主体已进入 main；历史任务记录曾验证上线，本次未重跑业务验收 | 有新真实问题时再开独立修复 |
| MarketScan | #175 已合并；异步加载、时间轴和图表标签修复完成 | 不从旧 worktree继续开发 |
| Hermes cockpit | #23 已合并并曾验证上线 | #182 只处理历史 event 恢复 |
| BOM Admin | #215/#218/#220 已合并 | 从最新 main 开新功能 |
| Hero Product | 旧 worktree 有少量 dirty 内容，没有开放 PR或清晰终态 | 先只读审计再决定是否恢复 |
| 飞书发运台账联动 | 已检索记录以可行性讨论为主，未确认正式实现线 | 先补查任务与产品范围，不据此断言从未实现 |

## 11. 开放 Draft PR处置建议

### 保留并恢复

| PR | 主题 | 建议 |
| --- | --- | --- |
| [#226](https://github.com/tristan419/JATO_Analysis_System/pull/226) | Candidate 应用登录 | 第一优先级收口 |
| [#179](https://github.com/tristan419/JATO_Analysis_System/pull/179) | Config Product Evidence | 保护本地后续后更新基线；是否拆分按实际范围决定 |
| [#157](https://github.com/tristan419/JATO_Analysis_System/pull/157) | AstrBot migration | 保留作已提交基线，不直接合并 |
| [#183](https://github.com/tristan419/JATO_Analysis_System/pull/183) | MSRP failure classification | 可更新原 PR 基线并重验，不强制重开 |
| [#227](https://github.com/tristan419/JATO_Analysis_System/pull/227) | 本交接与关联文档 | 文档修正，尚未进入 main |

### 仅列待审计项，不预判全部可关闭

| PR | 原因 |
| --- | --- |
| [#225](https://github.com/tristan419/JATO_Analysis_System/pull/225) | 内容已被 #226 吸收 |
| [#181](https://github.com/tristan419/JATO_Analysis_System/pull/181) | 与后续本地 AGENTS/环境卫生约定核对；后者未跟踪，不能假设已在 main 替代 |
| [#134](https://github.com/tristan419/JATO_Analysis_System/pull/134) | 已被 #181 明确替代 |
| [#182](https://github.com/tristan419/JATO_Analysis_System/pull/182) | 已冲突；需先按 eventId 检查是否已进入 main |
| [#191](https://github.com/tristan419/JATO_Analysis_System/pull/191) | 后续 #192–#224 已大幅改变 Candidate/release 流程 |
| [#164](https://github.com/tristan419/JATO_Analysis_System/pull/164) | 旧历史修正文档，需对照 #174 后流程 |
| [#11](https://github.com/tristan419/JATO_Analysis_System/pull/11) | 极旧、冲突、落后数百提交，不应直接合并 |

关闭或删除不属于本文档任务。执行前仍需逐项确认是否存在独有提交或未恢复证据。

## 12. 本地 worktree 风险摘要

截至本次审计，主要 dirty worktree：

| Worktree | Dirty paths | 含义 |
| --- | ---: | --- |
| 原始混合观察区 | 384（折叠）/ 1168（展开） | 157 tracked 修改 + 1011 untracked 文件；并非迁移后减少 |
| AstrBot migration | 121（折叠）/ 123（展开） | 71 tracked 修改 + 52 untracked 文件，优先保护 |
| 旧 AstrBot | 94 | 历史源，不作为新 base |
| Config hardening | 74 | 旧混合版本 |
| Config release v2 | 66 | 旧版本，已被 clean migration 替代 |
| 旧 MSRP | 53 | 源配置、报告、截图、Hermes 等混合 |
| 旧 BOM | 23 | 事故与恢复历史 |
| Config clean migration | 10 | #179 之后的有效未提交 follow-up |

其余数字沿用默认折叠口径。这些数字表示路径状态，不等于功能数量；数量相同也不能证明内容未变。此前 306/61/245 的精确文件比较未在本轮逐 hunk 复核，不能解释成 245 个未迁移功能，也不能断言混合区已全部搬完。任何清理前都必须先确认远端提交、未提交 diff、独有 commit 和当前 task owner。

## 13. 可直接复制给下一次主 session 的启动提示

```text
请先阅读：
Markdown_Readme/Fullstack/JATO_PLATFORM_HANDOVER_2026-09-14.md

本轮只做只读恢复核验，不修改代码、不关闭 PR、不清理 worktree：
1. fetch 并确认当前远端 main；
2. 重新列出全部开放 PR及 CI/mergeable 状态；
3. 核对 #226、#179、#157、#183 相对最新 main 的 ahead/behind；
4. 核对 Candidate、www、intl 的 build-meta/认证状态；
5. 使用 -uall 核对 AstrBot（上次 123）和 Config（上次 10）的路径与实际 diff，数量相同不等于内容相同；
6. 报告 handover 中哪些事实已变化；
7. 给出本轮唯一建议恢复的 task，但不要开始写代码，等我确认。
```

## 14. Codex task索引

| 业务线 | Task 标题 | Thread ID |
| --- | --- | --- |
| 总体盘点 | 可以帮我统计一下我现有哪些混合区改动… | `019fd9ea-cfb4-7812-902b-fb9f495a02f3` |
| Config | 配置对比功能 | `019eb660-2f47-7541-af78-128461e8209b` |
| AstrBot | astrbot内化优化 | `019eb286-3a7d-7721-aadc-91ce291cb0a4` |
| MSRP | msrp抓取功能 | `019eb67a-7088-7ae1-a4f6-f8581badede6` |
| MSRP Epic | [MSRP Epic] 00 父任务｜自动修复 | `019f5e3c-b86b-7a11-bfd2-f03907be5bd5` |
| JATO 月更 / Candidate | jato月更 | `019f49ff-a89c-7252-956c-2bc31e317d2d` |
| BOM | 修复BOM admin问题 | `019ece30-0e63-7692-99b0-092d4c2a6bf8` |
| BOM follow-up | BOM Admin 遗留缺陷 clean follow-up | `019f8a2b-629a-7411-a9e9-1bf9880a7e77` |
| COC | COC 填充工作台 task | `019ed464-55cc-7683-940c-63f5697c08ae` |
| Hermes | hermes管家 | `019eb65e-778a-7450-a9fd-34d6bd1a659f` |
| MarketScan | MarketScan 加载、时间轴与图表标签修复 | `019f8a3c-61a4-7c93-a606-3575be9454db` |

这些 task 是历史证据来源，不代表其中最后一条指令现在仍应直接执行。恢复决策以最新 `main`、PR、worktree 和线上事实为准。

## 15. 恢复待办：先完成一个可用验收入口，再继续 BOM

以下是任务顺序，不是新增门禁；勾选必须有实际完成证据。文档更新不授权关闭/合并 PR、删除 Candidate、更新 Active 或同步 intl。

- [x] 本次审计已确认 main、PR、正式站前端版本和混合区统计口径。
- [ ] **交接入口**：审阅本次 #227 文档修订，决定何时合并；合并前显式读取本 worktree，不把旧 main 中的 Goal 状态当最新指令。文档合并本身可能触发现有 main CI → Candidate 自动准备，不能当成完全无运行影响。
- [x] **Candidate 登录入口**：#226/#228 逻辑已在最终 main，Candidate 部署成功且 Basic Auth 未启用；用户提供的管理员 Candidate 账号已登录成功。仍需低权限 403、过期 token、断网和草稿保留验收，不能把管理员登录等同 D 全通过。
- [x] **Candidate BOM 写入样本**：现有 Preview/Apply 成功填充沙箱 8 行颜色名称；未向 Active/www/intl 发送业务写入。完整环境隔离以前序部署证据为准，不把单次 UI 测试等同于重新证明全部基础设施隔离。
- [ ] **P1 BOM 基线验收**：#215 当前行 Edit 和 #220 Preview/Apply 已有部分通过证据；仍需同模板跨车型/版本专门样本、BOM/Matrix 双入口、manual FOB/no-base 与数值重算，记录真实失败，不重新搬 #173。
- [ ] **P1-A BOM 输入/搜索收尾**：A 已随 PR #231 合入最终 main；Candidate 已观察搜索清除/恢复和 1.2s 搜索入口，尚未完成逐字输入、自动回填后二次修改、FOB 保存后搜索保持的完整浏览器验收。
- [ ] **P1-B BOM Model 布局**：B 已随 PR #230 合入最终 main；50/100/150% 缩放下 Model 保持最左，但拖拽列宽、刷新/数量保存后的持久化和真实窄窗口尺寸仍未验收。
- [x] **P1 BOM 本地批次**：B（业务 `53976fc0`，对齐 `ac59a832`）、C（含 E，业务 `2218c0df`，对齐 `579ea01e`）和 D（原始 `1a64194a`，对齐 `f6887fb3`，衔接修正 `34656898`）已在各自独立 worktree 完成本地实现与验证；不要重新执行这些批次，也不优先建设多人编辑 row_version。
- [ ] **P1 BOM PR 审阅与 Candidate 验收**：A/B/C/D、#233/#234/#235 已合入；最新已记录 Candidate 为 abd38688d35e。#236 必须按顶部最终方案修订并重新审阅；全量价格与完整交互仍未验收，不能直接合并或发布正式站。
- [ ] **当前 Candidate 构件（最新已记录）**：abd38688d35eec9d771d8a9fc5fa599545b172eb；详细实际样本见 BOM 实施文档 #235 Candidate 实测。旧 #233 cf10438fdeed 构件仅作历史。后续重新部署必须记录新的实际 SHA 与沙箱身份。
- [ ] **共享色卡剩余验收**：#235 已引入持久品牌＋色码标准并共用 BOM/Matrix 解析；UE 两模板样本通过，不等于 TE 创建后跨模板复用或全部历史缺色已通过。缺值应明确提示，不写默认灰色掩盖缺失。
- [ ] **颜色映射实现口径**：先按规范化“品牌＋Colour Code”查已有共享 HEX；未命中才按名称/别名找唯一候选；仍未命中才让用户选色或输入 HEX 并确认保存。不能把颜色名称自动猜成真实车漆色，也不能用默认灰色覆盖缺失数据。双色保存两份 HEX、Matte 保存一份；色块数量与 Dual/Special 价格 tier 分开维护。BOM 与 Matrix 必须引用同一映射。
- [ ] **2026-09-25 共享颜色映射 Candidate：部署通过，业务验收否决**：PR [#234](https://github.com/tristan419/JATO_Analysis_System/pull/234) 已合入 main，merge/main/Candidate commit `fd50e5a1b3222aec96d1b5dcb34d8d884018ffae`。Candidate run [#36099775361](https://github.com/tristan419/JATO_Analysis_System/actions/runs/36099775361)，操作 ID `2026-09-25T055411284Z-prepare-candidate-f3855f8b`，archive `0067a7c84c7f9ddbbf41ad567bb35784fde27861d28cbe8d3d4f2d5585fbc171`，manifest `9eaf85a81401a4912bec5bd9c6c94670bf3e703af5765dc0c9cc7111de39fef8`。部署报告通过 sandbox/database isolation/backend/monthly-disabled/preview 并记录 `active_unchanged`，但业务复核发现：JAECOO `dual=300` 的规则正确而 `T7160RG**MH0001` 的 CH Dual 实际仍为 `19,350`（行元数据同时出现 `baseFobEur=20,150`、`colourSurchargeEur=300`、`finalFobEur=19,350`）；BOM/Matrix 大量 `colourHex=null`，共享规则没有覆盖显示路径；当前沙箱 OMODA+TE lookup 为 `source=none`，且本次替换删除了 1 个旧 Candidate 沙箱，无法证明旧 TE 是否曾成功持久化。三项均不能验收通过；不手工逐色补值、不手工给每行加 300，先修根因并在当前沙箱重做 TE 全流程。不更新 Active/www/intl。
- [ ] **发布，单独决定**：若要让正式站看到已验收功能，另行批准把同一个已测构件 update-active；再按现有独立流程同步 intl，分别核验版本。Candidate 可用不等于必须立即发布。
- [x] **2026-09-25 #235 合并及 Candidate 准备**：已合入 abd38688d35eec9d771d8a9fc5fa599545b172eb；持久色库与部分重算已实现，Candidate OMODA9/UE 实测有通过证据。勾选仅表示合并/部署完成，不表示全量历史派生价格或所有交互验收通过。
- [x] **2026-09-25 #236 修订代码已完成**：按 [第 13 节最终确认方案](../features/BOM_ADMIN_CANDIDATE_ACCEPTANCE_2026-09-15.md#bom-pricing-final-20260925) 完成统一计算、歧义保护、计数、付款条件去耦、档位定制及新增颜色/手动 FOB 入口收口；提交见下方 2026-09-26 记录。来源为 import/manual/copy 不能永久冻结价格；缺基准/冲突仍不写。当前只等待组合审阅与明确授权合并，Candidate 先只读审计再授权 Apply；不自动改正式数据。
- [ ] **2026-09-25 #236 第一批已完成待审阅**：业务提交 `944bcc03` 已让 audit/writer 共用基准决策，去除付款条件基准过滤，保留歧义并补真实 `skippedAmbiguous`，Apply 按物料＋国家去重；定向测试 `10 passed`，完整 BOM 文件 `60 passed, 5 failed`（5 项为既有 country-column / `sync_missing_template_fobs` 基线缺口）。未合并、未部署 Candidate；详见 BOM 文档第 13 节批次记录。
- [ ] **2026-09-25 #236 第二批已完成待审阅**：业务提交 `95ba8941` 已将颜色特例绑定 `Dual/Special` tier，旧规则默认 Special，新增 migration `20260925_0048`，避免 Special 特例误用于 Dual；tier/audit/迁移定向 `13 passed`。未合并、未部署 Candidate；下一批才接折叠管理 UI。
- [ ] **2026-09-25 #236 第三批已完成待审阅**：业务提交 `75758ea6` 已将前端 API/type、色卡命中和定制规则入口绑定 `Dual/Special` tier；Special 定制入口默认折叠并显示规则数量，展开后可编辑适用档位，Single 不查询定制加价。前端 `tsc --noEmit`、Vitest `75 files / 411 tests`、Vite build 均通过。未合并、未部署 Candidate；下一步是审阅三批组合后再决定是否授权合并。
- [x] **2026-09-25 三批组合审阅结论已被第四批修订替代**：当时的 `75758ea6` 审阅确实发现旧 tier 回退、缺规则静默 0 和新增颜色/手动入口旁路；这些问题已由 #236 `95706e19`、`f72ce28b`、`61b5dd24` 修订。保留本条作为历史证据，当前不再按旧 head 重做。
- [x] **新增 Dual 颜色零加价根因已收口（待 Candidate 验收）**：新增颜色自动初始化、显式 FOB、单行/批量 FOB 编辑现在统一解释为 Single 基准并按 Dual/Special surcharge 派生；缺 tier/缺规则不静默按 0，名称类 legacy 值不自动推断 Special。代码尚未合并，不能把本地测试视为线上已修复。
- [ ] **P2 其他研发**：按产品优先级选择 Config 本地 10 文件收口、AstrBot JATO MCP 整理，或 MSRP #183；不要求全项目依次排队。保留 #157 对 #179 的现有合并约束。
- [ ] **P3 历史整理**：最后再审计旧 Draft PR/旧 worktree；未提交成果未确认前不关闭、删除或清理。

对应状态入口：[Candidate Goal](04_DevOps/SIMPLE_CANDIDATE_RELEASE_V2_2026-08-06.md)、[BOM 颜色目标](../features/BOM_COLOUR_RULES_UNIFICATION_GOAL_2026-08-09.md)、[Order Genius 路线图](../features/ORDER_GENIUS_IMPLEMENTATION_ROADMAP.md)、[MSRP 索引](MSRP/README.md)。Config/AstrBot 当前状态维护于本文第 6/7 节；本轮不跨入其 dirty worktree 修改目标或代码。

### 2026-09-26 · #236 第四批已实现，等待组合审阅与授权

针对“已有物料新增 Dual/Special 颜色后实际 FOB 仍等于 Single、颜色加价为 0”的根因，PR [#236](https://github.com/tristan419/JATO_Analysis_System/pull/236) 已在独立 worktree `/Users/litristan/Downloads/JATO_Analysis_System_bom_reprice_audit` 的 `codex/bom-colour-reprice-audit` 分支完成第四批实现提交 `95706e19`，以 `f72ce28b` 补齐测试契约，并以 `61b5dd24` 收紧 legacy tier 兼容。本批不是 JAECOO7 或某个颜色的特例：

- 新增颜色自动初始化、显式 FOB、单行/批量编辑、模板基准保存、国家调整和导入均把输入解释为同模板同国家的 Single 基准，再按已保存 `Single/Dual/Special` 档位解析颜色加价；付款条件只作为参考元数据。
- 统一决策区分合法显式 `0`、缺档位和缺规则；缺档位/缺规则不会静默写成零加价。旧数据只在合法时从 `exterior_color_type` 回退，不能把未知档位默认成 Single。
- legacy 回退只接受明确的 `single/dual/special` 类型；不会因 `matte`、`pearl` 或 `black edition` 等名称自动推断 Special。
- 计算结果同时保存 `base_fob_eur`、`colour_surcharge_eur` 和 `final_fob_eur`，重复保存不会把已加价最终值再次当成基准。

本地验证已记录在 BOM 实施文档第 13 节：专项 `66 passed, 5 failed`（5 项为最新 main 已存在的 country-column / `sync_missing_template_fobs` 基线缺口）；前端类型检查、411 个单测和生产构建通过。该提交尚未合并、未准备 Candidate、未写入 Candidate/Active/www/intl。下一步只能是审阅四批组合并获得明确授权，再按 PR → 最新 main → Candidate 流程做只读 audit 和浏览器验收；不得把本地测试结果当作线上价格已修复。
