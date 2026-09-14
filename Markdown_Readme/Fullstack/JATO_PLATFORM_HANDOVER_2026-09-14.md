# JATO Platform 开发交接与恢复入口

> 状态快照：2026-09-14（Asia/Shanghai）
>
> 目的：长时间暂停开发后，从一个可信入口重新判断“已经完成什么、什么仍在搁置、下一步从哪里开始”。
>
> 本文不是永久真相。开始任何新任务前，必须重新读取远端 `main`、开放 PR、目标 worktree 状态和线上构件版本。

## 1. 先看结论

当前项目并不是所有功能都停在半成品状态，而是四类状态混在一起：

1. BOM Admin、COC、MarketScan、Hermes 主体已经进入 `main`；
2. Config、AstrBot 有大量真实成果，但远端 PR 与本地未提交进展已经分叉；
3. MSRP 的安全与治理主干已经合并，剩余失败分类和真实来源覆盖工作；
4. Candidate 登录方案接近完成，但仍未完成线上切换，是恢复稳定验收环境前最直接的尾项。

建议恢复顺序：

```text
Candidate 登录 #226 收口
        ↓
BOM Admin 新功能（最快恢复产品开发）
        ↓
Config #179 从最新 main 收口
        ↓
AstrBot 本地成果分批迁移
        ↓
MSRP #183 与真实 21 国覆盖复核
```

如果优先考虑长期产品价值，可以在 Candidate 收口后直接做 Config；Config 是 AstrBot 使用可信产品配置证据的前置依赖。

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

随后只读检查目标 worktree 的：

```bash
git branch --show-current
git status --short
git rev-parse HEAD
git rev-list --left-right --count JATO_Analysis_System/main...HEAD
```

原始目录 `/Users/litristan/Downloads/JATO_Analysis_System` 是混合观察区，不是新开发目录。

## 3. 截至本次交接的基线

- 远端 `main`：`f29cf5096b528e2c0350047f2bc462cc8bfc8696`
- 最后一次 `main` 提交时间：2026-08-10
- 最后合并 PR：[#224](https://github.com/tristan419/JATO_Analysis_System/pull/224)，自动准备 current-main Candidate
- 当前开放 PR：11 个，全部为 Draft
- 本机磁盘可用空间：约 235 GiB，不构成恢复阻塞

线上只读检查结果：

- `intl.ojeur.cloud` 的 `build-meta.json` 仍指向 `cd4557cb932374a0fefb6c80a5fac9fb75a67d62`，不是当前 `main`；
- `candidate.ojeur.cloud` 仍返回 Nginx Basic Auth `401`；
- 本次从当前机器解析 `www.ojeur.cloud` 超时，未确认 www 的实际构件版本；
- 因此不能把“代码已在 main”自动等同于“当前线上已运行”。

## 4. Candidate / 部署：建议最先收口

### 当前有效进展

PR：[#226 fix(candidate): require application login](https://github.com/tristan419/JATO_Analysis_System/pull/226)

- 分支：`codex/candidate-app-login`
- 相对本次 `main`：0 behind
- required GitHub CI 全部通过；
- 唯一红项是非 required 的 Cloudflare Pages preview；
- 本地产品代码已 clean，只剩一条提交钩子生成的 Hermes event 未提交。

已实现：

- Candidate 复用现有 OJEUR `/login`，没有另建登录系统；
- Candidate 使用独立沙箱用户数据和独立 JWT secret；
- 除 login/health 外，未认证 Candidate API 严格拒绝；
- Candidate 禁止接收 Active OAuth callback 凭据；
- 前端测试、TypeScript、构建和后端相关测试已经通过。

### 为什么停下

当前运行中的 Candidate 仍是旧的 `APP_AUTH_ENABLED=false` + Nginx Basic Auth 方案。安全收口顺序尚未执行完：

1. 保留现有 Basic Auth；
2. 废弃旧 Candidate，确认 Active/www 不受影响；
3. 合并 #226，由 current `main` 自动重建严格认证 Candidate；
4. 在 Basic Auth 仍存在时验证真实 OJEUR 登录、401 和沙箱隔离；
5. 最后原子移除 Candidate vhost 的 Basic Auth。

### 恢复建议

先只读定位 Cloudflare preview failure 是否仍相关，再重新验证 #226 相对最新 `main`。不要重写认证代码；现有实现已经完成，剩余是验证与上线顺序。

[#225](https://github.com/tristan419/JATO_Analysis_System/pull/225) 的自动 Candidate 证据已经被 #226 吸收，原则上不应单独继续合并。

## 5. BOM Admin：最适合恢复新产品开发

### 已进入 main

- [#215](https://github.com/tristan419/JATO_Analysis_System/pull/215)：相同 BOM Template 只编辑当前车型/版本行；
- [#218](https://github.com/tristan419/JATO_Analysis_System/pull/218)：颜色规则统一目标；
- [#220](https://github.com/tristan419/JATO_Analysis_System/pull/220)：颜色规则与 swatch 统一实现；
- 更早的 Copy SKU、Copy Country、Batch Material、颜色 surcharge、Father Note、CBU Finance 已进入 `main`。

### 当前状态

- 没有开放的 BOM PR；
- `/Users/litristan/Downloads/JATO_Analysis_System_bom_admin_current` 为 clean；
- 旧 BOM worktree/branch 只作为事故与迁移审计源；
- 不需要再次迁移 #173 或整支合并旧 BOM branch。

### 恢复建议

如果要尽快重新开始业务开发，Candidate 收口后从届时最新远端 `main` 新建一个“具体 BOM 功能”的 worktree/branch/PR。不要继续使用已经合并的 `codex/bom-admin-edit-scope-current-main`。

## 6. Config 配置对比：成熟的大型未上线产品

### 远端已提交部分

PR：[#179 feat(config): govern published product evidence](https://github.com/tristan419/JATO_Analysis_System/pull/179)

- Draft、Mergeable、历史 CI 全绿；
- 67 个文件；
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

### 本地未提交的后续进展

当前还有 9 个 tracked 修改和 1 个新 service，约 `+1606/-32`，不在 #179 中：

- 从 Published snapshot 创建经审核的 augmentation child draft；
- Product Evidence schema preflight；
- schema 未部署时明确返回 `schema_unavailable`；
- Published snapshot 身份与实时 Trim 隔离；
- 更完整的 Digest、Evidence 和 API 回归测试。

### 为什么停下

- Config 与 AstrBot 同时需要 `config.py`、`App.tsx`、认证、models、API client 等共享入口；
- 当时通过 owner 边界避免两个 session 互相覆盖；
- #179 已提交版本与后续 10 文件本地版本尚未统一收口；
- 旧 base 已不适合直接合并。

### 恢复建议

先把本地 10 文件做只读差异清单和可迁移 checkpoint，再从最新 `main` 重建 Config PR。不要在旧 worktree 继续扩大功能，也不要先把后续 10 文件直接追加到旧 #179。

Config 应先于 AstrBot 合并，因为 AstrBot 需要它的 Published Product Evidence MCP 和迁移编号。

## 7. AstrBot：成果最多，恢复风险也最高

### 远端已提交部分

PR：[#157 feat(astrbot): migrate governed agent workspace onto latest main](https://github.com/tristan419/JATO_Analysis_System/pull/157)

- Draft、历史 CI 全绿；
- 85 个已提交文件；
- 截至本次审计落后 `main` 约 140 个提交；
- worktree：`/Users/litristan/Downloads/JATO_Analysis_System_astrbot_migration`。

#157 已提交 AstrBot workspace、Agent planning、Evidence、SSE、Follow-up、Memory、Usage、Eval、MCP 治理和前端工作台。

### 本地未提交的后续进展

- 71 个 tracked 修改；
- 50 个 untracked；
- tracked diff 约 `+23,181/-12,889`；
- staged 为 0，尚未形成正式 checkpoint。

已经完成的重要能力：

- 唯一 Capability Catalog；
- 12 个 JATO 页面 Capability；
- DPV4 Native Tool Calls；
- 每轮动态暴露 3–6 个相关工具；
- Query 参数由后端校验并冻结；
- 页面、MCP、Evidence 和图表逐步统一到同一事实源；
- 删除旧文本工具协议、固定中文摘要和基于问题文字猜图的路径；
- 最近一次 Composer/Visual/Evidence 收缩净删约 699 行；
- 核心回归 `318 passed`；
- AstrBot/JATO 广泛回归 `1190 passed`。

### 仍未完成

- 非 JATO legacy 文本工具协议尚有残留；
- MCP Tools、EvidencePackage、Visual Artifact 仍有重复兼容分支；
- Agent Graph、附件、Access Control、Config Evidence 等多批工作混在同一未提交 tree；
- 本地新增的 `0047/0048` migration 会与 Config #179 冲突；
- 整体仍是巨大净新增，不能把最近一批测试通过理解成整棵 worktree 可提交。

### 恢复建议

不要继续写新能力，也不要整体 stage。先按 manifest 拆成：

1. JATO Capability Catalog 和 Native Tool 主路径；
2. Composer/Visual/Evidence 删除与契约测试；
3. Workspace attachments；
4. Access Control / LLM entitlement；
5. Config Product Evidence 集成；
6. 明确排除的运行产物与跨业务文件。

Config 合入后，重新确定 AstrBot migration revision，再按可独立验证的批次迁到最新 `main`。

权威本地进度说明见仓库外当前 worktree 中的：

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

它统一 homepage redirect、403、official model unavailable 和 extractor failure 的分类。恢复时应把这两个提交重放到最新 `main`，不能直接合并旧 branch。

后续还需重新验证：

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

仍开放的 [#164](https://github.com/tristan419/JATO_Analysis_System/pull/164) 只是历史修正策略文档，临时 worktree 已不存在。恢复时应先判断它是否仍符合 #174 后的真实流程：符合则从最新 `main` 重开小文档 PR，不符合则关闭。

原始混合目录目前仍在 `codex/jatomonthly`，但它同时包含 MSRP、Config、BOM、COC、Hermes 和运行产物，不能视为 JATO 月更的未完成 branch。

## 10. 已基本完成或仅剩零散尾项

| 业务线 | 状态 | 建议 |
| --- | --- | --- |
| COC Fill / Match | 主体、失败结果、corner cases 已进入 main 并验证上线 | 有新真实问题时再开独立修复 |
| MarketScan | #175 已合并；异步加载、时间轴和图表标签修复完成 | 不从旧 worktree继续开发 |
| Hermes cockpit | #23 已合并并曾验证上线 | #182 只处理历史 event 恢复 |
| BOM Admin | #215/#218/#220 已合并 | 从最新 main 开新功能 |
| Hero Product | 旧 worktree 有少量 dirty 内容，没有开放 PR或清晰终态 | 先只读审计再决定是否恢复 |
| 飞书发运台账联动 | 只有可行性讨论，没有正式实现线 | 需要产品范围后才能启动 |

## 11. 开放 Draft PR处置建议

### 保留并恢复

| PR | 主题 | 建议 |
| --- | --- | --- |
| [#226](https://github.com/tristan419/JATO_Analysis_System/pull/226) | Candidate 应用登录 | 第一优先级收口 |
| [#179](https://github.com/tristan419/JATO_Analysis_System/pull/179) | Config Product Evidence | 从最新 main 重建并纳入本地后续 |
| [#157](https://github.com/tristan419/JATO_Analysis_System/pull/157) | AstrBot migration | 保留作已提交基线，不直接合并 |
| [#183](https://github.com/tristan419/JATO_Analysis_System/pull/183) | MSRP failure classification | 两提交重放到最新 main |

### 审计后大概率关闭或替代

| PR | 原因 |
| --- | --- |
| [#225](https://github.com/tristan419/JATO_Analysis_System/pull/225) | 内容已被 #226 吸收 |
| [#181](https://github.com/tristan419/JATO_Analysis_System/pull/181) | 文档已落后于后续 AGENTS 与开发环境卫生规则 |
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
| 原始混合观察区 | 384 | 多业务与运行产物混合，禁止开发 |
| AstrBot migration | 121 | 大量有效未提交研发成果，优先保护 |
| 旧 AstrBot | 94 | 历史源，不作为新 base |
| Config hardening | 74 | 旧混合版本 |
| Config release v2 | 66 | 旧版本，已被 clean migration 替代 |
| 旧 MSRP | 53 | 源配置、报告、截图、Hermes 等混合 |
| 旧 BOM | 23 | 事故与恢复历史 |
| Config clean migration | 10 | #179 之后的有效未提交 follow-up |

这些数字表示路径状态，不等于功能数量。任何清理前都必须先确认远端提交、未提交 diff、独有 commit 和当前 task owner。

## 13. 可直接复制给下一次主 session 的启动提示

```text
请先阅读：
Markdown_Readme/Fullstack/JATO_PLATFORM_HANDOVER_2026-09-14.md

本轮只做只读恢复核验，不修改代码、不关闭 PR、不清理 worktree：
1. fetch 并确认当前远端 main；
2. 重新列出全部开放 PR及 CI/mergeable 状态；
3. 核对 #226、#179、#157、#183 相对最新 main 的 ahead/behind；
4. 核对 Candidate、www、intl 的 build-meta/认证状态；
5. 核对 AstrBot migration 121 个 dirty path 和 Config migration 10 个 dirty path是否仍原样；
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
