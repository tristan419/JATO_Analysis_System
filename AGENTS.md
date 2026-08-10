# AGENTS.md

## Project Interaction Rules

- 写前端时，每次涉及交互逻辑和页面布局，都要用语言描述反馈。
- 写后端时，要用语言描述技术栈，并注意函数和对象复用。

## Local Git / Worktree Rules

本项目多 session 开发采用固定模型：

```text
一个 session = 一个 worktree = 一个 branch = 一个 PR
```

原始目录 `/Users/litristan/Downloads/JATO_Analysis_System` 视为混合观察区，默认只读，不作为新功能开发目录。

当前固定开发线：

- BOM Admin:
  - worktree: `/Users/litristan/Downloads/JATO_Analysis_System_bom_admin`
  - branch: `codex/bom-admin-panel-colour-rules-baseline`
  - PR scope: 只包含 BOM Admin / Order Genius 改动

- MSRP:
  - worktree: `/Users/litristan/Downloads/JATO_Analysis_System_msrp`
  - branch: `codex/msrp-scraping`
  - PR scope: 只包含 MSRP 抓取、修复、数据源改动

- AstrBot / CountryCopilot:
  - worktree: `/Users/litristan/Downloads/JATO_Analysis_System_astrbot`
  - branch: `codex/astrbot-countrycopilot`
  - PR scope: 只包含 AstrBot / agent / MCP 改动

- JATO 月更:
  - worktree: `/Users/litristan/Downloads/JATO_Analysis_System_jato_monthly_resilience`
  - branch: `codex/jato-monthly-resource-isolation`
  - PR scope: 只包含 JATO 月更、资源隔离和该链路的韧性改动

共享文件如 `App.tsx`、`api/client.ts`、`main.py` 可以改，但必须明确属于哪个 PR；其他 session 暂时不要碰同一段。

最后多条 PR 都要上线时，再开 integration 分支/PR 解决合并冲突。

详细治理规范见：

`Markdown_Readme/Fullstack/01_DevWorkflow/BRANCH_PR_DEPLOYMENT_GOVERNANCE_2026-07-14.md`

## Branch / PR / Production Deployment Rules

以下规则是强制规则，优先级高于历史文档中的旧部署说明：

1. `main` 是唯一生产部署来源，只有已合入 `main` 的 commit 可以部署到 `www.ojeur.cloud` 和 `intl.ojeur.cloud`。
2. feature、hotfix、integration、实验分支只允许 CI 和 preview；禁止直接触发生产部署。
3. 禁止从任何 session 直接 push 到 `main`。必须使用独立 worktree、独立 `codex/*` branch 和 PR，经审核后 merge。
4. integration 分支只用于临时冲突解决和集成预览，不是长期开发分支，也不是生产来源。
5. `www` 和 `intl` 必须部署同一个 immutable artifact；不能分别从不同 branch 或不同 working tree 构建。
6. 回滚必须回滚到已知可用的生产 artifact / `main` commit；禁止用旧 feature branch 覆盖生产目录。
7. 数据库迁移只能随 `main` 发布，并必须向后兼容当前线上版本；迁移和应用发布要有明确顺序及回滚方案。
8. 修改共享文件前必须先检查当前 diff，并在 PR 中声明 owner、必要性和影响范围；同一时间同一段共享代码只能由一个 PR 负责。
9. 原始混合目录只用于观察。发现其中存在跨业务未提交改动时，不得打包、提交、合并或部署整个目录。
10. 上线前必须核对 application commit、deploy commit、artifact id；上线后必须对 `www`、`intl` 和关键 API 做 smoke test。

任何 workflow 若允许非 `main` 分支进入 production environment，都视为阻断上线的问题，必须先修复。

## Git Safety Rules

在原始混合目录禁止执行：

- `git add .`
- `git reset --hard`
- `git clean -fd`
- `git stash`
- `git checkout`
- `git switch`
- `git rebase`
- `git pull --rebase`

提交前必须确认：

```bash
git status --short
git diff --name-only
git diff --cached --name-only
```

只允许 stage 当前业务线所属文件。不要使用 `git add .`。
