# AGENTS.md

## Project interaction rules

- 写前端时，每次涉及交互逻辑和页面布局，都要用语言描述用户会看到什么、如何操作，以及加载、错误、空态和防重复提交行为。
- 写后端时，要说明技术栈、数据边界和失败模式，并优先复用现有 service、repository、schema、serializer、任务状态对象和校验函数。

## Mandatory session model

本项目所有会修改仓库内容的开发任务采用：

```text
一个 session = 一个 worktree = 一个 branch = 一个 PR
```

这些 worktree 和 branch 都是短生命周期资源，不登记长期固定的业务线目录。每个新任务必须：

1. 从当时最新的远端 `main` 创建全新的 worktree；
2. 使用唯一的 `codex/<scope>` branch；
3. 只处理一个明确 PR scope；
4. 在写入前声明 worktree、branch、base SHA、文件范围和共享文件 owner；
5. PR 合并且确认没有活跃任务、未提交内容或独有提交后，再清理 worktree 和本地 branch。

原始目录 `/Users/litristan/Downloads/JATO_Analysis_System` 是历史混合观察区，默认只读。纯只读审计可以在这里进行，但它不能作为新功能开发、stage、commit、打包、部署或清理的来源。

## Start-of-task self-check

开始修改前必须执行并核对：

```bash
git rev-parse --show-toplevel
git worktree list
git branch --show-current
git status --short
git rev-parse HEAD
```

随后以只读方式获取最新远端 `main`，确认当前 worktree 的 base。若发现：

- 当前目录是混合观察区；
- branch 同时承载另一个 PR；
- worktree 含不属于本任务的改动；
- base 不是本任务创建时的最新远端 `main`；
- 计划修改的共享代码已有其他活跃 owner；

必须停止扩大改动，先迁移到干净 worktree 或协调 owner。

## Scope and shared-file ownership

一个 PR 只解决一个业务目标，不混入顺手修复、运行产物或其他 session 的文件。

典型共享文件包括：

- `06_AppPlatform/frontend/src/App.tsx`
- `06_AppPlatform/frontend/src/api/client.ts`
- `06_AppPlatform/frontend/src/types/*`
- `06_AppPlatform/backend/app/main.py`
- `06_AppPlatform/backend/app/core/config.py`
- `06_AppPlatform/backend/app/db/models.py`
- `.github/workflows/*`
- 发布脚本、数据库迁移和基础 schema

修改共享文件前必须检查当前 diff 和开放 PR，明确唯一 owner、必要 symbol/hunk、依赖和冲突风险。同一时间同一段共享代码只能由一个 PR 负责。

## Git safety rules

所有 worktree 均禁止：

- `git add .`
- 直接 push `main`
- 用 force push 覆盖他人提交
- 把 dirty tree 打包或部署
- 把另一个业务 branch 整体 cherry-pick 进当前 PR

在原始混合观察区还禁止：

- `git reset --hard`
- `git clean -fd`
- `git stash`
- `git checkout`
- `git switch`
- `git rebase`
- `git pull --rebase`
- 删除、移动或批量格式化现有文件

提交前必须确认：

```bash
git status --short
git diff --name-only
git diff --cached --name-only
```

只允许逐个 stage 当前 PR 的明确文件。

## PR and release coordination

所有变更通过 PR 合入受保护的 `main`。默认每个 PR 都可独立发布：

```text
Release-Group: independent
Depends-On: none
```

如果 PR 之间存在发布依赖或必须同批上线，必须遵循
`.github/RELEASE_COORDINATION.md`，提交不可变 coordination contract。不能用口头约定、合并顺序或 integration branch 代替依赖声明。

integration branch 只允许短期冲突验证和 preview，不是长期开发分支，也不是生产来源。冲突修复必须回到对应 owner PR，或创建独立、可审查的集成 PR。

## Production deployment rules

1. `main` 是唯一生产来源；feature、hotfix 和 integration branch 只能运行 CI/preview。
2. 生产 workflow 必须验证目标 SHA 仍是 current remote `main`；审批后、读取凭据或产生外部变更前必须再次验证。
3. 一次发布只构建一个 immutable artifact；`www` 与 `intl` 必须使用同一 artifact、checksum 和 application commit。
4. 服务器只能下载并校验发布产物，不能 checkout、pull 或重建旧 feature branch。
5. 数据库迁移只能随 `main` 发布，并说明前后兼容、执行顺序、备份和回滚顺序。
6. 回滚只能选择已知成功的 production artifact；禁止用旧 branch、dirty worktree 或单域名覆盖作为回滚。
7. 发布后必须核验 release SHA、artifact checksum、双域名 build metadata、backend health 和本次高风险相邻功能。

## Safe local cleanup

只有同时满足以下条件时，才允许删除本地 worktree：

- `git status --short` 为空；
- 对应 PR 已合并，且没有开放 PR；
- worktree `HEAD` 是 current remote `main` 的祖先；或 GitHub 能证明该精确
  `HEAD` 是已合并 PR 的最终 head，且没有 merge 后新增提交；
- 没有活跃 Codex session 或人工任务仍引用该目录；
- branch 没有未审查的独有提交、恢复证据或待迁移资产；
- 不是当前操作所在 worktree。

清理时先执行非强制 `git worktree remove <path>`，再执行
`git branch -d <branch>`。若 `-d` 拒绝，则保留 branch 继续审计；禁止 `--force`、
`git branch -D` 和远端 branch 批量删除。

任何 dirty、未合并、无法确认 owner、仍被任务引用或包含独有提交的 worktree/branch 都必须保留并登记原因。

详细流程见：

`Markdown_Readme/Fullstack/01_DevWorkflow/WORKTREE_SESSION_GOVERNANCE.md`
