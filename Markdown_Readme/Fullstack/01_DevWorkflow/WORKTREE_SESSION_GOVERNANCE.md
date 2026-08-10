# Branch, PR and Production Deployment Governance

> 最后修订：2026-07-23
>
> 适用范围：JATO Analysis System 的所有 Codex session、人工开发、PR、CI、生产发布、回滚和本地清理。
>
> 根目录 `AGENTS.md` 是强制摘要；发布依赖的机器可读契约以
> `.github/RELEASE_COORDINATION.md` 为准。

## 1. 目标与事故教训

本规范同时防止两类问题：

1. 多个 session 在同一混合目录开发，导致不同业务线的未提交内容被误带、覆盖或丢失；
2. 基于旧 `main` 的 feature branch 直接部署整棵代码树，导致已经合入 `main` 的功能在线上暂时回退。

需要区分：

- **application commit**：发布产物实际包含的业务代码；
- **target main SHA**：本次生产计划锁定的 current `main`；
- **artifact**：由该 SHA 构建、带 checksum 的不可变发布包；
- **deploy run**：验证并部署该 artifact 的 GitHub Actions run；
- **coordination contract**：多个 PR 存在发布依赖时，随 merge commit 永久保存的机器可读凭据。

“PR 合并成功”不等于“可以从该 feature branch 发布”。生产只能从合并后的 current
`main` 构建，所以先合入的功能 A 会自然包含在后合入功能 B 之后的 `main` artifact 中。

## 2. 会写仓库的 session 模型

```text
一个 session = 一个 worktree = 一个 codex/* branch = 一个 PR
```

该模型适用于会修改仓库内容的开发 session。纯只读审计可以在观察区执行，但不能产生文件改动。

worktree 和 branch 是任务资源，不是长期固定的业务目录。禁止维护容易过期的“BOM 永远用某 branch、JATO 永远用某 branch”静态登记。

### 2.1 创建任务

1. 获取最新远端 `main`；
2. 从该 SHA 创建全新 worktree 和 `codex/<scope>` branch；
3. 在开始写入前记录：
   - worktree 绝对路径；
   - branch；
   - base SHA；
   - PR 目标；
   - owned files；
   - 可能涉及的 shared files 及 owner；
4. 确认 `git status --short` 为空；
5. 确认没有复用已 merge branch 或另一个 PR 的 worktree。

参考命令：

```bash
git fetch <remote> main
git worktree add -b codex/<scope> <new-path> <remote>/main
git -C <new-path> status --short
git -C <new-path> rev-parse HEAD
```

`<remote>`、`<scope>` 和 `<new-path>` 必须使用当前仓库真实值，不复制历史 session 的固定映射。

### 2.2 混合观察区

`/Users/litristan/Downloads/JATO_Analysis_System` 是历史混合观察区：

- 允许只读审计、归属识别和迁移前对比；
- 禁止新增功能、stage、commit、打包、部署、stash、reset、clean、checkout、switch、rebase；
- 不得因为某一业务线已迁移，就批量删除剩余未归属内容；
- dirty tracked files 和 untracked files 必须分别归属、迁移、归档或明确放弃，不能默认视为垃圾。

### 2.3 开发与提交

一个 branch 只处理 PR 声明的 scope。提交前执行：

```bash
git status --short
git diff --name-only
git diff --cached --name-only
```

逐文件 stage，禁止 `git add .`。运行产物、数据缓存、上传副本、密钥和其他 session 文件不能进入 PR。

### 2.4 任务结束

PR 开放期间保留 worktree。PR 合并后仍需确认：

- CI 和生产验证已完成；
- 没有活跃任务仍引用路径；
- worktree clean；
- `HEAD` 已进入 current remote `main`；或 GitHub 证明该精确 `HEAD` 是已合并
  PR 的最终 head，且没有 merge 后新增提交；
- branch 没有未审查的独有提交或恢复证据。

满足全部条件后，使用非强制方式删除 worktree，再用 `git branch -d` 删除本地 branch。
若 squash/rebase merge 导致 `-d` 拒绝，则保留 branch 继续审计，不改用 `-D`。

## 3. Branch 角色

- `main`：受保护、始终可部署、唯一生产来源；禁止 session 直接 push。
- `codex/<scope>`：短生命周期功能、修复、文档或治理分支；只运行 CI/preview。
- `codex/integration-<scope>`：必要时创建的临时冲突验证分支；只运行 CI/preview。
- hotfix：同样从 latest `main` 新建 `codex/*` branch、走 PR，merge 后才发布。

禁止：

- 从旧 merge-base 的 feature branch 部署整套应用；
- 让 `workflow_dispatch` 的非 main ref 进入 production；
- 用 force push、旧 branch 或 dirty tree 覆盖生产；
- 在已 merge branch 上继续堆新功能；
- 把 integration branch 当作依赖管理、长期开发区或生产来源。

## 4. Shared-file ownership

典型共享文件：

- 前端入口、导航、全局类型和 API client；
- 后端 `main.py`、config、database model 和公共 serializer；
- `.github/workflows/*`、部署脚本和 release validators；
- Alembic chain、公共 schema 和跨业务测试基线。

修改前必须：

1. 检查 current `main`、当前 worktree diff 和开放 PR；
2. 确认唯一 owner PR；
3. 精确到 symbol/hunk 说明修改必要性；
4. 说明与其他业务线的依赖和 merge 顺序；
5. 只改最小路径；
6. 在 PR description 列出 shared files 和验证证据。

如果两个 PR 需要同一 hunk，先选定一个 owner。另一个 PR 声明依赖，或在 owner 合并后的 latest
`main` 上重放自己的最小改动；不能让两个 session 同时覆盖。

## 5. PR 与发布协同

### 5.1 默认独立

大多数 PR 使用：

```text
Release-Group: independent
Depends-On: none
```

独立表示：该 PR 合入 `main` 后，即使没有同时发布其他开放 PR，目标树仍功能完整且安全。

### 5.2 有依赖的 PR

如果功能 B 在代码、迁移、contract 或上线顺序上依赖功能 A，必须遵循
`.github/RELEASE_COORDINATION.md`：

- 在 PR body 使用严格 trailer；
- 添加属于本 PR 的不可变
  `.github/release-coordination/contracts/pr-<PR>.json`；
- dependency PR 必须已 merge、target `main`，且 merge SHA 是目标 main 的祖先；
- coordinated group 的非 anchor 先合并，anchor 最后合并；
- open、draft、closed-unmerged、循环依赖、API 不可验证或 contract 不一致全部 fail closed。

integration branch 不能代替 contract。PR 标题、聊天记录和“记得一起上”也不能作为发布依据。

### 5.3 防止 A 被 B 回退

合并 B 前必须检查 GitHub 计算出的 merge result，而不是只看 B branch：

1. B 的 diff 只包含自己的 scope；
2. B 没有删除 current `main` 中 A 的文件、route、test 或 workflow；
3. 冲突以 current `main` 为基线解决；
4. 对 A 的高风险入口保留回归测试；
5. 生产从 B 合并后的 current `main` 重新构建一次完整 artifact。

即使 B branch 的 merge-base 早于 A，只要 PR merge result 正确且生产只从合并后 `main`
构建，A 不会因 B 的 feature tree 直接覆盖而回退。若 A 与 B 必须同批才完整，则使用 release
coordination contract，不用手工猜测合并顺序。

## 6. Production release

唯一允许路径：

```text
PR merge
   ↓
current protected main
   ↓
release-coordination preflight
   ↓
build once → immutable artifact + checksum + provenance
   ├── www
   └── intl
```

生产实现、artifact provenance、双域名一致性、恢复行为和验证命令以
`.github/PRODUCTION_FRONTEND_RELEASE.md` 为唯一发布事实来源；PR dependency、release
group、immutable contract 和 frozen release plan 以
`.github/RELEASE_COORDINATION.md` 为唯一协同事实来源。

本治理文档只约束边界：

- 生产 target 必须是审批时仍有效的 current remote `main`；
- `www` 与 `intl` 必须消费同一 immutable artifact；
- feature、hotfix、integration branch 不能进入 production；
- 服务器不能 checkout、pull、build 或切换 feature branch；
- 任一身份、SHA、contract、artifact 或 checksum 无法证明时 fail closed。

## 7. Database migration

后端为 FastAPI + SQLAlchemy + PostgreSQL，迁移使用 Alembic：

1. migration 只能随已合入 `main` 的 release 执行；
2. 优先使用 expand → migrate → contract，保证新旧应用短时兼容；
3. destructive migration 必须拆阶段并提供备份、验证和回滚说明；
4. migration 失败时停止应用切换；
5. 应用回滚前先确认旧应用兼容当前 schema；
6. 多 PR 共享 migration chain 时必须声明依赖和顺序。

## 8. 上线验证与回滚

部署前：

- [ ] target 是 current remote `main`
- [ ] frozen release plan 与 run、attempt、target SHA 一致
- [ ] 所有 dependency / group contract 通过
- [ ] artifact application commit 与 target SHA 一致
- [ ] `www` 与 `intl` 使用同一 artifact id/checksum
- [ ] migration revision、执行顺序和回滚说明已确认

部署后：

- [ ] `www` 和 `intl` build metadata 一致
- [ ] backend health 和关键 API 正常
- [ ] 本 PR 功能 smoke test 通过
- [ ] 先前已上线的高风险相邻功能没有回退
- [ ] 日志无新增 500、migration 或 undefined-field 异常

回滚只能选择上一份已验证 production artifact，并同时回滚双域名。禁止 checkout 旧 feature
branch、从 dirty worktree 重打包、只回滚一个域名或在未核对 schema 时回退应用。

## 9. Incident 审计顺序

发现功能回退或双域名不一致时：

1. 读取 `www`、`intl` build metadata；
2. 确认 production run、target SHA、application commit、artifact checksum；
3. 对比 artifact commit 与当时 current `main`；
4. 检查 release plan 和 dependency contracts；
5. 检查 migration revision 和 backend 日志；
6. 先恢复已知成功 artifact；
7. 从最新 `main` 新建独立修复 PR。

不能只根据 commit message、feature branch HEAD 或“workflow 成功”判断生产实际版本。

## 10. 混合观察区退役

混合观察区只能逐步退役。每批清理都必须留下审计结论，并同时满足：

1. 活跃 session 已迁到独立 worktree；
2. tracked diff 已逐文件归属、迁移、归档或明确放弃；
3. untracked 内容已区分源码、用户资产和可再生产运行产物；
4. 没有开放 PR、独有 commit 或恢复任务依赖该目录；
5. 拟删除 worktree clean，且其 `HEAD` 已进入 current `main`；或其精确 HEAD 已由
   GitHub 证明为 merged PR 的最终 head、没有后续提交；
6. 删除使用非强制 `git worktree remove` 和 `git branch -d`。

dirty、未归属、仍被任务引用、PR 未合并或含独有提交的项目一律保留。禁止为了“看起来干净”
而使用 `git clean -fd`、`git reset --hard`、`git branch -D` 或批量远端删 branch。

## 11. Session close checklist

- [ ] 当前工作只在本任务 worktree / branch
- [ ] branch 基于任务创建时的 latest remote `main`
- [ ] diff 没有跨业务文件或运行产物
- [ ] shared files 已声明 owner
- [ ] 前端交互或后端技术栈与复用说明完整
- [ ] focused tests 和必要回归测试通过
- [ ] 已创建 PR，未直接 push `main`
- [ ] release trailers / contract 与真实依赖一致
- [ ] 需要上线时只等待 merge 后的 main production run
- [ ] PR 完成后按安全条件清理 worktree 和本地 branch
