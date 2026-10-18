# Branch, PR and Production Deployment Governance

> 生效日期：2026-07-14
>
> 适用范围：JATO Analysis System 的所有 Codex session、人工开发、CI、部署和回滚。
> 本文是分支与生产部署的唯一详细规范；根目录 `AGENTS.md` 是强制摘要。

## 1. 为什么需要这份规范

2026-07-13 的实际 Git 和 GitHub Actions 记录表明：一个基于旧 merge-base 的 feature branch
成功触发了生产部署。该 branch 没有回退 `main`，但部署任务把整套旧 `06_AppPlatform`
覆盖到服务器，导致已在 `main` 的 BOM Admin / Order Genius 功能在线上暂时回退。

这次事故证明需要分开管理三个概念：

- application commit：实际业务代码版本；
- deploy commit：触发部署的版本；
- artifact：真正部署到服务器的不可变构建产物。

生产环境不能只看“workflow 成功”，必须确认这三者都来自预期的 `main` 发布。

## 2. 唯一开发模型

```text
一个 session = 一个 worktree = 一个 branch = 一个 PR
```

要求：

1. 每个功能线使用自己的 worktree 和 `codex/*` branch。
2. 一个 branch 只解决一个明确 scope，不混入顺手修复和其他业务线文件。
3. 原始目录 `/Users/litristan/Downloads/JATO_Analysis_System` 是混合观察区，默认只读。
4. 原始目录中的 dirty files 不得整体 stage、commit、打包或部署。
5. 一个 session 结束后，PR merge 前保留 worktree；merge 且确认不再需要后再删除 worktree 和 branch。
6. 已结束功能若要继续，创建新的 branch / PR，不在已 merge 的 branch 上继续堆提交。

当前长期业务线登记在根目录 `AGENTS.md`。branch 或 worktree 发生正式调整时，必须同步更新登记。

## 3. Branch 和共享文件规则

### 3.1 Branch

- `main`：受保护、可部署、唯一生产来源；禁止 session 直接 push。
- `codex/<feature>`：正常功能和修复分支，只运行 CI / preview。
- `codex/integration-<date>`：临时集成分支，只用于解决多 PR 冲突和预览。
- hotfix 也必须从最新 `main` 创建 branch、走 PR、merge 后部署。

禁止：

- 从旧 merge-base 的 feature branch 部署整套应用；
- 在 feature branch 中放宽 production workflow trigger；
- 用 force push、旧 branch 或本地 dirty tree 覆盖生产；
- 把 integration branch 当作长期开发区或生产来源。

### 3.2 共享文件

典型共享文件包括：

- `06_AppPlatform/frontend/src/App.tsx`
- `06_AppPlatform/frontend/src/api/client.ts`
- `06_AppPlatform/frontend/src/types/*`
- `06_AppPlatform/backend/app/main.py`
- `06_AppPlatform/backend/app/core/config.py`
- `.github/workflows/*`
- 部署脚本和数据库基础模型

修改前必须：

1. 检查当前 diff 和其他活跃 PR；
2. 明确哪个 PR 是 owner；
3. 只改当前需求必需的最小段落；
4. 在 PR description 写明原因、影响范围和冲突风险；
5. 同一时间同一段共享代码只允许一个 PR 修改。

## 4. PR 流程

```text
最新 main -> 独立 worktree/branch -> 实现与测试 -> PR -> review -> merge main
```

提交前执行：

```bash
git status --short
git diff --name-only
git diff --cached --name-only
```

只 stage 当前 scope 的明确文件，禁止 `git add .`。

PR 必须说明：

- 目标和不在 scope 内的内容；
- 前端交互/布局变化；
- 后端技术栈、复用的 service/repository/schema；
- 共享文件变更及 owner；
- 数据库迁移顺序和兼容性；
- 测试和 smoke-test 证据；
- 是否需要生产部署以及预期 build metadata。

多条 PR 要一起验证时，创建临时 integration branch：

1. 从最新 `main` 创建；
2. 合入候选 PR，仅用于 CI / preview；
3. 冲突修复回到各自 PR 或新建专门 integration PR；
4. 各 PR 正式 merge `main` 后删除 integration branch；
5. integration branch 永不直接部署生产。

## 5. Production 部署门禁

### 5.1 唯一允许路径

```text
PR merged -> protected main -> build once -> immutable artifact
                                      |-> www
                                      |-> intl
```

硬性要求：

1. production job 必须同时满足：
   - `github.ref == 'refs/heads/main'`；
   - 使用 GitHub `production` environment；
   - environment 只允许 `main`；
   - 推荐配置人工 approval。
2. feature branch workflow 只执行 lint、test、build 和 preview。
3. `www` 与 `intl` 必须部署同一 artifact，禁止分别 checkout/build。
4. artifact 至少记录：
   - application commit SHA；
   - deploy commit SHA；
   - build timestamp；
   - artifact id / checksum；
   - migration revision。
5. 服务器部署接收 artifact，不自行切换 feature branch。

如果 workflow 允许 feature branch 进入 production environment，必须先修 workflow，不能继续上线。

### 5.2 数据库迁移

后端为 FastAPI + SQLAlchemy + PostgreSQL，迁移使用 Alembic。要求：

1. migration 只能随已合入 `main` 的 release 执行；
2. 优先采用 expand / migrate / contract，保证新旧应用短时共存；
3. destructive migration 必须拆阶段并提供备份、验证和回滚说明；
4. migration 失败时停止应用切换，不得继续发布半套版本；
5. rollback 前确认旧应用是否兼容已执行 schema。

## 6. 上线验证

部署前核对：

- [ ] PR 已 merge 到 `main`
- [ ] production run 的 source branch 是 `main`
- [ ] artifact application commit 是预期 commit
- [ ] `www` 与 `intl` 使用同一 artifact id/checksum
- [ ] migration revision 与 release 一致

部署后验证：

- [ ] `www.ojeur.cloud/build-meta.json`
- [ ] `intl.ojeur.cloud/build-meta.json`
- [ ] 两个域名的 application commit、artifact id 一致
- [ ] backend health 和关键 API 返回正常
- [ ] 本次功能 smoke test 通过
- [ ] 与本次 scope 相邻的高风险功能没有回退
- [ ] 日志无新增 500 / migration / undefined-field 异常

## 7. 回滚规则

正确回滚：

1. 选择上一份已通过 smoke test 的 production artifact；
2. 核对数据库 schema 兼容性；
3. 将同一 artifact 同步回滚到 `www` 和 `intl`；
4. 验证 build metadata、关键 API 和核心页面；
5. 记录 incident、触发原因、回滚 artifact 和后续修复 PR。

禁止回滚方式：

- checkout 某个旧 feature branch 后重新部署；
- 从本地 dirty worktree 打包；
- 只回滚一个域名；
- 未核对 migration 就回退应用；
- 用“重新跑一次 workflow”代替版本核对。

## 8. Incident 审计顺序

发现线上功能回退或域名不一致时，按以下顺序排查：

1. 读取 `www` 和 `intl` 的 build metadata；
2. 确认 production run id、source branch、deploy commit；
3. 确认 application commit 和 artifact id；
4. 对比 production application commit 与当前 `main`；
5. 查 feature branch 的 merge-base，判断是否部署了旧树；
6. 检查 migration revision 和 backend 日志；
7. 先恢复已知可用 artifact，再通过 PR 修根因。

不要仅根据 commit message 或用户提供的候选 SHA 下结论，必须检查实际 Git history、workflow run 和线上 metadata。

## 9. Session 结束清单

- [ ] 当前工作只在分配的 worktree / branch
- [ ] 没有跨业务线文件
- [ ] shared files 已声明 owner
- [ ] 测试和截图/日志证据完整
- [ ] 已提交并推送 feature branch
- [ ] 已创建 PR，未直接 push `main`
- [ ] 若需上线，等待 merge 后的 main production run
- [ ] merge 后确认 production 与 preview 结果一致
- [ ] worktree 仅在 PR 完成且不再需要时清理

## 10. 当前事故的直接结论

- 单纯修改部署脚本的 commit 不是 BOM 回退根因。
- `main` 没有被回退，不代表生产没有被旧代码覆盖。
- feature branch 成功运行 production workflow，同样可以造成整站版本回退。
- 因此必须同时保护 branch、environment 和 artifact 三层，不能只依赖“main 看起来没变”。
