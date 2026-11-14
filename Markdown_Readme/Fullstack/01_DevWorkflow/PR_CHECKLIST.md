# PR Checklist（每次功能变更必须逐项勾选）

> 来源：`FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md`
> 适用：任何涉及前后端、数据库、接口 contract 的改动。纯样式/文案调整可跳过标注 ★ 的条目。

## Worktree 与 scope

- [ ] 当前任务使用从 latest remote `main` 创建的独立 worktree 和 `codex/*` branch
- [ ] 已记录 worktree、branch、base SHA、PR scope 和 owned files
- [ ] 没有在 `/Users/litristan/Downloads/JATO_Analysis_System` 混合观察区开发
- [ ] 当前 branch 只服务一个 PR，未混入其他 session、运行产物或顺手修复
- [ ] 如需修改共享文件，已声明唯一 owner、精确 hunk、必要性和影响范围

## 开发前

- [ ] ★ 确认数据来源：字段来自哪个 endpoint / serializer / table
- [ ] ★ 确认 canonical contract：以 `payload_serializers.py`（或对应 service 层 payload 函数）为唯一事实来源
- [ ] ★ 确认字段语义：不混用不同 entity 的同名/近似字段（如 `msrpValue` vs `currentMsrpValue`）
- [ ] 确认 fallback 规则：是否兼容旧字段，兼容期限，谁负责移除
- [ ] 确认错误态：空数据、null、接口失败时的页面展示方案
- [ ] ★ 至少 1 条正常样例 + 1 条空值样例写进 commit message 或 PR description

## 实现中

- [ ] 后端 serializer 改动 → 同步更新前端 `types/index.ts`
- [ ] 前端类型改动 → 同步更新页面 helper / adapter（不散落内联 fallback）
- [ ] 新增/重命名字段 → 同步更新前后端 unit test
- [ ] 跨层 contract 变更 → 前后端各至少补 1 条 unit test

## 实现后 — 本地验证

- [ ] 前端通过：`cd 06_AppPlatform/frontend && npm run check:frontend`
  - tsc --noEmit ✓
  - vitest run ✓
  - vite build ✓
  - router regression ✓
- [ ] 后端通过：`cd 06_AppPlatform/backend && python -m pytest tests/unit`
- [ ] 如有新页面/新路由 → 确认 router regression 脚本已覆盖

## 提交前

- [ ] 已检查 `git status --short`、`git diff --name-only`、`git diff --cached --name-only`
- [ ] staged files 只属于当前 PR；逐文件 stage，未使用 `git add .`
- [ ] PR merge result 未删除或回退 current `main` 已有功能、route、test 或 workflow
- [ ] 未直接 push `main`，未把 integration branch 当作生产来源
- [ ] commit message 格式：`feat/fix/refactor(scope): 一句话描述`
- [ ] PR description 包含：变更目的、影响范围、验证步骤
- [ ] 更新 `ROADMAP.md` 状态（如果涉及新文档或阶段变更）
- [ ] 相关文档入口已回填（如新增了 spec / 设计文档）

## Release coordination

- [ ] 默认独立 PR 使用 `Release-Group: independent` 和 `Depends-On: none`
- [ ] 如有 PR 依赖或同批发布要求，已按 `.github/RELEASE_COORDINATION.md` 添加 trailer 和本 PR 的 immutable contract
- [ ] integration branch 只用于 CI/preview，未代替依赖声明
- [ ] 数据库迁移只随 `main` 发布，已说明兼容、顺序、备份和回滚

## 部署后

- [ ] 生产 target 是审批后仍为 current remote `main` 的 SHA
- [ ] `www` 和 `intl` 使用同一个 immutable artifact 和 checksum
- [ ] build metadata 中 application commit、artifact id 和 provenance 符合预期
- [ ] 双域名、backend health、关键 API 和本次功能均已验证
- [ ] 先前已上线的高风险相邻功能没有回退
- [ ] 日志无异常 / 无 500 / 无 undefined field

## 任务结束与本地清理

- [ ] PR 已 merged/closed，且没有活跃任务继续引用 worktree
- [ ] 已复查 dirty、untracked、未推送提交、PR 最终 head 和 current `main`
- [ ] worktree 无未迁移资产；否则已 quarantine 并登记原因
- [ ] 只使用非强制 `git worktree remove` 和 `git branch -d`；拒绝时未改用强删
