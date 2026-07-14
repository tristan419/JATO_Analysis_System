# PR Checklist（每次功能变更必须逐项勾选）

> 来源：`FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md`
> 适用：任何涉及前后端、数据库、接口 contract 的改动。纯样式/文案调整可跳过标注 ★ 的条目。

## 开发前

- [ ] 当前任务使用独立 worktree 和 `codex/*` branch；没有在原始混合目录开发
- [ ] 当前 branch 只服务一个 PR，scope 与其他 session 不重叠
- [ ] 如需修改共享文件，已声明 owner、必要性和受影响的业务线
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
- [ ] staged files 只属于当前 PR；未使用 `git add .`
- [ ] PR 不包含 feature branch 直接部署 production 的 workflow / environment 权限
- [ ] 未直接 push `main`，未把 integration branch 当作生产来源
- [ ] commit message 格式：`feat/fix/refactor(scope): 一句话描述`
- [ ] PR description 包含：变更目的、影响范围、验证步骤
- [ ] 更新 `ROADMAP.md` 状态（如果涉及新文档或阶段变更）
- [ ] 相关文档入口已回填（如新增了 spec / 设计文档）

## 部署后

- [ ] 部署来源是已合入的 `main` commit
- [ ] `www` 和 `intl` 使用同一个 immutable artifact
- [ ] build metadata 中 application commit / deploy commit / artifact id 符合预期
- [ ] `www`、`intl` 和关键 API 均已验证
- [ ] 日志无异常 / 无 500 / 无 undefined field
