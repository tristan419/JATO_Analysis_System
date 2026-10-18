# JATO Analysis System — 软件开发流程（2026-07-23）

> 本文档定义项目从需求到上线的完整开发流程。
> 目标：让开发过程专业化、可审计、可复现。
> 分支、worktree、发布协同和生产门禁的详细规则见
> `WORKTREE_SESSION_GOVERNANCE.md`。

---

## 1. 角色定义

| 角色 | 职责 |
|------|------|
| **产品负责人（PO）** | 提需求、验收功能、优先级排序 |
| **开发者** | 实现功能、写测试、本地验证 |
| **审核者** | Review PR、检查 checklist、合入 |

> 当前团队规模下 PO 和审核者可以是同一人，但流程不能省略。

---

## 2. 开发生命周期

```
需求 → 预研/Spec → 实现 → 测试 → PR → 审核 → 合入 → 部署 → 验证
  │       │          │       │      │      │       │       │       │
  ▼       ▼          ▼       ▼      ▼      ▼       ▼       ▼       ▼
 Issue   Tech Path   Code   Unit   PR    Check-  Merge   Deploy  Smoke
 / PRD   / Spec Doc  +Type  Test   +CL   list           Script   Check
```

### 2.1 需求阶段

1. PO 写需求描述（可以是自然语言，一段话即可）
2. 需求关联到一个对应的 **技术路径文档**（如 `MSRP/02_Research_Design/TECH_PATH_SCRAPE_AND_UPLOAD.md`）或使用已有 spec
3. 如果是跨层功能，必须先确认 contract（参考 `FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md`）

### 2.2 预研 / Spec 阶段

- 新功能：写 spec 文档或 tech path 文档放入 `Markdown_Readme/Fullstack/`
- UI 变更：更新 `UI_SPECIFICATION_V1.md` 或在 `Markdown_Readme/UI/` 写专题
- 纯 bug fix：直接进入实现阶段，但 commit message 需附根因

### 2.3 实现阶段

按 `FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md` 第 4 节执行：

1. 从 latest remote `main` 创建独立 worktree 和 `codex/*` branch
2. 明确 PR scope、owned files 和 shared-file owner
3. 明确 contract owner（默认：后端 serializer）
4. 后端先行 → 前端 type 对齐 → 页面实现
5. 跨层变更前后端各至少补 1 条 unit test

### 2.4 测试阶段

| 层 | 命令 | 覆盖范围 |
|----|------|---------|
| 前端 | `npm run check:frontend` | tsc + vitest + vite build + router regression |
| 后端 | `python -m pytest tests/unit` | serializer contract + 业务逻辑 |

测试通过后才允许提交。

### 2.5 PR 阶段

1. 按 `PR_CHECKLIST.md` 逐项勾选
2. commit message 格式：`feat/fix/refactor(scope): 一句话描述`
3. PR description 包含：变更目的、影响范围、验证步骤
4. 声明 `Release-Group` / `Depends-On`；有依赖时按
   `.github/RELEASE_COORDINATION.md` 添加 immutable contract
5. 如有新文档 → 更新主入口或 `ROADMAP.md`

### 2.6 审核阶段

审核者检查：

- [ ] checklist 是否完整勾选
- [ ] 测试是否通过
- [ ] contract 是否一致
- [ ] 文档是否更新

### 2.7 部署阶段

治理边界按 `WORKTREE_SESSION_GOVERNANCE.md` 执行；机器发布契约以
`.github/PRODUCTION_FRONTEND_RELEASE.md` 和 `.github/RELEASE_COORDINATION.md`
为准：

1. feature branch 只运行 CI/preview，不直接进入 production
2. PR 审核后 merge 到受保护的 `main`
3. release preflight 验证 current `main` 及未发布 PR 的 coordination contract
4. CI 从目标 `main` 构建一次 immutable artifact
5. production approval 后重新验证 target 仍是 current `main`
6. `www` 和 `intl` 校验并部署同一 artifact
7. 按审核顺序执行兼容 migration，完成双域名和关键 API smoke check

服务器端不得 checkout、pull、切换或重新构建 feature branch；回滚只能使用已验证的
production artifact。

### 2.8 验证阶段

- 功能验证：按需求描述逐条验证
- 日志检查：无 500 / 无 undefined field
- 性能观察：首屏加载无明显退化

---

## 3. 文档体系

```
Markdown_Readme/
├── Fullstack/
│   ├── ROADMAP.md                          # 唯一主索引
│   ├── FULLSTACK_DEVELOPMENT_SPEC_*.md     # 开发规范
│   ├── PR_CHECKLIST.md                     # PR 清单
│   ├── 01_DevWorkflow/
│   │   └── WORKTREE_SESSION_GOVERNANCE.md
│   ├── TECH_PATH_*.md                      # 技术路径预研
│   ├── STREAMLIT_VS_REACT_COMPARISON.md    # 功能对齐
│   ├── FULLSTACK_LOCAL_DEBUG.md            # 调试指南
│   ├── MANUAL_CICD.md                      # 发布流程
│   ├── TENCENT_CLOUD_DEPLOY.md             # 部署指南
│   └── ...
└── UI/
    ├── UI_SPECIFICATION_V1.md               # UI 规范（本次新增）
    ├── Dashboard_Layout_Baseline_*.md       # 布局基线
    ├── Dashboard_Shared_Filter_Reuse.md     # 复用抽象
    └── BMW.md                               # 设计灵感
```

### 3.1 文档规则

- `ROADMAP.md` 是唯一主索引，新文档必须回填
- spec 类文档命名包含日期：`*_2026-04-11.md`
- 不创建多级子索引

---

## 4. 分支策略

当前采用受保护 `main` + 短生命周期 task branch：

1. 一个 session = 一个 worktree = 一个 `codex/*` branch = 一个 PR
2. 每个新任务从当时 latest remote `main` 开始，不维护固定长期业务 branch
3. 原始混合观察区只读，不用于开发、提交、打包或部署
4. `main` 始终可部署，并且是唯一生产来源
5. feature / hotfix / integration branch 只运行 CI/preview
6. integration branch 只用于临时冲突验证，不代替 release dependency contract
7. 禁止直接 push `main`、force 覆盖和从旧 feature tree 发布

完整规则见 `WORKTREE_SESSION_GOVERNANCE.md`。

---

## 5. 需求 → 文档映射

PO 提需求后，开发者按以下规则确定文档动作：

| 需求类型 | 动作 |
|----------|------|
| 新功能（跨层） | 写 spec + tech path → 拆 PR → 按 checklist 执行 |
| 新功能（纯前端） | 写 UI 变更说明 → 更新 `UI_SPECIFICATION_V1.md` |
| Bug fix | commit message 附根因 → 按 checklist 执行 |
| 性能优化 | 更新 `DASHBOARD_PERFORMANCE_TODOS_50.md` |
| 部署/运维 | 更新 `MANUAL_CICD.md` 或 `TENCENT_CLOUD_DEPLOY.md` |

---

## 6. 质量门禁

| 阶段 | 门禁 | 失败处理 |
|------|------|---------|
| 本地 | `check:frontend` 全过 | 不允许提交 |
| 本地 | `pytest tests/unit` 全过 | 不允许提交 |
| PR | checklist 完整勾选 | 退回修改 |
| PR | release coordination trailer / contract 一致 | 阻断合并 |
| 部署 | target 为 current `main` + frozen plan + immutable artifact | 阻断上线 |
| 部署后 | 双域名、backend、关键 API 和相邻功能 smoke check | 回滚上一已验证 artifact |

---

## 7. 工具链

| 工具 | 用途 |
|------|------|
| TypeScript + Vite | 前端构建 |
| Vitest | 前端单元测试 |
| React 19 + react-router-dom 7 | 前端框架 |
| FastAPI + SQLAlchemy + Pydantic | 后端框架 |
| pytest | 后端测试 |
| PostgreSQL + Alembic | 数据库 + 迁移 |
| nginx + systemd | 生产部署 |
| Git | 版本控制 |
