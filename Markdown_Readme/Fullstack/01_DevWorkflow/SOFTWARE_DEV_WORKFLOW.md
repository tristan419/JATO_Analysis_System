# JATO Analysis System — 软件开发流程（2026-04-11）

> 本文档定义项目从需求到上线的完整开发流程。
> 目标：让开发过程专业化、可审计、可复现。

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

1. 明确 contract owner（默认：后端 serializer）
2. 后端先行 → 前端 type 对齐 → 页面实现
3. 跨层变更前后端各至少补 1 条 unit test

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
4. 如有新文档 → 更新 `ROADMAP.md`

### 2.6 审核阶段

审核者检查：

- [ ] checklist 是否完整勾选
- [ ] 测试是否通过
- [ ] contract 是否一致
- [ ] 文档是否更新

### 2.7 部署阶段

按 `BRANCH_PR_DEPLOYMENT_GOVERNANCE_2026-07-14.md`、`MANUAL_CICD.md` 和
`TENCENT_CLOUD_DEPLOY.md` 执行：

1. feature branch 通过 CI 和 preview，不直接进入生产环境
2. PR 审核后 merge 到受保护的 `main`
3. CI 从 `main` commit 构建一次 immutable artifact
4. `www` 和 `intl` 部署同一 artifact
5. 数据库迁移按审核后的顺序执行，随后重启 backend、切换 frontend
6. 核对 build metadata，并完成双域名和关键 API smoke check

服务器端不得通过切换或拉取旧 feature branch 作为部署/回滚手段。

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
│   ├── BRANCH_PR_DEPLOYMENT_GOVERNANCE_*.md # 分支、PR、部署、回滚治理
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

当前采用受保护 main + 短生命周期 feature branch：

1. 一个 session = 一个 worktree = 一个 `codex/*` branch = 一个 PR
2. 原始混合目录只读，不作为功能开发、提交或部署目录
3. `main` 始终可部署，并且是唯一生产部署来源
4. feature / hotfix / integration 分支只允许 CI 和 preview
5. 禁止直接 push 到 `main`；PR 合入前必须本地测试和 CI 通过
6. integration 分支只用于临时冲突验证，验证结束后删除
7. 不使用 force push，不以旧 branch 覆盖生产环境

完整规则及异常处理见
`BRANCH_PR_DEPLOYMENT_GOVERNANCE_2026-07-14.md`。

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
| PR | feature branch 不包含生产 deploy 权限 | 阻断合并 |
| 部署 | 来源为受保护 `main` + immutable artifact | 阻断上线 |
| 部署后 | `www`、`intl`、关键 API smoke check 通过 | 回滚到上一生产 artifact |

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
