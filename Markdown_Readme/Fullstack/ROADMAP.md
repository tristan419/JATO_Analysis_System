# JATO Analysis System Roadmap（主索引）

> 本文件是 `Markdown_Readme/` 的唯一入口。
> 目标：在保留历史改动与待办的前提下，保持文档架构精简、可维护、可追溯。

## 1. 维护原则

- `单索引`：仅保留一个总索引（本文件），不再拆分多级索引文档。
- `保历史`：历史报告、评估记录、执行看板不删除，归入 `Streamlit/` 归档目录。
- `保待办`：所有待办统一保留在原看板文档中，不分散到新文件。
- `低耦合`：专题文档只存专题信息，主索引只做导航与状态摘要。

## 2. 当前项目快照（2026-05-14）

- 阶段：`Phase 4 — Fullstack 主线`
- 架构：FastAPI + React + TypeScript（Streamlit 看板功能已全部迁移完成）
- 部署：https://www.ojeur.cloud (Tencent Cloud, nginx + systemd, GitHub Actions CI/CD)
- LLM：DeepSeek V4-Flash (deepseek-chat) — 直连 API，流式 SSE，6-section 结构化报告
- 已完成：
  - Fullstack 迁移、读取层优化、筛选下推、时间变换重构、跨层 contract 测试
  - Country Copilot: snapshot → report 重构，交叉维度因果分析（drive×fuel, registration×fuel, segment×fuel），6-section 报告提示（核心发现→数据证据→因果分析→市场背景→趋势展望→进一步建议），流式 SSE 响应，Token 花费展示（RMB）
  - CurrentPrice 直查 MSRP、engineering variant diff、`JatoMsrpLink + MatchOverride` 生命周期闭环
  - AI News & VOC 智能管线文档（crawler 采集 + LLM 增强）
  - **Version Comparison 版型气泡图 Smart Label System**：Label Mode 四模式（Clean / Smart Top / Selected / All）、优先级分层（P0–P3）、防重叠过滤、确定性 jitter、点击选中 + 框选、按优先级分透明度
- 进行中：**MSRP 官方价格补全系统** — Batch 1+2（7 国 209 个 source）dry-run 通过 92/209 (44.0%)；版本 reconcile 层；sales truth 接入 diff answer；review/workbench mismatch UI。
- 搁置：全球可视化（地球项目）不再继续；Round 3 剩余项保留为待办，不继续实现。

## 3. 文档结构

```
Markdown_Readme/
├── Fullstack/              ← 活跃文档（当前主线）
│   ├── ROADMAP.md          ← 唯一主索引（本文件）
│   ├── 01_DevWorkflow/     ← 开发规范与流程
│   ├── 02_DataETL/         ← 数据处理 / ETL
│   ├── 03_Database/        ← 数据库设计与迁移
│   ├── 04_DevOps/          ← 运维 / 部署 / 调试
│   ├── MSRP/               ← MSRP 官方价格体系（已细分 01–05）
│   ├── _archived/          ← 已搁置或完成的历史文档
│   └── image/
├── UI/                     ← UI 设计与规范
└── Streamlit/              ← 历史归档（Streamlit 看板时期）
    ├── README.md           ← 归档说明
    └── ...
```

- `Fullstack/01_DevWorkflow/`：开发规范、PR 清单、全栈联调规格。
- `Fullstack/02_DataETL/`：ETL 主链路、Baseline/Patch 迁移、预聚合方案。
- `Fullstack/03_Database/`：平台技术栈边界、PostgreSQL Schema、Alembic 迁移、本地开发。
- `Fullstack/04_DevOps/`：云部署、CI/CD、本地联调、Git 代理。
- `Fullstack/MSRP/`：MSRP 官方价格补全系统（PRD → 设计 → 实现 → 执行 → Backlog）。
- `Fullstack/_archived/`：搁置项目（全球可视化）和已完成的迁移历史文档。
- `UI/`：UI 设计语言、布局基线、筛选器复用规范。
- `Streamlit/`：Streamlit 看板时期的部署、性能优化、运维排查文档，**仅供历史追溯**。

## 4. Canonical 文档清单

### 4.1 活跃文档（Fullstack/ & UI/）

| 文档 | 领域 | 状态 | 说明 |
| --- | --- | --- | --- |
| `ROADMAP.md` | 总览 | Active | 唯一主索引 |
| `ARCHITECTURE_REVIEW_2026-04-17.md` | 架构评审 | Active | 跨域架构评审与 P0/P1/P2 改动建议（含后续实现增量说明） |
| `PRODUCT_DEEPDIVE_2026-04-17.md` | 产品/架构 | Active | 六问答卷：跨源 join / MSRP 抓取+配置差异 / Copilot 精准化 / 多源对账 / 统一抓取 |
| **01_DevWorkflow/** | | | |
| `01_DevWorkflow/SOFTWARE_DEV_WORKFLOW.md` | 开发规范 | Active | 从需求到上线的完整软件开发流程 |
| `01_DevWorkflow/FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md` | 开发规范 | Active | 跨前后端 contract、测试补齐与验收流程规范 |
| `01_DevWorkflow/PR_CHECKLIST.md` | 开发规范 | Active | 每次功能变更必须逐项勾选的 PR 清单 |
| `01_DevWorkflow/COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md` | 开发规范 | Active | Country Copilot 智能增强实施计划、已实现 tranche 与剩余缺口 |
| `01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md` | 开发规范/AI | Active | Copilot Snapshot / Dynamic / Live 架构、grounded answer 与 direct-answer 路由 |
| `01_DevWorkflow/COUNTRY_COPILOT_GOVERNED_ANALYTICAL_COPILOT_PRD_2026-05-11.md` | 产品/AI | Active | Country Copilot 治理化分析助手 PRD：tool-first 取证 → 强制最终答复、visible answer path、6-section 报告 |
| `01_DevWorkflow/version_comparison_cross_segment_model_picker_prd.md` | 产品/前端 | Active | Version Comparison 跨 Segment Model Picker + Smart Label System PRD |
| **02_DataETL/** | | | |
| `02_DataETL/ETL.md` | 实现 | Active | 数据处理主链路（Raw → ETL → 分区 → 刷新） |
| `02_DataETL/ETL_Baseline_Patch_Migration_Checklist_2026-04-10.md` | 实现 | Active | Baseline / Patch 迁移执行清单 |
| `02_DataETL/ETL_Raw_Compare_Review_Spec_2026-04-10.md` | 实现 | Active | Raw 对 Raw 比对 / Review 规格 |
| `02_DataETL/PRECOMPUTE_STRATEGY.md` | 实现 | Active | 后端预聚合 + 前端轻加载方案 |
| `02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md` | 架构/抓取 | Active | 统一抓取流水线（当前实现 + 后续扩展位） |
| `02_DataETL/VOC_FORUM_SCRAPING_FEASIBILITY_2026-04-17.md` | 架构/抓取 | Draft | 北欧汽车论坛 VOC 抓取可行性、边界与落地路径 |
| `02_DataETL/VOC_FORUM_IMPLEMENTATION_STATUS_2026-04-19.md` | 实现/抓取 | Active | VOC research 结论、当前实现状态、已完成项与下一步执行顺序 |
| `02_DataETL/AI_NEWS_VOC_INTELLIGENCE_PIPELINE_2026-05-09.md` | 架构/抓取/AI | Draft | AI News & VOC 智能管线：crawler 主采集 + LLM 增强理解/结构化/翻译/摘要 |
| **MSRP** | | | |
| `MSRP/README.md` | 功能/MSRP | Active | MSRP 文档总索引与阅读顺序 |
| `MSRP/01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md` | 产品/MSRP | Draft | MSRP 官方价格补全 PRD |
| `MSRP/02_Research_Design/MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md` | 设计/MSRP | Draft | MSRP 技术设计 |
| `MSRP/02_Research_Design/TECH_PATH_SCRAPE_AND_UPLOAD.md` | 预研/MSRP | Active | 官网 MSRP 爬取与上传技术路径 |
| `MSRP/03_Implementation/MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md` | 实现/MSRP | Active | MSRP 端到端技术流（核心） |
| `MSRP/03_Implementation/MSRP_OVERRIDE_AND_PRICE_HISTORY_2026-04-11.md` | 实现/MSRP | Active | Override 回流与价格时间序列 |
| `MSRP/03_Implementation/MSRP_BRAND_FAMILY_MODEL_RULES_2026-04-11.md` | 实现/MSRP | Active | 多车型映射规则 (model_rules) |
| `MSRP/03_Implementation/CSS_SELECTOR_TOOLCHAIN_2026-04-11.md` | 实现/MSRP | Active | CSS Selector 填充工具链 |
| `MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md` | 实现/MSRP | Active | Version/Feature Matrix + 多源 Reconciliation（含 link/review lifecycle 现状） |
| `MSRP/05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md` | Backlog/MSRP | Active | **当前执行计划（主入口）** |
| `MSRP/04_Execution/MSRP_KEYWORD_FILLING_AND_POC_RESULTS_2026-04-12.md` | 执行/MSRP | Active | Keyword 填充记录 + POC 结果 |
| `MSRP/04_Execution/MSRP_XC60_EXECUTION_RESULT_2026-04-11.md` | 验证/MSRP | Active | XC60 执行结果 |
| `MSRP/04_Execution/MSRP_XC60_SCRAPE_PREVIEW_2026-04-11.md` | 验证/MSRP | History | XC60 拓取预览（已执行） |
| `MSRP/04_Execution/MSRP_BATCH1_SOURCE_RESEARCH_2026-04-11.md` | 验证/MSRP | Active | Batch-1 品牌调研结果 |
| `MSRP/05_Backlog/MSRP_COUNTRY_BRAND_SOURCE_PRIORITY_PLAN_2026-04-11.md` | Backlog/MSRP | **Archived** | 旧 country×brand 计划（已被 SUV Top30 替代） |
| **03_Database/** | | | |
| `03_Database/PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md` | 架构 | Active | 平台技术栈与数据库边界设计 |
| `03_Database/POSTGRESQL_CORE_SCHEMA_2026-04-10.md` | 架构 | Active | PostgreSQL 核心表结构设计 |
| `03_Database/ALEMBIC_MIGRATION_PLAN_2026-04-10.md` | 架构 | Active | Alembic 数据库迁移方案 |
| `03_Database/BACKEND_POSTGRES_LOCAL_DEV_2026-04-10.md` | 运维/调试 | Active | 后端 PostgreSQL 本地开发环境搭建 |
| `03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md` | 架构 | Active | JATO Parquet × PostgreSQL MSRP 跨源 join（含当前 link/override contract） |
| **04_DevOps/** | | | |
| `04_DevOps/TENCENT_CLOUD_DEPLOY.md` | 运维/部署 | Active | 腾讯云 Ubuntu Fullstack 部署 |
| `04_DevOps/MANUAL_CICD.md` | 运维/发布 | Active | Fullstack 手动 CI/CD 与线上修复 |
| `04_DevOps/FULLSTACK_LOCAL_DEBUG.md` | 运维/调试 | Active | Fullstack 本地联调与问题排查 |
| `04_DevOps/GIT_PROXY_SWITCH.md` | 运维/工具 | Active | Git 代理快速切换 |
| **_archived/** | | | |
| `_archived/JATO_GLOBAL_VISUALIZATION.md` | 产品/规划 | Shelved | 全球可视化总控方案（不再继续） |
| `_archived/STREAMLIT_TO_FULLSTACK_MIGRATION.md` | 迁移 | History | Streamlit → Fullstack 迁移蓝图 |
| `_archived/STREAMLIT_VS_REACT_COMPARISON.md` | 迁移 | History | React 与 Streamlit 事实对比（迁移已完成） |
| `_archived/REACT_STREAMLIT_GAP_ANALYSIS.md` | 迁移 | History | 2025-07 历史差距快照 |
| **UI 规范** | | | |
| `../UI/UI_SPECIFICATION_V1.md` | UI/规范 | Active | UI 视觉、布局与响应式规范初版 |
| `../UI/BMW.md` | UI/设计 | Active | BMW CI2020 风格设计约束与落地规则 |
| `../UI/Dashboard_Layout_Baseline_2026-04-10.md` | UI/布局 | Active | Dashboard 布局基线 |
| `../UI/Dashboard_Shared_Filter_Reuse.md` | UI/组件 | Active | 共享筛选器复用抽象 |

### 4.2 历史归档（Streamlit/）

> 完整清单与归档原因见 [`../Streamlit/README.md`](../Streamlit/README.md)

| 文档 | 领域 | 说明 |
| --- | --- | --- |
| `../Streamlit/DASHBOARD_PERFORMANCE_PREPLAN.md` | 性能 | Streamlit 性能优化预案 |
| `../Streamlit/DASHBOARD_PERFORMANCE_TODOS_50.md` | 性能 | 性能优化 50 项执行看板 |
| `../Streamlit/DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md` | 性能 | 优化阶段报告 |
| `../Streamlit/DEPLOYMENT.md` | 部署 | Streamlit 部署模板 |
| `../Streamlit/OPERATIONS_TEMPLATES.md` | 运维 | Streamlit 回归测试 / 发布模板 |
| `../Streamlit/EXPORT_CHART_SETTINGS.md` | 功能 | Streamlit 图表导出设置 |
| `../Streamlit/Oregon_deployment.md` | 部署 | AWS 俄勒冈 Spot 部署 |
| `../Streamlit/PERFORMANCE_OPTIMIZATION_CACHE.md` | 性能 | `@st.cache_data` 磁盘缓存 |
| `../Streamlit/TENCENT_UBUNTU_LOW_BANDWIDTH_PLAYBOOK.md` | 部署 | 腾讯云低带宽部署 |
| `../Streamlit/TROUBLESHOOTING_TENCENT_CLOUD.md` | 排查 | 腾讯云 Streamlit 问题排查 |

## 5. 推荐阅读路径

### 新人入门
`ROADMAP.md` → `01_DevWorkflow/SOFTWARE_DEV_WORKFLOW.md` → `01_DevWorkflow/FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md` → `01_DevWorkflow/PR_CHECKLIST.md`

### 数据 / ETL
`02_DataETL/ETL.md` → `02_DataETL/ETL_Raw_Compare_Review_Spec_2026-04-10.md` → `02_DataETL/ETL_Baseline_Patch_Migration_Checklist_2026-04-10.md` → `02_DataETL/PRECOMPUTE_STRATEGY.md`

### MSRP 功能
`MSRP/README.md` → `MSRP/01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md` → `MSRP/02_Research_Design/TECH_PATH_SCRAPE_AND_UPLOAD.md` → `MSRP/02_Research_Design/MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md`

### 数据库设计
`03_Database/PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md` → `03_Database/POSTGRESQL_CORE_SCHEMA_2026-04-10.md` → `03_Database/ALEMBIC_MIGRATION_PLAN_2026-04-10.md` → `03_Database/BACKEND_POSTGRES_LOCAL_DEV_2026-04-10.md`

### UI 规范
`../UI/UI_SPECIFICATION_V1.md` → `../UI/Dashboard_Layout_Baseline_2026-04-10.md` → `../UI/Dashboard_Shared_Filter_Reuse.md` → `../UI/BMW.md`

### 运维 / 部署
`04_DevOps/TENCENT_CLOUD_DEPLOY.md` → `04_DevOps/MANUAL_CICD.md` → `04_DevOps/FULLSTACK_LOCAL_DEBUG.md` → `04_DevOps/GIT_PROXY_SWITCH.md`

### 迁移历史（仅追溯）
`_archived/STREAMLIT_TO_FULLSTACK_MIGRATION.md` → `_archived/STREAMLIT_VS_REACT_COMPARISON.md` → `_archived/REACT_STREAMLIT_GAP_ANALYSIS.md` → [`../Streamlit/README.md`](../Streamlit/README.md)

## 5.1 当前关注点（2026-05-14）

- Fullstack 迁移已完成，Streamlit 文档全部归档至 `Streamlit/`。
- 全球可视化（地球项目）已搁置，`JATO_GLOBAL_VISUALIZATION.md` 保留但不再推进。
- **Version Comparison 版型气泡图 Smart Label System 已完成**：Label Mode 四模式切换（Clean / Smart Top / Selected / All）、P0–P3 优先级分层、确定性 jitter + greedy 防重叠、click/框选交互、按优先级分透明度。详情见 `01_DevWorkflow/version_comparison_cross_segment_model_picker_prd.md` §20。
- **MSRP 官方价格补全系统**：Batch 1+2（7 国）dry-run 已完成，92/209 通过 (44.0%)。下一步是将通过的 source promote 到 `sources/` 目录。
- Country Copilot 已从“只给大盘”升级到 **grounded answer + direct answer**：窄问题优先走 `positioning-focus` / `segment-fuel-focus` / `precise-lookup`，并返回 `answerMode + grounding`。
- `positioningMap` 已补第一层 **peer corridor / residual**：不仅给 Length × MSRP × Sales 散点，还统一返回 sales-weighted peer P25 / median / P75、target residual 与 `below / within / above corridor` 判断，供 Dashboard 与 Country Copilot 共用。
- `peerCorridor` 已继续补成 **price stance / verdict**：在 corridor 之上再给 `进攻切入价 / 主流防守价 / 市场对齐价 / 上沿试探价 / 高溢价试探价` 这类确定性结论，并把 stance label/detail 同步给 Copilot 回答、grounding、inline chart、analysis deck 与 Dashboard hero chips。
- `/copilot` 现已作为 **mobile-web 一等入口**：主导航加入 Copilot，手机端默认隐藏悬浮 widget，页面本体改用 compact grounded answer / compact deck、折叠式 context/news/insight 面板与桌面接力深链接；后续又继续把手机首屏压缩到更偏 chat-first 的 reveal order（短 header、空状态才显示 prompts、更紧凑的 pending / inline charts），同时保持桌面端不受影响；最新又补了一层 **visible answer path**，把 route / focus / truth layers / output mode 组织成用户可读的回答路径，并把每次回答统一成“直接回答 → 关键结论 → 思考链 → 证据/来源层”的固定结构；同时把 NVIDIA tool-depth overflow 收敛成正常 fallback，并把 tool-first 执行改成 **单轮取证 + 强制最终答复**，不再把内部中断字符串直接展示给用户。
- MSRP 官方价格文档已收拢到 `Fullstack/MSRP/`，旧 `country×brand` 计划已归档，由 SUV Top30 计划替代；`JatoMsrpLink + MatchOverride` 生命周期与 `/v1/msrp/links` API 已接入主流程。
- 数据库边界设计（PostgreSQL）和 Alembic 迁移方案已起草并被实现引用；当前运行时已包含 `CurrentPrice`、`PriceHistory`、engineering normalized variants、`JatoMsrpLink`、`MatchOverride` 等核心表。
- 跨层 contract 测试覆盖 MSRP、Review Cases、Engineering 三大页面（15 前端 + 10 后端测试）。
- ETL baseline / patch / staging / release 工作流规格已定义。
- Scraping Toolkit 文档已添加 README，详见 `07_ScrapingToolkit/README.md`。
- `/data-management` 页面已提供 **local-only Airflow start/stop/open-UI controls**，仅作为本地 orchestration 辅助层，不改变核心抓取 / 刷新主链路。
- VOC forum pipeline 已落地 **raw → heuristic enriched → country deck artifact**：`jato-voc-fetch` 现已升级到 **Trafilatura-first main-text extraction + lxml fallback**，并在 raw artifact 记录 `textExtraction.method`，同时开始保留 fetch-time `contentUnits`；`jato-voc-enrich` 继续产出 `enriched/customer_insight_signals.json` 与 `deck/customer_insight_deck.json`，且 enrichment 已补 **taxonomy-driven automatic analysis**（theme tags、persona cohorts、product/competitor matching、heuristic auto scores、cross analysis、content-unit transaction-based association graph with Fisher/FDR validation and replication axes）；当前 batch_a 八国本地实跑已可产出 43 docs / 68 analysis units，而隔离 refetch smoke run 已在 SE / NO / HU / HR / AT / CZ 看到 `fetch_lxml_block` units，但整体仍需继续前推 comment/reply structure preservation；`CustomerInsightsPage` 现已补 **Benchmark Excel / Forum VOC Live** 双模式，live 模式展示 observed-only forum deck，并支持按已生成 country deck 做 country-focus 查看，且 evidence card 可直接展开查看抓取正文预览与 observation 句子；`/data-management` 现已补 **VOC 观察台**，可按国家查看 raw / enriched / deck artifact、source runs、staging 同步与 VOC 文档路径；benchmark 继续承载样本画像。
- VOC 已补一份正式状态文档 `02_DataETL/VOC_FORUM_IMPLEMENTATION_STATUS_2026-04-19.md`，用于汇总 research 结论、已做项与后续执行顺序。

## 5.2 2026-04-17 新增规划（六问答卷）

基于用户 2026-04-17 提出的六个核心难点，新增以下 5 份 Draft 专题文档：

- [`PRODUCT_DEEPDIVE_2026-04-17.md`](./PRODUCT_DEEPDIVE_2026-04-17.md) — 主答卷（六问概览 + 动作清单）
- [`03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md`](./03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md) — Q1 跨源 join
- [`MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md`](./MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md) — Q2+Q5 配置差异 + 多源对账
- [`01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md`](./01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md) — Q3+Q4 Copilot 精准化与本地 × 联网
- [`02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md`](./02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md) — Q6 统一抓取流水线

同时把根目录 `Markdown_Readme/country_assistant_upgrade_plan.md` 标记为 History（被 Copilot 新文档 supersede），但暂不移动文件（等下一轮归档批次一起处理）。

## 6. 变更流程（简化）

1. 修改专题文档。
2. 回填 `ROADMAP.md` 状态与导航。
3. 若涉及新文档，必须加入 §4 清单。

### 6.1 文档状态定义

- `Draft`：主要是方案草稿，尚未被当前代码或运行流程真正采用。
- `Active`：已被现有代码、API、运行流程或团队协作直接引用；允许同时包含“已实现 + 下一步”。
- `History`：被新的 Active 文档取代，但保留原始决策或阶段记录供追溯。
- `Archived` / `Shelved`：明确不继续推进，只保留历史材料。

## 7. 维护检查清单

- [ ] 所有活跃文档是否在 §4.1 中列出。
- [ ] 历史文档是否在 `Streamlit/README.md` 中登记。
- [ ] 推荐阅读路径是否覆盖全部领域。
- [ ] 本索引的文件路径是否与磁盘一致。

## 8. 快速使用（按场景）

1. **看项目全貌**：先读 `ROADMAP.md`，再按"推荐阅读路径"进入对应专题。
2. **做数据刷新**：按 `ETL.md` 的"日常执行最短路径"运行 ETL / 分区 / 刷新命令。
3. **MSRP 开发**：先读 `MSRP/README.md`，再按 PRD → 技术路径 → 实现文档 → backlog 顺序推进。
4. **数据库迁移**：按 `PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md` 了解边界，`ALEMBIC_MIGRATION_PLAN_2026-04-10.md` 执行迁移。
5. **部署上线**：按 `TENCENT_CLOUD_DEPLOY.md` + `MANUAL_CICD.md` 执行。
6. **查阅 Streamlit 历史**：[`../Streamlit/README.md`](../Streamlit/README.md) 提供归档清单与追溯入口。
