# JATO Fullstack 架构评审与改动建议（2026-04-17）

状态：Draft（供讨论 / 逐项落地）
作者：Copilot（基于现有 `Markdown_Readme/` 全量阅读）
关联入口：[ROADMAP.md](./ROADMAP.md)

> 定位：对"Streamlit → Fullstack 完成迁移 + 全球可视化搁置"这个新阶段，给出一份从外部视角的系统性架构评审与推荐改动清单。不做新一轮改文档，只做结论与可执行动作建议。

---

## 1. TL;DR（一句话结论）

当前 JATO 平台已经走出 Streamlit 阶段，进入 **"Parquet 读模型 + PostgreSQL 业务库 + FastAPI + React + Scraping Toolkit + Country Copilot"** 的混合架构。架构方向是正确的，但**六条边界线目前是隐性的**：`分析读模型 vs 业务数据`、`抓取 vs 平台`、`批处理 vs 交互`、`LLM 代理 vs 工具调用`、`前端 chart 标准`、`文档生命周期`。把这六条线显性化，就能把后面 2–3 个月的不确定性降到可管理。

---

## 2. 我所看到的项目现状（从文档还原出的系统画像）

### 2.1 技术栈
- **Frontend**：React + Vite + TypeScript + Plotly（Dashboard / MarketScan / MSRP / Country Copilot / Engineering / Review / Monthly Update / Specification）
- **Backend**：FastAPI + Pydantic + SQLAlchemy 2 + Alembic，`app/{api,services,domain,infra,db,scraper,core}` 分层
- **读模型**：PyArrow + 分区 Parquet + 预聚合 `summaries/`
- **业务库**：PostgreSQL 16（MSRP source / observation / current / review / override / price_history / alerts / engineering config）
- **抓取**：`07_ScrapingToolkit`（brand-family YAML source + scrapling + playwright + currency_converter + news_runner）
- **部署**：腾讯云 Ubuntu 单机，nginx + systemd + 手动 CI/CD，12 Mbps 带宽
- **AI**：Country Copilot（Function Calling + Local RAG/Chroma + fact sheets）

### 2.2 正在同时推进的主线
1. MSRP 官方价格补全（Batch 1+2，92/209 dry-run 通过，44.0%）。
2. Country Copilot 智能化升级（Phase 3 news intake 未完成持久化）。
3. ETL baseline / patch / staging / release 规格落地。
4. 前端 BMW 风格 UI 规范持续收拢。
5. PostgreSQL Alembic 迁移已执行到 0006。

### 2.3 做得好的地方（后续不要回退）
- 单索引 ROADMAP 文档策略已经很克制，MSRP 子域的 01–05 分层干净。
- `PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md` 提前把"什么不进库"讲清楚，避免了后期返工。
- `FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md` 把 contract owner 显式定在后端 serializer，是避免前后端字段漂移的关键约束。
- 预聚合 + 分区下推这条读路径，已经是"12 Mbps 带宽 × 70 万行数据"下唯一现实解。
- Streamlit 文档整体归档 + `_archived/` 分区非常干净，历史可追溯而不污染活跃文档。

---

## 3. 从外部视角看到的六条隐性边界

### 3.1 `分析读模型 vs 业务数据` 边界（PG 与 Parquet 交叉读）
**现象**：MSRP 页面要把 `current_msrp`（PG）拼回 JATO 维度（Parquet），Review Cases 要把 review_decision（PG）套在 observation（PG）上但又要展示 JATO 维表字段。目前这层拼接散在 `msrp_admin_service.py` / `review_service.py` / `query_service.py` 三处。

**风险**：后面任何一个字段改名都要三处跟进，前端 type / 单测跟着受牵连。`FULLSTACK_DEVELOPMENT_SPEC` 中的 "contract owner = serializer" 原则目前只在 API 层执行，没有在**跨源拼接层**执行。

**建议**：在 `app/services/` 下增加一个**薄薄的 Composition Layer**（例如 `compositions/`），约定"跨 Parquet × PG 的拼接只发生在这里"。上层 route 和下层 repository 不再互相认识字段。

### 3.2 `抓取工具箱 vs 应用平台` 边界（Toolkit 不承担运行时依赖）
**现象**：`07_ScrapingToolkit` 既是月度批处理产品（MSRP / news），又被 `06_AppPlatform` 读取（currency_converter、local_wiki 候选输入）。但目前是否有"toolkit 只暴露制品"这一约束并没有在文档里写清楚。

**风险**：未来 toolkit 改造（换抓取引擎 / 换汇率源 / 换 news 结构）时，会隐式回染到 FastAPI 进程里。

**建议**：把 `07_ScrapingToolkit` 正式定位为**"离线制品生产者"**：
- 输出：`sources/*.yaml`、`04_Processed_data/fx/`、`04_Processed_data/news/normalized/`（新增）、dry-run 报告。
- 禁止：平台层运行时 import toolkit 的 scraping / playwright 子模块。
- `currency_converter` 这种会被双方使用的模块，迁出到 `shared/` 或发布成内部 package，toolkit / platform 都只引用其纯函数。

### 3.3 `批处理 vs 交互` 边界（refresh job 的反压不应该影响 FastAPI）
**现象**：`run_data_refresh_job.py` 在主数据 refresh 时会重建分区 + 预聚合；MSRP 夜间抓取也会落盘；都和 FastAPI 共享 16 GB 内存 / 180 GB 磁盘。

**风险**：MSRP 批量 dry-run 的 headless browser + FastAPI + PG + nginx + Parquet 读写会抢内存，曾经的 12 Mbps 带宽瓶颈问题（`fullstack-bandwidth-bottleneck-2026-04-08`）会以 CPU/IO 形态重现。

**建议**：
- 把 refresh job、MSRP 抓取、news 抓取统一纳入一个**单机调度清单**（仍然是 systemd timer，不要上 Airflow）。
- 在 `04_DevOps/` 下补一份 `SINGLE_NODE_SCHEDULING_2026-04-17.md`（1 页），明确 3 件事：各任务时间窗、最大并发、允许抢占顺序。
- FastAPI 通过 `st_atime` / manifest mtime 惰性 reload 分区数据，不要与 refresh job 强耦合。

### 3.4 `LLM 代理 vs 工具调用` 边界（Country Copilot 不承载业务逻辑）
**现象**：`country_assistant_upgrade_plan.md` 的 Phase 2 要求把现有 Sales Trend / Pricing 封成 Tools。但目前 `country_chat_service.py` 既做 intent、又做数据拼装、又做 narrative 生成。Phase 3 的 news intake 又停留在 toolkit 里没有落库。

**风险**：未来新页面（例如 Positioning Pricing 已经进前端）一旦要 LLM 化，就会在 chat service 里再加一套拼装逻辑，Copilot 会成为"又一个查询服务"。

**建议**：把 Copilot 收缩为**纯 Orchestrator**：
- 入口：`country_chat_service` 仅做 prompt / tool routing / RAG 调用。
- 工具：每个 tool 对应一个现有 service 的**只读入口**（`market_scan_service.scan()`、`engineering_service.summary()`…）。
- 禁止：tool 内部再写新的 SQL / Parquet 查询；tool 返回结构必须是前端已在消费的同一个 payload schema。

这样 `LazyPlotlyChart` 那套就自然可以被 Copilot 复用，Phase 4 的"图表大一统"不需要再额外做组件级改造。

### 3.5 `前端 chart 标准` 边界（唯一的图表契约）
**现象**：`CountryChatAnalysisDeck.tsx` 还有 Recharts 残留；`MarketScanPage` / `DashboardPage` 已经全面 Plotly。`frontend-plotly-types` 这条内存里也记着类型收敛的痛点。

**风险**：一旦引入第二套 chart，BMW 风格的 UI 规范（零圆角 / 受控配色 / 语义色）就会失效，UI spec 逐步失去强制力。

**建议**：
- 在 `UI/UI_SPECIFICATION_V1.md` 里**写死一行硬约束**：项目内图表只允许通过 `components/LazyPlotlyChart` 渲染，Recharts 禁止新增。
- 给现有 `colors.ts` 加一个"canonical palette registry"（powertrain / segment / country），任何图表读色只走注册表。Country Copilot 最近踩的 powertrain 配色漂移就是这个层缺位的结果。

### 3.6 `文档生命周期` 边界（Draft → Active → Archived 规则）
**现象**：ROADMAP § 4 目前有 `Draft / Active / History / Shelved / Archived` 五种状态，但转换条件没文档化。`PostgreSQL 核心表设计`、`Alembic 方案`、`MSRP PRD / Tech Design` 至今仍是 Draft，而实际上 PG 已经迁移到 0006，MSRP 已经跑到 Batch 2。

**建议**：给 ROADMAP § 6 的"变更流程"补一小节：
- `Draft`：只在草拟，尚未指导实施。
- `Active`：至少有一次实现或部署引用了它。
- `History`：被新的 Active 文档取代但保留以追溯。
- `Archived`：明确搁置（e.g. 全球可视化）。

**动作**：把已经被实现引用的 PG / MSRP 相关文档从 Draft 改 Active，让状态字段真的可信。

---

## 4. 具体推荐改动（按优先级 P0 → P2）

### P0（近 1–2 周可落地，收敛当前主线）

| # | 动作 | 归属文档 / 位置 | 理由 |
|---|------|------|------|
| P0-1 | 新建 `app/services/compositions/` 放跨 PG × Parquet 拼接，MSRP / Review 先迁 | 03_Database + 02_DataETL | 消除跨源字段漂移，配合 FULLSTACK_DEV_SPEC |
| P0-2 | `UI_SPECIFICATION_V1.md` 增加"图表唯一入口 = LazyPlotlyChart"硬约束，完成 CountryChatAnalysisDeck Recharts 清理 | UI/ | 彻底关掉双标的图表栈 |
| P0-3 | 在 `colors.ts` 落一个 `canonicalPalette` 注册表（powertrain / segment / country），所有 chart 读色走它 | UI/Dashboard_Shared_Filter_Reuse.md 记录 | 避免 Country Copilot 类的配色漂移 |
| P0-4 | `ROADMAP.md` § 6 加"状态机规则"，把 PG / MSRP 已落地的 Draft 文档升为 Active | Fullstack/ROADMAP.md | 让状态真实可信 |
| P0-5 | 新建 `04_DevOps/SINGLE_NODE_SCHEDULING_2026-04-17.md`（~1 页），把 refresh / MSRP / news 三类批任务排成单机时间表 | 04_DevOps/ | 避免批处理压垮交互请求 |

### P1（近 1 个月，打磨数据边界）

| # | 动作 | 归属 | 理由 |
|---|------|------|------|
| P1-1 | 把 `07_ScrapingToolkit` 定位为"离线制品生产者"，平台禁止 import 其运行时模块，共享模块（currency / news schema）抽到 `shared/` | 02_DataETL / 07_ScrapingToolkit README | 长期解耦 |
| P1-2 | Country Copilot tool layer 重构：每个 tool = 现有 service 只读入口，返回前端已消费的 payload schema | 01_DevWorkflow/COUNTRY_COPILOT_*.md | 消除"第二套查询逻辑" |
| P1-3 | Phase 3 news intake 持久化：在 PG 加 `news_article` / `news_batch_run` 表（Alembic 0007），app 读取 | 03_Database + country_assistant_upgrade_plan.md | 让 news 真正进入闭环 |
| P1-4 | MSRP `model_rules` / `edition_rules` / `price_band_bonuses` 规则校验工具：一个 `validate_rules.py` 跑全量 YAML | MSRP/03_Implementation/ | 现在规则增长很快，需要守门 |
| P1-5 | 为 `compositions/` 层加契约测试：每个组合返回的 payload 必须通过 snapshot 测试，前端 type 自动同步 | 01_DevWorkflow/FULLSTACK_DEV_SPEC | 把 contract 守门拉到拼接层 |

### P2（近 2–3 个月，面向下一个形态）

| # | 动作 | 说明 |
|---|------|------|
| P2-1 | PG 迁移完成后评估 DuckDB/Polars 替代 PyArrow 作为 Parquet 读层 | 现状 PyArrow OK，但当 filter 组合扩大（MarketScan drilldown ranking）后 DuckDB 的谓词优化更稳 |
| P2-2 | 前端 precomputed summary 的缓存策略（SWR / query key 统一） | 12 Mbps 带宽下，summary 命中率决定体感 |
| P2-3 | Country Copilot 的"可解释性层"：每条回答挂上 data source + timestamp + row count | 进入决策场景前必须先解决可信度问题 |
| P2-4 | MSRP 抓取稳定后，把 review override 回流规则固化为 `rules/` 目录，进入 CI dry-run | 现在 override 是手动回流，再长就不可控 |

---

## 5. 我不建议做的事

1. **不建议**现阶段引入 Redis / Celery / Airflow / K8s。现有 systemd timer + 单机节奏足够支撑年底的 MSRP Batch 3+4 扩张，引入任何一个都会让运维复杂度翻倍而收益很薄。
2. **不建议**把 JATO 主事实数据搬进 PostgreSQL。`PLATFORM_STACK_AND_DATABASE_BOUNDARY` 的结论现在看仍然正确，Parquet 读多写少 + 预聚合是性价比最高的路径。
3. **不建议**恢复全球可视化。地球项目与当前业务优先级正交，重启代价至少 2 个月，应继续维持 Archived。
4. **不建议**在 Copilot 里做数据写入工具（任何 mutating tool）。Copilot 只读，所有写回路仍走现有 review / override 入口。
5. **不建议**给 Streamlit 归档目录继续加内容。如果确实需要跟 Streamlit 时代对比（如性能复盘），直接在 Fullstack 新文档里引用归档即可。

---

## 6. 与 ROADMAP 的衔接

本文档由 ROADMAP § 4.1 "活跃文档（Fullstack/）" 索引收录，定位为"跨域架构评审"，不替代任何专题文档：

- 专题执行仍然落在 `01_DevWorkflow` / `02_DataETL` / `03_Database` / `04_DevOps` / `MSRP` 原有结构下。
- 本文只做**边界判断与动作推荐**，一旦某项动作开始执行，请把它补进对应专题文档的 checklist，并在本文件勾掉 P0/P1/P2 行。

---

## 7. 下一步（建议的落地顺序）

1. 先做 P0-2 / P0-3（UI 图表唯一入口 + 配色注册表）：改动面小、收益立即可见、能立刻让 Country Copilot 的 chart 体感统一。
2. 接着 P0-4 / P0-5（ROADMAP 状态机 + 单机调度清单）：一小时内可完成，文档侧信号显著。
3. 再做 P0-1（compositions 层）：需要一次小重构，配合下一次 MSRP / Review 的改动顺带迁移，不单独开 PR。
4. P1 系列在 MSRP Batch 3 启动前完成 P1-1 / P1-2 / P1-3，为 Copilot news 能力和 toolkit 解耦铺路。

---

## 8. 附：我在阅读过程中注意到但没放进动作项的小问题

- `customer_insight_service.py` `_build_lifestyle_items` 的 for 循环体是空的（line ~276 区域），counter 没有实际累加（参考当前编辑器打开文件）。
- `01_DevWorkflow/COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md` 的 Phase 4 Step 4.1（空卡片按数据依赖审计）和 Step 4.3（PM workbench 当年 / 车型范围）仍未完成，建议在 P1 批次一起收。
- `_archived/JATO_GLOBAL_VISUALIZATION.md` 既然已 Shelved，可考虑在 ROADMAP 标记明确的"解封条件"（例如：当带宽升级到 100 Mbps 或团队扩张到 ≥ 3 人时才重评估），否则每次新人入门都会问一次。

以上。欢迎逐项勾选要进入 ROADMAP 的项，我再把具体动作补到对应专题文档里。
