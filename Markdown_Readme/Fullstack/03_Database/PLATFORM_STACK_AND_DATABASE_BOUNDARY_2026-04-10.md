# Platform Stack And Database Boundary Design

状态：Draft

日期：2026-04-10

关联文档：

1. [MSRP_Official_Price_Enrichment_PRD_2026-04-10.md](./MSRP/01_Requirements/MSRP_Official_Price_Enrichment_PRD_2026-04-10.md)
2. [MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md](./MSRP/02_Research_Design/MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md)
3. [POSTGRESQL_CORE_SCHEMA_2026-04-10.md](POSTGRESQL_CORE_SCHEMA_2026-04-10.md)
4. [ALEMBIC_MIGRATION_PLAN_2026-04-10.md](ALEMBIC_MIGRATION_PLAN_2026-04-10.md)

## 1. 目标

本设计文档用于正式确定全栈平台后续开发时的技术栈边界，重点回答：

1. 哪些数据继续保留在 Parquet。
2. 哪些数据必须迁入 PostgreSQL。
3. 哪些文件只保留在磁盘，不进入数据库。
4. 在当前单机服务器条件下，哪些方案现实可行，哪些方案暂时不应该上。

## 2. 当前约束

### 2.1 业务约束

当前平台要同时支持以下几类能力：

1. 每月获取最新 JATO 数据并做 ETL。
2. 网站读取工程配置表，例如 Omoda 9 的数百条配置记录。
3. 抓取官网 MSRP，并保留历史快照。
4. 人工校对 MSRP 匹配结果并做增删查改。
5. 输出价格预警与价格变化效果验证。

### 2.2 运维约束

当前现实条件如下：

1. 单台服务器。
2. 4 核 CPU。
3. 16 GB 内存。
4. 180 GB 磁盘。
5. 12 Mbps 带宽。
6. 当前站点打开速度用户已经满意，首开约 10 秒，后续约 1 秒。

### 2.3 操作模式约束

当前短期操作模式已经明确：

1. JATO 原始 xlsx 继续以人工方式处理。
2. 通过 VS Code 上传、脚本 ETL、再同步代码到服务器即可。
3. JATO 更新频率约每月一次。
4. 暂时不追求高度自动化的数据接入。
5. MSRP 抓取任务尽量放夜间执行。

## 3. 核心结论

平台后续应采用混合架构，而不是纯文件架构，也不是全量数据库架构。

正式结论如下：

1. 保留 JATO Parquet 主读模型。
2. 新增 PostgreSQL 作为业务库。
3. 原始 xlsx 和抓取快照继续保留在磁盘。
4. 定时任务先用 systemd timer 或 cron 跑独立 Python 脚本。
5. 暂时不要引入 Redis、Celery、Airflow、Kubernetes。

## 4. 推荐技术栈

### 4.1 前端

继续使用：

1. React。
2. Vite。
3. 当前现有的前端页面与共享组件体系。

原因：

1. 已经完成从 Streamlit 到全栈的迁移。
2. 当前首屏与二次打开速度已达可接受范围。
3. 没有必要再做前端栈重构。

### 4.2 后端 API

继续使用：

1. FastAPI。
2. Uvicorn。
3. Pydantic。

原因：

1. 已有成熟的分析和 CRUD 路由结构。
2. 与 Python 的 ETL、抓取脚本天然兼容。
3. 后续接 PostgreSQL 和 SQLAlchemy 成本较低。

### 4.3 分析读模型

继续使用：

1. Parquet。
2. PyArrow。
3. 现有分区数据集与预计算汇总策略。

原因：

1. JATO 主数据是典型读多写少场景。
2. 当前性能已经证明这条路径有效。
3. 不需要把 JATO 主分析数据整包搬进 PostgreSQL。

### 4.4 业务数据库

新增：

1. PostgreSQL 16。
2. SQLAlchemy 2。
3. Alembic。

原因：

1. 工程配置表、MSRP、review、override、alerts 都属于持续写入的数据。
2. 这些场景需要事务、并发编辑、唯一约束、索引和审计能力。
3. 继续使用 JSON 文件或纯 Parquet 做这类业务写入，后续一定会到边界。

### 4.5 抓取与任务执行

继续使用 Python 脚本，配合：

1. httpx 或 requests。
2. BeautifulSoup 或 lxml。
3. Playwright。
4. systemd timer 或 cron。

暂时不引入：

1. Redis。
2. Celery。
3. Airflow。
4. Kubernetes。

原因：

1. 任务量还不大。
2. 当前服务器资源有限。
3. 增加这些组件只会显著提高运维复杂度。

## 5. 数据边界

### 5.1 继续放在 Parquet 的数据

以下数据继续保留在 Parquet：

1. JATO 月度主事实数据。
2. JATO 分区数据集。
3. Dashboard 预计算汇总数据。
4. 面向图表分析的宽表和聚合结果。
5. 其他高读频、低写频的分析型读模型。

判断标准：

1. 数据体量大。
2. 写入频率低。
3. 主要用途是筛选、聚合、分析、导出。

### 5.2 必须放在 PostgreSQL 的数据

以下数据迁入 PostgreSQL：

1. 工程配置表。
2. 工程配置表导入批次与版本。
3. MSRP source registry。
4. MSRP 结构化 observation。
5. 当前生效 MSRP。
6. review 队列。
7. review decision log。
8. manual override。
9. price alerts。
10. price effectiveness 结果。
11. 后台操作审计数据。

判断标准：

1. 需要持续写入。
2. 需要事务。
3. 需要并发安全。
4. 需要后台增删查改。
5. 需要唯一约束、状态流转和审计。

### 5.3 只放在磁盘的文件

以下文件只保留在磁盘，不进入数据库：

1. JATO 原始 xlsx。
2. 工程配置表原始 xlsx。
3. 官网 HTML 快照。
4. 官网 JSON 响应快照。
5. PDF 价格单。
6. review 需要的截图。

原因：

1. 这些是原始证据，不适合直接当在线数据库字段。
2. 放数据库会推高存储成本和备份成本。
3. 后续只需在数据库中保存文件路径和元数据。

## 6. 为什么不是全量 PostgreSQL

不建议把 JATO 主数据也整体迁入 PostgreSQL。

原因：

1. JATO 主数据本质是分析型数据集。
2. 现有 Parquet 读模型已经满足性能要求。
3. 若整包迁入 PostgreSQL，会增加索引、维护和存储成本。
4. 在当前单机资源条件下，收益很低。

因此最终边界应是：

1. JATO 主分析数据在 Parquet。
2. 业务型可变数据在 PostgreSQL。
3. 原始证据文件在磁盘。

## 7. 为什么不能继续纯文件存储

纯文件存储已经接近边界。

具体原因：

1. 工程配置表后续会有多版本导入。
2. MSRP 抓取结果会持续追加。
3. 人工 review 会产生状态流转。
4. override 会需要长期维护。
5. alerts 与审计记录会越来越多。

这些都不适合长期继续用 JSON 文件或直接改 xlsx 来支撑。

## 8. 推荐部署形态

建议在当前单机环境下采用：

1. Nginx。
2. React 静态资源。
3. FastAPI。
4. PostgreSQL。
5. Parquet 数据目录。
6. 原始文件目录。
7. systemd service。
8. systemd timer。

建议物理形态如下：

    [Nginx]
      -> frontend 静态资源
      -> FastAPI API

    [FastAPI]
      -> 读取 Parquet 分析数据
      -> 读写 PostgreSQL 业务数据

    [systemd timer / cron]
      -> 夜间 MSRP 抓取脚本
      -> review 物化脚本
      -> alerts 生成脚本

    [磁盘文件系统]
      -> JATO xlsx
      -> 工程配置 xlsx
      -> 官网快照
      -> ETL 产物

## 9. 推荐操作流

### 9.1 JATO 月度更新

短期继续采用人工操作模式：

1. 通过 VS Code 或 SCP 上传原始 xlsx 到服务器指定目录。
2. 执行 ETL 脚本。
3. 更新 Parquet 主读模型。
4. 必要时同步代码变更到服务器。

说明：

1. 原始 xlsx 不应进入 Git 仓库版本控制。
2. Git 只管理代码、脚本、配置和文档。

### 9.2 工程配置表更新

推荐流程：

1. 上传工程配置 xlsx。
2. 运行导入脚本。
3. 导入 PostgreSQL。
4. 记录 import batch。
5. 网站从 PostgreSQL 读取结构化配置数据。

### 9.3 MSRP 夜间任务

推荐流程：

1. 夜间生成 candidate list。
2. 按国家和品牌执行抓取。
3. 原始快照落磁盘。
4. 结构化结果写 PostgreSQL。
5. 生成 review queue、alerts 和 current prices。

## 10. 当前服务器上的现实边界

### 10.1 当前配置可承载的范围

4 核 16G 单机在第一阶段可以稳定承载：

1. 现有全栈网站。
2. 本机 PostgreSQL。
3. 每月一次 JATO ETL。
4. 每周一次夜间 MSRP 抓取。
5. 少量品牌、少量国家的 Playwright 或 API 抓取。
6. 少量内部用户的 review 与后台操作。

### 10.2 开始吃紧的场景

当前配置会开始吃紧的情况：

1. 多国家多品牌高并发 Playwright 抓取。
2. 白天高峰时同时跑重分析和抓取任务。
3. 把所有截图、PDF 和快照长期无限制保留。
4. 多个后台用户高频批量修改。

### 10.3 当前配置不适合的场景

当前配置不适合：

1. 全欧洲全品牌高频自动抓取。
2. 高并发浏览器自动化。
3. 多节点任务队列系统。
4. 复杂容器编排平台。

## 11. 资源控制建议

在当前服务器上建议控制：

1. FastAPI worker 数保持保守。
2. PostgreSQL 使用本机部署，但限制合理内存。
3. Playwright 并发控制在 1 到 2 个 browser worker。
4. 所有抓取任务尽量在夜间低峰执行。
5. 原始快照默认压缩保存。
6. 截图仅保留失败案例和 review_required 案例。

## 12. 暂时不建议上的技术

当前阶段不建议上：

1. Redis。
2. Celery。
3. Airflow。
4. Kubernetes。
5. 对象存储体系。

原因：

1. 当前任务规模和团队规模都还没到必须引入的阶段。
2. 会增加部署复杂度和排障成本。
3. 对当前核心收益有限。

## 13. 第一阶段正式架构结论

后续开发应以以下正式组合为准：

1. 前端：React + Vite。
2. API：FastAPI。
3. 分析主读模型：Parquet + PyArrow。
4. 业务库：PostgreSQL + SQLAlchemy + Alembic。
5. 原始文件：磁盘目录。
6. 定时任务：systemd timer 或 cron + Python 脚本。

## 14. 开发边界结论

后续所有开发应遵守以下边界：

1. 不把 JATO 主事实表整体迁入 PostgreSQL。
2. 不再用 JSON 文件长期承载复杂业务 CRUD。
3. 不在数据库里保存原始 xlsx、HTML、PDF 或截图大文件。
4. 不把官网抓取做成前端实时操作。
5. 不在当前阶段引入复杂分布式组件。

## 15. 下一步建议

基于本设计文档，后续应优先推进：

1. 建立 PostgreSQL 核心表结构。
2. 为工程配置表建立导入链路。
3. 为 MSRP 建立 observation、review、override、alerts 表。
4. 保持 JATO ETL 继续输出 Parquet 主读模型。
