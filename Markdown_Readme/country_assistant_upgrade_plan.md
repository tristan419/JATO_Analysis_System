# 国家助手升级计划（History）

状态：Historical / superseded

这个文件保留为最早期的 upgrade 草案入口，当前**有效文档**已经迁移到 `Markdown_Readme/Fullstack/` 下面：

1. `Fullstack/01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md`
   - 当前国家助手主架构文档
   - 包含 intent routing、local + live retrieval、Google AI 模式差距、是否需要换模型
2. `Fullstack/01_DevWorkflow/COUNTRY_COPILOT_INTELLIGENCE_IMPLEMENTATION_2026-04-15.md`
   - 已实现能力与阶段性落地
3. `Fullstack/02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md`
   - news / policy / finance / regulation / MSRP 的统一抓取平台
4. `Fullstack/03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md`
   - JATO sales × 本地 MSRP 的联表边界
5. `Fullstack/MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md`
   - 多 version / 多动总 / 配置 diff / 多源对账

最初这个草案里的方向（按需取数、Local RAG、图表按问题渲染）已经被上面的文档和后续实现承接；后续不要再把新设计写回这个 history 文件。
