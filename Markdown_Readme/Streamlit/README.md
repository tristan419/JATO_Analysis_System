# Streamlit 历史文档归档

> **状态：Archive（归档）**
>
> 本目录保留 Streamlit 看板时期的历史文档，仅供追溯参考。
> 当前系统已全面迁移至 Fullstack（FastAPI + React），所有活跃文档请查看 [Fullstack/ROADMAP.md](../Fullstack/ROADMAP.md)。

## 归档说明

Streamlit 是 JATO Analysis System 的第一代可视化看板，基于 `05_DashBoard/app.py` 运行。
2026 年初启动 Fullstack 迁移，将前端重构为 React + TypeScript，后端迁移为 FastAPI。
迁移完成后，Streamlit 代码仍保留在 `05_DashBoard/` 目录，但不再作为主线维护。

以下文档描述的是 Streamlit 时期的部署方式、性能优化策略和运维排查手段，**不适用于当前 Fullstack 架构**。

## 归档文档清单

| 文档 | 内容 | 归档原因 |
| --- | --- | --- |
| `DASHBOARD_PERFORMANCE_PREPLAN.md` | Streamlit 性能优化预案（"基座思维"） | 策略针对 `@st.cache_data`，不适用于 React |
| `DASHBOARD_PERFORMANCE_TODOS_50.md` | 性能优化 50 项执行看板 | Streamlit Round 任务，已完成或暂停 |
| `DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md` | 性能优化阶段报告 | 历史报告 |
| `DEPLOYMENT.md` | Streamlit 部署模板（`streamlit run`） | 已被 Fullstack 部署文档替代 |
| `OPERATIONS_TEMPLATES.md` | Streamlit 回归测试 / 发布模板 | 回归步骤引用 `streamlit run`，已过时 |
| `EXPORT_CHART_SETTINGS.md` | Streamlit 图表导出设置 & PNG 选型 | UI 功能已迁移至 React |
| `Oregon_deployment.md` | AWS 俄勒冈 Spot 部署（Streamlit 时期） | 已被腾讯云方案替代 |
| `PERFORMANCE_OPTIMIZATION_CACHE.md` | Streamlit `@st.cache_data` 磁盘缓存 | 纯 Streamlit API |
| `TENCENT_UBUNTU_LOW_BANDWIDTH_PLAYBOOK.md` | 腾讯云低带宽部署（Streamlit） | 已被 Fullstack 部署文档替代 |
| `TROUBLESHOOTING_TENCENT_CLOUD.md` | 腾讯云 Streamlit 主题 / 配置排查 | `.streamlit/config.toml` 相关 |

## 迁移后的活跃文档去向

以下文档原位于本目录，已迁移至 `Fullstack/`：

- `ETL.md` → 数据处理 Pipeline 主链路
- `ETL_Baseline_Patch_Migration_Checklist_2026-04-10.md` → Baseline / Patch 迁移清单
- `ETL_Raw_Compare_Review_Spec_2026-04-10.md` → Raw Compare / Review 规格
- `MSRP_Official_Price_Enrichment_PRD_2026-04-10.md` → MSRP 官方价格补全 PRD
- `MSRP_Official_Price_Enrichment_Technical_Design_2026-04-10.md` → MSRP 技术设计
- `JATO_GLOBAL_VISUALIZATION.md` → 全球可视化总方案
- `PRECOMPUTE_STRATEGY.md` → 后端预聚合策略
