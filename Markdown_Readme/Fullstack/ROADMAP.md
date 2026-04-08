# JATO Analysis System Roadmap（主索引）

> 本文件是 `Markdown_Readme/` 的唯一入口。
> 目标：在保留历史改动与待办的前提下，保持文档架构精简、可维护、可追溯。

## 1. 维护原则

- `单索引`：仅保留一个总索引（本文件），不再拆分多级索引文档。
- `保历史`：历史报告、评估记录、执行看板不删除。
- `保待办`：所有待办统一保留在原看板文档中，不分散到新文件。
- `低耦合`：专题文档只存专题信息，主索引只做导航与状态摘要。

## 2. 当前项目快照（2026-03-08）

- 阶段：`Phase 4`
- 已完成：读取层优化、筛选下推、时间变换重构、PNG 导出与导出样式、基础可观测、CI smoke + nightly gate。
- 进行中：增量入库后续能力与运维标准化。
- 暂停：Round 3 剩余项按用户指令保留为待办，不继续实现。

## 3. 文档结构（精简版）

- `主索引`：`ROADMAP.md`
- `专题文档`：按产品/实现/质量/运维直接分组，不再新增子索引文件。

## 4. Canonical 文档清单（全部 md）

| 文档 | 领域 | 状态 | 说明 |
| --- | --- | --- | --- |
| `ROADMAP.md` | 总览 | Active | 唯一主索引 |
| `JATO_GLOBAL_VISUALIZATION.md` | 产品/规划 | Active | 全球可视化总控方案 |
| `DASHBOARD_PERFORMANCE_PREPLAN.md` | 产品/规划 | Active | 性能优化方案基线 |
| `DASHBOARD_PERFORMANCE_TODOS_50.md` | 质量/执行 | Active | 性能与 Round 待办执行看板 |
| `DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md` | 质量/报告 | Active | 阶段报告与证据 |
| `ETL.md` | 实现 | Active | 数据处理主链路与增量入库（合并版） |
| `EXPORT_CHART_SETTINGS.md` | 实现 | Active | 图表导出设置与 PNG 选型（合并版） |
| `DEPLOYMENT.md` | 运维/发布 | Active | 部署模板与容量建议 |
| `OPERATIONS_TEMPLATES.md` | 运维/流程 | Active | 运维与发布模板（含日志与可观测，合并版） |

## 5. 推荐阅读路径

- 产品/业务：`JATO_GLOBAL_VISUALIZATION.md` -> `DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md` -> `DASHBOARD_PERFORMANCE_TODOS_50.md`
- 开发：`ETL.md` -> `DASHBOARD_PERFORMANCE_PREPLAN.md`
- 测试/质量：`DASHBOARD_PERFORMANCE_TODOS_50.md` -> `DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md` -> `OPERATIONS_TEMPLATES.md`
- 运维/发布：`DEPLOYMENT.md` -> `OPERATIONS_TEMPLATES.md`

## 6. 变更流程（简化）

1. 修改专题文档。
2. 回填 `ROADMAP.md` 状态与导航。
3. 若有待办变化，仅更新对应看板（如 `DASHBOARD_PERFORMANCE_TODOS_50.md`）。

## 7. 维护检查清单

- [ ] 历史报告与评估文档是否保留。
- [ ] 待办是否仍集中在执行看板中。
- [ ] 本索引是否覆盖全部 canonical 文档。

## 8. 快速使用（按场景）

1. 看项目全貌：先读 `ROADMAP.md`，再按“推荐阅读路径”进入对应专题。
2. 做数据刷新：按 `ETL.md` 的“日常执行最短路径”运行 ETL/分区/刷新命令。
3. 做发布与回归：按 `OPERATIONS_TEMPLATES.md` 执行回归清单、发布模板与监控阈值检查。
4. 做图表导出：按 `EXPORT_CHART_SETTINGS.md` 调整导出样式并执行 PNG 导出。
5. 查性能推进与待办：使用 `DASHBOARD_PERFORMANCE_TODOS_50.md` + `DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md`。
