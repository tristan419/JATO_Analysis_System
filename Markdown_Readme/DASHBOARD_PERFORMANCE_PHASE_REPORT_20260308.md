# Dashboard 性能优化阶段报告（2026-03-08）

> 关联看板：[DASHBOARD_PERFORMANCE_TODOS_50.md](./DASHBOARD_PERFORMANCE_TODOS_50.md)
> 关联方案：[DASHBOARD_PERFORMANCE_PREPLAN.md](./DASHBOARD_PERFORMANCE_PREPLAN.md)

## 1. 阶段结论

- 阶段状态：`已完成`
- 完成度：`50 / 50`
- 核心结论：国家筛选与大数据场景的图表前计算瓶颈已从行级 `melt` 转为聚合后构图，默认渲染路径与筛选交互均已建立回归保障。

## 2. 本阶段交付

- 计算链路重构：年/月图时间变换从行级 `melt` 改为聚合后构图。
- 渲染策略落地：主图优先渲染、非活跃图按需、增强图按需。
- 计算复用基座：`sum_sales_for_columns`、`prepare_numeric_axis`、`get_series_contribution` 单次渲染缓存。
- 时间轴优化：`parse_time_keys` 增加缓存映射，年度排序改为数值键排序，月/季/年粒度转换路径优化。
- 筛选基座优化：侧栏国家/细分/动总/品牌/Model/Version 选项改为 Arrow 侧 `distinct + filter pushdown`。
- 筛选缓存键统一：新增 `normalize_filter_payload` 与 `build_filter_signature`，读取层与选项层共享签名逻辑。
- CI 回归接入：新增回归脚本已并入 `03_Scripts/ci_smoke_check.py`，支持无数据环境下按规则跳过数据依赖回归。
- CI 分层落地：新增 `.github/workflows/nightly-performance.yml`，支持定时与手动触发 nightly 性能门禁。

## 3. 回归与基准证据

## 3.1 自动回归脚本

- `03_Scripts/regression_csv_download_guardrails.py`：PASS
- `03_Scripts/regression_render_strategy_defaults.py`：PASS
- `03_Scripts/regression_time_selector_consistency.py`：PASS
- `03_Scripts/regression_filter_option_pushdown.py`：PASS

## 3.2 时间变换基准（2026-03-08）

命令：

```bash
python 03_Scripts/benchmark_time_transform_pipeline.py --repeats 2
python 03_Scripts/benchmark_time_transform_pipeline.py --country 德国 --repeats 3
```

结果摘要：

- 全量（724,501 行）
- `sum`：旧 `P50/P95 ~4.8103s/4.8127s`，新 `P50/P95 ~0.0472s/0.0474s`，约 `101.90x`
- `group:powertrain`：旧 `P50/P95 ~4.7695s/4.8296s`，新 `P50/P95 ~0.2205s/0.2234s`，约 `21.63x`
- 德国（117,338 行）
- `sum`：旧 `P50/P95 ~0.7124s/0.7196s`，新 `P50/P95 ~0.0084s/0.0089s`，约 `82.88x`
- `group:powertrain`：旧 `P50/P95 ~0.7285s/0.7302s`，新 `P50/P95 ~0.0405s/0.0424s`，约 `17.64x`
- 口径一致性：`Parity check: PASS`

## 4. 风险与余项

- Streamlit 外运行环境会出现 `cache_data_api` 的 runtime warning，属于脚本态预期行为，不影响回归结论。
- `views.py` 存在历史样式告警（非本阶段引入），建议后续单独整理一次样式修复提交。

## 5. 下一阶段优化计划（Phase-Next）

- P-N1 读取层持续优化：探索侧栏选项并行预取与高基数字段限流策略。
- P-N2 交互层治理：增加筛选切换与图表切换的端到端耗时采样（按会话分桶）。
- P-N3 明细层策略：增加大结果集分页导出与后台导出任务。
- P-N4 运维闭环：CI 分层已启动（`ci_smoke_check.py` + `ci_nightly_performance_check.py`），后续继续收敛阈值与告警联动。
- P-N5 数据侧规划：预研对象存储直读场景下的分区裁剪与统计信息复用。

## 6. 用户反馈追加待办（Round 2）

- [x] 优化国家切换顺序性能一致性。
- [x] 合并概览渲染策略到加载模式模块。
- [x] 重构 NEV 续航分布 23-25 变化逻辑。

## 7. 用户反馈追加待办（Round 3，进行中）

- [x] 新增 NEV 净变化洞察指标卡（净变化、绝对变化、对冲率、加权续航变化）。
- [x] 新增 NEV 净变化结构拆解面板（续航分桶明细、Top 车型明细）。
- [x] 新增 BEV/PHEV 净变化贡献提示与高对冲率提醒。
- [x] 已将 Round 3 待办重排为“动态时间窗净变化”方向（首末年动态，不再固定 23-25）。
- [x] NEV 净变化口径已改为时间窗首末年动态净变化，年度与 caption 文案同步动态化。
- [x] 已新增 Top 正/负向车型分栏、TopN 集中度阈值告警、续航分桶 Top 正/负区间排序。
- [x] 已明确轮子复用策略：CSV/PNG 导出、时间轴选择、回归调度、渲染计时均复用现有基座。
- [ ] 按用户指令暂停 Round 3 后续实现，剩余项已回写到待办清单。
