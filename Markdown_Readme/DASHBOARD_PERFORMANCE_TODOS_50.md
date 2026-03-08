# Dashboard 性能优化 50 TODO 执行看板

> 关联方案：[DASHBOARD_PERFORMANCE_PREPLAN.md](./DASHBOARD_PERFORMANCE_PREPLAN.md)
> 返回总览：[ROADMAP.md](./ROADMAP.md)
> 阶段报告：[DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md](./DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md)

## 执行规则

- 状态：`[ ]` 未开始，`[-]` 进行中，`[x]` 已完成。
- 默认优先级：P0 -> P1 -> P1.1 -> P1.2 -> P2 -> P3。
- 每次提交需同步：代码变更、基准数据、风险说明。

## 已完成里程碑（截至 2026-03-08）

- 已完成年/月图时间变换去 `melt` 重构，核心热点从秒级下降到亚秒级。
- 已新增渲染耗时拆分指标：`Year Transform`、`Month Transform`。
- 已新增旧/新时间变换对比脚本：`03_Scripts/benchmark_time_transform_pipeline.py`。
- 已完成全量与国家场景基准对比，并通过口径一致性校验（`Parity check: PASS`）。
- 已完成慢模块榜单快照，当前首要优化点转向读取层与渲染策略层。
- 已完成时间轴路径优化：`parse_time_keys` 缓存映射、年度排序优化、月/季/年粒度转换优化。
- 已完成侧栏六级选项 Arrow 下推（国家/细分/动总/品牌/Model/Version）。
- 已完成统一筛选签名与筛选层缓存键：`normalize_filter_payload` + `build_filter_signature`。
- 已新增交互回归脚本：
  - `03_Scripts/regression_time_selector_consistency.py`
  - `03_Scripts/regression_filter_option_pushdown.py`
- 已输出阶段报告与下一阶段计划：`DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md`。

## TODO 列表（50）

1. [x] 创建50任务执行清单
2. [x] 在md记录执行看板
3. [x] 固化全量读取基线命令
4. [x] 固化国家筛选基线命令
5. [x] 增加渲染转换耗时指标
6. [x] 拆分年图数据构建逻辑
7. [x] 拆分月图数据构建逻辑
8. [x] 新建时间列数值化助手
9. [x] 新建按系列时间聚合函数
10. [x] 替换年图melt流程
11. [x] 替换月图melt流程
12. [x] 保留旧函数兼容路径
13. [x] 校验总和模式口径一致
14. [x] 校验分组模式口径一致
15. [x] 校验TopN逻辑一致
16. [x] 校验其他分组逻辑
17. [x] 校验时间轴滑块一致
18. [x] 校验日历输入一致
19. [x] 复用sum_sales结果缓存
20. [x] 复用numeric_axis结果缓存
21. [x] 限制默认非活跃图渲染
22. [x] 添加主图优先开关
23. [x] 优化高级图惰性加载
24. [x] 减少重复groupby计算
25. [x] 优化parse_time_keys开销
26. [x] 优化year排序开销
27. [x] 优化month粒度转换
28. [x] 侧栏国家选项下推
29. [x] 侧栏细分选项下推
30. [x] 侧栏动总选项下推
31. [x] 侧栏品牌选项下推
32. [x] 侧栏Model选项下推
33. [x] 侧栏Version选项下推
34. [x] 统一筛选签名生成器
35. [x] 增加筛选层缓存键
36. [x] 增强大数据默认策略
37. [x] 增加性能告警阈值文档
38. [x] 更新运维回归清单
39. [x] 更新路线图执行状态
40. [x] 编写性能对比脚本
41. [x] 增加口径一致性脚本
42. [x] 跑全量性能回归
43. [x] 跑国家筛选性能回归
44. [x] 汇总P50P95结果
45. [x] 记录最慢模块榜单
46. [x] 回归CSV下载功能
47. [x] 回归增强图默认路径
48. [x] 回归筛选交互体验
49. [x] 输出阶段完成报告
50. [x] 规划下一阶段优化

## 追加待办（用户反馈 Round 2）

1. [x] 优化国家切换顺序性能一致性
2. [x] 合并概览渲染策略到加载模式模块
3. [x] 重构 NEV 续航分布 23-25 变化逻辑

## 追加待办（用户反馈 Round 3：NEV 洞察增强 25）

1. [x] 建立 Round 3 洞察增强 25 项清单
2. [x] 新增净变化核心指标卡（净变化、绝对变化）
3. [x] 新增结构对冲率指标卡
4. [x] 新增销量加权平均续航指标（含 2025 对 2023 变化）
5. [x] 新增 BEV/PHEV 净变化贡献提示
6. [x] 新增净变化结构拆解折叠面板
7. [x] 新增续航分桶净变化明细表
8. [x] 新增 Top 车型净变化明细（按 |净变化| 排序）
9. [x] 新增高对冲率提示文案
10. [x] 统一 TopN 控件口径文案（按当前口径）
11. [x] 将净变化口径从固定 23-25 改为“时间轴首末年动态净变化”
12. [x] 将年度销量与 caption 文案改为动态年标签（首年/中间年/末年）
13. [x] 新增正负向 Top 车型分栏展示
14. [x] 新增 TopN 绝对变化集中度阈值告警
15. [x] 新增续航分桶贡献排序（Top 正/负区间）
16. [ ] 新增品牌层净变化贡献明细
17. [ ] 复用现有 CSV 导出底座接入洞察结果导出（`build_preview_csv_payload` + `st.download_button`）
18. [ ] 复用现有 PNG 导出底座接入图表导出注释模板（`render_plotly_chart_with_png_export`）
19. [ ] 复用现有回归框架新增净变化口径一致性回归脚本
20. [ ] 复用现有回归框架新增洞察表字段完整性回归脚本
21. [ ] 复用现有回归框架新增 TopN 排序口径一致性回归脚本
22. [ ] 复用渲染计时框架增加洞察计算耗时采样与告警阈值
23. [ ] 文档补充：NEV 洞察口径定义与解读指南
24. [ ] 新增动态时间窗边界提示与单年回退文案
25. [ ] 输出 Round 3 阶段小结与下一步计划

> 备注：按用户指令，Round 3 剩余项暂缓实现，统一保留为待办。

### Round 3 轮子复用清单（已具备）

- [x] 时间轴选择与范围复用：`get_time_selection_for_chart` + `TimeSelection`
- [x] PNG 导出复用：`render_plotly_chart_with_png_export` + `export_figure_png`
- [x] CSV 导出护栏复用：`build_preview_csv_payload` + `regression_csv_download_guardrails.py`
- [x] 回归脚本接入复用：`03_Scripts/ci_smoke_check.py` 统一调度
- [x] 渲染耗时采样复用：`render_dashboard` 的 `render_timing` 框架
