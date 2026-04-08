# Dashboard 国家筛选性能优化 Preplan（基座思维）

> 文档定位：在不牺牲可维护性的前提下，将“默认国家筛选后作图约 10s”优化到可交互时延目标。
> 返回总览：[ROADMAP（总览导航）](./ROADMAP.md)

## 1. 背景与目标

### 背景

当前在大数据量场景下，国家筛选后进入图表分析仍存在明显等待，体感约 `10s` 左右。

### 目标

- 用户目标：国家筛选后，主图进入可交互状态显著提速。
- 技术目标：优先优化“图表前计算管线”，而不是盲目继续压缩 I/O。
- 过程目标：遵循基座思维，先建立稳定可复用基座，再叠加功能点优化。

## 2. 已确认基线（2026-03-08）

### 数据规模

- 分区数据集：`04_Processed_data/partitioned_dataset_v1`
- 总行数：`724,501`
- 字段数：`91`
- 国家字段：`国家`
- 时间列：`37` 月度列 + `4` 年度列

### 读取层基线

命令：

```bash
PYTHONPATH=05_DashBoard python 03_Scripts/benchmark_dashboard_load.py --repeats 2
```

结果摘要：

- 侧边栏读取 avg：`~0.57s`
- 分析投影读取 avg：`~1.13s`
- 全列读取 avg：`~4.33s`
- 投影相对全列加速：`~3.82x`

国家筛选下推读取（分析投影）实测：

- 德国：`117,338` 行，`~0.17s`
- 意大利：`97,952` 行，`~0.15s`
- 丹麦：`23,427` 行，`~0.07s`

结论：国家筛选下推已经有效，读取层不是当前 `10s` 的主瓶颈。

### 计算层热点

实测热点在图表前数据变形：

- 全量场景：
  - 年图管线约 `7.60s`
  - 月图管线约 `6.28s`
- 德国场景：
  - 年图管线约 `1.12s`
  - 月图管线约 `0.82s`

根因是：同一批数据在年/月图中重复执行大规模 `melt + groupby`。

### 时间变换基准（P50/P95，2026-03-08）

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

### 慢模块榜单快照（优化后，2026-03-08）

基于最新基准（读取层 `benchmark_dashboard_load.py --repeats 2` + 时间变换基准）整理：

1. 分析投影读取（Analysis Load）avg `~1.117s`
2. 侧边栏读取（Sidebar Load）avg `~0.576s`
3. 分组时间变换（group:powertrain，全量）P95 `~0.2319s`
4. 总和时间变换（sum，全量）P95 `~0.0501s`

说明：年/月图核心转换已不再是第一慢点，后续应优先关注读取层与非活跃图渲染策略。

## 3. 根因定位（代码锚点）

- 年/月长表构建：`05_DashBoard/dashboard/views.py:386`
- 年图入口：`05_DashBoard/dashboard/views.py:1716`
- 月图入口：`05_DashBoard/dashboard/views.py:1807`
- 总渲染流程：`05_DashBoard/dashboard/views.py:4424`
- 读取与筛选主流程：`05_DashBoard/dashboard/runner.py:150`
- 读取实现：`05_DashBoard/dashboard/data.py:251`
- 侧栏筛选实现：`05_DashBoard/dashboard/filters.py:187`

## 4. 基座思维（必须满足）

基座思维要求：每一轮优化都先建设可复用、可观测、可回滚的基础层，再做上层局部提速。

### 4.1 观测基座

- 每个阶段必须有可量化指标（读取、转换、绘图）。
- 优化前后必须可复现对比（同数据版本、同筛选条件）。

### 4.2 数据基座

- 优先在数据层完成裁剪、过滤和聚合。
- 降低把“大明细表直接喂给图层”的概率。

### 4.3 计算基座

- 同一输入只做一次重计算，多图复用中间结果。
- 能用列聚合就不要先 `melt` 成超长表。

### 4.4 渲染基座

- 默认路径只渲染必要图表，非关键图按需触发。
- 交互控件变化尽量避免触发全链路重算。

### 4.5 运维基座

- 变更必须具备灰度开关与回退路径。
- 发布后持续看 P50/P95，而不是只看单次最优值。

## 5. 分阶段 Preplan（按优先级）

### P0 基线固化（先做）

目标：把当前瓶颈和目标量化成门槛，避免“优化后无证据”。

动作：

- 固化两类基准：`全量` 与 `国家筛选`。
- 在渲染耗时面板中补充“转换耗时”分解。
- 形成基准记录模板（命令、版本、结果）。

产出：可比较的性能基线和回归标准。

### P1 计算基座重构（最高收益）

目标：去掉年/月图对明细表的双重 `melt`。

动作：

- 新增“先聚合后成图”的时间聚合函数：
  - 总和模式：`df[selected_columns].sum(axis=0)`
  - 分组模式：`df.groupby(group_col)[selected_columns].sum()`
- 年图与月图共享同一中间聚合结果，不重复构建长表。
- 仅在绘图前将已聚合的小表转换为 Plotly 需要的结构。

预期收益：

- 全量场景年/月转换耗时从 `~13s+` 降到 `1s` 级以内（目标值，需复测）。
- 国家筛选场景进一步降到亚秒级到低秒级。

风险与控制：

- 风险：分组+TopN 逻辑口径变化。
- 控制：先做“口径一致性回归”（同筛选同时间窗对比总量和分组）。

### P1.1 复用基座补齐

目标：去掉同一轮渲染中的重复 `to_numeric/sum/groupby`。

动作：

- 对 `sum_sales_for_columns` 结果按 `(filter signature, selected_columns)` 复用。
- 对 `prepare_numeric_axis` 的结果在当前渲染周期复用。
- 将默认增强图所需公共中间结果复用到相关图表。

预期收益：减少 `0.5s~2s` 级的重复 CPU 消耗。

### P1.2 渲染基座策略

目标：默认路径只算用户当前真正在看的图。

动作：

- 大数据模式下默认采用“主图优先渲染”。
- 非当前图表延后到用户切换时再计算。

预期收益：首屏可交互时延下降 `20%~40%`（依机器配置浮动）。

### P2 筛选基座优化（中期）

目标：降低侧栏筛选在 Pandas 层的级联过滤成本。

动作：

- 侧栏选项改为 Arrow 端 `distinct + filter pushdown`。
- 仅把“选项集合”拉回前端，而不是拉整表再 `unique/sort`。

预期收益：高基数筛选器联动更流畅，减少筛选输入延迟。

### P3 运维基座闭环（持续）

目标：把性能优化纳入日常发布标准。

动作：

- 在 `OPERATIONS_TEMPLATES.md` 回归清单中增加国家筛选场景门槛。
- 每次发布记录 `P50/P95` 和最慢模块排名。

产出：性能回归不依赖个人经验，形成团队流程。

## 6. 验收标准（DoD）

- 国家筛选后主图可交互：`<= 2s`（目标）
- 全量场景年/月主图计算：`<= 2.5s`（目标）
- 读取层保持：分析投影 `~1s` 级，不因优化反向退化
- 口径一致：优化前后销量总和与分组结果一致
- 回归可追溯：保留命令、数据版本、结果快照

## 7. 执行顺序建议

1. 先做 `P0`（基线固化）
2. 再做 `P1`（计算基座重构）
3. 接着做 `P1.1` 和 `P1.2`（复用 + 渲染策略）
4. 最后推进 `P2`（筛选基座）与 `P3`（运维闭环）

## 8. 非目标（本轮不做）

- 不先切换底层 BI 引擎或重写整个图层框架。
- 不以牺牲指标口径一致性换取短期速度。
- 不以关闭功能作为主要优化手段。

## 9. 附：建议复测命令

```bash
# 读取层基线
PYTHONPATH=05_DashBoard python 03_Scripts/benchmark_dashboard_load.py --repeats 3

# 应用层（手工）
streamlit run 05_DashBoard/app.py
# 检查“图表渲染耗时（本次）”面板并记录最慢模块
```

## 10. 执行进展（已完成）

- 已落地 `P1` 核心改造：`build_time_long_dataframe` 由行级 `melt` 改为聚合后构图。
- 已落地观测增强：年/月图拆分为转换耗时与绘图耗时。
- 已落地基准工具：`03_Scripts/benchmark_time_transform_pipeline.py`（支持旧/新性能对比 + 口径校验）。
- 已完成基准对比：全量 `sum` 模式约 `71x`，全量 `group:powertrain` 模式约 `14x`。
- 已完成口径校验：总和/分组/TopN/其他分组在采样场景下 `max_abs_diff=0.0`。
- 已落地主图优先渲染策略：非活跃概览图可按需加载。
- 已落地增强图惰性渲染策略：默认可延迟加载增强分析图。
- 已完成 CSV 下载回归：新增 `03_Scripts/regression_csv_download_guardrails.py`，验证截断与大小阈值行为。
- 已完成单次渲染周期缓存：`sum_sales_for_columns`、`prepare_numeric_axis`、`get_series_contribution` 复用。
- 已抽离渲染默认策略函数：`get_default_render_strategy`，并新增回归脚本 `03_Scripts/regression_render_strategy_defaults.py`。
- 已完成时间轴交互一致性回归：新增 `03_Scripts/regression_time_selector_consistency.py`。
- 已完成侧栏六级选项 Arrow 下推：国家/细分/动总/品牌/Model/Version。
- 已完成筛选签名与缓存键统一：`normalize_filter_payload` + `build_filter_signature`。
- 已完成筛选交互回归：新增 `03_Scripts/regression_filter_option_pushdown.py`。
- 已输出阶段完成报告与下一阶段计划：`DASHBOARD_PERFORMANCE_PHASE_REPORT_20260308.md`。
