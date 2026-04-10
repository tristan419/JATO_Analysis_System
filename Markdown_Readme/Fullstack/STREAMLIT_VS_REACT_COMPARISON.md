# Streamlit vs React App — 功能逐项对比 + 原子级行为差异

> 更新时间：2026-04-09
> 本文档是当前 Fullstack 对比的主文档。若与 `REACT_STREAMLIT_GAP_ANALYSIS.md` 有出入，以本文为准。
> 左列 = Streamlit 现有功能 (05_DashBoard/dashboard/views.py, 7819行)
> 右列 = React App 现有实现 (06_AppPlatform/frontend，以 DashboardPage.tsx + ExportPanel.tsx + RvFinanceDashboard.tsx 为主)
> 📁 文件夹已拆分：Streamlit相关文档 → `Markdown_Readme/Streamlit/`，Fullstack相关文档 → `Markdown_Readme/Fullstack/`

---

## 0.0 动力总成固定配色规范（全局）

> 所有涉及动力总成（Powertrain）维度的图表 **一律** 使用以下固定颜色，不可被配色方案覆盖。

| 动力类型 | 颜色 | Hex 值 | 说明 |
|----------|------|--------|------|
| **ICE** | 灰色 | `#6b7280` | 传统内燃机 |
| **MHEV** | 橙色 | `#f97316` | 轻混 |
| **HEV** | 黄色 | `#eab308` | 混动 |
| **PHEV** | 蓝色 | `#3b82f6` | 插混 |
| **BEV** | 绿色 | `#22c55e` | 纯电 |

**适用范围：**
- 散点/气泡图 `color=Powertrain`（动力气泡图、电池容量 vs MSRP、估算TCO）
- 堆叠条形图 `stackKey=Powertrain`（动力×价格带、NEV续航分布）
- 分组时序图 `groupBy=动总规整`
- 版型气泡图 `colorBy=Powertrain`
- 导出面板/图例/pill 控件

---

## 0. 技术栈对比

| 维度 | Streamlit | React App |
|------|-----------|-----------|
| 前端框架 | Streamlit (Python → WebSocket) | React 19 + Vite 7 |
| 图表引擎 | **Plotly.js** (px.scatter/line/bar/area + go.Waterfall/Contour) | **Plotly.js + react-plotly.js** ✅ 已对齐 |
| 交互能力 | Plotly toolbar: zoom/pan/box-select/lasso/hover | Plotly toolbar: zoom/pan/box-select/lasso/hover ✅ 已对齐 |
| 后端 | Streamlit内置 (单进程) | FastAPI + Parquet |
| 部署 | `streamlit run` | `vite dev` + `uvicorn` |

## 0.1 本轮已落地

| 功能 | 状态 | 说明 |
|------|------|------|
| 全局时间轴 | ✅ 已实现 | 支持滑块/日历双模式、年/月粒度切换、时间范围过滤 |
| 图表缩放/平移 | ✅ 已实现 | Dashboard 主图已切换到 Plotly，获得原生 zoom/pan/toolbar |
| 导出面板 | ✅ 已实现 | 支持网格线、图例、配色、标题、尺寸、PNG 导出，且标签语义已统一到 `model / sales / value / series` |
| RV 金融仪表板 | ✅ 已实现 | 支持可编辑车辆表、模板应用/重置、预设/手动汇率、月供对比、瀑布图、敏感性分析、等高线 |
| 默认筛选预设 | ✅ 已实现 | 动总规整默认 ICE/HEV/BEV/MHEV/PHEV，细分市场支持一键筛选 SUV |
| 月度/年度时间过滤修复 | ✅ 已实现 | 前端按真实时间顺序过滤，不再因字符串比较导致 2025.1-2025.12 明细消失 |
| Dashboard / Specification 页面缓存 | ✅ 已实现 | 两页切换时恢复筛选、概览、明细、图表与分页状态，使用内存 + sessionStorage TTL 缓存，避免重复首帧取数 |
| Hero 指标重构 | ✅ 已实现 | Dashboard 首屏指标改为 Total sales + Version count，并为 loading 态增加数字冲高 + 液位填充视觉反馈 |
| 气泡尺寸统一 | ✅ 已实现 | 动力气泡图、版型气泡图、OJ 定位图统一使用 Plotly sizeref/sizemode=area，同一套尺寸算法 |
| Dashboard 首屏启动链路优化 | ✅ 已实现 | 首屏直接使用 URL / 默认筛选请求 overview，顶层 filter options 改为并发，移除空筛选 overview 的重复聚合 |
| 全维度筛选 / Market Overview rail 折叠 | ✅ 已实现 | 移除 Current scope 大面板，左侧筛选与 Hero 均支持 rail 化折叠，统一使用 28px 微按钮控制 |
| Full timeline 筛选可见性修复 | ✅ 已实现 | 首屏 bootstrap 会补齐 make / model / version 级联 options，Hero rail 直接展示 active lens token，不再依赖拖动时间轴才看清当前筛选 |
| Dashboard 响应式密度修正 | ✅ 已实现 | 左侧 KPI 数字改成长度感知字号，Hero 展开态缩小到更紧凑比例，收纳态进一步压缩并统一断点行为 |

## 0.2 2026-04-09 修复说明与后续待办

### 已修复

- 首次进入 `/?powertrain=ICE,HEV,BEV,MHEV,PHEV` 时，不再先打一轮空筛选 `overview`，首屏聚合直接按当前筛选执行。
- `make / model / version` 在 full timeline 初始态也会正常出现在筛选链和 Hero summary 中，不再出现“拖一下时间轴才显示”的错觉。
- `Current scope` 卡片已下线，Dashboard 改为更轻的 rail 交互，折叠态在桌面和移动端都保持单行/单轨语义。
- `全维度筛选` 摘要卡与 `01 / Market Overview` Hero 已统一响应式密度规则，长数字不再冲出卡片边界，展开态和收纳态高度进一步压缩。

### 待办

- `plotly-vendor` 仍然是首屏最大静态资源，后续继续做 chunk 拆分与压缩验证。
- `groupedTimeSeries`、高级分析等次级请求仍未做 abort / request dedupe，快速切换筛选时还有继续优化空间。
- 月度分组模式的 `monthGrain` 仍未完全对齐 Streamlit，这是剩余的主要行为差距之一。

## 0.3 2026-04-09 第一阶段已落地

- 顶部 `Overview / Specification / Control` 在窄屏下已切到 hamburger + drawer，不再继续依赖固定卡片换行。
- `全维度筛选` 的桌面折叠态已改成 toggle-only rail，竖排/倒置文本已移除。
- `/v1/filters/options` 已去掉无收益的 `rowCount` 双扫描负担，Dashboard 与 Specification 均已切到顶层 eager + 下游按需加载。
- 前端已为 options 请求补上 TTL cache 与 abort；PostgreSQL 仍不作为本轮首选方案。

---

## 1. 全局时间轴

| 功能点 | Streamlit ✅ | React App |
|--------|-------------|-----------|
| 滑块拖拽模式 | ✅ 双端 select_slider | ✅ 双端 dual-slider (单轨双拖柄) |
| 日历输入模式 | ✅ date_input 起止 | ✅ 文本输入 + 应用按钮 |
| 模式切换 | ✅ radio 滑动条/日历 | ✅ 按钮切换 |
| 自动提取可用时间范围 | ✅ build_time_axis | ✅ 从 overview API 获取 |
| 年/月Tab切换 | ✅ | ✅ |
| 全局控制所有图表时间窗口 | ✅ render_global_time_controls | ✅ timeRange 状态过滤 |
| 单图跟随/自定义切换 | ✅ resolve_chart_time_selection + checkbox | ❌ |
| 月度粒度切换 (月/季/年) | ✅ selectbox + convert_dates_to_period_start | ⚠️ `monthGrain` 已接入，当前总和模式支持前端聚合，分组模式仍按月 |
| 时间窗KPI (当前窗口销量) | ✅ st.metric 随时间窗联动 | ✅ timeWindowSales 从 filteredSingle 计算 |

**差距等级：🟡 中等（基础已落地，细节缺失）**

---

## 2. 图表缩放 / 平移 / 工具栏

| 功能点 | Streamlit ✅ | React App |
|--------|-------------|-----------|
| 缩放 (Zoom) | ✅ Plotly内置 | ✅ Plotly内置 |
| 平移 (Pan) | ✅ Plotly内置 | ✅ Plotly内置 |
| 框选放大 (Box Select) | ✅ Plotly内置 | ✅ Plotly内置 |
| 套索选择 (Lasso) | ✅ Plotly内置 | ✅ Plotly内置 |
| 自动缩放 (Autoscale) | ✅ Plotly内置 | ✅ Plotly内置 |
| 重置坐标轴 | ✅ Plotly内置 | ✅ Plotly内置 |
| 鼠标滚轮缩放 | ✅ Plotly内置 | ✅ Plotly内置 |
| 工具栏（toolbar） | ✅ PLOT_CONFIG | ✅ Plotly默认 |

**差距等级：✅ 已对齐**

---

## 3. 导出 / 样式面板

| 功能点 | Streamlit ✅ | React App |
|--------|-------------|-----------|
| X网格线开关 | ✅ | ✅ |
| Y网格线开关 | ✅ | ✅ |
| 坐标轴线开关 | ✅ | ✅ |
| 图例显示/隐藏 | ✅ | ✅ |
| 图例位置 (右/顶/底/左) | ✅ | ✅ |
| 配色方案 (6种) | ✅ | ✅ |
| 字体大小 (8-24) | ✅ slider | ✅ number input |
| 网格线颜色 | ✅ color_picker | ✅ color input |
| 坐标轴线颜色 | ✅ color_picker | ✅ color input |
| X轴刻度格式 (6种) | ✅ | ✅ |
| Y轴刻度格式 (6种) | ✅ | ✅ |
| 小数位控制 (0-4) | ✅ slider | ✅ number input 0-4 |
| 整体背景色 | ✅ | ✅ |
| 绘图区背景色 | ✅ | ✅ |
| 图表标题 | ✅ | ✅ |
| X轴标题 | ✅ | ✅ |
| Y轴标题 | ✅ | ✅ |
| 导出宽度 | ✅ slider 800-2400 | ✅ number 400-2400 |
| 导出高度 | ✅ slider 500-1800 | ✅ number 300-1800 |
| 数据标签模式 (7种) | ✅ 实际应用到图表 | ✅ applyDataLabelsToTraces 已应用到 trace |
| 数据标签位置 (5种) | ✅ 实际应用到图表 | ✅ applyDataLabelsToTraces 已应用到 trace |
| 自定义标签模板+预设 | ✅ | ❌ |
| 逐系列手动改色 (最多30) | ✅ color_picker ×N | ✅ ExportPanel 逐系列 color_picker |
| PNG导出 | ✅ kaleido引擎 | ✅ Plotly.downloadImage |
| CSV下载 | ✅ | ✅ |

**差距等级：🟢 基本一致（24/25已实现，1项缺失）**

---

## 4. KPI 卡片

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 筛选后记录数 | ✅ header card | ✅ header card |
| 累计销量 (全局时间窗) | ✅ st.metric | ✅ kpi-card |
| 品牌数 | ✅ st.metric | ✅ kpi-card |
| Model 数 | ✅ st.metric | ✅ kpi-card |
| Version 数 | ✅ st.metric | ✅ kpi-card |
| 选择口径说明 | ✅ st.caption | ✅ kpi-caption 显示筛选口径+时间窗 |

**差距等级：🟢 基本一致**

---

## 5. 时序图 — 年度Tab

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 总和/分组切换 | ✅ st.radio | ✅ tab-btn |
| 折线图 | ✅ px.line | ✅ Plotly scatter+lines |
| 累积条形图 | ✅ px.bar barmode="relative" | ✅ Plotly barmode="relative" |
| 分组维度选择 | ✅ st.selectbox | ✅ select |
| Top N 控制 | ✅ st.checkbox + st.number_input | ✅ checkbox + number |
| "其他"汇总显示 | ✅ st.checkbox | ✅ checkbox |
| "其他"明细展开表 | ✅ render_top_n_others_detail | ✅ details展开汇总表 |
| 数据标签 | ✅ 复用导出面板 | ✅ applyDataLabelsToTraces |
| 配色/字体统一 | ✅ apply_export_palette | ✅ POWERTRAIN_COLORS 固定配色 + 导出面板 |
| 单图时间覆盖 | ✅ resolve_chart_time_selection | ❌ |
| 系列点击显示/隐藏 | ✅ Plotly legend click | ✅ pill按钮 |

**差距等级：🟡 中等 (7/11)**

---

## 6. 时序图 — 月度Tab

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 月度折线/条形 | ✅ px.line/bar | ✅ Plotly |
| 粒度切换 (月/季/年) | ✅ st.selectbox | ⚠️ 总和模式已接入 `monthGrain` 前端聚合，分组模式仍按月 |
| 时间窗KPI | ✅ st.metric | ✅ timeWindowSales 从 filteredSingle 计算 |
| 分组模式 | ✅ 与年度共享 | ✅ 共享 |
| 数据标签 | ✅ | ✅ applyDataLabelsToTraces |

**差距等级：🟡 部分一致 (4/5)**

---

## 7. 高级分析 — 市场结构组

### 7a. 动力气泡图 (powertrain_bubble)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 基础散点 (Length×MSRP, color=Powertrain) | ✅ px.scatter | ✅ ScatterChart |
| 气泡大小=Sales | ✅ size="BubbleSize" | ✅ Plotly sizeref + raw Sales |
| Top N 控制 | ✅ st.number_input | ✅ advTopN |
| 品牌分面 (facet_col="Brand") | ✅ st.checkbox | ✅ 客户端品牌分面 |
| 气泡倍率放大 | ✅ st.select_slider [2,3,4] | ✅ advBubbleScale ×2/×3/×4 |
| YoY hover叠加 | ✅ st.checkbox + selectbox | ✅ hover + compare year |
| 分组TopN (per-group N) | ✅ 高级设置expander | ✅ per-group TopN |
| 分组维度 (动总/细分) | ✅ st.selectbox | ✅ select |

**React覆盖率：8/8 = 100%**

### 7b. 季节性热力图 (seasonality_heatmap)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 热力图矩阵 (Year×Month) | ✅ px.imshow Blues | ✅ Plotly heatmap Blues |
| 色阶可配 | ✅ color_continuous_scale | ✅ advHeatmapScale |
| Hover显示数值 | ✅ Plotly hover | ✅ Plotly hovertemplate |
| 单图时间覆盖 | ✅ | ❌ |

**React覆盖率：3/4 = 75%**

### 7c. 尺寸段份额 (segment_share_by_length)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 堆叠条形图 | ✅ px.bar stack | ✅ Plotly bar stack |
| 百分比标签 | ✅ texttemplate="%{percentParent:.0%}" | ✅ 自动计算百分比内嵌标签 |
| 带宽滑块 (50-500mm) | ✅ st.slider | ✅ advBandSize |

**React覆盖率：2/3 = 67%**

---

## 8. 高级分析 — NEV 分析组

### 8a. NEV 续航分布 (nev_range_distribution)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 水平堆叠条形图 | ✅ px.bar orientation="h" | ✅ orientation="h" 水平堆叠 |
| 动总类型筛选 (BEV/PHEV) | ✅ st.multiselect | ✅ advPowertrains checkbox组 |
| TopN 启用+滑块 | ✅ st.checkbox + st.slider | ✅ TopN 启用 + number input |
| 续航轴上限 (200-1500) | ✅ st.slider | ✅ advNevAxisMax |
| 续航分箱步长 (10-200) | ✅ st.slider | ✅ advRangeStep (10-200) |
| 分布口径 (当前窗/净变化) | ✅ st.radio | ✅ advNevMetricMode |
| 按Model堆叠 | ✅ st.checkbox | ✅ advNevStackByModel |
| 按品牌分面 | ✅ st.checkbox + slider | ✅ advNevFacetBrand + maxBrandFacets |
| 参数重置按钮 | ✅ st.button | ✅ |
| 增长KPI (4指标) | ✅ 4×st.metric | ✅ 4×kpi-card |
| 结构分解表 | ✅ 3 tables in expander | ✅ 年度/动力/续航带/Model 明细表 |
| 集中度告警 | ✅ st.warning | ✅ alert-info/warnings |

**React覆盖率：12/12 = 100%**

### 8b. 电池容量 vs MSRP (nev_capacity_vs_msrp)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 散点图 | ✅ px.scatter | ✅ ScatterChart |
| 动总筛选 | ✅ st.multiselect | ✅ advPowertrains checkbox组 |
| TopN滑块 | ✅ st.slider | ✅ advTopN |
| 品牌分面 | ✅ st.checkbox | ❌ |
| 相关系数显示 | ✅ st.caption | ✅ Pearson r 实时计算 |
| 参数重置 | ✅ st.button | ✅ |

**React覆盖率：5/6 = 83%**

---

## 9. 高级分析 — 价格价值组

### 9a. 价格迁移 (price_migration)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 折线图/面积图切换 | ✅ st.radio | ✅ advMigrationMode select |
| 价格带宽滑块 | ✅ st.slider | ✅ advBandSize |
| 分动总查看 (facet) | ✅ st.checkbox | ❌ |
| 动总类型筛选 | ✅ st.multiselect | ❌ |
| MSRP质量摘要 | ✅ P50/P95/IQR | ❌ |
| 峰值价格带表 | ✅ observation table | ❌ |

**React覆盖率：2/6 = 33%**

### 9b. 车长 vs 价格 (length_vs_price)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 散点图 | ✅ px.scatter | ✅ ScatterChart |
| TopN滑块 | ✅ st.slider | ✅ advTopN |
| C-SUV参考线 (4550mm) | ✅ fig.add_vline | ✅ shapes + annotation |
| D-SUV参考线 (4700mm) | ✅ fig.add_vline | ✅ shapes + annotation |
| 价值检测 (≥4700mm 低价) | ✅ 自动识别 | ❌ |

**React覆盖率：4/5 = 80%**

### 9c. 每米价格 (price_per_meter)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 散点图 | ✅ px.scatter | ✅ ScatterChart |
| TopN滑块 | ✅ st.slider | ✅ advTopN |
| hover_name=Model | ✅ | ⚠️ tooltip有 |

**React覆盖率：2/3 = 67%**

### 9d. 销量 vs 价格 (sales_vs_price)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 散点图+SegmentShare气泡 | ✅ px.scatter size="SegmentSharePct" | ✅ ZAxis=SegmentSharePct |
| TopN滑块 | ✅ st.slider | ✅ advTopN |

**React覆盖率：2/2 = 100%**

### 9e. 动力 × 价格带 (powertrain_vs_price)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 堆叠条形图 | ✅ px.bar stack | ✅ Plotly bar stack |
| 价格带宽 | ✅ st.slider | ✅ advBandSize |
| 拆分查看 (facet) | ✅ st.checkbox | ❌ |
| 拆分维度 (品牌/国家) | ✅ st.selectbox | ❌ |
| 最多拆分组数 | ✅ st.slider | ❌ |

**React覆盖率：2/5 = 40%**

---

## 10. 高级分析 — 动力成本组

### 10a. RV 金融杠杆看板 (rv_finance_dashboard) ★最复杂

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| **货币转换面板** | | |
| └ 币种选择 (EUR/SEK/NOK/DKK/GBP/USD) | ✅ st.selectbox | ✅ CURRENCIES select |
| └ 汇率来源 (预设/手动) | ✅ st.radio | ✅ preset/manual |
| └ 手动汇率输入 | ✅ st.number_input | ✅ manualRate input |
| **参数模板** | | |
| └ 模板选择 (平衡/保守/进取/品牌/车型) | ✅ st.selectbox | ⚠️ 当前为国家模板选择 |
| └ 应用模板到全部车型 | ✅ st.button | ✅ |
| └ 国家/品牌/车型预设 (6国) | ✅ resolve_*_finance_preset | ✅ 6国预设 + 后端返回 presets |
| **可编辑表格** | | |
| └ 车型参数编辑 | ✅ st.data_editor dynamic rows | ✅ input表格可编辑行 |
| └ Down Payment % 编辑 | ✅ 0-50 | ✅ number input |
| └ Residual Value % 编辑 | ✅ 30-70 | ✅ number input |
| └ APR % 编辑 | ✅ 0-10 | ✅ number input |
| └ Term 编辑 (12-84月) | ✅ step=12 | ✅ number input |
| **KPI摘要 (3指标)** | ✅ st.metric ×3 | ✅ 4个KPI卡片 |
| **Sub-chart 1: 多车对比柱状图** | ✅ px.bar | ✅ Plotly bar 双Y轴 (月供+总付) |
| **金融结果表** | ✅ st.dataframe | ✅ HTML results table |
| **方案对比** | | |
| └ 基准车型选择 | ✅ st.selectbox | ✅ sensitivity target select |
| └ A/B/C方案参数编辑 | ✅ st.data_editor | ⚠️ 在同一参数表格内编辑 |
| └ 方案KPI卡片 (3×3) | ✅ st.metric ×9 | ❌ |
| **Sub-chart 2: 方案月供对比柱状图** | ✅ px.bar | ✅ 已并入多方案对比主图 |
| └ Delta摘要表 | ✅ st.dataframe | ⚠️ 已并入结果表 delta 列 |
| **Sub-chart 3: 瀑布图** | ✅ go.Waterfall | ✅ Plotly waterfall |
| └ 展示车型选择 | ✅ st.selectbox | ✅ 跟随 sensitivity target |
| **PV-RV + 敏感性 (expander)** | | |
| **Sub-chart 4: PV-RV关系线图** | ✅ px.line | ❌ |
| **月供公式展示** | ✅ st.latex ×5 | ❌ |
| └ 步骤分解表 | ✅ st.dataframe | ❌ |
| └ 净融资额组成 | ✅ st.dataframe | ❌ |
| └ Balloon比率告警 | ✅ st.warning | ❌ |
| **综合敏感性分析** | | |
| └ 敏感性分析对象选择 | ✅ st.selectbox | ✅ select target |
| └ 4参数滑块 (APR/RV/Down/Term) | ✅ st.slider ×4 | ❌ |
| └ 摘要表 (Low/Base/High) | ✅ st.dataframe | ✅ sensitivity summary table |
| **Sub-chart 5: 参数变化柱状图** | ✅ px.bar group | ❌ |
| **Sub-chart 6: 低-基-高趋势图** | ✅ px.line | ❌ |
| **Sub-chart 7: Tornado龙卷风图** | ✅ go.Bar horizontal | ✅ Plotly bar relative |
| **Sub-chart 8: APR×RV等高线图** | ✅ go.Contour 9×9 | ✅ Plotly contour |
| **添加/删除车辆** | ✅ st.button | ✅ add/remove buttons |

**React覆盖率：22/35 = 63%（↑ 从40%）**

### 10b. 估算TCO (estimated_tco)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 散点图 | ✅ px.scatter | ✅ ScatterChart |
| 使用年限滑块 | ✅ st.slider | ✅ tcoYears range 1-10 |
| 年里程滑块 | ✅ st.slider | ✅ tcoAnnualKm range 5000-50000 |
| 折旧率滑块 | ✅ st.slider | ✅ tcoDepreciation range 10%-90% |
| 维保率滑块 | ✅ st.slider | ✅ tcoMaintenance range 0.5%-5% |
| 税费保险滑块 | ✅ st.slider | ✅ tcoTaxInsurance range 0.5%-6% |
| 能源成本基准 | ✅ st.slider | ✅ tcoEnergyCost range 0.02-0.30€/km |
| TopN滑块 | ✅ st.slider | ✅ advTopN |
| 动力能效因子 | ✅ 5种 preset | ⚠️ 后端内置 (BEV=0.55/ICE=1.10) |

**React覆盖率：8/9 = 89%**

---

## 11. 版型气泡图 (Bug 2 新增)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| Model input | N/A (React新功能) | ✅ |
| Version散点 (Length×MSRP) | N/A | ✅ |
| color by Powertrain/Trim | N/A | ✅ |
| 从筛选器快选Model | N/A | ✅ |

**React独有功能 ✅**

---

## 12. OJ 定位定价图 (Bug 3 新增)

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| KMeans聚类散点 | N/A (React新功能) | ✅ |
| 目标车型标记 | N/A | ✅ |
| 手动指定竞品 | N/A | ✅ |
| 聚类Top3展示 | N/A | ✅ |

**React独有功能 ✅**

---

## 13. 明细数据预览

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 数据表格 | ✅ st.dataframe | ✅ HTML table |
| 列选择 | ✅ st.multiselect | ✅ col-chip点选 |
| 列拖拽排序 | ✅ streamlit_sortables | ❌ |
| 预览行数控制 | ✅ st.slider 100-20000 | ✅ 分页 200/页 |
| CSV下载 | ✅ st.download_button | ✅ CSV导出 |
| CSV行数限制 | ✅ 10000 + 12MB guard | ✅ 10000 |
| 零销量过滤 | ❌ | ✅ 有 |

**差距等级：🟢 基本一致 (5/7)**

---

## 14. 侧边栏筛选

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 6级级联筛选 | ✅ 国家→细分→动总→品牌→Model→Version | ✅ 同 |
| 关键词搜索 | ✅ st.text_input | ✅ filter-search |
| 多选 | ✅ st.multiselect | ✅ checkbox list |
| 全选搜索结果 | ✅ st.button | ✅ 全选搜索结果 |
| 清空 | ✅ st.button | ✅ 清空 |
| 匹配/已选计数 | ✅ st.caption | ✅ filter-summary |
| 重置全部 | ✅ st.button | ✅ btn-reset |
| 无国家时延展加载 | ✅ st.toggle | ❌ |
| URL参数同步 | ✅ query_params | ✅ URL读取+replaceState同步 |
| 筛选摘要卡片 | ✅ st.caption | ✅ filter-summary-card |

**差距等级：🟢 基本一致 (8/10)**

---

## 15. 性能/渲染策略

| 功能点 | Streamlit | React App |
|--------|-----------|-----------|
| 懒渲染阈值 (≥80k defer) | ✅ get_default_render_strategy | ❌ |
| 按需加载按钮 | ✅ st.button ×3 | ❌ |
| 渲染耗时面板 | ✅ st.expander timing | ❌ |
| PERFORMANCE_FIRST_MODE | ✅ env var | ❌ |

**差距等级：🟡 中等**

---

## 汇总统计

| 模块 | Streamlit功能项 | React已实现 | 覆盖率 | 变化 |
|------|----------------|-------------|--------|------|
| 全局时间轴 | 9 | 6 | **67%** | ↑ 从11% |
| 缩放/平移/工具 | 8 | 8 | **100%** | ↑ 从0% |
| 导出/样式面板 | 25 | 24 | **96%** | ↑ 标签语义统一 + 逐系列改色 |
| KPI卡片 | 6 | 6 | **100%** | ↑ 口径说明 |
| 年度时序 | 11 | 9 | **82%** | ↑ 数据标签+配色 |
| 月度时序 | 5 | 4 | **80%** | ↑ 总和模式已接入粒度切换 |
| 动力气泡图 | 8 | 8 | **100%** | ↑ YoY + 分组TopN + 分面标题 |
| 季节性热力图 | 4 | 3 | **75%** | ↑ 改为 Plotly heatmap |
| 尺寸段份额 | 3 | 3 | **100%** | ↑ 百分比标签 |
| NEV续航分布 | 12 | 12 | **100%** | ↑ 轴上限/口径/分面/KPI/结构表 |
| 容量vs MSRP | 6 | 4 | **67%** | ↑ 动总筛选+相关系数 |
| 价格迁移 | 6 | 2 | **33%** | ↑ 折线/面积切换 |
| 车长vs价格 | 5 | 4 | **80%** | ↑ 参考线 |
| 每米价格 | 3 | 2 | 67% | — |
| 销量vs价格 | 2 | 2 | 100% | — |
| 动力×价格带 | 5 | 2 | 40% | — |
| **RV金融看板** | **35** | **22** | **63%** | ↑ 手动汇率/模板/汇总表 |
| 估算TCO | 9 | 8 | **89%** | ↑ 6参数滑块 |
| 明细表 | 7 | 5 | 71% | — |
| 侧边栏筛选 | 10 | 8 | 80% | — |
| 性能策略 | 4 | 0 | 0% | — |
| **总计** | **~183** | **~136** | **~74%** | ↑ 导出语义统一 + 热力图/URL/relative 修正 |

---

# 第二部分：原子级行为差异（逻辑比对）

> 以下对比的是**即使同名功能也存在的底层行为差异**，即"看起来都有，但用起来不一样"的地方。

---

### B1. 时间轴选择模型 — "一条轴"vs"两个Tab"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **粒度判定** | `build_time_axis()` **自动判定单一粒度**：如果月度列≥2则选月度，否则选年度。全页面统一粒度。 | **手动双Tab**：年度Tab和月度Tab是两个独立按钮，用户手动切换。两套数据并行存在于state中。 |
| **数据源** | 单一 DataFrame 列扫描确定可用时间列 | 后端 overview API 同时返回 yearSeries + monthSeries 两套数据 |
| **切换含义** | 不可切换 - 系统自动选择最优粒度 | 可自由切换，但切换后时间范围重置 |

**影响**：Streamlit用户无须选择粒度（系统自动识别），React用户需手动切换Tab。

---

### B2. 滑动条交互 — "原生双端控件"vs"双端轨道 + 双 range 驱动"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **控件形态** | `st.select_slider` **单控件双端**：一条滑轨上有起止两个拖柄 | React 用一条可视双端滑轨承载交互，但底层由两个重叠的 `<input type="range">` 驱动 |
| **即时反馈** | 拖动→松手→**立即重新渲染全部图表** | 拖动→松手→**立即更新标签与前端过滤结果**，无需每次重新请求 overview |
| **联动保护** | Streamlit select_slider 原生保证 start ≤ end | React 代码手动保证 `Math.max(s, endIdx)` |

**影响**：React 已经达到与 Streamlit 接近的双端交互体验，但底层实现仍是“前端组合控件”，不是框架原生单控件。

---

### B3. 筛选触发 — "选即变全部"vs"Overview自动 + 高级图手动"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **筛选变更** | 任何筛选器变更 → `st.rerun()` → **全页面重新执行** → 所有图表 + KPI 全量刷新 | 筛选变更 → `useEffect` 检测 `filterPayload` 变化 → **自动刷新 Overview**，并对已加载的高级图/明细做二次自动刷新 |
| **图表刷新范围** | **全量**：时序图、高级分析图、明细表全部同步刷新 | **分层**：Overview 永远自动；高级分析/明细在“首次手动加载后”会随筛选自动刷新；版型气泡图和定位图仍保持手动触发 |
| **感知差异** | 用户感觉"选完就变了" | 用户感觉“常驻主图自动联动，重图/重表首次按需加载”，是局部响应式而不是整页 rerun |

**影响**：React 现在不是“只刷 KPI 不刷重图”，而是“首次按需、之后自动联动”；与 Streamlit 的整页 rerun 仍然是不同实现范式。

---

### B4. 各功能区加载方式对比

| 功能区域 | Streamlit | React | 差异等级 |
|----------|-----------|-------|----------|
| Overview (KPI+时序图) | 页面打开自动加载 + 筛选变更自动刷新 | 页面打开自动加载 + 筛选变更自动刷新 | **一致** |
| 分组时序图 | 切换到"分组"模式时自动计算 | 切换到"分组"模式时 useEffect 自动调用 (300ms debounce) | **一致** |
| 高级分析图 (6组18图) | 默认自动渲染（除非>200k行触发懒渲染） | **首次手动点击「加载图表」**，加载过后筛选变更自动刷新 | 🟡 |
| 版型气泡图 | N/A (React新功能) | 手动点击「加载版型」 | — |
| OJ定位图 | N/A (React新功能) | 手动点击「加载定位图」 | — |
| 明细数据表 | **页面加载自动渲染** | **首次手动点击「加载明细」**，加载过后筛选变更自动刷新 | 🟡 |
| RV金融 | 输入后自动计算 | **必须手动点击「计算」** | 🟡 中等 |

---

### B5. 时间轴作用范围 — "全局+局部可覆盖"vs"仅全局"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **全局时间** | ✅ `render_global_time_controls()` 渲染在页面顶部 | ✅ `<TimeAxis>` 组件渲染在顶部 |
| **局部覆盖** | ✅ 每张高级分析图有 `resolve_chart_time_selection()` + "跟随全局时间轴" checkbox。取消勾选后出现独立时间选择器 | ❌ 无此功能。所有图表共享同一 `timeRange` |
| **使用场景** | "年度趋势看5年，月度明细只看最近6个月" — 不同图表不同时间窗 | 所有图表必须使用相同时间窗口 |

**影响**：分析师无法在同一页面内对不同图表使用不同时间窗。

---

### B6. 月度子粒度 — "后端统一聚合"vs"总和模式前端聚合"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **UI** | 月度Tab内有 selectbox "月/季/年" | TimeAxis组件有 `monthGrain` 按钮 (月/季/年) |
| **聚合逻辑** | `convert_dates_to_period_start()` 将月度数据统一聚合为季度或年度 | React 在 `aggregatedSingle` 中对总和模式做前端聚合；分组模式仍直接使用原始月度分组数据 |
| **实际效果** | 选"季"→图表X轴变为Q1/Q2/Q3/Q4、数据自动聚合 | 总和模式下“季/年”有效；分组模式下当前仍保持月粒度 |

---

### B7. KPI与时间联动 — "全量时间窗联动"vs"累计销量已联动"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **累计销量KPI** | `sum_sales_for_columns(filtered_df, time_selection.columns)` — **仅统计时间窗内的销量** | `timeWindowSales` 基于 `filteredSingle` 重新计算，**已随时间窗联动** |
| **KPI标题** | "累计销量（全局时间窗）" + caption 显示 "2022 Jan ~ 2024 Dec" | 标题与 caption 都会显示当前时间窗 |
| **品牌数/Model数/Version数** | 基于筛选+时间窗计算 | 仍主要基于筛选后的 overview 结果，不细分到时间窗 |

**影响**：React 的“累计销量”已经和 Streamlit 对齐，但品牌/Model/Version 计数仍不是严格时间窗口口径。

---

### B8. 季节性热力图 — "Plotly heatmap"vs"Plotly heatmap"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **渲染方式** | `px.imshow()` Plotly heatmap + colorscale | Plotly `heatmap` trace + `advHeatmapScale` 色阶 |
| **交互能力** | hover弹出详细数值、缩放、选区 | 同样支持 hover、缩放、选区与统一导出布局 |
| **配色** | Plotly color_continuous_scale 多种色阶 | 支持 `Blues / Viridis / YlOrRd / RdBu / Greens / Hot` |

---

### B9. 导出面板数据标签 — "Figure 原位改写"vs"Trace 元数据驱动"

| 维度 | Streamlit | React |
|------|-----------|-------|
| **标签模式** | 7种模式 (关闭/仅数值/仅系列名/仅Model/系列名+数值/Model+Sales/自定义字段) | 7种模式（`ExportLabelMode`），并可按图表限制仅展示当前支持的模式 |
| **字段来源语义** | 主要依赖 `trace.name`、`y/x`、`hovertext`、`marker.size` 等现有 Plotly 字段推导 | 通过 `withExportLabels()` 显式注入 `model / sales / value / series` 元数据，再由 `applyDataLabelsToTraces()` 统一解析；无元数据时才回退到 trace 推导 |
| **应用逻辑** | `apply_export_data_labels()` 原位修改 `go.Figure` 中各 trace 的 `text/texttemplate/textposition` | `applyDataLabelsToTraces()` 先复制 trace，再重写 `customdata`、`text`、`texttemplate`、`textposition` 和 scatter `mode` |
| **实际效果** | 选"仅数值"→柱状图上方出现数字标签 | 同样会真实落到 trace；且 React 现在保证 `model` 就是 model、`sales` 就是 sales，不再混用 `text/y/customdata` 的偶然位置 |

**实现结论**：两者都已经是“真实改 trace”的实现，但 Streamlit 更偏 `Figure` 原位加工，React 更偏“语义元数据 + 纯函数重写 trace”。

---

### B10. 逐系列手动改色

| 维度 | Streamlit | React |
|------|-----------|-------|
| **实现** | `collect_export_series_color_defaults()` 提取各 trace 颜色 → 渲染最多30个 `st.color_picker` → `apply_manual_series_colors()` | `ExportPanel` 基于 `seriesNames` 渲染颜色选择器，再通过 `applySeriesColors()` 将 override 写回 marker/line |
| **使用场景** | 手动把某个品牌设为特定颜色用于汇报 | 同样可用于单图导出前的局部配色修正 |

---

### B11. "其他"分类明细

| 维度 | Streamlit | React |
|------|-----------|-------|
| **功能** | 启用 Top N 后，非Top项归入"其他"。`render_top_n_others_detail()` 在 expander 中展示被合并的条目（名称+销量+占比） | `othersDetail` 已接入 `details` 展开表，可显示名称、销量、占比 |
| **信息可及性** | 可看到"其他"包含哪些品牌/Model | 同样可以展开看到被合并条目的细项 |

---

### B12. 懒渲染策略

| 维度 | Streamlit | React |
|------|-----------|-------|
| **机制** | `get_default_render_strategy(row_count)`: <80k全量; 80k-200k延迟Overview; >200k延迟高级图 | 没有按数据量动态切换策略，但高级图始终采用手动按需加载 |
| **目的** | 防止大数据量下页面卡死 | 统一控制首屏负载，而不是按数据规模分级调度 |

---

### B13. URL参数持久化

| 维度 | Streamlit | React |
|------|-----------|-------|
| **入参** | `hydrate_filter_states_from_query_params_once()` 从URL读取`?countries=德国&segments=C` 恢复筛选 | 启动时读取 URL query，恢复国家、品牌、细分市场、时间等筛选 |
| **出参** | `sync_query_params_from_selections()` 筛选变更后同步到URL | 使用 `history.replaceState` 持续回写当前筛选状态 |
| **影响** | 可分享带筛选状态的链接给同事 | React 现在也支持基于 URL 的可分享筛选态 |

---

### B14. 条形图 barmode 差异

| 维度 | Streamlit | React |
|------|-----------|-------|
| **分组时序条形图** | `barmode="relative"` — 正负分开显示 | `barmode="relative"`，与 Streamlit 一致 |
| **视觉差异** | 负增长会向下展示 | 同样按 relative 模式上下分离显示 |

---

### B15. 高级分析子控件缺失汇总

| 图表 | Streamlit独有控件 | React缺失 |
|------|-------------------|-----------|
| 动力气泡图 | ~~品牌分面 facet_col~~、~~气泡倍率×2/3/4~~、~~YoY叠加hover~~、~~分组TopN (per-group N)~~、~~分组维度选择~~ | 无 |
| NEV续航分布 | ~~BEV/PHEV筛选~~、~~续航轴上限~~、~~步长~~、~~分布口径~~、~~按Model堆叠~~、~~按品牌分面~~、~~参数重置~~、~~增长KPI(4指标)~~、~~结构分解表~~、~~集中度告警~~ | 无 |
| 电池容量vsMSRP | ~~动总筛选~~、品牌分面、~~相关系数显示~~ | 品牌分面 |
| 价格迁移 | ~~折线/面积切换~~、分动总查看、动总筛选、MSRP质量摘要、峰值价格带表 | 分动总查看、动总筛选、摘要/表 |
| 车长vs价格 | ~~C-SUV参考线~~、~~D-SUV参考线~~、"价值检测"自动标注 | 价值检测 |
| 动力×价格带 | 拆分查看(facet)、拆分维度、最多拆分组数 | 全部缺失 |
| 估算TCO | ~~使用年限/年里程/折旧率/维保率/税费保险/能源成本基准 6滑块~~ + 动力能效因子 | 能效因子前端可配（后端内置） |

---

## 原子级行为差异严重度排名

| # | 差异 | 严重度 | 说明 |
|---|------|--------|------|
| 1 | B3: 高级图/明细需手动加载 | ✅ 已修复 | 筛选变更后自动重新加载已有数据 |
| 2 | B7: KPI时间联动不完全 | 🟡 中等 | 累计销量已联动，但品牌/Model/Version 计数仍主要按筛选结果 |
| 3 | B9: 数据标签模式未实际应用 | ✅ 已修复 | applyDataLabelsToTraces 应用到所有图表 |
| 4 | B5: 无单图时间覆盖 | 🟡 中等 | 无法对不同图表设不同时间窗 |
| 5 | B6: 月度子粒度部分未接入 | 🟡 中等 | 总和模式已聚合，分组模式仍保持月粒度 |
| 6 | B1: 粒度判定模型不同 | 🟡 中等 | 自动判定 vs 手动Tab |
| 7 | B8: 热力图渲染差异 | ✅ 已修复 | 已改用Plotly heatmap |
| 8 | B10: 逐系列改色缺失 | ✅ 已修复 | ExportPanel添加逐系列color_picker |
| 9 | B11: "其他"无明细 | ✅ 已修复 | details展开汇总表 |
| 10 | B2: 滑动条交互差异 | ✅ 已修复 | 已改为双端单轨滑块 |
| 11 | B12: 缺少按数据量分级的懒渲染策略 | 🟢 轻微 | React 采用统一按需加载，但不会像 Streamlit 那样按数据量自动切换策略 |
| 12 | B13: URL参数不同步 | ✅ 已修复 | URL读取+replaceState双向同步 |
| 13 | B14: barmode差异 | ✅ 已修复 | 已改为 barmode="relative" |
| 14 | B15: 高级子控件大量缺失 | 📊 长期 | 需逐步补齐 |

---

## React独有功能（Streamlit没有的）

| 功能 | 说明 |
|------|------|
| ✅ 版型气泡图 (Model Version Bubble) | 输入Model名→查看不同版型的 Length×MSRP 散点分布 |
| ✅ OJ定位定价图 (Positioning Map) | KMeans聚类 + 目标车型标记 + 手动竞品指定 + 聚类Top3 |
| ✅ 零销量版型过滤 | 明细表有 "仅显示有销量版型" checkbox |
| ✅ 系列图例 pill 控件 | 时序分组图有可点击的彩色 pill 按钮切换单系列显隐 |
| ✅ 动力总成固定配色 | ICE=灰/MHEV=橙/HEV=黄/PHEV=蓝/BEV=绿，全局统一 |
