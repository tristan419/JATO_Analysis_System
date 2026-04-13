# React App vs Streamlit 功能差距分析

> 生成时间：2025-07
> 说明：本文档保留为历史差距快照；当前 Fullstack 对比与现状判断请优先参考 `STREAMLIT_VS_REACT_COMPARISON.md`。
> 对比对象：`06_AppPlatform/frontend` (React+recharts) vs `05_DashBoard/dashboard/views.py` (Streamlit+Plotly)

---

## 一、总览

| 维度 | Streamlit (views.py 7819行) | React (DashboardPage.tsx 903行) | 差距等级 |
|------|---------------------------|-------------------------------|---------|
| 全局时间轴 | ✅ 滑块+日历双模式，范围选择 | ❌ 仅年/月Tab切换 | **严重** |
| 图表缩放/平移 | ✅ Plotly内置工具栏 | ❌ recharts无此能力 | **严重** |
| 导出面板 | ✅ 配色/图例/标签/尺寸/PNG | ❌ 完全缺失 | **严重** |
| RV金融仪表板 | ✅ 8张子图+可编辑表 | ❌ 仅estimated_tco散点图 | **严重** |
| 月度粒度切换 | ✅ 月/季/年切换 | ❌ 仅月粒度 | **中等** |
| 单图时间覆盖 | ✅ 跟随/自定义切换 | ❌ 无概念 | **中等** |
| 高级图表子功能 | ✅ 各图多种子控件 | ⚠️ 仅基础渲染 | **中等** |
| KPI卡片 | ✅ 4指标 | ✅ 4指标 | 无 |
| 筛选侧边栏 | ✅ 搜索+多选+级联 | ✅ 搜索+多选+级联 | 无 |
| 时序图(总和/分组) | ✅ 折线/累积条形 | ✅ 折线/累积条形 | 无 |
| 明细表+CSV导出 | ✅ 有 | ✅ 有 | 轻微 |

---

## 二、严重差距详解

### 2.1 全局时间轴 — 完全缺失

**Streamlit 实现** (views.py L130-458):
- `build_time_axis()`：从数据自动提取可用年/月范围
- `render_time_selector()`：两种模式
  - **滑块拖拽**：`st.select_slider` 双端范围选择，拖动即刻看到数据变化
  - **日历输入**：`st.date_input` 精确选择起止日期
- `render_global_time_controls()`：渲染在页面顶部，全局控制所有图表的时间窗口
- `resolve_chart_time_selection()`：每张图可选择"跟随全局"或"自定义时间范围"

**React 现状**:
- 仅有 `<button>年度对比</button>` / `<button>月度明细</button>` 两个Tab
- 无任何范围选择、滑块、日历组件
- 无法指定 "2022-06 ~ 2024-03" 这样的时间窗口

**影响**：用户无法聚焦分析特定时间段，这是分析类仪表板的核心能力。

---

### 2.2 图表缩放/平移/工具栏 — 库级限制

**Streamlit 实现**:
- 使用 Plotly.js，配合 `PLOT_CONFIG = {"displaylogo": False, "responsive": True}`
- 每张图自动附带工具栏：📊 Zoom、Pan、Box Select、Lasso Select、Autoscale、Reset Axes
- 鼠标滚轮缩放、拖拽平移、框选放大

**React 现状**:
- 使用 `recharts` 库 — **设计上不支持缩放/平移**
- recharts 是声明式SVG图表库，适合静态展示，不适合交互式数据探索
- 没有内置工具栏，没有框选放大

**影响**：对于动辄上百个数据点的散点图/气泡图，无法缩放平移就无法做有效分析。

---

### 2.3 图表导出/样式面板 — 完全缺失

**Streamlit 实现** (views.py L780-1560 `render_export_style_controls()`):

| 功能 | 说明 |
|------|------|
| 网格/轴线开关 | 独立控制X/Y网格线和轴线 |
| 图例位置 | 6个选项：右上/右下/左上/左下/上方居中/隐藏 |
| 配色方案 | 6种：Plotly/D3/G10/Pastel1/Set3/Dark2 |
| 轴刻度格式 | 6种：原始/.2s/千分位/.0%/,.0f/.2f |
| 背景色 | 绘图区和画布区分别可选色 |
| 标题 | 图表标题 + X轴标题 + Y轴标题 |
| 导出尺寸 | 宽度 400-2400px，高度 300-1600px |
| 数据标签 | 7种模式 × 5种位置 |
| 每系列颜色 | 手动为每条线/柱指定颜色 |
| PNG导出 | kaleido引擎导出高清PNG |

**React 现状**: 无任何导出或样式控制。

**影响**：分析师无法制作可用于报告/汇报的自定义图表。

---

### 2.4 RV金融仪表板 — 最复杂功能完全缺失

**Streamlit 实现** (views.py L4500-5800，约1300行):

| 子功能 | 说明 |
|--------|------|
| 货币转换 | 支持多货币动态转换 |
| 参数模板 | 预定义金融参数集 |
| 可编辑表格 | `st.data_editor` 直接编辑金融参数 |
| 月供对比 | 贷款月供计算与对比 |
| 方案A/B/C | 三种金融方案并列比较 |
| 瀑布图 | `go.Waterfall` 展示成本分解 |
| PV-RV关系图 | 线图展示购置价vs残值关系 |
| 敏感性分析 | 4个滑块控制 + 龙卷风图 + 等高线图 |

**React 现状**: 仅 `estimated_tco` 散点图 (1张图，约60行代码)。

**影响**：8张子图+可编辑数据表+敏感性分析完全缺失，这是最高附加值的分析模块。

---

## 三、中等差距详解

### 3.1 月度粒度切换

**Streamlit**: 月度Tab内有 月/季/年 粒度切换器，允许按季度或年度聚合月度数据
**React**: 仅按月展示，无季度/年度聚合选项

### 3.2 单图时间覆盖

**Streamlit**: 每张高级分析图可选"跟随全局时间轴"或"自定义时间范围"
**React**: 无此概念，所有图使用相同筛选条件

### 3.3 各高级图表子功能缺失

| 图表 | Streamlit拥有 | React缺失 |
|------|-------------|-----------|
| 动力气泡图 | 品牌分面、YoY叠加、气泡倍率滑块 | 仅基础散点 |
| 续航分布 | BEV/PHEV筛选、净变化模式、增长KPI、结构分解表 | 仅基础堆叠条形 |
| 价格迁移 | 折线/面积切换、按动力总成分面 | 仅面积图 |
| 季节性热力图 | px.imshow + colorscale + hover | HTML表格+背景色(无hover) |
| 车长vs价格 | C-SUV/D-SUV参考线、"价值检测" | 仅基础散点 |
| 销量vs价格 | 细分市场份额气泡大小 | 部分实现(ZAxis=SegmentSharePct) |

### 3.4 明细表

**Streamlit**: 拖拽排序列、行数滑块、可展开行
**React**: 列芯片点选(无拖拽)、分页 — 功能基本可用但体验不如Streamlit

## 四、已落地项（2026-04-07）

- 全局时间轴已经接入 [TimeAxis.tsx](../06_AppPlatform/frontend/src/components/TimeAxis.tsx)，并挂到 [DashboardPage.tsx](../06_AppPlatform/frontend/src/pages/DashboardPage.tsx) 顶部，支持时间范围过滤。
- Dashboard 主图已迁移到 Plotly.js，现有时序图、气泡图、堆叠图、面积图、定位图都具备原生 zoom/pan/box select/toolbar。
- 导出面板已经接入 [ExportPanel.tsx](../06_AppPlatform/frontend/src/components/ExportPanel.tsx)，支持网格、图例、配色、标题、尺寸和 PNG 导出。
- RV 金融仪表板已经接入 [RvFinanceDashboard.tsx](../06_AppPlatform/frontend/src/components/RvFinanceDashboard.tsx)，后端路由位于 [analysis.py](../06_AppPlatform/backend/app/api/routes/analysis.py)。
- 这意味着本轮最核心的 4 个差距已经关闭，后续剩余项主要是高级图表的细节补强，而不是基础能力缺失。

## 五、在 React App 中嵌入/跳转 Streamlit 可行性分析

### 方案 A：iframe 嵌入

```
React App
┌──────────────────────────────┐
│  Header / Sidebar (React)    │
│  ┌────────────────────────┐  │
│  │  <iframe src="http://  │  │
│  │   host:8501/?embed=true│  │
│  │   &chart=rv_finance">  │  │
│  │                        │  │
│  │   Streamlit 图表区域    │  │
│  └────────────────────────┘  │
└──────────────────────────────┘
```

| 维度 | 评估 |
|------|------|
| 开发成本 | **低** — Streamlit原生支持 `?embed=true` 隐藏chrome |
| 功能完整性 | **高** — 100%保留Plotly交互 |
| 筛选同步 | **差** — iframe内Streamlit有独立session_state，无法与React共享筛选条件。需通过URL参数传递筛选（有限）或postMessage（复杂） |
| 认证 | **复杂** — 需要统一认证，否则iframe内需要独立登录 |
| 性能 | **差** — 双份前端资源，Streamlit本身内存占用高（≥200MB/session） |
| 部署复杂度 | **高** — 需同时运行FastAPI + React + Streamlit三个服务 |
| 交叉引用 | **差** — 从React图表跳到Streamlit图表，上下文断裂 |

**结论**：技术可行但体验割裂，不推荐作为长期方案。

### 方案 B：新Tab跳转

React页面放一个按钮 "🔗 在 Streamlit 中查看完整分析"，点击后打开 `http://host:8501/`。

| 维度 | 评估 |
|------|------|
| 开发成本 | **极低** — 一行`<a>`标签 |
| 功能完整性 | **高** — 完整Streamlit |
| 用户体验 | **差** — 两个独立应用，筛选不同步，界面风格不统一 |
| 部署 | **中** — 需同时部署两个应用 |

**结论**：适合临时过渡方案，不适合正式产品。

### 方案 C：React 中用 Plotly.js 替换 recharts（推荐）

```bash
npm install react-plotly.js plotly.js
```

| 维度 | 评估 |
|------|------|
| 开发成本 | **中高** — 需重写所有图表组件（约500行改动） |
| 功能完整性 | **高** — 与Streamlit使用**同一引擎**，zoom/pan/toolbar全部自动获得 |
| 筛选同步 | **完美** — 同一React应用，共享state |
| 性能 | **好** — 单一前端，plotly.js体积较大(~3MB gzip后~800KB)但可lazy load |
| 部署 | **简单** — 仍是单一前端+单一后端 |
| 可维护性 | **好** — 一套代码，一个技术栈 |

**具体实施路径**：
1. 安装 `react-plotly.js` + `plotly.js`
2. 用 `<Plot>` 组件逐步替换 recharts 的 `<LineChart>`, `<BarChart>`, `<ScatterChart>`, `<AreaChart>`
3. 所有图表自动获得 zoom/pan/hover/toolbar
4. 逐步添加导出面板（Plotly.js 原生支持 `Plotly.downloadImage()`）
5. 添加全局时间轴组件（React slider组件 + date picker）
6. 实现RV金融仪表板（新增路由/组件，使用Plotly的Waterfall和Contour）

### 方案 D：混合方案 — React为主 + Streamlit处理RV金融

对于RV金融仪表板（1300行，8张子图+可编辑表+敏感性分析），在React中从零实现成本极高。可以：
- React App主体切换为Plotly.js（方案C）
- RV金融仪表板通过iframe嵌入Streamlit专用页面
- 其余图表全部在React中用Plotly实现

| 维度 | 评估 |
|------|------|
| 开发成本 | **中** — 图表切Plotly + RV Finance保留iframe |
| ROI | **最高** — 80%的功能差距在方案C中解决，剩余20%最复杂部分用iframe |

---

## 五、推荐实施优先级

| 优先级 | 任务 | 预期效果 | 依赖 |
|--------|------|---------|------|
| P0 | recharts → react-plotly.js 迁移 | 所有图表获得zoom/pan/toolbar | npm install |
| P1 | 全局时间轴组件 | 解决"时间轴不能修改"问题 | Plotly迁移后更自然 |
| P2 | 导出面板(配色/标签/PNG) | 解决报告制作需求 | Plotly迁移后自动获得downloadImage |
| P3 | 月度粒度切换(月/季/年) | 完善时间分析维度 | 后端已支持 |
| P4 | 各高级图表子功能补全 | 气泡倍率/分面/参考线等 | 每张图约30-80行 |
| P5 | RV金融仪表板 | 最高附加值模块 | 建议混合方案或分期 |

---

## 六、结论

**核心问题**：React App使用的 `recharts` 库天然不支持交互式数据探索（zoom/pan），这是架构层面的限制而非代码层面的遗漏。

**推荐路径**：方案C（Plotly.js迁移）+ 方案D（RV Finance用iframe过渡），预计可覆盖90%+的功能差距。
