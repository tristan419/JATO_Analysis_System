# Dashboard Layout Baseline · 2026-04-10

这份文档记录 2026-04-10 当前 Dashboard / Specification UI 的真实落点，目的是给后续继续做布局、响应式、视觉统一时一个明确基线，避免再重复回到“现在到底已经改到哪一步”的状态。

## 1. 当前已经落地的状态

### 1.1 Dashboard 顶部与卡片结构

1. 01 Market Overview 继续作为固定的全局上下文卡。
2. 02 到 07 已统一成更接近 01 的 compact hero rhythm。
3. 02 到 07 原先重复展示的 scope / time rail 已全部移除。
4. 当前设计意图是：全局筛选上下文只在 01 持续表达，不再在每张卡片下重复一遍。

### 1.2 边框与视觉节奏

1. 02 到 07 的 hero stat block 已改成轻边框，不再出现黑色重边。
2. 01 外层 card 也已对齐为更轻的边框表达，只保留需要的深色导向线气质。
3. 当前主要问题不再是卡片内部，而是页面级布局：主内容区与 sidebar 的空间分配还需要更明确。

## 2. 当前确认的布局方向

桌面端目标已经明确，不再使用“内容区被一个居中 max-width 容器收住，剩余空间浪费在两边”的旧状态。

### 2.1 桌面端规则

1. 全维度筛选 sidebar 放在最左侧，并作为持续可见的控制栏。
2. 右边主内容区占满剩余宽度。
3. 布局使用 full-width shell，不再人为把 Dashboard / Specification 主体收在一个固定 max-width 的中心盒子里。
4. sidebar 继续 sticky，作为分析时始终可达的控制面板。

### 2.2 移动端规则

1. 小屏回到单列布局。
2. filter sidebar 重新回到主内容上方，保证先筛选再浏览的交互顺序。
3. collapse 行为保留，但不能因为桌面端右侧贴边而破坏手机端可读性。

## 3. 当前代码职责分层

### 3.1 其他工作视图

1. CRUD 与 404 不再保留单独的旧中心壳层。
2. 这两个 page 也应沿用 Dashboard 的 full-width page shell 与 hero 语言。
3. 应用级体验目标是：不同工作视图可以有不同内容，但不能有不同的页面空间语法。

### 3.2 页面结构层

1. DashboardPage 负责 Dashboard 专属分析卡和图表逻辑。
2. SpecificationPage 负责明细表、列选择、导出等 Specification 专属内容。
3. 两个页面共享同一套 sidebar shell 和 hero shell。

### 3.3 共享壳层

1. CollapsibleFilterSidebar 负责筛选栏 rail、toggle、body shell。
2. CollapsibleDeckHero 负责顶部 hero / overview shell。

### 3.4 样式层

1. index.css 内的 dashboard-layout / dashboard-shell 是页面级空间编排的核心入口。
2. 页面是否“铺满”和 sidebar 在左还是右，本质都应该在这一层解决，而不是在单张卡片里打补丁。

## 4. 后续继续做 UI 时的约束

1. 01 保持为固定的全局摘要卡，不要把它已经承载的上下文再复制到 02 到 07。
2. 新增分析卡优先复用 compact hero / analysis deck 语义，不再回退到旧的分裂式头部结构。
3. 页面级空间问题优先改 dashboard-layout / dashboard-shell，不要在 dashboard-main 内部再造二次容器抵消外层布局。
4. 如果以后还要收窄宽度，只能基于内容密度做局部约束，不能把整页重新锁回统一 max-width。

## 5. 这份基线对应的重点文件

1. 06_AppPlatform/frontend/src/pages/DashboardPage.tsx
2. 06_AppPlatform/frontend/src/pages/SpecificationPage.tsx
3. 06_AppPlatform/frontend/src/components/CollapsibleFilterSidebar.tsx
4. 06_AppPlatform/frontend/src/components/CollapsibleDeckHero.tsx
5. 06_AppPlatform/frontend/src/index.css

## 6. 验证方式

继续沿用：

1. npm run check:frontend
2. 本地页面观察 Dashboard 与 Specification 在桌面端和移动端的空间分配
3. 重点确认 sidebar 左侧固定、主内容右侧填充、collapse 后不会留下异常空槽
4. 重点确认 CRUD 首屏不再重复出现 reset / refresh action，Specification KPI 行高度与 Dashboard 的首屏密度更接近
