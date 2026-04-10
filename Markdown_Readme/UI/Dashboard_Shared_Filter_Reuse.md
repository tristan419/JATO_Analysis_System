# Dashboard / Specification 复用抽象拆解

这份笔记只讲这次前端改造里真正值得复用的部分，不讲表面样式怎么改。核心思路是把原来混在单页里的东西拆成三层：共享状态层、共享壳层、共享视觉语义层。这样后面你继续扩展新分析页时，不需要再复制一整页代码。

## 1. 第一层：共享筛选状态层

对应文件：

- 06_AppPlatform/frontend/src/contexts/SharedFilterScopeContext.tsx
- 06_AppPlatform/frontend/src/dashboardFilters.ts

这层解决的是 Dashboard 和 Specification 之间最难复用的问题：筛选状态、URL query、筛选项联动、overview 结果、折叠状态，都不应该由两个页面各自维护一份。

### 这一层负责什么

1. 维护统一的筛选选择：country、segment、powertrain、make、model、version。
2. 维护筛选项 optionsMap，并按依赖顺序同步下游选项。
3. 维护共享 overview、yearSeries、monthSeries、filteredRowCount。
4. 把筛选同步到 URL，这样分享链接和前进后退都能复用同一份状态。
5. 给 Dashboard 和 Specification 同时暴露 hero/sidebar 的折叠状态。

### 为什么这层必须独立

如果 Dashboard 自己维护筛选，Specification 再维护一份，马上会出现三个问题：

1. 切页时 query 能保留，但内存里的选项链和 overview 会重跑。
2. 一个页面修了筛选联动，另一个页面很容易忘记同步。
3. 分享链接只能保证“选中了什么”，不能保证“页面已经进入可交互状态”。

把这些都收进 provider 后，页面本身就只关心“用这份状态渲染什么”。

### 这层里的几个关键函数

`createSharedSelections`

- 作用：无论缓存、query、初始化来源是什么，最后都归一成固定六个键的对象。
- 价值：页面可以放心写 `selections.model.length` 这种逻辑，不需要每次判空整个对象结构。

`resolveFilterColumns`

- 作用：把真实列名解析成前端内部固定 filter key。
- 价值：后端列名可以有中英文差异，但页面逻辑始终只认统一 key。

`buildFilterPayloadFromResolved`

- 作用：把内部筛选状态重新投影回后端接口需要的 payload。
- 价值：页面无需知道接口字段名，避免业务逻辑散落在多个页面里。

### 这层的设计原则

1. 页面只拿结果，不重复实现筛选同步过程。
2. 页面只操作 filter key，不直接操作原始列名。
3. query 是共享状态的外部表现，不是页面私有状态。

## 2. 第二层：共享页面壳层

对应文件：

- 06_AppPlatform/frontend/src/components/CollapsibleFilterSidebar.tsx
- 06_AppPlatform/frontend/src/components/CollapsibleDeckHero.tsx

这层解决的是“两个页面结构很像，但内容不同”的问题。

Dashboard 和 Specification 现在都用了同一种骨架：

1. 左侧是可折叠 filter stack。
2. 顶部是可折叠 hero / headline 区。
3. 主内容区只放各自特有的分析内容。

### 为什么这层单独抽

如果你只复用筛选 state，不复用页面骨架，那么每个页面还是会各自保留一套：

1. sidebar 展开/收起按钮
2. hero 头部结构
3. 标题、副标题、状态 chip、CTA 排版

这种重复的代价不是代码行数，而是后面 UI 一改就要改两遍，而且很容易改出不一致。

### 这层的边界

这个壳层只管容器，不管业务：

1. 不决定图表怎么画。
2. 不决定明细表怎么查。
3. 不决定按钮点了以后调哪个接口。

它只保证两个页面的空间结构、折叠行为和头部语义一致。

## 3. 第三层：共享视觉语义层

对应文件：

- 06_AppPlatform/frontend/src/index.css

这层是这次把 RV Finance、Model Version Bubble、OJ Positioning Map 拉到同一套 deck 语言时新增的抽象。重点不是“配色统一”，而是把卡片内部的结构语义做成可复用原语。

### 这次沉淀出的原语

`analysis-deck-card`

- 整块分析卡片容器。

`analysis-deck-head`

- 卡片头部，统一 kicker、标题、摘要、meta 统计。

`analysis-deck-meta` / `analysis-deck-stat`

- 卡片右侧的小状态块，比如 data state、target ready、quick picks 数量。

`analysis-chip-row` / `analysis-chip` / `analysis-chip-button`

- 小颗粒状态、筛选摘要、quick pick 行为都收进同一类语义，不再每张卡片各写一套 pill。

`analysis-subsection` / `analysis-chart-block`

- 一张卡片内部的分段标题和图表包裹层。

`analysis-inline-note`

- 提示信息统一样式，不再混用 alert、说明段落、临时边框。

`analysis-disclosure` / `analysis-table-wrap`

- 表格、折叠说明、敏感性分析明细统一容器。

### 为什么这样比“复制 BMW 样式”更重要

BMW.md 给的是视觉方向，不是代码结构。真正能复用的是这些语义类名，因为它们让你新增一张分析卡时有稳定骨架：

1. 先放 header。
2. 再放 meta state。
3. 再放 controls panel。
4. 再放 chart block。
5. 最后按需放 disclosure/table。

以后如果你要加新的单卡分析，不需要从零拼结构，只要把业务控件塞进这几个槽位里。

## 4. 页面层现在应该怎么写

### DashboardPage 现在负责什么

DashboardPage 应该只负责：

1. 从 shared filter scope 里取共享状态。
2. 维护当前页独有的分析状态，例如 advanced chart、model bubble、positioning map。
3. 通过 deck 原语把不同分析段组织出来。

它不应该再负责：

1. 自己重新解析 query。
2. 自己重新实现筛选项联动。
3. 自己定义一套和其他页面不同的壳层结构。

### SpecificationPage 现在负责什么

SpecificationPage 应该只负责：

1. 复用 shared filter scope。
2. 管理 detail page、selected columns、CSV 导出、分页。
3. 用同一个壳层承接“明细页”的专属职责。

也就是说，Dashboard 是“图表页”，Specification 是“明细页”，但它们共享的是筛选主线和页面骨架。

## 5. 这次真实暴露出来的两个工程坑

### 坑 1：React dev strict mode 会把初始化 effect 跑两次

这次 provider 里最初的 boot 流程有一个典型问题：

1. 第一次 effect 开始拉取 metadata / options。
2. strict mode 立即 cleanup，前一次请求被 abort。
3. 代码却已经把 `bootDone` 标成 true。
4. 第二次 effect 因为看到 `bootDone === true`，直接跳过。

结果就是首屏只发出一部分请求，然后整个共享筛选链卡死。

修复方式不是“关掉 strict mode”，而是让 boot 状态区分：

1. 已开始
2. 已完成
3. 被中断但尚未完成

只有真正完成后，才允许后续 effect 视为 boot done。

### 坑 2：页面 state 不能盲信接口一定返回数组

这次 mock 回归里，advanced-chart 一开始返回的是空对象 `{}`。如果页面直接 `setAdvItems(r.items)`，那么 state 会变成 `undefined`，随后任何 `advItems.length` 都会直接把整页打崩。

因此页面层需要做最小兜底：

1. 数组字段用 `ensureArray`。
2. 可空对象用 `?? null`。
3. 不把接口契约错误直接传播成 React state 结构错误。

这不是为了掩盖后端问题，而是为了保证页面在回归、弱网、异常响应下还能保住骨架。

## 6. 为什么要补一份 mock 浏览器回归

对应文件：

- 06_AppPlatform/frontend/scripts/dashboard_spec_mock_regression.cjs

类型检查和 build 只能证明“代码能编译”，不能证明“路由来回切换时状态真的对”。

这份脚本的价值在于它直接验证了六件事：

1. Dashboard 跳到 Specification 时 query 保留。
2. 直接刷新 Specification 时，query 和明细表都还能正确 hydrate。
3. 浏览器 back 返回 Dashboard 时 query 仍保留。
4. Dashboard 进入 CRUD 页时，入口链路和列表请求都正常。
5. 直接打开 Specification 分享链接时，筛选能正确 hydrate。
6. 404 路由会落到统一的 deck fallback，而不是白页或裸文本。

另外，这份脚本还顺手守住一个共享层约束：Dashboard、Specification、CRUD 这些工作视图之间做前端路由切换时，共享筛选 boot 不应该被重复触发。

注意这里特意没有把“初次加载一定只请求一次 metadata”写成硬规则，因为 dev 模式下 strict mode 允许初始化双跑。真正需要守住的是：初次加载之后，路由切换不能再额外重跑共享 boot。

## 7. 以后继续扩展时，建议按这个顺序想

如果你还要继续加新页面或新分析卡，建议先问自己三个问题：

1. 这是共享状态，还是页面私有状态？
2. 这是共享壳层，还是业务内容？
3. 这是新的视觉原语，还是已有 deck 原语的组合？

只有第三个问题回答成“真的是新的原语”，才去加新的 CSS 结构类。否则优先复用已有 `analysis-*` 语义。

## 8. 一句话总结

这次复用的本质不是“把 Dashboard 拆成多个文件”，而是把原来耦合在页面里的三种职责拆开：

1. SharedFilterScopeProvider 负责共享状态。
2. Collapsible 组件负责共享壳层。
3. analysis deck CSS 原语负责共享视觉语义。

这样 Dashboard、Specification 以及后面的独立分析卡，才真正进入“同一套系统”，而不是看起来像同一套系统。

## 9. 固定检查现在怎么跑

对应文件：

- 06_AppPlatform/frontend/package.json
- 06_AppPlatform/frontend/scripts/dashboard_spec_mock_regression.cjs
- 06_AppPlatform/frontend/scripts/run_dashboard_spec_regression.cjs
- .github/workflows/ci.yml

这轮我把路由回归从“手工跑一次”收口成了固定检查。

### package.json 脚本

`npm run check:types`

- 只跑 TypeScript 类型检查。

`npm run check:router-regression`

- 启动本地 preview 服务。
- 跑 Dashboard / Specification 的 mock 浏览器回归。
- 回归结束后自动关闭 preview。

`npm run check:frontend`

- 先类型检查。
- 再生产构建。
- 再跑路由回归。

也就是说，这条命令现在就是前端固定检查入口。

### CI 里怎么接

CI 现在不再只做 `tsc + build`，而是：

1. `npm ci`
2. `npx playwright install --with-deps chromium`
3. `npm run check:frontend`

这样本地和 CI 跑的是同一条主线，而不是两套不同逻辑。

## 10. 页面要用 React Router 吗

对应文件：

- 06_AppPlatform/frontend/src/App.tsx

结论是：当前这个页面体系应该继续用 React Router，而且已经在用。

### 原因不是“因为 React 项目都爱用路由”

而是因为你这里已经满足了路由真正值得存在的三个条件：

1. 有多个工作视图：Dashboard、Specification、CRUD。
2. 这些视图之间需要共享筛选 query 和前进后退语义。
3. 明细页需要单独分享链接，而不是只做一个页内弹层。

现在 [06_AppPlatform/frontend/src/App.tsx](06_AppPlatform/frontend/src/App.tsx) 用的是 `createBrowserRouter`，主结构是：

1. `/` -> DashboardPage
2. `/specification` -> SpecificationPage
3. `/crud` -> CrudPage

这个方案和 SharedFilterScopeProvider 是匹配的，因为共享筛选不是做在某个单页组件内部，而是挂在路由壳层上。

### 什么情况下可以不用 React Router

如果以后只剩一个单页、没有独立分享链接、没有 Dashboard / Specification 的切页关系，那可以不用。

但按现在这套结构，不用 React Router 反而会把：

1. query 同步
2. 浏览器回退
3. 分享链接
4. 代码分割

重新塞回组件状态里，复杂度会更高。

## 11. 这次用了什么技术

### 页面与状态层

1. React 19：页面组件、Suspense、lazy、共享状态消费。
2. TypeScript：页面状态、接口响应、图表数据结构约束。
3. react-router-dom：Dashboard / Specification / CRUD 路由与分享链接。

### 构建与运行层

1. Vite：本地开发、生产构建、preview 服务。
2. npm scripts：把类型检查、构建、路由回归收成固定入口。

### 可视化与交互层

1. Plotly：主图表渲染。
2. analysis deck CSS 原语：卡片头部、meta、chip、subsection、chart block 的统一语义。

### 回归与自动化层

1. Playwright：mock 浏览器回归。
2. GitHub Actions：CI 固定检查。

## 12. 现在已经做了什么

1. Dashboard 和 Specification 用 React Router 正式承接跨页行为。
2. SharedFilterScopeProvider 统一了 query、筛选项联动和 overview 主线。
3. Dashboard 剩余的时间轴卡、时间序列卡、高级分析主卡，以及 Specification 的明细预览卡，都被收进同一套 analysis deck 语义。
4. 404 页面也补成了 deck 风格的 fallback，和主工作视图保持同一套壳层语言。
5. 路由回归脚本变成了固定检查，不再依赖手工起服务和手工执行顺序。
6. 固定回归现在已经覆盖 Dashboard / Specification / CRUD / 404，以及直接刷新 Specification 的链路。

## 13. 接下来要做什么

我建议后面继续沿这条线做，不要再回到“每张卡单独起样式”的方式。

### 近一步

1. 把高级分析里还存在的零散内联样式和 `details/table` 块，继续替换成 `analysis-disclosure / analysis-table-wrap / analysis-subsection`。
2. 如果后面 CRUD 页也要进入同一套分析壳层，可以评估把它的局部信息卡继续向 `analysis-deck-*` 语义靠拢。

### 工程一步

1. 继续补更深一层的跨页行为回归，例如 CRUD 创建/删除后的列表回流，以及 query 边界值场景。
2. 处理 Plotly vendor 包过大的构建告警，考虑做更细的 chunk 拆分。

### 维护原则

以后新增页面或卡片时，先判断：

1. 这是 React Router 下的新工作视图，还是一个页内分析块。
2. 这是 SharedFilterScope 的共享状态，还是页面私有状态。
3. 这是新的 deck 原语，还是已有原语的组合。

只要这三个边界守住，后面扩展就不会再乱掉。