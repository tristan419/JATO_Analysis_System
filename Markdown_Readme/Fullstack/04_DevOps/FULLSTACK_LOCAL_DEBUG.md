# Fullstack 本地一键启动与联调

## 1. 一键启动（重启模式）

在项目根目录执行：

```bash
bash 03_Scripts/fullstack_dev.sh start
```

脚本会自动：

1. 先停止已存在的 FastAPI 后端和 Vite 前端
2. 再重新启动 FastAPI 后端和 Vite 前端
3. 自动探测端口冲突并切换空闲端口
4. 输出运行地址与日志路径

如果你只是想查看状态或只做烟测，可以继续使用 `status` / `test` / `stop` / `restart`。

## 2. 常用命令

```bash
# 查看状态
bash 03_Scripts/fullstack_dev.sh status

# 联调烟测
bash 03_Scripts/fullstack_dev.sh test

# 重启
bash 03_Scripts/fullstack_dev.sh restart

# 停止
bash 03_Scripts/fullstack_dev.sh stop
```

## 3. 权限相关（已启用）

默认启用 token + role：

- Header `X-Auth-Token`
- Header `X-User-Role`（viewer/editor/admin）
- Header `X-User-Name`

脚本默认环境变量：

- `APP_AUTH_ENABLED=true`
- `APP_AUTH_TOKEN=change-me`
- `APP_USER_ROLE=admin`
- `APP_USER_NAME=local-dev`

可按需覆盖：

```bash
APP_AUTH_TOKEN=my-token APP_USER_ROLE=viewer bash 03_Scripts/fullstack_dev.sh start
```

## 4. 联调检查清单

1. 健康检查
   - `GET /healthz` 返回 `{ "status": "ok" }`
2. 元数据
   - `GET /v1/metadata/columns` 可返回列名列表
3. 看板概览
   - `POST /v1/analysis/overview` 返回 route + kpis + monthSeries/yearSeries
4. 明细
   - `POST /v1/analysis/detail` 返回 page/pageSize/total/items
5. CRUD
   - `GET /v1/crud/items?page=1&page_size=20` 返回分页结构

## 5. 前端联调建议

1. 打开脚本输出的前端地址
2. 在页面顶部 `Access Control` 面板保存 token/role/name
3. 先跑 Dashboard 的 `Load Overview`
4. 再跑 `Load Detail` 和 `Export Detail CSV`
5. 到 CRUD 页面验证搜索、排序、分页、创建、删除

## 6. 常见问题

1. 端口被占用
   - 脚本会自动切换到下一个空闲端口
2. 401 Unauthorized
   - 检查前端 Access Control 的 token 与后端 `APP_AUTH_TOKEN` 是否一致
3. 403 Forbidden
   - 检查 role 是否满足接口要求（CRUD 写操作需要 editor 及以上）
4. 无法查看日志
   - 后端日志：`06_AppPlatform/.runtime/logs/backend.log`
   - 前端日志：`06_AppPlatform/.runtime/logs/frontend.log`

## 7. Dashboard 首屏慢排查（2026-04-09）

### 7.1 线上测量

- 页面 HTML：`connect 27ms / TTFB 52ms / total 52ms`，说明 nginx 首页返回并不慢。
- `GET /v1/metadata/columns`：`TTFB 71ms`。
- 顶层筛选 `POST /v1/filters/options`：约 `88ms ~ 104ms / 次`。
- `POST /v1/analysis/overview`：`TTFB 688ms`，是首屏 API 中最重的一跳。
- 部署资源头部大小：`plotly-vendor ≈ 1.42MB`、`react-vendor ≈ 189KB`、`router-vendor ≈ 87KB`、`index.js ≈ 88KB`、`css ≈ 36KB`。

### 7.2 根因

- 慢点不在 HTML 首包，而在前端静态资源下载和 Dashboard bootstrap 请求链。
- 原 Dashboard 启动流程会先取 `columns`，再串行取 `country / segment / powertrain` 三个筛选，再请求一次 `overview`。
- 当 URL 自带筛选或默认动力总成触发时，页面还可能再发一次按当前筛选条件的 `overview`，造成首屏重复聚合。
- 旧逻辑不会在首屏把 `make / model / version` 的级联 options 一起补齐，导致首次进入时这些筛选容易出现“空列表/不显示”的错觉，交互后才补齐。

### 7.3 本轮修复

- Dashboard 现在会直接从当前 URL query 生成首屏筛选，不再先打一次空筛选 `overview`。
- `country / segment / powertrain` 三个顶层 options 改为并发请求，缩短首屏串行等待。
- `make / model / version` 级联 options 在 bootstrap 阶段补齐，避免 full timeline 下看起来像“model / version 没出来”。
- Hero 与筛选面板折叠成 rail 后，full timeline 也会显式展示 active lens token，不再依赖拖动时间轴才看到筛选上下文。

### 7.4 后续 TODO

- 验证线上 nginx 是否稳定启用 gzip / brotli，继续压缩 `plotly-vendor` 的首次传输成本。
- 继续拆分 Plotly 相关 chunk，只让 Dashboard 首屏加载必要模块。
- 给 `overview`、`groupedTimeSeries` 等请求加 abort / dedupe，避免快速切换筛选时排队。
- 如果后续确认聚合仍偏慢，再评估后端预聚合或 analytics engine，而不是先引入通用 OLTP 数据库。

## 8. Dashboard 响应式密度修正（2026-04-09）

### 8.1 现象

- 左侧 `全维度筛选` 摘要卡在部分浏览器和缩放比例下会出现 KPI 数字溢出，例如 `136,928` 被卡片边界截断。
- `01 / Market Overview` Hero 面板虽然已有折叠态，但展开态仍然偏高，内部 KPI、chip 和按钮密度过大，收纳后也还不够紧。

### 8.2 根因

- rail、sidebar、Hero 的外层断点是响应式的，但内部关键尺寸仍有一批固定值：`kpi-value`、`hero-meta-value`、`hero-meta-block`、按钮高度、padding、gap。
- 左侧摘要卡虽然是 grid，但内容字号没有跟随位数和断点缩放，导致 6 位以上数字在窄列里挤出边界。
- Hero 的折叠逻辑主要隐藏了 body，没有同步压缩标题、内边距、toggle 位置和内部控件的视觉占用。

### 8.3 可行性判断

- 这是纯前端布局密度问题，改动集中在 `DashboardPage.tsx` 和 `index.css`，不涉及 API、缓存模型或筛选状态机，因此风险可控。
- “展开态减半、折叠态减到四分之一” 可以按视觉占用实现，但不能把所有控件机械除以 2，因为 click target 仍需保留基本可用面积。
- 最稳妥的实现方式是：长度感知字号 + clamp 响应式尺寸 + 折叠态专用密度规则，而不是依赖某一个浏览器的字宽表现。

### 8.4 本轮修复

- 左侧 KPI 摘要卡改为长度感知字号：数字位数越长，字号自动降档，避免跨浏览器截断。
- `全维度筛选` 的 rail 标题、提示、摘要、按钮和 KPI 卡片高度统一缩小，并继续跟随断点收缩。
- `01 / Market Overview` Hero 的展开态整体压缩：标题、KPI 卡、chip rail、按钮、padding 和 gap 全部下调到更紧的比例。
- Hero 折叠态进一步压缩：隐藏 kicker、缩小标题、上移 toggle、减少内边距，使收纳态接近原视觉占用的四分之一。
- 保留 `24px` 左右的 toggle 点击区，兼顾密度和可操作性。

## 9. 移动端导航与 filters/options 延迟治理计划（2026-04-09）

### 9.1 新一轮现象

- 左侧 `全维度筛选` 在桌面折叠态仍保留 rail 文本，部分浏览器会把文字挤成竖排/倒置观感，用户预期是只保留 toggle。
- 顶部 `Overview / Specification / Control` 仍然是固定卡片式导航；在窄屏设备上虽然会换行，但没有真正切换为移动端导航模式。
- 线上最新网络面板显示 `POST /v1/filters/options` 已经成为新的主瓶颈，单次请求可达 `3s / 12s / 16s`，极端值甚至超过 `1min`。

### 9.2 已确认根因

- 顶部导航当前是 `Layout.tsx` 中固定渲染的三张 nav card，CSS 断点只做 `wrap / width` 调整，没有 hamburger/drawer 这种结构级切换。
- 左侧筛选折叠态当前仍渲染 `.filter-sidebar-rail-copy`，桌面窄 rail 只能依赖 `writing-mode / transform` 勉强容纳文字，因此在不同浏览器字形和缩放下不稳定。
- 后端 `filters_options()` 现在每次请求都同时执行 `repo.load_distinct_options()` 和 `repo.count_rows()`；这意味着一次 options 请求至少触发两次 parquet/dataset 扫描。
- 前端实际上没有消费 `filterOptions` 返回的 `rowCount`，但后端仍在为它付出代价。
- Specification 页的 `syncOptionsForColumns()` 会按 `FILTER_ORDER` 串行请求整条级联链；Dashboard boot 虽然把顶层 options 并发了，但 `make / model / version` 仍是顺序补齐，因此慢请求会被放大。

### 9.3 PostgreSQL 是否必要

- 结论：当前不应把 PostgreSQL 当成修复 `filters/options` 慢请求的第一解。
- 现阶段瓶颈是“读取策略 + options 接口设计 + 级联调用链”，不是缺少通用 OLTP 数据库。
- 对于以 parquet 分析和预聚合为主的读密集场景，优先级应是：`distinct 结果缓存`、`常用筛选预计算`、`调用链裁剪`，必要时再考虑 DuckDB/ClickHouse 这一类更偏分析引擎的方案。
- PostgreSQL 仍然有价值，但更适合作为未来 CRUD、权限、审计、任务编排等事务型能力的独立存储，而不是直接承接当前分析事实表。

### 9.4 实施计划

#### A. 窄屏导航与折叠态语义修正

- 桌面端 `全维度筛选` 折叠态改成 toggle-only rail：隐藏标题、摘要和竖排文案，只保留微型展开按钮。
- 移动端顶部导航在 `<=768px` 切换为 hamburger 入口，点击后展开纵向 drawer/list；桌面端继续保留 BMW 风格 card nav。
- 移动端 drawer 中的 nav item 改为整行可点击项，避免三张小卡片挤压在同一行。

#### B. filters/options 第一阶段降载

- 将 `rowCount` 从 `/v1/filters/options` 中改为可选字段，默认不返回，避免每次 options 请求额外执行一次 `count_rows()`。
- 前端为 options 请求补 `abort + request dedupe`，防止快速切筛选时旧请求堆积。
- Dashboard 和 Specification 的 boot/sync 链路改成“顶层 eager、下游按需加载”，不再在首屏一次性串行走完整个级联树。

#### C. filters/options 第二阶段加速

- 在后端增加基于 `column + normalized_filters` 的 TTL/LRU 缓存，优先覆盖 `country / segment / powertrain / make / model / version` 这些高频维度。
- 结合现有 refresh/precompute 流程，为常见前缀筛选生成 options manifest，减少线上临时 distinct 扫描。
- 如果阶段 B 之后仍有明显尾延迟，再评估把 options 索引落到 DuckDB 或其他本地分析引擎，而不是先迁移到 PostgreSQL。

#### D. 验证口径

- UI 验证：至少覆盖 `375 / 390 / 430 / 768 / 1024` 五档宽度，确认顶部导航、sidebar 折叠态和 Hero 不再出现挤压或倒置文本。
- 性能验证：记录 `/v1/filters/options` 的 `p50 / p95 / p99`，同时统计单次进入 Dashboard/Specification 的 options 请求数。
- 回归验证：确认 URL 同步、默认动力总成、级联筛选合法性、Specification 跳转不受影响。

### 9.5 实施顺序建议

1. 先做 A，解决最直观的移动端和折叠态体验错误。
2. 紧接着做 B，把无收益的双扫描和请求堆积先去掉。
3. 若线上尾延迟仍明显，再进入 C，补预计算和更强缓存层。
4. PostgreSQL 仅在 CRUD/权限/审计进入多用户事务阶段时再引入，不与本轮 options 延迟治理绑定。

### 9.6 第一阶段已落地（2026-04-09）

- `Layout.tsx` 已补齐 hamburger + drawer 结构，顶部导航在窄屏下不再依赖三张 nav card 被动换行。
- `全维度筛选` 折叠态已改为 toggle-only rail，`.filter-sidebar-rail-copy` 在收纳态隐藏，不再保留竖排/倒置文本。
- `/v1/filters/options` 已移除前端未消费的 `rowCount` 返回，后端不再为单次 options 请求额外执行一轮 `count_rows()`。
- Dashboard 与 Specification 的筛选链路都已切到“顶层 eager、下游按需加载”，并为 options 请求补上前端 TTL cache 与 abort 控制。
- 当前仍未进入第二阶段：服务端 TTL/LRU 缓存、options manifest 预计算和分析引擎评估继续保留为后续增强项。
