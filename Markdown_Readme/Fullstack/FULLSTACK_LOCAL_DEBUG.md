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
