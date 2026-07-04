# UI 规范文档（初版 · 2026-04-11）

> 来源：`Dashboard_Layout_Baseline_2026-04-10.md` + `Dashboard_Shared_Filter_Reuse.md` + `BMW.md`
> 目的：把三份 UI 专题文档提炼为一份可执行的视觉和布局规范，后续所有 UI 改动以此为约束。

---

## 1. 设计语言

### 1.1 风格基调

采用 BMW CI2020 启发的工业精密感：

- **零圆角**：所有容器、按钮、输入框使用 `border-radius: 0`
- **深浅交替**：深色 hero 区 + 白色内容区的交替节奏
- **轻边框**：卡片使用轻边框而非黑色重边
- **BMW Blue**（`#1c69d4`）：仅用于交互元素和焦点态，不做装饰色

### 1.2 字体

| 角色 | 字号 | 字重 | 行高 | 备注 |
|------|------|------|------|------|
| Display Hero | 3.75rem (60px) | 300 | 1.30 | `text-transform: uppercase` |
| Section Heading | 2rem (32px) | 400 | 1.30 | 主要分区标题 |
| Body | 1rem (16px) | 400 | 1.15 | 正文 |
| Button Primary | 1rem (16px) | 700 | 1.20 | CTA 按钮 |
| Button Secondary | 1rem (16px) | 400 | 1.15 | 次级按钮 |
| Nav Emphasis | 1.13rem (18px) | 900 | 1.30 | 导航 |

字体栈：`BMWTypeNextLatin, Helvetica, Arial, "Hiragino Kaku Gothic ProN", "Hiragino Sans", Meiryo, sans-serif`

中文内容回退：`"PingFang SC", "Microsoft YaHei", sans-serif`

### 1.3 色彩

| 角色 | 色值 | CSS 变量 |
|------|------|---------|
| 主背景 | `#ffffff` | `--site-context-theme-color` |
| 交互强调 | `#1c69d4` | `--site-context-highlight-color` |
| 焦点蓝 | `#0653b6` | `--site-context-focus-color` |
| 主文本 | `#262626` | — |
| 次级文本 | `#757575` | `--site-context-metainfo-color` |
| 三级文本 | `#bbbbbb` | — |

### 1.4 间距

基础单元：`8px`

常用尺度：8 · 16 · 24 · 32 · 40 · 56 · 60px

---

## 2. 页面布局

### 2.1 桌面端（≥ 1024px）

```
┌──────────────────────────────────────────────────────────┐
│  Top Nav (sticky)                                        │
├──────────┬───────────────────────────────────────────────┤
│ Filter   │  Main Content (fill remaining width)          │
│ Sidebar  │  ┌─────────────────────────────────────────┐  │
│ (sticky) │  │  CollapsibleDeckHero                    │  │
│          │  ├─────────────────────────────────────────┤  │
│          │  │  Analysis Cards / Tables / Charts       │  │
│          │  └─────────────────────────────────────────┘  │
└──────────┴───────────────────────────────────────────────┘
```

- Filter sidebar 在最左侧，持续可见（`position: sticky`）
- 主内容区占满剩余宽度，不使用固定 `max-width` 居中容器
- Hero 区用 `CollapsibleDeckHero` 统一壳层

### 2.2 平板端（iPad · 768px–1023px）

- sidebar 可折叠为 toggle-only rail（不显示竖排文字）
- 主内容区仍占满宽度
- Hero stat block 从 4 列压缩到 2×2 网格

### 2.3 手机端（iPhone · < 768px）

- 单列布局
- Filter sidebar 回到主内容上方，先筛选再浏览
- Hero 区 stat blocks 堆叠为单列
- 导航切换为 hamburger / drawer
- 触摸目标最小 44×44px

---

## 3. 响应式断点定义

| 名称 | 断点 | 目标设备 |
|------|------|---------|
| `desktop` | ≥ 1024px | 笔记本 / 外接显示器 |
| `tablet` | 768px – 1023px | iPad / iPad Air |
| `mobile` | < 768px | iPhone / Android 手机 |

```css
/* 推荐写法 */
@media (max-width: 1023px) { /* tablet + mobile */ }
@media (max-width: 767px)  { /* mobile only */ }
```

---

## 4. 共享组件规范

### 4.1 CollapsibleDeckHero

- 用途：页面顶部 hero / overview 区
- 必须提供：`head`（标题 + stat blocks）、`body`（展开后的详细信息）
- 折叠行为：桌面默认展开，移动端默认折叠

### 4.2 CollapsibleFilterSidebar

- 用途：左侧全维度筛选栏
- 桌面端 sticky，移动端 stacked above content
- 折叠后只显示 toggle 按钮，不保留竖排文字

### 4.3 Analysis Deck Card

- 用途：分析卡片（图表、表格等）
- 类名：`analysis-deck-card`
- 内部结构：`card-title` → `card-body`（内容区） → `card-footer`（可选操作区）
- 不在卡片内重复全局筛选上下文（由 01 Market Overview 统一表达）

### 4.4 CRUD Shell

- 用途：Review Cases / Engineering / Admin 等列表管理页
- 沿用 Dashboard 的 full-width page shell 和 hero 语言
- 不使用单独的居中窄壳层

---

## 5. 页面间一致性规则

1. 全局筛选上下文只在 01 Market Overview 持续表达，02–07 不重复
2. 新增分析卡复用 compact hero / analysis deck 语义
3. 页面级空间问题优先改 `dashboard-layout` / `dashboard-shell`，不在 `dashboard-main` 内造二次容器
4. 如需收窄宽度，只能基于内容密度做局部约束

### 5.1 加载态与通知位置

- **页面级加载态必须居中**：任何会阻塞整页内容读取的加载动画，必须放在当前 viewport 的视觉中心，使用统一的居中 loading shell；不能嵌在 hero、卡片、表格、图表区域左上角或局部内容流中。
- **局部刷新不打断布局**：不会阻塞整页阅读的局部刷新，只能使用轻量状态标记或按钮 loading，不得让主要内容跳动；如需展示进度，也应在对应控件附近保持固定尺寸。
- **通知统一顶端 banner**：错误、成功、警告、提示类通知统一进入页面顶部 banner 区域，位置在全局导航下方、页面内容上方；不要在卡片内部、表格内部、浮层底部或按钮旁边散落 `alert`/notice。
- **通知语义统一**：通知样式按 `error`、`warning`、`info`、`success` 四类处理，文案必须说明结果和可操作下一步；长错误详情进入展开区或日志，不直接撑开页面主体。
- **版型对比优先适配**：`VersionComparisonPage` 的首次 deck 加载必须使用屏幕中心 loading；Add Model、筛选、导出等局部动作只用局部按钮/控件状态。

---

## 6. 共享状态层

- `SharedFilterScopeContext`：统一管理筛选选择、URL query 同步、overview 数据
- 页面只拿结果，不重复实现筛选同步
- 页面只操作 filter key，不直接操作原始列名

---

## 7. 核心样式文件

| 文件 | 职责 |
|------|------|
| `index.css` | 页面级空间编排（`dashboard-layout` / `dashboard-shell`） |
| `CollapsibleFilterSidebar.tsx` | 筛选栏 rail、toggle、body shell |
| `CollapsibleDeckHero.tsx` | 顶部 hero / overview shell |
| `DashboardPage.tsx` | Dashboard 专属分析卡与图表逻辑 |
| `SpecificationPage.tsx` | 明细表、列选择、导出逻辑 |

---

## 8. 可搜索下拉选择器 (Searchable Dropdown)

> 首次落地：`VersionComparisonPage.tsx` — Country / Segment / Add Model 三个筛选器  
> 复用范围：任何需要从长列表中单选或多选选项的筛选器

### 8.1 结构

```
┌──────────────────────────────────────┐
│  Label                    (已选 N)   │
│  ┌──────────────────────────────────┐│
│  │ 搜索品牌或车型名称...            ││
│  └──────────────────────────────────┘│
│  ┌──────────────────────────────────┐│
│  │ [全选可见] [取消可见] [清空全部] ││  ← 批量操作行 (仅多选模式)
│  │ 匹配 X 项 · 已选 Y/10           ││
│  ├──────────────────────────────────┤│
│  │ ☑ Brand Model                   ││  ← 选项行: checkbox + 名称
│  │   Segment | Powertrain | Len mm  ││  ← 可选元数据行
│  │ ☐ Brand Model                   ││
│  │ ☐ Brand Model                   ││
│  └──────────────────────────────────┘│
│  仅显示当前 Segment 内车型           │  ← hint 文本
└──────────────────────────────────────┘
```

### 8.2 CSS 类名约定

| 类名 | 用途 |
|------|------|
| `version-comparison-model-picker-field` | 下拉容器 wrapper |
| `version-comparison-model-picker` | `position: relative` 锚点 |
| `version-comparison-model-search` | 搜索输入框 |
| `version-comparison-model-dropdown` | 下拉面板 (`position: absolute`, `z-index: 50`) |
| `version-comparison-model-dropdown-actions` | 批量操作行 (`position: sticky; top: 0`) |
| `version-comparison-batch-btn` | 批量操作按钮 |
| `version-comparison-model-option` | 选项行 |
| `version-comparison-model-checkbox` | 复选框 (18×18px, BMW Blue 选中态) |
| `version-comparison-model-option-name` | 选项主名称 |
| `version-comparison-model-option-meta` | 选项元数据标签行 |
| `version-comparison-model-empty` | 空结果提示 |
| `version-comparison-dropdown-count` | 匹配/已选计数 |

### 8.3 行为约定

- **单选** (如 Country)：点击选项后关闭下拉框，checkbox 为 radio 风格
- **多选** (如 Segment, Add Model)：点击选项切换选中状态，下拉框保持打开
- **搜索**：客户端即时过滤，匹配 `label`、`value`、及元数据字段
- **外部点击关闭**：`useEffect` + `mousedown` 事件监听
- **键盘导航**：保留箭头键切换 + Enter 选择 (由 `useArrowCountryNavigation` 提供范式)

### 8.4 数字范围行 (Range Row)

用于将两个数字输入合并为一条视觉范围（如 Length Range）：

```
Length Range (mm):  [4300] — [5000]
```

CSS 类名：`version-comparison-range-row` / `version-comparison-range-input` / `version-comparison-range-sep`

---

## 9. 图表工具栏 (Chart Toolbar)

> 首次落地：`VersionComparisonPage.tsx` — 版型气泡图 Label Mode 选择器
> 复用范围：任何需要在图表区内嵌轻量控制工具栏的场景

### 9.1 结构

```
┌──────────────────────────────────────────────────────┐
│  Panel Head                                          │
│  ┌──────────────────────────────────────────────┐    │
│  │                    [Smart Top] [Clean] [···]  │    │  ← Label Mode 选择器
│  └──────────────────────────────────────────────┘    │
│                                                      │
│  ┌──────────────────────────────────────────────┐    │
│  │                                              │    │
│  │              Chart Area                      │    │
│  │                                              │    │
│  └──────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────┘
```

- 定位：图表区域顶部、图表上方，右对齐
- 主控件：`btn-group--sm` 尺寸的按钮组，当前选项高亮为 `btn-primary`，其余为 `btn-ghost`
- 辅助控件（可选）：如"清除已选"按钮，在右侧显示已选计数
- 工具栏与图表之间间距：4px

### 9.2 CSS 类名约定

| 类名 | 用途 |
|------|------|
| `version-comparison-label-toolbar` | 工具栏容器 (`display: flex; justify-content: flex-end; gap: 8px`) |

### 9.3 行为约定

- **状态同步**：选中值存入 URL 参数（如 `?labelMode=smart_top`），支持浏览器前进/后退
- **默认值**：首次访问时默认 `smart_top`，不写入 URL（与默认值一致时省略参数）
- **切换代价**：按钮点击即切换，`useMemo` 只重建 traces 数据，不触发网络请求

---

## 10. 验证方式

1. `npm run check:frontend` — 类型 + 测试 + 构建 + 路由回归
2. 桌面端验证：sidebar 左侧固定、主内容右侧填充、collapse 后无异常空槽
3. iPad 端验证：375px → 768px → 1024px 三个断点切换
4. 手机端验证：hamburger 导航正常、筛选区在内容上方、stat blocks 堆叠
