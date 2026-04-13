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

## 8. 验证方式

1. `npm run check:frontend` — 类型 + 测试 + 构建 + 路由回归
2. 桌面端验证：sidebar 左侧固定、主内容右侧填充、collapse 后无异常空槽
3. iPad 端验证：375px → 768px → 1024px 三个断点切换
4. 手机端验证：hamburger 导航正常、筛选区在内容上方、stat blocks 堆叠
