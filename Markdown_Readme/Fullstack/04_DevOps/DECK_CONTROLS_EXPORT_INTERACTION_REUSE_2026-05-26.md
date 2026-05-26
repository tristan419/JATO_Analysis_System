# Deck 筛选版式与导出设置交互复用避坑

Date: 2026-05-26
Scope: Positioning Pricing 作为试点，沉淀筛选 / 版式控制、导出图设置、图表标签策略的前端复用方案。目标页面包括 Market Scan、Version Comparison、Positioning Pricing。

## 1. 当前试点做了什么

本轮只用 `PositioningPricingPage` 做试点，核心变化集中在三个文件：

- `06_AppPlatform/frontend/src/pages/PositioningPricingPage.tsx`
- `06_AppPlatform/frontend/src/components/ExportPanel.tsx`
- `06_AppPlatform/frontend/src/index.css`

已落地的交互模型：

1. 筛选 / 版式控制从页面头部大表单改为右上固定抽屉。
   - 入口固定在导航栏下方，不随页面内容滚走。
   - 展开面板不覆盖入口，入口可以再次点击关闭。
   - 面板内部拆成 `筛选 / 范围 / 版式` 三个 tab，避免用户上下滚动找控件。
   - 打开筛选抽屉时会关闭导出抽屉，避免两个浮层互相遮挡。

2. 导出图设置也改为抽屉式配置。
   - 保留原入口，不让展开内容盖住入口。
   - 面板高度控制在半屏附近，内部滚动。
   - Price Bands 和 Powertrain Bubble 各自独立设置。

3. 布局控制从 inline style 改为 CSS 变量驱动。
   - 图表高度由 `--positioning-chart-height` 统一驱动。
   - 并排比例由 `--positioning-split-ratio` / `--positioning-remainder-ratio` 驱动。
   - 布局方向、比例、高度写入 `localStorage`，刷新后保持。
   - Reset 同时恢复筛选、导出设置、版式设置。

4. PNG 导出不再固定使用预设高度。
   - 预览画布高度根据图表高度和布局方向自动计算。
   - PNG 导出高度取 `positioningCanvasHeight` 与 `slideRef.scrollHeight` 的较大值。
   - 避免外层放开了，但内层图表容器仍固定高度导致裁切。

5. 数据标签策略沉入 `ExportPanel`。
   - 当前策略：`All / Smart Top / Selected / Clean`。
   - 默认是 `All`。
   - `Smart Top` 保留重点标签并做碰撞过滤。
   - `Selected` 只保留核心高优先级标签。
   - `Clean` 去掉标签，保留 marker / bar 本体。
   - 为兼容旧值，`none -> clean`，`smart -> smart_top`。

6. 字号拆分。
   - `fontSize` 控制坐标轴、图例、整体 layout 字号。
   - `labelFontSize` 单独控制数据标签字号。
   - 不要再让坐标轴字号和数据标签字号一起动。

7. `Top N` 改为可自定义输入。
   - 默认值从 `50` 改为 `30`。
   - 输入范围与后端 schema 对齐：`1-200`。
   - 输入框使用 draft string state，允许用户先删空再输入。
   - 只有合法数字静置 1.2 秒后才提交到真正的 `topN`。
   - URL、API 请求、页面 chip 都只读取已提交的 `topN`，不读取输入草稿。

## 2. 关键避坑

### 2.1 数字输入不能直接绑定 number state

错误模式：

```tsx
<input
  type="number"
  value={topN}
  onChange={(event) => setTopN(Number(event.target.value))}
/>
```

问题：

- 用户删除输入框时，空字符串会被转成 `0` 或 `NaN`。
- 页面会立刻刷新，请求可能发出非法或非预期值。
- 用户无法自然地从 `30` 改成 `42`，因为删除中间态会触发副作用。

正确模式：

- `topNInput: string` 只服务输入框。
- `topN: number` 才是业务提交值。
- debounce 后把合法 `topNInput` commit 成 `topN`。

```tsx
const [topN, setTopN] = useState(DEFAULT_TOP_N);
const [topNInput, setTopNInput] = useState(String(DEFAULT_TOP_N));

useEffect(() => {
  const nextTopN = parseEditableTopN(topNInput);
  if (nextTopN === null) return undefined;
  const timeoutId = window.setTimeout(() => {
    setTopN(nextTopN);
    setTopNInput(String(nextTopN));
  }, 1200);
  return () => window.clearTimeout(timeoutId);
}, [topNInput]);
```

### 2.2 URL 同步只能读 committed state

URL 是导航状态，不是输入框草稿状态。

对于 `Top N` 这种需要 debounce 的控件：

- `topNInput` 不应该写 URL。
- `topNInput` 不应该触发 API。
- `topN` 才能进入 `syncUrlParams` 和请求 payload。

否则每次敲键盘都会触发 URL replace、API reload、loading overlay。

### 2.3 Reset 要同时重置 draft state 和 committed state

只写：

```ts
setTopN(DEFAULT_TOP_N);
```

不够。输入框还会显示旧值。Reset 必须同时做：

```ts
setTopN(DEFAULT_TOP_N);
setTopNInput(String(DEFAULT_TOP_N));
```

后续抽象时，`DebouncedNumberInput` 最好支持外部 `value` 变化后同步内部 draft。

### 2.4 Plotly 不一定会因为配置对象变化而立即刷新

标签策略、标签位置、字号变化时，Plotly 的内部 diff 有时不会完全重算 text trace。

试点页用了 chart key 强制 remount：

```ts
const bubbleChartKey = [
  "bubble",
  bubbleExport.dataLabelMode,
  bubbleExport.dataLabelPosition,
  bubbleExport.dataLabelOverlapStrategy,
  bubbleExport.fontSize,
  bubbleExport.labelFontSize ?? bubbleExport.fontSize,
  bubbleExport.decimalPlaces,
].join("-");
```

注意：

- key 不要塞所有设置，只放会影响 trace / layout diff 正确性的字段。
- 颜色、网格线这类 layout 常规变化一般不需要强制 remount。

### 2.5 标签字号必须和坐标轴字号分开

导出图常见需求是：

- 坐标轴、图例字号保持克制。
- 数据标签为了 PPT 可读性单独放大或缩小。

因此 `ExportSettings` 里需要同时有：

```ts
fontSize: number;
labelFontSize?: number;
```

应用规则：

- `applyExportToLayout` 使用 `fontSize`。
- `applyDataLabelsToTraces` 使用 `labelFontSize ?? fontSize`。

不要让 `fontSize` 同时写 layout 和 trace textfont。

### 2.6 标签防重叠不要只做开关

用户需要的是版型对比式的“策略”，不是单个 boolean。

建议统一策略名：

| 策略 | 行为 |
|---|---|
| `all` | 全部显示，默认值 |
| `smart_top` | 显示重点标签，过滤重叠 |
| `selected` | 只显示核心标签 |
| `clean` | 不显示数据标签 |

避免叫 `none`。如果历史数据已有 `none`，只作为 legacy alias 映射到 `clean`。

### 2.7 PNG 裁切通常不是一层高度造成的

这次裁切根因是多层固定高度叠加：

- 图表 panel body 固定 `430px`。
- 图表容器固定 `430px`。
- PNG 导出固定使用 preset `1080`。

后续排查导出裁切时，要按顺序检查：

1. 页面预览外框高度。
2. slide frame 高度。
3. grid / panel body 高度。
4. chart wrapper 高度。
5. Plotly canvas/svg 实际高度。
6. html-to-image 的 `height` / `canvasHeight`。

只改最外层高度通常不够。

### 2.8 抽屉展开不能盖住自己的入口

这条是交互硬约束。

正确做法：

- 抽屉 shell 固定定位。
- toggle 仍在正常布局流里。
- panel 用 `position: absolute; top: calc(100% + gap)`。
- panel 的 `max-height` 和内部滚动控制在面板 body。

错误做法：

- panel `position: fixed` 后覆盖 toggle。
- 面板高度全屏，用户必须滚回顶部才能关闭。
- 两个抽屉同时打开，互相遮挡。

### 2.9 浮层 z-index 要和主导航明确分层

当前经验值：

- 顶部黑色主导航在最上层。
- 控制抽屉低于主导航，但高于页面内容。
- 导出抽屉低于控制抽屉或互斥打开。

复用时不要在每个页面随手写新的 z-index。应该沉淀为同一套 class。

### 2.10 本地持久化只用于低频版式，不用于高频筛选

适合 localStorage：

- 图表布局方向。
- 图表高度。
- 左右比例。

不适合 localStorage：

- 国家、月份、动力、Top N。
- 这些应进入 URL 或页面 state，因为它们描述当前数据查询。

## 3. 建议抽象边界

### 3.1 第一层：通用交互组件

建议新增目录：

```text
06_AppPlatform/frontend/src/components/deckControls/
```

建议组件：

```text
DeckFloatingDrawer.tsx
DeckControlTabs.tsx
DebouncedNumberInput.tsx
DeckDrawerFooterChips.tsx
```

职责划分：

| 组件 | 职责 | 不应该做 |
|---|---|---|
| `DeckFloatingDrawer` | 固定入口、液态玻璃面板、展开关闭、互斥打开 | 不知道业务筛选字段 |
| `DeckControlTabs` | 三段式 tab 切换 | 不持有筛选数据 |
| `DebouncedNumberInput` | draft string、合法化、延迟提交 | 不写 URL、不发 API |
| `DeckDrawerFooterChips` | 展示当前状态 chip | 不计算业务默认值 |

`DeckFloatingDrawer` 建议 props：

```ts
interface DeckFloatingDrawerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  eyebrow?: string;
  triggerPrimary: string;
  triggerSecondary: string;
  className?: string;
  children: React.ReactNode;
  footer?: React.ReactNode;
}
```

`DebouncedNumberInput` 建议 props：

```ts
interface DebouncedNumberInputProps {
  value: number;
  onCommit: (value: number) => void;
  min: number;
  max: number;
  step?: number;
  delayMs?: number; // 默认 1200ms
  placeholder?: string;
  disabled?: boolean;
}
```

关键实现要求：

- 内部维护 `draft: string`。
- `value` 外部变化时同步 draft。
- draft 为空时不 commit。
- commit 前 clamp + trunc。
- debounce 清理必须写在 effect cleanup。

### 3.2 第二层：通用 Hook

建议新增：

```text
06_AppPlatform/frontend/src/hooks/useDeckLayoutControls.ts
06_AppPlatform/frontend/src/hooks/useCommittedSearchNumber.ts
```

`useDeckLayoutControls` 管：

- `layoutDirection`
- `splitRatio`
- `chartHeight`
- localStorage key
- reset
- CSS variables

建议返回：

```ts
interface DeckLayoutControls {
  layoutDirection: "row" | "column";
  splitRatio: number;
  chartHeight: number;
  gridStyle: React.CSSProperties;
  setLayoutDirection: (value: "row" | "column") => void;
  setSplitRatio: (value: number) => void;
  setChartHeight: (value: number) => void;
  resetLayout: () => void;
}
```

不要让这个 hook 知道 Positioning Pricing、Market Scan、Version Comparison 的业务字段。

### 3.3 第三层：导出图设置复用

`ExportPanel` 已经承担了大部分公共能力，后续应继续把这些留在公共层：

- `ExportSettings`
- `DEFAULT_EXPORT`
- `applyExportToLayout`
- `applyDataLabelsToTraces`
- `applySeriesColors`
- `buildExportLabelModeOptions`

建议再补一个轻量 wrapper：

```text
DeckExportDrawer.tsx
```

职责：

- 固定入口。
- 半屏 panel。
- 多图表 tab。
- 打开时关闭控制抽屉。

`ExportPanel` 不应该知道“当前是底部抽屉还是页面内静态配置”。它只负责具体表单。

### 3.4 第四层：CSS 变量和通用 class

现在很多 class 还是 `positioning-pricing-*`。复用前建议抽出通用 class：

```css
.deck-floating-drawer {}
.deck-floating-toggle {}
.deck-floating-panel {}
.deck-control-tabs {}
.deck-control-tab {}
.deck-chart-grid {}
.deck-chart-grid--row {}
.deck-chart-grid--column {}
.deck-export-drawer {}
.deck-export-panel {}
```

页面特化只保留：

```css
.positioning-pricing-slide-frame {}
.positioning-pricing-summary-hero {}
```

不要把 Market Scan 和 Version Comparison 直接复制一套 `market-scan-control-drawer`、`version-comparison-control-drawer`。复制会让后面三页一起修 bug 时成本翻倍。

## 4. 市场扫描和版型对比怎么迁移

### 4.1 推荐顺序

1. 先抽 `DebouncedNumberInput`。
   - 风险最低。
   - 立刻复用到 `Top N`、模型数量、价格步长等数字筛选。

2. 再抽 `DeckFloatingDrawer` 和 `DeckControlTabs`。
   - 保持 Positioning Pricing 视觉不变。
   - 用 props 替换页面里现有 shell 结构。

3. 再抽 `useDeckLayoutControls`。
   - 先只接管 layout direction / split ratio / chart height。
   - 不要顺手把业务筛选也塞进去。

4. 最后抽 `DeckExportDrawer`。
   - 把固定入口、互斥打开、半屏 panel、tab 壳复用。
   - 保留每个页面自己的 ExportPanel 配置项。

5. 迁移 Market Scan。
   - Market Scan 控件更多，先迁移布局壳，不要一次性重写所有筛选。
   - 数字类控件统一换 `DebouncedNumberInput`。
   - 图表导出设置接入同一套 `ExportSettings`。

6. 迁移 Version Comparison。
   - 优先复用标签策略。
   - 版型对比本身已有“策略”思路，避免重新发明一套标签防重叠命名。

### 4.2 每页保留自己的业务配置

通用层只提供 shell 和基础输入控件。每页仍应自己定义：

- 默认国家 / 月份。
- 默认动力。
- 默认 Top N。
- API payload 结构。
- 页面 tab。
- 图表 label mode options。
- 图表 trace 构造函数。

这可以避免通用组件变成“大而全页面生成器”。

### 4.3 URL 策略

建议统一原则：

| 状态类型 | 存放位置 |
|---|---|
| 国家、月份、动力、Top N | URL query + React state |
| 输入框中间态 | 组件内部 draft state |
| 图表高度、布局方向、左右比例 | localStorage + React state |
| 导出图设置 | React state，必要时以后再持久化 |
| loading / drawer open | React state |

## 5. 验证清单

每次迁移一个页面，至少验证：

1. TypeScript

```bash
cd 06_AppPlatform/frontend
npm run check:types
```

2. Production build

```bash
cd 06_AppPlatform/frontend
npm run build
```

3. 浏览器手测

- 抽屉入口固定可见。
- 展开后入口仍可点击关闭。
- 控制抽屉和导出抽屉不会同时遮挡。
- 数字输入可以删空再输入。
- debounce 期间不刷新 API。
- 1.2 秒后 URL 和 API payload 更新。
- Reset 后输入框和页面 chip 一起恢复。
- 图表标签策略切换立即生效。
- 坐标轴字号和标签字号互不影响。
- PNG 导出不裁切。

4. Playwright 关键断言

建议至少测：

- 默认 Top N。
- 清空输入 1.2 秒内不发请求。
- 输入合法数字后延迟提交。
- `dataLabelOverlapStrategy` 切换后 trace 数量或 text 数量变化。
- `labelFontSize` 改动不影响 layout font。

## 6. 最终目标

后续理想结构：

```text
PositioningPricingPage
  -> DeckFloatingDrawer
  -> DeckControlTabs
  -> DebouncedNumberInput
  -> useDeckLayoutControls
  -> DeckExportDrawer
  -> ExportPanel

MarketScanPage
  -> 同上

VersionComparisonPage
  -> 同上
```

核心原则：

- 抽屉、tab、debounce 输入、布局持久化、导出设置壳体应该复用。
- 数据请求、业务默认值、trace 构造、页面文案应该留在各自页面。
- 先抽小件，再迁移页面。不要一次性把三页改成一个超大配置驱动组件。

## 7. 条形图标签复用

Date: 2026-05-26 (追加)

### 7.1 共享格式化函数

文件：`06_AppPlatform/frontend/src/utils/plotlyDefaults.ts`

```ts
/** 水平条形图排名标签： "12,345台 · 23.5%" */
export function formatCompactBarLabel(volume: number, share: number): string

/** 条形图默认标签位置：h → "middle right", 默认 → "outside" */
export function barLabelPosition(orientation?: string): string
```

### 7.2 使用方

| 位置 | 用法 |
|---|---|
| `ExportPanel.tsx` percent 模式 | `formatCompactBarLabel(v, share)` — 水平条右侧显示 "销量台 · 占比%" |
| `DashboardPage.tsx` 排名条形图 label trace | `formatCompactBarLabel(item.volume, item.share)` — 复用同一格式 |
| `MarketScanPage.tsx` | 可后续迁移（当前手动拼 `marketShareLabel + formatVolume`） |

### 7.3 行为约定

- 水平条形图（`orientation: "h"`）：标签位置 `middle right`，`cliponaxis: false`（延伸至图表外不裁切）
- 垂直柱状图：标签位置 `outside`
- percent 模式：自动检测 `orientation`，水平条走 MarketScan 风格复合标签，垂直条走纯百分比
- 所有条形图默认 `textangle: 0`（标签不旋转）
- 标签字号由 `ExportSettings.labelFontSize` 独立控制，不受坐标轴字号影响
- 排名条形图 label trace 字号响应导出设置：`textfont.size = tsExport.labelFontSize ?? tsExport.fontSize`

## 8. Dashboard 全局控制抽屉集成

Date: 2026-05-26

### 8.1 架构

Dashboard 页面 (`DashboardPage.tsx`) 使用两个全局浮动抽屉，互斥打开：

```
DeckFloatingDrawer（右上角，top: 270px）
  └─ DeckControlTabs: 窗口 | 图表 | 版式

DeckExportDrawer（右下角，position: fixed）
  └─ 03/04/05/06 section tabs + ExportPanel
```

### 8.2 互斥打开

```ts
const handleControlDrawerOpen = (open: boolean) => {
  if (open) setDeckExportDrawerOpen(false);
  setDeckControlDrawerOpen(open);
};
const handleExportDrawerOpen = (open: boolean) => {
  if (open) setDeckControlDrawerOpen(false);
  setDeckExportDrawerOpen(open);
};
```

### 8.3 窗口 tab

内嵌 `<TimeAxis>` 组件，和 02 Global Time Axis 共享同一份 state（`activeTab`、`monthGrain`、`timeRange`），两边操作实时同步。

```tsx
<TimeAxis
  labels={timeLabels}
  value={timeRange}
  onChange={setTimeRange}
  grain={activeTab}
  onGrainChange={setActiveTab}
  monthGrain={monthGrain}
  onMonthGrainChange={setMonthGrain}
/>
```

### 8.4 图表 tab

- 顶部 03/04/05/06 section 切换 tab（和导出抽屉共享 `activeDeckSection` 状态）
- 各 section 特有控件：03→TS 控件、04→分析组/图表/TopN、05→Model Name、06→Length/MSRP/TopN
- 所有输入控件使用 `DebouncedNumberInput`（1200ms debounce）
- 和页面内联控件读写同一份 state，天然同步

### 8.5 导出抽屉 section 切换

导出抽屉内的 section tab 和图表 tab 共享 `activeDeckSection`。用户切换后 ExportPanel 自动切换到对应 section 的导出设置。每页保留自己的 `ExportSettings` state（`tsExport`/`advExport`/`mvExport`/`pmExport`）。

ExportPanel 在抽屉内使用精简模式：
```tsx
<ExportPanel
  showExportButton={false}
  showDimensionControls={false}
  collapsible={false}
/>
```

### 8.6 移除内联控件

DeckFloatingDrawer 覆盖后，03 Time-Series 的 `ts-group-bar`（分组维度、Top N、Rank Limit、Include Others）从页面内联移除。保留高频交互控件（年/月 tab、图表类型 radio、series 色块）。

## 9. 版式控制（高度/宽度 + localStorage）

Date: 2026-05-26

### 9.1 State 定义

```ts
const [deckChartHeight, setDeckChartHeight] = useState(() => {
  try { const v = localStorage.getItem("dashboard-deck-chart-height"); if (v) return Number(v); } catch {}
  return 500; // 默认 500px
});
const [deckChartWidth, setDeckChartWidth] = useState(() => {
  try { const v = localStorage.getItem("dashboard-deck-chart-width"); if (v) return Number(v); } catch {}
  return 0; // 0 = auto fill
});
```

### 9.2 持久化

```ts
useEffect(() => {
  try { localStorage.setItem("dashboard-deck-chart-height", String(deckChartHeight)); } catch {}
}, [deckChartHeight]);
```

### 9.3 应用到图表

- **高度**：所有 `PlotlyChart` 的 `height` prop 使用 `deckChartHeight`
- **宽度**：不在 Plotly layout 中设 `width`（让图表响应式填满容器）。`deckChartWidth: 0` = auto fill，拖到具体值时通过容器 `maxWidth` 约束
- 排名条形图高度：`Math.max(deckChartHeight, Math.min(1200, items * 26 + 50))`

### 9.4 版式 tab UI

```tsx
<label>图表高度 {deckChartHeight}px</label>
<input type="range" min={300} max={900} step={10} />

<label>图表宽度 {deckChartWidth === 0 ? "auto" : `${deckChartWidth}px`}</label>
<input type="range" min={0} max={1400} step={20} />

<button onClick={() => { setDeckChartHeight(500); setDeckChartWidth(0); }}>
  Reset 尺寸
</button>
```

## 10. percent 数据标签双模式

Date: 2026-05-26

### 10.1 行为

实现在 `applyDataLabelsToTraces`（`ExportPanel.tsx`），在 `smart_top`/`selected` 策略之前拦截：

| 场景 | 算法 | 标签含义 |
|---|---|---|
| 多 trace（分组图） | 同一时间点各 trace 互占比 | 1月 BEV 占 1月所有动总总和的 % |
| 单 trace（总和/排名） | 该 trace 跨时间/跨条目占比 | 该条目占自身总和的 % |

### 10.2 关键实现

```ts
if (settings.dataLabelMode === "percent" && traces.length > 0) {
  const multiTrace = traces.length > 1;
  if (multiTrace) {
    // 计算每个 x-key 上所有 trace 的总和
    for (const trace of traces) {
      for (let i = 0; i < keys.length; i++) {
        totalsByKey.set(keys[i], (totalsByKey.get(keys[i]) ?? 0) + vals[i]);
      }
    }
  }
  // 多 trace: value / crossTotal; 单 trace: value / ownTotal
}
```

### 10.3 水平条特殊处理

水平条形图（`orientation: "h"`）：值在 `x`，标签在 `y`。percent 模式自动检测并用 `formatCompactBarLabel(v, share)` 生成 "12,345台 · 23.5%" 格式。

## 11. 排名条形图模式

Date: 2026-05-26

### 11.1 组件结构

`DashboardPage.tsx` — `chartType === "rank"` 时渲染：

1. **Bar trace**（`type: "bar", orientation: "h"`）：每个 rank 条目一根水平条，颜色跟随 `seriesColor()`
2. **Label trace**（`type: "scatter", mode: "text"`）：独立 scatter text trace，标签放在条右侧（`textposition: "middle right"`），`cliponaxis: false`

### 11.2 关键参数

```ts
const chartHeight = Math.max(deckChartHeight, Math.min(1200, items * 26 + 50));

layout: {
  barmode: "relative",
  margin: { r: 160, t: 24, b: 30 },        // r 给标签留空间，l 由 automargin 自动算
  xaxis: { automargin: true, fixedrange: true, showgrid: false },
  yaxis: { automargin: true, fixedrange: true, showgrid: false },
}
```

### 11.3 标签格式

Label trace 使用共享函数 `formatCompactBarLabel(volume, share)` → `"12,345台 · 23.5%"`。

字号响应导出设置：`textfont: { size: tsExport.labelFontSize ?? tsExport.fontSize }`。

### 11.4 数据来源

从 `filteredGrouped` 客户端聚合：按 series 名汇总 → 降序排列 → `rankLimit` 条。

API 调用在 rank 模式下自动使用 `rankLimit`：
```ts
top_n: chartType === "rank" ? rankLimit : (tsTopNEnabled ? tsTopN : 9999)
```

## 12. CSS 避坑

Date: 2026-05-26

### 12.1 max-height 裁切

**问题**：`.ts-ranking-chart-shell { max-height: 70vh }` 导致版式高度 slider 拖到 900px 时图表被容器裁切（70vh ≈ 560px）。

**修复**：移除固定 `max-height`，让容器高度由 JS 动态 `chartHeight` 驱动。`overflow-y: auto` 保留用于极多条目场景。

**教训**：CSS 固定高度约束不要覆盖 JS 动态高度。如果容器需要滚动，用 `overflow: auto` 而非 `max-height`。

### 12.2 automargin 与字号联动

**问题**：排名图 `margin: { l: 16 }` + `fontSize: 12` 导致长品牌名（XC60 等）被裁切。

**修复**：移除显式 `l` margin，让 `yaxis.automargin: true` 根据实际标签宽度自动计算左边距。

**教训**：`automargin` 不能收缩显式设置的 margin（只扩展）。如果需要字号自适应，不要设 `l` 最小值，完全交给 automargin。

### 12.3 cliponaxis 必须同时设在 bar 和 scatter text trace

排名条形图有两个 trace：bar（柱子）+ scatter（标签文字）。`cliponaxis: false` 只设在 bar trace 上时，scatter text 标签仍然被绘图区边界裁切。

**修复**：两个 trace 都加 `cliponaxis: false`。

### 12.4 Plotly width 不要设固定值

`layout.width` 设固定像素会覆盖响应式 `useResizeHandler`，导致图表不随容器缩放。应让图表填满容器，宽度约束由外层 CSS `max-width` 控制。
