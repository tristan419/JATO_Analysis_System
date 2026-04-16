# SKILL: 多车型配置对比 HTML 生成器

> 适用场景：给定一份或多份车型配置 xlsx 文件，生成一个可在浏览器直接打开的单文件 HTML，支持多车型、多版型自由勾选对比，带差异高亮、搜索、分类筛选。

---

## 任务清单（按顺序执行）

### STEP 1 · 读取 xlsx，解析为结构化数据

- [ ] 用 `openpyxl` 或 `pandas` 读取每个 xlsx sheet
- [ ] 识别表格结构：通常第一列是**分类（category）**，第二列是**配置项（feature）**，第三列是**英文名（english）**，之后每列对应一个版型（variant）
  - 分类列可能存在合并单元格（merged cells），需向上填充（forward-fill）
  - 版型名称在表头行，读取时保存为 `variants[]` 数组
- [ ] 每个版型的值（`values[]`）与 `variants[]` 索引对应
- [ ] 每辆车生成一个对象：
  ```json
  {
    "car_id": "tiggo9",          // 唯一ID，字母数字，用于代码引用
    "car_name": "TIGGO 9",       // 展示名
    "color": "#e63329",          // 品牌色（每辆车分配一个色系）
    "variants": ["宁德+灰顶", "宁德+黑顶"],
    "rows": [
      {
        "category": "基本参数",
        "feature": "物料号",
        "english": "Material No.",
        "values": ["LX0006", "LX0010"]
      }
    ]
  }
  ```
- [ ] 多个 xlsx 文件 → 多个 car 对象，放入 `RAW` 数组

---

### STEP 2 · 为每辆车分配颜色

- [ ] 预设色板（每辆车一个主色）：
  ```
  车1: #e63329  (红)
  车2: #1a6ef5  (蓝)
  车3: #16a34a  (绿)
  车4: #d97706  (橙)
  车5: #7c3aed  (紫)
  ```
- [ ] 同一辆车的多个版型，在主色基础上做深浅渐变（每个版型亮度略有差异）

---

### STEP 3 · 字段模糊合并（normalizeKey）

在跨车型比较时，不同车的 xlsx 可能对同一配置项的命名不完全一致，例如：
- `DC最大充电功率（千瓦）` vs `DC最大充电功率`
- `EAER CITY续航` vs `EAER CITY续航（km）`

合并规则：
- [ ] 实现 `normalizeKey(str)` 函数：
  1. 去除所有全角/半角括号及其内容：`（千瓦）`、`(kW)` → 删除
  2. 去除所有空白字符
  3. 转小写
- [ ] 以 `normalizeKey` 结果为 key，建立全局特征 Map
- [ ] 同一 normalizeKey 下有多个原始名称时，**取最长的**作为展示用 canonical 名

---

### STEP 4 · 值归一化（比较用，非展示用）

展示时保留原始值；**仅在判断是否差异时**对值做归一化：

- [ ] 实现 `normalizeVal(str)` 用于比较：
  1. 将所有"标配/有"符号统一为 `__YES__`：`●`、`S`、`✓`、`有`、`yes`、`y`、`√`、`■`（不区分大小写）
  2. 去除数值末尾的单位：`kw`、`kW`、`千瓦`、`L`、`l`（避免 `70kw` ≠ `70` 误判）
  3. 去除首尾空格，转小写

- [ ] 差异判断逻辑（`isDiff`）：
  ```
  normVals = 所有选中版型对该特征的归一化值列表
  nonNulls = normVals 中非 null 的值

  规则：
  1. nonNulls 为空（全部缺失）→ allSame = true（都没有，不算差异）
  2. 有 null 且有非 null（一方有，一方无）→ allSame = false（差异）
  3. 全部非 null 且 Set(nonNulls).size === 1 → allSame = true（值相同）
  4. 全部非 null 且 Set(nonNulls).size > 1 → allSame = false（差异）

  isDiff = !allSame
  ```

---

### STEP 5 · 生成 HTML 结构

整个页面是一个**单文件 HTML**，无外部依赖（字体可用 Google Fonts CDN）。

#### 5.1 页面布局（从上到下）

```
[HERO]          标题 + 版型勾选区 + 统计数字
[CONTROLS]      搜索框 + 筛选按钮 + 分类下拉（sticky）
[LEGEND]        图例说明
[TABLE]         单张大表
```

#### 5.2 HERO 区 — 版型勾选

- [ ] 每辆车一个 `.car-group` 卡片，横向排列，`flex-wrap`
- [ ] 卡片内列出该车所有版型，每条是一个可勾选的 `.version-item`
- [ ] 勾选状态用 CSS class `selected-{car_id}` 控制样式（边框色+背景色用该车主色）
- [ ] 勾选/取消调用 `toggleVersion(carId, idx, el)`，触发重新渲染

统计栏：
- [ ] 显示"X 项有差异"和"共同 Y 项"（实时更新）

#### 5.3 CONTROLS 区（sticky）

- [ ] 搜索框：实时过滤配置项名称、英文名、值
- [ ] 筛选按钮：`全部` / `仅看差异`
- [ ] 分类下拉：动态生成选项，选中后只显示该分类

#### 5.4 TABLE 区 — 单张大表

**关键：使用单张 `<table>` + `<colgroup>` 保证跨分类列对齐。**

- [ ] `<colgroup>` 固定列宽：配置项列 40%，其余版型列平分剩余 60%
- [ ] 表头 `<thead>` 使用 `position: sticky; top: [controls高度]` 固定
- [ ] 分类用 `<tr class="cat-row">` 插入表体，`colspan` 覆盖全部列
- [ ] 每个配置项一行 `<tr class="data-row row-diff|row-same">`
  - 差异行：左边框高亮为橙色 `--diff: #ff9900`，行背景轻微着色
  - 同一行：无高亮

值的展示规则（`formatVal`）：

| 原始值 | 是否差异行 | 展示 |
|--------|-----------|------|
| `●` 或 `S` | 否 | `<span class="chip-yes">✓</span>`（绿色） |
| `●` 或 `S` | 是 | `<span class="chip chip-{car_id}">✓</span>`（该车颜色） |
| 空字符串 / null | — | `<span class="chip-absent">—</span>`（灰色） |
| 其他文字/数字 | 否 | `<span class="chip chip-val">{值}</span>` |
| 其他文字/数字 | 是 | `<span class="chip chip-{car_id}">{值}</span>`（该车颜色） |

---

### STEP 6 · JavaScript 数据注入

- [ ] 将 STEP 1 生成的 `RAW` 数组序列化为 JSON，直接内嵌在 HTML 的 `<script>` 标签中：
  ```html
  <script>
  const RAW = [ ...JSON... ];
  // ... 其余逻辑代码
  </script>
  ```
- [ ] `RAW` 是数组（支持任意多辆车），每个元素结构见 STEP 1

---

### STEP 7 · JS 运行逻辑

```
initVersionSelectors()
  └─ 遍历 RAW，为每辆车生成 .car-group + .version-item DOM

toggleVersion(carId, idx, el)
  └─ 维护 selectedVersions: [{ carId, idx }, ...]
  └─ 调用 rebuildTable()

buildMergedData()
  └─ 遍历 selectedVersions，收集各版型的 rows
  └─ 以 normalizeKey 合并同名字段（见 STEP 3）
  └─ 按原始 xlsx 顺序排列（先出现的 car 的 category 顺序优先）
  └─ 对每个特征计算 isDiff（见 STEP 4）
  └─ 返回 { columns, features, cats }

applyFilters()
  └─ 从 currentData.features 过滤：
       - filter === 'diff' → 只留 isDiff === true 的行
       - currentCat → 只留该分类
       - currentSearch → 模糊匹配 canonical + english + vals
  └─ 调用 renderTable(filteredFeatures, columns)

renderTable(features, columns)
  └─ 若 columns 为空 → 显示"请勾选版型"提示
  └─ 若 features 为空 → 显示"无结果"提示
  └─ 重新填充分类下拉（保留当前选中值）
  └─ 按 category 分组，跳过空分组（!items.length → continue）
  └─ 生成单张 <table>，colgroup 固定宽度，cat-row 插入分组标题
```

---

### STEP 8 · CSS 要点

```css
/* 单表固定列宽 */
table { table-layout: fixed; width: 100%; border-collapse: collapse; }
col.col-feat { width: 40%; }
col.col-ver  { width: auto; } /* 平分剩余 */

/* sticky 表头（高度按实际 controls 高度调整） */
thead th { position: sticky; top: 48px; background: var(--bg); z-index: 10; }

/* 分类标题行 */
tr.cat-row td { padding: 20px 10px 6px; border-bottom: 1px solid var(--border); }

/* 差异行 */
tr.row-diff td { background: rgba(255,153,0,0.02); }
tr.row-diff td.feat-col { border-left: 2px solid var(--diff); }

/* 勾选态（每辆车一套，动态生成或硬编码主色） */
.version-item.selected-tiggo9 { background: rgba(230,51,41,0.08); border-color: rgba(230,51,41,0.3); }
.version-item.selected-tiggo9 .ver-checkbox { background: #e63329; border-color: #e63329; }
/* ... 其他车同理 */
```

---

### STEP 9 · 输出

- [ ] 输出单个 `.html` 文件，可直接浏览器打开，无需服务器
- [ ] 文件名建议：`{车型列表}-配置对比.html`，例如 `Tiggo9-Omoda9-配置对比.html`
- [ ] 文件编码：UTF-8

---

## 常见坑（必读）

| 问题 | 原因 | 解决 |
|------|------|------|
| 不同分类的列不对齐 | 每个分类用了独立 `<table>` | 改成单张 `<table>`，用 `<colgroup>` 固定列宽 |
| `●` 和 `S` 被判定为差异 | 字符串直接比较 | `normalizeVal` 统一映射为 `__YES__` 再比较 |
| "仅看差异"里出现空分类标题 | 过滤行但没过滤分类标题 | 渲染分类前检查 `items.length === 0` 则跳过 |
| 全部缺失的行出现在差异里 | `nonNulls.length === 0` 时误判 | `nonNulls` 为空 → `allSame = true` |
| 一方有一方无不算差异 | 只比较非 null 值 | `hasNull && hasNonNull` → `isDiff = true` |
| `DC最大充电功率` vs `DC最大充电功率（千瓦）` 无法合并 | 字段名不一致 | `normalizeKey` 去括号内容后再 Map |
| 数值单位不同被误判差异（`70kw` vs `70`） | 带单位字符串不等 | `normalizeVal` 剥离末尾单位再比较 |
| xlsx 分类列合并单元格读取为空 | openpyxl 合并单元格只有第一格有值 | 读取后对 category 列做 forward-fill |
