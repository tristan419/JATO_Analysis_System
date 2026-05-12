# Version Comparison 页面优化需求文档  
## 主题：支持跨 Segment 版型对比 + 可搜索 Model 下拉选择器

> 适用页面：`/version-comparison`  
> 示例页面：`/version-comparison?country=瑞典&segment=SUV+B&models=Model+Y%7C%7CKodiaq%7C%7CPolestar+4&msrpMin=39000&msrpMax=71000&priceBandSize=1000`

---

# 1. 背景问题

当前 `Version Comparison / 版型对比` 页面存在一个核心限制：

> 车型选择逻辑被 `segment` 强绑定，导致用户只能在同一个 Segment 内选择 Model，无法同时加入不同 Segment 的车型。

例如当前 URL 中包含：

```txt
country=瑞典
segment=SUV+B
models=Model Y || Kodiaq || Polestar 4
msrpMin=39000
msrpMax=71000
priceBandSize=1000
```

但页面逻辑仍以 `segment=SUV B` 作为硬筛选条件，因此如果 `Model Y`、`Kodiaq`、`Polestar 4` 不属于同一个 Segment，就无法同时进入对比。

这会影响实际汽车产品分析，因为在真实业务场景中，用户并不总是只在同一 Segment 内做对比。

例如：

```txt
JAECOO 7 SHS / PHEV
vs Tiguan
vs Kodiaq
vs Model Y
vs XC60
vs Polestar 4
```

这类对比虽然不是严格的同级竞品对比，但非常适合用于：

- 消费者真实购车清单分析
- 价格带竞品分析
- 品牌向上 Benchmark
- 新能源转化路径分析
- Distributor / Management 汇报
- Hero SKU 定价定位分析

因此，页面需要从“单一 Segment 筛选”升级为“多种对比模式”。

---

# 2. 产品目标

本次优化目标不是简单取消 Segment，而是将页面升级为支持三种对比方式：

| 模式 | 英文名称 | 核心用途 |
|---|---|---|
| 同级别对比 | Same Segment Comparison | 严谨同级竞品对比 |
| 尺寸价格走廊对比 | Size & Price Corridor | 跨 Segment 但同尺寸 / 同价格带对比 |
| 自由 Benchmark 对比 | Free Benchmark Basket | 任意车型加入，用于战略 Benchmark |

核心原则：

> `Segment` 不应永远作为硬筛选条件，而应根据对比模式切换角色。  
> 在 Same Segment 模式中，Segment 是硬筛选条件。  
> 在 Size & Price Corridor 和 Free Benchmark Basket 模式中，Segment 是车型解释标签，而不是限制条件。

---

# 3. 页面核心改造逻辑

## 3.1 当前逻辑问题

当前大概率逻辑类似：

```ts
availableModels = allModels.filter(model => model.segment === selectedSegment)
```

这会导致：

```txt
只要用户选择了 SUV B，Model Picker 就只能显示 SUV B 车型。
```

问题是：

```txt
models 参数里即使有 Model Y / Kodiaq / Polestar 4，也可能因为 Segment 不匹配而被过滤掉。
```

因此，需要拆分两个概念：

```txt
Candidate Pool = 候选车型池
Selected Basket = 已选车型篮子
```

---

## 3.2 新逻辑原则

### Candidate Pool

Candidate Pool 由筛选器决定，例如：

```txt
Country
Body Type
Segment
Length Range
MSRP Range
Powertrain
Drive Type
Seats
Brand
```

### Selected Basket

Selected Basket 是用户最终选择加入对比的车型，不应被当前筛选条件自动清空。

```txt
models=Model Y||Kodiaq||Polestar 4
```

这些车型只要在当前国家数据中存在，就应该保留在已选车型篮子中。

---

# 4. 对比模式设计

页面顶部新增一个核心控件：

```txt
Comparison Mode / 对比模式
[ Same Segment ] [ Size & Price Corridor ] [ Free Benchmark Basket ]
```

中文显示建议：

```txt
对比模式：
[ 同级别对比 ] [ 尺寸价格走廊 ] [ 自由 Benchmark ]
```

---

## 4.1 模式 A：Same Segment Comparison / 同级别对比

### 适用场景

用于严谨的同级竞品分析。

例如：

```txt
SUV B:
OMODA 5 vs Toyota C-HR vs Corolla Cross vs T-Roc
```

### 筛选规则

在该模式下：

```txt
Segment = 硬筛选条件
Model Picker 只显示当前 Segment 内车型
```

### 可用筛选项

```txt
Country
Segment
MSRP Min / Max
Powertrain
Brand
Model
```

### 交互规则

1. 用户必须选择 Segment。
2. Model 下拉框只显示该 Segment 内车型。
3. 如果 URL 里已有跨 Segment 的车型，需要提示用户：

```txt
Some selected models are outside the selected segment and have been moved out of the comparison in Same Segment mode.
```

中文：

```txt
部分已选车型不属于当前 Segment，在同级别对比模式下已被移出对比。
```

也可以采用更温和方式：

```txt
当前为同级别对比模式，跨 Segment 车型将不会参与统计。
```

### 适合输出的图表

- 同级 MSRP 分布
- 同级配置差异表
- 同级销量排名
- 同级价格带分布
- 同级 Powertrain Mix
- 同级 SKU Price Ladder

---

## 4.2 模式 B：Size & Price Corridor / 尺寸价格走廊

### 适用场景

这是最推荐新增的核心模式。

用于解决：

```txt
不同 Segment，但车长接近、价格带接近、消费者可能同时考虑的车型。
```

例如：

```txt
Length: 4550–4900 mm
MSRP: 39000–71000 EUR
Body Type: SUV
Country: Sweden
```

可纳入：

```txt
Model Y
Kodiaq
Polestar 4
Tiguan
XC60
RAV4
J7 SHS
```

### 筛选规则

在该模式下：

```txt
Segment 不再作为硬筛选
Segment 只作为结果里的车型标签
```

Candidate Pool 主要由以下条件决定：

```txt
Country
Body Type
Length Min / Max
MSRP Min / Max
Powertrain
Drive Type
Seats
Brand
```

### 推荐默认字段

```txt
Country: required
Body Type: required
Length Min: optional
Length Max: optional
MSRP Min: optional
MSRP Max: optional
Powertrain: optional
Drive Type: optional
Seats: optional
Brand: optional
```

### 默认推荐范围

如果用户从某个 Target Model 进入页面，可以自动给出推荐范围：

```ts
lengthMin = targetModel.length - 250
lengthMax = targetModel.length + 350

msrpMin = targetModel.msrp * 0.75
msrpMax = targetModel.msrp * 1.35
```

例如 J7 SHS 长度约 4500 mm，可以默认：

```txt
Length Range: 4300–4850 mm
```

### 适合输出的图表

- Price vs Length Bubble Chart
- MSRP per Meter Value Map
- Powertrain Mix by Price Band
- Model Role Comparison Table
- Cross-Segment Benchmark Matrix

---

## 4.3 模式 C：Free Benchmark Basket / 自由 Benchmark

### 适用场景

用于战略汇报、消费者真实购车清单、品牌定位分析。

例如：

```txt
J7 SHS vs Model Y vs Kodiaq vs Polestar 4 vs XC60
```

这个模式不追求严格同级可比，而是模拟消费者真实 shortlist。

### 筛选规则

```txt
不做 Segment 限制
不做 Length 限制
不做 Price 限制
只要当前 Country 中存在该 Model，就可以加入 Selected Basket
```

### UI 必须提示

英文：

```txt
Mixed-segment benchmark. Suitable for strategic positioning and consumer shortlist analysis, not strict segment ranking.
```

中文：

```txt
当前为跨级别 Benchmark 对比，适合战略定位与消费者购车清单分析，不建议直接用于同级排名。
```

### 适合输出的图表

- Selected Models Summary
- Strategic Benchmark Table
- Pricing Corridor Chart
- Powertrain / Brand / Segment Tag Overview
- Consumer Choice Basket View

---

# 5. URL 参数设计

新增参数：

```txt
comparisonMode
```

可选值：

```txt
same_segment
size_price_corridor
free_basket
```

---

## 5.1 Same Segment URL

```txt
/version-comparison?
country=Sweden
&comparisonMode=same_segment
&segment=SUV+B
&models=OMODA+5||C-HR||Corolla+Cross
&msrpMin=30000
&msrpMax=50000
&priceBandSize=1000
```

---

## 5.2 Size & Price Corridor URL

```txt
/version-comparison?
country=Sweden
&comparisonMode=size_price_corridor
&bodyType=SUV
&lengthMin=4550
&lengthMax=4900
&msrpMin=39000
&msrpMax=71000
&powertrains=BEV||PHEV||HEV
&models=Model+Y||Kodiaq||Polestar+4
&priceBandSize=1000
```

---

## 5.3 Free Benchmark Basket URL

```txt
/version-comparison?
country=Sweden
&comparisonMode=free_basket
&models=Model+Y||Kodiaq||Polestar+4
&priceBandSize=1000
```

---

# 6. 数据结构建议

每个 Model 至少应包含以下字段：

```ts
type VehicleModel = {
  id: string;
  country: string;
  brand: string;
  model: string;
  version?: string;

  bodyType?: string;       // SUV, Sedan, Hatchback
  segment?: string;        // SUV B, SUV C, SUV D
  lengthMm?: number;
  widthMm?: number;
  heightMm?: number;
  wheelbaseMm?: number;

  powertrain?: string;     // ICE, MHEV, HEV, PHEV, BEV
  driveType?: string;      // FWD, RWD, AWD, 4WD
  seats?: number;

  msrp?: number;
  msrpCurrency?: string;
  sales?: number;

  source?: string;
};
```

Selected Basket 建议使用 Model ID，而不是 Model Name：

```ts
type SelectedModel = {
  id: string;
  model: string;
  brand: string;
  segment?: string;
  bodyType?: string;
  lengthMm?: number;
  powertrain?: string;
  role?: ComparisonRole;
};
```

---

# 7. Comparison Role / 对比角色

跨 Segment 对比时，必须增加 `Comparison Role`，避免用户误解为严格同级排名。

建议角色：

```ts
type ComparisonRole =
  | "Target Model"
  | "Direct Competitor"
  | "Price Benchmark"
  | "Size Benchmark"
  | "Premium Benchmark"
  | "EV Benchmark"
  | "Family Practical Benchmark"
  | "Value Benchmark";
```

中文显示：

| 英文 | 中文 |
|---|---|
| Target Model | 目标车型 |
| Direct Competitor | 直接竞品 |
| Price Benchmark | 价格 Benchmark |
| Size Benchmark | 尺寸 Benchmark |
| Premium Benchmark | 高端品牌 Benchmark |
| EV Benchmark | 纯电 Benchmark |
| Family Practical Benchmark | 家用实用 Benchmark |
| Value Benchmark | 性价比 Benchmark |

---

# 8. Model 选择器优化：可搜索下拉选择

## 8.1 当前问题

如果 Model 数量很多，普通下拉选择会非常难用。

用户需要快速找到：

```txt
Model Y
Kodiaq
Polestar 4
XC60
Tiguan
RAV4
J7 SHS
OMODA 5
```

因此 Model Picker 必须支持搜索。

---

## 8.2 Model Picker 目标体验

新增一个可搜索、多选、可展示标签的 Model Selector。

建议 UI：

```txt
Search and add models
[ Type brand or model name... ]

Selected Models:
[ Tesla Model Y ] [ Škoda Kodiaq ] [ Polestar 4 ]
```

中文：

```txt
搜索并添加车型
[ 输入品牌或车型名称... ]

已选车型：
[ Tesla Model Y ] [ Škoda Kodiaq ] [ Polestar 4 ]
```

---

## 8.3 基础功能要求

Model Picker 必须支持：

1. 输入关键词搜索
2. 支持品牌名搜索
3. 支持车型名搜索
4. 支持中英文 / 本地化名称搜索
5. 支持多选
6. 支持清除单个车型
7. 支持一键清空
8. 支持已选车型保留
9. 支持跨 Segment 车型显示
10. 支持候选车型为空时的提示

---

## 8.4 搜索匹配字段

搜索应覆盖以下字段：

```ts
brand
model
brand + model
localName
englishName
segment
powertrain
bodyType
```

例如输入：

```txt
model y
```

应匹配：

```txt
Tesla Model Y
```

输入：

```txt
tesla
```

应匹配：

```txt
Tesla Model Y
Tesla Model 3
```

输入：

```txt
phev
```

可匹配：

```txt
JAECOO 7 SHS
Tiguan eHybrid
RAV4 Plug-in Hybrid
```

输入：

```txt
suv d
```

可匹配：

```txt
Model Y
Kodiaq
XC60
Polestar 4
```

---

## 8.5 搜索结果展示格式

每条搜索结果建议显示：

```txt
Brand Model
Segment | Powertrain | Length | MSRP Range
```

示例：

```txt
Tesla Model Y
SUV D | BEV | 4751 mm | 45k–60k EUR
```

```txt
Škoda Kodiaq
SUV D | ICE / MHEV / PHEV | 4758 mm | 40k–55k EUR
```

```txt
Polestar 4
SUV D/E | BEV | 4840 mm | 55k–70k EUR
```

中文界面可以显示：

```txt
Tesla Model Y
SUV D | 纯电 BEV | 4751 mm | 45k–60k EUR
```

---

## 8.6 Same Segment 模式下的显示规则

在 Same Segment 模式中，搜索结果默认只显示当前 Segment 内车型。

但可以增加一个开关：

```txt
[ ] Show out-of-segment models
```

中文：

```txt
[ ] 显示跨 Segment 车型
```

如果用户勾选，则可以看到跨 Segment 车型，但加入时需要提示：

```txt
This model is outside the selected segment. Switch to Free Benchmark mode to compare it together.
```

中文：

```txt
该车型不属于当前 Segment。如需一起对比，请切换至自由 Benchmark 模式。
```

---

## 8.7 Size & Price Corridor 模式下的显示规则

在 Size & Price Corridor 模式中，搜索结果基于：

```txt
Country
Body Type
Length Range
MSRP Range
Powertrain
Drive Type
Seats
```

但 Segment 不作为硬限制。

搜索结果需要显示 Segment 标签，便于用户理解跨级别情况。

示例：

```txt
Tesla Model Y      SUV D | BEV | 4751 mm | Out of SUV B
Škoda Kodiaq       SUV D | ICE/PHEV | 4758 mm | Out of SUV B
JAECOO 7 SHS       SUV C | PHEV | 4500 mm | Target Range
```

---

## 8.8 Free Benchmark 模式下的显示规则

在 Free Benchmark 模式中：

```txt
只按 Country 搜索全部车型
不受 Segment、Length、MSRP 限制
```

但可以在搜索结果中显示标签：

```txt
Out of selected corridor
Out of selected segment
Premium benchmark
EV benchmark
```

---

# 9. 推荐技术实现

## 9.1 React 状态结构建议

```ts
type ComparisonMode = "same_segment" | "size_price_corridor" | "free_basket";

type VersionComparisonState = {
  country: string;
  comparisonMode: ComparisonMode;

  segment?: string;
  bodyType?: string;

  lengthMin?: number;
  lengthMax?: number;

  msrpMin?: number;
  msrpMax?: number;
  priceBandSize?: number;

  powertrains?: string[];
  driveTypes?: string[];
  seats?: number[];

  selectedModelIds: string[];
};
```

---

## 9.2 核心过滤逻辑

```ts
function getCandidateModels(
  allModels: VehicleModel[],
  state: VersionComparisonState
): VehicleModel[] {
  let models = allModels.filter(m => m.country === state.country);

  if (state.comparisonMode === "same_segment") {
    if (state.segment) {
      models = models.filter(m => m.segment === state.segment);
    }
  }

  if (state.comparisonMode === "size_price_corridor") {
    if (state.bodyType) {
      models = models.filter(m => m.bodyType === state.bodyType);
    }

    if (state.lengthMin != null) {
      models = models.filter(m => (m.lengthMm ?? 0) >= state.lengthMin!);
    }

    if (state.lengthMax != null) {
      models = models.filter(m => (m.lengthMm ?? 0) <= state.lengthMax!);
    }

    if (state.msrpMin != null) {
      models = models.filter(m => (m.msrp ?? 0) >= state.msrpMin!);
    }

    if (state.msrpMax != null) {
      models = models.filter(m => (m.msrp ?? 0) <= state.msrpMax!);
    }

    if (state.powertrains?.length) {
      models = models.filter(m => state.powertrains!.includes(m.powertrain ?? ""));
    }

    if (state.driveTypes?.length) {
      models = models.filter(m => state.driveTypes!.includes(m.driveType ?? ""));
    }
  }

  if (state.comparisonMode === "free_basket") {
    // Only country filter applies.
  }

  return models;
}
```

---

## 9.3 关键原则：不要用 Candidate Pool 覆盖 Selected Basket

错误逻辑：

```ts
selectedModels = selectedModels.filter(m => candidateModels.includes(m))
```

正确逻辑：

```ts
candidateModels = getCandidateModels(allModels, state)

selectedModels = allModels.filter(m =>
  state.selectedModelIds.includes(m.id)
)
```

也就是说：

```txt
Candidate Pool 用于下拉搜索候选项。
Selected Basket 用于最终对比展示。
两者不能互相覆盖。
```

---

## 9.4 搜索函数示例

```ts
function searchModels(
  models: VehicleModel[],
  keyword: string
): VehicleModel[] {
  const q = keyword.trim().toLowerCase();

  if (!q) return models;

  return models.filter(m => {
    const fields = [
      m.brand,
      m.model,
      `${m.brand} ${m.model}`,
      m.segment,
      m.bodyType,
      m.powertrain,
      m.driveType,
      String(m.lengthMm ?? ""),
      String(m.msrp ?? "")
    ]
      .filter(Boolean)
      .join(" ")
      .toLowerCase();

    return fields.includes(q);
  });
}
```

后续如果车型数量很大，可以升级为 Fuse.js 模糊搜索：

```bash
npm install fuse.js
```

示例：

```ts
import Fuse from "fuse.js";

const fuse = new Fuse(models, {
  keys: [
    "brand",
    "model",
    "segment",
    "bodyType",
    "powertrain",
    "driveType"
  ],
  threshold: 0.35,
});
```

---

# 10. 可搜索下拉组件建议

如果当前项目使用 `React + shadcn/ui`，建议用：

```txt
Command
Popover
Badge
Checkbox
```

如果当前项目使用 `Ant Design`，建议用：

```txt
Select mode="multiple" showSearch
```

---

## 10.1 Ant Design 示例

```tsx
<Select
  mode="multiple"
  showSearch
  allowClear
  placeholder="Search and add models"
  value={selectedModelIds}
  onChange={setSelectedModelIds}
  optionFilterProp="searchText"
  options={candidateModels.map(model => ({
    value: model.id,
    label: `${model.brand} ${model.model}`,
    searchText: [
      model.brand,
      model.model,
      model.segment,
      model.bodyType,
      model.powertrain,
      model.driveType,
      model.lengthMm,
      model.msrp
    ].filter(Boolean).join(" "),
  }))}
/>
```

---

## 10.2 shadcn/ui 结构示例

```tsx
<Popover>
  <PopoverTrigger>
    <Button variant="outline">
      Search and add models
    </Button>
  </PopoverTrigger>

  <PopoverContent>
    <Command>
      <CommandInput placeholder="Type brand or model name..." />
      <CommandList>
        {candidateModels.map(model => (
          <CommandItem
            key={model.id}
            onSelect={() => toggleModel(model.id)}
          >
            <div className="flex flex-col">
              <span>{model.brand} {model.model}</span>
              <span className="text-xs text-muted-foreground">
                {model.segment} | {model.powertrain} | {model.lengthMm} mm
              </span>
            </div>
          </CommandItem>
        ))}
      </CommandList>
    </Command>
  </PopoverContent>
</Popover>
```

---

# 11. Selected Models 展示区

已选车型建议固定显示在筛选区下方。

示例：

```txt
Selected Models

[ Tesla Model Y ]       SUV D | BEV | 4751 mm | EV Benchmark
[ Škoda Kodiaq ]        SUV D | PHEV/ICE | 4758 mm | Family Practical Benchmark
[ Polestar 4 ]          SUV D/E | BEV | 4840 mm | Premium EV Benchmark
```

建议使用卡片或 Badge：

```tsx
<Badge>
  Tesla Model Y · SUV D · BEV
</Badge>
```

每个 Badge 右侧提供删除按钮：

```txt
x
```

---

# 12. Mixed Segment 提示逻辑

当已选车型中存在多个 Segment 时：

```ts
const selectedSegments = new Set(selectedModels.map(m => m.segment));

const isMixedSegment = selectedSegments.size > 1;
```

如果 `isMixedSegment === true`，显示提示：

英文：

```txt
Mixed-segment comparison detected. Use this view for strategic benchmarking, not strict segment ranking.
```

中文：

```txt
当前已选择多个 Segment 车型。该视图适合战略 Benchmark，不建议直接用于同级销量或配置排名。
```

---

# 13. 表格字段调整

跨 Segment 后，所有对比表必须至少显示以下字段：

| 字段 | 说明 |
|---|---|
| Brand | 品牌 |
| Model | 车型 |
| Segment | 级别 |
| Body Type | 车身类型 |
| Length | 车长 |
| Powertrain | 动力形式 |
| Drive Type | 驱动形式 |
| Seats | 座位数 |
| MSRP | 建议零售价 |
| Sales | 销量 |
| Comparison Role | 对比角色 |

---

# 14. 图表规则调整

## 14.1 Same Segment 模式

允许使用：

```txt
Ranking
Average
Median
Market Share
Segment Share
```

因为同级对比具有统计可比性。

---

## 14.2 Size & Price Corridor 模式

允许使用：

```txt
Price vs Length
Price Band Distribution
Powertrain Mix
Sales Bubble
MSRP per Meter
```

但避免直接使用：

```txt
Segment Ranking
```

---

## 14.3 Free Benchmark 模式

允许使用：

```txt
Selected Model Comparison
Strategic Benchmark
Consumer Shortlist
Price Ladder
```

但必须避免：

```txt
Market Ranking
Segment Average
Strict Competitiveness Score
```

除非图表明确标注：

```txt
For reference only
```

---

# 15. 默认推荐逻辑

如果 URL 中没有 `comparisonMode`，建议默认：

```txt
same_segment
```

原因：

```txt
向后兼容当前页面逻辑。
```

如果 URL 中 `models` 包含多个不同 Segment 的车型，则自动切换为：

```txt
free_basket
```

或者显示提示：

```txt
Your selected models include multiple segments. Switch to Free Benchmark Basket?
```

为了减少用户操作，建议直接自动切换，并显示 Toast：

```txt
Multiple segments detected. Switched to Free Benchmark Basket mode.
```

中文：

```txt
检测到已选车型包含多个 Segment，已自动切换为自由 Benchmark 模式。
```

---

# 16. 验收标准

## 16.1 功能验收

- [ ] 页面新增 `comparisonMode` 参数。
- [ ] 页面支持 Same Segment / Size & Price Corridor / Free Benchmark 三种模式。
- [ ] Same Segment 模式下，车型选择仍限制在同一 Segment。
- [ ] Size & Price Corridor 模式下，可按车长、价格、车身类型筛选候选车型。
- [ ] Free Benchmark 模式下，可以搜索并加入当前国家内任意车型。
- [ ] Selected Basket 不会因为筛选条件变化而被自动清空。
- [ ] URL 中的 `models` 参数可以正确回填到已选车型。
- [ ] 如果已选车型跨 Segment，页面显示 Mixed Segment 提示。
- [ ] 所有跨 Segment 对比表都显示 Segment / Length / Powertrain / Role 字段。

---

## 16.2 Model Picker 验收

- [ ] Model 下拉选择器支持搜索。
- [ ] 支持品牌名搜索。
- [ ] 支持车型名搜索。
- [ ] 支持 Segment 搜索。
- [ ] 支持 Powertrain 搜索。
- [ ] 支持多选。
- [ ] 支持清除单个车型。
- [ ] 支持一键清空。
- [ ] 搜索结果显示 Brand、Model、Segment、Powertrain、Length、MSRP。
- [ ] 已选车型以 Badge 或 Card 形式展示。
- [ ] 已选车型不会因 Candidate Pool 变化丢失。

---

# 17. 推荐开发优先级

## P0：必须做

```txt
1. 增加 comparisonMode
2. 拆分 Candidate Pool 和 Selected Basket
3. Model Picker 改成可搜索多选
4. Free Benchmark 支持跨 Segment 车型
5. 跨 Segment 提示
```

## P1：建议做

```txt
1. Size & Price Corridor 模式
2. Length Min / Max
3. Body Type 筛选
4. Powertrain 多选
5. Selected Models 卡片化展示
```

## P2：后续增强

```txt
1. Comparison Role 手动标注
2. Fuse.js 模糊搜索
3. 自动推荐竞品
4. 根据 Target Model 自动生成长度 / 价格走廊
5. Consumer Shortlist 模板
```

---

# 18. 最终产品定义

英文：

```txt
Version Comparison should support both strict same-segment comparison and real-world shopping-basket benchmarking. Segment should be treated as a classification label, not always as a hard filter.
```

中文：

```txt
版型对比页面既要支持严谨的同级竞品分析，也要支持真实消费者购车清单式的跨级别 Benchmark。Segment 不应永远作为硬筛选条件，而应根据对比模式切换为筛选条件或解释标签。
```

---

# 19. 给 Claude Code 的执行摘要

请按以下顺序修改：

```txt
1. 在 version-comparison 页面状态中新增 comparisonMode。
2. 将当前基于 segment 的车型过滤逻辑拆成 candidateModels 和 selectedModels。
3. candidateModels 用于 Model Picker 搜索候选项。
4. selectedModels 只由 URL models 参数或用户手动选择决定，不被 segment 自动清空。
5. 新增三种模式：
   - same_segment
   - size_price_corridor
   - free_basket
6. Model Picker 改为可搜索、多选下拉。
7. 搜索字段覆盖 brand、model、segment、bodyType、powertrain、driveType、length、msrp。
8. 搜索结果展示车型摘要信息。
9. 已选车型以 Badge/Card 形式显示，并支持删除。
10. 当 selectedModels 跨 Segment 时，显示 mixed-segment warning。
```

