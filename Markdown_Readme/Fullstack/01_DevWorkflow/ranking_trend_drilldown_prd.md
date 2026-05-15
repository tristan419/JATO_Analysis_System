# Ranking Table Drilldown Trend — 方案 C

> 目标：将 Ranking 表从"静态排名"升级为"可钻取分析入口"，点击 Brand row → 右侧 Drawer 展示趋势 + Top Models → 点击 Model 切换到 Model 趋势。

---

## 1. 需求背景

当前页面有两张 Ranking 表（Monthly Brand Ranking / YTD Brand Ranking），只能看排名不能解释排名。用户点击 Brand row 后应打开右侧 Trend Drawer，展示该 Brand 的趋势、份额、排名变化，以及 Top Models 以支持进一步下钻。

---

## 2. 推荐方案：方案 C

```
Brand Ranking Table → click Brand row → 右侧 Drawer 打开
  → 默认显示 Brand Trend (Sales/Share/Rank/Price 多 tab)
  → 展示 Top Models chips
  → 点击 Model chip → 切换为 Model Trend
```

**为什么选 Drawer 而非 Modal 或表内展开**：
- 保留左侧 Ranking 表上下文，不遮挡
- 可连续点击不同 Brand 对比
- 承载更多内容（Metric Tabs / Top Models / Detail Table / Export）
- 适合 dashboard 型数据产品

---

## 3. 交互设计

### 3.1 Brand Click
- Monthly Ranking → 默认展示 Last 12 Months Monthly Sales
- YTD Ranking → 默认展示 Current Year YTD Cumulative
- Drawer 继承当前页面所有筛选条件（country/segment/fuel/msrp/length）

### 3.2 Brand → Model Drilldown
- Drawer 内展示 Top 5 Models (按销量排序)
- 点击 Model chip → 图表切换为 Model 趋势
- Drawer 标题从 "Volvo Trend" → "Volvo XC60 Trend"

### 3.3 Metric Tabs
- `[ Sales ] [ Share ] [ Rank ] [ Price ]`
- Rank Y 轴反转（#1 在上方）

---

## 4. API 设计

```
GET /api/ranking-trend?country=Sweden&brand=Volvo&model=XC60&sourceTable=monthly_brand_ranking&periodMode=last_12_months&fuelTypes=BEV||PHEV&msrpMin=39000&msrpMax=71000&lengthMin=4300&lengthMax=4900
```

响应结构：
```ts
type RankingTrendResponse = {
  entityType: "brand" | "model";
  brand: string; model?: string;
  context: { country; segment; period; sourceTable; filtersApplied };
  summary: { currentMonthSales; ytdSales; currentRank; rankChange; marketShare };
  trend: TrendPoint[];          // month-by-month sales/share/rank/msrp
  topModels?: { model; sales; shareWithinBrand }[];
};
```

---

## 5. Frontend 组件

- `RankingTrendDrawer` — 右侧 Drawer 壳
- `TrendChart` — 多 metric 切换的趋势图（Plotly）
- `MetricTabs` — Sales/Share/Rank/Price 切换
- `TopModelsChips` — 该 Brand 下 Top 5 Models
- `RankingRow` — 表格行，hover 显示 "View trend →"，点击高亮

---

## 6. 实现优先级

| Phase | 内容 |
|-------|------|
| P0 | Brand row 可点击 → Drawer 打开 → Sales Trend + Summary Cards |
| P1 | Top Models → Model drilldown → Share/Rank metric tabs |
| P2 | Price Trend, Detail Table, Export PNG, Pin, hover prefetch |

---

## 7. 验收标准

- [ ] Monthly/YTD Brand Ranking 行可点击
- [ ] 点击后右侧 Drawer 打开，展示 Brand trend line chart
- [ ] Drawer 数据继承当前筛选条件
- [ ] Drawer 内展示 Summary Cards (sales/rank/share)
- [ ] Drawer 内展示 Top 5 Models
- [ ] 点击 Top Model 后切换为 Model trend
- [ ] 支持 Sales/Share/Rank metric tabs
- [ ] Rank tab Y 轴反转
- [ ] Loading/Empty/Error 状态完整
- [ ] 点击行有高亮
