# 论坛 VOC 抓取可行性论证（北欧汽车用户声音）

状态：Draft（方向已确认；当前 repo 已实现 raw collector + heuristic enriched/deck artifact；Customer Insights 与 Data Management 已补 VOC 入口）

> 2026-04-20 实现增量：`CustomerInsightsPage` 已支持 `benchmark / forum_live` 双模式与 country-focus；`/data-management` 已新增 VOC 观察台，可按国家查看 raw / enriched / deck artifact、source runs、PostgreSQL staging 状态与 VOC 文档路径。

## 1. 结论

**可以做，但要分清“原始论坛 VOC”与“结构化洞察 deck”是两层产物。**

当前 `VOC_Nordic_SUV_Users_100.xlsx` 更像一份**已经整理过的研究样本**：里面有画像、购车理由、未来需求、使用场景、价格认知、动力偏好等字段。这些字段里的很多内容，论坛原帖里通常不会天然按问卷结构出现，而是分散在帖子、回复、评论和跨站讨论里。

所以更准确的判断是：

1. **可以抓论坛 VOC。**
2. **抓到的会先是“原始声音层”**，不是现成的 Excel 结构。
3. **可以再做一层归因 / 标签 / 聚合**，把原始声音转成和当前 deck 接近的结构化输出。
4. **但不能把论坛抓取结果直接等同于问卷样本**，尤其是人口属性和代表性。

---

## 2. 什么可以从论坛里抓出来

如果数据源是公开页面、允许访问、且没有登录墙，理论上可以稳定抓到下面这些内容：

| 层级 | 可抓字段 | 价值 |
| --- | --- | --- |
| 帖子元数据 | 标题、URL、发布时间、板块、品牌/车型标签、回复数、点赞数 | 构建时间序列和热度 |
| 正文内容 | 主帖文本、楼层回复、评论摘要、引用关系 | 获取原始 VOC 证据 |
| 语义标签 | 续航、冬季续航衰减、充电、空间、拖挂、价格、补贴、品牌、软件、售后 | 做主题聚类和决策因素抽取 |
| 场景语义 | 冬季通勤、家庭长途、滑雪、拖船、乡郊碎石路、城市通勤 | 映射到北欧使用场景 |
| 情绪与立场 | 喜欢 / 抱怨 / 观望 / 推荐 / 不推荐 | 做 sentiment 与 purchase barrier |
| 竞品关系 | A 车对比 B 车、换购前车型、备选车型 | 支持 positioning 与 competitor intelligence |

这意味着：**论坛非常适合生成“用户在乎什么、抱怨什么、如何比较车型”的 VOC 层。**

---

## 3. 什么很难直接从论坛可靠得到

这部分必须单独说清楚，因为它决定了论坛抓取能不能“替代”当前 Excel。

### 3.1 人口属性不可靠

论坛帖子通常**无法稳定识别**：

- 性别
- 年龄
- 家庭结构
- 收入
- 通勤里程
- 城市 / 郊区 / 农村居住状态

有些帖子会间接透露这些信息，但覆盖很不均衡，且很容易偏差很大。

### 3.2 样本代表性不可靠

论坛用户天然有选择偏差：

- 更愿意发帖的人往往更极端
- 出现问题的用户更愿意发投诉
- 特定品牌社区会放大品牌 loyalist 的声音
- Reddit / YouTube / 垂类论坛的人群结构差异很大

所以论坛抓出来的是**“讨论样本”**，不是**“统计代表性样本”**。

### 3.3 Excel 里的部分字段只能“推断”，不能“直接采样”

例如：

- `Top 3 Favourite Features`
- `Top 3 Complaints`
- `Spending Philosophy`
- `Future Car Requirements`

这些字段在论坛里通常不是单条结构化答案，而是要靠：

1. 文本切分  
2. 实体识别  
3. 主题归类  
4. 多帖聚合  
5. LLM 或规则总结

来**推断**出来。

---

## 4. 对当前 Excel 的判断

从字段设计看，这份 `VOC_Nordic_SUV_Users_100.xlsx` 更像是：

- 对多个论坛 / 评论站 / 讨论区做过筛选
- 再按研究问题做了人工整理或半结构化归纳
- 最终落成“可展示的用户画像 + 购车因素 + 场景洞察”

也就是说，它**很可能来源于论坛声音**，但不是“直接爬下来就长这样”。

更合理的理解方式是：

**论坛内容 = 原材料**  
**当前 xlsx = 研究整理后的中间层 / 展示层**

---

## 5. 从论坛爬 VOC，技术上是否值得做

### 5.1 值得做的原因

1. **可持续更新**  
   论坛与评论站可以持续新增内容，适合做月更或周更。

2. **更贴近真实表达**  
   用户会直接谈冬季续航、热泵、充电、雪地驾驶、拖挂、二手保值、补贴变化、经销商体验，这些都很适合北欧市场研究。

3. **适合做 cross-source triangulation**  
   可把论坛 VOC 与：
   - 销量 / segment 数据
   - MSRP
   - 竞品配置
   - 新闻舆情
   联合分析。

4. **能沉淀成长期资产**  
   一旦形成 source registry、抽取 schema、聚合规则，后续国家扩展会比人工整理快很多。

### 5.2 不值得直接替代 Excel 的原因

1. **不能保证代表性**
2. **人口画像字段缺失严重**
3. **需要大量清洗 / 去重 / 归因**
4. **多语言问题突出**（瑞典语、芬兰语、挪威语、丹麦语、英语混杂）
5. **论坛页面结构异构严重**

所以最佳定位不是“用爬虫替代这份 Excel”，而是：

**用爬虫构建一个持续更新的 VOC 原始层，再产出可复核的洞察层。**

---

## 6. 合规与风险边界

论坛 VOC 抓取最容易踩坑的不是技术，而是**合规与站点策略**。

### 6.1 可以做的前提

- 仅抓取**公开可访问**页面
- 优先使用**站点 API / RSS / sitemap / 静态 HTML**
- 遵守 robots / rate limit / terms
- 不绕过登录墙、反爬验证、付费墙
- 不长期保存可识别个人信息

### 6.2 不建议做的事情

- 绕过登录才能看的车主社区
- 抓取私信、封闭群组、会员专属区
- 保存用户名、头像、签名等 PII 作为分析主数据
- 大规模原文再分发

### 6.3 建议的数据治理规则

- `author_name` 不落库，改存 `author_hash`
- 只保留必要 excerpt，不做整站镜像式长期存储
- 原始 HTML 设短 TTL
- 输出以主题、证据摘录、链接引用为主
- 对外展示时只保留**最小必要证据**

---

## 7. 推荐的数据管线分层

建议把论坛 VOC 设计成 4 层，而不是一步到位生成 deck。

### Layer A：Source Registry

维护公开来源配置，例如：

- 论坛 / 评论站名称
- 国家 / 语言
- 品牌 / 车型覆盖
- 抓取入口（版块页、搜索页、RSS、sitemap）
- 抓取方式（HTTP、RSS、Playwright）
- 合规备注（是否允许、速率约束、是否需登录）

### Layer B：Raw VOC Observation

标准化最小原始记录：

| 字段 | 说明 |
| --- | --- |
| `source_name` | 来源站点 |
| `source_url` | 原文链接 |
| `published_at` | 发布时间 |
| `country` | 市场归属 |
| `language` | 文本语言 |
| `brand` / `model` | 命中的车型实体 |
| `content_type` | post / comment / review |
| `raw_excerpt` | 最小必要证据摘录 |
| `engagement` | 回复数 / 点赞等 |
| `author_hash` | 匿名作者标识 |

### Layer C：Enriched VOC Signals

在 observation 之上补充：

- topic tags
- sentiment
- complaint / praise / requirement
- usage scenario
- ownership stage（考虑 / 已购 / 试驾 / 换购）
- winter / charging / price / practicality 等因子

### Layer D：Insight Deck

把上面的 enriched signals 聚合为：

- 主题占比
- 购车决策因素
- 高频抱怨
- 使用场景画像
- 车型对比关系
- 证据卡片

这一层才是最接近当前 `CustomerInsightsPage` 的展示对象。

---

## 8. 与现有工具链的适配性

现有仓库已经有一个很好的起点：`07_ScrapingToolkit/`。

它已经具备：

- source registry 思路
- HTTP / headless 两类提取方式
- 批次化运行入口
- 结构化输出思路

但论坛 VOC 与 MSRP 抓取有一个关键差异：

**MSRP 抓的是“结构化价格事实”，论坛抓的是“非结构化用户表达”。**

所以论坛 VOC 需要新增的能力主要是：

1. 文本切片与去重  
2. 语言识别与翻译归一  
3. topic taxonomy  
4. 证据级摘要  
5. 跨帖聚合  
6. 弱监督 / LLM 辅助标签

换句话说：**抓取框架可以复用，抽取 schema 和后处理层需要新建。**

---

## 9. 能否复刻当前北欧 deck

**可以部分复刻，但需要明确哪些是“观测值”，哪些是“推断值”。**

### 可直接支撑的部分

- 决策因素
- 抱怨点
- 购车理由
- 未来需求
- 使用场景
- 动力偏好讨论
- 价格 / TCO 认知

### 只能弱推断的部分

- 性别分布
- 年龄分布
- 家庭结构
- 周通勤里程
- occupation / household 类 profile

因此建议后续把 deck 字段分成两类：

1. **Observed from VOC**  
2. **Inferred / estimated from VOC**

避免把论坛推断结果包装成“样本统计事实”。

---

## 10. 推荐落地路径

### Phase 1：做公开论坛 VOC 原始层

先覆盖少量高价值公开源：

- 北欧主流公开汽车媒体评论区
- 公共论坛帖子页
- 公开视频评论区（若 API / TOS 允许）
- 可公开访问的 Reddit / 讨论区页面

目标不是做全，而是先证明：

- 能稳定抽到文本与元数据
- 能做车型 / 主题归因
- 能出 evidence-backed summary

### Phase 2：做主题聚合与 deck 化

围绕北欧 SUV 购车问题，先固化这些标签轴：

- 冬季能力
- 续航 / 充电
- 拖挂 / 空间
- 价格 / TCO / 补贴
- 软件 / OTA / 车机
- 品牌 / 售后 / 经销商体验

> 2026-04-19 implementation increment  
> `07_ScrapingToolkit` 已补 `jato-voc-enrich`，会把 `04_Processed_data/voc/<country>/raw/*.json`
> 继续聚合成：
> - `enriched/customer_insight_signals.json`
> - `deck/customer_insight_deck.json`
>
> 当前这层先走 heuristic extraction，覆盖 sentiment、ownership stage、pain points、product signals、
> powertrain mentions、decision factors 与 evidence cards；人口画像 / 家庭结构 / 通勤等字段仍不作为 sample facts 输出。
>
> 同期 raw layer 也已把 fetch-time 正文抽取升级为 **Trafilatura-first + lxml fallback**：
> - 优先做 main-text / boilerplate removal，再落 rawText
> - 当 Trafilatura 结果过薄时，自动退回轻量 XPath 抽取
> - raw artifact 额外记录 `textExtraction.method`，方便后续 review / 对比抽取质量

### Phase 3：和现有 Excel / 研究样本做对照

把论坛聚合结果与当前 `VOC_Nordic_SUV_Users_100.xlsx` 对比：

- 哪些结论一致
- 哪些字段只能人工研究得到
- 哪些字段论坛抓取反而更新更快

最终决定：

- Excel 保留为 benchmark deck
- Forum VOC 变成持续更新的数据源

> 2026-04-20 implementation increment  
> `06_AppPlatform` 已把这条边界真正接进产品层：
> - backend `customer_insight_service` 新增 `forum_live` mode
> - `CustomerInsightsPage` 新增 **Benchmark Excel / Forum VOC Live** 双模式
> - live 模式继续补 country-focus filter，可按单一已生成 country deck 聚焦查看
>
> 当前 live 模式展示的是 observed-only forum deck：
> - source mix / site type / language / publish tier
> - sentiment / ownership-stage hits
> - pain points / product signals / decision factors
> - evidence cards
>
> 同时 benchmark mode 继续保留 Excel 的人口画像 / sample facts，不让 forum live 直接替换 benchmark 画像页。

---

## 11. 最终建议

**建议做。**

但建议的目标不是：

> “把论坛直接爬成现在这份 xlsx”

而是：

> “把论坛公开声音沉淀成一个可持续更新的 VOC 数据层，再通过规则 + LLM 聚合成 deck 级洞察。”

最稳妥的产品与技术定位是：

- `xlsx`：当前高质量 benchmark / curated insight sample
- `forum VOC raw`：持续采集的原始声音层
- `forum VOC enriched`：主题、情绪、车型、场景归因层
- `customer insight deck`：面向业务展示的洞察层

这个路线既能保留当前研究质量，也能把 VOC 更新从一次性文档，升级成持续增长的数据资产。
