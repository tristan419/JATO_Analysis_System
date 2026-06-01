# 夹逼分析页重构建议

## 核心判断

你要的不是再做一层“点进去、再点进去”的流程页，而是做一张**份额转移结论页**：在同一页里把“这个市场是增量、存量还是缩量”“增量最终落到哪些 model”“谁在吃别人份额”“缩量里谁最抗跌”“这些变化主要发生在 Private 还是 Business、4WD 还是 2WD”一次讲清楚。就你现在仓库里的实现看，`MarketModelRankingPage` 本质上只是把 `MarketScanPage` 预设到 `drilldown` 和 `model`，并不是一个独立的“份额转移分析架构”；而 `MarketScanPage` 本身仍是 `Overview / Origin / Segment / SUV / Drilldown / SUV-A / SUV-B / Body` 这种八页 deck 结构，所以继续沿这个方向加“流程下钻”，很难自然产出你想要的“model 份额转移结论页”。citeturn15view0turn16view4turn56view0

更关键的是，当前页面的洞察层还没有围绕“份额迁移”组织。前端的 `buildHeroMetrics` 现在直接返回空数组，而 `buildDrilldownInsight` 的主逻辑还是“榜首车型、动力主线、Top3 集中度、细分亮点、下月观察”这一类 deck narrative，它能描述头部格局和动力结构，但不能回答“谁吃掉了谁的份额”这种转移问题。换句话说，现有页面擅长**排行与结构**，不擅长**转移与归因**。citeturn47view1turn53view2turn53view4turn53view5

所以我的结论很明确：**不要把“夹逼分析”继续做成 drill-down workflow 页面；要把它定义成一页式、结论优先、证据联动的 transfer page。** 这张页不是 Market Scan 的附庸，也不是 Model Ranking 的展开版，而是一个新的分析视角：**Share Transfer 与 Gain/Loss Attribution**。这会比“国家 → 级别 → 渠道 → 驱动 → 品牌 → 动力 → 车型”的单路径点击流更贴近你的业务目标。citeturn15view0turn16view4turn47view1turn53view2

## 现状诊断

从你给的现状图看，当前主视图还是典型的**多车型单 y 轴折线**：适合看月度走势、峰谷和季节性，但对“份额转移”几乎没有直达解释力。它会告诉你 ELROQ 在某几个月冲起来了，却不会告诉你：这波量是吃了同 segment 里的谁，是增量市场顺风拿量，还是存量市场里硬抢别人的盘。这个问题不是再往图上叠几条线能解决的，而是**分析视角错了**。当前仓库里的 Market Scan 代码也没有 `yaxis2`、`secondary`、`overlaying`、右侧轴等实现痕迹，说明你现在的主干页面确实还停留在单轴表达阶段。citeturn48view0turn48view1turn48view2turn48view3turn48view4

![当前单 y 轴时间序列示意](sandbox:/mnt/data/de6f4b45-d5a0-4db2-b0f6-6ba81a251bf3.jpg)

再看你关心的字段利用率，仓库已经给了非常好的基础：后端会解析 `Drive type` 和 `Registration type` 列，并把驱动规整成 `4WD / 2WD / OTHER`，把渠道规整成 `Business / Private / Other`；前端类型里，`MarketScanRankingItem` 也已经预留了 `sharePct`、`driveMix`、`registrationMix`、`modelBreakdown` 这些结构。也就是说，**你想看的 business/private 占比、4WD/2WD 占比、model 内部分解，并不是缺字段，而是缺一套围绕转移问题的聚合逻辑和页面组织方式。**citeturn42view1turn29view2turn24view0turn23view3turn23view4

问题在于，这些字段目前主要被用在“附加信息”层面，而不是“主归因”层面。现有 segment channel mix 只支持 `overall` 和 `origin` 两种 view，说明它现在的渠道能力更像“市场结构补充视图”，还不是“Business/Private × Drive × Powertrain × Model”的归因链路；另一个很能说明问题的细节是，仓库里甚至有一个 `patch_ranking_group.py`，专门给 ranking bar 打 4WD 覆盖层和 4WD tag，这本质上是把驱动信息作为一个 ranking embellishment 去补，而不是把它升级成主分析维度。citeturn29view3turn47view1turn55view2turn54view0

## 字段能力

如果只看现在仓库可见的字段与类型，你已经足够做出一张很强的“份额转移页”。因为你至少已经有了国家、品牌、车型、细分市场、动力、驱动形式、注册类型以及月度销量列；同时前端 ranking item 还具备 `volume`、`sharePct`、`yoy`、`mom`、`driveMix`、`registrationMix`、`modelBreakdown` 这些消费结构。对一张管理层分析页来说，这已经足够支撑**总量归因、份额迁移、渠道迁移、驱动迁移、动力路线迁移**五层问题。citeturn42view1turn24view0turn23view3turn24view2

真正要厘清的是“你能回答到哪一层”。基于你当前这类聚合注册数据，我建议把“谁吃掉了谁”定义成**份额转移估算**，而不是买家级因果证明。原因很简单：你现在可见的数据结构是车型、渠道、驱动、动力和月份层面的聚合量与占比，而不是 VIN、订单、客户切换链路或置换去向层面的交易留痕。所以页面上应该写“probable transfer”“份额来源估算”“主要流失池”，而不要写成“客户从 A 真实转投 B”。这不是保守，是方法论上更严谨。这个边界并不会削弱页面价值，反而会让结论更可信。这个判断是基于现有字段粒度做出的推断。citeturn42view1turn24view0turn23view3

也正因为字段够全，我反而不建议你再围绕“字段有无”讨论太久。你现在真正缺的，不是更多字段，而是一个新的“分析事实表”与一套新的指标字典。我的建议是新增一个中间层，不直接让前端从通用 ranking item 临时拼，而是后端预聚合出一张**transfer mart**：每个 scope 下，给每个 model 输出市场增量承接、纯份额变化、渠道 mix 变化、驱动 mix 变化、动力 mix 变化、综合净得失、主要受益来源池、主要流失去向池。这样前端才有可能在一页里讲明白，而不是把很多 popover 和 tooltip 拼凑在一起。这个建议和你当前项目“Parquet + 预聚合 + page deck”的主链路并不冲突。citeturn56view0turn24view0turn29view3

## 分析框架

“夹逼分析”这页，我建议你把它拆成三层逻辑，而且每一层都只回答一个问题。

第一层回答**市场状态**。先定义当前 scope，可以是 `Country × Period × Segment`，也可以继续加 `Channel / Drive / Brand / Powertrain` 过滤。然后只看这个 scope 的总市场变化：  
`ΔM = M1 - M0`。  
如果 `ΔM > 0`，这是增量市场；如果接近 0，这是存量市场；如果 `ΔM < 0`，这是缩量市场。这里建议不要用绝对 0，当你做月度时可以给一个小阈值，比如 `|ΔM| / M0 < 2%` 归为存量。这样页面先把问题框死：你后面讲的是“抢增长”，还是“抢别人”，还是“逆风抗跌”。

第二层回答**单个 model 的得失来源**。这里最有效的不是回归，而是标准的 shift-share 分解。对任一 model `i`，设基期销量 `V_i0`、现期销量 `V_i1`，基期市场份额 `p_i0 = V_i0 / M0`，现期市场份额 `p_i1 = V_i1 / M1`。则可以把 `ΔV_i` 拆成三段：

```text
ΔV_i = 市场增长承接 + 份额转移 + 交互项

市场增长承接 = p_i0 × (M1 - M0)
份额转移 = M0 × (p_i1 - p_i0)
交互项 = (M1 - M0) × (p_i1 - p_i0)
```

这个拆法最大的好处，是它能直接回答你的三个核心问题。  
在增量市场里，谁“吃到了增量”，看的是**市场增长承接为正且份额转移也为正**，尤其是份额转移大于零的 model。  
在存量市场里，谁“吃掉了别人的份额”，看的是**市场增长项接近零但份额转移显著为正**。  
在缩量市场里，谁“最坚实”，看的是**总量虽跌但份额转移仍为正**，或者**它的实际跌幅明显小于市场增长承接项给出的基准跌幅**。这类 model 才是逆风里真正强的，不是单纯因为盘子缩得少。这里的方法是我基于你现有字段和页面目标给出的建议性设计。citeturn24view0turn42view1

第三层回答**份额主要从哪里来、流到哪里去**。这里不要试图做“绝对真实”的 buyer flow，而要做“同 scope 内的 donor-recipient 估算”。最朴素也最稳的做法，是在同一个过滤 scope 内，把所有份额流出者的负向 share shift 作为供给池，把所有份额流入者的正向 share shift 作为受益池，然后按相似度权重去配对。这个相似度权重我建议至少包含四个维度：`同 segment`、`同 channel`、`同 drive`、`同 powertrain`，必要时再加品牌相似性或价格带相邻度。  
如果 A 丢份额、B 涨份额，且二者都在 `Private + 2WD + BEV + SUV-A0` 的同一过滤域里，那么 B 从 A 那里“吃盘”的估算可信度远高于跨 drive、跨 channel、跨 powertrain 的配对。  
这一步做完，你就能得到一张很有业务价值的结论：**“在当前过滤下，增量主要落在 B/C/D；存量中 A 被 B 吃得最多；缩量中 E 虽然销量下降，但在 Private+2WD 上仍持续拿份额。”**

另外，渠道和驱动不应该只是 tooltip 信息，而应进入正式分解。也就是把 `ΔV_i` 再拆成：

```text
ΔV_i = 市场增长承接
     + 渠道 mix 变化贡献
     + 驱动 mix 变化贡献
     + 动力 mix 变化贡献
     + 纯 model 份额转移
     + 交互项
```

这样你最终给管理层看的就不是“这个 model 涨了”，而是“它为什么涨”。比如它涨 900 台，其中 300 台来自市场扩张，180 台来自 Business 占比上升，120 台来自 4WD 占比抬升，220 台来自 BEV 路线扩张，剩下 80 台才是纯 model 抢份额。这个层级一旦出来，你所谓“夹逼分析”的价值就立住了。

## 页面版式

我建议这一页固定成**一页六图加一条结论带**，不要做深钻 tab，不要做面包屑钻取树。筛选仍然保留，但筛选完成后，整页只服务一个问题：**当前 scope 下的份额转移结论**。`DeckFloatingDrawer` 和 `DeckExportDrawer` 可以保留，因为你的仓库已经把这套抽屉交互和导出设置复用机制梳理出来了，而且明确强调抽屉互斥、入口固定、本地持久化只管版式不管高频筛选，这和你要做的一页式分析非常契合。citeturn16view0turn16view5turn39view2

我会这样排版：

```text
顶部：过滤器 + 结论条 + scope chips

左上：市场状态与模型承接瀑布图
右上：Winner / Loser 蝴蝶图

左中：Model Transfer Sankey
右中：Business/Private × 4WD/2WD 热力图

左下：份额动量与韧性趋势条
右下：Powertrain / Origin 来源拆解条形图

底部：明细表
```

这六个图各自回答的问题应该非常明确。

**市场状态与模型承接瀑布图**只做一件事：把当前 scope 的总变化拆成“市场自然扩张/收缩”“渠道 mix”“驱动 mix”“动力 mix”“纯份额转移”。这是整页的总钥匙。没有它，后面所有 model gain/loss 都会显得碎。

**Winner / Loser 蝴蝶图**是页面的主结论图。左边摆前十个净流失 model，右边摆前十个净获益 model，中间统一以“净份额转移贡献”排序，而不是单纯按销量变动排序。这样存量市场与缩量市场都能读得很清楚，因为你看的不再是绝对量，而是“谁在抢盘、谁在丢盘”。

**Model Transfer Sankey**只画前五到前八个 donor 和 recipient，尾部全部并到 `Others`。Sankey 不是给你看总量，而是给你看“主要迁移路径”。如果你把 A、B、C 三个流失 model 的主要去向都集中到了 D，那这一页的一句话结论就已经出来了。

**Business/Private × 4WD/2WD 热力图**是把你手上最有价值但常被埋没的字段拉到台前。横轴是 `Business / Private`，纵轴是 `4WD / 2WD / Other`，格子里放“净份额变化”或“净转移量”。这样你很快就能知道一个 model 的 gain 到底发生在私售两驱，还是 fleet 四驱。你前面提到 MarketScan 有三角标可看 4WD/2WD 和 business/private 占比，这正说明这个维度值得被提升为主图，而不是继续埋在微标记里。citeturn29view2turn55view2

**份额动量与韧性趋势条**不需要复杂 trend line，只要做“近 3 个月份额动量”“近 6 个月份额动量”“Rolling 12M 稳定度”三个指标就够了。因为你当前系统已经天然支持 `month / ytd / rolling12 / custom range` 四种窗口，所以做轻量动量分析是水到渠成的；它会比直接加大而全的回归模块更贴近业务判断。citeturn47view0turn46view4turn46view5

**Powertrain / Origin 来源拆解图**则是为了给“为什么”再补一刀。当前 Market Scan 本来就很重视 `Origin` 和 `Fuel Trend`，但你现在需要的是把这些结构因素变成“对份额迁移的贡献”，而不是再放一张独立走势图。也就是说，同样是 BEV 份额上升，你要能看出来：这次到底是欧洲系放大带来的，还是单纯某一个 model 在 BEV 路线内上位。当前 front-end insight 工具还停留在“榜首车型、动力主线、Top3 集中度”的 narrative，所以这一步一定要升级成贡献表达。citeturn53view2turn53view4turn53view5

底部明细表则不要做普通 table，而要做**可排序的 transfer ledger**：每一行一个 model，列至少包括 `ΔVol`、`ΔShare`、`Market carryover`、`Channel mix`、`Drive mix`、`Powertrain mix`、`Pure share shift`、`主要来源池`、`主要流向池`、`韧性标签`。这样图看完以后，用户能下钻到证据，但仍然停留在同一页，不用跳路由。

## 能力取舍

关于你问的几个“有没有必要上马”，我的判断是这样的。

**双 y 轴**：有必要，但**不是大面积上马**。你现在最需要的是“归因清晰”，不是“刻度更花”。在这类份额转移页里，双 y 轴只适合放在一个非常克制的位置，比如左上角某一张总览图，用柱表示销量、折线表示份额，帮助管理层同时理解量与份额。但它绝不能成为整页默认范式。原因一是你现在主干代码里本身没有 secondary axis 结构；原因二是双 y 轴一多，用户会把注意力重新拉回“走势读图”，而不是“归因读图”。所以我的建议是：**可以有一张，不能到处有。**citeturn48view0turn48view1turn48view2turn48view3turn48view4

**回归分析**：现在不上页面主流程，更合适。不是说它没价值，而是它回答的问题和你现在最痛的问题不一样。你当前最需要先解决的是“归因展示”和“转移结论”，这用 shift-share、mix decomposition 和 transfer estimation 就能完成。回归应该放到第二阶段，变成**诊断工具**，例如专门回答“Business mix 每上升 10 个百分点，对某类 model 的份额弹性是多少”“4WD 占比变化和某品牌 gain 之间是否存在稳定关系”。仓库后端 requirements 里确实已经装了 `scikit-learn`，说明技术上不是障碍，但在没有先把 transfer page 做出来之前，直接上回归会把产品复杂度拉高，却不一定改善页面决策力。citeturn49view1

**趋势分析**：要上，但要上成**轻量、结论化的趋势分析**，不是再堆一屏折线。你已经有 month、YTD、Rolling 12M 和 custom range 窗口了，所以最自然的补强不是“再画更多走势”，而是新增两个指标：`share momentum` 和 `resilience score`。  
`share momentum` 看最近 3 个月或 6 个月份额变化的斜率。  
`resilience score` 看在缩量周期里 model 的相对抗跌程度。  
这两个指标一旦和 Winner/Loser、Transfer Sankey 联动，你就能把“这次赢的是偶发冲量，还是趋势性上位”讲清楚。citeturn47view0turn46view4turn46view5

## 实施重点

如果按投入产出比排优先级，我建议你这样落地。

先做**分析事实表**。这是最重要的一步。不要先改页面，不要先加交互，而是先在后端把 `scope × model` 的 transfer mart 做出来。这个表至少要有：基期量、现期量、基期份额、现期份额、市场承接、渠道贡献、驱动贡献、动力贡献、纯份额转移、交互项、韧性标签、主要 donor/recipient 池。只要这个表打通，前端一页式页面其实是顺水推舟。

然后做**新的页面模式**。我不建议你直接新起很多路由，最稳的做法是保留 `/market-model-ranking`，但把它从“MarketScan 的 drilldown 入口”升级成“Transfer 模式的 model 结论页”。因为现在它本来就只是 `MarketScanPage` 的别名路由，改造成本低，也符合用户对“model 分析页”的心智。citeturn15view0turn56view0

第三步才是**视觉与互动收口**。保留右上角 `DeckFloatingDrawer` 做筛选，保留 `DeckExportDrawer` 和 `ExportPanel` 做导出，但页面主体不再用 tab 去讲故事，而是固定六图一表。仓库里的抽屉复用文档已经明确了抽屉互斥、入口固定、布局写入 `localStorage`、高频筛选不进本地持久化，以及 `Top N` 这类输入要 debounce 处理；这些现成规范正好适合你这张重分析、轻跳转的页面。citeturn39view2turn16view0turn16view5

最后要提醒三个风险。第一，**Sankey 很容易脏**，所以一定要做头部裁剪和尾部分桶；第二，**“谁吃掉谁”必须写成估算**，不要写成买家级事实；第三，**深过滤会带来样本噪音**，所以当某个 `Channel × Drive × Powertrain × Segment` 样本过小的时候，页面必须主动降级成“只展示 gain/loss，不展示 donor-recipient 路径”。这类约束写清楚之后，你这张页会非常稳，而且会比现在的 drilldown 方案更像 Management Deck，而不是探索式报表。

综合看，我的最终建议是：**马上上”份额转移一页式分析”，但不要把双 y 轴和回归当成首要建设目标。** 先把 transfer mart、shift-share 分解、winner/loser 蝴蝶图、channel-drive 热力图、简化 Sankey 和韧性趋势做出来；这五件事做完，你要的”增量增到哪个 model、存量谁吃掉别人份额、缩量谁最坚实”就已经可以在一页里讲清楚，而且会比现在任何单轴折线或 drilldown 流程更有业务力度。

---

## 补充需求：图表类型增强 & 时间对比

> 更新时间：2026-05-30

### 1. 累积堆叠图（Stacked Bar / Stacked Column）

**目的**：将渠道（Business/Private）和动总（Powertrain）的月度趋势以堆叠形式展示，信息密度远高于多条独立折线。

- **渠道 × Sales 联动**：堆叠柱状图，每月一根柱子 = Business + Private 的销量，Y 轴为绝对量。同时在右侧 Y 轴叠加份额折线（双 Y 轴），显示 Business/Private 各自占比变化。
- **动总累积联动**：堆叠面积图或堆叠柱状图，每月 = BEV + HEV + PHEV + ICE 的销量堆叠。可切换为”绝对量堆叠”或”100% 占比堆叠”两种模式。支持点击单个动总分类高亮并联动下方明细表。

### 2. 双 Y 轴图（Dual Y-Axis）

**目的**：一张图同时表达”量”和”份额”，避免翻页对比。

- **左轴**：销量（柱状图，堆叠或分组）
- **右轴**：份额百分比（折线图，叠加在柱状图上）
- **典型用例**：
  - 某车型月度销量（柱）+ 市场份额（折线）
  - 某渠道 Business/Private 堆叠销量（柱）+ Business 占比趋势（折线）
  - 某动总路线累计销量（柱）+ 该路线占市场百分比（折线）

**注意**：双 Y 轴仅在总览图使用，不泛滥到所有图表。

### 3. 时间段对比（Period vs Period Comparison）

**目的**：用户可以任选两个时间段（A vs B），页面自动对比两段时间内的指标差异。

- **筛选复用 FloatingDeck 的时间轴**：DeckFloatingDrawer 中的时间筛选器应同时服务于单期分析和对比分析。
- **对比模式切换**：在筛选栏增加”对比模式”开关。开启后显示 Period A 和 Period B 两个独立的时间选择器。
- **对比输出**：
  - 蝴蝶图左侧 = Period A → Period B 的份额流失者，右侧 = 份额获得者
  - 瀑布图展示 ΔM 在两个时期之间的拆解
  - Sankey 展示 Period A 的份额分布 → Period B 的份额分布流向
  - 明细表增加 ΔVol、ΔShare、ΔChannel Mix、ΔDrive Mix、ΔPowertrain Mix 列

### 4. 浮动甲板筛选复用

**DeckFloatingDrawer** 已有时间范围选择（month / ytd / rolling12 / customRange），该组件应被共享复用：

- 单期模式：选择一个时间窗口（当前行为）
- 对比模式：选择 Period A 窗口 + Period B 窗口
- 筛选器状态不进 localStorage（高频筛选），但版式布局进 localStorage（图表排列）

### 实施优先级

```
Phase 1: Transfer Mart 后端预聚合表
Phase 2: 六图一表基础版（瀑布、蝴蝶、Sankey、热力、动量、动总拆解）
Phase 3: 堆叠图 + 双 Y 轴升级（渠道联动、动总联动）
Phase 4: 时间段对比模式（A vs B）
Phase 5: 浮动甲板筛选复用 & 导出
```citeturn24view0turn29view2turn29view3turn47view1turn53view2