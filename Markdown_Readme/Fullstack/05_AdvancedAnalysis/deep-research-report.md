# 增量市场与模型份额转移页面的深度研究结论

## 现状判断

从你给出的实现摘要和仓库当前快照看，这套系统其实已经具备做“模型份额转移结论页”的核心数据骨架。与 MarketScan 直接相关的最新前后端提交在 2026 年 5 月 28 日把 drilldown 从单选改成 searchable multi-select，并新增了 body type 过滤与 drilldown；前端 `MarketScanDeckRequest` 与后端 `/market-scan/deck` 也已经接收并透传 `drilldown_segments`、`body_types` 等参数；在排行项类型里，已经存在 `fuelMix`、`driveMix`、`registrationMix` 和带 `powertrain` 的 `modelBreakdown`。这意味着，按车型、渠道、驱动形式和动力类型做拆分，在现有数据契约层面已经基本成立。citeturn11view0turn29view0turn31view0turn28view0

但当前的信息架构仍然不是“份额转移问题导向”。应用实际路由仍把市场分析拆在多个页面里，包括 `market/overview`、`market/segments`、`market/ranking/brand`、`market/ranking/model` 和 `market/powertrain`，而 `/market-scan` 只是重定向到 `/market/overview`。与此同时，仓库里又存在一个单独的 `MarketScanPage` 文件，它内部仍采用 Overview、Origin、Segment、SUV、Drilldown、Body 等多个 tab，并接入了 `DeckFloatingDrawer` 和 `DeckExportDrawer`。换句话说，代码库已经有 “deck + filter drawer + export drawer” 这套能力，但对用户暴露的分析路径仍偏“多页浏览”或“多层 drilldown”，而不是“一页得出份额转移结论”。citeturn32view3turn18view0

更关键的是，现有 MarketScan 前端已经在排行层面暴露了你最关心的解释字段。代码中会把市场份额、总销量、四轮驱动占比、Business 与 Private 渠道占比写入标签或 hover 文本；而类型定义也清楚表明，排行项同时携带 drivetrain mix、registration mix 和 model breakdown。这说明系统并不缺字段，缺的是把这些字段组织成“谁赢、为什么赢、从谁那里赢来”的叙事层。citeturn19view0turn28view0turn28view1turn28view2

在你当前展示的单纵轴多车型时间序列里，图本身能说明“谁在涨、谁在跌”，但很难直接回答“增量到底落到哪个 model”“存量是谁吃掉了别人的份额”“缩量时谁最抗跌”。这类问题本质上不是简单的时间走势问题，而是**份额转移与结构分解问题**。

![现有的单纵轴多车型时间序列示意](sandbox:/mnt/data/de6f4b45-d5a0-4db2-b0f6-6ba81a251bf3.jpg)

所以，按第一性原理判断：你现在这套 drilldown redesign 方向并不算错，但它解决的是“逐层看细节”，不是“在一个屏幕里形成份额转移判断”。如果你的真实任务是给业务快速下结论，那么页面中心就不该是点击路径，而该是**转移结论句 + 分解证据栈**。这一点，和仓库当前仍然分散的路由结构是有张力的。citeturn32view3turn11view0

## 该如何定义增量、存量与缩量

要回答你最关心的三类问题，最合适的主框架不是继续堆 time series chart，而是把分析主轴切换到 **shift-share decomposition**。Shift-share 是标准的描述性经济分解方法：它把变化拆成整体市场变化、结构 mix 变化与竞争性 effect；其中 competitive effect 本质上是那些不能被整体趋势解释掉的剩余变化。官方与教学来源都把它定义为一种**描述性工具**，适合识别“哪些变化来自大盘、哪些变化来自结构、哪些变化来自竞争位置变化”，但并不自动给出因果解释。citeturn38view0turn39view0turn46view0

放到你这个汽车模型级场景，一个最小可解释分解可以写成：

\[
\Delta V_m = s_{m,0}\Delta M + M_0\Delta s_m + \Delta M \cdot \Delta s_m
\]

其中，\(V_m\) 是 model 的销量，\(M\) 是所选过滤范围内的总市场销量，\(s_m\) 是 model 份额。第一项是**市场扩容效应**，表示如果 model 只是按基期份额“跟着市场一起涨或跌”，本应获得多少增量；第二项是**纯份额变化效应**，表示 model 自身竞争力变化带来的份额得失；第三项是**interaction 交互项**，表示“市场变化”和“份额变化”同时发生时的联合作用。这个写法和 shift-share/扩展分解方法的思想是一致的，也正好对应你想要的“增长市场吃到的是市场增量，还是抢来的份额”。citeturn39view0turn46view0

如果你还要进一步回答“为什么赢”，就不能停在总份额层，而要把结构维度显式展开。对于你现有的字段，最实用的分解不是只做一个 market/share 两分法，而是做六段式解释：

\[
\Delta V_m = \text{Market} + \text{Channel Mix} + \text{Drive Mix} + \text{Powertrain Mix} + \text{Pure Share} + \text{Interaction}
\]

这里的 Channel 可以直接映射为 Business 商务/大客户渠道 与 Private 私人/零售渠道，Drive 对应 Four-Wheel Drive 四轮驱动与 Two-Wheel Drive 两轮驱动，Powertrain 对应 Battery Electric Vehicle 纯电动汽车、Hybrid Electric Vehicle 混合动力汽车、Plug-in Hybrid Electric Vehicle 插电式混合动力汽车、Internal Combustion Engine 内燃机、Mild Hybrid Electric Vehicle 轻混、Range-Extended Electric Vehicle 增程式电动汽车、Fuel Cell Vehicle 燃料电池汽车等。扩展 shift-share 文献特别强调，传统三段分解会把结构效应与竞争效应缠在一起，因此应该保留 interaction 或 allocation 这类扩展项，而不是硬把它们塞回“纯竞争力”。这和你现在 ledger 里存在 interaction 的方向是一致的。citeturn46view0

这套框架可以把三类市场问题明确定义出来。**增量市场**看的是“谁吃到了 market growth effect”；**存量市场**看的是“谁在总盘子几乎不变时拿到了 pure share effect”；**缩量市场**看的是“谁在 market effect 为负时，依然通过 pure share effect 或 mix effect 对冲了下滑”。这比仅看销量走势更接近业务决策，因为它把“随大盘变动”和“相对对手的竞争得失”拆开了。citeturn39view0turn46view0

还要特别说明一个边界：如果你要输出“Model A 的份额具体从 Model B、C、D 转移而来”，那在没有用户级 switching、换购链路或 VIN/owner panel 的情况下，这只能做成**估算 donor attribution**，而不能写成“真实转移”。Shift-share 本身就是描述性 accounting model，文献也明确指出它无法单独解释变化背后的因果理由，进一步解释通常需要额外分析。对你的页面来说，最好的做法是把这层结果明示为“估算来源车型”或“likely donors”，而不是“实际流失去向”。citeturn46view0turn39view0

对月度注册数据，我还建议你默认采用**动态 shift-share** 思路，而不是只做一个起点和终点的静态对比。相关综述指出，静态做法会丢失期间的连续结构变化，而动态 shift-share 通过按年或按期更新结构与竞争效应，更适合在 mix 变化较大、市场增长率波动明显的场景里追踪变化过程；但它仍然是描述性而非因果模型。对于汽车市场这种月度、政策驱动强、动力总成替代快的行业，这一点尤其重要。citeturn46view0

## 一页式模型份额转移页面应该怎么排

如果目标是一页讲明白，我的结论很明确：**不要把主页面设计成 Country → Segment → Channel → Drive → Brand → Powertrain → Model 的深点击链**。你的用户问题不是“我能逐层钻到多细”，而是“在当前过滤条件下，结论是什么、证据是什么”。仓库现状也支持这种转法：一方面，现有 App 路由仍把市场内容拆成多个页面；另一方面，后端已经有 `ranking-trend` 这样的辅助接口，可以给同一页上的 model/brand 补趋势上下文，而不用逼用户来回跳转。citeturn32view3turn31view0

我建议把页面改成**顶部过滤、下方证据栈**。过滤器保留在页首或 `DeckFloatingDrawer` 里，支持国家、周期、Segment、Body Type、Channel、Drive、Powertrain、Brand 等；页面主体只保留一套叙事顺序：先给结论，再给 why，再给 who，再给 where，再给 whether it is persistent。这样比 drilldown 更接近你提到的 MarketScan 版式思路，也更符合高频业务复盘。仓库里现成的 drawer/export 机制正好可以复用，不需要重写交互框架。citeturn18view0turn11view0

我建议的一页式版面，控制在六到七个图，最合适的组合如下：

| 区块 | 建议图型 | 它负责回答的问题 |
|---|---|---|
| 结论条 | 一句 Narrative + 关键指标卡 | 这是增长、存量还是缩量市场；Top Winner 和 Top Loser 是谁 |
| 市场分解 | Waterfall bridge chart 桥图 | 这一页 scope 的总变化，究竟由 market growth、channel mix、drive mix、powertrain mix、pure share、interaction 分别贡献多少 |
| 模型胜负榜 | 蝴蝶图或双向 bar | 哪些 model 是赢家，哪些是输家，量有多大 |
| 模型解释 | Transfer ledger 表 | 每个 model 的六段式分解；点开行可看 dominant component |
| 渠道结构 | Top models 的堆叠条形图 | Business 与 Private 渠道到底把增量送给了谁 |
| 驱动与动力总成 | Heatmap 或小倍数堆叠图 | 四驱/两驱、BEV/HEV/PHEV/ICE 等结构变化主要压在谁身上 |
| 趋势校验 | 小倍数 share trend 或 sparkline | 这次赢/输是一次性跳变，还是已经持续数月 |

之所以把桥图放在靠前位置，是因为 waterfall/bridge chart 天生适合表达“初值到终值之间由若干个正负贡献项叠加形成的变化”，而且 Power BI 与 Microsoft 的说明都强调它非常适合看 sequential changes 与 top contributors。也就是说，桥图最适合担任你页面里的“why”。citeturn41view0turn41view1

你前面提到想参考 MarketScan 的排版，这个方向是对的，但页面主轴要从“多个平行分析页”改成“一个结论页里的多个证据模块”。我会特别建议把 `Transfer ledger` 做成页面中心，而不是页面尾巴：因为业务讨论最后总会落回“某个具体 model 为什么赢/为什么输”。Waterfall 解释 scope，ledger 解释 model，这是最稳的搭配。相关趋势可以通过现有 `ranking-trend` 能力做 hover popover 或行内 sparkline，而不是另起一个跳转页面。citeturn31view0

## 双轴、趋势与回归到底该不该上

先说结论：**双轴默认不该上；趋势分析应该上；回归分析应该放到第二阶段，而不是第一页的首要能力。**

双轴图的问题并不只是“审美不好”，而是很容易视觉上误导。英国国家统计局的可视化博客明确指出，双 Y 轴图经常会让读者做出错误比较，甚至把“10 比 100 大”这种视觉错觉合理化；Datawrapper 也公开解释过，他们不支持双轴图，理由是第二个比例尺具有很强任意性，容易让受众误读两条曲线的关系。对你这种需要精确区分“绝对量变化”和“相对份额变化”的页面，双轴尤其危险，因为它会把“销量”和“份额”人为捆到一起，强化看似相关的视觉印象。citeturn40view0turn40view1

如果你的真实诉求是“既看销量，又看份额”，更好的做法不是双轴，而是两种替代方案。第一种是**并排小倍数图**：左边看销量，右边看份额，时间轴一致但比例尺分开；第二种是**指数化 share trend**：把基期设为 100，只比较相对变化。Datawrapper 明确把 side-by-side chart 和 indexed chart 作为双轴图的优先替代。对你的业务来说，这两种方式都比双轴更稳，也更容易解释。citeturn40view1

趋势分析则应该尽快补上，而且不需要等到回归。时间序列分析的标准流程，本来就强调先看 trend、seasonality、outlier、abrupt change 等基本特征；NIST 也把 trend-seasonal-residual decomposition 作为常见的一类 time-series approach。换言之，你现在最缺的不是复杂模型，而是让每个赢家/输家都带一个“这次变化是否持续、是否季节性、是否异常点”的时间上下文。这里完全可以先做 Month 当月、Year to Date 年初至今、Rolling 12 近 12 个月，再加 rolling 3 month share 或 EWMA 指数平滑（Exponentially Weighted Moving Average，指数加权移动平均）的小趋势条。citeturn42view2turn42view0

回归分析不该是第一页的首要能力，原因有两个。第一，shift-share 文献本身就提醒，描述性分解只能告诉你“变化落在哪些 effect 上”，要进一步解释 competitive effect 的深层原因，才需要额外回归或其他分析；也就是说，回归是**二阶解释层**，不是**首屏复盘层**。第二，如果你把回归用在时间序列上，又忽略了 residual autocorrelation，那么普通最小二乘 Ordinary Least Squares 的系数和标准误都会出问题；宾州州立大学的时间序列课程对此说得非常明确。citeturn46view0turn42view1

更现实的一点是：真正用于预测的时间序列回归，通常还要求你知道或预测未来的 predictor values。Forecasting: Principles and Practice 明确指出，做 ex-ante forecast 时，回归模型需要未来 predictor 的值；如果这些 predictor 无法可靠预测，就只能做 scenario-based forecasting，或者改用滞后变量。换成你的业务语言就是：如果你现在还在解决“当前 slice 到底谁赢、为什么赢”，那比起上 forecast regression，更应该先把 descriptive decomposition、trend context 和异常识别打透。citeturn43view0

所以，方法论上的排序我给得很明确：**Phase 1 先做 descriptive transfer page + trend context；Phase 2 再做 driver regression；Phase 3 如果业务真有前瞻需求，再做 scenario forecasting。** 这个顺序既符合统计方法要求，也更符合一线业务使用路径。citeturn42view1turn43view0turn46view0

## 工程落地与数据口径应该怎么收敛

从工程上看，你下一步最值得做的不是继续打磨 click flow，而是先把 **transfer mart** 的粒度定正确。这个 mart 不应该以 chart 为 grain，而应该以**country × period × segment × body type × brand × model × channel × drive × powertrain** 为 grain，至少保存 volume、share、YoY、MoM、rolling share、base/target 两期值以及分解后的各 effect。只有把粒度降到这个层级，你才能在上层任意聚合出“增长市场谁吃增量”“存量市场谁吃份额”“缩量市场谁最坚挺”。

在分解口径上，我建议你坚持三条原则。第一，**大盘 effect 与竞争 effect 必须硬拆开**，不要只给总 delta。第二，**channel、drive、powertrain 作为结构层单独出 effect**，不要统统混成一个“mix”。第三，**interaction 单列**，不要伪装成 pure share。因为扩展 shift-share 明确就是为了解决结构与竞争缠绕的问题。citeturn46view0

至于“谁从谁那里吸走份额”，工程上最实际的办法不是强行做假精确，而是做一个**estimated donors** 模块：先在同一分析 scope 内识别 share loss 的 losers，再按 segment × channel × drive × powertrain 的重合程度，加权分配到 winners。页面上把它标注成“估算来源车型”即可。这样既满足业务讨论，又不会越界声称“真实换购流向”。因为按现有方法论，这类结果本质上仍然是 accounting inference，不是 customer-level truth。citeturn46view0turn39view0

前端接入上，我会建议新页面作为一个独立路由而不是继续塞进旧的 `/market-scan` 语义下。当前 App 路由已经清晰表明市场模块是多页结构，并把 `/market-scan` 定义成重定向；如果你真要做“份额转移页”，最好直接给它一个明确入口，例如 `market/transfer` 或 `market/model-transfer`，把它定位成与 overview、segments、ranking 并列的**决策页**，而不是 drilldown 页的深入分支。citeturn32view3

而且，后端其实已经给你省掉了一部分趋势工作。`/market-scan/ranking-trend` 这个接口已经支持按 country、brand、model、segment、source_table 及其他过滤项拉趋势，你完全可以在 ledger 的行展开、tooltip 或 sparkline 里直接复用这条接口，而不必从零再建一套 trend API。也就是说，真正的新工作量集中在 transfer mart 与 decomposition，而不是趋势拉取。citeturn31view0

如果只看排期优先级，我会这样排。第一优先级是**让过滤切片稳定产出**，否则 Business、Private、Four-Wheel Drive 四轮驱动、Two-Wheel Drive 两轮驱动、Battery Electric Vehicle 纯电、Plug-in Hybrid Electric Vehicle 插混这些维度都无法形成可靠结论；第二优先级是**把 narrative 升级成页首单句结论**；第三优先级是**一页中的桥图、赢家/输家榜和 ledger 联动**；第四优先级才是 donor estimation 和回归层。这样排，最符合你“务实至上”的目标。

## 最终结论

我的判断很直接：**你不该把精力继续投在更深的 drilldown 工作流上，而应该改成一个 transfer-centric 的单页结论页。** 从仓库现状看，数据契约、drawer 机制、export 机制、趋势接口和 MarketScan 的 deck 模式都已经在；真正缺的是用 shift-share 式分解把所有字段组织成“谁赢、为什么赢、在哪个结构维度赢”的单页叙事。citeturn11view0turn18view0turn29view0turn31view0turn32view3

方法上，**趋势分析有必要尽快上；双轴图没必要作为默认方案；回归分析有必要，但应该后置。** 先把 descriptive decomposition、动态趋势、渠道/驱动/动力总成结构拆分做实，再上 regression with ARIMA errors 或 scenario-based forecasting，统计上更稳，业务上也更好用。citeturn40view0turn40view1turn42view0turn42view1turn42view2turn43view0turn46view0

如果只用一句话总结产品方向，那就是：**把“看图”改成“判案”——过滤一下，页面就直接告诉你这是不是增长、增长给了谁、谁在吃别人份额、谁在缩量里最坚挺，以及这些结论主要由渠道、驱动形式还是动力类型推动。** 这才是你要的页面。