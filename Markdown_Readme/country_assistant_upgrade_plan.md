# 国家助手 (Country Assistant) 升级改造计划

为了让现有的国家小助手变得像 Google AI 那样聪明，并支持精准定位车型、价格、本地配置（Local LLM Wiki），我们将进行以下架构升级：

## 一、当前痛点与重构目标
1.  **痛点**：目前基于关键字和正则式（Regex）提取意图（Intent），导致每次对话都会无差别全量加载图表，不仅加载慢而且毫无针对性。
2.  **痛点**：目前的 Assistant 图表采用 `Recharts`，与主仪表盘的 `Plotly` 风格割裂，排版丑陋。
3.  **痛点**：没有细粒度的实体知识（例如配置对比、尺寸数据），只有宏观大盘的静态 Json 摘要。
4.  **目标**：引入 **Function Calling (工具调用)** + **Local RAG (检索增强)**，并强化 Markdown 结构化输出。

---

## 二、架构演进步骤

### 阶段一：Prompt 与输出排版优化 (UI + 文本解耦)
*   **动作**：优化 `_SYSTEM_PROMPT`，强制要求当涉及到具体品牌、型号对比、尺寸规格时，采用 Markdown 表格形式严谨输出。
*   **目标**：即使在接入 RAG 之前，也能做到排版像 Google AI，层次分明，直接命中核心诉求。

### 阶段二：接入 Function Calling (模型自主按需取数)
*   **动作**：废除硬编码的无脑意图提取，把目前系统提供的 `Sales Trend`、`Pricing Map` 等宏观数据源封装为多个 `Tools`。
*   **核心逻辑**：
    *   接收用户问题 -> LLM 自主决定是否调用工具 -> 如果需要具体数据则执行查询并返回 JSON。
    *   绝不再一口气给前端推 5-10 张图表，只推按需调用的图表给到前端 `Plotly`。

### 阶段三：建立本地 Fact Sheets 知识库 (Local RAG)
*   **动作**：由于服务器有本地硬盘，我们将 `.parquet` 中的长宽高、售价信息、配置信息，以及政策相关新闻，提取并切分成轻量级 Markdown。
*   **核心逻辑**：
    *   存储引擎：可以先采用轻量级的本地 FAISS 或 Chroma。
    *   RAG Tool：增加一个 `search_local_wiki` 工具，专门用来应对涉及到“具体版型和价格”等高精度微观问题。

### 阶段四：前端图表大一统
*   **动作**：移除当前的 `CountryChatAnalysisDeck.tsx` 中的简易 Recharts 绘图。
*   **标准统一**：全面引入 `05_DashBoard` 中的高级图表代码逻辑，或者在助手侧直接调用 `LazyPlotlyChart`，做到与主驾驶舱一致的交互体验。

## 三、待办清单 (WIP)
- [x] 1. 编写此架构规划 MD 文件。
- [x] 2. 优化 `_SYSTEM_PROMPT`（加入 Markdown 表格强制性要求）。
- [x] 3. 修改 `country_chat_service.py` 以暴露 Function Calling 接口。
- [x] 4. 搭建并测试 `.parquet` 本地知识库的初步脚本。
- [ ] 5. 替换前端 React 的 `Recharts` 组件。
