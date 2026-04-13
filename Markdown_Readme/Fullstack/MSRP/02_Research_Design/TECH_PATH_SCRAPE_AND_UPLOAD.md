# 技术路径探讨：官网 MSRP 爬取 & Excel 工程配置上传

> 本文档是需求预研性质，**不是实现文档**。
> 目的：列清可选技术路径、风险和决策点，供后续按 PR 流程逐项推进。

---

## 一、不同国家官网 MSRP 爬取

### 1.1 现有架构

当前系统已有 `msrp.sources` 表（`MsrpSource`），每条记录绑定一个 `extractor_name` + `source_type` + `country`。爬取结果落 `msrp.observations`，由 `ScrapeBatch` 管理批次。

### 1.2 主流技术路径对比

| 路径 | 适用场景 | 优势 | 劣势 / 风险 |
|------|---------|------|------------|
| **A. 纯 HTTP + JSON API** | 品牌官网有公开 configurator API（BMW、Mercedes、Audi 等德系品牌） | 速度快、成本低、结果结构化 | API 不公开文档，随时可能变更；需按国家 × 品牌单独适配 |
| **B. Headless browser（Playwright / Puppeteer）** | 官网只通过 JS 渲染价格（Volvo、Stellantis 等） | 能处理 SPA / 动态渲染 | 资源占用高；需要维护 selector 和反爬对抗；部分站点有 bot 检测 |
| **C. 第三方数据源 / 合作方 API** | 已有 JATO Data 订阅 或 MarketScan 等 | 数据标准化、覆盖面广 | 许可证成本；数据延迟取决于供应商刷新频率 |
| **D. 混合方案** | 按 country × brand 选最优路径 | 灵活性最高 | 维护成本高；需要统一 observation schema |

### 1.3 按国家分类的初步建议（2026-04-12 更新）

当前执行口径已切换为 **SUV-only country model top30**，按国家批次推进：

| 批次 | 国家 | 推荐路径 | 币种 | keyword filling 状态 |
|------|------|----------|------|--------------------|
| Batch 1 | 瑞典（SE） | B | SEK | ✅ 完成 26 文件 |
| Batch 1 | 克罗地亚（HR） | B | EUR | ✅ 完成 30 文件 |
| Batch 2 | 匈牙利（HU） | B | HUF | ✅ 完成 30 文件 |
| Batch 2 | 挪威（NO） | B | NOK | ✅ 完成 30 文件 |
| Batch 2 | 奥地利（AT） | B/A | EUR | ✅ 完成 30 文件 |
| Batch 2 | 捷克（CZ） | B | CZK | ✅ 完成 30 文件 |
| Batch 2 | 瑞士（CH） | B | CHF | ✅ 完成 30 文件 |
| Batch 3 | 斯洛文尼亚（SI） | B | EUR | 未开始 |
| Batch 3 | 罗马尼亚（RO） | B | RON | 未开始 |
| Batch 4 | 德国（DE） | A 优先 | EUR | 未开始 |
| Batch 4 | 法国（FR） | A/B 混合 | EUR | 未开始 |
| Batch 4 | 意大利（IT） | A/B | EUR | 未开始 |
| Batch 4 | 西班牙（ES） | B | EUR | 未开始 |
| Batch 4 | 其他 8 国 | D | 各异 | 未开始 |

其中 A = 纯 HTTP + JSON API，B = Headless browser (Playwright/Scrapling)，D = 混合方案。

汇率转换：非 EUR 国家的价格在 extract 后由 `currency_converter.py` 自动调用 `open.er-api.com` 转为 EUR。

### 1.4 实现路径（需求化后按 PR 推进）

1. **定义 source 注册规范**：在 `msrp.sources` 表中为每个 country × brand 注册一条 source，指定 `source_type`（`official_api` / `headless_scrape` / `third_party_feed`）
2. **Extractor 接口抽象**：定义 `BaseExtractor` protocol，每个 extractor 实现 `extract(source: MsrpSource) -> list[RawObservation]`
3. **调度层**：`ScrapeBatch` 作为调度单元，按 country / brand 组合下发抓取任务
4. **反爬 & 合规**：
   - 遵守 robots.txt
   - 请求频率控制（per domain rate limiter）
   - 不缓存/转售原始 HTML，只保留结构化价格数据
5. **验证层**：每次抓取结果与历史 observation 做 delta 比较，价格变动超阈值自动生成 `ReviewCase`

### 1.5 风险清单

- 官网结构变更导致 extractor 失效 → 需要 CI smoke 检测 + 告警
- 反爬升级导致 IP 封禁 → 需要代理池或 cloud 函数分散
- 法律合规 → 不同国家对价格信息抓取的法律限制不同

---

## 二、Excel xlsx 上传工程配置表

### 2.1 现有架构

当前已有完整链路：

- 后端 `engineering_service.py` → `run_config_import()`
- 读取 Excel（calamine 引擎优先，fallback openpyxl）
- 自动列名匹配（`_resolve_column_mapping`）
- 行去重（`_build_row_hash`）
- 导入 → `ImportBatch` + `ConfigImportBatch` + `ConfigVariant`
- 导入状态机：`pending → success / failed`

### 2.2 当前缺少的前端上传入口

目前导入是通过后端 API 指定服务器上的文件路径，**没有前端文件上传 UI**。需求是：

> 用户在浏览器选择 xlsx 文件 → 上传到服务器 → 触发导入 → 审核 → 入库

### 2.3 推荐技术路径

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  前端上传组件  │───▶│  后端上传接口  │───▶│  格式校验     │───▶│  导入流程     │
│  (drag+drop)  │     │  POST /upload │     │  + 预览      │     │  run_config   │
│               │     │  multipart   │     │              │     │  _import()    │
└──────────────┘     └──────────────┘     └──────────────┘     └──────────────┘
                                                │
                                          ┌─────▼──────┐
                                          │  审核阶段   │
                                          │  review +   │
                                          │  confirm    │
                                          └────────────┘
```

#### Step 1: 后端文件上传接口

```
POST /engineering/projects/{id}/imports/upload
Content-Type: multipart/form-data
Body: file=<xlsx>, sheet_name=<optional>
```

- 文件保存到 `ENGINEERING_IMPORT_ROOT` 下的 `uploads/{project_id}/{timestamp}/` 目录
- 文件 hash 去重（已有 `_hash_file`）
- 返回预览结果（列映射、前 N 行、校验结果）

#### Step 2: 格式校验 & 预览

- 上传后不立即导入，先返回：
  - 识别出的列映射（`_resolve_column_mapping`）
  - 前 5 行样本数据
  - 校验警告（缺少必填列、数据类型异常等）
- 前端展示预览表格 + 校验结果

#### Step 3: 确认导入

```
POST /engineering/projects/{id}/imports/confirm
Body: { uploadId, replaceMode, notes }
```

- 用户确认后才触发 `run_config_import()`
- 结果记录在 `ConfigImportBatch`

### 2.4 审核流程

如果需要更严格的"审核才能入库"机制：

1. 导入后状态设为 `pending_review`（而非直接 `success`）
2. 管理员在 Engineering 页面审核 → 确认 → 状态变 `approved` → 变体激活
3. 拒绝 → 状态变 `rejected` → 变体标记 `is_active = false`

### 2.5 安全要求

- 文件类型白名单：仅接受 `.xlsx`、`.xlsm`、`.xls`（已有 `ENGINEERING_IMPORT_EXTENSIONS`）
- 文件大小限制：建议 ≤ 20MB
- 路径穿越防护：已有 `_resolve_import_file_path` 的 `relative_to` 校验
- 上传目录不允许指向项目根目录之外

---

## 三、这些内容是否写进 PR 文档

**建议**：是的，但按粒度拆成独立 PR。

| PR | 范围 | 前置条件 |
|----|------|---------|
| PR-A | `BaseExtractor` protocol + 第一个国家（DE）的 BMW extractor | 无 |
| PR-B | Scrape scheduler + ScrapeBatch 调度逻辑 | PR-A |
| PR-C | 前端 Excel 上传组件 + 后端 upload endpoint + 预览 | 无 |
| PR-D | 导入审核流程（pending_review → approved/rejected） | PR-C |
| PR-E | 多国家 extractor 扩展 | PR-A + PR-B |

每个 PR 都应按 `PR_CHECKLIST.md` 逐项勾选后才合入。
