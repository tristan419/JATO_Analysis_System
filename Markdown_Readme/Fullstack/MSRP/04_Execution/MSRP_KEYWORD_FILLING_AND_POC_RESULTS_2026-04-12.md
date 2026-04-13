# MSRP Keyword Filling & POC Scraping Results

Date: 2026-04-12

Status: Batch 1+2 keyword filling complete；2026-04-13 Volkswagen configurator supplemental pass 已补记

---

## 1. 执行背景

MSRP source draft YAML 的标准化推进分为三步：

1. **骨架 scaffold 生成**：由 `source_bootstrap.py` 自动生成 626 个 draft YAML（每国 SUV Top 30）。
2. **Keyword filling**：按国家批次填充本地化关键词（动力总成、版本、价格带），将 `TODO_KEYWORD` 替换为真实值。
3. **CSS selector 填充**：用 Playwright 交互式检查真实网页结构，将 `TODO_SELECTOR` 替换为可工作的 CSS selector。

本文档记录第 2 步（keyword filling）和第 3 步的 POC 验证测试。

---

## 2. Keyword Filling 批次执行

### 2.1 Batch 1（SE + HR）

| 国家 | 文件数 | 脚本 | 状态 |
|------|------:|------|------|
| 瑞典 (SE) | 26 | `batch_fill_se_keywords.py` | ✅ 26/26 UPDATED |
| 克罗地亚 (HR) | 30 | `batch_fill_hr_keywords.py` | ✅ 30/30 UPDATED |

特点：
- SE 使用瑞典语关键词（laddhybrid, mildhybrid, bensin 等）
- HR 使用克罗地亚语关键词（hibrid, blagi hibrid, benzin 等），货币 EUR

### 2.2 Batch 2（HU + NO + AT + CZ + CH）

| 国家 | 文件数 | 脚本 | 货币 | 状态 |
|------|------:|------|------|------|
| 匈牙利 (HU) | 30 | `batch_fill_hu_keywords.py` | HUF | ✅ 30/30 UPDATED |
| 挪威 (NO) | 30 | `batch_fill_no_keywords.py` | NOK | ✅ 30/30 UPDATED |
| 奥地利 (AT) | 30 | `batch_fill_at_keywords.py` | EUR | ✅ 30/30 UPDATED |
| 捷克 (CZ) | 30 | `batch_fill_cz_keywords.py` | CZK | ✅ 30/30 UPDATED |
| 瑞士 (CH) | 30 | `batch_fill_ch_keywords.py` | CHF | ✅ 30/30 UPDATED |

特点：
- HU 使用匈牙利语（hibrid, lágy hibrid, benzin 等）
- NO 使用挪威语（hybrid, mildhybrid, bensin 等）
- AT 使用德语（Hybrid, Mild-Hybrid, Benzin 等），EUR 区
- CZ 使用捷克语（hybrid, mild hybrid, benzín 等）
- CH 使用德语（Hybrid, Mild-Hybrid, Benzin 等），CHF 货币

### 2.3 残余清理

`batch2_residual_cleanup.py` 修复了 32 个边缘 case：

| 边缘类型 | 示例 | 修复方式 |
|----------|------|---------|
| LPG 车型 | Dacia Jogger ECO-G | 补充 LPG/autogas/Flüssiggas 关键词 |
| BEV-only 车型 | Ford Puma Gen-E | 补充 elektrisch/electric 关键词 |
| PHEV 车型 | Audi Q3 TFSI e | 补充 Plug-in/plug-in-hybrid 关键词 |
| HEV-only 对应 ICE | Toyota 部分车型 | 移除不存在的 ICE 规则 |
| MHEV 全系 | 多品牌 | 补充 48V/mildhybrid 关键词 |

### 2.4 总计

| 指标 | 数值 |
|------|------|
| 总 YAML 数 | 626 |
| Batch 1+2 已填充 | 206 |
| Batch 1+2 残余修复 | 32 |
| 当前 TODO_KEYWORD 残留 | 0（在 Batch 1+2 范围内）|
| 完成率 | ~33% |

---

## 3. POC 抓取测试

### 3.1 测试目标

验证填充完 keyword 的 draft YAML 能否产出真实可用的抓取结果，为后续大规模 CSS selector 填充工作打样。

### 3.2 测试 1: Volvo SE XC60（Batch 1 代表）

**Source**: `07_ScrapingToolkit/sources/volvo_se_xc60.yaml`

**命令**:
```bash
cd 07_ScrapingToolkit
python run.py --sources volvo_se_xc60_build_scrapling --dry-run -v
```

**结果**:

| 步骤 | 结果 |
|------|------|
| Fetch | HTTP 200, stealth tier via Patchright |
| CSS 提取 | 3 个 trim card (Core Nordic Edition / Plus Nordic Edition / Ultra) |
| Validation | 3 valid, 0 rejected |
| EUR 转换 | ✅ 自动完成 |

**价格明细**:

| Trim | 本地价格 (SEK) | EUR 价格 | FX Rate |
|------|---------------:|----------:|--------:|
| Core Nordic Edition | 569,900 | 52,438.23 | 10.8680 |
| Plus Nordic Edition | 599,900 | 55,198.62 | 10.8680 |
| Ultra | 773,000 | 71,126.08 | 10.8680 |

**发现与修复**:
- 初次运行失败：currency=EUR（默认值）但 validation 期望 SEK
- 修复：在 `volvo_se_xc60.yaml` 中添加 `default_currency: SEK`
- 修复后 3/3 全部通过

### 3.3 汇率转换模块

**新增文件**: `07_ScrapingToolkit/jato_scraper/currency_converter.py`

**调用链**:
```
extract() → validate_observations() → enrich_observations_with_eur() → runner 输出
```

**API 调用**:
- 主 API: `https://open.er-api.com/v6/latest/EUR`
  - 免费、无需注册、ECB 数据源
  - 返回 166 种货币的实时汇率
- 备用 API: `https://api.exchangerate-api.com/v4/latest/EUR`

**缓存策略**: 会话级缓存，一次 `run.py` 执行只请求一次 API

**新增 RawObservation 字段**:

| 字段 | 类型 | 说明 |
|------|------|------|
| `msrp_value_eur` | `float | None` | EUR 转换后的价格 |
| `fx_rate_to_eur` | `float | None` | 1 EUR = X 本地货币的汇率 |

**Ingest payload 新增字段**: `msrp_value_eur`, `fx_rate_to_eur`

---

## 4. 相关文件

### Keyword filling 脚本

| 脚本 | 国家 |
|------|------|
| `03_Scripts/batch_fill_se_keywords.py` | SE |
| `03_Scripts/batch_fill_hr_keywords.py` | HR |
| `03_Scripts/batch_fill_hu_keywords.py` | HU |
| `03_Scripts/batch_fill_no_keywords.py` | NO |
| `03_Scripts/batch_fill_at_keywords.py` | AT |
| `03_Scripts/batch_fill_cz_keywords.py` | CZ |
| `03_Scripts/batch_fill_ch_keywords.py` | CH |
| `03_Scripts/batch2_residual_cleanup.py` | 跨国残余修复 |

### Pipeline 变更

| 文件 | 变更 |
|------|------|
| `07_ScrapingToolkit/jato_scraper/currency_converter.py` | 新增：汇率获取 + EUR 转换 |
| `07_ScrapingToolkit/jato_scraper/base.py` | 新增：`msrp_value_eur`, `fx_rate_to_eur` 字段 |
| `07_ScrapingToolkit/jato_scraper/runner.py` | 变更：import converter, 在 validate 后调用 enrich, ingest dict 新增字段 |
| `07_ScrapingToolkit/sources/volvo_se_xc60.yaml` | 修复：添加 `default_currency: SEK` |

---

## 5. 2026-04-13 Volkswagen Configurator 补充执行

### 5.1 共享 preset 与 blocker 处理

- `07_ScrapingToolkit/source_drafts/suv_only_country_model_top30/_shared/presets/volkswagen_configurator_trim_overview_playwright.yaml` 已补充 Volkswagen overlay / Ensighten / TCF dismiss 支持，用于 FR 类 trim-overview 页面。
- `07_ScrapingToolkit/source_drafts/suv_only_country_model_top30/_shared/presets/volkswagen_configurator_playwright.yaml` 已补充 OneTrust reject 支持，用于 FI 类 numeric __app configurator 页面。

### 5.2 已完成转换与验证

- BE: `be/07_volkswagen_id_4_be.yaml`、`be/10_volkswagen_t_roc_be.yaml`、`be/25_volkswagen_tiguan_be.yaml` 已切到 Playwright configurator flow，dry-run 结果分别为 8/8、10/10、21/21；production sources 已落到 `07_ScrapingToolkit/sources/`。
- DK: `dk/02_volkswagen_id_4_dk.yaml`、`dk/06_volkswagen_t_roc_dk.yaml` 已切到 numeric __app Playwright flow，dry-run 结果分别为 7/7、4/4；production sources 已落到 `07_ScrapingToolkit/sources/`。
- FR: `fr/15_volkswagen_t_roc_fr.yaml`、`fr/18_volkswagen_tiguan_fr.yaml`、`fr/20_volkswagen_t_cross_fr.yaml` 已切到 Playwright trim-overview flow，dry-run 结果分别为 4/4、7/7、6/6。
- FR production promotion 已完成：`07_ScrapingToolkit/sources/volkswagen_t_roc_fr.yaml`、`07_ScrapingToolkit/sources/volkswagen_tiguan_fr.yaml`、`07_ScrapingToolkit/sources/volkswagen_t_cross_fr.yaml`。
- IT: `it/05_volkswagen_t_roc_it.yaml`、`it/09_volkswagen_tiguan_it.yaml`、`it/11_volkswagen_t_cross_it.yaml` 已切到 Playwright trim -> engine flow，dry-run 结果分别为 5/5、24/24、12/12。
- FI: `fi/03_volkswagen_id_4_fi.yaml`、`fi/12_volkswagen_tiguan_fi.yaml`、`fi/15_volkswagen_t_cross_fi.yaml`、`fi/16_volkswagen_taigo_fi.yaml` 已切到 numeric __app Playwright flow，dry-run 结果分别为 9/9、9/9、7/7、7/7。

### 5.3 暂停点 / 下次继续

- SI / SK: Volkswagen 页面属于 Porsche configurator family，后续单独抽一套 Porsche configurator shared preset 或 extractor，不再继续硬套当前 Volkswagen shared preset。
- PL: Volkswagen 页面仍混有总价与 finance 月供语义，后续先把 total MSRP 与 finance 月供做干净分离，再转到 Playwright configurator flow。

---

## 6. 下一步

1. **SI / SK 专项**: 单独抽一套 Porsche configurator shared preset 或 extractor，再继续 Volkswagen Slovenia / Slovakia。
2. **PL 专项**: 先把总价 MSRP 与 finance 月供做干净分离，再继续 Volkswagen Poland 转换。
3. **剩余 Volkswagen backlog**: 待 SI/SK 与 PL 的专项方案稳定后，再回补 PT / RO / GR 等尚未完成的市场。
4. **其它国家与品牌**: 继续按原计划推进非 Volkswagen 的 keyword / selector / dry-run 工作流。
