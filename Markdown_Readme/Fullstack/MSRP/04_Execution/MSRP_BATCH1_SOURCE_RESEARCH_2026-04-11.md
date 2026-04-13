# MSRP Batch-1 Source Research — 执行记录

Date: 2026-04-11

Status: Research completed, partial blocker remains

目标：验证 batch-1 的三个高优先级品牌国家 source 是否可以直接升为生产 MSRP source：

- 德国 Volkswagen
- 法国 Renault
- 法国 Peugeot

---

## 1. 结论摘要

### 1.1 Renault France

已验证页面：

- `https://www.renault.fr/offres-vehicules.html`
- `https://www.renault.fr/trouver-votre-renault/citadines.html`
- `https://www.renault.fr/trouver-votre-renault/suv-crossovers.html`

结论：

- `offres-vehicules.html` 能稳定列出多车型卡片，也能抓到车型文案和价格文本
- 但页面主价格语义是 `XX €/mois` 的 leasing / LLD 月供，不是 MSRP
- `trouver-votre-renault/*` 分类页能稳定列出 Clio / Nouvelle Clio / Captur / Symbioz / Austral 等车型，但当前公开文本里没有直接暴露购车价

判断：

- 适合继续做 brand-family research
- 暂不适合直接接入 MSRP pipeline

### 1.2 Peugeot France

已验证页面：

- `https://www.peugeot.fr/offres.html`
- `https://www.peugeot.fr/choisir-une-voiture/gamme-peugeot.html`
- `https://www.peugeot.fr/nos-vehicules/gamme/citadines.html`

结论：

- `offres.html` 能稳定列出 208 / 2008 / 308 / 3008 / 5008 等目标车型
- 但价格文本是 `À partir de XXX €/mois` 的月供 offer，不是 MSRP
- 分类 / gamme 页面目前主要暴露车型介绍，不暴露可直接 materialize 的购车价

判断：

- 适合继续做 source discovery
- 暂不适合直接接入 MSRP pipeline

### 1.3 Volkswagen Germany

已验证页面：

- `https://www.volkswagen.de/de/modelle.html`
- `https://www.volkswagen.de/de/modelle/golf.html`
- `https://www.volkswagen.de/de/angebote-und-produkte/aktuelle-angebote.html`
- `https://www.volkswagen.de/de.sitemap.xml`

结论：

- `modelle.html` 和单车型页在无浏览器同意 cookie 的抓取条件下，公开正文基本只剩 cookie wall
- `aktuelle-angebote.html` 确实包含多车型 offer 内容，但主体内容主要嵌在 `script#spaModel` 的 JSON 中
- 已能定位到 offer 条目对象，例如 `Tayron Life`，其中包含车型文案、月供价格和详情链接
- 但该页面同样是 leasing / offer 语义，而不是 MSRP

判断：

- Volkswagen 不是“完全抓不到”，而是当前能稳定拿到的公开入口仍然偏 offer / finance 语义
- 如果后续发现真正购车价入口，可优先考虑从 `script#spaModel` 同类 JSON 页面延展能力

---

## 2. 为什么这次没有直接升生产

MSRP pipeline 当前假设的输入是：

- 官方购车价 / MSRP / 起售价
- 或至少是与 `base_msrp` / `as_configured` / `on_the_road` 相兼容的价格语义

本次 batch-1 已验证到的品牌级页面，大多给的是：

- `XX €/mois`
- LLD / LOA 月供
- 金融 offer 示例

这类价格不能直接写入 `msrp.current_prices`，否则会把“月供”伪装成“车价”，污染整个价格事实表。

因此本次执行中，选择保持生产 source 的语义纯度，而不是为了追求 batch 数量硬接错误价格。

---

## 3. 后续建议

### 3.1 Renault / Peugeot

优先寻找：

- 官方车型页里的 `à partir de XX XXX €`
- 官方 configurator / stock / brochure / tariff PDF 的起售价入口
- 若只存在 PDF tariff，再评估是否值得引入 PDF price-list source，而不是网页 source

### 3.2 Volkswagen

优先寻找：

- `script#spaModel` 同源页面里是否存在购车价而非月供的 content section
- sitemap 中单车型页或 `verfuegbare-fahrzeuge` 页是否有可接受的语义
- 如确认 JSON 结构稳定且存在真实购车价，可考虑补一个轻量 JSON field-path 能力

### 3.3 执行顺序

建议顺序：

1. 继续保留当前已落地的 `model_rules` 能力
2. 先把 Renault / Peugeot / Volkswagen 的“真实 MSRP 入口”补齐
3. 找到真实价格入口后，再把 batch-1 source 升到 `07_ScrapingToolkit/sources/`

---

## 4. 当前状态定义

- `Implemented`:
  - brand-family `model_rules`
  - `skip_if_model_unmapped`
  - candidate scope 对 `model_rules` 的覆盖识别
  - 前端 `price history` / `override_applied` 可见性
- `Not promoted yet`:
  - Volkswagen Germany production source
  - Renault France production source
  - Peugeot France production source

原因不是 extractor 不够，而是当前确认到的品牌级公开页面价格语义不满足 MSRP 要求。