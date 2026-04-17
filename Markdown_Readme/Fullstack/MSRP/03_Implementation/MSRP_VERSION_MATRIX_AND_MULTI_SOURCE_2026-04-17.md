# MSRP：Version / Feature Matrix + 多源对账（2026-04-17）

状态：Draft（合并 Q2 抓取优化 + 配置差异 + Q5 多源 reconciliation，同一张 PG schema）

关联：
- [PRODUCT_DEEPDIVE_2026-04-17.md](../../PRODUCT_DEEPDIVE_2026-04-17.md) § Q2 + Q5
- [MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md](./MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md)
- [MSRP_OVERRIDE_AND_PRICE_HISTORY_2026-04-11.md](./MSRP_OVERRIDE_AND_PRICE_HISTORY_2026-04-11.md)
- [CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md](../../03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md)

---

## 1. 目标

解决三件事：
1. MSRP 抓取通过率从 44% 推到 70%+，减少人审成本。
2. 一款车的"**多 version / 多动总 / 配置差异**"能被查询、对比、可视化。
3. 官网 vs 第三方 MSRP 有冲突时，保留多源并产出一个可解释的"当选值"。

## 2. 抓取侧优化（Q2 前半）

### 2.1 置信分级（confidence scoring）
每条 `RawObservation` 进入 pipeline 时计算 `confidence_score` ∈ [0, 1]：

```
score = 0.35 * css_hit_ratio        # 定义的 selector 命中率
      + 0.20 * field_completeness   # trim/price/currency/model 完整度
      + 0.20 * schema_org_presence  # AggregateOffer/Vehicle/@type 命中
      + 0.15 * history_sanity       # 与历史价偏差 ≤ 20% 得满分
      + 0.10 * source_tier_weight   # Tier 1 加权
```

路由：
- `≥ 0.90` → 直接 publish 到 `msrp_current_price`（跳过人审）
- `0.60–0.90` → 进 review 队列（现状）
- `< 0.60` → drop + flag 到 `msrp_low_confidence_log`

### 2.2 LLM 辅助 selector 草稿（半自动化）
新 URL 进入时，一次性跑：
1. httpx 拿 HTML。
2. 切割到前 20 KB。
3. 喂给小模型（本地 Qwen / 云端 gemini-flash），prompt："为 price / trim_name / currency 返回 CSS selector 草稿"。
4. 把草稿写入 `source_drafts/` 的 YAML，人工 diff。

这一步把 selector 从 0 → 60% 的工作外包给 LLM，60% → 100% 仍人工。

### 2.3 Firecrawl fallback（针对长尾 / 无规律页）
对 `confidence_score < 0.60` 的源，自动触发 Firecrawl `extract` API，给定 JSON schema：
```json
{"type": "object", "properties": {
  "trims": {"type": "array", "items": {"type": "object", "properties": {
    "name": {"type": "string"}, "price_local": {"type": "number"},
    "currency": {"type": "string"}, "powertrain": {"type": "string"}
  }}}
}}
```
Firecrawl 返回结构化 JSON，绕过 CSS selector。这条路径成本约 $0.003 / page，只对低置信 URL 开启。

### 2.4 PDF price sheet 抽取
BMW / Mercedes / Volvo 会定期发 PDF price list。新增 `extractors/pdf_table.py`：
- `pdfplumber` 抽 page text。
- `camelot` 抽表格（lattice 模式）。
- 输出 DataFrame → 走 `RawObservation` 常规 pipeline。

## 3. 配置差异 PG 设计（Q2 后半）

### 3.1 表结构（Alembic 0008）

```sql
-- trim 级已有，扩字段
ALTER TABLE msrp_trim ADD COLUMN trim_slug VARCHAR(128);   -- 'rav4-hybrid-active-plus-2026'
ALTER TABLE msrp_trim ADD COLUMN powertrain VARCHAR(32);
ALTER TABLE msrp_trim ADD COLUMN model_year SMALLINT;
ALTER TABLE msrp_trim ADD COLUMN drivetrain VARCHAR(16);   -- 'AWD'|'FWD'|'RWD'
ALTER TABLE msrp_trim ADD UNIQUE (country_code, brand, model, trim_slug);

-- 新：trim 价格（替代旧 msrp_current_price）
CREATE TABLE msrp_trim_price (
    price_id           BIGSERIAL PRIMARY KEY,
    trim_id            BIGINT NOT NULL REFERENCES msrp_trim(trim_id),
    currency           VARCHAR(8) NOT NULL,
    base_msrp          NUMERIC(12, 2) NOT NULL,
    base_msrp_eur      NUMERIC(12, 2),
    fx_rate_to_eur     NUMERIC(12, 6),
    effective_from     DATE NOT NULL,
    effective_to       DATE,           -- NULL = 当前生效
    reconciled_from    BIGINT[],       -- 引用 msrp_observation.observation_id
    reconciliation_log JSONB,          -- {tier1_count, tier2_count, decision, delta_pct, ...}
    is_primary         BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX ix_trim_price_active ON msrp_trim_price (trim_id) WHERE effective_to IS NULL;

-- 新：configuration 字典
CREATE TABLE feature_dictionary (
    feature_key    VARCHAR(96) PRIMARY KEY,       -- 'heads_up_display'
    zh_name        VARCHAR(128) NOT NULL,         -- 'HUD 抬头显示'
    en_name        VARCHAR(128),
    category       VARCHAR(32) NOT NULL,          -- 'tech' | 'safety' | 'comfort' | 'exterior' | 'powertrain'
    is_premium     BOOLEAN NOT NULL DEFAULT FALSE,
    description    TEXT
);

-- 新：trim 配置目录
CREATE TABLE trim_feature_catalog (
    catalog_id     BIGSERIAL PRIMARY KEY,
    trim_id        BIGINT NOT NULL REFERENCES msrp_trim(trim_id),
    feature_key    VARCHAR(96) NOT NULL REFERENCES feature_dictionary(feature_key),
    feature_kind   VARCHAR(16) NOT NULL,   -- 'standard' | 'optional' | 'package'
    option_price   NUMERIC(12, 2),         -- NULL for standard
    package_name   VARCHAR(96),            -- 属于哪个选装包
    source_id      BIGINT REFERENCES msrp_source(source_id),
    observed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (trim_id, feature_key)
);
CREATE INDEX ix_tfc_trim ON trim_feature_catalog (trim_id);
CREATE INDEX ix_tfc_feature ON trim_feature_catalog (feature_key);

-- 物化视图：两个 trim 差异
CREATE MATERIALIZED VIEW mv_trim_feature_diff AS
SELECT
    a.trim_id  AS trim_a,
    b.trim_id  AS trim_b,
    COALESCE(a.feature_key, b.feature_key) AS feature_key,
    a.feature_kind AS kind_a,
    b.feature_kind AS kind_b,
    CASE
      WHEN a.feature_kind IS NOT NULL AND b.feature_kind IS NULL THEN 'only_a'
      WHEN a.feature_kind IS NULL AND b.feature_kind IS NOT NULL THEN 'only_b'
      WHEN a.feature_kind <> b.feature_kind THEN 'different'
      ELSE 'same'
    END AS diff_kind
FROM trim_feature_catalog a
FULL OUTER JOIN trim_feature_catalog b
  ON a.feature_key = b.feature_key
 AND a.trim_id <> b.trim_id;
```

### 3.2 前端呈现
- `SpecificationPage.tsx` 已有 trim detail 骨架，扩展：右上角 "Compare" 按钮 → 选第二个 trim → 显示 diff 三栏（`only_a` / `only_b` / `different`），同时显示两者差价。
- `VersionComparisonPage.tsx` 负责跨 model 比对（不同车型同价位段）。

### 3.3 数据来源
配置数据来源分三层：
1. **官方配置表 PDF**（最权威）：通过 Q2.4 的 PDF pipeline 抓。
2. **官网配置页** schema.org `Vehicle.vehicleConfiguration`：Scrapling 扩 rule。
3. **汽车媒体试驾报告**：Firecrawl `extract` 作为补全（标 `source_tier=3`）。

## 4. 多源 Reconciliation（Q5）

### 4.1 Source Tier 定义（建表时 seed）
```sql
ALTER TABLE msrp_source ADD COLUMN tier SMALLINT NOT NULL DEFAULT 3;
ALTER TABLE msrp_source ADD COLUMN source_category VARCHAR(32) NOT NULL DEFAULT 'third_party';
-- tier=1: 品牌官网
-- tier=2: 官方经销商网
-- tier=3: 权威汽车媒体
-- tier=4: 其他第三方 / 比价站
-- tier=5: 租赁月供页（不入 reconciliation，只做 research）
```

每条 `msrp_source` 录入时必须指派 tier；历史数据在 Alembic 0009 batch update。

### 4.2 Reconciliation Job
```
reconcile_prices(country, trim_id, price_period):
  obs = SELECT * FROM msrp_observation
        WHERE trim_id=? AND country=?
          AND observed_at BETWEEN price_period
          AND source_tier <= 4
        ORDER BY source_tier, observed_at DESC

  tier1 = obs WHERE source_tier=1 AND age_days <= 30
  if tier1:
      decision = tier1[0]  # 最新一条
      log = { method: 'tier1_authoritative', used: tier1[0].obs_id }
  else:
      tier2p = obs WHERE source_tier IN (2,3)
      prices = [o.price_local for o in tier2p]
      delta_pct = (max(prices) - min(prices)) / median(prices)
      if delta_pct <= 0.03:
          decision.price = weighted_median(prices, by=source_tier)
          log = { method: 'tier2_weighted_median', delta_pct, used: [o.obs_id for o in tier2p] }
      else:
          enqueue_review(country, trim_id, reason='tier_conflict', delta_pct)
          return  # 不发布

  upsert msrp_trim_price SET
      base_msrp=decision.price,
      reconciled_from=log.used,
      reconciliation_log=log
```

### 4.3 前端呈现
- MSRP detail drawer 增加 "Sources (n)" 折叠项：并排列各 source price + tier + observed_at。
- 差异 > 3% 时 red badge "Conflict pending review"。
- 当前价旁小标 `reconciled · 3 sources` 可点击查看。

## 5. 回流到 review workbench
- Review 队列新增 tab "Tier Conflict"（delta_pct > 3% 的案例）。
- Review decision 写回 `reconciliation_log.reviewer_choice` + `source_tier_override`。
- 高频冲突品牌自动生成 override 规则，回归到 source YAML 里固化。

## 6. 迁移步骤

| # | Alembic | 动作 |
|---|---------|------|
| 1 | 0008 | `msrp_trim` 扩字段、建 `msrp_trim_price` / `feature_dictionary` / `trim_feature_catalog` / `mv_trim_feature_diff` |
| 2 | 0009 | `msrp_source` 加 `tier` + `source_category`，历史数据 batch update |
| 3 | 0010 | 废弃旧 `msrp_current_price`（保留 view 兼容期 30 天） |
| 4 | — | `run_msrp_reconciliation.py` 新脚本，systemd timer 每日 04:00 跑 |
| 5 | — | 前端 SpecificationPage / VersionComparisonPage 加 feature diff 面板 |

## 7. 验收

- reconciliation 脚本 dry-run 全量 ≤ 10 min。
- 7 国 Top 30 SUV 中 **≥ 80%** 的 trim 落到 `msrp_trim_price.is_primary=true`。
- Feature diff 物化视图刷新 ≤ 2 min。
- 前端 Spec compare 首次 render ≤ 1 s（单 trim 约 150 行 features）。
- 零自动发布错误（tier1 冲突 > 3% 时必须 100% 走 review）。

## 8. 已知风险

- **trim_slug 归一化**：同一款 RAV4 Active Plus 在不同国家官网命名不同（"Active Plus" vs "Active Business Plus"）——需要 `trim_alias` 辅助表映射。
- **feature_key 命名**：必须团队维护一份术语表，避免 `heads_up_display` / `hud` / `head_up_display` 三个 key 并存。初期由 reviewer 在 UI 选择已存在的 key，禁止自由输入。
- **package 价格**：同一个 package 在不同 trim 上可能 include 或 extra-cost，需要 `trim_feature_catalog.package_name` + `option_price` 一起记录，不能只记"属于哪个 package"。
