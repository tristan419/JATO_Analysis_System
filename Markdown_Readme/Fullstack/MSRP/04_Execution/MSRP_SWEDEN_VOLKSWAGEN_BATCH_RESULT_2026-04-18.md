# Sweden Volkswagen MSRP Batch Result

日期：2026-04-18

## 范围

本轮收口的是 Sweden Volkswagen configurator 批次里的两项新增落地：

- ID.4：切到真实 configurator trim-overview flow，并接入 maintained seed + overlay 验证
- Tayron：切到 trim -> engine-card Playwright flow，并纳入 maintained seed 的稳定 apply 流程

同时补了一次 seed apply 的稳定性加固：

- official source file extract 加有限重试，吸收 Volkswagen configurator 的瞬时抖动
- explicit link seed apply 增加 stale active row reconcile，防止同一 `SEED_LABEL` 下旧 link 残留

## 最终状态

### 1. Sweden ID.4

- official current prices：10 条官网行
- Current Prices deck 可见：11 trims
- 其中 10 条来自 `volkswagen_id_4_se_draft_scrapling`，另 1 条是 EVKX 第三方参考价
- Pricing deck 验证：Sweden + 2025-12 + rolling12 + BEV + SUV-A 下，ID.4 bubble 与价格区间正常
- stale seed link：旧 `PRO EDITION -> Pro` active row 已停用

### 2. Sweden Tayron

- standalone dry-run：14/14 valid
- official current prices：14 条官网行
- Current Prices deck 可见：14 trims
- 所有 Tayron 当前价均来自 `volkswagen_tayron_se_draft_scrapling`

### 3. Seed apply / overlay

- maintained seed `--apply` 最终成功
- Sweden current prices touched：55
- Germany current prices touched：31
- link rows upserted：81
- Positioning Pricing overlay 验证通过，ID.4 已在 Sweden SUV-A pricing page 被 current-price overlay 吃入

## 前端核验结论

### Pricing

- `08 Pricing` 的 Sweden + 2025-12 + rolling12 + BEV + SUV-A 页面里，Lead Model 为 ID.4
- ID.4 气泡存在，最低/最高 MSRP 区间落在本轮官网价格范围内

### MSRP

- `06 MSRP` 的 Sweden / VOLKSWAGEN / ID.4 分组显示正常
- `06 MSRP` 的 Sweden / VOLKSWAGEN / TAYRON 分组显示 14 trims，未回退

## Sweden Volkswagen top30 状态

`suv_only_country_model_top30/se/` 下的 Volkswagen 候选当前只有四个：

- `09_volkswagen_tiguan_se.yaml`
- `10_volkswagen_id_4_se.yaml`
- `12_volkswagen_t_roc_se.yaml`
- `23_volkswagen_tayron_se.yaml`

结论：Sweden Volkswagen top30 候选已经全部覆盖到 configurator-capable draft 范围内，本轮之后不存在“下一个未落地的 Sweden Volkswagen 候选”。

## 关联提交

- Git commit：`fc2c91c06ae94a40b2b6fd176f130809ec96fbc3`
- Title：`Roll out Sweden Volkswagen Tayron and ID.4 MSRP seeds`

## 后续建议

1. Sweden Volkswagen 不再继续横向扩 batch，除非 candidate scope 新增模型或官网 configurator 结构变化。
2. 后续 Volkswagen 工作优先切到其它国家 backlog，而不是重复回到 Sweden 已验证模型。
3. 如果再次做 maintained seed rollout，默认保留 source extract retry + stale link reconcile 逻辑，不回退到单次 apply 假设。
