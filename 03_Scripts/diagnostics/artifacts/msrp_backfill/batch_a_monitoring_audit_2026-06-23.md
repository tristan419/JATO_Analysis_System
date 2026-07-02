# Batch A MSRP Monitoring Backfill And Launch Alert Audit

Generated: 2026-06-23

Source of truth:

- Batch config: `07_ScrapingToolkit/msrp_batches/batch_a.yaml`
- Monitor API: `/v1/msrp/monitoring/events?window_days=30&threshold_pct=0&limit=500&mode=live`
- DB schema: `msrp.current_prices`, `msrp.price_history`, `msrp.observations`

## Batch A Coverage

| Code | Country | Status | Current rows | Price history rows | Closed periods | Backfill periods | Launch candidates |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| SE | Sweden | backfilled | 18 | 20 | 2 | 2 | 16 |
| FI | Finland | not_loaded | 0 | 0 | 0 | 0 | 0 |
| NO | Norway | not_loaded | 0 | 0 | 0 | 0 | 0 |
| DK | Denmark | history_without_backfill | 5 | 14 | 9 | 0 | 0 |
| HU | Hungary | not_loaded | 0 | 0 | 0 | 0 | 0 |
| HR | Croatia | not_loaded | 0 | 0 | 0 | 0 | 0 |
| AT | Austria | current_only | 74 | 74 | 0 | 0 | 74 |
| CZ | Czechia | not_loaded | 0 | 0 | 0 | 0 | 0 |
| DE | Germany | backfilled | 23 | 34 | 11 | 2 | 16 |
| FR | France | not_loaded | 0 | 0 | 0 | 0 | 0 |
| IT | Italy | not_loaded | 0 | 0 | 0 | 0 | 0 |
| PL | Poland | not_loaded | 0 | 0 | 0 | 0 | 0 |

Summary:

- Loaded countries: 4 / 12
- Countries with historical monitoring periods: 3 / 12
- Countries with explicit historical backfill evidence: 2 / 12
- Total launch baseline candidates: 90 in the 30-day monitor window, including 16 Sweden launch baselines

## Interpretation

- Sweden has real official historical backfill for Skoda ENYAQ and Volvo EX90, plus 16 current-only launch baselines that should be sampled as new product launch price alerts.
- Germany already has explicit historical backfill evidence in the current DB and should be audited next to confirm evidence quality before it is treated the same as Sweden's official evidence pack.
- Denmark has historical price periods but no explicit backfill evidence flag, so it is history monitoring rather than backfilled evidence.
- Austria has current MSRP coverage but no closed historical periods yet; its 74 rows should be treated as launch baseline alerts until a second scrape confirms unchanged or moved prices.
- FI / NO / HU / HR / CZ / FR / IT / PL are not loaded in the current MSRP DB snapshot, despite being in Batch A.

## Monitor Page Behavior

The existing `/market/msrp-monitor` page now exposes:

- Batch A monitoring coverage chips for all 12 countries.
- Launch alert cards for current-only price baselines.
- Existing price-drop chart, source evidence, country drilldown, and floating deck controls.

No separate view was added.
