# Country Copilot Intelligence Implementation 2026-04-15

Status: In Progress

## Current State

- Country chat intent recognition has been upgraded to clause splitting + weighted scoring.
- Static market knowledge now covers the entire user-prioritized batch A plus the original strategic markets.
- The country assistant monthly trend uses the oldest 12 monthly columns for filtered queries instead of the latest 12 months.
- CountryChatAnalysisDeck uses an index-based palette for powertrain views, so BEV/HEV/MHEV/PHEV/ICE colors drift with ranking order.
- News crawling now has a lightweight RSS/Atom batch foundation, but it is not yet persisted into the app platform or database.

## Target State

- Country chat always answers from the latest rolling 12 months for monthly trend views.
- Powertrain visuals use a fixed canonical palette across ranking and stacked charts.
- Country market context starts covering the user-prioritized batch, beginning with Finland.
- A documented execution sequence exists for country knowledge, news collection, and chart data completeness fixes.
- CountryChatAnalysisDeck explains empty sections explicitly instead of silently dropping them.

## Affected Files

| File | Change Type | Dependencies |
|------|-------------|--------------|
| 06_AppPlatform/backend/app/infra/parquet_repository.py | modify | blocks latest-month trend fix |
| 06_AppPlatform/frontend/src/components/CountryChatAnalysisDeck.tsx | modify | depends on colors.ts palette helpers |
| 06_AppPlatform/frontend/src/utils/colors.ts | reference only | canonical powertrain palette source |
| 06_AppPlatform/backend/app/services/country_profiles.py | modify | blocks Finland market-context coverage |
| 06_AppPlatform/backend/tests/unit/test_country_chat_service.py | modify | validates Finland profile aliases |
| 06_AppPlatform/backend/tests/unit/test_parquet_repository.py | create | validates latest-12 month behavior |
| 07_ScrapingToolkit/jato_scraper/news_base.py | create | news batch/article types |
| 07_ScrapingToolkit/jato_scraper/news_config_loader.py | create | loads batch YAML configs |
| 07_ScrapingToolkit/jato_scraper/news_runner.py | create | RSS/Atom batch fetch runner |
| 07_ScrapingToolkit/news_sources/batch_a.yaml | create | first-batch country news feeds |
| 07_ScrapingToolkit/news_sources/batch_b.yaml | create | remaining data-country news feeds |
| 07_ScrapingToolkit/tests/test_news_runner.py | create | validates config loading and RSS parsing |

## Execution Plan

### Phase 1: Immediate Fixes

- [x] Step 1.1: Write down implementation plan in this markdown document.
- [x] Step 1.2: Fix monthly trend selection to use the latest rolling 12 months for filtered country snapshots.
- [x] Step 1.3: Fix CountryChatAnalysisDeck powertrain colors to use canonical mappings instead of positional palette order.
- [x] Step 1.4: Add Finland market profile so Finland can use policy/hot-topic context immediately.
- [x] Verify: backend unit tests + frontend type/build checks.

### Phase 2: First Country Batch Knowledge Expansion

- [x] Step 2.1: Extend country_profiles.py to the remaining user-prioritized batch A countries.
- [x] Step 2.2: Add alias coverage tests for FI/HU/CZ/SK/HR/SI/AT/CH/RO/GR/DK.
- [x] Verify: country chat unit tests and manual snapshot inspection for covered countries.

### Phase 3: News Intake Foundation

- [x] Step 3.1: Add a news observation schema and a lightweight batch runner under 07_ScrapingToolkit.
- [x] Step 3.2: Define batch A and batch B country source configuration for news ingestion.
- [ ] Step 3.3: Persist normalized news results for the app platform to consume.
- [x] Verify: config loading and RSS parsing tests pass for the new toolkit foundation.

### Phase 4: Data Completeness and UI Fallbacks

- [ ] Step 4.1: Audit empty cards in CountryChatAnalysisDeck by data dependency.
- [x] Step 4.2: Hide empty sections or render explicit no-data reasons instead of silent gaps.
- [ ] Step 4.3: Align PM workbench charts with current-year and selected-model scope rules.
- [x] Verify: frontend type checks, unit tests, and build pass after section-level empty-state changes.

## Rollback Plan

If something fails:

1. Revert parquet_repository.py latest-month selection to the prior slice behavior.
2. Revert CountryChatAnalysisDeck.tsx to the existing PALETTE-only rendering path.
3. Remove newly added country profiles if any profile content causes unexpected regressions.

## Risks

- Precomputed yearMonth summaries may still need chronological normalization outside filtered country snapshots.
- Some workbench charts remain empty because source MSRP fields are genuinely missing, not because of rendering bugs.
- News scraping needs a separate normalization schema; MSRP extractors cannot be reused unchanged.

## Started Changes

- 2026-04-15: Started implementation with latest-month trend fix, fixed powertrain palette usage, and Finland profile coverage.
- 2026-04-15: Expanded batch A market profiles, added toolkit RSS/Atom news foundation with batch configs, and changed CountryChat deck empty sections into explicit reason panels.