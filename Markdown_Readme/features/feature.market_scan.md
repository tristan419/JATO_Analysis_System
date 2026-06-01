# Market Scan

## Status
implemented

## Category
backend · source: git_commit

## Summary
feat: Mega Menu navigation + Dashboard all-countries default + backend fixes
Navigation: 4-module mega menu (Dashboard, Market Scan, Product Deck, Data Ops)
- Market Scan: simple dropdown with 5 sub-items
- Product Deck: 2-column mega menu (Pricing/Positioning + Product/Customer)
- Data Ops: 2-column mega menu (Data View + Data Workflow)
- Copilot: moved from top nav to AI icon in user area
- AdminToolsNav: removed, links merged into Data Ops mega menu
- Mobile: hamburger drawer with accordion sections

Customer Insight: merged Customer + Hybrid into single page with
Cus

## Endpoints

- `POST /login`
- `POST /register`
- `GET /me`
- `PATCH /me/profile`
- `POST /logout`
- `GET /users`
- `PATCH /users/{user_id}/role`
- `PATCH /users/{user_id}/profile`
- `PATCH /users/{user_id}/toggle-active`
- `POST /role-upgrade/request`
- `POST /query`
- `POST /time-series`
- `POST /overview`
- `GET /data-freshness`
- `POST /detail`
- `POST /detail-csv`
- `POST /time-series-grouped`
- `POST /advanced-chart`
- `POST /model-versions`
- `POST /positioning-map`
- `GET /overview`
- `GET /pipeline-health`
- `GET /source-quality`
- `GET /cost`
- `GET /code-audit`
- `GET /proposals`
- `GET /gaps`
- `GET /features`
- `GET /toolchain`
- `GET /architecture`

## Backend

- config.py: MARKET_SCAN_CACHE_SCHEMA_VERSION 2→3 to invalidate server Redis cache
- market_scan_cache.py: build_deck_cache_key now includes trend_window_months, origin_window_months, body_window_months, drilldown_segment in cache key
- market_scan_service.py: both Redis cache call sites now pass the 4 additional params
- test_market_scan_service.py: fixed broken _get_redis_client_safe → get_redis_client patch
- 06_AppPlatform/backend/app/core/config.py
- 06_AppPlatform/backend/app/infra/parquet_repository.py
- 06_AppPlatform/backend/app/services/market_scan_cache.py
- 06_AppPlatform/backend/app/services/market_scan_service.py
- 06_AppPlatform/backend/app/services/query_service.py
- 06_AppPlatform/backend/tests/unit/test_market_scan_service.py
- 06_AppPlatform/backend/app/infra/redis_client.py
- 06_AppPlatform/backend/tests/integration/test_api_contracts.py
- 06_AppPlatform/backend/app/api/routes/auth.py
- 06_AppPlatform/backend/app/api/routes/hermes.py
- 06_AppPlatform/backend/app/api/routes/market_scan.py
- 06_AppPlatform/backend/app/api/schemas.py
- 06_AppPlatform/backend/app/core/security.py
- 06_AppPlatform/backend/app/services/country_chat_service.py
- 06_AppPlatform/backend/app/services/insight_card_service.py
- 06_AppPlatform/backend/app/api/routes/analysis.py
- 06_AppPlatform/backend/app/api/routes/filters.py
- 06_AppPlatform/backend/app/api/routes/metadata.py
- 06_AppPlatform/backend/app/services/coc_match_service.py
- 06_AppPlatform/backend/tests/unit/test_coc_match_service.py
- 06_AppPlatform/backend/app/services/jato_monthly_update_service.py
- 06_AppPlatform/backend/tests/unit/test_jato_monthly_update_service.py
- 06_AppPlatform/backend/tests/unit/test_market_scan_cache.py
- 06_AppPlatform/backend/app/services/hermes_sentinel_service.py
- 06_AppPlatform/backend/tests/unit/test_hermes_routes.py
- 06_AppPlatform/backend/app/core/startup_validation.py
- 06_AppPlatform/backend/app/services/hermes_deploy_status_service.py
- 06_AppPlatform/backend/app/services/hermes_ops_runner_service.py
- 06_AppPlatform/backend/tests/audit_metric_correctness.py
- 06_AppPlatform/backend/tests/unit/test_hermes_deploy_status_service.py
- 06_AppPlatform/backend/tests/unit/test_hermes_sentinel.py
- 03_Scripts/deploy/systemd/jato-fullstack-backend.env.example
- 06_AppPlatform/backend/app/main.py
- 06_AppPlatform/backend/app/services/hermes_devsync_service.py
- 06_AppPlatform/backend/tests/unit/test_hermes_devsync.py

## Frontend

- 06_AppPlatform/frontend/src/App.tsx
- 06_AppPlatform/frontend/src/components/Layout.tsx
- 06_AppPlatform/frontend/src/contexts/SharedFilterScopeContext.tsx
- 06_AppPlatform/frontend/src/index.css
- 06_AppPlatform/frontend/src/pages/CrudPage.tsx
- 06_AppPlatform/frontend/src/pages/DataManagementPage.tsx
- 06_AppPlatform/frontend/src/pages/EngineeringConfigPage.tsx
- 06_AppPlatform/frontend/src/pages/EngineeringPage.tsx
- 06_AppPlatform/frontend/src/pages/JatoMonthlyUpdatePage.tsx
- 06_AppPlatform/frontend/src/pages/MsrpPage.tsx
- 06_AppPlatform/frontend/src/components/MegaMenu.tsx
- 06_AppPlatform/frontend/src/contexts/AuthContext.tsx
- 06_AppPlatform/frontend/src/dashboardFilters.ts
- 06_AppPlatform/frontend/src/pages/MarketScanPage.tsx
- 06_AppPlatform/frontend/src/pages/dashboardHelpers.ts
- 06_AppPlatform/frontend/src/tests/unit/marketScanPageState.test.ts
- 06_AppPlatform/frontend/src/types/index.ts
- 06_AppPlatform/frontend/src/utils/filterOptions.ts
- 06_AppPlatform/frontend/src/pages/PositioningPricingPage.tsx
- 06_AppPlatform/frontend/src/pages/VersionComparisonPage.tsx
- 06_AppPlatform/frontend/src/hooks/useFuelChipClick.ts
- 06_AppPlatform/frontend/src/components/ExportPanel.tsx
- 06_AppPlatform/frontend/src/components/deckControls/DebouncedNumberInput.tsx
- 06_AppPlatform/frontend/src/components/deckControls/DeckControlTabs.tsx
- 06_AppPlatform/frontend/src/components/deckControls/DeckExportDrawer.tsx
- 06_AppPlatform/frontend/src/components/deckControls/DeckFloatingDrawer.tsx
- 06_AppPlatform/frontend/src/components/deckControls/index.ts
- 06_AppPlatform/frontend/src/utils/colors.ts
- 06_AppPlatform/frontend/src/utils/pageNavigation.ts
- 06_AppPlatform/frontend/src/api/client.ts
- 06_AppPlatform/frontend/src/pages/CocMatchPage.tsx
- 06_AppPlatform/frontend/src/pages/AccessControlPage.tsx
- 06_AppPlatform/frontend/src/pages/CountrySetupPage.tsx
- 06_AppPlatform/frontend/src/utils/jatoCountries.ts
- 06_AppPlatform/frontend/src/types/hermes.ts
- 06_AppPlatform/frontend/scripts/write_build_meta.cjs
- 06_AppPlatform/frontend/package.json
- 06_AppPlatform/frontend/src/components/DeckSubpageNav.tsx
- 06_AppPlatform/frontend/src/tests/unit/deckSubpageNav.test.tsx

## Tests

- **backend**: 54 passed (market_scan + cache + deck)
- **frontendTsc**: not run
- **frontendBuild**: not run
- **frontendVitest**: not run
- **backendPytest**: 5 passed (tests/integration/test_api_contracts.py)
- **frontend**: npm run check:types && npm run build
- **compile**: PYTHONPATH=. ../../.venv/bin/python -m py_compile app/services/market_scan_cache.py app/services/market_scan_service.py app/services/jato_monthly_update_service.py
- **jsonl**: hermes/dev_events/dev_events.jsonl and hermes/evidence_ledger.jsonl valid

## Linked Dev Events

- `dev_evt_20260516_002`
- `dev_evt_20260516_122928_8df87f`
- `dev_evt_20260516_071246_cf7c72`
- `dev_evt_20260518_010411_331753`
- `dev_evt_20260518_012715_a1b2c3`
- `dev_evt_20260528_061720_2bca16`
- `dev_evt_20260527_070400_84c642`
- `dev_evt_20260527_063828_5687e5`
- `dev_evt_20260526_042207_c429f4`
- `dev_evt_20260525_050249_4bea9c`
- `dev_evt_20260525_045749_df69c4`
- `dev_evt_20260525_032159_006a4c`
- `dev_evt_20260525_031510_803464`
- `dev_evt_20260525_030110_9f9d68`
- `dev_evt_20260525_013357_4e9234`
- `dev_evt_20260525_010353_30420d`
- `dev_evt_20260521_035526_d9beec`
- `dev_evt_20260521_035355_427158`
- `dev_evt_20260521_035118_marketscan_cache`
- `dev_evt_20260521_025707_a340be`
- `dev_evt_20260519_160805_cefbac`
- `dev_evt_20260518_105214_7ac590`
- `dev_evt_20260518_075835_9e198b`
- `dev_evt_20260518_071315_9c42b4`
- `dev_evt_20260518_065247_86c39f`
- `dev_evt_20260518_015944_60fde5`
- `dev_evt_20260518_015712_09f43c`
- `dev_evt_20260518_015438_704a7b`
- `dev_evt_20260518_015302_666396`

## Docs

- Markdown_Readme/features/feature.market_scan.md

## Risks

- Server must be redeployed for cache schema version bump to take effect
- Auto-generated dev event from git commit — tests not auto-verified.
- prior_rolling12_columns derived from current rolling12 shift means window sizes may differ by 1-2 months when data gaps exist, but avoids entire window being empty
- MarketScan first request after a real JATO publish still performs one cold compute; Redis/local warm path should be fast after that.

## Next Steps

- Redeploy to server → old ms:deck:v2:* cache keys become stale, v3 keys used
- Compare local vs server Sweden March data after redeploy
- Monitor Redis cache hit rate on server
- Run tests
- Verify in Hermes UI Dev tab
- Wire contract tests into CI diagnostics
- Expand only when a regression repeats
- Verify in UI: drill-down fuel panel YoY values no longer all New
- Trigger Hermes DevSync
- Deploy and verify /v1/hermes/deploy/status
- After next JATO publish, inspect job publication.cacheInvalidation and Hermes evidence ledger

*Auto-generated by Hermes DevSync. Last updated: 2026-05-29T06:03:33.840023+00:00*
