# Jato Monthly Update

## Status
implemented

## Category
backend · source: git_commit

## Summary
feat: JATO monthly update — publish blocker classification + resolution panels + info flowchart
- 2026-07-21: routine country updates now treat active history as immutable by default and require an explicit per-country historical decision when the uploaded workbook differs.
- `keep_active` preserves active rows through each country's active latest month and uses uploaded rows only after that boundary.
- `use_latest` is limited to explicit historical reclassification when historical monthly sales totals remain stable; historical sales changes remain blocked from the routine fast path.
- Review approval and Publish fail closed when decisions, validation coverage, fingerprints, country partitions, or historical guards are incomplete.
- Backend: publish_jato_monthly_update_job() now returns structured 409
  detail for country_regression (regressions[]) and sales_doubling
  (anomalies[]) instead of flat string messages
- Frontend types: new PublishBlocker, PublishCountryRegression,
  PublishSalesDoublingAnomaly, PublishSalesDoublingSampleMonth interfaces
- Frontend API: readErrorMessage preserves object detail as JSON string
- Frontend page: Resolution Panel renders in Job Detail when publish is
  blocked — country regression

## Endpoints

- `POST /monthly-update-jobs`
- `POST /monthly-update-jobs/from-upload`
- `POST /monthly-update-jobs/single-country`
- `GET /monthly-update-jobs`
- `GET /monthly-update-jobs/{job_id}`
- `POST /monthly-update-jobs/{job_id}/retry`
- `POST /monthly-update-jobs/{job_id}/recheck`
- `POST /monthly-update-jobs/{job_id}/cancel`
- `GET /monthly-update-jobs/{job_id}/review`
- `POST /monthly-update-jobs/{job_id}/historical-reclassification-resolution`
- `POST /monthly-update-jobs/{job_id}/review-approval`
- `POST /monthly-update-jobs/{job_id}/publish`
- `POST /v1/msrp/monthly-update-jobs/{job_id}/recheck`
- `POST /v1/msrp/monthly-update-jobs/{job_id}/cancel`
- `POST /monthly-update-jobs/{job_id}/rollback`
- `POST /monthly-update-jobs/{job_id}/smart-merge`
- `GET /overview`
- `GET /pipeline-health`
- `GET /pipeline/status`
- `GET /pipeline/status/{pipeline_id}`
- `GET /source-quality`
- `GET /cost`
- `GET /msrp-country-progress`
- `GET /msrp-dryrun-history`
- `GET /code-audit`
- `GET /proposals`
- `GET /v1/hermes/pipeline/status`
- `GET /v1/hermes/pipeline/status/{pipelineId}`
- `POST /v1/msrp/monthly-update-jobs/{job_id}/smart-merge`
- `POST /v1/msrp/monthly-update-jobs/single-country`
- `POST /monthly-update-uploads/initiate`

## Backend

- 06_AppPlatform/backend/app/services/jato_monthly_update_service.py
- 06_AppPlatform/backend/app/db/models.py
- 06_AppPlatform/backend/alembic/versions/20260521_0019_order_genius_price_dimension_mapping.py
- 06_AppPlatform/backend/scripts/seed_order_genius_rules.py
- 06_AppPlatform/backend/app/api/routes/msrp_monthly_update.py
- 06_AppPlatform/backend/tests/unit/test_jato_monthly_update_routes.py
- 06_AppPlatform/backend/tests/unit/test_jato_monthly_update_service.py
- JATO monthly update service now persists currentProcess for subprocesses, rechecks stale running jobs, terminates process groups on cancel, and keeps cancelled jobs outside the publish path.
- 06_AppPlatform/backend/app/services/market_scan_cache.py
- 06_AppPlatform/backend/app/services/market_scan_service.py
- 06_AppPlatform/backend/app/api/routes/hermes.py
- 06_AppPlatform/backend/app/services/hermes_sentinel_service.py
- 06_AppPlatform/backend/tests/unit/test_hermes_routes.py
- 06_AppPlatform/backend/tests/unit/test_hermes_sentinel.py
- 06_AppPlatform/backend/app/services/hermes_pipeline_status_service.py
- 06_AppPlatform/backend/tests/unit/test_hermes_pipeline_status_service.py
- FastAPI Hermes router exposes pipeline status endpoints
- Hermes pipeline status service reads standard JSON, legacy scheduled status, source quality report, and JATO monthly update job_state
- Sentinel pipeline probe classifies standard status records
- JATO monthly update writes jato_etl status on job success/failure
- Added _smart_merge_dataframes() — merges active+candidate at country level, keeping regressed countries from active
- Added _run_smart_merge() — background thread that creates merged parquet then rebuilds partitions/manifest/fingerprint
- Added create_smart_merge_candidate() — public API with validation guards (no double-merge, only for success/completed jobs)
- Added POST /v1/msrp/monthly-update-jobs/{job_id}/smart-merge route with editor-level auth
- Created rebuild_from_parquet.py helper script called as subprocess to rebuild derived artifacts
- create_single_country_job() — lightweight job creation with country/month metadata
- _run_single_country_job() — background runner: validate country, skip prepare/compare, run refresh with supplement
- Upload-time validation: reject if uploaded month <= active latest for that country
- POST /v1/msrp/monthly-update-jobs/single-country route
- Historical Review reports `decisionRequired`, `allowedDecisions`, monthly-total stability, and country-level differences.
- `_keep_active_history_country_frame()` reuses active history through the active latest month and accepts candidate rows only after that boundary.
- Smart Merge validates non-overlapping month boundaries and rebuilds the full candidate without changing untouched-country partitions.
- Approval requires exact `keep_active` validation coverage and rejects missing, failed, duplicated, or extra validation records.
- Publish retains live historical sales/configuration, fingerprint, duplicate, regression, and suspected-accumulation hard gates.

## Frontend

- 06_AppPlatform/frontend/src/api/client.ts
- 06_AppPlatform/frontend/src/index.css
- 06_AppPlatform/frontend/src/pages/JatoMonthlyUpdatePage.tsx
- 06_AppPlatform/frontend/src/types/index.ts
- 06_AppPlatform/frontend/src/tests/unit/jatoMonthlyUpdate.test.ts
- 06_AppPlatform/frontend/src/utils/jatoMonthlyUpdate.ts
- JATO Monthly Update Job Detail now shows 刷新查验 and 终止任务 controls plus runtime process/recheck details.
- 06_AppPlatform/frontend/src/pages/MarketScanPage.tsx
- Added Smart Merge button to country_regression blocker panel with loading/complete states
- Added hasSmartMerge state variable to disable re-merge after completion
- Updated info section to remove '功能开发中' label for Smart Merge
- Checkbox on existing upload form to enable single-country quick mode with country/month fields
- Historical Review renders only backend-authorized decisions; it does not auto-select a replacement policy.
- Countries with unstable historical monthly totals show `use_latest` as locked and require `keep_active` in the routine flow.
- A resolved `keep_active` decision is shown as safe only after matching backend `resolutionValidation=pass`; otherwise approval remains locked.

## Tests

- **backendPytest**: 57 passed (test_jato_monthly_update_service.py, test_market_scan_service.py, test_parquet_repository.py)
- **backend**: 31/31 jato tests pass
- **frontend**: npm run check:types && npm run build
- **syntax**: py_compile passed; bash -n run_msrp_low_concurrency.sh passed
- **jsonl**: hermes/dev_events/dev_events.jsonl and hermes/evidence_ledger.jsonl valid
- **compile**: PYTHONPATH=. ../../.venv/bin/python -m py_compile app/services/market_scan_cache.py app/services/market_scan_service.py app/services/jato_monthly_update_service.py
- **frontendTsc**: clean
- **frontendVitest**: 129/129 passed
- **integration**: 5/5 passed
- **2026-07-21 backend JATO suite**: 212 passed
- **2026-07-21 frontend suite**: 58 files, 310 tests passed; typecheck and production build passed

## Linked Dev Events

- `dev_evt_20260517_082454_62bb03`
- `dev_evt_20260517_082427_6191d6`
- `dev_evt_20260517_072443_e5a3c6`
- `dev_evt_20260517_072417_ebd2b6`
- `dev_evt_20260517_063812_fdf6ee`
- `dev_evt_20260517_063739_22786c`
- `dev_evt_20260517_052537_10ca5c`
- `dev_evt_20260517_052331_210c20`
- `dev_evt_20260518_004957_370678`
- `dev_evt_20260521_143417_32cda0`
- `dev_evt_20260521_143251_0987aa`
- `dev_evt_20260521_063642_f7b7d1`
- `dev_evt_20260521_062858_jato_cancel_recheck`
- `dev_evt_20260521_055134_d4545f`
- `dev_evt_20260521_051712_3992c8`
- `dev_evt_20260521_045430_6cfce1`
- `dev_evt_20260521_035712_334439`
- `dev_evt_20260521_035118_marketscan_cache`
- `dev_evt_20260521_005711_50c02c`
- `dev_evt_20260521_005355_pipeline_status`
- `dev_evt_20260520_193112_2b9fc3`
- `dev_evt_20260520_smart_merge`
- `dev_evt_20260520_single_country`
- `dev_evt_20260520_090749_6ea97f`

## Docs

- Markdown_Readme/features/feature.jato_monthly_update.md
- Markdown_Readme/Fullstack/04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md

## Risks

- Auto-generated dev event from git commit — tests not auto-verified.
- Cancel can kill tracked subprocess groups; direct in-thread validation remains best-effort until it reaches a cancellation checkpoint.
- MarketScan first request after a real JATO publish still performs one cold compute; Redis/local warm path should be fast after that.
- Existing runtime pipelines must run once to create fresh standard status files for MSRP dryrun/ingest; Hermes still falls back to legacy scheduled status until then.
- Smart Merge rebuilds partition/manifest/fingerprint via subprocess — if the rebuild script fails, the job enters smart_merge_failed phase and user must retry
- Sales-doubling remains a deeper data-integrity blocker and cannot be resolved by historical reclassification.
- The routine flow intentionally does not permit correcting changed historical sales totals. That requires an explicit high-risk historical-correction mode with a bounded country/month scope and separate approval.
- `use_latest` can deliberately change historical analysis dimensions when monthly totals are stable; the Review diff and explicit user decision are therefore mandatory audit evidence.
- No CSV upload support — only xlsx
- Upload-time month check is best-effort; publish guard still runs separately

## Next Steps

- Run tests
- Verify in Hermes UI Dev tab
- Deploy to cloud
- Use UI recheck on jato-update-dc0fd9ed, then cancel if still running before creating 2026-04-r2
- Deploy and verify /v1/hermes/deploy/status
- After next JATO publish, inspect job publication.cacheInvalidation and Hermes evidence ledger
- Deploy to Tencent cloud
- Verify /v1/hermes/pipeline/status and Sentinel pipeline findings
- Deploy and verify Smart Merge in staging with a test regression scenario
- Consider adding Smart Merge status indicator to job detail panel (e.g., 'Smart Merged' badge)
- Frontend re-upload button in blocker panel still disabled — can be future enhancement
- Deploy and test on production
- Verify subsequent batch upload behavior with previously single-country-updated countries

*Auto-generated by Hermes DevSync. Last updated: 2026-05-29T06:03:38.913411+00:00*
