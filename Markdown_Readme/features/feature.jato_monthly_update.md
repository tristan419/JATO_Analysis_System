# Jato Monthly Update

## Status
implemented

### 2026-10-29 static carry-forward collision correction (validated locally)

- Actual 4c inspection: Finland's raw advancing rows have no normalized static
  duplicates. Carry-forward fills null MSRP with active zero, creating 473
  duplicate pairs; the first failed hash bucket reports 8 rows / 4 groups.
- Fix the existing carry-forward helper to preserve uploaded null/value
  distinctions within each stable-key group. No row removal or automatic sum.
- Unify Review normalization with Smart Merge and enrich true-collision
  feedback using the existing service/error path; no new table or workflow.
- Verification: 323 monthly backend tests pass. Retained production parquet
  copies pass raw Review → bounded streaming merge → complete Review with no
  blockers (1,046,221 output rows). All four countries retain published history;
  new monthly totals match the upload. All 17 untouched country checks pass.
- Finland July/August/September remain 6,044 / 6,643 / 6,982; its 30,076 uploaded
  rows are not deleted or deduplicated by carry-forward. Local input hashes and
  server source hashes match before/after validation.
- Review schema v5 uses the existing stale-report refresh path. A failed
  Smart Merge can use its existing dedicated resume flow after code deployment;
  successful jobs with old cached Reviews require normal Review refresh.
- Pending: PR/CI and Candidate validation. Do not resume the production job or
  publish data automatically; Active promotion requires explicit authorization.
- Business rules: [data lifecycle runbook](../Fullstack/04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md#2026-10-29-preserve-uploaded-static-field-distinctions).

### 2026-10-28 month-forward correction (deployed Active 69d0d871)

- Incident: `jato-update-398aa04a` converted successfully but Review rejected
  four countries for omitted 2023 month columns (`candidate_missing_historical_months`).
- Advancing uploads need not re-export complete history. Default streaming
  Smart Merge retains active history and consumes only subsequent months.
- Review checks supplied overlap for explicit revisions; `use_latest` must also
  preserve omitted old months. Missing columns are never interpreted as zeros.
- Reuse current Review, per-country decisions, static-key matching and Smart
  Merge; no new table, migration, upload workflow or automatic publish.
- Verification: 317 monthly backend regression tests and all 648 frontend tests
  passed, including a synthetic four-country September patch missing all 2023
  columns through raw Review → streaming merge → complete Review. Frontend
  type check, production build and router regression also passed. The original
  production upload was not replayed in that code validation. PR #310 was
  deployed to Active `69d0d871`; the user subsequently rebuilt Review and ran
  Smart Merge, exposing the carry-forward collision described above. No monthly
  data was published by that deployment.
- Review schema advances to v4 using the existing stale-review / isolated
  refresh mechanism. Cached v3 conclusions cannot be reused under the new rules;
  complete candidates still must contain the preserved historical months.
- Business rules: [data lifecycle runbook](../Fullstack/04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md#2026-10-28-advance-months-without-requiring-a-full-history-export).

## Category
backend · source: git_commit

## Summary
feat: JATO monthly update — publish blocker classification + resolution panels + info flowchart
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

## Risks

- Auto-generated dev event from git commit — tests not auto-verified.
- Cancel can kill tracked subprocess groups; direct in-thread validation remains best-effort until it reaches a cancellation checkpoint.
- MarketScan first request after a real JATO publish still performs one cold compute; Redis/local warm path should be fast after that.
- Existing runtime pipelines must run once to create fresh standard status files for MSRP dryrun/ingest; Hermes still falls back to legacy scheduled status until then.
- Smart Merge rebuilds partition/manifest/fingerprint via subprocess — if the rebuild script fails, the job enters smart_merge_failed phase and user must retry
- Works only for country_regression blocker, not sales_doubling (which indicates deeper data integrity issue)
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
