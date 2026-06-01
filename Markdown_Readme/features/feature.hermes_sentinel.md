# feat(jato): add monthly update recheck and cancel controls

## Status
implemented

## Category
backend · source: claude_code

## Summary
Added probe_pipeline_failures() to Hermes Sentinel Governor, the only module that proactively alerts.

Checks:
- scheduled_fetch_status.json exists and is readable
- All 5 expected pipelines (news, voc, msrp_dryrun, msrp_ingest, jato_etl) have status entries
- Per-pipeline failure detection (critical for msrp_ingest, high for others)
- Degraded pipeline detection (medium, partial success)
- Pipeline staleness (daily: 30h threshold, weekly ingest: 192h)
- source_quality_report.json freshness (<24h)

Also added:
- Intelligence Governor daily timer: hermes-source-quality.timer (06:10 UTC)
- News Batch B systemd timer: jato-country-news-sync-b.timer (23:45 UTC)
- Systemd unit files in 03_Scripts/deploy/systemd/

## Endpoints

- `POST /v1/msrp/monthly-update-jobs/{job_id}/recheck`
- `POST /v1/msrp/monthly-update-jobs/{job_id}/cancel`

## Backend

- JATO monthly update service now persists currentProcess for subprocesses, rechecks stale running jobs, terminates process groups on cancel, and keeps cancelled jobs outside the publish path.
- 06_AppPlatform/backend/app/services/jato_monthly_update_service.py
- 06_AppPlatform/backend/app/services/market_scan_cache.py
- 06_AppPlatform/backend/app/services/market_scan_service.py
- Added curated evidence seed for feature.current_price and feature.review_workbench
- DevSync hydration now works even when Kanban runtime registry is empty or damaged
- /hermes/features overlays curated docs/tests/backendApis for the two seeded legacy features
- DevSync list_features hydrates missing docs/tests from curated Kanban feature registry
- _sync_to_kanban preserves curated docs/tests/backendApis/knownIssues when DevSync event fields are empty
- Current Price feature registry now records backend and frontend test references
- VOC runner now writes real failedCount/failureCount into scheduled_fetch_status.json
- Sentinel pipeline probe treats partial_success as degraded for backward compatibility
- Source Quality Governor treats partial_success as degraded runtime status
- Hermes source quality script now maps runtime status entries to sources and supports --write-registry
- Country news runner supports JATO_COUNTRY_NEWS_STATUS_KEY so Batch B no longer overwrites Batch A status
- Added probe_pipeline_failures() to Hermes Sentinel service
- Added focused unit coverage for pipeline failure probes
- Updated MSRP dry run and ingest scripts to write scheduled_fetch_status.json entries
- Updated deploy script to install and restart Batch B and source-quality timers

## Frontend

- JATO Monthly Update Job Detail now shows 刷新查验 and 终止任务 controls plus runtime process/recheck details.
- 06_AppPlatform/frontend/src/pages/MarketScanPage.tsx

## Tests

- **backend**: 34 passed (test_hermes_sentinel.py + test_hermes_devsync.py)
- **frontend**: npm run check:types && npm run build
- **syntax**: py_compile passed for sentinel and MSRP batch scripts; bash -n passed for deploy script
- **jsonl**: hermes/dev_events/dev_events.jsonl and hermes/evidence_ledger.jsonl valid
- **compile**: PYTHONPATH=. ../../.venv/bin/python -m py_compile app/services/market_scan_cache.py app/services/market_scan_service.py app/services/jato_monthly_update_service.py
- **ops**: 2 passed (VOC failure summary helper)
- **data**: source_registry.yaml and governance_gaps.yaml parse; source_quality_report.json parses

## Linked Dev Events

- `dev_evt_20260521_062858_jato_cancel_recheck`
- `dev_evt_20260521_035118_marketscan_cache`
- `dev_evt_20260520_devsync_curated_seed_overlay`
- `dev_evt_20260520_devsync_preserve_curated_tests`
- `dev_evt_20260520_voc_failure_tracking_closure`
- `dev_evt_20260520_source_quality_registry_scoring`
- `dev_evt_20260520_sentinel_pipeline_probes`

## Docs

- Markdown_Readme/features/feature.hermes_sentinel.md

## Risks

- Cancel can kill tracked subprocess groups; direct in-thread validation remains best-effort until it reaches a cancellation checkpoint.
- MarketScan first request after a real JATO publish still performs one cold compute; Redis/local warm path should be fast after that.
- The seed is intentionally narrow; broader canonical feature evidence should still live in registries and dev events.
- Production DevSync runtime registry will stop warning after this deploy because Sentinel can hydrate tests from Kanban; any genuinely missing tests still remain visible.
- Gap remains in_progress until the next production VOC scheduled run proves voc-failed-sources.json contains live per-source errors.
- Source quality rates remain deterministic estimates from available counts and known issues; detailed per-source crawler error types still depend on future runner output.
- Sentinel probe may generate notifications until msrp pipeline runs write status file

## Next Steps

- Deploy to cloud
- Use UI recheck on jato-update-dc0fd9ed, then cancel if still running before creating 2026-04-r2
- Deploy and verify /v1/hermes/deploy/status
- After next JATO publish, inspect job publication.cacheInvalidation and Hermes evidence ledger
- Deploy and verify Sentinel devsync missing_tests clears online
- Deploy and verify /v1/hermes/sentinel/status no longer reports feature.current_price or feature.review_workbench missing_tests
- Deploy and verify /v1/hermes/sentinel/status still reports deploy ok
- After next VOC timer run, inspect 03_Scripts/logs/voc-failed-sources.json and close gap.source_quality.voc_errors_untracked if populated
- Deploy and verify /v1/hermes/source-quality generatedAt is current
- Confirm next Batch B news run writes news_batch_b in scheduled_fetch_status.json
- Deploy and verify pipeline_failures probe appears in /v1/hermes/sentinel/status
- Confirm production systemd timers are enabled after deploy
- Verify msrp_dryrun/msrp_ingest entries appear in scheduled_fetch_status.json after next scheduled run

*Auto-generated by Hermes DevSync. Last updated: 2026-05-29T06:03:20.396435+00:00*
