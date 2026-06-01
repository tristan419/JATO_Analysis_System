# Hermes DevSync

## Status
implemented

## Category
governance · source: git_commit

## Summary
hermes: full automation loop — sync auth, hooks, workspace health
Backend:
- POST /hermes/dev/sync: Bearer token auth + idempotency (commitSha/runId)
- GET /hermes/dev/workspace-health: detects uncommitted code without dev events

GitHub Actions (hermes-devsync.yml):
- After generating dev event, calls remote DevSync API with token
- Non-blocking — warns if server unreachable, never fails CI

Git hooks (.githooks/):
- pre-commit: warns if code changed but dev_events.jsonl not updated
- post-commit: auto-generates commit-level dev event from git diff

Local gua

## Endpoints

- `POST /hermes/dev/events`
- `GET /hermes/dev/events`
- `POST /hermes/dev/sync`
- `GET /hermes/dev/features`
- `GET /hermes/dev/features/{featureId}`
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
- `GET /v1/hermes/pipeline/status`
- `GET /v1/hermes/pipeline/status/{pipelineId}`

## Backend

- 06_AppPlatform/backend/app/services/hermes_devsync_service.py
- 06_AppPlatform/backend/tests/unit/test_hermes_devsync.py
- hermes_devsync_service.py with full sync pipeline
- 5 new dev endpoints in hermes.py
- 06_AppPlatform/backend/app/services/hermes_chat_service.py
- 06_AppPlatform/backend/tests/unit/test_hermes_chat.py
- 06_AppPlatform/backend/app/core/config.py
- 06_AppPlatform/backend/requirements.txt
- 06_AppPlatform/backend/app/api/routes/hermes.py
- Sentinel notifications include pipeline, statusRecord, lastRunAt, failedCount, warningCount, artifactRefs, pipelineStatus, and statusPath context
- FastAPI Hermes router exposes pipeline status endpoints
- Hermes pipeline status service reads standard JSON, legacy scheduled status, source quality report, and JATO monthly update job_state
- Sentinel pipeline probe classifies standard status records
- JATO monthly update writes jato_etl status on job success/failure
- Added curated evidence seed for feature.current_price and feature.review_workbench
- DevSync hydration now works even when Kanban runtime registry is empty or damaged
- /hermes/features overlays curated docs/tests/backendApis for the two seeded legacy features
- DevSync list_features hydrates missing docs/tests from curated Kanban feature registry
- _sync_to_kanban preserves curated docs/tests/backendApis/knownIssues when DevSync event fields are empty
- Current Price feature registry now records backend and frontend test references
- Reused the existing _is_noisy_commit_feature_id path for feature normalization, gap creation, and gap retirement
- DevSync records expected deploy commits
- Deploy workflow writes hermes/deploy_release.json into the archive
- Sentinel deploy probe compares release and expected commits
- Notification records include action level, blocking flag, recommended action, and mailbox states

## Frontend

- Dev subtab with feature registry table
- Dev events feed with changed file counts
- Missing governance items (No Docs/Tests/Evidence/Gaps)
- Feature detail modal
- Sync Now button
- 06_AppPlatform/frontend/src/components/HermesAskResponseCard.tsx
- DataManagementPage.tsx: Sentinel Inbox notification cards render compact pipeline detail fields from notification.context
- Sentinel Inbox uses unread/read/archive/all filters with search and fixed-height scrolling
- Hermes diagrams can be filtered by category
- Hermes full design document is readable in a collapsible bottom panel

## Tests

- **backend**: 101 Hermes tests pass (29 devsync + 36 chat + 36 routes)
- **frontendTsc**: clean
- **frontendBuild**: succeeds (DataManagementPage 95kB)
- **frontendVitest**: 5/5 devsync types pass
- **ciContract**: backend compile and tests/integration/test_api_contracts.py remain blocking in ci.yml
- **frontend**: npm run check:types passed; npm run build passed
- **syntax**: py_compile passed for hermes_devsync_service.py; feature_registry.yaml parses
- **ops**: 2 passed (VOC failure summary helper)
- **backendPytest**: 82 passed (test_hermes_devsync.py, test_hermes_sentinel.py, test_hermes_routes.py)
- **pyCompile**: hermes_devsync_service.py compiled
- **backendHermesPytest**: 85 passed
- **workflowYaml**: deploy-fullstack-tencent.yml and hermes-devsync.yml parsed
- **shellSyntax**: deploy_fullstack_server.sh bash -n passed

## Linked Dev Events

- `dev_evt_20260516_101640_0a45d6`
- `dev_evt_20260516_101615_9187bd`
- `dev_evt_20260516_084652_b49e9d`
- `dev_evt_20260516_043306_eaf36c`
- `dev_evt_20260515_002`
- `dev_evt_20260515_154843_f46cad`
- `dev_evt_20260515_114917_6765da`
- `dev_evt_20260515_112609_6c993d`
- `dev_evt_20260515_104148_1d371a`
- `dev_evt_20260518_010516_2813cf`
- `dev_evt_20260524_151130_392b50`
- `dev_evt_20260521_010458_pipeline_inbox_detail`
- `dev_evt_20260521_005355_pipeline_status`
- `dev_evt_20260520_191405_0af5b7`
- `dev_evt_20260520_devsync_curated_seed_overlay`
- `dev_evt_20260520_190732_1e0a34`
- `dev_evt_20260520_devsync_preserve_curated_tests`
- `dev_evt_20260520_061437_771e4c`
- `dev_evt_20260520_devsync_auto_event_noise_filter`
- `dev_evt_20260519_deploy_sentinel_inbox`

## Docs

- Markdown_Readme/features/hermes-devsync.md

## Risks

- Auto-generated dev event from git commit — tests not auto-verified.
- Feature ID inference from title is heuristic-based
- No git hook or CI/CD integration yet
- DevSync requires manual trigger (Sync Now button or API call)
- Frontend diagnostics can still fail visibly inside the job logs; they no longer fail the workflow conclusion.
- Existing notifications emitted before this change will not have enriched context until Sentinel emits a fresh pipeline notification.
- Existing runtime pipelines must run once to create fresh standard status files for MSRP dryrun/ingest; Hermes still falls back to legacy scheduled status until then.
- The seed is intentionally narrow; broader canonical feature evidence should still live in registries and dev events.
- Production DevSync runtime registry will stop warning after this deploy because Sentinel can hydrate tests from Kanban; any genuinely missing tests still remain visible.
- Only auto-generated bookkeeping featureIds are filtered; real Hermes feature ids such as hermes-devsync and hermes-chat-gateway remain registered.
- Server must be redeployed before production has deploy_release.json and the new deploy probe
- If both deploy and DevSync cannot reach Tencent, server-side Hermes cannot infer the latest GitHub commit

## Next Steps

- Run tests
- Verify in Hermes UI Dev tab
- Add git post-commit hook for auto dev event generation
- Integrate with GitHub Actions CI/CD
- Add stale feature detection
- Wire Claude Code session end hook to auto-write dev events
- Restore specific frontend/typecheck gates after pre-existing failures are fixed
- Keep deploy workflows blocking
- Deploy and verify Sentinel Inbox pipeline notifications show details on cloud
- Deploy to Tencent cloud
- Verify /v1/hermes/pipeline/status and Sentinel pipeline findings
- Deploy and verify Sentinel devsync missing_tests clears online
- Deploy and verify /v1/hermes/sentinel/status no longer reports feature.current_price or feature.review_workbench missing_tests
- Deploy and run DevSync once so production Sentinel retires existing auto-event missing_tests gaps
- Deploy to Tencent
- Verify /v1/hermes/deploy/status
- Verify Sentinel Inbox receives production_commit_drift when expected and release diverge

*Auto-generated by Hermes DevSync. Last updated: 2026-05-29T06:03:38.072523+00:00*
