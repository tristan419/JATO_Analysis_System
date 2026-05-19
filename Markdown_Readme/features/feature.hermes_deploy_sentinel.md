# Hermes Deploy Sentinel

## Purpose

Detect production version drift: GitHub `main` has advanced, but Tencent production is still running an older deployed archive because the deploy workflow failed or did not restart cleanly.

## Data Flow

1. `deploy-fullstack-tencent.yml` writes `hermes/deploy_release.json` before packaging the deploy archive.
2. Tencent deployment extracts that file with the release artifact.
3. `hermes-devsync.yml` calls `/v1/hermes/dev/sync` with the latest pushed `commitSha`.
4. Hermes records that expected commit in `hermes/deploy_expected.json`.
5. `/v1/hermes/deploy/status` compares release vs expected.
6. Sentinel `probe_deploy` emits `production_commit_drift` when they diverge.

## Important Boundary

Hermes does not deploy code. It reports drift and evidence. GitHub Actions or manual SCP/systemctl still perform the actual deployment.

## Verification

- `pytest tests/unit/test_hermes_deploy_status_service.py tests/unit/test_hermes_sentinel.py tests/unit/test_hermes_routes.py -q`
- Full target backend subset: `169 passed`
- Frontend type check: `npm run check:types`
