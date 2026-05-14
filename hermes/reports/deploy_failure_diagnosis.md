# Deploy Failure Diagnosis

> Generated: 2026-05-14
> Branch: main
> Commit range: 8ef534c → 988eeef (Phase 5 → Phase 5.5)

## 1. Findings Summary

| Workflow | Status | Root Cause |
|---|---|---|
| `deploy-fullstack-tencent` | success (most runs) | Not broken. Gets **cancelled** by `cancel-in-progress: true` when rapid pushes occur. |
| `ci` | failure (all runs) | Style check / compile check failures. Pre-existing, not caused by Hermes. |
| `deploy-ssh-auto-update` | failure (all runs) | Legacy workflow. SSH to old EC2 instance that likely doesn't exist. |

## 2. Deploy Run History

| Run ID | Commit | Deploy Result |
|---|---|---|
| 25865288695 | Phase 5.5 (988eeef) | **success** |
| 25864769654 | Phase 5 (8ef534c) | cancelled |
| 25864153370 | dashboard fix | cancelled |
| 25863552869 | Phase 4 (f56e9c5) | cancelled |
| 25862921267 | Phase 3 (6676622) | **success** |
| 25859317922 | Phase 2 (6485775) | **success** |

**Pattern**: deploy-fullstack-tencent succeeds when it runs to completion. It gets cancelled when a newer push arrives before the previous deploy finishes (concurrency control).

## 3. Proactive Fixes Applied

### Fix 1: Node version validation mismatch

**Evidence:**
- `06_AppPlatform/frontend/package.json` engines: `>=20.19.0 <21 || >=22.12.0`
- `03_Scripts/ops/deploy_fullstack_server.sh` validation: `20.10+ or 22.x+`
- Gap: versions 20.10–20.18 and 22.0–22.11 would pass validation but fail `npm ci`/`vite build`

**Fix:** Updated deploy script validation to match package.json exactly.

### Fix 2: deploy trigger paths missing hermes/

**Evidence:**
- Deploy paths include `03_Scripts/**` but not `hermes/**`
- Hermes runtime files (registries, pricing, reports) live under `hermes/`
- Push that only changes `hermes/*.yaml` would NOT trigger deploy

**Fix:** Added `hermes/**` to deploy trigger paths.

## 4. Files Changed

| File | Change |
|---|---|
| `.github/workflows/deploy-fullstack-tencent.yml` | Added `hermes/**` to trigger paths |
| `03_Scripts/ops/deploy_fullstack_server.sh` | Node validation: 20.10+ → 20.19+, 22.0+ → 22.12+; added `node -v` and `npm -v` logging |

## 5. Not Fixed (Separate Issues)

| Issue | Reason |
|---|---|
| `ci` workflow always fails | Pre-existing style check issues. Not deploy-related. |
| `deploy-ssh-auto-update` always fails | Legacy workflow for old EC2 instance. Should be disabled separately. |
