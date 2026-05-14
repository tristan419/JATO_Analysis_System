# CI / Deploy Workflow Governance

> Created: 2026-05-14
> Based on: Phase 5.6 deploy diagnosis (commit `ecb7154`)
> See also: `hermes/reports/deploy_failure_diagnosis.md`

## 1. Current Workflow Roles

| Workflow | Role | Status | Action |
|---|---|---|---|
| `deploy-fullstack-tencent` | **Production deploy** | Active — mostly successful | Keep. Cancelled runs are normal. |
| `ci` | Diagnostics / quality gate | Active — consistently failing | Convert to non-blocking diagnostics. |
| `deploy-ssh-auto-update` | Legacy EC2 deploy | Active — consistently failing | Convert to manual-only or disable. |

## 2. How to Interpret GitHub Actions Status

### deploy-fullstack-tencent

- **Success** = deployed to ojeur.cloud
- **Cancelled** = a newer push arrived before this deploy finished. Normal. NOT a failure.
- **Failure** = actual deploy problem. Check the "Deploy on server over SSH" step logs.

### ci

- **Failure** = pre-existing code style/check issue. NOT caused by Hermes. NOT a deploy failure.
- This workflow should not be used to judge whether a push is safe to deploy.

### deploy-ssh-auto-update

- **Failure** = legacy EC2 SSH connection failed. Target server likely does not exist.
- This workflow should be ignored or disabled.

## 3. Production Deploy Source of Truth

The single source of truth for production deployment is:

```
deploy-fullstack-tencent
```

All other workflow statuses are informational.

## 4. Deploy Trigger Paths

Current trigger paths (as of 2026-05-14):

```yaml
paths:
  - "06_AppPlatform/**"
  - "07_ScrapingToolkit/**"
  - "03_Scripts/**"
  - "hermes/**"              # Added 2026-05-14
  - "01_RAW_DATA/VOC_Nordic_SUV_Users_100.xlsx"
  - ".github/workflows/deploy-fullstack-tencent.yml"
```

## 5. Recommended Actions

| Priority | Action | Workflow |
|---|---|---|
| P1 | Convert to `workflow_dispatch` only | `deploy-ssh-auto-update` |
| P2 | Split CI into non-blocking diagnostic stages | `ci` |
| P3 | Restore CI as quality gate with focused checks | `ci` |

### Safe Disable for Legacy EC2

Change `.github/workflows/deploy-ec2-auto-update.yml`:

```yaml
on:
  workflow_dispatch:
  # push:        # Commented out — EC2 target obsolete
  #   branches: ["main"]
```

Do NOT delete the file. It preserves history and can be re-enabled if the EC2 instance is restored.

### Safe CI Split

Current CI runs 3 jobs. Short-term: convert to `continue-on-error: true` on the smoke job, or split into separate workflows so one failure doesn't block the others.

Long-term: separate into:
1. `hermes-registry-check` — YAML validation
2. `python-syntax-check` — compileall
3. `frontend-typecheck` — tsc --noEmit
4. `regression-check` — existing regression scripts

## 6. Registry References

- `pipeline.deploy.tencent_github` — production deploy
- `pipeline.ci.github` — diagnostics
- `pipeline.deploy.ec2_legacy_github` — legacy, deprecated candidate
- `gap.ci.preexisting_failures` — CI pre-existing failures
- `gap.deploy.legacy_ec2_workflow` — legacy EC2 workflow
- `proposal.ci.non_blocking_diagnostics` — CI split proposal
- `proposal.deploy.disable_legacy_ec2` — legacy EC2 disable proposal
