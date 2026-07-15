# Production frontend immutable release

The `production-release` workflow builds the frontend once on Node `20.19.0`
with `06_AppPlatform/frontend/package-lock.json`. It uploads one immutable
artifact named `frontend-dist-${GITHUB_SHA}` and makes both Tencent/www and
Cloudflare/intl download that artifact by the numeric GitHub artifact id.

## Artifact and provenance fields

- `artifact.name`: deterministic artifact name for the source revision.
- `artifact.id`: logical immutable identity derived from repository, workflow
  run id, run attempt, and artifact name.
- `artifact.githubId`: numeric id assigned by `actions/upload-artifact@v4`.
- `artifact.githubDigest`: SHA256 digest assigned to the outer GitHub artifact.
- `artifact.checksum`: SHA256 of the deterministic `frontend-dist.tar.gz`
  payload. Consumers independently recompute this value before deployment.
- `source.githubSha` and `source.deployCommit`: workflow source revision.
- `source.appCommit`: application revision resolved by the existing Hermes
  commit semantics. It may differ from `deployCommit` for a Hermes-only
  metadata commit; parity requires both platforms to expose the same values.
- `frontend.buildId`: SHA256 fingerprint of sorted frontend dist paths and
  bytes before release metadata and server-side compression derivatives.
- `frontend.nodeVersion`: exact Node version used by the single build job.

Both origins expose the same enriched `build-meta.json` and
`release-provenance.json`. The final parity job compares these public documents
against the downloaded manifest; it does not accept platform-specific commit,
build id, Node, artifact identity, or checksum values.

## Fail-closed behavior

Deployment stops before credentials or platform deploy commands when the
artifact is missing, its payload checksum or size differs, required manifest
metadata is incomplete, the GitHub SHA differs, or artifact identities are not
consistent. Tencent has no sparse checkout or server-side frontend build
fallback. The verified dist is precompressed in a staged directory and then
installed with a same-filesystem directory move, retaining the prior dist if
the move fails. Cloudflare publishes the materialized downloaded dist directly.

`intl-edge-prewarm` has only a `workflow_run` trigger for a completed,
successful `production-release` run on `main`. It resolves and downloads that
run's artifact, validates the intl public provenance, and only then starts the
cache prewarm script. It is not a deployment entry point.

## Validation and governance

Run the deterministic local checks without production secrets:

```bash
python .github/scripts/validate_frontend_release_workflow.py
python -m unittest 03_Scripts/tests/test_frontend_release_artifact.py -v
```

This workflow change does not replace PR #137. Before merge, replay it after
#137 reaches `main`, reconcile the shared workflow/CI files, and rerun the
combined static contracts. Repository rulesets and the authoritative
Cloudflare Production branch are external configuration and are not changed by
this workflow.
