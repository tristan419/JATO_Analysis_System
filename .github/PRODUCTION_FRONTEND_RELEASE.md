# Production frontend immutable release

The `production-release` workflow builds the frontend once on Node `20.19.0`
with `06_AppPlatform/frontend/package-lock.json`. It uploads one immutable
artifact named `frontend-dist-${GITHUB_SHA}`. Its schema-v2 payload contains
both the static `dist/` tree and the Cloudflare Pages `functions/` tree.
Tencent/www and Cloudflare/intl both consume that exact artifact by the numeric
GitHub artifact id; neither deployment reads runtime frontend code from an
unverified checkout path.

## Artifact and provenance fields

- `artifact.name`: deterministic artifact name for the source revision.
- `artifact.id`: logical immutable identity derived from repository, workflow
  run id, run attempt, and artifact name.
- `artifact.githubId`: numeric id assigned by `actions/upload-artifact@v4`.
- `artifact.githubDigest`: SHA256 digest assigned to the outer GitHub artifact.
- `artifact.checksum`: SHA256 of the deterministic `frontend-dist.tar.gz`
  payload containing `dist/` and `functions/`. Consumers independently
  recompute this value before deployment.
- `source.githubSha` and `source.deployCommit`: workflow source revision.
- `source.appCommit`: application revision resolved by the existing Hermes
  commit semantics. It may differ from `deployCommit` for a Hermes-only
  metadata commit; parity requires both platforms to expose the same values.
- `frontend.buildId`: SHA256 fingerprint of sorted frontend dist paths and
  bytes before release metadata and server-side compression derivatives.
- `frontend.nodeVersion`: exact Node version used by the single build job.
- `edgeFunctions.treeId`: SHA256 fingerprint of sorted Pages Function paths and
  bytes. The manifest also records the required entrypoints and generated route
  contract (`/v1/*`, `/oauth-relay/*`, and `/healthz`).

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
the move fails. Cloudflare publishes the same materialized dist from a temporary
Pages project containing only the artifact's verified Functions tree. A pinned
Node/Wrangler pair compiles the tree before any production mutation, and the
generated routes must contain all three required routes.

After Cloudflare switches, the workflow rejects an intl release unless root
health, API data freshness, and OAuth relay health all return real JSON with
their expected edge markers. A SPA fallback returning `200 text/html` is a
release failure, not a successful health check.

## Backend transport and recovery

The backend release uses the existing SSH connection and CVM disk; it does not
require COS, a CDN, or another long-lived cloud credential. The deterministic
archive is addressed by its SHA-256 and transferred with resumable rsync. The
server independently verifies its byte length and checksum before sealing it,
and never overwrites a different immutable archive.

Durable checkpoints bind every deploy phase to the main commit, backend archive,
workflow build identity, and the single verified frontend artifact. Transport
and preparation failures may resume the exact artifact. An interrupted database
migration fails closed for manual revision and backup inspection, while an
ambiguous intl deployment is resolved by reading public provenance before any
repeat publish. The operational state machine, retry classes, prerequisites,
and interruption tests are documented in
[`FREE_RELEASE_RECOVERY_RUNBOOK.md`](FREE_RELEASE_RECOVERY_RUNBOOK.md).

Schema v2 intentionally fails closed on schema-v1 artifacts. The first
successful schema-v2 production release becomes the new directly reusable
rollback baseline. An older schema-v1 release may only be restored together
with its matching historical release helper; do not mix a v1 payload with the
v2 verifier.

`intl-edge-prewarm` has only a `workflow_run` trigger for a completed,
successful `production-release` run on `main`. It resolves and downloads that
run's artifact, validates the intl public provenance, and only then starts the
cache prewarm script. It is not a deployment entry point.

The manual `sync-www-active-to-intl` workflow is the independent intl release
entry point. It accepts only an explicit confirmation on current `main`; it
does not accept a release SHA. Under the production release lock it proves the
current content-addressed www Active root and runtime seal, downloads only that
root's embedded `hermes/frontend_release` manifest and payload, verifies public
www against them, and idempotently deploys the same bytes to Cloudflare. A
legacy/non-content-addressed Active is rejected rather than rebuilt. This flow
does not depend on Candidate state and does not modify backend or JATO data.

## Validation and governance

Before any build or production environment, the release coordination preflight
checks the unpublished range from the last successful verified production SHA
through the target main SHA. Explicit `Release-Group` and `Depends-On`
contracts are stored as append-only
`.github/release-coordination/contracts/pr-<PR>.json` receipts. Production
loads each receipt from that PR's exact merge SHA, so later body, label, issue,
or target-tree edits cannot erase a partial group. The gate fails closed on
partial groups, unresolved dependencies, disagreeing immutable snapshots,
malformed metadata, stale main, cycles, PR-file visibility limits, or GitHub
API ambiguity. Its decision is frozen as a same-run immutable artifact. After
production approval and before deployment credentials, the workflow consumes
that frozen plan and rechecks that the target is still current `main`; it does
not re-read mutable PR bodies.
See [`RELEASE_COORDINATION.md`](RELEASE_COORDINATION.md).

Run the deterministic local checks without production secrets:

```bash
python .github/scripts/validate_production_workflow_guards.py
python .github/scripts/validate_frontend_release_workflow.py
python -m unittest \
  03_Scripts/tests/test_release_coordination_guard.py \
  03_Scripts/tests/test_frontend_release_artifact.py \
  03_Scripts/tests/test_verify_intl_runtime_contract.py \
  -v
```

Repository rulesets and the authoritative Cloudflare Production branch are
external configuration and are not changed by this workflow.
