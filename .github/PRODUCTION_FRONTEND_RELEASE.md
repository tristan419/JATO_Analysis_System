# Production frontend immutable release

The `production-release` workflow builds the frontend once on Node `20.19.0`
with `06_AppPlatform/frontend/package-lock.json`. It uploads one immutable
artifact named `frontend-dist-${GITHUB_SHA}`. Its schema-v2 payload contains
both the static `dist/` tree and the Cloudflare Pages `functions/` tree.
Tencent/www and Cloudflare/intl both consume that exact artifact by the numeric
GitHub artifact id; neither deployment reads runtime frontend code from an
unverified checkout path.

The complete backend release archive uses Tencent COS only as its transport.
GitHub obtains a short-lived upload credential through OIDC, uploads to the
globally accelerated endpoint, and verifies object bytes, SHA-256 metadata, and
CRC64 with a HEAD request. The object key is content-addressed as
`releases/<main commit>/<archive sha256>.tar.gz` and forbids overwrite. SSH no
longer carries archive bytes; the host-pinned channel sends release controls,
the existing deploy environment, and the release script through a mode-`0600`
stdin payload instead of process arguments. The CVM downloads with its read-only
instance role through the derived same-region internal endpoint.

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

The outer COS archive is also fail-closed. The GitHub uploader validates every
multipart part's CRC64, the completed object's CRC64, exact size, SHA-256
metadata, commit metadata, and object-key namespace. The CVM downloads to a
temporary file and independently recomputes size, SHA-256, and CRC64 before an
atomic local rename. Archive paths are checked before extraction. None of these
transport failures reaches the production code directories. COS does not make
the later backend directory copy or database migration transactional; those
continue to use the existing deployment and migration failure boundaries.

Only after Tencent provenance, Cloudflare provenance, and the intl runtime API
gate and the final www/intl parity audit all succeed does the workflow seal a
`verified-production` COS receipt. An uploaded candidate without that sealed
receipt is not an approved rollback source. Candidate evidence is retained for
7 days; the sealed receipt is retained for 30 days to match the `releases/`
object lifecycle. Longer-lived rollback objects require a separate,
administrator-approved copy into the protected `rollback/` prefix.

After Cloudflare switches, the workflow rejects an intl release unless root
health, API data freshness, and OAuth relay health all return real JSON with
their expected edge markers. A SPA fallback returning `200 text/html` is a
release failure, not a successful health check.

Schema v2 intentionally fails closed on schema-v1 artifacts. The first
successful schema-v2 production release becomes the new directly reusable
rollback baseline. An older schema-v1 release may only be restored together
with its matching historical release helper; do not mix a v1 payload with the
v2 verifier.

`intl-edge-prewarm` has only a `workflow_run` trigger for a completed,
successful `production-release` run on `main`. It resolves and downloads that
run's artifact, validates the intl public provenance, and only then starts the
cache prewarm script. It is not a deployment entry point.

## Validation and governance

Run the deterministic local checks without production secrets:

```bash
python -m pip install --require-hashes \
  --requirement 03_Scripts/deploy/requirements-cos-release.txt
python .github/scripts/validate_frontend_release_workflow.py
python -m unittest \
  03_Scripts/tests/test_frontend_release_artifact.py \
  03_Scripts/tests/test_cos_release_transport.py \
  03_Scripts/tests/test_verify_intl_runtime_contract.py \
  -v
```

Repository rulesets and the authoritative Cloudflare Production branch are
external configuration and are not changed by this workflow.
