# Tencent COS production release transport runbook

This runbook bootstraps the private transport used by
`.github/workflows/production-release.yml`. It does not replace the immutable
frontend artifact: www and intl continue to deploy the exact same verified
frontend payload. COS carries the complete backend release archive only.

Do not merge the transport PR until every preflight in this document passes.
The workflow intentionally has no SSH archive fallback.
Merging the PR changes both `03_Scripts/**` and the production workflow, so the
merge itself will enqueue a main-branch production release. Configure and test
all external prerequisites before approving that merge; use the existing
`production` environment approval as the final human hold point.

## Fixed trust contract

```text
main + production approval
  -> GitHub OIDC token
  -> Tencent STS upload role
  -> COS accelerated multipart upload
  -> HEAD size / SHA metadata / CRC64 verification
  -> host-pinned SSH control payload (no archive bytes or secrets in argv)
  -> CVM instance role + regional internal COS download
  -> local size / SHA-256 / CRC64 verification
  -> existing release validation and deployment
  -> www + intl runtime gates
  -> verified-production receipt
```

The object key is always:

```text
releases/<40-character main commit>/<64-character archive SHA-256>.tar.gz
```

Only a separately approved administrator may copy a fully verified object to:

```text
rollback/<40-character main commit>/<64-character archive SHA-256>.tar.gz
```

The GitHub upload role cannot write `rollback/`. Runtime roles cannot copy,
tag, or delete objects in either prefix.

The uploader and CVM downloader use four 8 MiB in-flight parts (about 32 MiB of
payload memory per side). The helper hard-caps upload configuration at four
threads and 64 MiB per part so a future workflow edit cannot accidentally turn
release transport into an unbounded memory spike.

The code derives both COS endpoints. Operators cannot supply an arbitrary URL:

- GitHub upload: `<bucket>.cos.accelerate.tencentcos.cn`
- CVM download: `<bucket>.cos-internal.<region>.tencentcos.cn`

## 1. Private bucket

Create a new, dedicated, private, standard-storage bucket in the production
CVM's exact region. Do not reuse a bucket that has ever had versioning enabled
or suspended. The bucket name supplied to the workflow must include the APPID
suffix. Public anonymous read/write must remain disabled. Every release upload
explicitly uses COS-managed AES256 server-side encryption, and HEAD verification
rejects an object without that encryption marker.

Before configuring either runtime role, treat all of the following as blocking
checks:

- `GET Bucket versioning` returns an empty configuration. `Enabled` and
  `Suspended` both fail this gate. Once versioning has ever been enabled,
  `x-cos-forbid-overwrite` cannot provide this transport's immutability contract.
- The bucket region exactly equals the CVM metadata region.
- Bucket ACL is private and anonymous public access is disabled.
- Global acceleration reports `Enabled` before the GitHub upload trial.
- The lifecycle configuration exactly matches the two rules below and has no
  rule that expires `rollback/`.

Enable global acceleration only for GitHub's cross-region upload. Do not use
the acceleration endpoint from the CVM. Global acceleration has incremental
traffic cost and, after first enablement, can be paused but not returned to a
never-enabled state.

Never enable or suspend bucket versioning for this transport bucket. Both
Initiate Multipart Upload and Complete Multipart Upload must carry
`x-cos-forbid-overwrite=true`; the upload policy explicitly denies either call
when that header is absent or not `true`.

Configure two independent lifecycle rules:

1. Prefix `releases/`, expire current objects after 30 days. This rule does not
   depend on an object tag.
2. Prefix `releases/`, abort incomplete multipart uploads after 3 days.

Expiration and incomplete-multipart cleanup must be separate rules. Do not add
an expiration rule for `rollback/`.

An object is copied to `rollback/` only after the workflow has produced a
`verified-production` receipt and a separate administrator has reviewed the
source key, target key, bytes, SHA-256, CRC64, commit, and successful run ID. A
simple COS copy requires source `cos:GetObject` and target `cos:PutObject`; grant
those permissions only to the short-lived administrator identity and only for
the exact reviewed source and destination objects. The copy request must use
HTTPS, `x-cos-forbid-overwrite=true`, private/default ACL, and explicit SSE-COS
AES256. HEAD the destination and re-check all receipt fields before accepting it
as rollback evidence. Do not grant the administrator `DeleteObject` or tagging
permission. Revoke the temporary copy permission after verification.

## 2. GitHub OIDC upload role

Create a Tencent CAM OIDC provider with:

- issuer: `https://token.actions.githubusercontent.com`
- audience: a dedicated value such as `jato-production-cos`
- GitHub's current OIDC signing public keys

Create a role whose trust conditions require exact string equality for:

```text
oidc:iss = https://token.actions.githubusercontent.com
oidc:aud = <dedicated audience>
oidc:sub = repo:tristan419/JATO_Analysis_System:environment:production
```

Do not assume that subject format. Before creating the Tencent trust policy,
record the current repository OIDC configuration:

```bash
gh api repos/tristan419/JATO_Analysis_System/actions/oidc/customization/sub
```

Then use an approved diagnostic job with `id-token: write` and
`environment: production` to request a token for the dedicated audience. Decode
and record only `iss`, `aud`, `sub`, `repository`, `ref`, and `environment`; do
not print or retain the JWT. The actual `sub` must exactly match the CAM trust
condition. Stop if GitHub immutable subjects or a custom subject template change
the value.

Attach a policy rendered from
`03_Scripts/deploy/cos/github-upload-policy.template.json`. Replace all three
placeholders and retain no wildcard outside `releases/*`. The role must not
receive GetObject, PutObject, object-copy, DeleteObject, bucket configuration,
ACL, lifecycle, tagging, `rollback/*`, or account-listing permissions. The
policy requires HTTPS for every granted operation, permits Head/UploadPart/Abort
only as needed, and requires `x-cos-forbid-overwrite=true` on both multipart
boundary calls.

GitHub requests this role through `AssumeRoleWithWebIdentity`. Do not create
`COS_SECRET_ID` or `COS_SECRET_KEY` repository secrets.

Create the Tencent OIDC provider from GitHub's current discovery document and
the complete JWKS at its advertised `jwks_uri`; record the JWKS SHA-256 and key
IDs without recording a token. Tencent role-OIDC configuration does not provide
automatic GitHub JWKS rotation, so assign an owner and scheduled monitor. When
the GitHub key set changes, update the Tencent provider while old and new keys
overlap, run an approved STS probe, and alert before a previously configured key
disappears. A failed or stale-key probe blocks production deployment.

## 3. CVM read role and pinned downloader

First inventory the role already bound to the production CVM. A CVM can have
only one bound role, so do not replace an existing role blindly. If a role is
already bound, review its existing consumers and attach the rendered
`03_Scripts/deploy/cos/cvm-read-policy.template.json` to that role. If no role
is bound, create one whose trusted Tencent service principal includes
`cvm.qcloud.com`, attach the rendered read policy, and bind it. The policy grants
HTTPS-only HeadObject and GetObject for `releases/*` and `rollback/*`; it cannot
list, upload, copy, delete, tag, or configure the bucket.

Install the pinned downloader once before the transport PR is merged:

```bash
bash 03_Scripts/deploy/install_cos_release_transport.sh
```

The installer supports Linux amd64 and arm64, tries Tencent's domestic COSCLI
mirror before the versioned GitHub release, verifies the official COSCLI v1.0.8
SHA-256 in either case, and installs `/usr/local/bin/coscli` root-owned. The
release script creates a short-lived config in `/tmp` with `mode: CvmRole`; no
Tencent credential is written by the workflow or persisted by the deploy
script.

Preflight the instance role without printing credentials:

```bash
test -x /usr/local/bin/coscli
/usr/local/bin/coscli --version
METADATA=http://metadata.tencentyun.com/latest/meta-data
test "$(curl --fail --silent "$METADATA/placement/region")" = '<REGION>'
curl --fail --silent "$METADATA/cam/security-credentials/" \
  | grep -Fx '<CVM_ROLE_NAME>'
curl --fail --silent \
  "$METADATA/cam/security-credentials/<CVM_ROLE_NAME>" \
  | python3 -c 'import json, sys; data=json.load(sys.stdin); required=("TmpSecretId", "TmpSecretKey", "Token", "Expiration"); raise SystemExit(0 if data.get("Code") == "Success" and all(data.get(key) for key in required) else 1)'
```

The last command validates `Code=Success` and required fields without emitting
the credential. Never run it with shell tracing and never save its response.

Resolve `<BUCKET-APPID>.cos-internal.<REGION>.tencentcos.cn` on the CVM and
require a Tencent private/internal address; a public resolution blocks rollout.
Using the same temporary `mode: CvmRole` COSCLI configuration as the deployment
script, copy an administrator-created, non-sensitive canary under `releases/`
to a new `/tmp` file. This single-object download exercises HeadObject and
GetObject. Verify its expected byte length and SHA-256, then delete only the
local file. A 403, public endpoint, checksum mismatch, or request for
HeadBucket/ListBucket indicates a policy or endpoint error and blocks rollout.

## 4. GitHub production variables

After bucket and roles exist, add these as `production` environment variables,
not repository secrets:

```text
COS_RELEASE_BUCKET=<bucket including APPID>
COS_RELEASE_REGION=<same region as CVM>
COS_RELEASE_UPLOAD_ROLE_ARN=<OIDC upload role ARN>
COS_RELEASE_OIDC_PROVIDER_ID=<Tencent OIDC provider name>
COS_RELEASE_OIDC_AUDIENCE=<dedicated exact audience>
COS_RELEASE_CVM_ROLE_NAME=<bound CVM role name>
```

Also add `SSH_KNOWN_HOSTS` as a `production` environment secret. Populate it
from the production CVM console or another already trusted, out-of-band channel;
do not create the trust pin with `ssh-keyscan` from the same GitHub runner that
will use it. The entry must match `SSH_HOST` and the configured `SSH_PORT`
(bracketed host syntax for a non-default port). The workflow fails before the
COS upload if that exact host entry is absent. SSH deployment variables and
application secrets are sent through a mode-`0600` stdin payload and never in
the remote process argument list.

The existing production required reviewer and main-only deployment policy must
stay enabled. The upload job has only `contents: read` and `id-token: write`.

## 5. Pre-merge verification

Run locally without cloud credentials:

```bash
python -m pip install --require-hashes \
  --requirement 03_Scripts/deploy/requirements-cos-release.txt
python .github/scripts/validate_production_workflow_guards.py
python .github/scripts/validate_frontend_release_workflow.py
python -m unittest 03_Scripts/tests/test_cos_release_transport.py -v
bash -n 03_Scripts/deploy/fullstack_remote_release.sh
bash -n 03_Scripts/deploy/install_cos_release_transport.sh
```

Then perform one explicitly approved private-bucket trial with the current
approximately 22 MB release archive and record:

- legacy SSH upload elapsed time;
- normal COS endpoint upload elapsed time;
- accelerated COS endpoint upload elapsed time;
- CVM internal download and local verification time;
- total production release time.

Before the trial, retain evidence that the bucket versioning query was empty,
global acceleration was enabled, the two lifecycle rules were active, the
actual OIDC claims matched CAM, the JWKS monitor was assigned, the CVM role was
not destructively replaced, and the internal canary passed.

Inject at least one bad SHA and one interrupted download before enabling the
new path. Both must stop before archive extraction and leave the current
service, frontend fingerprint, and database unchanged.

## 6. Rollback boundary

An object is not a rollback release merely because it exists. Only a workflow
run that reaches the final `verified-production` receipt after Tencent, intl,
and API checks, followed by the separately approved and verified copy into
`rollback/`, is eligible. A rollback must pin the protected object key, source
object key, bytes, SHA-256, CRC64, commit, copy approval, and successful run
identity. It must also confirm the old code remains compatible with the current
database schema; this transport does not automatically reverse Alembic
migrations.
