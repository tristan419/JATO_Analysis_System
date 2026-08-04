# Free production release recovery runbook

This runbook covers the zero-new-service release path used by
`.github/workflows/production-release.yml`. It keeps the existing GitHub
Actions, SSH connection, and Tencent CVM disk. It does not require COS, a CDN,
an additional long-lived credential, or a second backend instance.

The transport is free of new service charges, but it still consumes the
bandwidth, disk, and GitHub Actions quota already attached to the existing
systems.

## Safety boundary

The transport and archive preflight may retry automatically. A database
migration with an incomplete marker may not. Never clear or edit a production
checkpoint merely to make a workflow green.

Production remains subject to all existing governance:

- only a merge to `main` or an approved manual `main` dispatch may release;
- the GitHub `production` environment remains the approval boundary;
- Tencent/www and Cloudflare/intl consume the same immutable frontend artifact;
- releases are serialized and a newer push does not cancel an active release;
- rollback selects a previously verified artifact and never deploys a feature
  branch;
- a JATO monthly candidate, approval, promotion, or data publication is outside
  this deployment workflow.

## Required one-time checks

Before merging the transport change, verify all of the following without
starting a production deployment:

1. Set the `SSH_KNOWN_HOSTS` secret in the GitHub `production` environment from
   an independently verified Tencent host key. Do not generate the trust pin on
   the same untrusted connection it is intended to verify.
2. Prefer `SSH_PRIVATE_KEY`. If the existing password path must remain, verify
   that the runner installs `sshpass` before it is used and schedule migration
   to a key.
3. Confirm GNU/Linux `rsync >= 3.0`, protocol 30 or later, `flock`, `sha256sum`,
   `tar`, `df`, and Python 3 exist on the CVM. The release workflow must fail
   before mutation if any prerequisite is absent.
4. Confirm the SSH user can create its private release cache and state
   directories with mode `0700` and checkpoint files with mode `0600`.
5. Confirm free space is sufficient for the immutable archive, partial upload,
   extracted candidate, runtime preservation, and the prior recovery copy.
6. Confirm the backend remains configured for two workers, cgroup
   `MemoryHigh=6G` / `MemoryMax=8G`, and the JATO monthly worker remains
   disabled.

## Immutable SSH transport

The runner creates a deterministic backend archive. Re-running a failed deploy
job against the same build artifact must produce the same byte length and
SHA-256. Run/attempt timestamps used only for the current deploy invocation live
outside the archive identity.

The archive is uploaded to a private, content-addressed server path:

```text
<release-cache>/<main-sha>/<archive-sha256>.tar.gz
<release-cache>/<main-sha>/<archive-sha256>.tar.gz.partial
```

`rsync --partial --append-verify` resumes the `.partial` file over the existing
SSH channel. Rsync compression is disabled because the payload is already
gzip-compressed. A server-side lock fences concurrent writers. The final path is
never overwritten.

Before the same-filesystem seal from `.partial` to the immutable final path,
the server independently checks the exact byte length and SHA-256. An existing
final object is reusable only when both values match. An equal-name mismatch is
a hard failure. A corrupt partial may be reset and retransmitted once in the
same job; it may never be promoted because rsync returned success alone.

The verified archive remains available through the deployment and public parity
checks. The current implementation does not automatically delete verified
archives, because it cannot yet prove which two objects are the current and
previous rollback baselines. It only replaces a corrupt partial during the
bounded upload retry. Disk-watermark monitoring and an explicit policy that
preserves at least the current and immediately previous verified releases remain
follow-up work.

## Checkpoint state machine

Each checkpoint is written to a temporary file, flushed, and atomically renamed.
An append-only JSONL journal keeps the transition history. Every record is bound
to repository, main commit, archive SHA-256 and size, workflow run/build attempt,
and frontend artifact identity/checksum.

| Phase | Owner | Interrupted action |
| --- | --- | --- |
| `packaged` | GitHub | Rebuild and verify deterministic identity. |
| `transport_verified` | GitHub/CVM | Safe to reuse or resume the same archive. |
| `prepared` | CVM | Safe to repeat archive and frontend preflight. Active production is unchanged. |
| `source_install_started` | CVM | Stop. The live source tree may be partially replaced; automatic retry is forbidden pending rollback inspection. |
| `source_installed` | CVM | The exact source and preserved runtime paths are installed; do not repeat live-tree replacement. |
| `backup_verified` | CVM | Safe to continue the same release. Verify the recorded dump before migration. |
| `migration_started` | CVM | Stop. Inspect database revision and backup; automatic retry is forbidden until reconciled. |
| `migrated` | CVM | Continue only the same artifact. Automatic application downgrade is forbidden unless the migration explicitly documents backward compatibility. |
| `switch_started` | CVM | Inspect active paths and retained previous frontend before resuming. |
| `switched` | CVM | Resume the same artifact through service health. |
| `backend_healthy` | CVM | Do not repeat migration; continue www/intl verification. |
| `www_verified` | GitHub | Inspect intl provenance before issuing another Cloudflare deploy. |
| `intl_deploy_started` | GitHub | The result is ambiguous: read public provenance first. |
| `intl_verified` | GitHub | Run final cross-origin parity. |
| `parity_verified` | GitHub | Seal the verified release receipt. |
| `complete` | GitHub | No-op when public provenance and health still match. |
| `pre_switch_aborted` | Reviewed recovery only | A specific failed release was proven never to have started Candidate or switched traffic, then sealed as abandoned by an immutable recovery receipt. It is not a successful deployment or migration. |

The retry classes exposed to operators are:

- `automatic`: retry the exact artifact without production ambiguity;
- `inspect_then_resume`: inspect the recorded active state, then continue only
  the exact artifact;
- `manual_db_recovery`: reconcile Alembic current/head and the recorded backup;
- `rollback_required`: stop forward progress and follow the reviewed rollback
  procedure;
- `complete`: no deployment action is required.

An identity mismatch between checkpoint, archive, manifest, active release, or
public provenance always fails closed.

After taking the global server deployment lock and before writing `prepared`,
the CVM validates every file in the checkpoint namespace. Evidence documents
are recognized by their exact `<archive-sha256>.evidence.json` filename and are
excluded from checkpoint JSON parsing; symlinks, malformed checkpoint JSON,
unsupported schemas, unexpected files, and commit/archive identities that do
not match their paths all fail closed.

A non-current checkpoint at `prepared` or earlier does not block a new immutable
artifact. Once any release reaches `source_install_started`, only that exact
release may resume until its checkpoint reaches `backend_healthy` with
`status=completed` (or a later settled/`complete` phase). A non-current
`manual_db_recovery` or `rollback_required` checkpoint always blocks a new
commit or a different archive for the same commit. Never delete such a record
to bypass the gate; reconcile or roll back the recorded release first.

## Database rule

When the production database is enabled, backup is a strict prerequisite. A
missing URL, missing `pg_dump`, failed command, empty dump, or missing dump
SHA-256 is a deployment failure. The checkpoint binds a private evidence file
by path and SHA-256; that evidence records the backup manifest identity and the
pre-migration, target, and resulting revisions. Resume and healthy no-op paths
revalidate the evidence, manifest, and database dump before trusting them.
The manifest and dump stay under root-owned `0700` directories with `0600`
files. Both the first deploy and every resume/no-op call the same fail-closed
Python verifier through non-interactive `sudo`; only a non-sensitive success
summary returns to the deploy user, so recovery never depends on weakening the
backup permissions.
At `migrated` or any later phase, evidence must say either `completed` or
`not_required`. A completed migration requires the normalized target heads to
equal—not merely overlap—the recorded result heads. When the database is safely
readable, resume also compares live `alembic current` with that bound result.

If `migration_started` exists without a completed `migrated` transition:

1. stop automatic reruns;
2. compare `alembic current` with the recorded pre-head and target head;
3. verify the recorded database dump exists and matches its SHA-256;
4. determine whether the migration did nothing, completed, or partially applied;
5. either authorize the exact release to continue or restore using a reviewed
   database-specific procedure.

Do not automatically run `alembic downgrade` and do not deploy a different
application artifact over an unresolved migration.

## Reviewed pre-switch checkpoint recovery

A release at `migrated/completed/automatic` is eligible for this procedure only
when an incident-specific validator proves the exact checkpoint and journal
never started the Candidate service or listener and never reached
`switch_started` or a traffic switch. The validator must also
prove one exact, versioned database-evidence profile. This is not a generic way
to abandon every checkpoint at `migrated`.

Incident history and the single active recovery target are:

| Incident | Evidence profile | State |
| --- | --- | --- |
| `2026-07-30-ce5-pre-switch-db-evidence` | schema v1; exact legacy `not_required` plus three null revision fields | Historical; not the current workflow target. Its plan remains an immutable audit asset. |
| `2026-07-30-86ce-pre-switch-db-evidence` | schema v2; exact `completed` plus equal pre/target/result revision sets | Historical; retained for compatibility and audit, but no longer selected by the workflow. |
| `2026-08-03-29df-pre-switch-candidate-residue` | schema v3; schema-v2 database proof plus an inode-bound inventory of one never-started Candidate | Active. Release `29df5e6e667351f09305783932b34e5438d6a9d5` stopped before Candidate start or traffic switch but left reviewed materialized files. |

For the active 29df incident, the database comparison is read-only and all
recorded revisions are `20260715_0046`. The Candidate unit is inactive and
disabled, has no PID, invocation, start timestamp, restart, or port-8001
listener, and public traffic still points at the reviewed active release. The
remaining marker, Candidate slot link, environment, explicit unit, sandbox
drop-in, and three resource-control drop-ins are residue, not a live Candidate.
Do not edit, delete, recreate, or manually move those paths. Also preserve the
checkpoint, journal, legacy evidence, archive, backup manifest, dump, and
previous-metadata record. The enabled Nginx route remains the reviewed
7,303-byte/SHA-256 configuration, while the distinct canonical
`sites-available` file is frozen by device, inode, owner, mode, link count,
size, mtime, and SHA-256. `/etc/jato-fullstack/nginx/active-release.conf` and
both public and private Candidate cache paths must remain absent. Do not rerun
the failed production release.

### Recovery-only production release hold

The reviewed 29df recovery PR carries the versioned hold document
`.github/recovery-plans/2026-08-03-29df-pre-switch-candidate-residue-production-hold.v1.json`.
It is bound to incident `2026-08-03-29df-pre-switch-candidate-residue`, the fixed
plan path, and plan SHA-256
`61045c5b1f39516f910ab89cf80fdd97796920e7e3bdb479f52e741b73f2f144`.
Its presence is not recovery or deployment authorization.

On a main push, `production-release` still runs the no-environment coordination
guard and freezes its coordination plan. The same guard resolves exactly one
`release-action`: `hold` when only the reviewed active document exists, or
`deploy` when the active document is absent and only the exact reviewed
retirement record exists. `hold` skips frontend build, Tencent/Cloudflare
deployment, parity audit, and the artifact/provenance/cache work in
`intl-edge-prewarm`; it therefore releases `production-release-main` without
entering the `production` environment. Missing both documents, finding both
documents, or finding a malformed, non-canonical, linked, oversized, stale, or
plan-digest-mismatched document fails the guard and never falls back to deploy.

The checkpoint-recovery workflow requires the exact active hold once before its
production-environment job and again immediately after approval. Dry-run and
apply never remove the hold. It may remain through Nginx reconciliation and the
no-traffic Candidate canary. The retirement path is fixed as
`.github/recovery-plans/2026-08-03-29df-pre-switch-candidate-residue-production-hold-retirement.v1.json`,
but that file must not exist while the hold is active. Only an explicitly
reviewed final production-release PR may delete the hold and add the exact
canonical retirement record in the same change, after checkpoint recovery and
reconciliation evidence are complete. Deleting the hold alone fails closed.
Both paths are production triggers, so merging the paired retirement change to
`main` starts the fresh release that resumes normal deployment behavior. Keep
the retirement record on `main` afterward as durable release authorization.

Use `.github/workflows/production-checkpoint-recovery.yml` only when a reviewed,
versioned incident plan exists on `main`. The workflow shares the normal
production concurrency group, requires the `production` environment approval,
and defaults to `dry-run`. Dry-run and apply both hold the canonical production
lock and prove:

- the exact checkpoint, journal, archive, legacy evidence, backup manifest, and
  dump still match their reviewed byte lengths and SHA-256 values;
- the live database is queried with read-only transactions and its Alembic
  current revisions equal the old source heads, new source heads, and backup
  revision;
- the Candidate unit has never started, port 8001 is not listening, Nginx still
  serves the reviewed active slot, and every allowed residue path has the exact
  reviewed device, inode, owner, mode, link count, size, timestamp, digest or
  symlink target;
- no unreviewed residue, runtime control drop-in, scheduler snapshot, active
  alias, cache directory, template preimage, Nginx preimage, or switch backup
  appears;
- the canonical Nginx file and previous-metadata record retain their exact
  identities, while all nine reviewed absent paths remain absent before and
  after settlement;
- the old active release and both public origins remain healthy at the reviewed
  commit, with two workers, `MemoryHigh=6G`, `MemoryMax=8G`, and the JATO
  monthly worker disabled.

Apply must consume one successful, manually reviewed dry-run result from the
same immutable `main` SHA and plan SHA. The workflow retrieves that result by
workflow run and artifact ID, verifies its raw SHA-256 and complete schema,
freezes the result plus an authorization document before production approval,
then revalidates both after approval. Both jobs call the same tested
`reviewed_recovery_authorization.py` contract; the workflow does not duplicate
or independently drift that validation and does not use a cross-job SHA output.

After repeating every live proof, apply moves only the eight allowlisted paths
to the root-owned quarantine while keeping a recovery fence at the maintenance
marker. Renames are same-filesystem, no-replace operations; the original inodes
are preserved. One `systemctl daemon-reload` makes systemd forget the moved
unit fragments, but the Candidate is never started, stopped, enabled, or
disabled. Apply writes immutable quarantine, finalization, and operation
receipts, transitions the checkpoint to
`pre_switch_aborted/completed/automatic`, validates settlement, and only then
moves the fence into quarantine. A crash at any boundary remains fenced and can
resume only with the same authorization. This terminal means the reviewed
release was abandoned before Candidate start or traffic switch; it is not a
successful migration, deployment, or rollback. A generic checkpoint write
cannot enter this phase.

The quarantine remains `root:root 0700`; it is never weakened, and the normal
SSH deployment user cannot traverse it. During apply, the root recovery process
validates the manifest, final fence, and every quarantined inode. Later normal
releases run `assert-cross-release-safe` without sudo: they validate the two
checkpoint-bound `0600` receipts, require the recovery maintenance marker to
remain absent, and revalidate the immutable previous-metadata record. They do
not permanently require the incident's Candidate slot, units, drop-ins,
active aliases, caches, scheduler snapshot, or canonical Nginx inode to remain
absent or unchanged. Those paths are deliberately reusable and are governed by
each successor release's own checkpoint. A later root audit still validates
the private manifest, final fence, and every quarantined inode. A malicious
root able to rewrite production and the private quarantine is outside this
deployment-integrity threat model; ordinary deploy-user receipt, marker, or
permanent-evidence drift remains fail-closed.

Run the recovery in this exact order:

1. Dispatch `production-checkpoint-recovery` from the current `main` with
   `mode=dry-run`; leave confirmation and all four `reviewed_*` inputs empty,
   then approve its `production` environment gate. The workflow must be fixed
   to
   `.github/recovery-plans/2026-08-03-29df-pre-switch-candidate-residue.json`;
   it must not accept an arbitrary plan path.
2. Download the uniquely named
   `checkpoint-recovery-result-<main-sha>-<run-id>-<run-attempt>` artifact.
   Require `decision=candidate-residue-dry-run-eligible`,
   `candidateResiduePresent=true`, and every mutation/change flag to be false.
   Review the target identity, inventory digest, checkpoint, journal, database,
   active runtime, public identity, and exact residue inventory. Record the
   workflow run ID, raw result-file SHA-256, main SHA, and plan SHA-256.
3. Dispatch the workflow again from that exact same `main` SHA with
   `mode=apply`, the four recorded `reviewed_*` values, and the exact
   confirmation
   `QUARANTINE 29df5e6e667351f09305783932b34e5438d6a9d5 RESIDUE AND ABORT PRE-SWITCH`.
   Do not approve the production environment until the pre-approval job has
   retrieved, verified, and refrozen the one reviewed dry-run artifact.
4. After approval, require
   `decision=pre-switch-residue-quarantined-and-aborted` (or the fully
   revalidated idempotent result `already-pre-switch-aborted`),
   `candidateResiduePresent=false`, and `trafficChanged=false`. Verify the live
   maintenance marker is absent, the final fence and all eight original inodes
   are in quarantine, and the checkpoint settlement gate passes.
5. Stop. Do not start a production release from this recovery workflow. First
   complete the separate Nginx reconciliation PR and no-traffic canary; then
   resync and retest the dependent blue/green PR before authorizing a fresh
   production release from `main`.

If `main` advances between dry-run and apply, discard that dry-run and repeat it
from the new reviewed SHA. Never retry the old failed 29df production run.
If any digest, revision, runtime, or public identity differs, stop and review
the new state instead of weakening or regenerating the incident plan to fit it.

## Public platform ambiguity

Tencent/www is verified before Cloudflare/intl is switched. The two providers
cannot change in one cross-provider atomic operation. If the workflow stops
after `intl_deploy_started`, first read `release-provenance.json` from intl. An
exact target artifact is skipped. A valid non-target immutable release may be
advanced to the target artifact. A target commit with conflicting artifact
identity, malformed provenance, or an ambiguous HTTP response fails closed. An
HTTP `200` containing the SPA HTML fallback is not health or provenance success.

## Remaining structural work

Checkpointing makes failures diagnosable; it does not make every production
mutation transactional. These changes should remain separate, reviewed release
work:

1. versioned application directories and versioned virtual environments with an
   atomic `current` pointer;
2. a detached server-side deployment controller so a GitHub/SSH disconnect
   cannot terminate a migration or switch;
3. systemd socket activation for the single backend port, preserving two
   workers without the memory cost of two simultaneous backend stacks;
4. atomic Nginx configuration install with tested rollback;
5. scheduler activation only after final cross-origin parity, rather than the
   current backend-health boundary.

Until versioned code and virtual environments exist, failures after production
mutation begins must keep the archive, checkpoint, logs, and recovery paths for
inspection.

## Acceptance tests

Before production rollout, test without publishing JATO data:

- interrupt transfer near 10%, 50%, and 99%, then verify byte-level resume;
- corrupt and oversize the partial file; confirm it is never promoted;
- verify correct-final reuse and wrong-final fail-closed behavior;
- start two writers and confirm the server lock serializes or rejects them;
- simulate insufficient disk and host-key mismatch before production mutation;
- package the same build twice and require identical archive SHA-256;
- reject tar traversal, links/devices, missing workbook, missing referenced MSRP
  evidence, and mismatched frontend provenance without changing active files or
  the database;
- interrupt at backup, migration, switch, restart, and intl deployment and
  verify the documented retry class;
- keep backup directories/files at `0700`/`0600`; confirm the deploy user
  cannot read them directly while the bounded `sudo` evidence verifier passes;
- verify `_deploy_status.txt` is atomically written and agrees with the final
  health result;
- verify www/intl expose the same immutable artifact;
- verify two backend workers, cgroup `6G/8G`, and disabled JATO monthly worker.
- require checkpoint-recovery dry-run to leave the checkpoint, journal, receipt
  directory, Candidate, Nginx route, database, and public traffic unchanged;
- require checkpoint-recovery apply to write exactly one bound receipt and the
  `pre_switch_aborted` terminal, then prove an idempotent replay changes nothing;
- require schema-v3 apply to reject a missing, stale, ambiguous, expired, or
  digest-mismatched reviewed dry-run artifact; reject any main, plan, target,
  inventory, run, or artifact binding mismatch before production secrets;
- interrupt schema-v3 apply after manifest creation, marker/fence exchange,
  individual residue moves, receipt write, checkpoint seal, and fence
  finalization; require deterministic same-authorization resume without a
  moment when neither the old maintenance marker nor recovery fence exists;
- require every quarantined object to retain the reviewed inode identity, keep
  previous metadata and canonical Nginx untouched, and reject hard links,
  symlink substitution, cross-device roots, unknown quarantine entries,
  altered/missing/duplicate required-absence entries, and both/neither path
  states;
- run schema-v3 incident settlement once as root and once as an unprivileged
  deploy user against a `root:root 0700` quarantine; require the incident-time
  path to reject any reappeared source or required-absent path;
- run at least two successor cross-release gates after legally rebuilding the
  shared Candidate/runtime/cache/Nginx paths; require both to pass while still
  rejecting receipt, finalization, maintenance-marker, permanent-metadata, and
  root-visible quarantine tampering;
- reject checkpoint recovery when any reviewed digest, database revision,
  runtime identity, or public identity differs.
- accept schema v2 only when migration evidence is `completed` and each of its
  pre/target/result revision sets exactly equals current, old heads, new heads,
  and backup; reject any v1/v2 profile swap or partial revision mismatch.
- keep schema-v1/v2 receipt validation byte-compatible while allowing schema v3
  only for the hard-coded 29df incident and its exact residue allowlist.
