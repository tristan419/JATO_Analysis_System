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
