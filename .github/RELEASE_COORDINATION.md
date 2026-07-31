# Release coordination guard

The repository treats every pull request as an independent release by default.
Use the coordination contract only when two or more PRs would be incomplete or
unsafe if production received only part of the set.

## Pull request trailers

Keep the machine-readable trailers at the left edge of the PR body:

```text
Release-Group: independent
Depends-On: none
```

An independent PR may omit both trailers. A directed dependency uses:

```text
Release-Group: independent
Depends-On: #174, #175
```

Dependencies must be PRs in this repository, target `main`, no longer be drafts,
be merged, and have merge SHAs that are ancestors of the main revision being
checked. Open, draft, closed-without-merge, cross-repository, non-main,
malformed, missing, cyclic, or API-unverifiable dependencies fail closed.

The parser accepts only exact comma-separated `#<number>` references. Metadata
inside fenced, HTML-commented, tab-indented, or four-space-indented examples is
ignored.

## Immutable per-PR contract

Every PR that declares a release group or dependency must add exactly one new
file named:

```text
.github/release-coordination/contracts/pr-<THIS_PR_NUMBER>.json
```

Independent PRs must not add a contract. Use
[`contract.example.json`](release-coordination/contract.example.json) as the
schema example. A dependency-only contract sets `releaseGroup` to `null`.
`repository` and `pullRequest` must identify this repository and this PR;
`dependsOn` must exactly match the PR trailer. A grouped contract must contain
the complete issue number, anchor, and ordered member list from the group issue.

The PR evaluator reads this JSON through the GitHub Contents API at the exact
PR head SHA. It never checks out or executes the PR head. It also inspects the
entire visible PR file list: the contract must be newly added by its own PR,
and a PR may not edit, rename, or delete any other contract. If GitHub's
3,000-file PR API limit prevents proving ownership, validation fails closed.

All member PRs copy the identical release-group snapshot. The anchor contract
must depend on every other member. Contract files are append-only historical
receipts: after merge they must never be edited, renamed, or deleted.

## Coordinated release group

Create an issue from the `Release group` issue template. The issue must:

- be authored by an owner, member, or collaborator;
- carry the maintainer-controlled `release-group` label;
- remain open until the coordinated release succeeds;
- contain exactly one anchor and at least two unique members.

Its machine block is:

```text
Release-Group-Anchor: #175
Release-Group-Members: #174, #175
```

Every member PR must also be authored by an owner, member, or collaborator,
carry the `release-group` label, target `main`, and contain:

```text
Release-Group: #200
Depends-On: none
```

The anchor PR replaces `none` with every other member:

```text
Release-Group: #200
Depends-On: #174
```

Merge non-anchor members first and the anchor last. The anchor cannot receive a
passing coordination status until all of its dependencies are merged into its
current main base. If branch protection was bypassed and an anchor entered
early, production remains blocked while the group is partial; merging all
declared members allows recovery without a permanent historical deadlock.

## Parallel development example: BOM Admin and AstrBot

BOM Admin and AstrBot may be developed at the same time, but they must not
share a checkout, branch, or PR. Start both from the same current remote
`main`, using one session, worktree, `codex/*` branch, and PR per business
line:

| Work | Session and worktree | Branch | Example PR |
| --- | --- | --- | --- |
| BOM Admin | BOM session in `JATO_Analysis_System_bom_admin` | `codex/bom-colour-rule-library` | `#201` |
| AstrBot | AstrBot session in `JATO_Analysis_System_astrbot` | `codex/astrbot-countrycopilot` | `#202` |

Each PR owns only its business-line changes and tests. Neither feature branch
is a production source.

If the features can be released independently:

1. validate, merge, and release `#201` from `main`;
2. synchronize `#202` with the latest `main` that contains `#201`;
3. verify that the resulting diff and regression tests preserve BOM Admin as
   well as AstrBot;
4. rerun all required CI and canary checks for the new `#202` head SHA;
5. merge `#202`, then release the resulting `main`.

If the features must reach production together, create a release-group issue
whose members are `#201` and `#202`, and choose `#202` as the anchor. Add the
identical immutable contract and maintainer-controlled `release-group` label
to both PRs. Merge `#201` first; the partial group deliberately holds
production. Synchronize the anchor with that new `main`, verify that its final
tree contains both features, rerun CI and a combined BOM Admin plus AstrBot
canary for that exact head SHA, and merge the anchor last. Only then may the
resulting `main` trigger the coordinated production release.

Shared files such as `App.tsx`, `api/client.ts`, or `main.py` require an
explicit owner before either session edits the overlapping file or hunk. The
non-owner should keep its integration minimal, wait for the owner PR to merge,
then synchronize the latest `main`, resolve any remaining integration change,
and retest both features. Do not copy an older complete shared file into the
second PR.

Every change to a proposed final SHA—including synchronization, conflict
resolution, amended commits, or a new merge commit—invalidates earlier CI and
canary evidence. Required checks and canaries must run again against the exact
new SHA; evidence from a predecessor SHA is not transferable.

Production remains `main`-only. The production release must build or retrieve
one SHA-bound immutable artifact and deploy that same verified artifact to
both `www` and `intl`; a feature, hotfix, or integration branch must never
overwrite production directly.

In short: parallel development is independent, while production convergence
is explicit and SHA-bound.（并行开发各走各的 PR，生产发布只认最终合并后的同一个
`main` SHA。）

## PR status behavior

`release-coordination` runs only trusted code checked out from
`refs/heads/main`; it never checks out or executes a PR head. It responds to PR
open, edit, synchronization, label, draft, and close events, as well as
release-group issue edits, labels, reopen, and close events. Each run sweeps all
open PRs so merging a dependency refreshes its dependents.

The trusted script writes a hard-coded `release-coordination-guard` commit
status directly to every open PR head SHA. Each evaluation moves from `pending`
to `success` or `failure`; no-change sweeps reuse the existing final status to
avoid GitHub's per-SHA/context status limit. PR mutation events first revoke an
old success with `pending`. An API outage fails closed. If a workflow is
cancelled while a status is pending, manually dispatch `release-coordination`
to sweep and finalize all open PRs.

Sweeps use one repository-wide, non-cancelling concurrency group so issue and
PR events cannot race status writes.

Ruleset rollout is intentionally staged:

1. merge this workflow so `pull_request_target` executes only default-branch
   code;
2. a repository owner creates the exact `release-group` label (the issue
   template cannot create a missing label);
3. test an internal independent PR, an internal grouped PR, and—when policy
   permits—a harmless fork canary; confirm each status targets the actual PR
   head and duplicate-head PRs fail closed;
4. only after those canaries pass, configure
   `release-coordination-guard` as a required status bound to the GitHub Actions
   App integration id `15368`.

This repository change does not mutate the external GitHub ruleset.

## Production behavior

The first `production-release` job runs without an environment or production
secret. It finds the SHA of the latest successful `production-release` whose
`audit_frontend_parity` job also succeeded, compares that verified baseline
with the current target main SHA, and evaluates every associated,
not-yet-published PR in that range.

This range is a persistent hold. For example, if grouped PR `#174` merges
first, production is blocked because `#175` is pending. Merging an unrelated
independent PR `#176` does not bypass the hold: `#174` remains in the
unpublished baseline-to-target range. Once `#175` completes the group, the
release can proceed.

Production does not trust current PR bodies, labels, group issues, or the
target tree for historical coordination. For every unpublished PR it fetches
that PR's contract path from the exact immutable merge SHA and binds the
contract blob SHA into the frozen plan. All member snapshots must agree and
the complete group must be present in the unpublished range. Removing trailers,
closing or editing the group issue, or deleting an old contract in a later
commit cannot erase a partial-group hold.

The preflight also verifies that its target is the current remote `main`, then
freezes the validated baseline, target, PRs, groups, dependencies, run id, and
run attempt into a same-run, overwrite-disabled artifact. The frontend build
depends on this preflight.

After production approval, the deployment job first downloads that exact frozen
plan. Before reading deployment credentials or mutating either platform, it
checks the artifact's repository/run/SHA binding and verifies again that the
target is still current `main`. It does not re-read mutable PR or issue bodies
after approval. A queued or rerun release for an old main SHA fails closed.

Historical PRs and contracts before the last verified production baseline are
not scanned, so later metadata edits cannot create an unbounded historical
denial of service.

## Local validation

```bash
python .github/scripts/validate_production_workflow_guards.py
python .github/scripts/validate_frontend_release_workflow.py
python -m unittest 03_Scripts/tests/test_release_coordination_guard.py -v
```
