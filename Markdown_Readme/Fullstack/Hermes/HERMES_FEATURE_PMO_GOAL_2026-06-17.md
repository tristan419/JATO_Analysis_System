# Hermes Feature PMO Goal

> Date: 2026-06-17
> Workstream: Hermes
> Target featureId: `feature.hermes_feature_pmo_cockpit`
> Current base: `codex/hermes-history-progress-cockpit-clean`

## 1. Product Goal

Hermes should become the platform development assistant for JATO Analysis System.

It should help the developer answer four questions before, during, and after each feature:

- What was already planned in PRD or feature documentation?
- What existing frontend components, backend services, APIs, data objects, tests, and documents can be reused?
- Which phase is each feature in now, and what evidence proves that status?
- What is the safest next action for each active worktree, branch, and PR?

Hermes is not intended to become an autonomous code modifier. The default operating mode remains read-only governance, recommendation, and evidence tracking.

## 2. Current Progress

Hermes already has these working foundations:

- Registry foundation: `hermes/*.yaml` tracks features, sources, pipelines, prompts, artifacts, proposals, and gaps.
- DevSync foundation: `hermes/dev_events/dev_events.jsonl` and `hermes/registry/features.yaml` track implementation events and feature evidence.
- Sentinel foundation: Sentinel notifications are deduplicated by stable fingerprints and surfaced as a control-plane inbox.
- Cost foundation: Country Copilot and AstrBot usage records are normalized through `hermes_cost_ledger_service.py`.
- History cockpit MVP: git, DevSync, evidence, gaps, Sentinel, pipeline, deploy, and usage records are aggregated into:
  - `GET /v1/hermes/history/events`
  - `GET /v1/hermes/history/clusters`
  - `GET /v1/hermes/progress/features`
  - `GET /v1/hermes/progress/swimlanes`
  - `GET /v1/hermes/workflow/cockpit`
- Frontend cockpit MVP:
  - `HermesHistoryMap.tsx`
  - `HermesProgressSwimlane.tsx`
  - `HermesWorkflowView.tsx`
- Feature PMO MVP:
  - Feature goals are parsed from Markdown contracts and merged with git/history/test evidence.
  - Local worktree and branch metadata is linked read-only from `git worktree list --porcelain`.
  - The PMO board surfaces lifecycle state, evidence checklist, reuse candidates, branch, and local worktree path.
  - Local worktree dirty files are classified as in-scope, out-of-scope, unknown, or generated/runtime against the active feature workstream.

The current gap is no longer basic feature parsing. The next gap is PR metadata and richer evidence capture: Hermes can infer feature status from Markdown, git, events, tests, and worktree state, but it still needs stronger PR links, smoke evidence attachments, and an append-only implementation log workflow.

## 3. Target Operating Model

The development model should be:

```text
MD = feature contract and planning source
PR = reviewable delivery unit
Git = actual implementation history
Tests = executable proof
Evidence = smoke, deploy, verification, and audit proof
Hermes = aggregated control plane
```

Hermes should not treat Markdown as the only source of truth. Markdown records intent and plan; git and tests record implementation; evidence records verification; PRs record review.

Recommended local development rule:

```text
one session = one worktree = one branch = one PR
```

Each feature should have one stable `featureId`. Each PR should either implement one feature slice or explicitly state which feature slices it touches.

## 4. Atomicity Rule

Hermes should track work at feature-slice level, not at every function or CSS tweak.

Good tracking units:

- `feature.hermes_feature_pmo_cockpit`
- `feature.astrbot_mcp_tool_registry`
- `feature.msrp_source_repair_backlog`
- `feature.config_comparison_source_evidence`
- `feature.bom_colour_rule_library`

Bad tracking units:

- Rename one helper function
- Change one margin value
- Add one local variable
- Move one import

Small code changes should be attached to their parent feature slice through git commit, PR, or DevSync evidence.

## 5. Feature State Machine

Hermes should use a light state machine for feature lifecycle. Checklist items are evidence, not independent state machines.

Main lifecycle:

```text
draft
-> prd_ready
-> ready_for_dev
-> in_progress
-> implemented
-> tested
-> ready_for_pr
-> in_review
-> merged
-> deployed
-> verified
-> done
```

Side states:

```text
blocked
archived
reopened
```

Transition rules:

- `draft -> prd_ready`: PRD or feature MD exists with goal, scope, acceptance criteria, and owner/workstream.
- `prd_ready -> ready_for_dev`: Hermes intake identifies affected files, APIs, tests, docs, and reuse candidates.
- `ready_for_dev -> in_progress`: active branch/worktree or DevSync implementation event exists.
- `in_progress -> implemented`: code paths changed and linked to featureId.
- `implemented -> tested`: backend or frontend test evidence exists.
- `tested -> ready_for_pr`: tests pass and no blocking open gaps are attached.
- `ready_for_pr -> in_review`: PR exists.
- `in_review -> merged`: PR merged into main.
- `merged -> deployed`: deploy metadata shows the commit in expected or actual production release.
- `deployed -> verified`: smoke evidence or manual verification evidence exists.
- `verified -> done`: no open blocking gaps remain.
- Any development state can enter `blocked` when a blocking gap, failed pipeline, missing dependency, or explicit user hold exists.
- `verified` or `done` can enter `reopened` if a regression, missing scope, or follow-up bug is linked.

## 6. Evidence Checklist

Every tracked feature should expose this checklist:

```md
- [ ] PRD / feature MD exists
- [ ] Reuse candidates identified
- [ ] Backend contract defined
- [ ] Backend implemented
- [ ] Frontend contract defined
- [ ] Frontend implemented
- [ ] Unit tests added or updated
- [ ] Type/build checks passed
- [ ] Smoke evidence attached
- [ ] Docs updated
- [ ] PR opened
- [ ] PR merged
- [ ] Deployed
- [ ] Verified
```

Hermes should compute each checkbox from evidence where possible:

| Checklist item | Preferred evidence |
|---|---|
| PRD / feature MD exists | `Markdown_Readme/**` file with `featureId` |
| Reuse candidates identified | Hermes intake report or reuse radar output |
| Backend contract defined | route/schema/service file or API section in MD |
| Backend implemented | backend code diff linked to featureId |
| Frontend contract defined | TypeScript type/API client/page section in MD |
| Frontend implemented | TS/TSX code diff linked to featureId |
| Unit tests added or updated | test file diff and passing test output |
| Type/build checks passed | recorded command result or CI check |
| Smoke evidence attached | screenshot, Playwright result, curl output, or evidence ledger record |
| Docs updated | feature MD, README, or Hermes registry diff |
| PR opened | GitHub PR metadata |
| PR merged | merge commit or PR merged state |
| Deployed | `deploy_release.json`, `deploy_expected.json`, or pipeline status |
| Verified | explicit smoke/manual verification evidence |

Manual checkboxes are allowed only as audited overrides. Hermes should show the evidence source next to every checked item.

## 7. Markdown Policy

Markdown is the human-readable contract. Hermes can read it and recommend updates, but should not silently rewrite it.

Recommended document structure:

```md
---
featureId: feature.example
workstream: Example
status: in_progress
owner: codex
branch: codex/example-feature
---

# Feature Title

## Goal

## Scope

## Reuse Candidates

## Phase Checklist

## Acceptance Criteria

## Implementation Log

## Decisions / Scope Changes
```

When a feature changes, update Markdown by appending:

- `Current Status`
- `Implementation Log`
- `Decisions / Scope Changes`
- `Evidence`

Do not rewrite the original PRD goal unless the product decision itself changed.

## 8. Reuse Radar Goal

Before implementation, Hermes should suggest existing reusable assets.

Target questions:

- Is there already a frontend component for this interaction pattern?
- Is there already an API client method or type contract?
- Is there already a backend service/repository function for this data path?
- Is there already a test pattern that should be copied?
- Is there already a document that explains this feature family?

First implementation can be rules-based:

| Need | Candidate source |
|---|---|
| Page layout / drawer / deck controls | `06_AppPlatform/frontend/src/components/deckControls/` |
| API method | `06_AppPlatform/frontend/src/api/client.ts` |
| TypeScript contract | `06_AppPlatform/frontend/src/types/` |
| Backend route | `06_AppPlatform/backend/app/api/routes/` |
| Backend service | `06_AppPlatform/backend/app/services/` |
| Repository/data access | `06_AppPlatform/backend/app/infra/` |
| Existing tests | `06_AppPlatform/backend/tests/unit/`, `06_AppPlatform/frontend/src/tests/unit/` |
| Feature docs | `Markdown_Readme/features/`, `Markdown_Readme/Fullstack/` |

Later versions can add semantic search, embeddings, or GitNexus indexing.

## 9. MCP / Skill Growth Goal

Hermes can eventually become the local platform assistant that knows which MCP servers, tools, and skills are useful for each feature type.

Initial safe scope:

- Inventory installed MCP/tool/skill capabilities.
- Map capabilities to feature tasks.
- Record which capability helped which feature.
- Track model/tool usage cost in Hermes cost ledger.
- Recommend capability usage before development.

Explicitly out of scope for the first implementation:

- Auto-installing MCP servers.
- Auto-changing global Codex configuration.
- Auto-running commands without user approval.
- Auto-deploying code.

## 10. UI Goal

The Hermes UI should become a control surface in this order:

```text
Ask Hermes
Git History Cluster
Feature PMO Board
Progress Swimlane
Workflow Cockpit
Sentinel Inbox
Cost / Activity / Toolchain drilldowns
```

The Feature PMO Board should show:

- Active feature cards grouped by workstream.
- State machine status.
- Evidence checklist.
- Next action.
- Reuse candidates.
- Linked branch, worktree, PR, docs, tests, and smoke evidence.
- Worktree dirty-file scope: in current feature scope, out of scope, unknown/shared, or generated/runtime.
- Blocking gaps.

Frontend layout expectation:

- Dense dashboard, not a marketing page.
- One feature card per tracked feature slice.
- Detail drawer or side panel for evidence, files, decisions, and checklist.
- Empty state that tells the user how to create or link a feature MD.

## 11. Backend Goal

Implement this as read-only aggregation first.

Expected backend services:

```text
hermes_feature_goal_service.py
hermes_feature_state_machine.py
hermes_reuse_radar_service.py
```

Expected endpoints:

```text
GET /v1/hermes/goals/features
GET /v1/hermes/goals/features/{featureId}
GET /v1/hermes/goals/swimlanes
GET /v1/hermes/reuse/candidates?featureId=...
```

The backend should reuse existing Hermes readers:

- `hermes_history_service.py`
- `hermes_devsync_service.py`
- `hermes_cost_ledger_service.py`
- registry YAML readers
- evidence/gap JSONL/YAML readers

Do not duplicate path parsing, event normalization, cost loading, or feature registry merging logic where existing helpers can be reused.

## 12. First Implementation PR

Suggested branch:

```text
codex/hermes-feature-pmo-goal-board
```

Suggested PR scope:

- Parse feature MD frontmatter and checklist blocks.
- Add pure state-machine function.
- Add read-only `GET /v1/hermes/goals/features`.
- Add unit tests for state transitions and checklist evidence mapping.
- Add frontend Feature PMO Board below Git History Cluster.
- Add smoke test for empty, active, blocked, and ready-for-PR feature cards.

Do not include MCP installation or semantic embeddings in the first PR.

## 13. Acceptance Criteria

- Hermes can list active feature goals from Markdown and registries.
- Each feature has a computed lifecycle state.
- Each feature has a checklist with evidence source labels.
- Missing evidence produces a clear next action.
- Feature state does not advance to `ready_for_pr` without tests or explicit audited override.
- Feature state does not advance to `verified` without smoke/manual verification evidence.
- Reuse candidates are visible before implementation.
- Worktree status shows whether dirty files belong to the selected feature/workstream and separates generated runtime files from real code edits.
- Existing History Cluster, Progress Swimlane, Workflow Cockpit, Sentinel, and Cost views remain reachable.
- Hermes remains read-only by default.

## 14. Non-Goals

- Do not build a full Jira clone.
- Do not track every function-level edit as a feature.
- Do not use Markdown as the only truth source.
- Do not let Hermes silently mutate PRDs or registries.
- Do not auto-run deployment, install MCP servers, or execute shell commands from this board.

## 15. Decision

Use a lightweight feature state machine plus evidence checklist.

Use Markdown as the feature contract, PR as the delivery unit, and Hermes as the aggregator.

This keeps Hermes useful as a development memory and project control plane without turning it into an unsafe automation layer.
