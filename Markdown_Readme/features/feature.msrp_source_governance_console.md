# MSRP Source Governance Console

## Document status

- Status: implementation in progress after user confirmation
- Goal version: 6.0
- Date: 2026-07-14
- Branch: codex/msrp-source-governance-console
- Worktree: /Users/litristan/Downloads/JATO_Analysis_System_msrp_governance
- Implementation authorization: granted on 2026-07-14 for the Governance-owned scope
- Parent system: [MSRP Self-Healing Governance System](./msrp_self_healing_governance_system.md)
- Agent feature: [MSRP Hermes Self-Iterating Agent](./feature.msrp_hermes_self_iterating_agent.md)
- Integration boundary: [MSRP Multi-Session Ownership and Integration Boundary](./msrp_multi_session_integration_boundary.md)

## 1. Executive decision

Build a database-backed MSRP governance control plane around the existing scraper, Dryrun, observations, Mapping, CurrentPrice, PriceHistory, and MSRP Monitor.

This console is not the Agent. It stores durable governance truth and gives humans visibility, evidence entry, approval, publication, and rollback.

Hermes is the self-iterating Agent; Composer is its runtime planning core. An editor requests Hermes diagnosis; Hermes chooses deterministic repair tools and optionally uses DPV4 for bounded reasoning. Hermes may also produce versioned memory, prompt, tool, evaluation, code, and contract proposals under different authority levels. DPV4 is only an API-key-based LLM Provider and is never exposed as an independent repair actor.

The control-plane flow is:

~~~mermaid
flowchart LR
    S["Scheduled finding or user report"] --> C["Governance Repair Case"]
    U["Human URL / official PDF"] --> E["Immutable Evidence Asset"]
    C --> H["Request Hermes Diagnosis"]
    E --> H
    H --> P["Repair Plan / Proposal"]
    P --> D["Targeted Dryrun / Replay"]
    D --> G["Source / Mapping / FX Gate"]
    G -->|"safe proposal"| R["Human review or policy-authorized action"]
    R --> V["Versioned publish / correction / rollback"]
    V --> M["Continuous monitoring"]
    M --> C
~~~

The screen must answer:

1. Which country/brand/model targets are healthy?
2. Is the problem source, parser, semantics, result, Mapping, FX, or runtime?
3. What evidence and Source Version are active?
4. What did Hermes try, what did DPV4 contribute, and what did Dryrun prove?
5. Is human URL/PDF evidence required?
6. What will publish, correction, or rollback change?

## 2. Problem statement

The current MSRP system already has:

- official source YAML and extractor profiles;
- scrapling, HTTP JSON/text, Playwright, and PDF extractors;
- Dryrun aggregation and source audits;
- JSON/Markdown repair backlogs;
- MsrpSource, MsrpObservation, CurrentPrice, and PriceHistory;
- JatoMsrpLink, MatchOverride, ReviewCase, and ReviewDecision;
- multi-source reconciliation and MSRP Monitor issue flags;
- Hermes progress, source-quality, and cost views.

The missing layer is durable, transactional governance.

Current limitations:

- executable YAML and mutable MsrpSource records can drift;
- artifacts are reports rather than resumable Cases;
- ReviewCase requires an observation and cannot represent a source that extracted nothing;
- source conflicts are mixed into Mapping Review;
- source issue flags are artifact-backed rather than durable workflow items;
- direct MsrpSource CRUD has no immutable version, Dryrun, publish, or rollback;
- materialization checks Mapping status but does not centrally enforce Source Gate plus Mapping Gate;
- no persistent Agent Run, Repair Episode, Composer Policy, or outcome loop exists;
- current LLM utilities do not make DPV4 a production Agent, and they should not.

## 3. Product goal

Create one operational console where:

- all Top30 and manually enrolled targets are visible;
- an editor can enter an official URL or upload an official PDF;
- health and Monitor anomalies create deduplicated Cases;
- Cases route by repair domain;
- Hermes diagnoses and composes a bounded repair plan;
- known deterministic paths are tried before DPV4;
- proposed changes run through typed validation, targeted Dryrun, replay, and Gate evaluation;
- high-risk changes require human publication;
- every change is versioned and reversible;
- original observations and local-currency facts are never silently overwritten.

## 4. Non-goals

- The console does not implement the Hermes Composer loop.
- The console does not call DPV4 directly.
- DPV4 does not own Cases, actions, approval, or publication.
- No neural network is trained or fine-tuned.
- The console does not edit CurrentPrice or PriceHistory in place.
- FX correction does not alter official local MSRP.
- Persistent anti-bot sites are not attacked with endless automated retries.
- Third-party sites are discovery leads, not canonical evidence.
- Source conflicts are not resolved by averaging prices.
- Mapping Review is not replaced.
- MSRP Monitor remains the market movement surface.
- The JATO monthly sales-workbook upload workflow is not reused as MSRP evidence storage.

## 5. Repair-domain routing

The console presents a unified Case queue with an explicit repair_domain.

| Domain | Typical issue | Primary owner/action | Publication result |
|---|---|---|---|
| runtime | timeout, DNS, 429/5xx, worker interruption | Hermes deterministic retry | no source change unless needed |
| source | URL/PDF missing, redirect, stale evidence, canonical source | Governance plus Hermes | new Evidence Asset or Source Version |
| parser | selector, regex, JSON path, PDF region, extractor drift | Hermes tool or DPV4-assisted proposal | new Source Version |
| semantic | MSRP versus offer/finance/lease, tax/validity ambiguity | deterministic evidence plus human when needed | semantic profile/version correction |
| result | normalization or materialization result is wrong | correction decision and reprocessing | new observation/rematerialization event |
| mapping | model/trim/powertrain alias or candidate ambiguity | Automatic Mapping Engine and Matching Review | Mapping decision/rule |
| fx | provider/rate date/stale normalization | approved FX tool and policy | new derived normalization calculation |

Mapping decisions remain authoritative in Mapping services and Matching Review. Governance displays linked Mapping cases and Gate state without duplicating their decision.

## 6. Anti-bot and manual evidence policy

Anti-bot is a distinct operating lane.

1. Hermes performs bounded retry and environment checks.
2. If the site remains blocked, the Case becomes manual_evidence_required.
3. The console shows Enter Official URL and Upload Official PDF as the primary commands.
4. The editor may provide an alternative official same-country source or an official PDF.
5. Hermes resumes deterministic validation from the new evidence.
6. Last-known-good remains active until an approved replacement exists.

A screenshot may be attached for diagnosis. It is not sufficient production price evidence by itself unless its official origin, timestamp, content, and extraction are validated by evidence policy.

## 7. Existing stack and reuse

### 7.1 Backend

Stay on:

- FastAPI routes and role dependencies;
- Pydantic request/response contracts;
- SQLAlchemy models and PostgreSQL schemas;
- Alembic migrations;
- Python Scraping Toolkit;
- existing systemd scheduling and deployment.

### 7.2 Reuse existing functions and objects

Reuse rather than fork:

- MsrpSource as stable runtime source identity/projection;
- MsrpObservation as immutable extracted result;
- CurrentPrice and PriceHistory materialization/history services;
- JatoMsrpLink and MatchOverride for accepted Mapping reuse;
- ScrapeBatch and existing ingest/Dryrun entry points;
- source YAML loader and existing extractor implementations;
- dryrun aggregation, accessibility audit, price-signal audit, repair backlog, and Hermes progress;
- existing PDF extraction and resumable-upload primitives where generic;
- existing canonical Mapping resolver and link service;
- existing DPV4 usage/cost ledger through the Agent feature.

Add one shared materialization eligibility service. Ingest, review, remap, correction, and replay must call the same Source Gate plus Mapping Gate logic.

### 7.3 Source configuration truth

The database is authoritative for governance state.

- Source Version is the immutable authoring and audit truth.
- Publishing serializes validated executable YAML deterministically.
- Existing MsrpSource is the active runtime projection and identity.
- The runner consumes only a published profile/export.
- A draft or direct CRUD edit cannot silently become production configuration.

## 8. Core domain model

### 8.1 Monitoring Target

Recommended table: msrp.monitoring_targets

| Field | Purpose |
|---|---|
| target_id | UUID identity |
| country, brand, model | stable target scope |
| trim_scope, powertrain_scope | optional narrowing |
| roster_type | country_top30, manual, or future roster |
| roster_rank | Top30 rank |
| monitoring_status | pending, active, degraded, manual_evidence_required, paused, retired |
| active_source_version_id | published primary version |
| fallback_source_version_id | approved last-known-good |
| schedule_json | cadence and run window |
| owner, notes | operating ownership |

### 8.2 Evidence Asset

Recommended table: msrp.source_evidence_assets

Evidence may be:

- official URL capture;
- uploaded official PDF;
- downloaded official PDF;
- immutable HTML/API snapshot;
- supporting screenshot.

Required metadata:

- source/final URL and redirect chain;
- official-domain verification;
- filename, MIME signature, size, storage key, SHA-256;
- captured time, document date, valid from/until;
- page count or content/text hash;
- source type and semantic-lane claim;
- creator and lifecycle state.

Evidence is immutable. A changed PDF or page capture creates a new asset.

Resumable PDF transfer state is stored separately in `msrp.evidence_upload_sessions`. An upload session records expected size/SHA-256, chunk size, received part hashes, expiry, row version, and the completed Evidence Asset reference. Upload sessions are mutable transport state; the completed Evidence Asset is immutable governance truth.

### 8.3 Source Version

Recommended table: msrp.source_versions

Fields:

- source_version_id, source_id, version number;
- typed profile_json;
- deterministic profile_yaml and SHA-256;
- Evidence Asset references;
- extractor name/type/version;
- semantic lane, currency, tax mode, validity;
- previous_version_id;
- validation, targeted Dryrun, replay, and conflict summaries;
- draft, validated, dryrun_passed, approved, published, superseded, rejected, rolled_back states;
- creator, approver, timestamps, and decision reason.

Only one published version is active for a source. Publish and supersede happen in one database transaction.

### 8.4 Governance Repair Case

Recommended table: msrp.repair_cases

Fields:

- case_id and repair_domain;
- target/source/observation/Mapping/FX references as applicable;
- case_type and failure classifier;
- severity, priority, first/last seen, occurrence count;
- recent run IDs and evidence;
- manual_evidence_required flag;
- linked Agent Runs and Proposals;
- open, diagnosing, awaiting_evidence, proposal_ready, dryrun_passed, awaiting_approval, resolved, rejected, paused, superseded states;
- resolution and recurrence references.

Open Cases are deduplicated by scoped identity, domain, and case type. New findings append occurrences rather than producing unlimited rows.

### 8.5 Repair Proposal

Recommended table: msrp.repair_proposals

A Proposal contains:

- Case and target/source references;
- proposal origin: manual, deterministic, or hermes_agent;
- Agent Run and Agent Step references;
- DPV4 provider/model/cost metadata only when Hermes used it;
- input Evidence Asset IDs and hashes;
- typed proposed profile or correction action;
- field-level diff and assumptions;
- unresolved questions and risk flags;
- schema, Dryrun, replay, conflict, and Gate results;
- author, reviewer, timestamps, and final decision.

DPV4 is never proposal_origin. It is optional reasoning metadata inside a Hermes-authored Proposal.

### 8.6 Result Correction Decision

Recommended table: msrp.result_correction_decisions

Fields:

- original observation/materialization references;
- correction type and reason;
- Evidence Asset and Source Version references;
- corrected semantic/normalization/mapping inputs;
- replay and Gate evaluation;
- replacement observation/rematerialization references;
- actor, approval, timestamps.

The original observation and PriceHistory record remain auditable.

### 8.7 FX Normalization Run

Recommended table: msrp.fx_normalization_runs

Fields:

- observation and local currency/value;
- approved FX provider;
- rate, effective date, retrieved time;
- policy version;
- normalized currency/value;
- status and failure reason;
- superseded run reference.

This table stores derived calculations, not official local MSRP.

### 8.8 Persisted Gate Decision

Table: `msrp.governance_gate_decisions`

Every evaluation is append-only and stores:

- target and observation identity;
- Source, Mapping, and optional FX Gate snapshots;
- local and normalized materialization eligibility;
- policy/evaluation context, actor, and evaluation time.

Target list/detail reads use the latest persisted decision rather than a Source Version preview. Result Correction and FX Normalization mutation contracts accept a `gateDecisionId`; they do not trust a caller-supplied eligible `gateResult`. The immutable Gate snapshot is copied into the correction/run for audit, while the foreign key preserves its authoritative origin.

### 8.9 Agent references

Agent Run, Agent Step, Repair Episode, Composer Policy Version, Playbook Version, and LLM invocation are owned by the Hermes Agent feature. Governance stores foreign references and read models rather than duplicate Agent state.

## 9. Human workflows

### 9.1 Add or replace an official URL

1. Editor selects a target and repair domain.
2. Editor enters an official URL and semantic claim.
3. Backend verifies official domain, redirect chain, accessibility, and immutable evidence capture.
4. The editor requests Hermes diagnosis.
5. Hermes selects deterministic tools and optionally DPV4.
6. Console displays typed Proposal, Agent plan, evidence, and field-level diff.
7. Targeted Dryrun shows extracted and rejected rows.
8. Source and Mapping Gate preview shows whether facts would remain frozen.
9. Editor submits and admin publishes.
10. Previous version remains fallback/rollback.

### 9.2 Upload an official PDF

1. Editor selects/creates a pending target.
2. Editor uploads the PDF or provides its official download URL.
3. Upload validates extension, MIME signature, size, SHA-256, and PDF structure.
4. Deterministic page text extraction records document and validity dates.
5. UI previews pages/text and requires explicit semantics when not provable.
6. Editor requests Hermes diagnosis.
7. Known PDF templates run first; DPV4 may propose page regions or regex only when needed.
8. Dryrun displays exact page evidence per output.
9. Approval publishes an immutable Source Version.

### 9.3 Repair a parser

1. Case shows last-known-good and current content fingerprint.
2. Hermes tries the applicable known template.
3. If ambiguity remains, DPV4 returns a typed patch proposal.
4. Backend validates the profile and reloads deterministic YAML.
5. Targeted Dryrun/replay compares counts, semantics, values, and rejected rows.
6. Human publishes in P0.

### 9.4 Repair a result

1. User or Monitor flags an observation/materialized result.
2. Console opens a result Case and preserves original evidence.
3. Hermes replays normalization, semantics, Mapping, and Gate decisions.
4. A Result Correction Decision explains old versus proposed outcome.
5. Approval creates a replacement observation/rematerialization event.
6. Existing history service closes/supersedes the effective period without deleting audit history.

There is no editable CurrentPrice value field.

### 9.5 Repair FX normalization

1. Console shows local price, rate provider, rate date, and normalized value separately.
2. Hermes validates the approved FX provider and effective date.
3. A new FX Normalization Run is produced.
4. Local price remains unchanged.
5. A provider/policy change requires admin approval.

### 9.6 Resolve Mapping

1. Governance displays Mapping Gate failure and candidate summary.
2. User opens Matching Review.
3. Review groups the same unknown pattern.
4. One decision creates a scoped reusable rule and resolves the cluster.
5. Governance re-evaluates materialization eligibility.

### 9.7 Add a non-Top30 target

1. Create manual pending Monitoring Target.
2. Add official evidence.
3. Request Hermes diagnosis and pass Source Gate.
4. Complete Mapping Gate.
5. Publish Source Version.
6. Activate schedule.
7. Target appears in Dryrun, coverage, CurrentPrice/history, MSRP Monitor, and Hermes metrics.

## 10. Multi-source conflict policy

Sources compete only inside the same lane:

~~~text
country + brand + model + trim/powertrain scope
+ semantic lane + currency + tax mode + validity period
~~~

Priority:

1. current official configurator/model price page;
2. current official PDF price list;
3. dated official campaign page in its campaign lane;
4. archived official evidence within validity;
5. explicitly approved official national importer/dealer/press material.

Resolver order:

1. hard compatibility;
2. evidence validity and active model status;
3. source priority;
4. evidence completeness and reproducibility;
5. successful extraction recency;
6. explicit governance decision.

When equally eligible sources exceed country absolute or percentage conflict threshold:

- preserve both observations and evidence;
- create/update a source/semantic conflict Case;
- keep last-known-good;
- freeze automatic source switching;
- require stronger dated evidence or recorded human decision.

Never average prices or pick the lower value.

## 11. Gate model

### 11.1 Source Gate

Requires:

- published Source Version;
- verified immutable official evidence;
- deterministic extraction;
- valid semantic lane, currency, tax, and dates;
- no blocking source/semantic conflict;
- no unresolved result correction;
- loader/schema and targeted Dryrun pass.

### 11.2 Mapping Gate

Requires:

- accepted link, override, or automatic Mapping decision;
- hard constraints pass;
- score and Top1/Top2 margin pass;
- accepted Mapping Policy Version;
- otherwise human approval.

### 11.3 FX Gate

Controls derived normalized values only. Local price can remain valid when FX is pending. The console visibly marks stale/pending normalization.

### 11.4 Materialization

All paths call one central evaluate_materialization_eligibility service. A failed Gate may retain observation/evidence, but cannot update accepted price facts.

## 12. API design

Prefix: /v1/msrp/source-governance

### 12.1 Query APIs

| Method | Path | Purpose |
|---|---|---|
| GET | /targets | filterable monitoring table |
| GET | /targets/{target_id} | target, health, Gates, source versions, Agent summary |
| GET | /targets/{target_id}/gate-decisions/latest | latest persisted Gate read model, or null before evaluation |
| GET | /cases | repair queue by domain/status/priority |
| GET | /cases/{case_id} | evidence, Agent Runs, Proposals, decisions |
| GET | /proposals/{proposal_id} | typed diff, Dryrun, replay, Gate result |
| GET | /source-versions/{version_id} | immutable version and audit |
| GET | /result-corrections/{id} | old/new result and evidence |
| GET | /fx-runs | normalization history |
| GET | /conflicts | unresolved source/semantic conflicts |
| GET | /audit-events | immutable actions |

### 12.2 Mutation APIs

| Method | Path | Minimum role | Purpose |
|---|---|---|---|
| POST | /targets | editor | create pending target |
| POST | /targets/{id}/url-evidence | editor | add official URL evidence |
| POST | /evidence-uploads/initiate | editor | begin resumable PDF upload |
| PUT | /evidence-uploads/{id}/parts/{part} | editor | upload verified chunk |
| POST | /evidence-uploads/{id}/complete | editor | validate immutable evidence |
| POST | /cases/findings | editor/service | record and deduplicate a typed repair finding |
| POST | /cases/{id}/request-hermes-diagnosis | editor/service | create bounded Agent Run |
| POST | /cases/{id}/agent-run-results | editor/service | consume a registered Hermes AgentRunResult and advance Case state |
| POST | /cases/{id}/proposals | editor/service | create manual/deterministic Proposal |
| POST | /proposals/{id}/dryrun | editor | run isolated verification |
| POST | /proposals/{id}/submit | editor | submit |
| POST | /source-versions | editor/service | create immutable version from a Dryrun-passed Proposal |
| POST | /source-versions/{id}/publish | admin | atomic publish |
| POST | /source-versions/{id}/rollback | admin | restore prior version |
| POST | /gate-decisions/evaluate | editor/service | centrally evaluate and persist Source/Mapping/FX eligibility |
| POST | /result-corrections | editor/service | create replay-based correction from a persisted gateDecisionId without fact mutation |
| POST | /result-corrections/{id}/approve | admin | approve replay-based correction |
| POST | /fx-runs | editor/service | create derived normalization from immutable local MSRP and a persisted gateDecisionId |
| POST | /fx-runs/{id}/approve | admin | approve policy-sensitive FX repair |
| POST | /cases/{id}/resolve | admin | close with evidence/reason |
| POST | /findings/source-runs | editor/service | consume SourceRunResult v1 and deduplicate a Case |
| POST | /findings/monitor-anomalies | editor/service | consume MonitorAnomaly v1 and deduplicate a Case |

There is no run-dpv4 endpoint. Only Hermes submits Agent proposals/results, and the callback rejects a run ID not already registered on the Case. Agent detail queries use the Agent feature API/read model. Mutations require idempotency keys or row-version checks.

## 13. Frontend interaction and layout

The page is an operational console.

### 13.1 Main layout

- compact header filters: country, brand/model, status, repair domain, owner, source type;
- summary strip: healthy, degraded, manual evidence required, Proposal ready, conflict, stale evidence, Mapping blocked, FX pending;
- dense table: country/rank, model, health, Gate state, repair domain, active source/version, last success, evidence freshness, Agent status, owner, commands;
- selecting a row opens a right-side FloatingDeck and keeps queue context.

### 13.2 Detail deck

Tabs:

- Overview and Gates;
- Evidence;
- Source Versions;
- Repair Cases and Proposals;
- Agent Runs;
- Agent Capabilities and Evaluations;
- Dryrun and Replay;
- Result Corrections;
- FX Normalization;
- Conflicts;
- Audit History.

Mapping details show a summary and Open Matching Review, not duplicated approve controls.

### 13.3 Commands

Primary contextual commands:

- Request Hermes Diagnosis;
- Enter Official URL;
- Upload Official PDF;
- Retry deterministic check;
- Run targeted Dryrun;
- Submit Proposal;
- Publish;
- Rollback;
- Open Matching Review.

There is no DPV4 button. DPV4 provider/model/tokens/cost appear only inside an Agent Step.

For anti-bot, the deck stops showing repeated automatic retry as the main action and clearly asks for URL/PDF evidence.

For Result repair, the UI compares original versus proposed observation and history impact; it does not expose an editable price input.

For FX repair, the UI visually separates local MSRP from normalized EUR and shows provider/rate/effective date.

### 13.4 Publish confirmation

Confirmation states:

- active and fallback Source Version;
- affected targets/schedules;
- Source/Mapping/FX Gate result;
- impacted observations/materializations;
- unresolved conflicts;
- rollback target;
- whether action is human publication or policy-authorized low risk.

Upload completion never hides a source switch.

## 14. Permissions and audit

Roles:

- viewer: read targets, evidence summaries, health, Agent Runs, Gates, versions, corrections, and conflicts;
- editor: add official evidence, request Hermes, create/edit Proposals, run Dryrun, submit;
- admin: publish, rollback, approve result/FX corrections, manage targets/policies, resolve conflicts;
- hermes_agent service account: read bounded evidence, create Runs/Steps/Proposals, call allowlisted tools, and perform only policy-authorized low-risk actions.

DPV4 has no database service account.

Audit records:

- actor/action/object;
- request, Case, Agent Run, and Step IDs;
- old/new version or result;
- evidence hashes;
- policy/playbook version;
- DPV4 invocation reference when used;
- reason, time, approval, and rollback link.

Secrets and full API-key-bearing headers are redacted.

## 15. Control-plane background work

Governance-owned jobs:

- source_case_sync: migrate/sync existing artifacts into Cases;
- source_evidence_extract: validate URL/PDF evidence;
- source_conflict_scan: compatible-lane comparison;
- source_freshness_scan: evidence expiry;
- source_governance_rollup: database-backed Console metrics;
- result_correction_outcome: later correction verification;
- fx_normalization_audit: stale/missing normalized values.

Agent planning/execution jobs live in the Hermes Agent feature. Existing systemd scheduling remains the production scheduler unless a later architecture decision changes it.

## 16. Success metrics and baseline

Current 2026-07-08 artifact baseline:

- 611 source targets;
- 199 complete passes;
- 230 deterministic transport retry candidates;
- 21 last-known-good drift candidates;
- 38 configured structural repairs;
- 8 anti-bot/manual-evidence candidates;
- 115 placeholder candidates owned by the P0 source-fill session.

Planning targets:

- all production changes have evidence, version diff, Dryrun, actor, and rollback;
- zero direct CurrentPrice/PriceHistory edits;
- zero local-price mutations during FX repair;
- persistent anti-bot enters manual evidence rather than infinite retry;
- deterministic Composer recovery reaches approximately 360 to 406 targets on the measured baseline;
- repeated verified playbooks reach approximately 394 to 455 targets before placeholder fill;
- after P0 source fill, initial total Source Gate planning range is 75% to 89%;
- Mapping automation is evaluated at at least 99.5% audited precision;
- manual review operates on new patterns/clusters, not individual repeated rows;
- median critical Case detection is below 24 hours;
- median non-blocked Case age is below 7 days.

Predicted ranges must be replaced by rolling measured metrics after implementation.

## 17. Rollout

### P0: human-governed control plane

- Monitoring Targets, Evidence Assets, Source Versions, Repair Cases, Proposals;
- Result Correction Decision and FX Normalization Run;
- import Top30 roster, source identities, and artifact backlog;
- URL/PDF evidence workflows;
- Request Hermes Diagnosis integration;
- typed diff, targeted Dryrun, replay, and Gate preview;
- Source Version publish and rollback;
- linked clustered Matching Review;
- human publication of Source Version and Composer Policy;
- high-confidence Mapping and low-risk retry may automate under explicit policy.

### P1: bounded automated actions

- allowlisted low-risk Source/parser actions after replay;
- database-backed Agent outcomes and Composer policy comparison;
- evidence expiry and last-known-good automation;
- automatic rollback when monitored regression occurs;
- repair-effectiveness and human-time metrics.
- versioned prompt, template, tool, and evaluation candidates from verified Episodes.

### P2: scale

- broader approved rosters;
- brand/page-family Composer playbooks;
- country-specific thresholds and SLAs;
- cross-country reuse only after evidence fingerprint and replay;
- stability, recurrence, cost, and queue analytics.
- reviewable extractor/tool/code and contract proposals with authority-specific approval.

No planned phase adds neural-network training. A future training proposal would be a separate explicit architecture decision.

## 18. Implementation scope for this branch

When implementation is later authorized, this console branch owns:

- governance models/migrations/schemas/repositories/services/routes/tests;
- feature-local React/TypeScript page/components/API types/tests;
- artifact adapters and targeted Dryrun integration;
- evidence storage and PDF integration;
- central Gate read model and materialization eligibility integration assigned to this PR;
- Console documentation and runbook.

The Hermes Agent worker, Composer, Playbook, Episode, capability/evaluation proposal system, and DPV4 Provider implementation should be a separate implementation slice/PR to preserve one-session/one-worktree/one-branch ownership.

Shared files receive minimal registration only:

- backend main/router registration;
- frontend App route/navigation;
- shared materialization call site only where required;
- no unrelated BOM, Order Genius, AstrBot, COC, or performance changes.

## 19. Test strategy

### Backend

- constraints and immutable Source Version behavior;
- evidence MIME/SHA/redirect/official-domain validation;
- Case deduplication and domain routing;
- Proposal origin and Agent/DPV4 metadata boundary;
- targeted Dryrun isolation and YAML round-trip;
- source/semantic conflict and last-known-good freeze;
- central Source/Mapping Gate eligibility;
- result correction without in-place observation/fact mutation;
- FX recomputation preserving local price;
- permissions, idempotency, publish, rollback, and audit.

### Frontend

- target filters/table/FloatingDeck;
- repair-domain state and commands;
- URL and resumable PDF upload;
- anti-bot manual evidence state;
- Agent Run and DPV4 metadata display;
- Proposal diff, Dryrun/replay, publish, rollback;
- result correction comparison without editable price;
- FX local-versus-normalized separation;
- Matching Review deep link;
- role visibility, empty/error states, desktop/mobile layout.

### Operational

- run small SE/FI or comparable target slice first;
- verify server networking separately from local proxy;
- prove execution without an open Codex/ChatGPT session;
- verify Governance and Agent read the same database facts;
- verify last-known-good remains active on failure;
- verify no mock-only state is needed for the production Console.

## 20. Acceptance criteria

P0 is accepted when:

1. all current targets are visible regardless of the latest run.
2. an editor can add official URL or PDF evidence without hand-editing production YAML.
3. anti-bot Cases stop and request manual URL/PDF evidence.
4. every Case has an explicit repair domain.
5. the user requests Hermes diagnosis; no direct DPV4 business action exists.
6. Agent plan, tool calls, DPV4 metadata, Dryrun, and Gate result are visible.
7. a Proposal can Dryrun/replay without changing the active source.
8. an admin can publish and rollback an immutable Source Version.
9. failed replacement leaves last-known-good active.
10. semantic lanes never overwrite one another.
11. source conflicts freeze switching and keep both evidence sets.
12. Result repair preserves original observations and uses correction/rematerialization events.
13. FX repair preserves official local MSRP.
14. Mapping exceptions open clustered Matching Review and accepted rules are reusable.
15. local price materialization requires Source Gate plus Mapping Gate.
16. every publish/correction includes evidence, policy, actor, time, and rollback/supersession link.
17. the workflow runs server-side without an open GPT session.
18. production state is database-backed and not mock-only.
19. Gate evaluations are append-only, target list/detail read the latest persisted decision, and Result/FX mutations require its `gateDecisionId`.
20. a Hermes terminal callback must reference a Run already registered on the Case; DPV4 can appear only as Hermes invocation metadata.

## 21. Risks and controls

| Risk | Control |
|---|---|
| DPV4 appears to be an autonomous repair Agent | only Hermes owns Runs/Plans; DPV4 is invocation metadata |
| plausible but wrong parser proposal | typed schema, existing loader, Dryrun, replay, Gate, human P0 publish |
| anti-bot burns retries/tokens | bounded retry then manual_evidence_required |
| Result repair rewrites history | immutable evidence/observation and correction/rematerialization event |
| FX repair changes source truth | separate derived FX lane; local value immutable |
| direct source CRUD drifts from YAML | Source Version is governance truth; deterministic published export |
| duplicate Cases | scoped deduplication and occurrence aggregation |
| source conflict is hidden | store both, freeze last-known-good, explicit decision |
| Agent bypasses Gate | central eligibility service; no direct fact-write tool |
| cross-session conflict | dedicated worktree/branch and separate Agent implementation slice |

## 22. Final direction

The Console is the human-visible governance control plane. Hermes is the self-iterating Agent, and Composer is one of its core runtime components. DPV4 is a replaceable LLM Provider reached through a server-side API key.

Self-healing spans source, parser, semantic, result, Mapping, FX, and runtime repairs. Hermes may improve orchestration, memory, prompts, routing, templates, tools, evaluation, and reviewable code/contract proposals. Protected changes remain human-approved and official price truth remains deterministic.

Official evidence, deterministic extraction, Source Gate, Mapping Gate, immutable history, and rollback remain responsible for correctness.

## 23. Implementation authorization

The user authorized implementation on 2026-07-14 after confirming the multi-session boundary. This branch may implement only the Governance-owned paths in section 18. Shared route/navigation registration, source extractors, Monitor behavior, and Hermes runtime internals remain outside this branch and are deferred to the integration or owning PR.

## 24. Current implementation snapshot

Implemented in the feature branch:

- feature-local SQLAlchemy models and Alembic revision `20260714_0044`;
- Monitoring Target, persisted Gate Decision, Evidence Asset, resumable PDF upload, Source Version, Case, Proposal, Result Correction, FX Run, and audit persistence;
- SourceRunResult v1, MonitorAnomaly v1, AgentRunRequest v1, AgentRunResult v1, GateDecision v1, and MappingDecision v1 contracts;
- one central Source/Mapping/FX materialization eligibility evaluator, append-only GateDecision write path, and latest target read model;
- idempotent mutation audit, Case deduplication, atomic publish, and rollback;
- verified EvidenceReference reuse, registered Hermes run/proposal identity, AgentRunResult callback, and persisted Gate links for Result/FX repair;
- feature-local React/TypeScript Console, target table, detail deck, URL/PDF evidence, Hermes request, Proposal submission, publish/rollback, Result/FX read models, and Matching Review deep link;
- focused backend and frontend tests.

Deferred to the integration PR:

- import/register the feature-local SQLAlchemy metadata module for autogenerate;
- include the Governance router in FastAPI `main.py`;
- register the Console route and navigation in `App.tsx`;
- connect the central eligibility evaluator to each existing fact-materialization call site;
- dispatch AgentRunRequest v1 to the separately owned Hermes runtime.
