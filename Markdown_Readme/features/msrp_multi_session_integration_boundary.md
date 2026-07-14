# MSRP Multi-Session Ownership and Integration Boundary

## Document status

- Status: active implementation and integration contract
- Date: 2026-07-14
- Implementation authorization: confirmed on 2026-07-14
- Applies to: MSRP source repair, historical price monitor, Hermes Agent improvement, and MSRP self-healing governance sessions
- Parent design: [MSRP Self-Healing Governance System](./msrp_self_healing_governance_system.md)

## 1. Executive decision

The four work streams must use:

~~~text
one session = one worktree = one branch = one PR
~~~

No two active sessions may share a worktree, even when they intend to edit different files. Shared registration files are frozen during parallel development and changed once in an integration branch.

The four modules communicate through versioned contracts:

~~~mermaid
flowchart LR
    S["Source Repair<br/>official evidence and observations"] -->|"SourceRunResult v1"| G["Self-Healing Governance<br/>cases, versions, Gates"]
    M["History Monitor<br/>accepted facts and anomalies"] -->|"MonitorAnomaly v1"| G
    G -->|"AgentRunRequest v1"| H["Hermes Self-Iterating Agent"]
    H -->|"AgentRunResult / Proposal v1"| G
    G -->|"GateDecision v1"| F["CurrentPrice / PriceHistory"]
    F --> M
    R["Matching Review"] -->|"MappingDecision v1"| G
~~~

## 2. Current-state audit and immediate risk

### 2.1 Confirmed and required worktrees

| Work stream | Current worktree/branch | Required steady state |
|---|---|---|
| source repair | /Users/litristan/Downloads/JATO_Analysis_System_msrp on codex/msrp-scraping | may remain only after Monitor changes are committed and moved out |
| historical monitor | currently sharing /Users/litristan/Downloads/JATO_Analysis_System_msrp and codex/msrp-scraping | move to /Users/litristan/Downloads/JATO_Analysis_System_msrp_monitor on codex/msrp-history-monitor, or record another dedicated worktree |
| MSRP Hermes Agent | actual dedicated path still to be recorded | reserve /Users/litristan/Downloads/JATO_Analysis_System_msrp_hermes on codex/msrp-hermes-self-healing if none exists |
| self-healing governance | /Users/litristan/Downloads/JATO_Analysis_System_msrp_governance on codex/msrp-source-governance-console | feature-local backend, migration, Console, tests, and integration manifest implemented; shared registration remains deferred |

The Monitor path/branch is a required destination, not permission to discard or move current uncommitted work destructively.

### 2.2 Existing collision evidence

The current msrp-scraping worktree contains simultaneous uncommitted changes in:

- source/reference/repair artifacts;
- msrp_workflow route;
- msrp_monitoring_service;
- MsrpMonitorPage;
- frontend api/client and types/index;
- Hermes route and cost report;
- DPV4 usage service.

The astrbot worktree currently contains simultaneous changes in:

- msrp_lookup_service;
- jato_msrp_source_draft_service;
- ScrapingToolkit validation;
- backend main;
- frontend App route;
- Hermes runtime artifacts.

These are mixed ownership states. They must not be used as the starting point for a new shared MSRP self-healing implementation without first separating commits/worktrees.

Project rules reserve the astrbot worktree/branch for AstrBot, CountryCopilot, agent, and MCP changes in that product line. It must not become the MSRP Hermes implementation worktree merely because generic Composer code exists there. Likewise, Hermes History Cockpit worktrees own their history/PMO UI and are not the MSRP Agent line. Create or explicitly assign a dedicated MSRP Hermes worktree.

The governance and msrp-scraping branches also have a distant merge base and substantial independent histories. Do not merge one entire branch into the other as an integration shortcut.

### 2.3 Grandfathered Monitor patch

The Monitor session has confirmed one coherent five-file patch in the shared msrp worktree:

~~~text
06_AppPlatform/backend/app/api/routes/msrp_workflow.py
06_AppPlatform/backend/app/services/msrp_monitoring_service.py
06_AppPlatform/frontend/src/api/client.ts
06_AppPlatform/frontend/src/pages/MsrpMonitorPage.tsx
06_AppPlatform/frontend/src/types/index.ts
~~~

Its behavior is:

- UI modes are Live, Sweden demo, and Switzerland demo;
- Switzerland demo is the default and locks CH;
- Sweden demo locks SE;
- the legacy sweden_swiss_demo remains API compatibility only;
- demo/backfilled scenario values are not official historical conclusions;
- the local 404 is an environment/deployment mismatch because the remote backend does not yet expose the new route.

Until this patch is committed and separated:

- Source Repair, Governance, and Hermes sessions must not edit these five files.
- The Monitor session owns the current diffs and their verification.
- Governance must not create Cases from demo data or treat it as CurrentPrice/PriceHistory evidence.
- The 404 is resolved by the Monitor/deployment integration path, not by adding Governance compatibility code.

## 3. Session A: MSRP source repair

### 3.1 Product responsibility

Own official-source acquisition and deterministic observation production:

- official URL/PDF source fill;
- source profiles and page-family templates;
- fetch/extract/normalize;
- current-source Dryrun;
- retry/accessibility diagnostics;
- parser implementation;
- source health findings;
- immutable observation/evidence output through existing ingestion.

### 3.2 Owned paths

Primary ownership:

~~~text
07_ScrapingToolkit/jato_scraper/**
07_ScrapingToolkit/sources/**
07_ScrapingToolkit/source_drafts/**
07_ScrapingToolkit/tests/**
03_Scripts/batch_dryrun.py
03_Scripts/batch_ingest.py
03_Scripts/msrp_dryrun_aggregate.py
03_Scripts/msrp_source_*.py
03_Scripts/msrp_reference_evidence.py
03_Scripts/msrp_official_price_signal_audit.py
03_Scripts/run_msrp_*.sh
03_Scripts/tests/test_*msrp*source*.py
03_Scripts/diagnostics/artifacts/dryrun_report.*
03_Scripts/diagnostics/artifacts/msrp_source_*.*
~~~

Feature-local backend additions may use:

~~~text
06_AppPlatform/backend/app/services/msrp_source_*.py
06_AppPlatform/backend/tests/unit/test_msrp_source_*.py
~~~

### 3.3 Must not modify

- MsrpMonitorPage and historical-monitor components;
- Governance Console feature folder, routes, schemas, tables, or migrations;
- MSRP Hermes Agent services, policies, prompts, or memory;
- generic Hermes route;
- Mapping Review UI;
- CurrentPrice/PriceHistory business semantics;
- shared frontend App/api/client/types files during parallel work.

### 3.4 Output contract

The source session emits SourceRunResult v1 and evidence/observation references. It does not create governance Cases or Agent Plans directly.

## 4. Session B: MSRP historical price monitor

### 4.1 Product responsibility

Own the read-side business experience:

- CurrentPrice and PriceHistory queries;
- historical/backfill evidence lane;
- price increase/drop and offer movement classification;
- local versus EUR-normalized presentation;
- country/model/trim drilldowns;
- FloatingDeck evidence timeline;
- source/result anomaly reporting to Governance.

### 4.2 Owned paths

Primary ownership:

~~~text
06_AppPlatform/backend/app/services/msrp_monitoring_service.py
06_AppPlatform/backend/app/api/routes/msrp_monitoring.py
06_AppPlatform/backend/tests/unit/test_msrp_monitoring_*.py
06_AppPlatform/frontend/src/pages/MsrpMonitorPage.tsx
06_AppPlatform/frontend/src/features/msrp-monitor/**
06_AppPlatform/frontend/src/tests/unit/msrpMonitor*.tsx
03_Scripts/diagnostics/artifacts/msrp_backfill/**
03_Scripts/msrp_price_alert_review_queue.py
~~~

New API types and calls should be feature-local:

~~~text
06_AppPlatform/frontend/src/features/msrp-monitor/api.ts
06_AppPlatform/frontend/src/features/msrp-monitor/types.ts
~~~

The current five-file grandfathered patch may use msrp_workflow.py, api/client.ts, and types/index.ts only to preserve its already completed work. After that commit, new Monitor changes follow the feature-local route/API/type boundary.

### 4.3 Must not modify

- source YAML, source drafts, extractors, or source templates;
- Source Version, Evidence Asset, Repair Case, or Agent tables;
- Hermes Composer, DPV4 Provider, prompt/model routing;
- Mapping decision rules;
- direct production source configuration;
- shared msrp_workflow route when a feature-local monitoring route can be added;
- broad api/client.ts or types/index.ts during parallel work.

### 4.4 Output contract

Monitor reads only accepted facts and evidence. It emits MonitorAnomaly v1 to Governance and never edits a source or invokes DPV4.

## 5. Session C: MSRP Hermes Self-Iterating Agent

### 5.1 Product responsibility

Own Agent capability:

- Agent Run/Step execution;
- Composer and Planner;
- Repair Playbooks;
- episodic/semantic/procedural memory;
- deterministic Tool Registry adapters;
- DPV4 Provider and usage/cost routing;
- capability/evaluation proposals;
- prompt/template/tool/evaluation iteration;
- reviewable extractor/tool/code proposals;
- replay, shadow evaluation, and rollback recommendation.

### 5.2 Owned paths

Use a dedicated namespace:

~~~text
06_AppPlatform/backend/app/services/msrp_hermes_agent/**
06_AppPlatform/backend/app/api/routes/msrp_hermes_agent.py
06_AppPlatform/backend/app/api/msrp_hermes_agent_schemas.py
06_AppPlatform/backend/app/workers/msrp_hermes_agent_worker.py
06_AppPlatform/backend/tests/unit/test_msrp_hermes_agent_*.py
hermes/msrp_agent/**
03_Scripts/hermes/msrp_agent_*.py
~~~

DPV4 adapter belongs under the Agent namespace or a dedicated shared LLM provider module agreed by integration. Do not place new Agent workflow state inside generic country-chat or lease services.

### 5.3 Must not modify

- source YAML/extractors;
- MsrpMonitorPage or monitor service;
- Source Governance page or publication service;
- CurrentPrice/PriceHistory directly;
- generic hermes.py route when a namespaced route can be added;
- generic DataManagementPage;
- Source/Mapping Gate implementation;
- production deployment, secrets, or shared feature registries during parallel work.

### 5.4 Output contract

Hermes consumes AgentRunRequest v1 and returns AgentRunResult v1, typed Proposals, Tool Executions, Evaluations, and Capability Proposals.

It has no direct fact-write or source-publish permission.

## 6. Session D: MSRP self-healing governance

### 6.1 Product responsibility

Own the integration control plane:

- Monitoring Target;
- Evidence Asset;
- Source Version;
- Repair Case and Repair Proposal;
- Result Correction Decision;
- FX Normalization Run;
- Source/Mapping/FX Gate read model;
- central materialization eligibility service;
- Source Governance Console;
- Agent Run/Capability read surfaces;
- human URL/PDF entry;
- approval, publication, rollback, audit;
- cross-module contracts and system documentation.

### 6.2 Owned paths

~~~text
Markdown_Readme/features/feature.msrp_source_governance_console.md
Markdown_Readme/features/feature.msrp_hermes_self_iterating_agent.md
Markdown_Readme/features/msrp_self_healing_governance_system.md
Markdown_Readme/features/msrp_multi_session_integration_boundary.md
06_AppPlatform/backend/app/api/routes/msrp_source_governance.py
06_AppPlatform/backend/app/api/msrp_source_governance_schemas.py
06_AppPlatform/backend/app/services/msrp_source_governance/**
06_AppPlatform/backend/app/services/msrp_materialization_eligibility_service.py
06_AppPlatform/backend/app/infra/msrp_source_governance_repository.py
06_AppPlatform/backend/tests/unit/test_msrp_source_governance_*.py
06_AppPlatform/frontend/src/features/msrp-source-governance/**
~~~

This session is the P0 schema authority for new shared self-healing entities and Alembic migration order.

### 6.3 Must not modify

- source profile YAML or extractor logic;
- historical Monitor charts/business classification;
- generic Hermes implementation;
- DPV4 client internals;
- Mapping Review interaction except a feature-local deep link;
- existing source repair artifacts except read-only adapters;
- shared registration files until integration.

### 6.4 Output contract

Governance owns the Case and version lifecycle, accepts findings, creates Agent Run requests, evaluates Gates, and publishes only after authority policy.

## 7. Shared files: integration-owner only

During parallel implementation, freeze:

~~~text
06_AppPlatform/backend/app/main.py
06_AppPlatform/backend/app/db/models.py
06_AppPlatform/backend/app/db/session.py
06_AppPlatform/backend/app/api/routes/msrp_workflow.py
06_AppPlatform/backend/app/api/routes/hermes.py
06_AppPlatform/frontend/src/App.tsx
06_AppPlatform/frontend/src/api/client.ts
06_AppPlatform/frontend/src/types/index.ts
06_AppPlatform/frontend/src/pages/DataManagementPage.tsx
hermes/feature_registry.yaml
hermes/artifact_registry.yaml
hermes/dev_events/dev_events.jsonl
hermes/sentinel_notifications.jsonl
hermes/answer_audit.jsonl
~~~

Rules:

1. Create feature-local route, type, API, model, and component files first.
2. Do not register routes/navigation in parallel branches.
3. The integration PR performs the single registration edit after all feature commits are available.
4. If a shared-file change is unavoidable, declare exact file and line ownership to all sessions before editing.
5. Runtime JSONL, screenshots, generated reports, and node_modules are not staged unless they are intentional test fixtures.
6. No session uses git add dot.

Exception: the confirmed Monitor patch owns its current five shared-file diffs until that patch is committed. This exception does not grant ongoing shared-file ownership.

ScrapingToolkit validation belongs to the source-repair session. Other sessions consume its public result rather than editing the validator.

## 8. Database and migration ownership

P0 schema authority is the Governance session.

- Source Repair continues using existing MsrpSource, ScrapeBatch, and MsrpObservation paths.
- Historical Monitor reads CurrentPrice/PriceHistory and uses existing controlled backfill/history services.
- Governance creates the new shared governance and Agent-persistence foundation migration.
- Hermes implements Agent repositories/services against the frozen schema contract and does not create a competing migration head.
- Mapping continues using JatoMsrpLink, MatchOverride, ReviewCase, and ReviewDecision until an explicitly approved mapping schema change.

To avoid models.py collision:

- define feature-local SQLAlchemy model modules;
- reserve one integration import/metadata registration edit;
- do not have multiple sessions append classes independently to models.py.

To avoid Alembic multiple heads:

- Governance reserves the foundational revision;
- later Agent-specific schema changes must use that revision as down_revision after it is merged;
- integration verifies alembic heads returns one head.

## 9. Versioned integration contracts

### 9.1 SourceRunResult v1

Required fields:

~~~text
schemaVersion
runId
targetKey
sourceCode
runtimeSourceId
publishedSourceVersionId optional
status
failureClass
retryability
extractorName/version
sourceUrl/finalUrl
evidenceRefs
contentHash
extractedCount
validCount
rejectedCount
lastKnownGoodRunId/time
startedAt/completedAt
~~~

Producer: Source Repair. Consumer: Governance/Hermes.

### 9.2 MonitorAnomaly v1

Required fields:

~~~text
schemaVersion
anomalyId
country/brand/model/trim/powertrain
currentPriceId
priceHistoryIds
observationIds
evidenceRefs
movementType
localCurrency/currentLocal/previousLocal
normalizedCurrency/currentNormalized/previousNormalized optional
suspectedRepairDomain
reason
detectedAt
~~~

Producer: Historical Monitor. Consumer: Governance.

### 9.3 AgentRunRequest v1

Required fields:

~~~text
schemaVersion
runId
caseId
targetId
repairDomain
evidenceRefs
currentSourceVersionId
lastKnownGoodVersionId
sourceGateSnapshot
mappingGateSnapshot
fxGateSnapshot optional
allowedToolIds
authorityPolicyVersion
composerPolicyVersion
attempt/time/token/cost budgets
requestedBy
~~~

Producer: Governance. Consumer: Hermes.

### 9.4 AgentRunResult v1

Required fields:

~~~text
schemaVersion
runId
status
planVersion
stepRefs
toolExecutionRefs
llmInvocationRefs
proposalRefs
evaluationRefs
capabilityProposalRefs
stopReason
humanEscalation
completedAt
~~~

Producer: Hermes. Consumer: Governance.

### 9.5 GateDecision v1

Required fields:

~~~text
schemaVersion
targetId
observationId
sourceGate status/reasons/policyVersion
mappingGate status/reasons/policyVersion
fxGate status/reasons/policyVersion optional
eligibleForLocalMaterialization
eligibleForNormalizedMaterialization
evaluatedAt
~~~

Producer: Governance eligibility service. Consumers: ingest, review, history, Monitor.

The persisted read-model envelope adds `gateDecisionId`, `evaluationContext`, `createdBy`, and `createdAtUtc`. These are additive storage metadata; the v1 decision body remains unchanged. Result Correction and FX Normalization commands reference this record by `gateDecisionId` instead of submitting an untrusted eligible Gate body.

### 9.6 MappingDecision v1

The existing Mapping domain remains authoritative. Contract includes accepted link/override/rule, candidate scores, Top1/Top2 margin, hard conflicts, policy version, decision actor, and decision time.

Producer: Automatic Mapping/Matching Review. Consumer: Governance eligibility.

## 10. API prefix ownership

| Prefix | Owner |
|---|---|
| /v1/msrp/monitoring | Historical Monitor |
| /v1/msrp/source-governance | Governance |
| /v1/msrp/hermes-agent | Hermes Agent |
| /v1/review | Existing Matching Review |
| existing /v1/msrp workflow endpoints | frozen/legacy until integration |

No session adds unrelated endpoints to another owner's route module.

## 11. Frontend route and interaction ownership

| Route/surface | Owner |
|---|---|
| /market/msrp-monitor | Historical Monitor |
| new Source Governance route/page | Governance |
| Agent tabs inside Governance deck | Governance reads Hermes API |
| /data/matching-review | Existing Matching Review |
| Current MSRP page | existing product surface; integration only if Gate status is added |

Layout rules:

- Monitor retains business charts and market drilldowns.
- Governance uses a dense target table and right-side FloatingDeck.
- Agent does not add a competing standalone page in P0.
- Matching Review remains a clustered Mapping exception page.
- Shared FloatingDeck components are reused without parallel modification; feature-local wrappers handle new behavior.

## 12. Commit and PR protocol

Each PR must:

1. list owned files changed;
2. list contract versions produced/consumed;
3. list shared-file exceptions;
4. exclude runtime ledgers and unrelated artifacts;
5. run scoped tests;
6. report migration head impact;
7. report whether facts, evidence, or only read models changed.

Before staging:

~~~text
git status --short
git diff --name-only
git diff --cached --name-only
~~~

Stage only explicit files.

Contract change protocol:

- backward-compatible optional field: increment contract minor metadata and notify consumers;
- required field/semantic change: new schemaVersion;
- never silently reinterpret an existing field;
- consumer must tolerate unknown optional fields;
- store producer version with every persisted event.

## 13. Integration sequence

Create a clean integration worktree from the agreed deployment/main base. Do not integrate in the dirty original mixed directory.

Recommended order:

1. Source Repair PR: stable evidence/observation and SourceRunResult.
2. Historical Monitor PR: read-side Monitor and MonitorAnomaly.
3. Governance foundation PR: schema, Cases, Source Versions, Gates, control plane.
4. Hermes Agent PR: consume Governance contracts, execute Runs, emit Proposals/Evaluations.
5. Integration PR:
   - register backend routes;
   - register frontend routes/navigation;
   - register feature-local models with SQLAlchemy metadata;
   - align Alembic to one head;
   - connect source and Monitor findings to Governance;
   - connect Governance to Hermes;
   - connect central eligibility to ingest/review/materialization;
   - update feature/artifact registries once;
   - resolve shared UI/types without broad compatibility layers.

If generic Hermes improvements are a separate clean commit, merge them before the MSRP Hermes Agent adapter. Do not mix unrelated Hermes History Cockpit or AstrBot changes into the MSRP Agent PR.

## 14. End-to-end integration tests

Required cross-module scenarios:

1. transient source failure -> SourceRunResult -> Hermes retry -> Source Gate pass;
2. parser drift -> Hermes Proposal -> Dryrun -> human Source Version publish;
3. anti-bot -> manual_evidence_required -> PDF upload -> resumed validation;
4. valid source plus ambiguous Mapping -> clustered Review -> MappingDecision -> materialization;
5. Monitor anomaly -> Governance result Case -> replay/correction -> immutable history;
6. stale FX -> FX repair -> normalized value changes while local MSRP does not;
7. DPV4 outage -> deterministic/manual flow remains available;
8. failed Agent run -> last-known-good remains active;
9. policy/tool capability proposal -> shadow evaluation -> approval or rollback;
10. full Source Gate plus Mapping Gate -> CurrentPrice/PriceHistory -> Monitor.

## 15. Immediate actions before coding

1. Stop using the same msrp worktree for source repair and Monitor work.
2. Preserve current uncommitted work; split it into explicit source, monitor, and Hermes commits/worktrees without destructive Git commands.
3. Record actual worktree/branch for the Monitor and MSRP Hermes sessions.
4. Freeze the shared-file list.
5. Freeze contract schemaVersion v1 names and required fields.
6. Let Governance reserve the P0 migration/schema foundation.
7. Start feature implementation only inside owned paths.
8. Create the integration branch only after each PR has a narrow verified commit.

## 16. Implementation authorization

The user confirmed this boundary and authorized implementation on 2026-07-14. This does not authorize one session to move, overwrite, or register another session's work. Governance implementation proceeds only in the dedicated Governance worktree; Monitor's grandfathered five-file patch remains Monitor-owned, and shared registration remains deferred to the integration PR.

## 17. Governance integration handoff manifest

### 17.1 Endpoint surface

All paths below are relative to `/v1/msrp/source-governance`.

| Lane | Endpoints |
|---|---|
| Target/read model | `GET/POST /targets`, `GET /targets/{targetId}`, `GET /targets/{targetId}/gate-decisions/latest` |
| Evidence | `POST /targets/{targetId}/url-evidence`, `POST /evidence-uploads/initiate`, `PUT /evidence-uploads/{uploadId}/parts/{part}`, `POST /evidence-uploads/{uploadId}/complete` |
| Findings/Cases | `GET /cases`, `GET /cases/{caseId}`, `POST /cases/findings`, `POST /findings/source-runs`, `POST /findings/monitor-anomalies`, `GET /conflicts`, `POST /cases/{caseId}/resolve` |
| Hermes handoff | `POST /cases/{caseId}/request-hermes-diagnosis`, `POST /cases/{caseId}/agent-run-results` |
| Proposal/version | `POST /cases/{caseId}/proposals`, `GET /proposals/{proposalId}`, `POST /proposals/{proposalId}/dryrun`, `POST /proposals/{proposalId}/submit`, `POST /source-versions`, `GET /source-versions/{versionId}`, `POST /source-versions/{versionId}/publish`, `POST /source-versions/{versionId}/rollback` |
| Gate/fact repair | `POST /gate-decisions/evaluate`, `POST /result-corrections`, `GET /result-corrections/{id}`, `POST /result-corrections/{id}/approve`, `GET/POST /fx-runs`, `POST /fx-runs/{id}/approve` |
| Audit | `GET /audit-events` |

There is no DPV4 endpoint. DPV4 has no service account and cannot call Proposal, Gate, publish, correction, or FX mutations. Hermes owns the Agent identity and may attach DPV4 invocation metadata only inside its registered Run/Step context.

### 17.2 Cross-session events

| Producer | Contract/action | Governance consumer or next hop |
|---|---|---|
| Source Repair | `SourceRunResult v1` | `POST /findings/source-runs`; success updates target health, failure deduplicates a Repair Case |
| Historical Monitor | `MonitorAnomaly v1` | `POST /findings/monitor-anomalies`; creates a result/source/mapping/FX Case without editing Monitor data |
| Mapping/ingest integration | Source/Mapping/optional FX Gate inputs | `POST /gate-decisions/evaluate`; persists the authoritative append-only Gate Decision |
| Governance | `AgentRunRequest v1` | integration dispatcher sends it to `/v1/msrp/hermes-agent`; the Governance response remains `pending_integration` until that dispatcher is registered |
| Hermes | Proposal commands plus `AgentRunResult v1` | create Proposal through the Case endpoint, then callback `/cases/{caseId}/agent-run-results`; an unregistered run/proposal is rejected |
| Result/FX repair | `gateDecisionId` | correction and normalization services load the persisted Gate; caller-supplied pass flags are not accepted |
| Materialization integration | persisted `GateDecision v1` | local fact write requires Source+Mapping pass; normalized write additionally requires FX pass |

### 17.3 Shared registration checklist

The integration PR, not an owning feature PR, performs these exact shared changes:

1. Import `app.db.msrp_source_governance_models` in the central SQLAlchemy/Alembic metadata registry.
2. Include `app.api.routes.msrp_source_governance.router` once from FastAPI `main.py` with outer prefix `/v1`; the feature router already owns `/msrp/source-governance`, so `/v1/v1` must never appear.
3. Register the feature-local `MsrpSourceGovernancePage` under the recommended `/market/msrp-governance` route and add one MSRP-family navigation entry. Reuse the existing page and styles; do not rebuild it in `App.tsx`.
4. Wire Source Repair and Monitor producers to the two findings endpoints with stable idempotency keys.
5. Wire the diagnosis response to the Hermes Agent dispatcher and wire Hermes terminal results back to the registered Case callback.
6. Call the central eligibility endpoint/service after authoritative Source and Mapping decisions, then pass only the persisted `gateDecisionId` into Result/FX repair and fact-materialization work.
7. Register the migration in the final single-head integration chain. Revision `20260714_0044` generates isolated PostgreSQL SQL successfully; the repository's older revision `20260516_0017` in `20260516_0016_role_upgrade_requests.py` uses runtime `inspect()` and must be made offline-compatible separately if full-chain `alembic upgrade head --sql` is required.
8. Do not overwrite the Monitor-owned `msrp_workflow.py`, `msrp_monitoring_service.py`, `frontend/src/api/client.ts`, `MsrpMonitorPage.tsx`, or shared `types/index.ts` changes while resolving integration conflicts.
