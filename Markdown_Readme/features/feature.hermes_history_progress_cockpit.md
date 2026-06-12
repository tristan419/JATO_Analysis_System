# Hermes History Progress Cockpit

## Purpose

Hermes History Progress Cockpit turns existing Hermes governance data into a project history, progress, and workflow view. It is not a raw git log viewer. It aggregates git commits, DevSync events, feature registries, evidence, gaps, Sentinel notifications, pipeline status, deploy metadata, and usage cost records into records that answer:

- What changed recently?
- Which workstream or feature moved forward?
- Which feature is implemented, tested, deployed, verified, or blocked?
- Which commits, files, tests, evidence, and gaps explain the current state?
- Which session or model worked on which feature, files, tests, and commits?

## MVP Scope

### Progress Swimlane

The first view groups features by workstream and shows a fixed lifecycle:

```text
PRD -> Implemented -> Tested -> Deployed -> Verified -> Resolved
```

Each feature row exposes current phase, risk, open gaps, test/doc/evidence counts, recent event, and the next action Hermes can recommend.

### Git History Cluster

The second view clusters git and governance events over time. The first implementation uses rules instead of machine learning:

- paths under `hermes/` map to Hermes
- `07_ScrapingToolkit`, MSRP, and monthly update paths map to JATO Monthly / MSRP
- `MarketScanPage` and `market_scan_service` map to MarketScan
- `astrbot`, `jato_agent`, `country_chat`, and MCP paths map to AstrBot / CountryCopilot
- engineering config and navigation paths map to Config Comparison
- workflow, deploy, and pipeline paths map to Deploy / CI

The UI supports a cluster-detail slider:

```text
Epic -> Workstream -> Feature -> Session -> Commit
```

The Y axis can be switched between workstream, phase, risk, and session.

### Workflow Cockpit

The third view groups normalized events by session and model. It shows:

- model summary by event, commit, test, and workstream counts
- session cards with status, risk, latest meaningful event, and source mix
- session details with related features, files, recent events, evidence, and gaps
- top review items derived from blocking or ready-for-PR features

AstrBot and other model usage records enter this view through the same Hermes cost ledger used by the cost report, so token consumption can be reviewed alongside commits and DevSync events.

## Backend

New read-only service:

```text
06_AppPlatform/backend/app/services/hermes_history_service.py
```

New endpoints:

```text
GET /v1/hermes/history/events
GET /v1/hermes/history/clusters?level=feature&yAxis=workstream
GET /v1/hermes/progress/features
GET /v1/hermes/progress/swimlanes
GET /v1/hermes/workflow/cockpit
```

The service does not execute commands, deploy code, rewrite registry files, or mutate Hermes ledgers.

## Frontend

New components:

```text
06_AppPlatform/frontend/src/components/HermesProgressSwimlane.tsx
06_AppPlatform/frontend/src/components/HermesHistoryMap.tsx
06_AppPlatform/frontend/src/components/HermesWorkflowView.tsx
```

Hermes adds two subtabs under the "Understands" group:

```text
Progress
Git History Cluster
Workflow
```

## Deferred Work

- Semantic clustering with embeddings or changed-file overlap scoring
- Direct links from event IDs to GitHub commits, evidence records, gaps, and pipeline artifacts
- Dedicated route or URL state for shareable History Map filters
