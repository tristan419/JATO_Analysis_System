# Hermes Registry Foundation

> Phase 1 — Registry-Only Layer
> Created: 2026-05-14
> Based on: Phase 0 REPOSITORY_ASSET_MAP.md

## Alias

Also known as:
- Hermes Steward
- Hermes 小管家
- 小管家
- Hermes governance assistant

Canonical name in code: Hermes
Frontend label: Hermes Steward / Hermes 小管家
Route: `/data/overview?view=hermes`
Backend prefix: `/v1/hermes/*`

## Purpose

Hermes Phase 1 is a **registry-only layer**. It records assets, dependencies, risks, and governance gaps. It **does not** automatically modify code, infrastructure, production environment, database schema, or deployment state.

## Registry Files

| File | Purpose |
|---|---|
| `source_registry.yaml` | News, VOC, MSRP, and official data sources |
| `pipeline_registry.yaml` | systemd timers, Airflow DAGs, GitHub Actions, ETL jobs |
| `feature_registry.yaml` | Product features (routes, APIs, data deps) |
| `prompt_registry.yaml` | LLM prompts (location, version, model, quality) |
| `artifact_registry.yaml` | Data artifacts (path, schema, freshness, consumers) |
| `governance_gaps.yaml` | Known governance gaps from Phase 0 audit |
| `proposal_registry.yaml` | Improvement proposals (draft → approved → implemented) |

## Runtime Files

| File | Purpose |
|---|---|
| `sentinel_notifications.jsonl` | Sentinel notification facts keyed by stable fingerprint-derived IDs |
| `sentinel_notification_state.json` | User mailbox state for Sentinel notifications, including read/acked/archived/resolved; ignored by Git |
| `deploy_release.json` | Production release metadata with expected and actual commit fields |
| `deploy_expected.json` | Latest commit production is expected to run, usually recorded by DevSync/GitHub Actions |

## Cockpit APIs

Hermes also exposes read-only project history and progress cockpit endpoints:

| Endpoint | Purpose |
|---|---|
| `GET /v1/hermes/history/events` | Normalized git, DevSync, evidence, Sentinel, pipeline, and deploy events |
| `GET /v1/hermes/history/clusters` | Rule-based time clusters for History Map, with `level` and `yAxis` controls |
| `GET /v1/hermes/progress/features` | Feature lifecycle state with phase, risk, gaps, tests, docs, and next action |
| `GET /v1/hermes/progress/swimlanes` | Progress features grouped by workstream for the swimlane UI |

## How to Update Registries

1. Edit the relevant YAML file.
2. Run `python -c "import yaml; yaml.safe_load(open('hermes/<file>'))"` to validate.
3. Commit with a message like `hermes: update <registry> — <reason>`.

## Rules

- **Do not store secrets** in any registry file.
- **Do not store production DB URLs** or API keys.
- **Do not auto-execute proposals** — all proposals require human review.
- Use `needs verification` where uncertain.
- Use `null` for unknown fields (not empty strings).

## ID Convention

Stable, dot-separated IDs:

```
source.<type>.<country>.<name>
pipeline.<domain>.<system>
feature.<name>
prompt.<domain>.<name>
artifact.<domain>.<name>
gap.<category>.<short_name>
proposal.<category>.<short_name>
```

## Status Values

- `active` — Running normally in production
- `watch` — Active but needs monitoring
- `degraded` — Running with known issues
- `disabled` — Intentionally turned off
- `planned` — PRD exists, not implemented
- `deprecated` — Scheduled for removal
- `archived` — Historical only
- `unknown` — Needs investigation
