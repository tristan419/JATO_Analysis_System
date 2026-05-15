# Hermes Requirement Intake Report

## 1. PRD Summary

- **PRD file:** `Markdown_Readme/Fullstack/01_DevWorkflow/presence_websocket_prd.md`
- **Title:** Presence + WebSocket 实时在线协作 PRD
- **Generated at:** 2026-05-15T04:29:07Z
- **Sections detected:** 44
- **Keywords detected:** backend(7), cost(1), data(1), feature(2), frontend(5), llm(1), pipeline(2), test(1)
- **Routes mentioned:** `/backend/app/api/routes/presence`, `/v1/ws`, `/v1/engineering/variants/`, `/variants/`, `/lock`, `/v1/presence`, `/src/hooks/usePresence`, `/backend/app/services/ws_service`
- **API paths mentioned:** `/v1/ws`, `/v1/engineering/variants/`, `/v1/engineering/projects/`, `/v1/presence`, `/v1/presence/online`, `/v1/presence/heartbeat`

## 2. Matched Features

| featureId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `feature.jato_monthly_update` | JATO Monthly Update Pipeline | 0.5 | low | route match: ['/msrp/monthly-update']; API match; name match: JATO Monthly Update Pipeline |
| `feature.review_workbench` | Review Workbench | 0.4 | low | route match: ['/review']; API match; name match: Review Workbench |
| `feature.engineering` | Engineering Config Management | 0.3 | low | route match: ['/engineering']; API match; name match: Engineering Config Management |
| `feature.country_copilot` | Country Copilot / Country Chat | 0.2 | low | API match; data source match; artifact match |
| `feature.dashboard` | Dashboard | 0.2 | low | API match; data source match; artifact match |
| `feature.data_management` | Data Management | 0.2 | low | API match; docs/deps/issue match |
| `feature.msrp_workbench` | MSRP Workbench | 0.1 | low | API match |
| `feature.current_price` | Current Price | 0.1 | low | route match: ['/msrp (current-prices tab)']; API match |
| `feature.specification` | Specification / Data Explorer | 0.1 | low | API match; data source match; artifact match |
| `feature.market_scan` | Market Scan | 0.1 | low | API match; data source match; artifact match |

## 3. Matched Pipelines

| pipelineId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `pipeline.news.country_systemd` | Country News Sync (systemd timer) | 0.1 | low | path match: 03_Scripts/deploy/systemd/jato-country-news-sync.service; name match: Country News Sync (systemd timer); trigger/schedule/consumer match |
| `pipeline.voc.forum_systemd` | VOC Forum Sync (systemd timer) | 0.1 | low | path match: 03_Scripts/deploy/systemd/jato-voc-forum-sync.service; name match: VOC Forum Sync (systemd timer); trigger/schedule/consumer match |
| `pipeline.msrp.dryrun_systemd` | MSRP Dry-run (systemd timer) | 0.1 | low | path match: 03_Scripts/deploy/systemd/jato-msrp-sync@.service; name match: MSRP Dry-run (systemd timer); trigger/schedule/consumer match |
| `pipeline.msrp.ingest_systemd` | MSRP Ingest (systemd timer) | 0.1 | low | path match: 03_Scripts/deploy/systemd/jato-msrp-sync@.service; name match: MSRP Ingest (systemd timer); trigger/schedule/consumer match |
| `pipeline.deploy.ec2_legacy_github` | Deploy to EC2 (Legacy, GitHub Actions) | 0.1 | low | path match: .github/workflows/deploy-ec2-auto-update.yml; name match: Deploy to EC2 (Legacy, GitHub Actions); output match |
| `pipeline.deploy.tencent_github` | Deploy Fullstack to Tencent Cloud (GitHub Actions) | 0.1 | low | name match: Deploy Fullstack to Tencent Cloud (GitHub Actions); output match; trigger/schedule/consumer match |
| `pipeline.news.country_airflow` | Country News Sync (Airflow DAG) | 0.1 | low | path match: airflow/dags/jato_country_news_sync.py; trigger/schedule/consumer match; known issue match |
| `pipeline.jato.etl` | JATO ETL Pipeline | 0.1 | low | pipelineId match; name match: JATO ETL Pipeline; trigger/schedule/consumer match |
| `pipeline.scraping_toolkit.airflow` | Scraping Toolkit Manual (Airflow DAG) | 0.0 | low | path match: airflow/dags/jato_scraping_toolkit_manual.py; output match |
| `pipeline.msrp.airflow` | MSRP Low Concurrency (Airflow DAG) | 0.0 | low | path match: airflow/dags/jato_msrp_low_concurrency.py |

## 4. Matched Sources

| sourceId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `source.msrp.batch_a` | MSRP Batch A (SUV Top30 — 7 countries) | 0.0 | low | name match: MSRP Batch A (SUV Top30 — 7 countries) |
| `source.msrp.evkx` | EVKX BEV Catalog | 0.0 | low | type/country/path match |

## 5. Matched Prompts

_No matches found._

## 6. Matched Artifacts

| artifactId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `artifact.jato.monthly_update_job` | JATO Monthly Update Job | 0.1 | low | artifactId match; name match: JATO Monthly Update Job |
| `artifact.jato.parquet` | JATO Full Archive (Parquet) | 0.1 | low | artifactId match; name match: JATO Full Archive (Parquet); consumer/producer match |
| `artifact.jato.partitioned` | JATO Partitioned Dataset | 0.1 | low | artifactId match; name match: JATO Partitioned Dataset; consumer/producer match |
| `artifact.jato.summaries` | Precomputed Summaries | 0.0 | low | artifactId match; consumer/producer match |
| `artifact.status_json` | Scheduled Fetch Status | 0.0 | low | name match: Scheduled Fetch Status |

## 7. Risk Assessment

| Area | Risk | Reason |
|---|---|---|
| Backend | medium | keyword: backend, fastapi, api, endpoint, route, service, uvicorn |
| Frontend | low | keyword: frontend, component, page, ui, route |
| Pipeline | medium | keyword: systemd, fetch |
| Intelligence | medium | keyword: pro |
| Cost | low | keyword: pro |
| Tests | low | keyword: snapshot |
| Docs | low | PRD should identify docs to update |

## 8. Required Registry Updates

- [ ] Feature Registry: review 16 matched features
- [ ] Pipeline Registry: review 12 matched pipelines
- [ ] Source Registry: review 2 matched sources
- [ ] Artifact Registry: review 5 matched artifacts

## 9. Required Tests

- [ ] Backend unit tests
- [ ] Frontend component tests
- [ ] API contract test (backend serializer ↔ frontend type)
- [ ] Pipeline integration test

## 10. Claude Code Task Brief

```txt
Feature: Presence + WebSocket 实时在线协作 PRD
Goal: See PRD for full requirements.
Affected files: 06_AppPlatform/frontend/src/hooks/usePresence.ts, 06_AppPlatform/frontend/src/hooks/useWebSocket.ts, 06_AppPlatform/backend/app/services/presence_service.py, 06_AppPlatform/backend/app/services/ws_service.py, 06_AppPlatform/backend/app/api/routes/presence.py
Affected registries: feature.jato_monthly_update, feature.review_workbench, feature.engineering, pipeline.news.country_systemd, pipeline.voc.forum_systemd, pipeline.msrp.dryrun_systemd, artifact.jato.monthly_update_job, artifact.jato.parquet
Required tests: Backend unit tests, Frontend component tests, API contract test
Do not do:
  - Do not auto-deploy
  - Do not modify production env
  - Do not change DB schema without migration
  - Do not skip registry updates
Acceptance criteria:
  - See PRD §11 or equivalent
```

## 11. Human Review Notes

- **Low-confidence matches (please verify):**
  - `feature.jato_monthly_update` (features, score=0.45)
  - `feature.review_workbench` (features, score=0.38)
  - `feature.engineering` (features, score=0.3)
  - `feature.country_copilot` (features, score=0.2)
  - `feature.dashboard` (features, score=0.17)
  - `feature.data_management` (features, score=0.17)
  - `feature.msrp_workbench` (features, score=0.11)
  - `feature.current_price` (features, score=0.11)
- **Suggested next step:** Review matched registries, confirm scope, then proceed with Claude Code implementation.
