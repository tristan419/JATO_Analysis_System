# Hermes Requirement Intake Report

## 1. PRD Summary

- **PRD file:** `Markdown_Readme/Fullstack/01_DevWorkflow/ranking_trend_drilldown_prd.md`
- **Title:** Ranking Table Drilldown Trend — 方案 C
- **Generated at:** 2026-05-14T15:04:57Z
- **Sections detected:** 11
- **Keywords detected:** backend(2), data(1), feature(1), frontend(3), pipeline(1)
- **Routes mentioned:** `/YTD`, `/Empty/Error`, `/rank/share`, `/Share/Rank`, `/Rank`, `/api/ranking-trend`, `/segment/fuel/msrp/length`, `/share/rank/msrp`

## 2. Matched Features

| featureId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `feature.msrp_workbench` | MSRP Workbench | 0.5 | low | route match: ['/msrp']; API match; name match: MSRP Workbench |
| `feature.current_price` | Current Price | 0.4 | low | route match: ['/msrp (current-prices tab)']; API match; name match: Current Price |
| `feature.jato_monthly_update` | JATO Monthly Update Pipeline | 0.4 | low | route match: ['/msrp/monthly-update']; API match |
| `feature.ranking_trend_drilldown` | Ranking Trend Drilldown | 0.3 | low | API match; name match: Ranking Trend Drilldown; docs/deps/issue match |
| `feature.country_copilot` | Country Copilot / Country Chat | 0.1 | low | API match; data source match; docs/deps/issue match |
| `feature.market_scan` | Market Scan | 0.1 | low | API match |
| `feature.dashboard` | Dashboard | 0.1 | low | name match: Dashboard |
| `feature.review_workbench` | Review Workbench | 0.1 | low | data source match; artifact match; docs/deps/issue match |
| `feature.positioning_pricing` | Positioning & Pricing | 0.0 | low | docs/deps/issue match |

## 3. Matched Pipelines

| pipelineId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `pipeline.msrp.ingest_systemd` | MSRP Ingest (systemd timer) | 0.4 | low | pipelineId match; path match: 03_Scripts/deploy/systemd/jato-msrp-sync@.service; name match: MSRP Ingest (systemd timer) |
| `pipeline.msrp.dryrun_systemd` | MSRP Dry-run (systemd timer) | 0.3 | low | pipelineId match; path match: 03_Scripts/deploy/systemd/jato-msrp-sync@.service; name match: MSRP Dry-run (systemd timer) |
| `pipeline.msrp.airflow` | MSRP Low Concurrency (Airflow DAG) | 0.2 | low | pipelineId match; name match: MSRP Low Concurrency (Airflow DAG); output match |
| `pipeline.deploy.ec2_legacy_github` | Deploy to EC2 (Legacy, GitHub Actions) | 0.1 | low | output match; trigger/schedule/consumer match |
| `pipeline.news.country_airflow` | Country News Sync (Airflow DAG) | 0.1 | low | trigger/schedule/consumer match; known issue match |
| `pipeline.perf.nightly_github` | Nightly Performance Gate (GitHub Actions) | 0.1 | low | trigger/schedule/consumer match |
| `pipeline.jato.etl` | JATO ETL Pipeline | 0.0 | low | trigger/schedule/consumer match |

## 4. Matched Sources

| sourceId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `source.msrp.batch_a` | MSRP Batch A (SUV Top30 — 7 countries) | 0.3 | low | sourceId match; name match: MSRP Batch A (SUV Top30 — 7 countries); type/country/path match |
| `source.msrp.production` | MSRP Production Sources | 0.2 | low | sourceId match; name match: MSRP Production Sources; type/country/path match |
| `source.msrp.drafts_suv_top30` | MSRP Source Drafts (SUV Top30 per country) | 0.2 | low | sourceId match; name match: MSRP Source Drafts (SUV Top30 per country); type/country/path match |
| `source.msrp.evkx` | EVKX BEV Catalog | 0.1 | low | sourceId match |

## 5. Matched Prompts

_No matches found._

## 6. Matched Artifacts

| artifactId | name | Score | Confidence | Why |
|---|---|---:|---|---|
| `artifact.msrp.observations` | MSRP Observations | 0.3 | low | artifactId match; name match: MSRP Observations; path/pipeline match |
| `artifact.msrp.current_prices` | Current Prices | 0.2 | low | artifactId match; path/pipeline match; consumer/producer match |
| `artifact.status_json` | Scheduled Fetch Status | 0.1 | low | name match: Scheduled Fetch Status |
| `artifact.jato.parquet` | JATO Full Archive (Parquet) | 0.0 | low | consumer/producer match |
| `artifact.jato.partitioned` | JATO Partitioned Dataset | 0.0 | low | consumer/producer match |
| `artifact.jato.summaries` | Precomputed Summaries | 0.0 | low | consumer/producer match |

## 7. Risk Assessment

| Area | Risk | Reason |
|---|---|---|
| Backend | medium | keyword: api, endpoint |
| Frontend | low | keyword: frontend, chart, plotly |
| Pipeline | medium | keyword: fetch |
| Docs | low | PRD should identify docs to update |

## 8. Required Registry Updates

- [ ] Feature Registry: review 9 matched features
- [ ] Pipeline Registry: review 7 matched pipelines
- [ ] Source Registry: review 4 matched sources
- [ ] Artifact Registry: review 6 matched artifacts

## 9. Required Tests

- [ ] Backend unit tests
- [ ] Frontend component tests
- [ ] API contract test (backend serializer ↔ frontend type)

## 10. Claude Code Task Brief

```txt
Feature: Ranking Table Drilldown Trend — 方案 C
Goal: See PRD for full requirements.
Affected files: (determine from scope)
Affected registries: feature.msrp_workbench, feature.current_price, feature.jato_monthly_update, pipeline.msrp.ingest_systemd, pipeline.msrp.dryrun_systemd, pipeline.msrp.airflow, artifact.msrp.observations, artifact.msrp.current_prices
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
  - `feature.msrp_workbench` (features, score=0.45)
  - `feature.current_price` (features, score=0.4)
  - `feature.jato_monthly_update` (features, score=0.38)
  - `feature.ranking_trend_drilldown` (features, score=0.34)
  - `feature.country_copilot` (features, score=0.11)
  - `feature.market_scan` (features, score=0.1)
  - `feature.dashboard` (features, score=0.07)
  - `feature.review_workbench` (features, score=0.06)
- **Suggested next step:** Review matched registries, confirm scope, then proceed with Claude Code implementation.
