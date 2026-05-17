# CLAUDE.md — JATO Analysis System

> Companion file: `Markdown_Readme/AI_README.md` — structured machine-readable index of all modules, routes, pages, data flows, and external services. Read it alongside this file for full project context.

## Project identity

Fullstack automotive market analysis system. Ingests JATO vehicle sales data, scrapes official MSRP pricing, monitors news/VOC, and surfaces insights through a FastAPI + React SPA.

**Production URL:** <https://www.ojeur.cloud>
**Deployment:** Tencent Cloud Ubuntu, nginx + systemd, GitHub Actions CI/CD
**LLM provider:** DeepSeek V4-Flash (deepseek-chat) — direct API, streaming SSE

## Quickstart: what to read before editing

1. `Markdown_Readme/Fullstack/ROADMAP.md` — master index of all docs
2. `Markdown_Readme/Fullstack/WORKFLOWS/README.md` — business + technical workflow maps
3. Then task-specific docs:
   - Data / ETL: `Markdown_Readme/Fullstack/02_DataETL/ETL.md`
   - MSRP: `Markdown_Readme/Fullstack/MSRP/README.md`
   - Backend: `06_AppPlatform/backend/README.md`
   - Scraping / VOC / news: `07_ScrapingToolkit/README.md`
   - UI / frontend: `Markdown_Readme/UI/UI_SPECIFICATION_V1.md`

`Markdown_Readme/Streamlit/` is historical archive only. Do not treat Streamlit as current mainline.

## Architecture overview

```
Browser (React SPA, port 5173 dev / 80 prod)
  → nginx → FastAPI (port 8000)
      ├── JATO Parquet files (04_Processed_data/) — sales/registration analytics
      ├── PostgreSQL (port 5432) — business truth: MSRP, reviews, configs, auth, news
      ├── Redis — caching layer
      ├── Airflow (port 8080) — local orchestration for batch jobs
      └── External APIs — DeepSeek, Tavily, Google CSE, SerpAPI, Gemini
```

**Stack:** FastAPI (Python 3.11+) + React 18 + TypeScript + Vite + PostgreSQL + Redis + Parquet (PyArrow/DuckDB)

## Repository map

| Directory | Purpose | Do not open unless needed |
|-----------|---------|---------------------------|
| `01_RAW_DATA/` | Raw JATO input Excel/files | ✓ |
| `02_Config_MetaData/` | Field mappings, business rules (xlsx) | |
| `03_Scripts/` | ETL, ingestion, deploy, diagnostics, Hermes CLI tools | |
| `04_Processed_data/` | Processed Parquet, partitioned datasets, news/VOC artifacts | ✓ (large files) |
| `05_DashBoard/` | Legacy Streamlit dashboard (archived) | ✓ |
| `06_AppPlatform/backend/` | FastAPI backend — 20 routers, 40+ services, Alembic migrations | |
| `06_AppPlatform/frontend/` | React + TypeScript + Vite — 21 pages, 42 components | |
| `07_ScrapingToolkit/` | MSRP/news/VOC/EVKX scraping (installable as `jato-scraping-toolkit`) | |
| `08_GitNexus/` | Separate code-intelligence product (independent git repo) | |
| `airflow/` | Airflow DAGs (3 DAGs), Dockerfile, plugins | |
| `hermes/` | Governance data: 8 YAML registries, JSONL ledgers, prompts, reports | |
| `Markdown_Readme/` | All project documentation | |
| `.github/` | GitHub Actions workflows (CI/CD, country-news-sync, hermes-devsync) | |
| `.githooks/` | Pre-commit/post-commit hooks | |

### Backend structure (`06_AppPlatform/backend/app/`)

```
app/
  main.py                    # FastAPI app, 20 routers mounted
  api/
    routes/                  # 20 route files (health, analysis, msrp*, engineering*, review*, hermes, auth, presence, assistant, market_scan, data_management, metadata, filters, platform_db)
    schemas.py               # General Pydantic schemas
    msrp_schemas.py          # MSRP-specific schemas
    review_schemas.py        # Review-specific schemas
  services/                  # 40+ service files (see below)
  infra/                     # 6 repository files (parquet, msrp, engineering, review, redis)
  copilot_governance/        # 16 modules: answer_composer, fact_checker, query_plan, semantic_layer, etc.
  scraper/                   # Embedded subset of 07_ScrapingToolkit: extractors, config_loader, runner, validation
  core/                      # config.py, security.py
  db/                        # base.py, models.py, session.py
  domain/                    # models.py
alembic/                     # 16 migration versions
tests/                       # unit/ + integration/ (48 test files)
evals/                       # Copilot evaluation harness
```

**Key service groups:**
- **Analytics:** `market_scan_service`, `query_service`, `country_service`, `payload_serializers`
- **MSRP:** `msrp_lookup_service`, `msrp_link_service`, `msrp_mapping_service`, `msrp_admin_service`, `msrp_workflow_service`, `msrp_dryrun_progress`, `jato_monthly_update_service`
- **Engineering:** `engineering_service`, `engineering_normalization_service`, `engineering_variant_diff_service`, `engineering_config_matrix_parser`, `config_field_mapping_parser`, `config_availability`
- **Country Copilot:** `country_chat_service`, `country_chat_models`, `country_profiles`, `web_search_service`
- **Hermes:** `hermes_chat_service`, `hermes_devsync_service`, `hermes_sentinel_service`
- **Data:** `data_management_service`, `customer_insight_service`, `news_digest_service`, `news_wiki_service`, `voc_staging_service`, `local_wiki_service`, `evkx_import_service`
- **Auth/Infra:** `auth_service`, `platform_db_service`, `review_service`, `review_workbench_service`, `insight_card_service`, `presence_service`, `fx_service`, `feishu_service`, `google_service`, `identity_key_service`, `market_scan_cache`

**20 API route prefixes (all under `/v1/` except health):**
`health` `/v1/assistant` `/v1/metadata` `/v1/filters` `/v1/analysis` `/v1/crud` `/v1/platform/db` `/v1/engineering` `/v1/engineering/config` `/v1/msrp` `/v1/msrp/links` `/v1/msrp/workflow` `/v1/msrp/monthly-update` `/v1/msrp/dryrun` `/v1/review` `/v1/review/cases` `/v1/hermes` `/v1/auth` `/v1/presence`

### Frontend structure (`06_AppPlatform/frontend/src/`)

**21 pages:** `DashboardPage`, `MarketOverviewPage`, `MarketScanPage`, `MarketSegmentsPage`, `MarketBrandRankingPage`, `MarketModelRankingPage`, `MarketPowertrainPage`, `PositioningPricingPage`, `VersionComparisonPage`, `SpecificationPage`, `EngineeringPage`, `EngineeringConfigPage`, `MsrpPage`, `ReviewCasesPage`, `CountryChatPage`, `CustomerInsightsPage`, `DataManagementPage`, `JatoMonthlyUpdatePage`, `LoginPage`, `CrudPage`, `NotFoundPage`

**42 components:** Including `CountryChatWidget`, `CopilotGovernancePanel`, `CountryChatAnalysisDeck`, `CountryChatGroundedAnswer`, `LazyPlotlyChart`, `PlotlyChart`, `MegaMenu`, `CollapsibleFilterSidebar`, `MsrpDryrunDashboard`, `PriceHistoryTimeline`, `RankingTrendDrawer`, `ReviewDeliveryPanel`, `ConfigDiffPanel`, `ConfigMatrixEditor`, `PresenceWidget`, `HermesAskResponseCard`, `HermesMermaidBlock`

**3 contexts:** `AuthContext`, `CountryChatContext`, `SharedFilterScopeContext`

### Scraping Toolkit (`07_ScrapingToolkit/`)

Installable as `jato-scraping-toolkit`. Core package `jato_scraper/` with:
- **4 extractors:** `scrapling_web` (headless browser), `http_json` (API), `playwright_card_flow` (dynamic configurator), `pdf_text`
- **Runners:** `runner.py` (MSRP), `news_runner.py` (RSS/Atom), `voc_runner.py` (VOC planning)
- **VOC pipeline:** `voc_fetcher.py` → `voc_enricher.py` → `voc_taxonomy.py`
- **LLM module:** `llm/client.py`, `llm/providers.py`, `llm/msrp_page_analyzer.py`
- **Config:** `sources/` (production YAMLs), `source_drafts/suv_only_country_model_top30/` (20 countries × ~30 drafts), `news_sources/`, `voc_sources/`

### Hermes governance (4 locations)

| Location | Content |
|----------|---------|
| `hermes/` (root) | 8 YAML registries, 3 JSONL ledgers, prompt templates, 15+ reports |
| `03_Scripts/hermes/` | 13 Python CLI tools (intake, code_audit, pipeline_audit, evidence_writer, etc.) |
| `Markdown_Readme/Fullstack/Hermes/` | 7 governance docs (implementation plan, asset map, PRD template, etc.) |
| `Markdown_Readme/Hermes/` | DevSync contract doc |

## Running services rule

Frontend, backend, PostgreSQL, Redis, Docker, Airflow may already be running.

**Do NOT start, stop, restart, or recreate these services unless explicitly asked.**
Banned commands without approval: `npm run dev`, `uvicorn`, `docker compose up/down`, `docker restart`, `streamlit run`, `alembic upgrade/downgrade`.

If a service appears unavailable, report it first. Check port liveness before assuming it's down.

**Known ports:** FastAPI:8000, Frontend dev:5173, PostgreSQL:5432, Redis:6379, Airflow:8080

## Coding rules

- Read docs before code. Make small, targeted changes.
- Do not scan the whole repository unless necessary.
- Do not open raw data (`01_RAW_DATA/`) or processed data (`04_Processed_data/`) unless needed.
- Do not rename columns, API fields, business keys, or folders without checking downstream usage.
- Do not expose API keys, `.env` values, raw data, local paths, or generated cache files.
- Prefer existing project patterns over new dependencies.
- Do not add features, refactor, or introduce abstractions beyond what the task requires.

## JATO monthly data rule

- Production JATO active data is runtime state in `04_Processed_data/jato_full_archive.parquet` + `partitioned_dataset_v1`.
- Monthly JATO advancement should go through the web monthly-update publish flow; deploy is for code and excludes `04_Processed_data`.
- Before changing monthly update, MarketScan, Dashboard data reads, Redis cache keys, or sync scripts, read `Markdown_Readme/Fullstack/04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md`.
- Do not overwrite cloud active data from stale local parquet. Sync cloud runtime to local first when local data work depends on production active data.

## Validation commands

Backend:
```bash
cd 06_AppPlatform/backend
python -m pytest tests/unit -x -q
python -m pytest tests/integration -x -q
```

Frontend:
```bash
cd 06_AppPlatform/frontend
npx tsc --noEmit --pretty
npx vitest run --reporter=verbose
```

## Hermes DevSync rule (MANDATORY after every implementation)

After EVERY non-trivial implementation, bug fix, refactor, or test change:

1. **Write dev event** to `hermes/dev_events/dev_events.jsonl` per `Markdown_Readme/Hermes/HERMES_CLAUDE_CODE_DEVSYNC_CONTRACT.md`
   Required fields: `eventId`, `eventType`, `source`, `title`, `summary`, `linkedFeatureIds`, `changedFiles`, `tests`, `createdAt`
2. **Trigger DevSync** via `POST /v1/hermes/dev/sync` (or Dev tab → Sync Now in UI)
3. **Verify** feature appears in Hermes UI → Dev tab
4. **Report status:** event written, feature updated, markdown generated, evidence written, gaps created, test results

**Automated enforcement:** pre-commit hook, post-commit hook, GitHub Actions `hermes-devsync.yml`

## Key business entities

- **JATO vehicle:** `jato_model` + `jato_variant` + `jato_version` — unique vehicle identification
- **MSRP source → observation → review case → current_price:** pricing intelligence lifecycle
- **JatoMsrpLink:** stable mapping from JATO key to official source key
- **MatchOverride:** dated exception overriding link results within a validity window
- **Engineering Config:** `material_no|vehicle_code|market|model_year|trim_name` identity key, versioned draft→published→archived
- **Country Copilot routes:** `market-scan`, `positioning-focus`, `segment-fuel-focus`, `precise-lookup` — tool-first evidence gathering → 6-section report
