# AI_README — JATO Analysis System (machine-readable index)

> Purpose: single-file structured index for AI agents (GPT, Claude, etc.) to understand this project without reading every file.
> Companion to: `CLAUDE.md` (project root, AI behavior rules) and `Fullstack/ROADMAP.md` (human-readable master index).

## Project identity

- **Name:** JATO Analysis System
- **Type:** Fullstack automotive market analysis platform
- **Stack:** FastAPI (Python 3.11+) + React 18 + TypeScript + Vite + PostgreSQL + Redis + Parquet
- **Production:** https://www.ojeur.cloud (Tencent Cloud Ubuntu, nginx + systemd, GitHub Actions CI/CD)
- **LLM:** DeepSeek V4-Flash (deepseek-chat), direct API, streaming SSE
- **Phase:** Phase 4 — Fullstack mainline (Streamlit migration complete)

## Directory purpose map

```
01_RAW_DATA/           = Raw JATO input files (DO NOT OPEN unless needed)
02_Config_MetaData/    = Business config: field mappings (.xlsx), VS Code workspace
03_Scripts/            = 79+ operational scripts in 13 subdirectories
  ├── data_pipeline/   = ETL pipeline: build_partitioned, precompute, refresh, cleanup, seed
  ├── hermes/          = 13 Hermes CLI tools (intake, code_audit, pipeline_audit, evidence_writer, etc.)
  ├── deploy/          = AWS/nginx/systemd deployment configs + loadtest K6 scripts
  ├── local_dev/       = Local dev launchers: fullstack_dev, start_frontend, start_postgres
  ├── diagnostics/     = 14 benchmark/smoke/regression/health check scripts
  ├── ops/             = Deploy, sync, restart, launchd installer scripts
  ├── news/            = Country news sync scripts + launchd
  ├── voc/             = VOC Excel generation + forum sync shell
  ├── tests/           = Script-level tests (elt_worker, raw_compare, seed_msrp)
  ├── DoubleClick/     = macOS .command convenience launchers
  ├── probe_results/   = CSS selector probe output data
  └── logs/            = Script execution logs
04_Processed_data/     = Parquet files, partitioned datasets, news/VOC artifacts, chroma_db (DO NOT SCAN)
05_DashBoard/          = Legacy Streamlit (ARCHIVED, DO NOT MODIFY)
06_AppPlatform/
  ├── backend/         = FastAPI (20 routers, 40+ services, 6 infra repos, 16 alembic versions)
  └── frontend/        = React+TypeScript+Vite (21 pages, 42 components, 3 contexts, 20 test files)
07_ScrapingToolkit/    = Installable package jato-scraping-toolkit (MSRP/news/VOC/EVKX scrapers)
08_GitNexus/           = Separate code-intelligence product (independent git repo, DO NOT MODIFY from JATO context)
airflow/               = 3 DAGs + Dockerfile + plugins (local orchestration)
hermes/                = Governance data: 8 YAML registries + 3 JSONL ledgers + prompts + 15 reports
Markdown_Readme/       = All documentation (Fullstack/, UI/, Streamlit/, Hermes/)
.github/               = GitHub Actions (CI/CD, country-news-sync, hermes-devsync)
.githooks/             = Pre-commit and post-commit hooks
data_wangler/          = Single compareO9.html utility
```

## Backend route map (all under /v1/ except health)

| Route prefix | Router file | Purpose |
|---|---|---|
| `/healthz` | `routes/health.py` | Health check |
| `/v1/assistant` | `routes/assistant.py` | Country Copilot chat, chart-deck, news |
| `/v1/metadata` | `routes/metadata.py` | Column metadata |
| `/v1/filters` | `routes/filters.py` | Filter options |
| `/v1/analysis` | `routes/analysis.py` | Analytics queries |
| `/v1/crud` | `routes/crud.py` | Generic CRUD (may be disconnected) |
| `/v1/platform/db` | `routes/platform_db.py` | Platform DB health |
| `/v1/engineering` | `routes/engineering.py` | Engineering projects, variants, imports |
| `/v1/engineering/config` | `routes/engineering_config.py` | Config matrix CRUD + versioning |
| `/v1/msrp` | `routes/msrp.py` | MSRP batches, current prices, sources |
| `/v1/msrp/links` | `routes/msrp_links.py` | JatoMsrpLink CRUD |
| `/v1/msrp/workflow` | `routes/msrp_workflow.py` | MSRP workflow state |
| `/v1/msrp/monthly-update` | `routes/msrp_monthly_update.py` | Monthly JATO update jobs |
| `/v1/msrp/dryrun` | `routes/msrp_dryrun_dashboard.py` | Dry-run dashboard |
| `/v1/review` | `routes/review.py` | Review overrides |
| `/v1/review/cases` | `routes/review_cases.py` | Review cases + workbench |
| `/v1/hermes` | `routes/hermes.py` | Hermes governance chat + dev sync |
| `/v1/auth` | `routes/auth.py` | Authentication |
| `/v1/presence` | `routes/presence.py` | User presence WebSocket |
| `/v1/market-scan` | `routes/market_scan.py` | Market scan data |
| `/v1/data-management` | `routes/data_management.py` | Data management overview |

## Frontend page map

| Page component | Route/file | Key features |
|---|---|---|
| `DashboardPage` | `/` | KPI cards, trend charts, hero chips |
| `MarketOverviewPage` | `/market-overview` | Country-level market overview |
| `MarketScanPage` | `/market-scan` | Monthly market scan deck |
| `MarketSegmentsPage` | `/market-segments` | Segment analysis |
| `MarketBrandRankingPage` | `/market-brand-ranking` | Brand rankings |
| `MarketModelRankingPage` | `/market-model-ranking` | Model rankings |
| `MarketPowertrainPage` | `/market-powertrain` | Powertrain mix analysis |
| `PositioningPricingPage` | `/positioning-pricing` | Positioning map + peer corridor + price stance |
| `VersionComparisonPage` | `/version-comparison` | Cross-segment bubble chart + Smart Label System |
| `SpecificationPage` | `/specification` | Vehicle specifications |
| `EngineeringPage` | `/engineering` | Engineering data browser |
| `EngineeringConfigPage` | `/engineering-config` | 5-tab: trims/compare/matrix/upload/diff |
| `MsrpPage` | `/msrp` | MSRP management |
| `ReviewCasesPage` | `/review-cases` | Review cases + delivery panel |
| `CountryChatPage` | `/copilot` | Country Copilot (mobile-first, grounded answers) |
| `CustomerInsightsPage` | `/customer-insights` | Benchmark Excel + Forum VOC Live dual mode |
| `DataManagementPage` | `/data-management` | System overview, Airflow controls, VOC observatory |
| `JatoMonthlyUpdatePage` | `/monthly-update` | Monthly JATO upload + refresh jobs with UX guardrails (publish requires review, upload disabled during active job) |
| `LoginPage` | `/login` | Auth + role upgrade |
| `CrudPage` | `/crud` | Generic CRUD |
| `NotFoundPage` | `*` | 404 |

## Key data flow

```
JATO monthly Excel (01_RAW_DATA)
  → elt_worker.py (03_Scripts/)
  → Partitioned Parquet dataset (04_Processed_data/partitioned_dataset_v1/)
  → FastAPI query_service.py reads Parquet + precomputed aggregates
  → Frontend pages consume /v1/analysis, /v1/filters, /v1/metadata

JATO monthly update production rule:
  → Active runtime data is 04_Processed_data/jato_full_archive.parquet + partitioned_dataset_v1
  → Web monthly-update publish owns production data advancement
  → GitHub Actions deploys code and excludes 04_Processed_data
  → Do not overwrite cloud active data from stale local parquet
  → See Fullstack/04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md

MSRP scraping (07_ScrapingToolkit/)
  → source YAML → dry-run → ingest → review cases
  → CurrentPrice + PriceHistory tables (PostgreSQL)
  → Frontend: MsrpPage, ReviewCasesPage, PositioningPricingPage

News/VOC scraping (07_ScrapingToolkit/)
  → news_runner.py / voc_fetcher.py → raw artifacts
  → voc_enricher.py → enriched signals + country deck
  → news_digest_service.py → PostgreSQL news tables
  → Frontend: CountryChatPage, CustomerInsightsPage, DataManagementPage

Engineering Config
  → Excel upload → FieldMappingParser + MatrixParser
  → identity_key match + diff preview → draft → published → archived
  → Frontend: EngineeringConfigPage (5 tabs)
```

## Key business entities

- **JATO identity:** `jato_make` + `jato_model` + `jato_variant` + `jato_version` — 4-level vehicle hierarchy
- **MSRP lifecycle:** source → scrape_batch → observation → review_case → decision → current_price (+ price_history)
- **JatoMsrpLink:** stable JATO→official source key mapping
- **MatchOverride:** dated override of link results within `valid_from_date`/`valid_to_date`
- **Engineering identity key:** `material_no|vehicle_code|market|model_year|trim_name`
- **Config versioning:** draft → published → archived (never overwrite published directly)
- **Copilot routes:** `market-scan`, `positioning-focus`, `segment-fuel-focus`, `precise-lookup`
- **Copilot output:** 6-section report (核心发现 → 数据证据 → 因果分析 → 市场背景 → 趋势展望 → 进一步建议)

## Hermes governance (4 physical locations)

| Location | What | Count |
|---|---|---|
| `hermes/` (root) | YAML registries, JSONL ledgers, prompt .txt | 8 YAML + 3 JSONL + 1 txt |
| `03_Scripts/hermes/` | Python CLI tools | 13 .py + 1 .sh |
| `Markdown_Readme/Fullstack/Hermes/` | Governance design docs | 7 .md |
| `Markdown_Readme/Hermes/` | DevSync contract | 1 .md |

## Tests

| Layer | Location | Count | Run command |
|---|---|---|---|
| Backend unit | `06_AppPlatform/backend/tests/unit/` | ~30 files | `python -m pytest tests/unit -x -q` |
| Backend integration | `06_AppPlatform/backend/tests/integration/` | ~18 files | `python -m pytest tests/integration -x -q` |
| Frontend unit | `06_AppPlatform/frontend/src/tests/unit/` | 20 files | `npx vitest run --reporter=verbose` |
| Frontend typecheck | — | — | `npx tsc --noEmit --pretty` |
| Scraping toolkit | `07_ScrapingToolkit/tests/` | 12 files | `python -m pytest tests/ -x -q` |
| Script-level | `03_Scripts/tests/` | 3 files | `python -m pytest` |

## External services / API keys

| Service | Env var | Used by |
|---|---|---|
| DeepSeek | `DEEPSEEK_API_KEY` | Country Copilot (primary LLM) |
| Tavily | `TAVILY_API_KEY` | Country Copilot live search (priority 1) |
| Google CSE | `GOOGLE_CUSTOM_SEARCH_API_KEY` + `GOOGLE_CSE_ID` | Country Copilot live search (priority 2) |
| SerpAPI | `SERPAPI_API_KEY` | Country Copilot live search (priority 3) |
| Gemini | `GEMINI_API_KEY` | News digest summary/tagging |
| NVIDIA NIM | `NVIDIA_API_KEY` or `NVAPI_KEY` | MSRP page LLM analysis |
| HuggingFace | HF token | LLM provider (alternative) |
| PostgreSQL | `APP_DATABASE_URL` | Business truth layer |
| Redis | `APP_REDIS_URL` | Caching |

## Documentation index (quick lookup)

| Topic | Primary doc |
|---|---|
| Project overview | `Fullstack/ROADMAP.md` |
| Architecture review | `Fullstack/ARCHITECTURE_REVIEW_2026-04-17.md` |
| Dev workflow | `Fullstack/01_DevWorkflow/SOFTWARE_DEV_WORKFLOW.md` |
| PR checklist | `Fullstack/01_DevWorkflow/PR_CHECKLIST.md` |
| Data/ETL | `Fullstack/02_DataETL/ETL.md` |
| Precompute strategy | `Fullstack/02_DataETL/PRECOMPUTE_STRATEGY.md` |
| Database design | `Fullstack/03_Database/PLATFORM_STACK_AND_DATABASE_BOUNDARY_2026-04-10.md` |
| PostgreSQL schema | `Fullstack/03_Database/POSTGRESQL_CORE_SCHEMA_2026-04-10.md` |
| Cross-source join | `Fullstack/03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md` |
| MSRP overview | `Fullstack/MSRP/README.md` |
| MSRP pipeline | `Fullstack/MSRP/03_Implementation/MSRP_PIPELINE_TECHNICAL_FLOW_2026-04-11.md` |
| MSRP execution plan | `Fullstack/MSRP/05_Backlog/MSRP_SUV_COUNTRY_MODEL_TOP30_PLAN_2026-04-12.md` |
| Deployment | `Fullstack/04_DevOps/TENCENT_CLOUD_DEPLOY.md` |
| CI/CD | `Fullstack/04_DevOps/MANUAL_CICD.md` |
| JATO monthly data lifecycle | `Fullstack/04_DevOps/JATO_MONTHLY_UPDATE_DATA_LIFECYCLE_2026-05-17.md` |
| Scheduled fetch recovery | `Fullstack/04_DevOps/SCHEDULED_FETCH_OPERATIONS.md` |
| Local dev | `Fullstack/04_DevOps/FULLSTACK_LOCAL_DEBUG.md` |
| UI spec | `../UI/UI_SPECIFICATION_V1.md` |
| Backend README | `../../06_AppPlatform/backend/README.md` |
| Scraping README | `../../07_ScrapingToolkit/README.md` |
| Business workflows | `Fullstack/WORKFLOWS/BUSINESS_PIPELINE_WORKFLOWS.md` |
| System workflow | `Fullstack/WORKFLOWS/REPOSITORY_SYSTEM_WORKFLOW.md` |
| Copilot PRD | `Fullstack/01_DevWorkflow/COUNTRY_COPILOT_GOVERNED_ANALYTICAL_COPILOT_PRD_2026-05-11.md` |
| Hermes plan | `Fullstack/Hermes/HERMES_IMPLEMENTATION_PLAN_2026-05-14.md` |
| Hermes asset map | `Fullstack/Hermes/REPOSITORY_ASSET_MAP.md` |
| Streamlit archive | `../Streamlit/README.md` |

## Key conventions

- **Language:** Variable names, comments, and commit messages in English. Business docs mix English + Chinese.
- **Date format in filenames:** `YYYY-MM-DD` suffix (e.g., `*_2026-04-10.md`)
- **Config format:** YAML for source/batch configs, .xlsx for business metadata
- **Data format:** Parquet for analytical data, JSON for artifacts/reports, JSONL for event streams
- **API style:** RESTful, JSON request/response, prefix `/v1/`
- **Auth:** Token-based, optional per env (`APP_AUTH_ENABLED`), roles: viewer/editor/admin
- **Git:** main branch, pre-commit + post-commit hooks, GitHub Actions CI/CD
