# Repository Asset Map for Hermes Governance

> Generated: 2026-05-14
> Phase: 0 — Asset Discovery
> Status: First pass complete. Items marked `needs verification` indicate uncertainty.

---

## 1. Executive Summary

| Metric | Count |
|---|---|
| Product features | 12 |
| Pipelines (systemd timers) | 4 |
| Pipelines (Airflow DAGs) | 3 |
| Pipelines (GitHub Actions scheduled) | 1 |
| Crawler modules (Scraping Toolkit) | 7 CLI entry points |
| Scheduled jobs (total, all systems) | 8 |
| LLM prompts (identified in code) | 3 (unversioned) |
| Artifact types | 10+ |
| API endpoints | 100+ |
| Frontend routes | 14 |
| Backend services | 25+ |
| Documentation assets (active) | 55 |
| Major governance gaps | 15+ |

---

## 2. Data Assets

| Asset | Path / Table | Producer | Consumer | Governance Status |
|---|---|---|---|---|
| JATO Full Archive (Parquet) | `04_Processed_data/jato_full_archive.parquet` | `elt_worker.py` (ETL) | `query_service.py`, `market_scan_service.py` | Registered |
| Partitioned Dataset | `04_Processed_data/partitioned_dataset_v1/` | `build_partitioned_dataset.py` | All analytical queries | Registered |
| Precomputed Summaries | `04_Processed_data/summaries/` | `precompute_summaries.py` | Dashboard, Country Copilot | Registered |
| CRUD Entities JSON | `04_Processed_data/app_entities.json` | Backend CRUD service | CRUD API, Data Management | Registered |
| Country News Digest (PG) | `ops.country_news_digest` | `sync_country_news_digest.py` | Country Copilot, Country Chat | Registered |
| Country News Articles (PG) | `ops.country_news_article` | `sync_country_news_digest.py` | Country Copilot | Registered |
| VOC Source Runs (PG) | `ops.voc_source_run` | `voc_fetcher.py` → staging sync | Customer Insights, Data Management | Registered |
| VOC Raw Documents (PG) | `ops.voc_raw_document` | `voc_fetcher.py` → staging sync | Customer Insights, VOC Live | Registered |
| MSRP Sources (PG) | `msrp.msrp_source` | Manual + seed scripts | MSRP Workbench | Registered |
| MSRP Observations (PG) | `msrp.msrp_observation` | `batch_ingest.py`, scrapers | CurrentPrice, Review | Registered |
| Current Prices (PG) | `msrp.current_price` | `materialize_current_prices()` | Country Copilot, MSRP API | Registered |
| Price History (PG) | `msrp.price_history` | Materialize pipeline | MSRP Workbench | Registered |
| Review Cases (PG) | `review.review_case` | MSRP observation pipeline | Review Workbench | Registered |
| Engineering Variants (PG) | `engineering.*` | Excel import → normalization | Engineering Page, Spec diff | Registered |
| MSRP Candidate Scope | `04_Processed_data/msrp_candidate_scope/` | `generate_msrp_candidate_scope.py` | `source_bootstrap.py`, backlog | Registered |
| MSRP Source Drafts | `07_ScrapingToolkit/source_drafts/` | `generate_msrp_source_drafts.py` | `batch_dryrun.py`, `batch_ingest.py` | Registered |
| VOC Raw Artifacts | `04_Processed_data/voc/` | `voc_fetcher.py` | `voc_enricher.py`, staging sync | Registered |
| VOC Enriched Artifacts | `04_Processed_data/voc/` | `voc_enricher.py` | Customer Insights, Country Copilot | Registered |
| Scheduled Fetch Status | `03_Scripts/logs/scheduled_fetch_status.json` | `run_voc_forum_sync.sh`, `run_country_news_sync.sh` | Data Management (future) | Registered |
| Dataset Fingerprint | `04_Processed_data/dataset_fingerprint.json` | `run_data_refresh_job.py` | CI, freshness checks | Registered |

---

## 3. Crawler Assets

| Crawler | Path | Target | Source Type | Output | Schedule | Status |
|---|---|---|---|---|---|---|
| MSRP Scraper (runner) | `07_ScrapingToolkit/jato_scraper/runner.py` | Official brand configurators | HTML/CSS/JSON | MSRP Observations → PG | systemd timer (daily dryrun, weekly ingest) | Active |
| MSRP HTTP JSON extractor | `.../extractors/http_json.py` | REST pricing APIs | JSON API | MSRP Observations | Part of runner | Active |
| MSRP Scrapling extractor | `.../extractors/scrapling_web.py` | Configurator pages | HTML DOM | MSRP Observations | Part of runner | Active |
| MSRP Playwright extractor | `.../extractors/playwright_card_flow.py` | Interactive configurators | Browser automation | MSRP Observations | Part of runner | Active |
| MSRP PDF extractor | `.../extractors/pdf_text.py` | Official price PDFs | PDF text | MSRP Observations | Part of runner | Active |
| News RSS/Atom fetcher | `.../news/news_runner.py` | RSS/Atom feeds | XML | News articles → PG | systemd timer (daily 23:15) | Active |
| VOC Forum fetcher | `.../voc/voc_fetcher.py` | Nordic automotive forums | HTML (Trafilatura) | Raw docs → PG | systemd timer (daily 01:45) | Active |
| VOC Enricher | `.../voc/voc_enricher.py` | Raw VOC docs | JSON | Enriched signals, deck | Part of VOC pipeline | Active |
| EVKX Catalog fetcher | `.../evkx_catalog.py` | evkx.net API | JSON API | EV catalog JSON | Manual | Active |
| LLM MSRP Page Analyzer | `.../llm/msrp_page_analyzer.py` | MSRP page screenshots | LLM vision | Page analysis JSON | Manual | Active |

### Source Configurations

| Config Set | Path | Countries | Count | Status |
|---|---|---|---|---|
| MSRP Batch A | `msrp_batches/batch_a.yaml` | SE,FI,NO,DK,HU,HR,AT,CZ | ~200+ sources | Active |
| MSRP Source Drafts (SUV Top30) | `source_drafts/suv_only_country_model_top30/` | 7+ countries | ~30/country | Active |
| News Batch A | `news_sources/batch_a.yaml` | SE,FI,HU,NO,DK,AT,CZ,HR | 8 countries | Active |
| News Batch B | `news_sources/batch_b.yaml` | DE,FR,IT,ES,BE,NL,PL,PT,RO,SI,SK,GR | 12 countries | Active |
| VOC Batch A | `voc_sources/batch_a.yaml` | SE,FI,NO,DK,AT,CZ,HR,HU | 8 countries | Active |
| MSRP Sources (prod) | `sources/` | Mixed | ~3 (Volvo SE, BMW DE, template) | Active |

---

## 4. Pipeline Assets

### 4.1 Systemd Timers (Production Schedulers)

| Pipeline | Type | Trigger | Output | Depends On | Risk |
|---|---|---|---|---|---|
| `jato-country-news-sync.timer` | systemd timer | Daily 23:15 UTC | News articles in PG | `backend.env` (DB), `country-news.env` | Medium — duplicate with Airflow DAG |
| `jato-voc-forum-sync.timer` | systemd timer | Daily 01:45 UTC | VOC raw + enriched + deck | `backend.env` (DB), `voc.env` | Medium — single-source failure kills batch (soft-failure added 2026-05-14) |
| `jato-msrp-dryrun.timer` | systemd timer | Daily 03:30 UTC | Dry-run reports | `msrp.env`, source drafts | Low |
| `jato-msrp-ingest.timer` | systemd timer | Weekly Sat 05:30 UTC | MSRP observations in PG | `msrp.env`, `backend.env` (DB) | High — writes production data |

### 4.2 Airflow DAGs

| Pipeline | DAG ID | Trigger | Output | Depends On | Risk |
|---|---|---|---|---|---|
| Country News Sync | `jato_country_news_sync` | Daily 06:15 UTC | News via API → PG | `JATO_API_BASE`, `JATO_API_TOKEN` | Medium — **duplicates systemd timer** |
| MSRP Low Concurrency | `jato_msrp_low_concurrency` | Manual only | MSRP dryrun → ingest | `03_Scripts/run_msrp_low_concurrency.sh` | Low |
| Scraping Toolkit Manual | `jato_scraping_toolkit_manual` | Manual only | On-demand scrape | `run_scraping_tool.sh` | Low |

### 4.3 GitHub Actions Scheduled

| Pipeline | Workflow | Trigger | Output | Depends On | Risk |
|---|---|---|---|---|---|
| Nightly Performance | `nightly-performance` | Daily 01:30 UTC | Performance gate report | `requirements.txt`, parquet data | Low |
| Country News Sync (legacy) | `country-news-sync` | ~~schedule removed~~ → `workflow_dispatch` only | Dry-run news digest | `secrets.APP_DATABASE_URL` (optional) | Low — now manual only |

### 4.4 ETL Pipeline

| Step | Script | Trigger | Input | Output |
|---|---|---|---|---|
| 1. Raw ingest | `prepare_monthly_raw_update.py` | Manual | `01_RAW_DATA/new/*.xlsx` | `01_RAW_DATA/baseline/` |
| 2. Parquet build | `elt_worker.py` | Manual | `01_RAW_DATA/*.xlsx` | `jato_full_archive.parquet` |
| 3. Partition | `build_partitioned_dataset.py` | Part of refresh job | `jato_full_archive.parquet` | `partitioned_dataset_v1/` |
| 4. Precompute | `precompute_summaries.py` | Manual | Partitioned dataset | `summaries/*.parquet` |
| 5. Cloud sync | `sync_data_to_cloud.sh` | Manual (local) | All processed data | Tencent Cloud server |

### 4.5 Duplicate Schedule Alert

| Function | System A | System B | Risk |
|---|---|---|---|
| **Country News Sync** | systemd timer: `jato-country-news-sync.timer` (23:15 UTC) | Airflow DAG: `jato_country_news_sync` (06:15 UTC) | **HIGH — double ingestion** |

---

## 5. LLM Assets

### 5.1 Prompts (Hardcoded in Code — NOT Versioned)

| Prompt | File:Line | Model | Purpose | Versioned? | Risk |
|---|---|---|---|---|---|
| Nvidia/Gemini System Prompt | `country_chat_service.py:469-506` | NVIDIA NIM / Gemini | 13 Chinese analysis principles for Country Copilot | No | Medium |
| DeepSeek Stable System Prompt | `country_chat_service.py:508-542` | DeepSeek V4-Flash | 6-section structured report (核心发现→数据证据→因果分析→市场背景→趋势展望→进一步建议) | No | Medium |
| DeepSeek Ephemeral System Message | `routes/assistant.py:95-109` | DeepSeek V4-Flash | Streaming answer with evidence pack context | No | Medium |

### 5.2 LLM Provider Configurations

| Provider | Config Location | Model Used | API Base |
|---|---|---|---|
| DeepSeek | `country_chat_service.py` + env | `deepseek-chat` (V4-Flash) | `api.deepseek.com/chat/completions` |
| NVIDIA NIM | `country_chat_service.py` + `jato_scraper.llm.providers` | Configurable | NVIDIA API |
| Google Gemini | `news_digest_service.py` | `gemini-2.5-flash` | `generativelanguage.googleapis.com` |
| HuggingFace | `jato_scraper.llm.providers` | Configurable | HF Inference API |

### 5.3 Model Routing (Country Copilot)

| Route | Model | When |
|---|---|---|
| `direct_lookup` | None (data only) | Factual metric query |
| `short_answer` | DeepSeek V4-Flash | Simple question |
| `grounded_analysis` | DeepSeek V4-Flash | Multi-source analysis |
| `deep_report` | DeepSeek V4-Flash | Complex multi-section report |
| `hypothesis` | DeepSeek V4-Flash | Speculative with disclaimer |
| `insufficient_evidence` | None | Not enough data |

---

## 6. Product Features

| Feature | Route | Backend API | Data Dependencies | Pipeline Dependencies | Docs | Risk |
|---|---|---|---|---|---|---|
| **Dashboard** | `/` | `/v1/analysis/*` | JATO Parquet | ETL pipeline | `Dashboard_Layout_Baseline` | Low |
| **Specification** | `/specification` | `/v1/analysis/detail` | JATO Parquet | ETL pipeline | `UI_SPECIFICATION_V1.md` | Low |
| **Country Copilot** | `/copilot` | `/v1/assistant/country/*` | JATO Parquet + PG (news, MSRP, VOC) | News sync, MSRP, VOC | `COUNTRY_COPILOT_PRD` | Medium |
| **Market Scan** | `/market-scan` | `/v1/market-scan/deck` | JATO Parquet | ETL pipeline | WORKFLOWS docs | Low |
| **Positioning Pricing** | `/positioning-pricing` | `/v1/market-scan/positioning-pricing-deck` | JATO Parquet | ETL pipeline | PRD in §14 | Low |
| **Version Comparison** | `/version-comparison` | `/v1/market-scan/version-comparison-deck` | JATO Parquet | ETL pipeline | PRD §20 Smart Label | Low |
| **Customer Insights** | `/customer-insights` | `/v1/market-scan/nordic-customer-deck` | VOC artifacts + Excel | VOC pipeline | `VOC_FORUM_IMPLEMENTATION_STATUS` | Medium |
| **Nordic HEV Insights** | `/customer-hev` | `/v1/market-scan/nordic-hev-customer-deck` | HEV Excel workbooks | Manual generation | PRD implied | Medium |
| **Data Management** | `/data-management` | `/v1/data-management/*` | Disk + PG + Airflow | All pipelines | `TENCENT_CLOUD_DEPLOY.md` | Low |
| **Engineering** | `/engineering` | `/v1/engineering/*` | Excel imports → PG | None (self-contained) | `PLATFORM_STACK` doc | Low |
| **Review Workbench** | `/review` | `/v1/review/*` | MSRP observations | MSRP pipeline | MSRP docs | Low |
| **MSRP Workbench** | `/msrp` | `/v1/msrp/*` | MSRP sources + prices | MSRP pipeline | MSRP docs §01-05 | Medium |
| **JATO Monthly Update** | `/msrp/monthly-update` | `/v1/msrp/monthly-update-*` | Uploaded XLSX + PG | ETL pipeline | MSRP docs | Medium |
| **Current Price** | `/msrp` (tab) | `/v1/msrp/current-prices` | PG | MSRP pipeline | MSRP docs | Low |

---

## 7. Operations Assets

| Asset | Path | Purpose | Related Service | Risk |
|---|---|---|---|---|
| CI workflow | `.github/workflows/ci.yml` | Code quality gate (style, compile, regression) | All pushes/PRs | Low |
| Deploy (Tencent Cloud) | `.github/workflows/deploy-fullstack-tencent.yml` | Auto-deploy to production | ojeur.cloud | Medium |
| Deploy (AWS ECS) | `.github/workflows/deploy-aws-ecs.yml` | Manual AWS deploy | ECS | Low |
| Deploy (EC2 auto-update) | `.github/workflows/deploy-ec2-auto-update.yml` | Streamlit auto-deploy | Legacy | Low |
| Nightly Performance | `.github/workflows/nightly-performance.yml` | JATO data read/transform perf gate | All analytical features | Low |
| Docker Compose (Airflow) | `docker-compose.yml` | Local Airflow stack | Airflow DAGs | Low |
| Docker Compose (GitNexus) | `08_GitNexus/docker-compose.yaml` | Local GitNexus | GitNexus | Low (separate project) |
| Systemd timers | `03_Scripts/deploy/systemd/*.timer` | Production cron jobs | Server | Medium |
| Nginx configs | `03_Scripts/deploy/nginx/` | Reverse proxy + HTTPS | ojeur.cloud | Medium |
| Load tests | `03_Scripts/deploy/loadtest/` | K6 + cold-start benchmarks | Performance monitoring | Low |

---

## 8. Knowledge Assets

| Document / Tool | Path | Purpose | Freshness | Risk |
|---|---|---|---|---|
| ROADMAP.md | `Markdown_Readme/Fullstack/ROADMAP.md` | Master index | 2026-05-14 | Low |
| CLAUDE.md | Root `CLAUDE.md` | Project instructions | 2026-05-11 | Low |
| Hermes Implementation Plan | `Markdown_Readme/Fullstack/Hermes/` | Governance layer plan | 2026-05-14 | New |
| Architecture Review | `Fullstack/ARCHITECTURE_REVIEW_2026-04-17.md` | Cross-domain review | 2026-04-17 | Low |
| Product Deep Dive | `Fullstack/PRODUCT_DEEPDIVE_2026-04-17.md` | Six-question answer scroll | 2026-04-18 | Low |
| Software Dev Workflow | `01_DevWorkflow/SOFTWARE_DEV_WORKFLOW.md` | SDLC process | 2026-04-11 | Low |
| Fullstack Dev Spec | `01_DevWorkflow/FULLSTACK_DEVELOPMENT_SPEC_2026-04-11.md` | Contract tests | 2026-04-11 | Low |
| PR Checklist | `01_DevWorkflow/PR_CHECKLIST.md` | Pre-submit checklist | Undated | Low |
| Country Copilot PRDs | `01_DevWorkflow/COUNTRY_COPILOT_*` (3 docs) | Copilot design | 2026-04-15 to 2026-05-11 | Medium — 3 overlapping docs |
| Version Comparison PRD | `01_DevWorkflow/version_comparison_*` | VC + Smart Label | Undated | Low |
| Presence WebSocket PRD | `01_DevWorkflow/presence_websocket_prd.md` | Real-time collab | Undated | Medium — unimplemented |
| Ranking Trend PRD | `01_DevWorkflow/ranking_trend_drilldown_prd.md` | Trend drilldown | Undated | Medium — unimplemented |
| ETL docs | `02_DataETL/` (8 docs) | Data processing pipeline | Various | Low |
| Database docs | `03_Database/` (5 docs) | Schema + migration | Various | Low |
| DevOps docs | `04_DevOps/` (7 docs) | Deploy + debug | Various | Low |
| MSRP docs | `MSRP/` (16 docs across 5 phases) | Full MSRP lifecycle | Various | Low |
| Business Workflows | `WORKFLOWS/` (4 docs) | Business + system maps | Various | Low |
| UI Spec | `../UI/UI_SPECIFICATION_V1.md` | Visual + layout spec | 2026-04-11 | Low |
| Streamlit Archive | `../Streamlit/` (10 docs) | Historical reference | Various | Archived |
| Scraping Toolkit README | `07_ScrapingToolkit/README.md` | Toolkit docs | 2026-04-21 | Low |
| GitNexus | `08_GitNexus/` | Code intelligence (separate product) | Ongoing | Low (separate project) |

---

## 9. Dependency Graph

```
Crawler Layer:
  MSRP scrapers ──→ MSRP Observations (PG)
  News RSS/Atom ──→ News Articles (PG)
  VOC Forum fetch ──→ VOC Raw Docs (PG)
  EVKX Catalog ──→ EV reference data (PG)

Artifact Layer:
  ETL (elt_worker.py) ──→ jato_full_archive.parquet
  build_partitioned_dataset.py ──→ partitioned_dataset_v1/
  precompute_summaries.py ──→ summaries/*.parquet
  VOC enricher ──→ enriched signals + deck

Backend API Layer:
  query_service.py ──→ Parquet data → Dashboard, MarketScan, Positioning
  country_chat_service.py ──→ Parquet + PG (news, MSRP, VOC) → Country Copilot
  market_scan_service.py ──→ Parquet → Market Scan Deck
  news_digest_service.py ──→ RSS + Gemini → News Digest
  customer_insight_service.py ──→ VOC artifacts → Customer Insights

Frontend Feature Layer:
  Dashboard ← query_service
  Market Scan ← market_scan_service
  Country Copilot ← country_chat_service (SSE streaming)
  Version Comparison ← market_scan_service
  Positioning Pricing ← market_scan_service
  Customer Insights ← customer_insight_service
  Data Management ← data_management_service
  Engineering ← engineering_service
  MSRP Workbench ← msrp_workflow_service
  Review Workbench ← review_service
  JATO Monthly Update ← jato_monthly_update_service
```

---

## 10. Governance Gaps

### 10.1 Unregistered Assets
- [ ] 3 LLM prompts hardcoded in Python — no version numbers, no registry
- [ ] News Batch B (12 countries) has no scheduled fetch — only Batch A (8 countries) is scheduled
- [ ] EVKX catalog fetcher has no schedule — manual only
- [ ] Precomputed summaries have no automated refresh trigger

### 10.2 Duplicate Scheduling
- [ ] **Country News Sync duplicated**: systemd timer (23:15 UTC) + Airflow DAG (06:15 UTC) — double ingestion risk
- [ ] MSRP has both systemd timer and Airflow DAG — but Airflow DAG is manual-only (acceptable)

### 10.3 Missing Versioning
- [ ] All 3 LLM prompts unversioned
- [ ] Artifact schemas not versioned (VOC raw, enriched, deck; News digest)
- [ ] No prompt changelog

### 10.4 Missing Documentation
- [ ] `presence_websocket_prd.md` — PRD exists but feature not implemented
- [ ] `ranking_trend_drilldown_prd.md` — PRD exists but feature not implemented
- [ ] No runbook for scheduled fetch failure recovery
- [ ] No architecture decision log (ADR)

### 10.5 Missing Tests
- [ ] No integration tests (all tests are unit tests per directory structure)
- [ ] No contract tests between frontend types and backend serializers
- [ ] No pipeline end-to-end tests

### 10.6 Environment Variable Gaps
- [ ] `APP_DATABASE_URL` commented out in example — no clear setup instructions
- [ ] `DEEPSEEK_API_KEY` commented out in example
- [ ] `GEMINI_API_KEY` commented out in example
- [ ] No env var documentation in a single location

### 10.7 Feature Registry Gaps
- [ ] No single source of truth listing all features with owners
- [ ] No feature status tracking (active/beta/deprecated)
- [ ] Country Copilot has 3 overlapping PRD documents

### 10.8 Source Quality Gaps
- [ ] No per-source success rate tracking in registry
- [ ] VOC source errors (8 errors in last run) not systematically tracked
- [ ] News source timeouts (Google RSS blocked from Chinese server) not tracked per-source

---

## 11. Recommended Registry Seeds

### 11.1 source_registry.yaml (initial entries)
- 8 MSRP crawler extractors (Scrapling, HTTP JSON, Playwright, PDF)
- 2 News batch configs (Batch A: 8 countries, Batch B: 12 countries)
- 1 VOC batch config (Batch A: 8 countries)
- 1 EVKX catalog fetcher

### 11.2 pipeline_registry.yaml (initial entries)
- 4 systemd timers
- 3 Airflow DAGs
- 1 GitHub Actions schedule (nightly perf)
- 1 ETL pipeline (4 steps)
- 1 deploy pipeline (Tencent Cloud)

### 11.3 feature_registry.yaml (initial entries)
- 12 product features (see §6)

### 11.4 prompt_registry.yaml (initial entries)
- 3 Country Copilot prompts (unversioned → needs versioning)
- 1 News digest Gemini enrichment prompt
- 1 VOC enrichment LLM prompt

### 11.5 artifact_registry.yaml (initial entries)
- 20 data artifacts (see §2)

---

## 12. Next Steps

1. **Phase 1**: Create the 5 registry YAML files with seed entries from this map
2. **Phase 1**: Assign initial risk levels and owners (where known)
3. **Phase 2**: Implement `hermes_intake.py` for PRD impact analysis
4. **Phase 2**: Add Hermes Governance section to PRD template
5. **Phase 3**: Implement `hermes_code_audit.py` for post-development audit
6. **Address P0 governance gaps**: Resolve duplicate News scheduling, version the 3 LLM prompts
