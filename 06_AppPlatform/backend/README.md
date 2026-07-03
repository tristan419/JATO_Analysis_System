# Backend (FastAPI)

## Run

```bash
cd 06_AppPlatform/backend
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

# PostgreSQL (optional but recommended for new business modules)
export APP_DATABASE_ENABLED=true
export APP_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/jato_app
export APP_ENGINEERING_IMPORT_ROOT=/absolute/path/to/engineering-xlsx-dir

# Alembic
alembic upgrade head

# Country news prefetch (recommended before using Country Copilot news layer)
python ../../03_Scripts/sync_country_news_digest.py --workers 4
python ../../03_Scripts/run_country_news_sync.sh

# Optional Gemini enrichment for digest summary/tagging
export GEMINI_API_KEY=your-key
python ../../03_Scripts/sync_country_news_digest.py --workers 4

# Optional country assistant live-search providers
# Priority: Tavily -> Google Custom Search -> SerpAPI -> Google News RSS fallback.
export TAVILY_API_KEY=your-tavily-key
export GOOGLE_CUSTOM_SEARCH_API_KEY=your-google-cse-api-key
export GOOGLE_CSE_ID=your-google-cse-id
export SERPAPI_API_KEY=your-serpapi-key

# Runtime requests read DB snapshots first; live external fetch is off by default
export APP_COUNTRY_NEWS_LIVE_FETCH=false

# Optional local scheduler (macOS launchd)
/bin/bash ../../03_Scripts/install_local_country_news_sync_launchd.sh 6 15

# Auth (enabled by default)
export APP_AUTH_ENABLED=false
export APP_AUTH_TOKEN=change-me

# Example request header
# X-Auth-Token: change-me
# X-User-Role: viewer|editor|admin
# X-User-Name: your-name
```

## Test

```bash
cd 06_AppPlatform/backend
pip install -r requirements-dev.txt
python -m pytest tests/unit
```

## API

- GET /healthz
- GET /v1/assistant/country/metadata
- POST /v1/assistant/country/chat
- POST /v1/assistant/country/chart-deck
- GET /v1/assistant/country/news/status?country=Germany
- POST /v1/assistant/country/news/refresh
- GET /v1/metadata/columns
- POST /v1/filters/options
- POST /v1/analysis/query
- POST /v1/analysis/time-series-grouped
  - response header `X-JATO-Server-Cache`: `MISS`, `MEMORY`, `REDIS`, `DISK`, or `INFLIGHT`
- GET /v1/crud/items
- POST /v1/crud/items
- PATCH /v1/crud/items/{item_id}
- DELETE /v1/crud/items/{item_id}
- GET /v1/platform/db/health
- GET /v1/engineering/projects
- POST /v1/engineering/projects
- POST /v1/engineering/projects/{project_id}/imports
- POST /v1/engineering/projects/{project_id}/normalize
- PATCH /v1/engineering/projects/{project_id}
- GET /v1/engineering/projects/imports
- GET /v1/engineering/projects/imports/{config_import_batch_id}
- GET /v1/engineering/projects/imports/{config_import_batch_id}/page-data
- GET /v1/engineering/projects/variants
- GET /v1/engineering/projects/base-variants
- GET /v1/engineering/projects/market-variants
- GET /v1/engineering/projects/feature-overrides
- POST /v1/msrp/batches
- GET /v1/msrp/current-prices
- POST /v1/msrp/current-prices/materialize
- GET /v1/msrp/links
- POST /v1/msrp/links
- PATCH /v1/msrp/links/{link_id}
- GET /v1/msrp/sources
- POST /v1/msrp/sources
- PATCH /v1/msrp/sources/{source_id}
- GET /v1/msrp/sources/batches
- GET /v1/msrp/sources/observations
- GET /v1/review/cases
- GET /v1/review/cases/workbench
- GET /v1/review/cases/{review_case_id}
- POST /v1/review/cases/{review_case_id}/decisions
- GET /v1/review/overrides
- POST /v1/review/overrides
- PATCH /v1/review/overrides/{override_id}

## Dashboard cache prewarm

After a backend deploy, warm the default Dashboard grouped time-series queries
before real users hit the cold path:

```bash
python 03_Scripts/diagnostics/prewarm_grouped_time_series.py \
  --origin http://127.0.0.1:8000 \
  --token "$APP_AUTH_TOKEN" \
  --user-name prewarm \
  --user-role order_filler \
  --require-server-cache \
  --require-repeat-hit
```

The script prints one JSON line per request. A healthy repeated run should show
`serverCache` as `MEMORY`, `REDIS`, `DISK`, or `INFLIGHT` rather than `MISS`.
Default startup prewarm covers month/year grouped Dashboard lenses for
`viewer`, `order_filler`, `editor`, and `admin`, including the wide country +
powertrain filter set used by the Intl Dashboard.
`POST /v1/analysis/overview` uses the same server-cache pattern and returns
`X-JATO-Server-Cache: MISS|MEMORY|REDIS|DISK`; startup prewarm covers the empty
Dashboard and the same wide country + powertrain filter set for each role.
Dashboard metadata endpoints also prewarm and persist `GET /v1/metadata/columns`
and `GET /v1/analysis/data-freshness` snapshots using the parquet dataset token,
so backend restarts do not push the first user through schema reads or freshness
aggregation.
`03_Scripts/ops/deploy_fullstack_server.sh` runs the same prewarm automatically
after backend health checks when `APP_GROUPED_TIME_SERIES_PREWARM_ENABLED=true`;
set `RUN_GROUPED_TIME_SERIES_PREWARM=strict` to fail deployment on a cold repeat.

MSRP mapping lifecycle:
- `JatoMsrpLink` = stable active mapping from JATO key to official key
- `MatchOverride` = dated exception that overrides link results within `valid_from_date` / `valid_to_date`
- review approve/remap now writes an active link; `persist_override=true` additionally writes a dated override

## Production

- Tencent Cloud Ubuntu deployment: `Markdown_Readme/Fullstack/TENCENT_CLOUD_DEPLOY.md`
- Manual CI/CD flow: `Markdown_Readme/Fullstack/MANUAL_CICD.md`
- PostgreSQL local dev: `Markdown_Readme/Fullstack/BACKEND_POSTGRES_LOCAL_DEV_2026-04-10.md`
- systemd template: `03_Scripts/deploy/systemd/jato-fullstack-backend@.service`
- nginx template: `03_Scripts/deploy/nginx/jato_fullstack.conf.example`

## Architecture docs

- Country Copilot routing + local/live retrieval: `Markdown_Readme/Fullstack/01_DevWorkflow/COUNTRY_COPILOT_INTENT_AND_HYBRID_RETRIEVAL_2026-04-17.md`
- JATO sales × MSRP join boundary: `Markdown_Readme/Fullstack/03_Database/CROSS_SOURCE_JOIN_DESIGN_2026-04-17.md`
- MSRP version matrix + multi-source reconciliation: `Markdown_Readme/Fullstack/MSRP/03_Implementation/MSRP_VERSION_MATRIX_AND_MULTI_SOURCE_2026-04-17.md`
- Unified scraping pipeline: `Markdown_Readme/Fullstack/02_DataETL/UNIFIED_SCRAPING_PIPELINE_2026-04-17.md`

## Country News Ops

- 默认问答路径只读数据库快照，不实时抓外网。
- 当用户对结果不满意时，前端会调用 `/v1/assistant/country/chat` 并带上 `refresh_news=true`，先在线抓新闻，再让当前选中的聊天模型基于新上下文重答。
- 模型分工：`RSS/Atom` 负责抓取，`Gemini` 负责新闻摘要/归因/标签，国家聊天模型可按 `APP_COUNTRY_CHAT_MODEL_OPTIONS` 在前端切换；支持 `provider:*` 自动展开 provider 可用模型，`auto` 仅在各 provider 默认模型之间轮换。

### Scheduling

- 本地 macOS：`03_Scripts/install_local_country_news_sync_launchd.sh`
- 本地 runner：`03_Scripts/run_country_news_sync.sh`
- GitHub Actions：`.github/workflows/country-news-sync.yml`
- Airflow 适合管理**定时抓取 / 重试 / 依赖串联 / 补跑(backfill)** 这类批处理流程；用户点击后要立刻刷新的在线请求，仍建议保留在应用侧直接触发。

### Airflow UI (local)

```bash
docker compose --profile airflow up airflow-init
docker compose --profile airflow up -d airflow-webserver airflow-scheduler airflow-postgres

# Open http://localhost:8080
# username: admin
# password: admin
```

本地打开 `/data-management` 时，如果宿主机存在 Docker Compose，页面里的 **Local Airflow** 卡片也会提供一键**启动 / 暂停 / 打开 UI**；腾讯云这类无 Docker 环境会自动显示为不可用。

默认会挂出这些 DAG：
- `jato_country_news_sync`
- `jato_msrp_low_concurrency`
- `jato_scraping_toolkit_manual`

手动触发时可在 Airflow UI 的 **Trigger DAG w/ config** 里传 JSON，例如：

```json
{"countries":"se,no","pause_seconds":10}
```

### Cron Example

```bash
15 6 * * * cd /Users/you/Downloads/JATO_Analysis_System && /bin/bash 03_Scripts/run_country_news_sync.sh
```

<!-- deploy trigger: BOM zero FOB empty-state hotfix, 2026-06-22 -->
