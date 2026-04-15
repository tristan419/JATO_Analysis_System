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

# Runtime requests read DB snapshots first; live external fetch is off by default
export APP_COUNTRY_NEWS_LIVE_FETCH=false

# Optional local scheduler (macOS launchd)
/bin/bash ../../03_Scripts/install_local_country_news_sync_launchd.sh 6 15

# Auth (enabled by default)
export APP_AUTH_ENABLED=true
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
- GET /v1/crud/items
- POST /v1/crud/items
- PATCH /v1/crud/items/{item_id}
- DELETE /v1/crud/items/{item_id}
- GET /v1/platform/db/health
- GET /v1/engineering/projects
- POST /v1/engineering/projects
- POST /v1/engineering/projects/{project_id}/imports
- PATCH /v1/engineering/projects/{project_id}
- GET /v1/engineering/projects/imports
- GET /v1/engineering/projects/imports/{config_import_batch_id}
- GET /v1/engineering/projects/imports/{config_import_batch_id}/page-data
- GET /v1/engineering/projects/variants
- POST /v1/msrp/batches
- GET /v1/msrp/current-prices
- POST /v1/msrp/current-prices/materialize
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

## Production

- Tencent Cloud Ubuntu deployment: `Markdown_Readme/Fullstack/TENCENT_CLOUD_DEPLOY.md`
- Manual CI/CD flow: `Markdown_Readme/Fullstack/MANUAL_CICD.md`
- PostgreSQL local dev: `Markdown_Readme/Fullstack/BACKEND_POSTGRES_LOCAL_DEV_2026-04-10.md`
- systemd template: `03_Scripts/deploy/systemd/jato-fullstack-backend@.service`
- nginx template: `03_Scripts/deploy/nginx/jato_fullstack.conf.example`

## Country News Ops

- 默认问答路径只读数据库快照，不实时抓外网。
- 当用户对结果不满意时，前端会调用 `/v1/assistant/country/chat` 并带上 `refresh_news=true`，先在线抓新闻，再让 NVIDIA 基于新上下文重答。
- 模型分工：`RSS/Atom` 负责抓取，`Gemini` 负责新闻摘要/归因/标签，`NVIDIA` 负责最终国家市场分析回答。

### Scheduling

- 本地 macOS：`03_Scripts/install_local_country_news_sync_launchd.sh`
- 本地 runner：`03_Scripts/run_country_news_sync.sh`
- GitHub Actions：`.github/workflows/country-news-sync.yml`

### Cron Example

```bash
15 6 * * * cd /Users/you/Downloads/JATO_Analysis_System && /bin/bash 03_Scripts/run_country_news_sync.sh
```
