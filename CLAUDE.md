# CLAUDE.md

## Must-read docs first

Before editing code, read these first:

1. `Markdown_Readme/Fullstack/ROADMAP.md`
2. `Markdown_Readme/Fullstack/WORKFLOWS/README.md`

Then read task-specific docs:

- Data / ETL: `Markdown_Readme/Fullstack/02_DataETL/ETL.md`
- MSRP: `Markdown_Readme/Fullstack/MSRP/README.md`
- Backend: `06_AppPlatform/backend/README.md`
- Scraping / VOC / news: `07_ScrapingToolkit/README.md`
- UI / frontend: `Markdown_Readme/UI/UI_SPECIFICATION_V1.md`

`Markdown_Readme/Streamlit/` is historical archive only. Do not treat Streamlit as the current mainline unless explicitly requested.

## Current architecture

This project is now a Fullstack JATO automotive analysis system.

Mainline:

- FastAPI backend
- React + TypeScript + Vite frontend
- PostgreSQL business truth layer
- JATO Parquet / partitioned dataset layer
- Scraping Toolkit for MSRP, news, VOC, policy, incentives, and specs
- Airflow / Docker for local orchestration
- Streamlit is legacy

## Running services rule

Frontend, backend, PostgreSQL, Docker services, Airflow, and local services may already be running in VS Code.

Default rule:

- Do not start, stop, restart, or recreate frontend, backend, PostgreSQL, Docker, Airflow, or Streamlit services unless explicitly asked.
- Do not run `npm run dev`, `uvicorn`, `docker compose up`, `docker compose down`, `docker restart`, `streamlit run`, `alembic upgrade`, or `alembic downgrade` unless explicitly approved.
- Prefer using the existing running services.
- If validation needs a service, first check whether the existing port is alive.
- If a service appears unavailable, report it first instead of restarting it.

## Repository map

- `01_RAW_DATA/`: raw JATO input data. Do not open unless explicitly needed.
- `02_Config_MetaData/`: configuration, mappings, and business rules.
- `03_Scripts/`: ETL, ingestion, sync, deployment, and utility scripts.
- `04_Processed_data/`: processed Parquet, partitioned data, reports, news/VOC artifacts. Do not scan large files by default.
- `05_DashBoard/`: legacy Streamlit dashboard.
- `06_AppPlatform/backend/`: FastAPI backend.
- `06_AppPlatform/frontend/`: React + TypeScript + Vite frontend.
- `07_ScrapingToolkit/`: MSRP / news / VOC / policy / incentive / spec scraping toolkit.
- `Markdown_Readme/Fullstack/`: active architecture and workflow docs.
- `Markdown_Readme/UI/`: active UI docs.
- `Markdown_Readme/Streamlit/`: historical archive only.
- `airflow/`: local orchestration.

## Coding rules

- Read docs before code.
- Make small, targeted changes.
- Do not scan the whole repository unless necessary.
- Do not open raw data or processed data unless needed.
- Do not rename columns, API fields, business keys, or folders without checking downstream usage.
- Do not expose API keys, `.env` values, raw data, local paths, or generated cache files.
- Prefer existing project patterns over new dependencies.

## Hermes DevSync rule (MANDATORY after every implementation)

After EVERY non-trivial implementation, bug fix, refactor, or test change, Claude Code MUST:

1. **Write a dev event** to `hermes/dev_events/dev_events.jsonl` following the contract in
   `Markdown_Readme/Hermes/HERMES_CLAUDE_CODE_DEVSYNC_CONTRACT.md`.

   Required fields: `eventId`, `eventType`, `source`, `title`, `summary`,
   `linkedFeatureIds`, `changedFiles`, `tests`, `createdAt`.

2. **Trigger DevSync** by calling `POST /v1/hermes/dev/sync` (or via the
   Dev tab → Sync Now button in the UI if the backend is running).

3. **Verify** the feature appears in Hermes UI → Dev tab with correct status,
   docs, and evidence.

4. **Report Hermes DevSync status** in the final summary:
   - Dev event written: yes/no
   - Feature updated: yes/no
   - Markdown generated: yes/no
   - Evidence written: yes/no
   - Gaps created: yes/no
   - Tests run: backend/frontend results

This is NOT optional. Every implementation session must close the DevSync loop.
Contract reference: `Markdown_Readme/Hermes/HERMES_CLAUDE_CODE_DEVSYNC_CONTRACT.md`

### Automated enforcement layers
1. **pre-commit hook**: warns if code changed but dev_events.jsonl not updated
2. **post-commit hook**: auto-generates commit-level dev event from git diff
3. **GitHub Actions (hermes-devsync.yml)**: on push → scans diff → generates dev event → commits it back → calls remote DevSync API

## Validation rules

Use targeted validation only.

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
