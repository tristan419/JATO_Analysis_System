# Hermes Governance Layer Implementation Plan

> Created: 2026-05-14
> Status: Phase 0 — Repository Asset Map

## Current Phase: 0

Scanning the entire repository to produce `REPOSITORY_ASSET_MAP.md`.

### Scan Coverage

- `.github/workflows/` — CI/CD, scheduled fetch, deploy
- `airflow/dags/` — Airflow DAGs
- `03_Scripts/deploy/systemd/` — systemd services/timers
- `03_Scripts/` — all shell scripts, Python scripts
- `07_ScrapingToolkit/` — crawlers, fetchers, enrichers
- `06_AppPlatform/backend/` — API routes, services, config, prompts
- `06_AppPlatform/frontend/` — pages, routes, components, API client
- `Markdown_Readme/` — all docs
- `08_GitNexus/` — knowledge graph tooling
- `CLAUDE.md` — project instructions
- `docker-compose.yml` — services

### Hermes Principles (Phase 0)

- 只登记 — Register only
- 只评分 — Score only
- 只审计 — Audit only
- 只建议 — Recommend only
- 不自动改代码 — No auto code changes
- 不自动 merge — No auto merge
- 不自动 deploy — No auto deploy
- 不自动改生产环境 — No auto production changes
