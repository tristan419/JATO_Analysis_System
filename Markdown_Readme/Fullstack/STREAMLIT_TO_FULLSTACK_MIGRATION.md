# Streamlit To Fullstack Migration Blueprint

## 1. Goal

Move from a single Streamlit analytics app to a reusable app platform:

- Backend: FastAPI (query, aggregation, CRUD, metadata)
- Frontend: React + Router (dashboard page + CRUD subpages)
- Data strategy: precomputed -> dynamic aggregate -> raw fallback

This migration is incremental, not a one-shot rewrite.

## 2. Current Capability Abstraction

## 2.1 Data and Query Layer

- Dataset discovery and version token
- Column projection and filter normalization
- Arrow pushdown filter and cached slices
- Distinct option lookup and row count lookup
- Precomputed summary loading
- Dynamic group aggregation fallback

Current source modules:

- `05_DashBoard/dashboard/data.py`
- `03_Scripts/precompute_summaries.py`
- `03_Scripts/run_data_refresh_job.py`

## 2.2 Orchestration Layer

- Data source resolution (partitioned preferred)
- Sidebar filter orchestration
- Route selection for analysis path
- Render strategy and lazy chart loading

Current source module:

- `05_DashBoard/dashboard/runner.py`

## 2.3 Presentation Layer

- KPI and dashboard cards
- Time selector + year/month trends
- Advanced chart suite
- Detail preview and export flow

Current source module:

- `05_DashBoard/dashboard/views.py`

## 2.4 State and Interaction

- Query-param hydration/sync
- Cascading filter pushdown
- Session-state based interactions

Current source module:

- `05_DashBoard/dashboard/filters.py`

## 3. New Target Architecture

## 3.1 Backend (FastAPI)

- API layer
  - `GET /healthz`
  - `GET /v1/metadata/columns`
  - `POST /v1/filters/options`
  - `POST /v1/analysis/query`
  - `CRUD /v1/crud/items/*`
- Service layer
  - query route selection
  - analysis response assembly
  - CRUD orchestration
- Infra layer
  - parquet access and pushdown
  - precomputed summary access
  - CRUD storage adapter

Implemented path:

- `06_AppPlatform/backend/app`

## 3.2 Frontend (React)

- Router pages
  - `/` dashboard API page
  - `/crud` CRUD subpage
- API client abstraction
- Extensible layout shell for future business modules

Implemented path:

- `06_AppPlatform/frontend/src`

## 4. Migration Mapping (Old -> New)

- `runner.py` orchestration -> backend `services/query_service.py` + frontend route actions
- `data.py` dataset operations -> backend `infra/parquet_repository.py`
- `filters.py` cascading options -> backend `/v1/filters/options` + frontend filter state module (next step)
- `views.py` chart rendering -> frontend chart modules (next steps, incremental by chart domain)
- precompute pipeline -> keep in current scripts, consumed by backend precomputed route

## 5. Stepwise Plan + TODO

- [x] P0 bootstrap fullstack folders and base configs
- [x] P0 implement backend core APIs for metadata/filter/analysis/crud
- [x] P0 implement frontend router shell and API wiring
- [x] P1 move sidebar filter UX from Streamlit to React state store (baseline cascading)
- [x] P1 move overview charts (year/month) to frontend chart components (baseline trend)
- [x] P1 add overview/detail/detail-csv endpoints and frontend integration
- [x] P1 add advanced chart grouped endpoint skeleton and dashboard integration
- [ ] P1 add auth-ready middleware and role placeholders
- [x] P1 add pagination/sorting contracts for CRUD list
- [ ] P2 split analysis domains into independent route modules
- [ ] P2 add request-level cache key policy and rate limits
- [ ] P2 add observability endpoints (hit ratio, route ratio, slow query top N)
- [ ] P3 retire Streamlit page domains only after parity checks pass

## 6. Acceptance Criteria

- API response route clearly reports one of:
  - `precomputed`
  - `dynamic-aggregate`
  - `raw`
- CRUD subpage supports create/list/delete via backend
- Dashboard page can execute analysis query through API
- Existing precompute scripts remain usable and compatible

## 7. Runbook

Backend:

```bash
cd 06_AppPlatform/backend
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

Frontend:

```bash
cd 06_AppPlatform/frontend
npm install
cp .env.example .env
npm run dev
```

## 8. Next Implementation Slice

- Build `filters` frontend module with cascading option loading
- Migrate year/month trend chart endpoints and frontend rendering
- Introduce persistent DB adapter for CRUD (replace JSON file adapter)
