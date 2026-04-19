# Backend PostgreSQL Local Dev

Status: Active

Date: 2026-04-18

## 1. Goal

This note explains how to run the backend in local PostgreSQL mode without affecting the existing Parquet analytics path.

## 2. Boundary

The current backend is hybrid:

1. JATO analytical data remains in Parquet.
2. Mutable business domains move into PostgreSQL.

That boundary is intentional.

1. Keep current dashboard query performance on the analytical side.
2. Give engineering config, MSRP, and review flows transactional storage.
3. Avoid forcing all analytical history into a relational database.

## 3. Backend Dependencies

The backend now relies on:

1. SQLAlchemy
2. Alembic
3. psycopg

These packages are already listed in `06_AppPlatform/backend/requirements.txt`.

## 4. Start PostgreSQL

Use either Docker or a local installation.

### 4.1 Docker Example

```bash
docker run --name jato-postgres \
  -e POSTGRES_DB=jato_app \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  -d postgres:16
```

### 4.2 Local Installation

If PostgreSQL is already installed locally, make sure these exist:

1. Database: `jato_app`
2. User: `postgres`
3. Password: `postgres`, or your own value if you update the connection string

## 5. Environment Variables

From the backend directory:

```bash
export APP_DATABASE_ENABLED=true
export APP_DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/jato_app
export APP_DATABASE_ECHO=false
export APP_ENGINEERING_IMPORT_ROOT=/absolute/path/to/engineering-xlsx-dir
```

`APP_ENGINEERING_IMPORT_ROOT` is the allowed root for engineering XLSX imports. Files outside this directory are rejected by the API.

## 6. Apply Migrations

From `06_AppPlatform/backend`:

```bash
alembic upgrade head
```

Current revisions now extend through:

1. `20260410_0001`: foundation schemas and first business tables
2. `20260410_0002`: engineering import core and MSRP observation core
3. `20260411_0003`: current prices and review loop tables
4. `20260411_0004` ~ `20260412_0007`: FX normalization, structured variant fields, override feedback, price history refinements
5. `20260415_0008`: country news cache
6. `20260417_0009` ~ `20260417_0011`: EVKX variant business key, MSRP source tier + JATO links, engineering market overrides

## 7. Start Backend

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```

## 8. First Verification Endpoints

### 8.1 Database Health

```text
GET /v1/platform/db/health
```

Use it to verify:

1. Whether the database path is enabled
2. Whether FastAPI can connect to PostgreSQL

### 8.2 Engineering Projects

```text
GET /v1/engineering/projects
POST /v1/engineering/projects
POST /v1/engineering/projects/{project_id}/imports
POST /v1/engineering/projects/{project_id}/normalize
PATCH /v1/engineering/projects/{project_id}
DELETE /v1/engineering/projects/{project_id}
```

Use it to manage the project container for engineering config imports.

The import endpoint accepts a source XLSX path, sheet name, replace mode, schema version, and optional valid-from date. The backend reads the file, creates `ops.import_batches`, creates `engineering.config_import_batches`, and materializes normalized rows into `engineering.config_variants`.

Import batches are now persisted even when the import fails. A failed run updates both batch tables to `failed` and stores a structured summary with failure detail, warning count, and resolved column mapping.

### 8.3 Engineering Imports and Variants

```text
GET /v1/engineering/projects/imports
GET /v1/engineering/projects/imports/{config_import_batch_id}
GET /v1/engineering/projects/imports/{config_import_batch_id}/page-data
GET /v1/engineering/projects/variants
GET /v1/engineering/projects/base-variants
GET /v1/engineering/projects/market-variants
GET /v1/engineering/projects/feature-overrides
```

Use it to inspect imported engineering batches, persisted failure details, and materialized variant rows.

`GET /v1/engineering/projects/imports/{config_import_batch_id}` returns the config import batch, the linked `ops.import_batches` row, and a sample of variants written by that batch.

`GET /v1/engineering/projects/imports/{config_import_batch_id}/page-data` reshapes the same batch into a frontend-friendly admin page payload with header metadata, summary cards, warning panel, mapping rows, full summary, and sample variants.

`POST /v1/engineering/projects/{project_id}/normalize` runs the engineering normalization pipeline and returns both the import summary and a `normalization` summary payload for the resulting base variants / market variants / feature overrides.

### 8.4 Data Management and Local Airflow Controls

```text
GET /v1/data-management/overview
GET /v1/data-management/airflow/status
POST /v1/data-management/airflow/start
POST /v1/data-management/airflow/stop
```

Use it to inspect local data pipelines and, when Docker Compose is available, control the **local-only Airflow** helper stack from the app. This is a local orchestration aid, not a required production dependency.

### 8.5 MSRP Sources, Batches, Observations, and Links

```text
POST /v1/msrp/batches
GET /v1/msrp/current-prices
POST /v1/msrp/current-prices/materialize
GET /v1/msrp/links
POST /v1/msrp/links
PATCH /v1/msrp/links/{link_id}
DELETE /v1/msrp/links/{link_id}
GET /v1/msrp/sources
POST /v1/msrp/sources
PATCH /v1/msrp/sources/{source_id}
GET /v1/msrp/sources/batches
GET /v1/msrp/sources/observations
```

Use it to inspect source registration, ingest scrape batches, materialize current official prices, manage `JatoMsrpLink`, and inspect structured observation records.

`POST /v1/msrp/batches` is the first write entry for scraper output. It creates a scrape batch, writes observations, opens review cases for unresolved `review_required`, and materializes `msrp.current_prices` after the canonical mapping resolver runs (`valid MatchOverride > active JatoMsrpLink > raw observation`).

`POST /v1/msrp/current-prices/materialize` rebuilds current prices from eligible observations when you need a controlled backfill or recompute.

### 8.6 Review Overrides

```text
GET /v1/review/overrides
POST /v1/review/overrides
PATCH /v1/review/overrides/{override_id}
DELETE /v1/review/overrides/{override_id}
```

Use it to inspect and maintain manual match overrides.

### 8.7 Review Cases and Decisions

```text
GET /v1/review/cases
GET /v1/review/cases/{review_case_id}
POST /v1/review/cases/{review_case_id}/decisions
```

Use it to inspect review-required MSRP matches, view case detail plus decision history, and close the loop with approve/reject/remap actions.

Approve or remap decisions update the observation, mark the case approved, materialize `msrp.current_prices`, upsert an active `JatoMsrpLink`, and can optionally persist a dated `review.match_overrides` rule.

## 9. What This Skeleton Covers

The current backend skeleton now covers:

1. PostgreSQL runtime wiring
2. Alembic migrations
3. Engineering project management
4. Engineering XLSX import execution path
5. Frontend-friendly engineering import batch page-data payload
6. MSRP source management
7. MSRP scrape batch ingestion
8. Current official price materialization
9. Review case and review decision loop
10. Review override management
11. JATO-to-official link management
12. Data management overview + local Airflow helper controls
13. Read-only engineering import and variant queries
14. Read-only MSRP batch and observation queries

## 10. What Still Comes Next

Not implemented yet:

1. Automated MSRP scraper jobs
2. Alerts and price-sales effectiveness

## 11. Frontend Boundary

This step only builds the backend foundation.

That is deliberate.

1. The data model and query surface should stabilize first.
2. A frontend added too early would mostly be placeholder scaffolding.
3. After variants, review, and serving tables settle, the frontend can be designed in the BMW-style direction you asked for.
