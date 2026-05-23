# Order Genius Implementation Roadmap (V1)

> Status: P0-P5 complete. V2 spec at [ORDER_GENIUS_V2_SPEC.md](./ORDER_GENIUS_V2_SPEC.md).
> Updated 2026-05-23.

## Architecture overview

```
Frontend (React): OrderGenius Page
  → /v1/order-genius/* endpoints
    → order_genius_service.py
      → order_genius_repository.py
        → PostgreSQL (schema: ordering)

Key tables:
  ordering.material_baseline_version → baseline/upload tracking
  ordering.material_sku_master       → SKU master (brand/model/version/color/material)
  ordering.country_sku_fob_resolved  → per-country per-SKU FOB pricing
  ordering.country_payment_term_master → payment term config per country
  ordering.payment_term_price_rule   → FOB adjustments by payment term
  ordering.brand_colour_surcharge_rule → colour surcharge by brand/color
  ordering.country_fob_source_mapping → FOB source country mapping
  ordering.order_quantity_cell       → monthly quantity cells
  ordering.material_sku_remark_history → remark change log

Key files:
  06_AppPlatform/backend/app/api/order_genius_schemas.py  → Pydantic schemas
  06_AppPlatform/backend/app/api/routes/order_genius.py    → API routes
  06_AppPlatform/backend/app/services/order_genius_service.py → business logic
  06_AppPlatform/backend/app/services/material_master_parser.py → Excel ingest
  06_AppPlatform/backend/app/infra/order_genius_repository.py   → DB queries
  06_AppPlatform/backend/app/db/models.py                   → SQLAlchemy models
  06_AppPlatform/frontend/src/pages/OrderGeniusPage.tsx     → frontend
```

---

## P0: DB constraint fix (DONE)

**Commit:** `20260522_0020_order_genius_fob_columns_nullable`

**What:** `base_fob_eur`, `payment_term_adjustment_eur`, `colour_surcharge_eur` on `ordering.country_sku_fob_resolved` changed from `NOT NULL` to nullable. Not all SKUs have explicit price breakdowns — a `final_fob_eur` may exist without intermediate columns. NULL means "not provided" (distinct from zero).

**Migration:** `alembic/versions/20260522_0020_order_genius_fob_columns_nullable.py`

**Model update:** `CountrySkuFobResolved` in `models.py` now includes the three columns as `Mapped[float | None]`.

**Apply on production:**
```bash
cd /opt/JATO_Analysis_System-main/06_AppPlatform/backend
python -m alembic upgrade head
```

---

## P1: Fix quantity cell re-edit (rowVersion)

**Problem:** After saving a quantity cell, the frontend doesn't have the new `row_version`. A second edit submits the old version → backend sees a concurrency conflict → 409 or silent failure.

**Root cause:** `OrderQuantityCell.row_version` is an integer optimistic lock. On save, the backend checks that the submitted version matches the DB version, then increments it. But the frontend never receives the new version back.

**Fix:**
1. `PUT /v1/order-genius/quantity` response must include the new `rowVersion` for each updated cell.
2. Frontend must update the cell's `rowVersion` in local state immediately after a successful save.
3. Alternatively: force a matrix reload after save and clear dirty flags.

**Files to change:**
- `order_genius.py` (route) — return updated rowVersion in response
- `OrderGeniusPage.tsx` — update local cell state with new rowVersion on save success

**Test:** Save a cell, then immediately edit it again and save. Should succeed without conflict.

---

## P2: Page cleanup

### 2a. Remove standalone Payment display
- Country dropdown already shows `Romania (LC90)` — no need for a separate `Payment: LC90` UI element.

### 2b. Year + Month filter
```text
[Year: 2026 ▼] [Month: All months ▼]
```
- `Year` dropdown: list of years with order data (e.g., 2025, 2026, 2027)
- `Month` dropdown:
  - `All months` — show 12-month matrix
  - `Jan`..`Dec` — single-month view with TTL/amount summaries
- API already supports `order_year` and `order_month` filters.

### 2c. Column visibility panel
Toggleable columns:
- Month columns: `Jan`, `Feb`, ..., `Dec`
- Amount column: `Monthly Amount` (= quantity × FOB)
- TTL Quantity
- TTL Amount (= TTL qty × FOB)
- FOB (per-cell base FOB)
- Material Code
- Remark

Implementation: dropdown/panel with checkboxes, persisted to localStorage.

### 2d. Amount = quantity × FOB
- `Monthly Amount` column: read-only, computed as `row.quantity × row.fob_eur`
- `TTL Amount`: sum of monthly amounts for the year

---

## P3: Material display & remark

### 3a. Material dropdown format
```text
Display label:  "ABC-12345（Blue metallic / LC90）"
Selected value: "ABC-12345"
```
- Active materials: black text
- Historical/archived: gray text (still selectable, still shows order history)

### 3b. Product-level remark
Two-tier remark system:
1. `remark` on `material_sku_master` — specific to a material code
2. `product_remark` — new field on a product grouping (brand + model + version + powertrain)

For now, `remark` field on `material_sku_master` is sufficient. Product-level remark can be stored in a separate table or as a convention on the first SKU of a product group.

---

## P4: Product block view

### Visual grouping
Products grouped by: `brand + model_name + version + powertrain`

Each product = one visual block containing all color variants.

### Color coding by powertrain
| Powertrain | Block color |
|-----------|-------------|
| BEV / EV | Green |
| HEV | Yellow |
| ICE | Dark gray |
| PHEV / SHS | Blue |

### Color column
- Show `exterior_color_name` + `exterior_color_code` (e.g., "Metallic Blue (A51)")
- If different colors under same product, show `**` as placeholder in the product header row

### Product remark
- New field or usage of existing `remark` on a "parent" SKU
- Displayed in the product block header

---

## P5: Material lifecycle

### Country-level material validity
New table `ordering.material_lifecycle`:
```sql
country_code       TEXT NOT NULL      -- e.g. 'RO'
material_code      TEXT NOT NULL      -- e.g. 'ABC-12345'
product_identity   TEXT NOT NULL      -- brand|model|version|powertrain
valid_from         DATE NOT NULL      -- e.g. 2025-01-01
valid_to           DATE               -- NULL = currently active
lifecycle_status   TEXT NOT NULL      -- 'active' | 'phased_out' | 'replaced'
replaced_by_code   TEXT               -- replacement material code
```

### Display rules
- Current active material: bold black
- Historical materials: gray, with label `Historical · 2025-01 to 2025-08`
- Historical order quantities visible (read-only)
- When switching materials: dropdown shows active first, then historical below a separator

### API endpoint
`GET /v1/order-genius/material-lifecycle?country=RO&brand=BYD&model=Seal&version=...`
Returns timeline of material codes with valid_from/valid_to.

---

## JATO monthly data note

Local `04_Processed_data` and server data are separate. Deploy scripts exclude `04_Processed_data` from tarball. Never mix local parquet with production. Use the web monthly-update flow for production data advancement.
