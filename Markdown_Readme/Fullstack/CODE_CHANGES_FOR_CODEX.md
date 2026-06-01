# Uncommitted Code Changes — Codex Handover

> Generated 2026-06-01. All diffs are vs `HEAD` (commit `5903716`).
> 45 files changed, ~6259 insertions, ~710 deletions.

---

## Feature 1: `order_filler` Role & Permission System (NEW)

A restricted role for order-entry accounts. Current approved design:
- `order_filler` inherits the same viewer-visible frontend pages as `viewer`.
- `order_filler` can view/edit/import/export Order Genius for assigned countries only (primary + secondary).
- `order_filler` can view & edit their own profile display name only (NOT countries).

They CANNOT access editor/admin operations: Payment Terms, BOM Admin, Access Control, config import, matching review, monthly update admin workflow, or SKU editing endpoints.

### Backend

| File | Changes |
|------|---------|
| `app/core/security.py` | `ROLE_LEVEL["order_filler"] = 1`. New `require_roles(*allowed)` dependency (exact role match, not level-based). New `validate_country_access(session, username, role, country)` — raises 403 if order_filler accesses unassigned country. `get_current_user` + `get_optional_user` now try `_token_user()` first even when `AUTH_ENABLED=False`, falling back to admin. |
| `app/api/routes/auth.py` | Register & role-update accept `"order_filler"`. `update_my_profile` blocks country changes for order_filler (403). New `DELETE /auth/users/{id}` (hard delete, can't self-delete). New `PATCH /auth/users/{id}/password` (admin reset, min 6 chars). |
| `app/api/routes/order_genius.py` | `GET /payment-terms`, `GET /colour-surcharges` → `require_roles("viewer","editor","admin")` (excludes order_filler). `PATCH /quantity-cell`, `POST /import-quantities/preview`, `POST /import-quantities/{id}/apply` → `require_roles("editor","admin","order_filler")` (includes order_filler). `GET /matrix`, `GET /options`, `POST /export` → added `validate_country_access()`. `GET /countries` → filters to assigned countries for order_filler. |

### Frontend

| File | Changes |
|------|---------|
| `src/components/RequireRole.tsx` | **NEW**. React Router guard wrapper. Calls `isRouteAllowedForRole(path, role)` and redirects unauthorized routes to `/dashboard`. |
| `src/App.tsx` | `<Layout />` wrapped with `<RequireRole>`. New lazy import for `AdvancedAnalysisPage` at `/market/advanced-analysis` and `/market/transfer`. |
| `src/utils/pageNavigation.ts` | `MenuRole` extended: `"order_filler"`. Frontend `ROLE_LEVEL`: `viewer=0, order_filler=1, editor=2, admin=3`. Menu filtering and route guard share the same role policy. Legacy redirect paths are handled by `ROUTE_ROLE_OVERRIDES`. |
| `src/pages/AccessControlPage.tsx` | Massive rewrite — see Feature 2. |
| `src/pages/ProfilePage.tsx` | order_filler users see a read-only amber notice instead of country pickers. Can only edit display name. |
| `src/pages/OrderGeniusPage.tsx` | Fixed `userCountries` to include primary country (was missing — affected all non-admin). Admin-only buttons (BOM Admin, Payment Terms, Upload Material Master) already gated behind `isAdmin`. |

---

## Feature 2: Access Control Page — CRUD + Filters + Multi-Select

Complete overhaul of the admin user management page.

| Capability | Status |
|-----------|--------|
| Role filter dropdown | NEW — filter by viewer/order_filler/editor/admin |
| Username search | NEW — filters by display name or username |
| Create user | EXISTING — username + password + role |
| Edit user (modal) | NEW — "Edit" button opens modal with: role dropdown, primary country dropdown, secondary countries (multi-select with search), new password field, active/inactive toggle, delete user button |
| Delete user | NEW — double-confirm → `DELETE /auth/users/{id}` |
| Reset password | NEW — integrated into Edit modal |
| Secondary countries | NEW — `CountryMultiSelect` component: search box + scrollable checkbox list + "N selected" count. Used BOTH in inline table popover (click "DE, AT, CH" text) AND in Edit modal (embedded). Both save independently via API. |
| Country options | JATO_COUNTRIES ∪ payment-term countries (fetched from `/order-genius/countries`), deduplicated, sorted by country code. Display format: `Germany DE`. |
| Permissions matrix | NEW order_filler column added. |

Backend support: `DELETE /auth/users/{id}`, `PATCH /auth/users/{id}/password`.

---

## Feature 3: Order Genius / BOM Admin Improvements (Pre-existing)

These are larger changes that existed before this session but are still uncommitted.

### Backend — New Endpoints (9+)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/material-skus/{code}/colour-hex` | PATCH | Set custom hex color |
| `/material-skus/{code}/confirm-colour-code` | PATCH | Mark color code as confirmed |
| `/material-skus/{code}/colour-code` | PATCH | Update color code + regenerate material code |
| `/material-skus/{code}/material-code` | PATCH | Direct material code edit with `**` pattern replace |
| `/material-skus/{code}/colour-tier` | PATCH | Assign single/dual/special tier |
| `/material-skus/{code}/interior` | PATCH | Update interior fields (name, code, package, edition tag) |
| `/material-skus/{code}/lifecycle` | PATCH | Lifecycle status with optimistic locking |
| `/material-skus/{code}/fob` | PATCH | Update/create FOB per country (null clears it) |
| `/material-skus/{code}` | DELETE | Hard delete SKU + FOB records (admin only) |
| `/material-skus` | POST | Create new SKU manually |
| `/bom-admin` | GET | BOM with FOB per country for admin panel |
| `/material-skus-admin` | GET | All SKUs for admin listing |
| `/import-quantities/preview` | POST | Upload XLSX, parse, return diff preview |
| `/import-quantities/{id}/apply` | POST | Apply previewed import |

### Backend — DB Schema

- `MaterialSkuMaster`: new columns `colour_hex`, `colour_code_confirmed`, `colour_tier` (single/dual/special), `interior_colour_code`, `interior_package`, `edition_tag`. Default `colour_tier` changed from "normal" → "single".
- New tables: `ordering.fob_resolved_history`, `ordering.quantity_cell_history` (audit trails).
- Alembic migration: `20260527_0027_order_genius_history_tables.py`.

### Backend — Services

- `order_genius_service.py`: FOB-based color tier auto-classification (`_assign_fob_based_tiers`). Paint surcharge = FOB - base_FOB_of_BOM. Cross-BOM comparison for single-SKU BOMs. Powertrain canonicalization (`_extract_canonical_pt`). Power train detection priority: PHEV > MHEV > HEV > BEV > ICE.
- `order_genius_repository.py`: `update_sku_lifecycle()`, `delete_sku()`, `update_sku_fob_for_country()`, `list_bom_with_fob()`, `update_sku_material_code()`.
- **NEW** `services/backup_utils.py`: `backup_ordering_schema(trigger)` — pg_dump before publish.
- **NEW** `services/order_quantity_parser.py`: Parses exported XLSX back into import format.

### Frontend — Order Genius

- **NEW** `components/OrderGeniusGrid.tsx`: AG Grid component with sticky left columns (BOM, Interior, Single, Dual, Special at cumulative offsets), inline quantity editing, group headers, color hex swatches.
- `pages/OrderGeniusPage.tsx`: Switched to AG Grid. Country picker uses searchable dropdown. Multi-select countries with consolidated view. "Hide empty rows" toggle. Quantity import flow (drag-drop → preview → apply). Sticky columns with horizontal scroll for 23 country columns. Double-confirm delete. Debounced loading.
- `api/client.ts`: Added `delete()` method, `exportOrderGenius(opts)`, `previewOrderQuantityImport`, `applyOrderQuantityImport`, `getBomAdmin`, `updateSkuLifecycle`, `updateSkuFob`, `getSkuFobDetail`, `createMaterialSku`, `updateColourHex`, `confirmColourCode`, `updateColourCode`, `updateMaterialCode`, `updateColourTier`, `deleteMaterialSku`, `updateSkuInterior`.
- `types/orderGenius.ts`: `QuantityImportCell`, `QuantityImportRow`, `QuantityImportFobChange`, `QuantityImportNewRow`, `QuantityImportPreview`, `QuantityImportResult` types. `MaterialSkuMatrixRow` extended with interior fields.

### Backend — Other

- `powertrain_normalizer.py`: Pattern order re-arranged (specific before generic). `BEV` now canonical (was `EV`).
- `material_master_parser.py`: `&` → dual color. Edition tag parsing from `（Black edition）`. Auto-detect color tier. Interior color code extraction. Brand typo tolerance.
- `order_genius_export_service.py`: "总列表" aggregate sheet. Grouped by powertrain.

### Order Genius — Multi-Country Interior Binding Design

Current decision: keep the existing Order Genius table/consolidated view. Do not replace the main workflow with Country Matrix for now.

The immediate issue is incomplete `Interior` values from Material Master uploads. Interior should be bound to the BOM/material template containing `**`, not carried forward globally.

Implemented parser rule:
- Extract the current BOM template before deciding whether to skip a row.
- If a row has `Interior Color` and the current BOM template contains `**`, store `interior_by_bom_template[bom_template] = interior`.
- Later colour rows under the same `**` template inherit that interior when their own interior cell is blank.
- Switching to a new `**` template does not inherit the previous template's interior.
- Conflicting interiors for the same `**` template are reported as parser warnings.

Example:

```text
BOM template: T9000**EX001
Interior: Black/Black

Black (BK) row with blank interior -> T9000BKEX001 gets Black/Black

BOM template: T9000**EX002
Interior: blank

White (WT) row with blank interior -> T9000WTEX002 stays blank
```

Deferred idea: **方案 B / Country Matrix** can still be revisited later if multi-country comparison becomes too hard in the existing table.

If revisited, left side fixed identity columns:
- Brand
- Model
- Version
- Powertrain
- Exterior colour name/code
- Exterior package / edition, for example Black Warrior / 黑武士
- Interior colour name/code
- Interior package
- BOM template

If revisited, right side country matrix:
- One compact cell per country, for example `SE`, `NO`, `DK`, `FI`.
- Each country cell shows material code, FOB, selected month quantity, TTL, and status.
- Clicking a country cell opens a side drawer for that country/configuration with 12-month quantity editing, FOB, remark, material code, lifecycle, and validation details.
- Missing country/configuration combinations show an explicit empty state such as `Missing`, not a hidden row.
- Conflicting matches show a warning state rather than being merged.

Configuration matching must use a dedicated identity key, not material code alone:

```text
brand
+ modelName
+ version
+ powertrain
+ exteriorPackage / exteriorEdition
+ exteriorColorCode / exteriorColorName
+ interiorColourCode / interiorColorName
+ interiorPackage
+ bomTemplate
```

OMODA9 example that must stay distinct:
- `Exclusive + standard exterior package + black exterior + black/black interior`
- `Exclusive + standard exterior package + black exterior + black/red interior`
- `Exclusive + Black Warrior / 黑武士 exterior package + black exterior + black/black interior`

The important rule: **interior package and exterior package are separate identity dimensions**. Black Warrior / 黑武士 is an exterior package/edition layered on top of the Exclusive trim and black/black interior; it must not be merged with the normal black exterior + black/black interior configuration.

Possible future implementation shape:
- Backend/service exposes `configurationKey` and `configurationIdentity` in matrix rows.
- Frontend groups by `configurationKey`.
- Country-specific material code stays inside the country cell; it is not the grouping key.

---

## Feature 4: Advanced Analysis Page

A standalone page at `/market/advanced-analysis` for shift-share decomposition, seasonal adjustment, transfer matrix analysis.

- Backend: `app/services/advanced_analysis_service.py`, `app/api/routes/advanced_analysis.py`
- Frontend: `pages/AdvancedAnalysisPage.tsx`, `types/advancedAnalysis.ts`
- Time mode uses `sales_mode = month | ytd | rolling12`; frontend no longer sends a fake `time_range.mode`.
- Backend aggregates fact rows into month/YTD/rolling-12 windows, then reuses the same transfer mart calculation path.

---

## Feature 5: Hermes Governance & Feature Registry Updates

Multiple hermes YAML/JSONL files updated with feature registrations, governance gaps, evidence ledger entries, and dev events.

---

## Files NOT to commit

| Path | Reason |
|------|--------|
| `04_Processed_data/ops/coc_match/` | Runtime data |
| `04_Processed_data/ops/order_genius_uploads/` | Runtime upload sessions |
| `04_Processed_data/ops/order_quantity_imports/` | Runtime import sessions |
| `hermes/sessions/` | Runtime session data |
| `*.png` files | Screenshots |

---

## Key Design Decisions

1. **`order_filler` inherits viewer-visible frontend routes**. Backend `ROLE_LEVEL["order_filler"] = 1` so `require_min_role("viewer")` passes. Frontend role level is `viewer=0, order_filler=1, editor=2, admin=3`, so order_filler sees viewer pages plus order_filler-specific tools.

2. **Dev mode (`AUTH_ENABLED=False`) now tries token resolution first**. This allows testing order_filler in dev by logging in as an order_filler user. Falls back to admin if no valid token.

3. **Country isolation is backend-enforced**. `validate_country_access()` queries DB for user's `primary_country_code` + `secondary_country_codes`. Not JWT-authoritative (countries can change, JWT can't reflect that instantly).

4. **`require_roles` is for excluding roles from level-gated endpoints**. Used where a simple level check (`require_min_role`) would let order_filler through inappropriately, such as Payment Terms or SKU admin operations.

5. **Secondary countries use JATO ∪ payment-term country union** as options, displayed as `CountryName CODE`.

6. **Multi-country Order Genius must group by configuration identity, not material code**. Material code is country-specific display/edit data inside each country cell.

---

## Test Accounts

| Username | Password | Role | Primary | Secondary |
|----------|----------|------|---------|-----------|
| `filler_nordic` | `test123456` | order_filler | SE | DK, NO, FI |
| `filler_dach` | `test123456` | order_filler | DE | AT, CH, NL |
| `filler_south` | `test123456` | order_filler | ES | PT, FR, IT |
| `filler_east` | `test123456` | order_filler | CZ | SK, HU, PL |
