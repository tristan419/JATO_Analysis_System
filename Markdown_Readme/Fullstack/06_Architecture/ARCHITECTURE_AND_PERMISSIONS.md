# JATO Analysis System — Architecture & Permissions

> Last updated: 2026-05-31

## 1. Role Hierarchy

Four-tier role system. Higher levels inherit all lower-level permissions.

| Role | Frontend Level | Backend Level | Description |
|------|---------------|---------------|-------------|
| `viewer` | 0 | 1 | Read-only access. Cannot see Order Genius. |
| `order_filler` | 1 | 1 | Between viewer and editor. Sees Order Genius but only assigned countries. Can edit quantities. |
| `editor` | 2 | 2 | Full access to all countries. Can upload BOM, manage SKUs, configure payment terms. |
| `admin` | 3 | 3 | Full access + user management, country assignments, access control. |

### Role assignment flow

```
Admin sets user role + secondaryCountries via /admin/access-control
  → User logs in via OAuth (Google/Feishu) or token
  → GET /v1/auth/me returns { role, secondaryCountries, primaryCountry }
  → Frontend AuthContext stores role + country list
  → RequireRole gates page access by role
  → OrderGeniusPage filters country list by secondaryCountries for non-admin users
```

### Backend endpoint guards

| Endpoint Group | Min Role | Notes |
|---------------|----------|-------|
| Matrix view (`GET /options`, `GET /matrix`) | `viewer` | order_filler can access (same level) |
| Quantity edit (`PATCH /quantity-cell`) | `order_filler` | Explicitly listed: editor, admin, order_filler |
| Material upload (`/material-master-uploads/*`) | `editor` | |
| SKU management (`/material-skus/*/lifecycle`, `/fob`, `/colour-*`) | `editor` | |
| BOM Admin (`GET /bom-admin`) | `editor` | |
| SKU delete (`DELETE /material-skus/*`) | `admin` | |
| Payment terms CRUD | `editor` | |
| Publish baseline | `admin` | |

### Country-level access control

- `admin` / `editor`: See ALL countries in Order Genius
- `order_filler`: Only sees countries listed in `user.secondaryCountries` (set by admin)
- `viewer`: Cannot access Order Genius at all

**Implementation**: `OrderGeniusPage.tsx` line 64-70:
```tsx
if (!isAdmin && userCountries.length > 0) {
  filtered = countries.filter((c) => userCountries.includes(c.countryCode));
}
```

### Permission files

| Layer | File | Key Code |
|-------|------|----------|
| Frontend hierarchy | `frontend/src/utils/pageNavigation.ts` | `ROLE_LEVEL`, `filterMenuByRole()` |
| Frontend route guard | `frontend/src/components/RequireRole.tsx` | `ORDER_FILLER_ROUTES` |
| Frontend auth context | `frontend/src/contexts/AuthContext.tsx` | `useAuth()`, `User` interface |
| Backend hierarchy | `backend/app/core/security.py` | `ROLE_LEVEL`, `require_min_role()`, `require_roles()` |
| Backend country guard | `backend/app/core/security.py` | `validate_country_access()` |

---

## 2. Page Architecture

### Route map

```
/login                          — LoginPage (public)
/                               — redirect to /dashboard
/dashboard                      — DashboardPage
/account/profile                — ProfilePage

─── Market ───
/market/overview                — MarketOverviewPage (→ MarketScanPage)
/market/segments                — MarketSegmentsPage (→ MarketScanPage)
/market/ranking/brand           — MarketBrandRankingPage (→ MarketScanPage)
/market/ranking/model           — MarketModelRankingPage (→ MarketScanPage)
/market/powertrain              — MarketPowertrainPage (→ MarketScanPage)
/market/transfer                — AdvancedAnalysisPage (Share Transfer)
/market/advanced-analysis       — AdvancedAnalysisPage (alias)

─── Product ───
/product/order-genius           — OrderGeniusPage (Order Matrix + BOM Admin)
/product/pricing                — PositioningPricingPage
/product/compare                — VersionComparisonPage
/product/current-msrp           — MsrpPage
/product/customer-insight       — CustomerInsightsPage
/product/coc-match              — CocMatchPage

─── Data ───
/data/order-genius              — OrderGeniusPage (alias)
/data/spec-detail               — SpecificationPage
/data/overview                  — DataManagementPage
/data/config-import             — EngineeringPage
/data/matching-review           — ReviewCasesPage
/data/jato-monthly-update        — JatoMonthlyUpdatePage

─── Admin ───
/admin/access-control           — AccessControlPage

─── Other ───
/copilot                        — CountryChatPage
/engineering-config             — EngineeringConfigPage
/market-scan                    — redirect → /market/overview
/*                              — NotFoundPage
```

### Nav visibility by role

| Section | Items | viewer | order_filler | editor | admin |
|---------|-------|--------|-------------|--------|-------|
| Dashboard | Dashboard, Spec Detail | ✅ | ✅ | ✅ | ✅ |
| Market Scan | Overview, Advanced Analysis | ✅ | ✅ | ✅ | ✅ |
| Product Deck | Pricing, Compare, Customer Insight, MSRP, Order Genius, COC Match | ✅ (no Order Genius) | ✅ (own countries) | ✅ (all) | ✅ (all) |
| Data | Overview, Config Import, Matching Review, JATO Update, Eng Config | ✅ (read-only) | ✅ (read-only) | ✅ | ✅ |
| Admin | Access Control | ❌ | ❌ | ❌ | ✅ |

### Page-to-component mapping

| Page | File | Lines | Key Components |
|------|------|-------|----------------|
| DashboardPage | `pages/DashboardPage.tsx` | ~3000 | Hero metrics, bubble sizing, trend charts |
| MarketScanPage | `pages/MarketScanPage.tsx` | ~3461 | Deck-based drilldown, Plotly charts, FloatingDeck |
| AdvancedAnalysisPage | `pages/AdvancedAnalysisPage.tsx` | ~650 | Waterfall, Butterfly, Sankey, Heatmap, Momentum, Stacked, Ledger |
| PositioningPricingPage | `pages/PositioningPricingPage.tsx` | ~1456 | Bubble chart, MSRP positioning |
| VersionComparisonPage | `pages/VersionComparisonPage.tsx` | ~1898 | Multi-version radar, spec comparison |
| OrderGeniusPage | `pages/OrderGeniusPage.tsx` | ~1852 | AG Grid matrix, BomAdminPanel, upload, export |
| MsrpPage | `pages/MsrpPage.tsx` | ~ | MSRP workflow, link management |
| EngineeringPage | `pages/EngineeringPage.tsx` | ~ | Config import, variant management |
| ReviewCasesPage | `pages/ReviewCasesPage.tsx` | ~ | MSRP review cases |
| JatoMonthlyUpdatePage | `pages/JatoMonthlyUpdatePage.tsx` | ~ | Monthly data lifecycle |
| CountryChatPage | `pages/CountryChatPage.tsx` | ~ | AI copilot chat |
| AccessControlPage | `pages/AccessControlPage.tsx` | ~ | User/role management |

---

## 3. Order Genius Architecture

### Data flow

```
Excel upload → Parser (material_master_parser.py)
  → Import Preview (preview_parsed_upload)
  → Publish (publish_baseline)
    → MaterialBaselineVersion (version tracking)
    → MaterialSkuMaster (SKU catalogue)
    → CountrySkuFobResolved (FOB per country per SKU)
    → OrderQuantityCell (monthly quantities)
```

### API endpoints (30+ under `/v1/order-genius/`)

| Group | Key Endpoints | Role |
|-------|--------------|------|
| Upload | `POST /material-master-uploads/initiate`, `PUT .../parts/{n}`, `POST .../complete`, `POST .../parse`, `GET .../preview`, `POST .../publish` | editor+ |
| Matrix | `GET /options`, `GET /matrix`, `PATCH /quantity-cell` | viewer+ |
| BOM Admin | `GET /bom-admin`, `PATCH .../lifecycle`, `PATCH .../fob`, `PATCH .../colour-hex`, `PATCH .../colour-code`, `PATCH .../colour-tier`, `PATCH .../interior`, `POST /material-skus`, `DELETE /material-skus/{code}` | editor+ |
| Payment Terms | `GET/POST/PATCH /payment-terms/*` | editor+ |
| Export | `POST /export` | viewer+ |
| Import | `POST /quantity-import/preview`, `POST /quantity-import/apply` | editor+ |

### DB schema (ordering schema)

```
MaterialBaselineVersion
  └─ MaterialSkuMaster (brand, model, version, bom_template, material_code,
       exterior_color_*, interior_color_*, colour_tier, edition_tag, lifecycle_status)
       ├─ CountrySkuFobResolved (country, uploaded_fob, final_fob, colour_surcharge)
       └─ OrderQuantityCell (country, year, month, quantity, fob_snapshot)

Reference:
  ├─ CountryPaymentTermMaster
  ├─ PaymentTermPriceRule
  └─ BrandColourSurchargeRule

Audit:
  ├─ FobResolvedHistory
  ├─ QuantityCellHistory
  └─ PaymentTermAuditLog
```

---

## 4. Advanced Analysis (Share Transfer) Architecture

### Page layout (8 charts in 5 rows)

```
Row 1: Waterfall (market decomposition) + Butterfly (winner/loser)
Row 2: Channel Volume (stacked) + Channel Share (indexed)
Row 3: Transfer Ledger (sortable table with sparklines + decomposition expand)
Row 4: Powertrain Stacked + Sankey (model transfer flows)
Row 5: Channel×Drive Heatmap + Share Momentum
Row 6: Powertrain×Origin Breakdown
```

### API

- `POST /advanced-analysis/transfer-mart` — Full shift-share decomposition
- `GET /advanced-analysis/segments` — Available segments per country
- `POST /advanced-analysis/shift-share` — Shift-share only
- `POST /advanced-analysis/kpi` — KPI table
- `POST /advanced-analysis/drilldown` — Nested drilldown

### Filter dimensions (FloatingDeck)

- Country (dropdown)
- Period A (month picker)
- Period B (compare mode toggle)
- Channel: Business / Private / All
- Drive: 4WD / 2WD / All
- Powertrain: multi-select chips (BEV, HEV, PHEV, ICE, MHEV, REEV, FCV)
- Segment: dropdown from API

### Backend data flow

```
Parquet → build_fact_sales_monthly() → normalized long table
  → scope_filters applied (segment, channel, drive, powertrain)
  → shift-share decomposition per model
  → TransferMartResponse:
    - scope_summary (market state, ΔM, YoY)
    - market_waterfall (decomposition items)
    - winners / losers (butterfly data)
    - models (ledger data)
    - channel_drive_heatmap
    - powertrain_origin_breakdown
    - momentum
    - channel_timeseries / powertrain_timeseries (stacked charts)
    - model_timeseries (sparklines)
```

---

## 5. Admin View: Architecture & Page Status

Admins can audit the system from the Access Control page (`/admin/access-control`), which shows:

- All users with their roles and assigned countries
- Role management (viewer / order_filler / editor / admin)
- Country assignment per user (primaryCountry, secondaryCountries)

To verify the nav/permission structure, an admin can:
1. Log in as different roles to confirm visibility
2. Check `pageNavigation.ts` for `ROLE_LEVEL` and `MEGA_MENU_ITEMS`
3. Check `RequireRole.tsx` for route-level gating
4. Check backend `security.py` for API-level guards

### How to add a new role

1. Add to `MenuRole` type in `frontend/src/utils/pageNavigation.ts`
2. Add to `ROLE_LEVEL` in both `pageNavigation.ts` and `backend/app/core/security.py`
3. Add route gating in `RequireRole.tsx` if needed
4. Update nav items with appropriate `minRole`
5. Update backend endpoint guards with `require_min_role()` or `require_roles()`
