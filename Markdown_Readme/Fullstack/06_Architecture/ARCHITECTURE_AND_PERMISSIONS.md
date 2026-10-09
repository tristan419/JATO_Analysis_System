# JATO Analysis System — Architecture & Permissions

> Last updated: 2026-10-09. Ordering 现行契约见 [Permission Management](../../features/PERMISSION_MANAGEMENT.md)。部署／验收状态见当前批次 Progress；后文其他产品架构保留基线事实，不代表全系统已完成品牌隔离。

## 1. Role Hierarchy and Ordering Scope

复用 viewer／order_filler／editor／admin 及现有 developer（Admin 等级），不新增角色平台。后端 Viewer 与 Filler 等级相同，订单写接口必须明确列出 Filler／Editor／Admin，不能仅凭等级放行 Viewer。

- Admin 及以上全国家、全品牌可见，不受个人偏好限制。
- Filler：Admin 分配主国家＋附加国家、品牌；不可自改。
- Editor：可自改本人国家，品牌仅 Admin 分配；共享 BOM 维护按获配品牌，国家数量／价格／车辆按国家×品牌。
- Viewer：可自改国家、无需品牌，不扩大现有页面或只读能力。
- 新 Filler／Editor 品牌默认空；现有全部 Filler 一次性补 OMODA＋JAECOO，现有 Editor 不补。迁移不改国家、角色或历史业务记录。
- 无品牌提示只在选品／分车页面需要时出现；复用权限申请表和审批工作流，批准分配品牌，不提升角色。

### Existing flow, not a new permission platform

Admin 在 Access Control 分配，/auth/me 返回当前角色／国家／品牌；AuthContext 和既有路由／页面控制入口。后端认证读取当前有效账号；现有 SQL 查询先按授权过滤再计数／分页，事务写前检查全部目标。旧 token 与请求筛选不能扩权。

混合 PI 的 line／allocation／车辆与数量／导出使用订单保存品牌及实际国家；Editor 整 PI 删除须所有目标有权。共享 HEX 同步历史 PI 展示，订单物料号／BOM／描述／成交价不重写。CBU 与独立 COC 库管理不在本轮，PI 发起 COC 下载仍限制授权 VIN。

权限文件：backend/app/core/security.py、services/auth_service.py、api/routes/auth.py、ordering route/service/repository；frontend AuthContext、pageNavigation、AccessControl、Profile 和选品／分车页面。完整角色矩阵与回归以 Permission Management 为准。

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
