# Permission Management

## Status
implemented — order_filler role added, country-level isolation enforced

## Category
governance · source: claude-code

## Summary
Added `order_filler` role (level 1) for restricted order-entry accounts. New `require_roles(*allowed)` dependency for explicit role whitelisting. New `validate_country_access()` for backend-enforced country isolation. Frontend route guard (`RequireRole`) limits order_filler to 12 allowed paths. Access Control page enhanced with role filter, search, secondary country multi-select, delete user, password reset.

## Implementation

- `app/core/security.py` — `require_roles()`, `validate_country_access()`, dev-mode token resolution
- `app/api/routes/auth.py` — role validation, country-change block for order_filler, `DELETE /users/{id}`, `PATCH /users/{id}/password`
- `app/api/routes/order_genius.py` — country enforcement on matrix/options/export, role gating on payment-terms/bom-admin
- `frontend/src/components/RequireRole.tsx` — client-side route guard (12 allowed paths)
- `frontend/src/utils/pageNavigation.ts` — menu filtering with deny-role pattern
- `frontend/src/pages/AccessControlPage.tsx` — full CRUD, `CountryMultiSelect`, Edit modal, role filter, search
- `frontend/src/pages/ProfilePage.tsx` — order_filler country edit blocked

## Linked Dev Events

- (pending hermes sync)

## Test Accounts

| Username | Role | Primary | Secondary |
|----------|------|---------|-----------|
| filler_nordic | order_filler | SE | DK, NO, FI |
| filler_dach | order_filler | DE | AT, CH, NL |
| filler_south | order_filler | ES | PT, FR, IT |
| filler_east | order_filler | CZ | SK, HU, PL |

*Updated: 2026-06-01*
