# Reusable Frontend Components

Use this file as the local index for components that are intended to be reused across pages. Prefer importing from the component barrel file when one exists.

## Deck Controls

Location: `src/components/deckControls`

Import:

```tsx
import {
  DebouncedNumberInput,
  DeckControlTabs,
  DeckExportDrawer,
  DeckFloatingDrawer,
  FlipToolCard,
} from "../components/deckControls";
```

### `DeckFloatingDrawer`

Use for a right-side floating control drawer with a toggle button, panel header, body, and optional footer.

Good fit:

- Filter/action panels on Product Deck pages.
- Control panels that should float over a dense table or chart.

Avoid for:

- Inline page toolbars.
- Full-width admin tool sections.

### `DeckControlTabs`

Use for tab-like controls inside a deck drawer or panel.

Good fit:

- Switching between filter sections, import/export sections, or batch tools.

### `DeckExportDrawer`

Use for export workflows that need the standard deck drawer treatment.

Good fit:

- Export settings and download actions.

### `DebouncedNumberInput`

Use for numeric inputs where edits should wait briefly before updating parent state.

Good fit:

- Chart/table numeric controls.
- Inputs that would otherwise trigger expensive recalculation on every keypress.

### `FlipToolCard`

Use for a two-sided tool card with a front face and a back face using the shared flip animation and accessibility handling.

Good fit:

- Compact summary card that flips into admin/configuration tools.
- Inline tool panels that should reuse the same flip behavior as deck UI.

Avoid for:

- Modal dialogs.
- Multi-step workflows where browser history or route state matters.

Notes:

- The component owns the flip container, face visibility, `aria-hidden`, and `inert` states.
- The caller owns the front/back content and card sizing.
- Use neutral CSS classes `deck-flip-card`, `deck-flip-inner`, `deck-flip-face`, `deck-flip-front`, and `deck-flip-back`.

## Finance / CBU

Location: `src/components/finance`

Import:

```tsx
import { CountryCbuPastePanel, MaterialFinanceMatrix, MaterialFinanceWorkbench } from "../components/finance";
```

### `MaterialFinanceMatrix`

Use for editable country material finance rows: BOM FOB reference, maintained FOB, unit margin, margin rate, unit profit, profit rate, FOB delta, margin delta, note, source metadata, and row save.

Good fit:

- BOM Admin single-country finance/CBU quick cards.
- Model-level country finance drawers.
- Future upload digest preview/apply screens for finance rows.

Notes:

- The component owns input drafts, signed EUR delta inputs, and percent display conversion.
- The caller owns data loading, row saving, and whether the rows represent one material, one BOM template, one model, or a future upload digest.
- `vehicleMarginRate` and `vehicleProfitRate` are stored as decimal values (`0.1682`) and displayed as percentages (`16.82%`).

### `MaterialFinanceWorkbench`

Use for a country-switchable CBU/margin workbench that wraps `MaterialFinanceMatrix`.

Good fit:

- Standalone CBU detail pages.
- BOM Admin country-header CBU flip cards.

Notes:

- `NL` renders the dedicated price CBU / margin matrix.
- Non-NL countries render the Excel paste flow through `CountryCbuPastePanel`.

### `CountryCbuPastePanel`

Use for non-NL country CBU rows that should be pasted from Excel at BOM-template grain.

Good fit:

- Country CBU detail pages where the source data is a copied Excel table.
- BOM Admin country-header CBU cards for countries that do not use the NL price CBU format.

Notes:

- Accepted grain is the BOM template material code containing `**`.
- Default paste order is `Material Code`, `FOB`, `Retail`, `Wholesale`, `Dealer`, `Cost`, `Note`.
- The component reuses `UploadDigestPanel` for parsed row counts, errors, and apply actions.

## Upload Digest

Location: `src/components/UploadDigestPanel.tsx`

Import:

```tsx
import { UploadDigestPanel } from "../components";
```

### `UploadDigestPanel`

Use for upload parsing summaries with metrics, warnings, errors, preview children, and apply/cancel footer actions.

Good fit:

- Material Master upload preview.
- Quantity import preview.
- Future CBU Excel paste/upload digest before applying finance rows.

## Vehicle Allocation

Location: `src/components/vehicleAllocation`

Import:

```tsx
import {
  VehicleImportDigestPanel,
  VehicleStatusBoard,
  VinPasteDigestPanel,
} from "../components/vehicleAllocation";
```

### `VehicleStatusBoard`

Use for PI vehicle status summaries that consume a backend status-flow config.

Good fit:

- PI allocation tool drawers.
- Country/account-specific vehicle status boards where the step order, labels, colours, icons, terminal state, and allowed transitions come from configuration.

Notes:

- Do not hardcode country status flows in page JSX.
- Pass `VehicleStatusFlowConfig` from `/order-genius/vehicle-allocation/status-flow`.
- Empty configured statuses still render so each country/account flow is visible even when count is zero.

### `VinPasteDigestPanel`

Use for month/material/PI-scope VIN paste workflows.

Good fit:

- Assigning pasted VINs to empty vehicle slots in PI line and car-code order.
- Previewing pasted / matched / ready / duplicate / invalid / overflow counts before apply.

Notes:

- The panel reuses `UploadDigestPanel` and `LoadingActionButton`.
- It does not parse Excel/image files directly; it consumes text pasted by the user.

### `VehicleImportDigestPanel`

Use for vehicle allocation import preview/apply workflows.

Good fit:

- Excel workbook import preview.
- Parsed image/OCR result import where the image parser already produced structured JSON rows.

Notes:

- Excel files use `/order-genius/vehicle-allocation/import/preview`.
- Parsed image rows use `/order-genius/vehicle-allocation/import/preview-rows` with `{ rows: [...] }`.
- Raw image files are not parsed by this component; convert the image to rows first, then use the same digest/apply flow.
