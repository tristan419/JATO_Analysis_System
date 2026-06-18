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

## Upload Primitives

Location: `src/components/upload`

Import:

```tsx
import { FileDropzone } from "../components";
```

### `FileDropzone`

Use for file upload controls that need drag/drop, click-to-select, selected-file display, and a clear action.

Good fit:

- Excel/PDF/ZIP/RAR upload panels.
- Floating deck controls where two related files are selected before starting a job.
- Future upload workflows such as JATO monthly update, CBU import, or Engineering Config source files.

Notes:

- The component owns drag state and the hidden file input.
- The caller owns accepted extensions, selected file state, upload progress, validation, and submit behavior.
- Use separate `FileDropzone` state per workflow tab so switching tabs cannot submit the wrong files.

## Workbench Components

Location: `src/components/workbench`

Import:

```tsx
import {
  SheetGroupedPreview,
  StatusMetricCard,
  type SheetGroupedPreviewColumn,
  type SheetGroupedPreviewGroup,
} from "../components";
```

### `StatusMetricCard`

Use for compact workbench metrics that may also act as filters.

Good fit:

- Job status summaries.
- Preview filters such as total / filled / missing / ambiguous / skipped.
- Dashboard cards where `tone`, `active`, and `onClick` cover the interaction.

Notes:

- The component owns visual tone and active styling.
- The caller owns metric value calculation and filter state.

### `SheetGroupedPreview`

Use for preview tables grouped by Sheet, source section, country, or another stable group key.

Good fit:

- COC fill preview grouped by Excel sheet.
- Engineering Config source preview grouped by sheet.
- Excel digest/import preview where rows need expand/collapse and per-group metrics.

Notes:

- The component owns the panel, blue disclosure triangle, group header, table skeleton, and empty/truncated state.
- The caller owns columns, filtering, expanded key state, and row rendering.
- Business-specific actions belong in `renderRow`; do not push COC-only candidate selection or WVTA rules into the shared component.

## Common Utilities

Location: `src/components/common`, `src/utils`

Import:

```tsx
import { EmptyState } from "../components";
import { downloadBlob } from "../utils/download";
import { formatDateTime } from "../utils/timeFormatting";
```

### `EmptyState`

Use for small table/panel empty states with consistent copy spacing.

### `downloadBlob`

Use when an API returns a `Blob` that should be downloaded with a known filename.

### `formatDateTime`

Use for job history and audit timestamps that should render consistently in the frontend locale.

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

## Backend Workbook Scanner

Location: `06_AppPlatform/backend/app/services/workbook_table_scanner.py`

Import:

```python
from app.services.workbook_table_scanner import (
    MaterialGroupRow,
    extract_material_rows,
    target_columns_for_sheet,
)
```

### `workbook_table_scanner`

Use for backend jobs that read an Excel workbook, scan multiple sheets, infer a header/material column, create target columns, and keep source row/cell references for preview or writeback.

Good fit:

- COC fill workbook parsing.
- Future Engineering Config source digest jobs.
- Excel workflows that follow `upload -> preview -> confirm -> writeback`.

Notes:

- The scanner uses `openpyxl` so workbook sheets, styles, widths, and existing values survive writeback.
- Domain services still own matching, validation, persistence, and output naming.
- PDF parsing is intentionally not part of this scanner; keep PDF-specific extraction in the owning service until formats stabilize.
