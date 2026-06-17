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
