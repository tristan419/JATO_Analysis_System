import { describe, expect, it } from "vitest";

import layoutSource from "../../components/Layout.tsx?raw";

describe("Layout auxiliary widgets", () => {
  it("keeps chat and presence widgets outside the first dashboard load window", () => {
    expect(layoutSource).toContain("const PresenceWidget = lazy(");
    expect(layoutSource).toContain("const CountryChatWidgetHost = lazy(");
    expect(layoutSource).toContain("AUXILIARY_WIDGET_DELAY_MS = 30_000");
    expect(layoutSource).toContain("AUXILIARY_WIDGET_IDLE_TIMEOUT_MS = 8_000");
    expect(layoutSource).not.toContain("AUXILIARY_WIDGET_DELAY_MS = 10_000");
  });
});
