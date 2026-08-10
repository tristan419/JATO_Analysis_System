import { describe, expect, it } from "vitest";

import { buildBomEditScopeKey, resolveBomAdminColourTier } from "../../utils/orderGeniusBomAdmin";

describe("Order Genius BOM Admin edit scope", () => {
  it("keeps the same BOM template independent across product versions", () => {
    const first = buildBomEditScopeKey("JAECOO|JAECOO 5|PHEV", "Luxury", "T7000Z5**MY0026");
    const second = buildBomEditScopeKey("JAECOO|JAECOO 5|PHEV", "Exclusive", "T7000Z5**MY0026");

    expect(first).not.toBe(second);
  });

  it("returns a stable key for the same rendered BOM row", () => {
    expect(buildBomEditScopeKey("JAECOO|JAECOO 5|PHEV", "Luxury", "T7000Z5**MY0026"))
      .toBe(buildBomEditScopeKey("JAECOO|JAECOO 5|PHEV", "Luxury", "T7000Z5**MY0026"));
  });
});

describe("Order Genius BOM Admin colour tier", () => {
  it("keeps an explicit Single tier even when the swatch has two colours", () => {
    expect(resolveBomAdminColourTier({ colourTier: "single", colourType: "dual" })).toBe("single");
  });

  it("does not infer tier from a legacy colour name", () => {
    expect(resolveBomAdminColourTier({ colourTier: null, colourType: null, colour: "Black & White" })).toBe("single");
    expect(resolveBomAdminColourTier({ colourTier: null, colourType: "dual" })).toBe("dual");
  });
});
