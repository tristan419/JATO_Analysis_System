// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { materialAssets } from "../../api/materialAssets";
import { MaterialColourPreview } from "../../components/orderGenius/MaterialColourPreview";
import { MaterialComparisonPanel } from "../../components/orderGenius/MaterialConfigurationComparison";
import type { AssetCatalog, AssetMaterial, ConfigCell, ConfigComparison } from "../../types/materialAssets";

const base = (key: string): AssetMaterial => ({ key, brand: "JAECOO", modelName: key === "B" ? "J8" : "J7", bomTemplate: `${key}**`, version: "FWD", powertrain: "PHEV", effectiveFrom: "2026-10-01", effectiveTo: null, historical: false,
  colours: [{ key: "code:CL", code: "CL", name: "Black", hex: "#000000" }], interiors: [{ key: "code:BK", code: "BK", name: "Black", hex: "#000000" }], notes: [], issues: [] });
const cell = (value: string, source = "Z"): ConfigCell => ({ category: "Safety", feature_name: "Airbags", raw_value: value, normalized_value: value, availability: "available", source_row: 9, source_column: source });
const comparison: ConfigComparison = {
  materials: ["B", "A"].map(key => ({ key, bomTemplate: `${key}**`, modelName: base(key).modelName, versionId: `uuid-${key}`, version: 1 })),
  rows: [{ category: "Safety", featureName: "Same", values: [cell("6"), cell("6")] }, { category: "Safety", featureName: "Airbags", values: [cell("8"), cell("6", "AA")] }, { category: "Other", featureName: "Missing vs unavailable", values: [null, { ...cell("Not equipped"), availability: "unavailable" }] }],
};
function catalog(): AssetCatalog {
  return { today: "2026-10-31", materials: [base("A"), base("B")],
    bindings: [{ id: "link", materialKey: "A", kind: "exterior", colourKey: "code:CL", groupId: "photos", versionId: null, rowVersion: 1 }],
    groups: [{ id: "photos", brand: "JAECOO", modelName: "J7", kind: "exterior", colourKey: "code:CL", name: "Black views", rowVersion: 1, references: ["A"], files: ["front", "rear"].map(id => ({ id, filename: `${id}.png`, contentType: "image/png", sha256: id })) }],
    versions: ["A", "B"].map(key => ({ id: `uuid-${key}`, brand: "JAECOO", modelName: base(key).modelName, bomTemplate: `${key}**`, number: 1, parentId: null, createdBy: "editor", createdAt: "2026-10-01", source: "matrix.xlsx", references: [key], files: [{ id: `source-${key}`, filename: "matrix.xlsx", contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", sha256: key }] })),
  };
}
beforeEach(() => {
  localStorage.clear();
  vi.stubGlobal("URL", class extends URL { static createObjectURL = vi.fn(() => "blob:photo"); static revokeObjectURL = vi.fn(); });
  vi.spyOn(materialAssets, "photo").mockResolvedValue(new Blob(["image"], { type: "image/png" }));
});
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); });

describe("selection comparison", () => {
  it("preserves the user's cross-model order, defaults to differences and offers original sources", async () => {
    vi.spyOn(materialAssets, "catalog").mockResolvedValue(catalog());
    const compare = vi.spyOn(materialAssets, "compare").mockResolvedValue(comparison);
    const download = vi.spyOn(materialAssets, "download").mockResolvedValue(new Blob(["zip"]));
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
    const close = vi.fn();
    render(<MaterialComparisonPanel materials={[base("B"), base("A")]} onClose={close} />);
    await screen.findByText("Airbags");
    expect(compare).toHaveBeenCalledWith(["B", "A"], expect.any(AbortSignal));
    expect(Array.from(document.querySelectorAll(".mw-comparison thead .mw-compare-label")).map(item => item.textContent)).toEqual(["1B**", "2A**"]);
    expect(screen.queryByText("Same")).toBeNull(); expect(screen.getByText("Missing vs unavailable")).toBeTruthy();
    expect(screen.getByText("Not equipped")).toBeTruthy(); expect(screen.getByTitle(/uuid-B\sZ9/)).toBeTruthy();
    fireEvent.click(screen.getByLabelText("Differences only")); expect(screen.getByText("Same")).toBeTruthy();
    fireEvent.click(screen.getAllByRole("button", { name: "Source ↗" })[0]);
    await waitFor(() => expect(download).toHaveBeenCalledWith(["source-B"]));
    fireEvent.click(screen.getByRole("button", { name: "Close" })); expect(close).toHaveBeenCalledOnce();
  });
  it("does not silently drop an unavailable selected material or compare a partial set", async () => {
    vi.spyOn(materialAssets, "catalog").mockResolvedValue(catalog());
    const compare = vi.spyOn(materialAssets, "compare");
    render(<MaterialComparisonPanel materials={[base("A"), base("MISSING")]} onClose={() => undefined} />);
    await screen.findByRole("alert"); expect(compare).not.toHaveBeenCalled();
  });
});

describe("selection photo preview", () => {
  const material = { ...base("A"), colourCode: "CL", interiorColourCode: "BK", interiorColorName: "Black" };
  it("loads only the first photo on hover and opens the existing multi-angle gallery on click", async () => {
    const load = vi.fn(async () => catalog());
    render(<MaterialColourPreview material={material} kind="exterior" name="Black" scope="user" loadCatalog={load} />);
    expect(load).not.toHaveBeenCalled(); expect(materialAssets.photo).not.toHaveBeenCalled();
    const button = screen.getByRole("button", { name: "Preview Black photos" });
    fireEvent.mouseEnter(button); await screen.findByAltText("Black");
    expect(materialAssets.photo).toHaveBeenCalledWith("front", expect.any(AbortSignal));
    expect(materialAssets.photo).not.toHaveBeenCalledWith("rear", expect.any(AbortSignal));
    fireEvent.click(button); await screen.findByRole("dialog");
    fireEvent.click(screen.getByRole("button", { name: "→" }));
    await screen.findByAltText("Black views · rear.png");
    expect(materialAssets.photo).toHaveBeenCalledWith("rear", expect.any(AbortSignal));
  });
  it("opens on the first touch click, without requiring prior hover", async () => {
    render(<MaterialColourPreview material={material} kind="exterior" name="Black" scope="user" loadCatalog={async () => catalog()} />);
    fireEvent.click(screen.getByRole("button", { name: "Preview Black photos" }));
    await screen.findByRole("dialog"); expect(await screen.findByAltText("Black views · front.png")).toBeTruthy();
  });
  it("matches interior by the saved code and never substitutes an exterior with the same HEX", async () => {
    const data = catalog();
    data.bindings[0] = { ...data.bindings[0], kind: "interior", colourKey: "code:BK" };
    data.groups[0] = { ...data.groups[0], kind: "interior", colourKey: "code:BK" };
    const rendered = render(<MaterialColourPreview material={material} kind="interior" name="Black interior" scope="user" loadCatalog={async () => data} />);
    fireEvent.focus(screen.getByRole("button")); await screen.findByAltText("Black interior");
    rendered.unmount();
    render(<MaterialColourPreview material={{ ...material, colourCode: "OTHER" }} kind="exterior" name="Black exterior" scope="user" loadCatalog={async () => catalog()} />);
    fireEvent.focus(screen.getByRole("button")); await screen.findByText("No photos linked");
    expect(screen.queryByAltText("Black exterior")).toBeNull();
  });
  it("reports metadata failures and retries on the next hover", async () => {
    const load = vi.fn<() => Promise<AssetCatalog>>().mockRejectedValueOnce(new Error("offline")).mockResolvedValue(catalog());
    render(<MaterialColourPreview material={material} kind="exterior" name="Black" scope="user" loadCatalog={load} />);
    const button = screen.getByRole("button"); fireEvent.mouseEnter(button); await screen.findByText("offline");
    fireEvent.mouseLeave(button); await waitFor(() => expect(screen.queryByText("offline")).toBeNull());
    fireEvent.mouseEnter(button); await screen.findByAltText("Black"); expect(load).toHaveBeenCalledTimes(2);
  });
});
