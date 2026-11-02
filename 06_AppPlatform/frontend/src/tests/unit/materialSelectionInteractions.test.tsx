// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { materialAssets } from "../../api/materialAssets";
import { MaterialColourPreview } from "../../components/orderGenius/MaterialColourPreview";
import { MaterialComparisonPanel, MaterialConfigurationComparison } from "../../components/orderGenius/MaterialConfigurationComparison";
import { MaterialPhotoThumbnail, MaterialPhotoViewer } from "../../components/orderGenius/MaterialPhotoViewer";
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
  it("switches baseline without reordering, expands common items and exposes cell provenance inline", async () => {
    render(<MaterialConfigurationComparison comparison={comparison} catalog={catalog()} t={en => en} />);
    const baseline = screen.getByRole("button", { name: "Set baseline A**" });
    fireEvent.click(baseline); expect(baseline.getAttribute("aria-pressed")).toBe("true");
    expect(Array.from(document.querySelectorAll(".mw-compare-card .mw-compare-label")).map(item => item.textContent)).toEqual(["1B**", "2A**"]);
    fireEvent.click(screen.getByRole("button", { name: /Common items/ })); expect(screen.getByText("Same")).toBeTruthy();
    const source = screen.getByRole("button", { name: "Airbags · A** · Source" });
    fireEvent.click(source);
    const region = screen.getByRole("region", { name: "Cell source" });
    expect(region.textContent).toContain("matrix.xlsx"); expect(region.textContent).toContain("AA9"); expect(region.textContent).toContain("uuid-A");
    fireEvent.click(screen.getByRole("button", { name: "Close source" })); expect(document.activeElement).toBe(source);
    fireEvent.click(screen.getByRole("button", { name: /▾ Safety/ })); expect(screen.queryByText("Airbags")).toBeNull();
    expect(materialAssets.photo).not.toHaveBeenCalled();
  });
  it("distinguishes missing, blank, zero and different units rather than inventing equipment", () => {
    const rows: ConfigComparison["rows"] = [
      { category: "Safety", featureName: "Zero", values: [cell("0"), null] },
      { category: "Safety", featureName: "Blank", values: [cell(""), cell("Not equipped")] },
      { category: "Powertrain", featureName: "Unit", values: [{ ...cell("100"), unit: "kW" }, { ...cell("100"), unit: "hp" }] },
    ];
    render(<MaterialConfigurationComparison comparison={{ ...comparison, rows }} catalog={catalog()} t={en => en} />);
    for (const value of ["0", "Not recorded", "Not filled", "Not equipped", "100 kW", "100 hp"]) expect(screen.getByText(value)).toBeTruthy();
  });
  it("compares normalized signing markers without discarding meaningful annotations", () => {
    const rows: ConfigComparison["rows"] = [
      { category: "Safety", featureName: "Same equipment", values: [
        { ...cell("S"), availability: "STANDARD", normalized_value: "标配" },
        { ...cell("●"), availability: "STANDARD", normalized_value: "标配" },
      ] },
      { category: "Safety", featureName: "Lighting detail", values: [
        { ...cell("S（LED）"), availability: "STANDARD", normalized_value: "标配（LED）" },
        { ...cell("S（卤素）"), availability: "STANDARD", normalized_value: "标配（卤素）" },
      ] },
    ];
    render(<MaterialConfigurationComparison comparison={{ ...comparison, rows }} catalog={catalog()} t={en => en} />);
    expect(screen.queryByText("Same equipment")).toBeNull(); expect(screen.getByText("Lighting detail")).toBeTruthy();
    expect(screen.getByText("S（LED）")).toBeTruthy(); expect(screen.getByText("S（卤素）")).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: /Common items/ })); expect(screen.getByText("Same equipment")).toBeTruthy();
  });
  it("shows BOM interior differences even when the configuration source and values are identical", () => {
    const data = catalog();
    data.materials[1].interiors = [{ key: "code:BN", code: "BN", name: "Brown", hex: "#885533" }];
    const same = { ...comparison, rows: [{ category: "Safety", featureName: "Same", values: [cell("6"), cell("6")] }] };
    render(<MaterialConfigurationComparison comparison={same} catalog={data} t={en => en} />);
    expect(screen.getByText("Interior")).toBeTruthy(); expect(screen.getByText("BN · Brown")).toBeTruthy();
    expect(screen.queryByText("Same")).toBeNull(); expect(screen.queryByText("No recorded differences")).toBeNull();
  });
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
    expect(materialAssets.photo).toHaveBeenCalledWith("front", expect.any(AbortSignal), "thumbnail");
    expect(materialAssets.photo).not.toHaveBeenCalledWith("rear", expect.any(AbortSignal), "thumbnail");
    fireEvent.click(button); await screen.findByRole("dialog");
    fireEvent.click(screen.getByRole("button", { name: "→" }));
    await screen.findByAltText("Black views · rear.png");
    expect(materialAssets.photo).toHaveBeenCalledWith("rear", expect.any(AbortSignal), "display");
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

describe("bounded photo loading", () => {
  it("preloads only adjacent display images, reuses them on navigation and releases every URL on close", async () => {
    const group = { ...catalog().groups[0], files: Array.from({ length: 6 }, (_, index) => ({ id: `p${index}`, filename: `${index}.webp`, contentType: "image/webp", sha256: String(index) })) };
    const view = render(<MaterialPhotoViewer group={group} scope="memory" t={en => en} onClose={() => undefined} />);
    await screen.findByAltText(/0.webp/);
    expect(vi.mocked(materialAssets.photo).mock.calls.map(call => [call[0], call[2]])).toEqual([["p0", "display"], ["p1", "display"]]);
    fireEvent.click(screen.getByRole("button", { name: "→" })); await screen.findByAltText(/1.webp/);
    await waitFor(() => expect(materialAssets.photo).toHaveBeenCalledTimes(3));
    expect(vi.mocked(materialAssets.photo).mock.calls.map(call => call[0])).toEqual(["p0", "p1", "p2"]);
    const revoke = vi.mocked(URL.revokeObjectURL); view.unmount(); await waitFor(() => expect(revoke).toHaveBeenCalledTimes(3));
    expect(vi.mocked(materialAssets.photo).mock.calls.every(call => call[1]?.aborted)).toBe(true);
  });
  it("loads thumbnails only near the viewport and aborts/releases them when offscreen", async () => {
    let visibility: IntersectionObserverCallback | null = null;
    vi.stubGlobal("IntersectionObserver", class { constructor(callback: IntersectionObserverCallback) { visibility = callback; } observe(): void {} disconnect(): void {} });
    render(<MaterialPhotoThumbnail group={catalog().groups[0]} scope="viewport" onClick={() => undefined} />);
    expect(materialAssets.photo).not.toHaveBeenCalled();
    const intersect = (visible: boolean): void => { if (visibility) visibility([{ isIntersecting: visible } as IntersectionObserverEntry], {} as IntersectionObserver); };
    act(() => intersect(true)); await screen.findByAltText("Black views");
    expect(materialAssets.photo).toHaveBeenCalledWith("front", expect.any(AbortSignal), "thumbnail");
    act(() => intersect(false)); await waitFor(() => expect(URL.revokeObjectURL).toHaveBeenCalledOnce());
    expect(vi.mocked(materialAssets.photo).mock.calls[0][1]?.aborted).toBe(true);
  });
});
