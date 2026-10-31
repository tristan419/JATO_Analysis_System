// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { useState } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { MaterialTimelineBoard, monthIndex, monthIso, replacementChains, timelineBar } from "../../components/orderGenius/MaterialTimelineBoard";
import type { MaterialSkuMatrixRow, MaterialTimelineNode, MaterialTimelineResponse } from "../../types/orderGenius";
import type { MaterialIdentity } from "../../types/materialAssets";
const node = (base: string, values: Partial<MaterialTimelineNode> = {}): MaterialTimelineNode => ({
  bomTemplate: base, brand: "JAECOO", modelName: "J7", version: "FWD", powertrain: "PHEV", materialCodes: [base + "-CL"],
  lifecycleStatus: "active", effectiveFrom: "2026-10-15", effectiveTo: "2026-10-30", notes: [], issues: [], quoteDate: "2026-10-15", quotedSkuCount: 1, quoteReasons: [], ...values,
});
const data = (countryCode = "CH", nodes = [node("NEW**")]): MaterialTimelineResponse => ({ countryCode, today: "2026-10-12", windowFrom: "2026-09-12", windowTo: "2026-11-11", nodes, links: [], events: [] });
const props = { countries: ["CH"], authorizationKey: "account1", selectionRows: [], selectionComplete: true, onViewModel: vi.fn(), onOpenAssets: vi.fn(), onCompare: vi.fn() };
function Fixture({ override }: { override: Partial<Parameters<typeof MaterialTimelineBoard>[0]> }) {
  const [selected, setSelected] = useState<MaterialIdentity[]>([]);
  return <MaterialTimelineBoard {...props} comparisonMaterials={selected} onToggleComparison={({ brand, modelName, bomTemplate }) => setSelected(previous => previous.some(item => item.brand === brand && item.modelName === modelName && item.bomTemplate === bomTemplate)
    ? previous.filter(item => item.brand !== brand || item.modelName !== modelName || item.bomTemplate !== bomTemplate) : [...previous, { brand, modelName, bomTemplate }])} {...override} />;
}
function view(override: Partial<Parameters<typeof MaterialTimelineBoard>[0]> = {}) { return <MemoryRouter><Fixture override={override} /></MemoryRouter>; }
beforeEach(() => { localStorage.clear(); props.onOpenAssets.mockReset(); props.onCompare.mockReset(); vi.spyOn(api, "getMaterialNotices").mockImplementation(async country => ({ countryCode: country, notices: [] })); });
afterEach(() => { cleanup(); vi.restoreAllMocks(); });
describe("Lightweight Notice Board", () => {
  it("uses one shared four-month window and groups Model, Trim, base", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data("CH", [node("A**"), node("B**"), node("C**", { version: "AWD" })]));
    render(view()); await screen.findByText("A**");
    expect(document.querySelectorAll(".mw-trim-line")).toHaveLength(2); expect(document.querySelectorAll(".mw-calendar-row")).toHaveLength(3);
    expect(document.querySelectorAll(".mw-range")).toHaveLength(1);
    const start = screen.getByLabelText<HTMLInputElement>("Window start"), end = screen.getByLabelText<HTMLInputElement>("Window end");
    expect(start.value).toBe("2026-07"); expect(end.value).toBe("2026-10"); expect(start.min).toBe("2023-10"); expect(end.max).toBe("2029-10");
    expect(screen.queryByText(/Edit.*BOM/)).toBeNull();
  });
  it("retains overlapping bars, unknown dates, and metadata conflicts without invented windows", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data("CH", [node("A**"), node("B**"), node("MISSING**", { effectiveFrom: null, effectiveTo: null }), node("CONFLICT**", { issues: ["Conflicting dates"] })]));
    render(view()); await screen.findByText("MISSING**");
    expect(document.querySelectorAll(".mw-bar")).toHaveLength(2); expect(screen.getAllByText(/ · Dates to review$/)).toHaveLength(2);
    expect(timelineBar(node("ENDONLY", { effectiveFrom: null }), monthIndex("2026-07"), monthIndex("2026-10"))).toBeNull();
  });
  it("uses day boundaries and open periods, without inventing an end date", () => {
    const bar = timelineBar(node("A", { effectiveFrom: "2026-10-16", effectiveTo: null }), monthIndex("2026-10"), monthIndex("2026-10"));
    expect(Number.parseFloat(String(bar?.left))).toBeCloseTo(15 / 31 * 100);
    expect(Number.parseFloat(String(bar?.width))).toBeCloseTo(16 / 31 * 100);
    expect(monthIso(monthIndex("2026-01"))).toBe("2026-01");
  });
  it("merges identical country lanes but not conflicting dates", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockImplementation(async country => data(country, [node("A**", country === "SE" ? { effectiveFrom: "2026-09-01" } : {})]));
    render(view({ countries: ["CH", "SE"] })); await waitFor(() => expect(document.querySelectorAll(".mw-calendar-row")).toHaveLength(2));
  });
  it("selection-only keeps an explicit replacement chain, not all same-model bases", async () => {
    const source = data("CH", [node("A**"), node("B**"), node("C**"), node("OTHER**")]); source.links = [{ from: "A**", to: "B**" }, { from: "B**", to: "C**" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(source);
    const row: MaterialSkuMatrixRow & { _countryCode: string } = { materialCode: "B**-CL", bomTemplate: "B**", brand: "JAECOO", modelName: "J7", version: "FWD", colour: "Black", colourCode: "CL", powertrain: "PHEV", fobEur: 20000, lifecycleStatus: "active", months: {}, _countryCode: "CH", editable: true, displayStyle: null, remark: null, ttl: 0 };
    render(view({ selectionRows: [row] })); await screen.findByText("OTHER**"); fireEvent.click(screen.getByLabelText("Selection"));
    expect(screen.queryByText("OTHER**")).toBeNull(); expect(document.querySelectorAll(".mw-calendar-row")).toHaveLength(1);
  });
  it("disables incomplete selection and leaves it without showing stale rows", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data()); const rendered = render(view()); await screen.findByText("NEW**");
    fireEvent.click(screen.getByLabelText("Selection")); rendered.rerender(view({ selectionComplete: false }));
    expect(screen.queryByText("NEW**")).toBeNull(); fireEvent.click(screen.getByLabelText("Selection")); expect(screen.getByText("NEW**")).toBeTruthy();
  });
  it("clears old countries immediately and ignores late responses", async () => {
    let resolveOld: (value: MaterialTimelineResponse) => void = () => undefined;
    vi.spyOn(api, "getMaterialTimeline").mockImplementation(country => country === "CH" ? new Promise(resolve => { resolveOld = resolve; }) : Promise.resolve(data("SE", [node("SWEDEN**")])));
    const rendered = render(view()); rendered.rerender(view({ countries: ["SE"], authorizationKey: "changed" })); await screen.findByText("SWEDEN**");
    await act(async () => resolveOld(data("CH", [node("OLDCH**")]))); expect(screen.queryByText("OLDCH**")).toBeNull();
  });
  it("does not present partial country success and supports retry", async () => {
    const get = vi.spyOn(api, "getMaterialTimeline").mockImplementation(country => country === "CH" ? Promise.resolve(data()) : Promise.reject(new Error("network")));
    render(view({ countries: ["CH", "SE"] })); await screen.findByRole("alert"); expect(screen.queryByText("NEW**")).toBeNull();
    get.mockImplementation(async country => data(country)); fireEvent.click(screen.getByLabelText("Reload")); await screen.findByText("NEW**");
  });
  it("keeps reminder country scope independent and never marks read on calendar navigation", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data()); const read = vi.spyOn(api, "readMaterialNotice");
    render(view({ noticeCountries: ["CH", "SE"] })); await screen.findByText("NEW**"); expect(api.getMaterialNotices).toHaveBeenCalledWith("SE"); expect(read).not.toHaveBeenCalled();
  });
  it("loads WVTA only after opt-in and rejects mixed reference snapshots", async () => {
    const get = vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country, reference) => ({ ...data(country), ...(reference ? { wvtaReference: { status: "ready", filename: "ref.pdf", sha256: country, updatedAt: null, records: [] } } : {}) }));
    render(view({ countries: ["CH", "SE"] })); await screen.findByText("NEW**"); expect(get).toHaveBeenCalledWith("CH", false);
    fireEvent.click(screen.getByLabelText("Show reference")); await screen.findByRole("alert"); expect(screen.queryByText("NEW**")).toBeNull();
    fireEvent.click(screen.getByLabelText("Show reference")); await screen.findByText("NEW**");
  });
  it("reloads after BOM maintenance and permission changes", async () => {
    const get = vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data()); const rendered = render(view()); await screen.findByText("NEW**");
    rendered.rerender(view({ bomEditing: true })); expect(screen.queryByText("NEW**")).toBeNull();
    rendered.rerender(view()); await waitFor(() => expect(get).toHaveBeenCalledTimes(2));
    rendered.rerender(view({ authorizationKey: "newRole" })); await waitFor(() => expect(get).toHaveBeenCalledTimes(3));
  });
  it("compares explicit bases across models and preserves EN/中文 preference", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data("CH", [node("A**"), node("B**", { modelName: "J8" })]));
    render(view()); await screen.findByText("A**"); fireEvent.click(screen.getByLabelText("Compare A** CH")); fireEvent.click(screen.getByLabelText("Compare B** CH"));
    fireEvent.click(screen.getByRole("button", { name: "Compare 2" }));
    expect(props.onCompare).toHaveBeenCalledWith([{ brand: "JAECOO", modelName: "J7", bomTemplate: "A**" }, { brand: "JAECOO", modelName: "J8", bomTemplate: "B**" }]);
    fireEvent.click(screen.getByRole("button", { name: "中文" })); expect(screen.getByRole("region", { name: "物料公告" })).toBeTruthy(); expect(localStorage.getItem("jato:material-language")).toBe("zh");
  });
  it("folds calendar and reminder details into one compact row", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data());
    render(view()); await screen.findByText("NEW**");
    fireEvent.click(screen.getByRole("button", { name: "▾ Notice Board" }));
    expect(document.querySelector(".mw-board.is-collapsed")).toBeTruthy();
    expect(screen.queryByLabelText("Window start")).toBeNull();
    expect(document.querySelector("details.mw-notices")).toBeNull();
    expect(document.querySelector(".mw-notices-compact")?.textContent).toContain("Reminders 0");
    expect(screen.queryByRole("link", { name: "Material Assets" })).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "▸ Notice Board" }));
    expect(screen.getByLabelText("Window start")).toBeTruthy();
  });
  it("shows saved A → B → C replacements without changing their actual dates", async () => {
    const response = data("CH", [node("A**"), node("B**"), node("C**")]);
    response.links = [{ from: "A**", to: "B**" }, { from: "B**", to: "C**" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(response);
    render(view()); const chain = await screen.findByLabelText("Replacement chain");
    expect(Array.from(chain.querySelectorAll(".mw-material-code")).map(item => item.textContent)).toEqual(["A**", "B**", "C**"]);
    expect(document.querySelectorAll(".mw-calendar-row")).toHaveLength(1);
    expect(document.querySelectorAll(".mw-bar")).toHaveLength(3);
    fireEvent.click(screen.getByRole("button", { name: "Compare B** → C**" }));
    expect(props.onCompare).toHaveBeenCalledWith([expect.objectContaining({ bomTemplate: "B**" }), expect.objectContaining({ bomTemplate: "C**" })]);
    fireEvent.click(screen.getByRole("button", { name: "B**" }));
    expect(props.onOpenAssets).toHaveBeenCalledWith({ materials: [{ brand: "JAECOO", modelName: "J7", bomTemplate: "B**" }], compare: false });
  });
  it("shares a lane only for saved replacement links and stacks real overlapping periods", async () => {
    const response = data("CH", [node("A**", { effectiveFrom: "2026-07-01", effectiveTo: "2026-08-31" }), node("B**", { effectiveFrom: "2026-08-15", effectiveTo: null }), node("PARALLEL**")]);
    response.links = [{ from: "A**", to: "B**" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(response);
    render(view()); await screen.findByText("PARALLEL**");
    expect(document.querySelectorAll(".mw-calendar-row")).toHaveLength(2);
    const bars = document.querySelectorAll<HTMLButtonElement>(".mw-calendar-row .mw-bar");
    expect(bars[0].style.top).not.toBe(bars[1].style.top);
    expect(bars[0].title).toContain("2026-08-31");
    expect(bars[1].title).toContain("2026-08-15 → ∞");
  });
  it("never stitches links from different countries/models, or infers links from dates", () => {
    const ch = data("CH", [node("A"), node("B"), node("C"), node("OTHER", { modelName: "J8" })]);
    ch.links = [{ from: "A", to: "B" }, { from: "C", to: "OTHER" }];
    const se = data("SE", [node("A"), node("B"), node("C")]); se.links = [{ from: "B", to: "C" }];
    expect(replacementChains([ch, se]).map(chain => [chain.countries, chain.materials])).toEqual([[["CH"], ["A", "B"]], [["SE"], ["B", "C"]]]);
    expect(replacementChains([data("CH", [node("A"), node("B")])])).toEqual([]);
  });
  it("keeps branches separate and invalid cycles finite", () => {
    const response = data("CH", [node("A"), node("B"), node("C")]);
    response.links = [{ from: "A", to: "B" }, { from: "A", to: "C" }];
    expect(replacementChains([response]).map(chain => chain.materials)).toEqual([["A", "B"], ["A", "C"]]);
    response.links = [{ from: "A", to: "B" }, { from: "B", to: "A" }];
    expect(replacementChains([response]).map(chain => chain.materials)).toEqual([["A", "B"], ["B", "A"]]);
  });
});
