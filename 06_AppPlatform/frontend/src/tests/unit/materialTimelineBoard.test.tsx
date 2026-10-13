// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { api } from "../../api/client";
import { MaterialTimelineBoard } from "../../components/orderGenius/MaterialTimelineBoard";
import type { MaterialTimelineNode, MaterialTimelineResponse } from "../../types/orderGenius";

const node = (bomTemplate: string, values: Partial<MaterialTimelineNode> = {}): MaterialTimelineNode => ({
  bomTemplate, brand: "JAECOO", modelName: "JAECOO5 BEV", version: "Select-FWD", powertrain: "BEV",
  materialCodes: [`${bomTemplate}-BW`, `${bomTemplate}-CL`], lifecycleStatus: "active",
  effectiveFrom: "2026-10-15", effectiveTo: "2026-10-30", notes: ["Tyre upgrade"], issues: [],
  quoteDate: "2026-10-15", quotedSkuCount: 2, quoteReasons: [], ...values,
});
const dataset = (countryCode = "SE", nodes = [node("NEW")]): MaterialTimelineResponse => ({
  countryCode, today: "2026-10-12", windowFrom: "2026-09-12", windowTo: "2026-11-11", nodes, links: [],
  events: nodes.flatMap((value) => value.effectiveFrom ? [{ bomTemplate: value.bomTemplate, kind: "start" as const, date: value.effectiveFrom }] : []),
});
const props = { countries: ["SE"], authorizationKey: "filler-SE-JAECOO", selectionRows: [], selectionComplete: true, onViewModel: vi.fn() };
beforeEach(() => { vi.spyOn(api, "getMaterialNotices").mockImplementation(async (country) => ({ countryCode: country, notices: [] })); });
afterEach(() => { cleanup(); vi.restoreAllMocks(); vi.unstubAllGlobals(); props.onViewModel.mockClear(); });
async function expand() {
  await waitFor(() => expect(screen.getByRole("status").textContent).not.toContain("Loading"));
  fireEvent.click(screen.getByRole("button", { name: "View timeline / 查看时间轴" }));
}

describe("Model material changes board", () => {
  it("uses one model ruler for two saved branches and collapses unlinked bases by default", async () => {
    const data = dataset("SE", [node("A1"), node("A2"), node("B1"), node("B2"), node("UNLINKED")]);
    data.links = [{ from: "A1", to: "A2", note: "Battery upgrade" }, { from: "B1", to: "B2" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data);
    render(<MaterialTimelineBoard {...props} />); await expand();
    expect(document.querySelectorAll(".og-lineage-axis")).toHaveLength(1);
    expect(document.querySelectorAll(".og-lineage-branch")).toHaveLength(2);
    expect(document.querySelector(".og-lineage-pending")?.hasAttribute("open")).toBe(false);
    expect(document.querySelectorAll(".og-timeline-version-details")).toHaveLength(0);
    fireEvent.click(screen.getByText(/^SE · JAECOO · JAECOO5 BEV · 2 chains/));
    fireEvent.click(screen.getByRole("button", { name: "A1 → A2 · Note / 备注" }));
    expect(screen.getByRole("region", { name: "Material change detail" }).textContent).toContain("Battery upgrade");
    fireEvent.change(screen.getByRole("combobox", { name: "Time zoom" }), { target: { value: "2" } });
    expect((document.querySelector(".og-lineage-map") as HTMLElement).style.minWidth).toBe("1600px");
  });

  it("retains overlapping windows and does not stretch missing dates into an invented period", async () => {
    const data = dataset("SE", [node("OLD"), node("NEW"), node("FUTURE", { effectiveFrom: null, effectiveTo: null })]);
    data.links = [{ from: "OLD", to: "NEW" }, { from: "NEW", to: "FUTURE" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data);
    render(<MaterialTimelineBoard {...props} />); await expand();
    const versions = [...document.querySelectorAll<HTMLElement>(".og-lineage-version")];
    expect(versions).toHaveLength(2);
    expect(versions.map((version) => version.style.top)).toEqual(["0px", "84px"]);
    expect(screen.getByText(/FUTURE · Start not set/)).toBeTruthy();
    expect(screen.queryByRole("button", { name: "FUTURE starts" })).toBeNull();
  });

  it("does not link a colliding base code from another brand", async () => {
    const data = dataset("SE", [node("A"), node("B", { brand: "OMODA", modelName: "OMODA5 BEV" })]);
    data.links = [{ from: "A", to: "B" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data);
    render(<MaterialTimelineBoard {...props} />); await expand();
    expect(document.querySelectorAll(".og-lineage-branch")).toHaveLength(0);
  });

  it("shows only a known boundary pin for an open BOM window", async () => {
    const data = dataset("SE", [node("OLD"), node("OPEN", { effectiveFrom: "2026-10-01", effectiveTo: null })]);
    data.links = [{ from: "OLD", to: "OPEN" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data);
    render(<MaterialTimelineBoard {...props} />); await expand();
    expect(screen.getByRole("button", { name: "OPEN starts 2026-10-01" })).toBeTruthy();
    expect(screen.queryByRole("button", { name: /^OPEN ends/ })).toBeNull();
    expect(document.querySelectorAll(".og-lineage-period")).toHaveLength(1);
    expect(screen.getByText(/OPEN · 2026-10-01 → End not set/)).toBeTruthy();
  });

  it("keeps personal reminder countries independent of the temporary timeline filter", async () => {
    const timeline = vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country) => dataset(country));
    render(<MaterialTimelineBoard {...props} countries={["CH"]} noticeCountries={["CH", "SE"]} />);
    await waitFor(() => expect(api.getMaterialNotices).toHaveBeenCalledWith("SE"));
    expect(timeline).toHaveBeenCalledWith("CH", false);
    expect(timeline).not.toHaveBeenCalledWith("SE", false);
  });
  it("loads certificate segments only after opt-in, without changing BOM windows or reminders", async () => {
    const record = { materialGroup: "NEW", wvtaNo: "e4*2018/858*00273*02", cocNo: "00273-02&COC002",
      validFrom: "2026-03-11", validTo: "2026-04-14", comments: "Certificate note", pageNumber: 3, tableRowNumber: 40,
      sourceStatus: "recorded" as const, reviewReason: null };
    const source: MaterialTimelineResponse["wvtaReference"] = { status: "ready", filename: "reference.pdf", sha256: "a".repeat(64), updatedAt: "2026-10-12", records: [
      record, { ...record, wvtaNo: "e4*2018/858*00273*03", validFrom: "2026-04-15", validTo: null },
      { ...record, sourceStatus: "cancelled", reviewReason: "Red strike-through" },
    ] };
    const request = vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country, include) => ({ ...dataset(country), ...(include ? { wvtaReference: source } : {}) }));
    const write = vi.spyOn(api, "updateSkuLifecycle");
    render(<MaterialTimelineBoard {...props} />); await expand();
    expect(request).toHaveBeenLastCalledWith("SE", false);
    const summary = screen.getByRole("status").textContent;
    const checkbox = screen.getByRole("checkbox", { name: /Show WVTA/ });
    fireEvent.click(checkbox);
    await waitFor(() => expect(screen.getByText(/Reference \/ 参考：reference.pdf/)).toBeTruthy());
    expect(request).toHaveBeenLastCalledWith("SE", true);
    expect(screen.getByRole("status").textContent).toBe(summary);
    expect(document.querySelectorAll(".og-wvta-segment > i").length).toBe(2);
    expect(document.querySelector(".og-wvta-segment.is-cancelled > i")).toBeNull();
    fireEvent.click(screen.getAllByRole("button", { name: /WVTA · e4.*00273\*02/ })[0]);
    expect(screen.getByRole("region", { name: "Material change detail" }).textContent).toContain("2026-10-15");
    expect(screen.getByRole("region", { name: "Material change detail" }).textContent).toContain("P3 · row / 行 40");
    fireEvent.click(checkbox);
    await waitFor(() => expect(document.querySelector(".og-wvta-segment")).toBeNull());
    expect(write).not.toHaveBeenCalled();
  });

  it("allows unchecking WVTA after read failure and reports a missing source separately", async () => {
    const request = vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country, include) => {
      if (include) throw new Error("Reference unreadable"); return dataset(country);
    });
    render(<MaterialTimelineBoard {...props} />); await expand();
    const checkbox = screen.getByRole("checkbox", { name: /Show WVTA/ }); fireEvent.click(checkbox);
    await waitFor(() => expect(screen.getByRole("alert")).toBeTruthy());
    fireEvent.click(checkbox);
    await waitFor(() => expect(screen.queryByRole("alert")).toBeNull());
    request.mockImplementation(async (country) => ({ ...dataset(country), wvtaReference: { status: "missing", filename: null, sha256: null, updatedAt: null, records: [] } }));
    fireEvent.click(checkbox);
    await waitFor(() => expect(screen.getByText(/尚未配置WVTA参考/)).toBeTruthy());
  });

  it("does not mix different country reference snapshots", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country, include) => ({ ...dataset(country), ...(include ? {
      wvtaReference: { status: "ready" as const, filename: "reference.pdf", sha256: country, updatedAt: "2026-10-12", records: [] },
    } : {}) }));
    render(<MaterialTimelineBoard {...props} countries={["CH", "SE"]} />); await expand();
    fireEvent.click(screen.getByRole("checkbox", { name: /Show WVTA/ }));
    await waitFor(() => expect(screen.getByRole("alert")).toBeTruthy());
    expect(screen.queryByText("NEW")).toBeNull();
  });

  it("shows the admin-only upload confirmation and saves via the existing upload helper", async () => {
    vi.stubGlobal("crypto", { subtle: { digest: vi.fn().mockResolvedValue(new Uint8Array(32).fill(170).buffer) } });
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(dataset());
    const upload = vi.spyOn(api, "cocFillUploadFile").mockResolvedValue("upload-id");
    const save = vi.spyOn(api, "setWvtaReference").mockResolvedValue({ filename: "new.pdf", sha256: "a".repeat(64), recordCount: 2, reviewCount: 0, cancelledCount: 1 });
    const view = render(<MaterialTimelineBoard {...props} />); await expand();
    expect(screen.queryByLabelText(/Replace WVTA reference PDF/)).toBeNull();
    view.rerender(<MaterialTimelineBoard {...props} canManageReference />);
    const file = new File(["pdf"], "new.pdf", { type: "application/pdf" });
    Object.defineProperty(file, "arrayBuffer", { value: async () => new ArrayBuffer(3) });
    fireEvent.change(screen.getByLabelText(/Replace WVTA reference PDF/), { target: { files: [file] } });
    expect(screen.getByRole("dialog").textContent).toContain("BOM dates, links, prices and PI remain unchanged");
    expect(upload).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Upload reference / 上传参考" }));
    await waitFor(() => expect(screen.getByText(/Reference saved/)).toBeTruthy());
    expect(upload).toHaveBeenCalledWith(file, `wvta-reference-${"aa".repeat(32)}`);
    expect(save).toHaveBeenCalledWith("upload-id", "aa".repeat(32));
  });

  it("aggregates colors and countries by model and opens its dated detail without writing", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country) => dataset(country));
    const write = vi.spyOn(api, "updateSkuLifecycle");
    render(<MaterialTimelineBoard {...props} countries={["CH", "SE"]} />);
    await expand();
    expect(screen.getByRole("status").textContent).toContain("1 models");
    fireEvent.click(screen.getByRole("button", { name: /CH \/ SE · JAECOO · JAECOO5 BEV/ }));
    expect(screen.getByText(/As of.*2026-10-12/).textContent).toContain("Not the selected order month");
    fireEvent.click(screen.getAllByRole("button", { name: /NEW · Select-FWD/ })[0]);
    const detail = screen.getByRole("region", { name: "Material change detail" });
    expect(detail.textContent).toContain("Tyre upgrade");
    expect(detail.textContent).toContain("NEW-BW · NEW-CL");
    fireEvent.click(screen.getByRole("button", { name: "View model selection / 查看车型选品" }));
    expect(props.onViewModel).toHaveBeenCalledWith("JAECOO", "JAECOO5 BEV");
    expect(write).not.toHaveBeenCalled();
  });

  it("keeps missing dates and relations explicit rather than inventing an upgrade", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(dataset("SE", [node("UNKNOWN", { effectiveFrom: null, effectiveTo: null })]));
    render(<MaterialTimelineBoard {...props} />);
    await expand();
    expect(screen.getByText(/Relation not maintained \/ 关联未维护/)).toBeTruthy();
    expect(screen.getByText("Dates not maintained / 日期待维护")).toBeTruthy();
    expect(screen.getByRole("status").textContent).toContain("0 models");
    expect(screen.queryByRole("button", { name: /UNKNOWN starts/ })).toBeNull();
  });

  it("only-selection retains a saved whole chain, not unrelated same-model bases", async () => {
    const data = dataset("SE", [node("OLD"), node("MID"), node("NEW"), node("UNRELATED")]);
    data.links = [{ from: "OLD", to: "MID" }, { from: "MID", to: "NEW" }];
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(data);
    render(<MaterialTimelineBoard {...props} selectionRows={[{
      _countryCode: "SE", materialCode: "NEW-BW", bomTemplate: "NEW", brand: "JAECOO", modelName: "JAECOO5 BEV",
      version: "Select-FWD", powertrain: "BEV", colour: "White", colourCode: "BW", fobEur: 1000,
      lifecycleStatus: "active", editable: true, displayStyle: null, months: {}, ttl: 0, remark: null,
    }]} />);
    await expand();
    fireEvent.click(screen.getByRole("checkbox", { name: /Only current selection/ }));
    expect(screen.getByText("Saved relation / 已保存关联：OLD → MID → NEW")).toBeTruthy();
    expect(screen.queryByText("UNRELATED")).toBeNull();
    expect(screen.getAllByText("OLD").length).toBeGreaterThan(0);
    expect(document.querySelectorAll(".og-timeline-chain").length).toBe(1);
  });

  it("disables selection filtering until the full selection has loaded", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(dataset());
    render(<MaterialTimelineBoard {...props} selectionComplete={false} />);
    await expand();
    expect((screen.getByRole("checkbox", { name: /Only current selection/ }) as HTMLInputElement).disabled).toBe(true);
  });

  it("does not call an incomplete selection empty and allows leaving that filter", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(dataset());
    const view = render(<MaterialTimelineBoard {...props} />);
    await expand();
    fireEvent.click(screen.getByRole("checkbox", { name: /Only current selection/ }));
    view.rerender(<MaterialTimelineBoard {...props} selectionComplete={false} />);
    expect(screen.getByRole("status").textContent).toContain("Selection loading");
    expect(screen.queryByText("No eligible products in this scope / 当前范围没有符合条件的产品。")).toBeNull();
    expect((screen.getByRole("checkbox", { name: /Only current selection/ }) as HTMLInputElement).disabled).toBe(false);
    fireEvent.click(screen.getByRole("checkbox", { name: /Only current selection/ }));
    expect(screen.getByText("NEW")).toBeTruthy();
  });

  it("clears old countries immediately and ignores their late response", async () => {
    let resolveOld: (value: MaterialTimelineResponse) => void = () => {};
    vi.spyOn(api, "getMaterialTimeline").mockImplementation((country) => country === "CH"
      ? new Promise((resolve) => { resolveOld = resolve; }) : Promise.resolve(dataset("SE", [node("SWEDEN")])));
    const view = render(<MaterialTimelineBoard {...props} countries={["CH"]} />);
    view.rerender(<MaterialTimelineBoard {...props} countries={["SE"]} />);
    await expand();
    await act(async () => { resolveOld(dataset("CH", [node("SECRET-OLD-COUNTRY")])); });
    expect(screen.getByText("SWEDEN")).toBeTruthy();
    expect(screen.queryByText("SECRET-OLD-COUNTRY")).toBeNull();
  });

  it("does not present partial country success and reloads after failure", async () => {
    const request = vi.spyOn(api, "getMaterialTimeline").mockImplementation(async (country) => {
      if (country === "CH") throw new Error("private backend detail");
      return dataset(country);
    });
    render(<MaterialTimelineBoard {...props} countries={["CH", "SE"]} />);
    await expand();
    expect(screen.getByRole("alert").textContent).toContain("Unable to read");
    expect(screen.queryByText("NEW")).toBeNull();
    expect(screen.queryByText("private backend detail")).toBeNull();
    request.mockImplementation(async (country) => dataset(country));
    fireEvent.click(screen.getByRole("button", { name: "Reload / 重读" }));
    await waitFor(() => expect(screen.queryByRole("alert")).toBeNull());
    expect(screen.getByRole("status").textContent).toContain("1 models");
  });

  it("rereads after closing BOM maintenance and after authorization changes", async () => {
    const request = vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(dataset());
    const view = render(<MaterialTimelineBoard {...props} />);
    await expand();
    view.rerender(<MaterialTimelineBoard {...props} bomEditing />);
    expect(screen.queryByText("NEW")).toBeNull();
    request.mockResolvedValue(dataset("SE", [node("UPDATED")]));
    view.rerender(<MaterialTimelineBoard {...props} />);
    await waitFor(() => expect(screen.getByText("UPDATED")).toBeTruthy());
    request.mockResolvedValue(dataset("SE", []));
    view.rerender(<MaterialTimelineBoard {...props} authorizationKey="revoked-brand" />);
    expect(screen.queryByText("UPDATED")).toBeNull();
    await waitFor(() => expect(screen.getByText("No eligible products in this scope / 当前范围没有符合条件的产品。")).toBeTruthy());
  });
});
