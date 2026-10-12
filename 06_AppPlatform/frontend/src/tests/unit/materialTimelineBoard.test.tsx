// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
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
afterEach(() => { cleanup(); vi.restoreAllMocks(); props.onViewModel.mockClear(); });
async function expand() {
  await waitFor(() => expect(screen.getByRole("status").textContent).not.toContain("Loading"));
  fireEvent.click(screen.getByRole("button", { name: "View timeline / 查看时间轴" }));
}

describe("Model material changes board", () => {
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
    expect(screen.getByText("Relation not maintained / 关联未维护")).toBeTruthy();
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
    fireEvent.click(screen.getByRole("checkbox"));
    expect(screen.getByText("Saved relation / 已保存关联：OLD → MID → NEW")).toBeTruthy();
    expect(screen.queryByText("UNRELATED")).toBeNull();
    expect(screen.getByText("OLD")).toBeTruthy();
  });

  it("disables selection filtering until the full selection has loaded", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(dataset());
    render(<MaterialTimelineBoard {...props} selectionComplete={false} />);
    await expand();
    expect((screen.getByRole("checkbox") as HTMLInputElement).disabled).toBe(true);
  });

  it("does not call an incomplete selection empty and allows leaving that filter", async () => {
    vi.spyOn(api, "getMaterialTimeline").mockResolvedValue(dataset());
    const view = render(<MaterialTimelineBoard {...props} />);
    await expand();
    fireEvent.click(screen.getByRole("checkbox"));
    view.rerender(<MaterialTimelineBoard {...props} selectionComplete={false} />);
    expect(screen.getByRole("status").textContent).toContain("Selection loading");
    expect(screen.queryByText("No eligible products in this scope / 当前范围没有符合条件的产品。")).toBeNull();
    expect((screen.getByRole("checkbox") as HTMLInputElement).disabled).toBe(false);
    fireEvent.click(screen.getByRole("checkbox"));
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
