// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen, within } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";
import { BomAdminPanel } from "../../pages/OrderGeniusPage";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
};

function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((nextResolve) => {
    resolve = nextResolve;
  });
  return { promise, resolve };
}

function bomResponse(modelName: string) {
  return {
    items: [{
      materialCode: `T-${modelName}`,
      bomTemplate: `T-${modelName}-**`,
      brand: "OMODA",
      modelName,
      version: "Comfort-FWD",
      powertrain: "BEV",
      colour: "Water blue",
      colourCode: "W3",
      colourType: "single",
      colourTier: "single",
      colourHex: "#94A3B8",
      interiorColorName: "Black-Black",
      lifecycleStatus: "active",
      fobByCountry: { SE: { finalFobEur: 13600 } },
      rowVersion: 1,
    }],
    countries: ["SE"],
    activeFobCountries: ["SE"],
  };
}

const colourRuleSummary = {
  totalRules: 0,
  fillable: 0,
  missing: 0,
  nameConflict: 0,
  swatchConflict: 0,
  complete: 0,
  fillableSkus: 0,
  invalidIdentitySkuCount: 0,
  invalidIdentitySampleMaterialCodes: [],
};

describe("BOM Admin A load continuity", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    localStorage.clear();
    vi.spyOn(api, "getAccountCountryOptions").mockResolvedValue({ items: [] });
    vi.spyOn(api, "getOrderGeniusColourSurcharges").mockResolvedValue({ items: [] });
    vi.spyOn(api, "getOrderGeniusSpecialColourSurcharges").mockResolvedValue({ items: [] });
    vi.spyOn(api, "getOrderGeniusColourHexRules").mockResolvedValue({
      items: [],
      summary: colourRuleSummary,
    });
  });

  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("keeps only the final A→B→A search intent and refreshes that key", async () => {
    const requests: Array<{ params: unknown; deferred: Deferred<any> }> = [];
    vi.spyOn(api, "getBomAdmin").mockImplementation((params) => {
      const request = { params, deferred: deferred<any>() };
      requests.push(request);
      return request.deferred.promise;
    });

    render(<BomAdminPanel />);
    expect(requests).toHaveLength(1);
    await act(async () => {
      requests[0].deferred.resolve(bomResponse("Initial"));
      await requests[0].deferred.promise;
    });

    const input = screen.getByPlaceholderText(/Search model/);
    await act(async () => {
      fireEvent.change(input, { target: { value: "alpha" } });
      fireEvent.keyDown(input, { key: "Enter" });
      fireEvent.change(input, { target: { value: "beta" } });
      fireEvent.keyDown(input, { key: "Enter" });
      fireEvent.change(input, { target: { value: "alpha" } });
      fireEvent.keyDown(input, { key: "Enter" });
    });

    expect(requests.map((request) => request.params)).toEqual([
      undefined,
      { search: "alpha" },
    ]);

    await act(async () => {
      requests[1].deferred.resolve(bomResponse("Alpha"));
      await requests[1].deferred.promise;
      await Promise.resolve();
    });

    expect(requests.map((request) => request.params)).toEqual([
      undefined,
      { search: "alpha" },
      { search: "alpha" },
    ]);
    expect(requests.some((request) => JSON.stringify(request.params) === JSON.stringify({ search: "beta" }))).toBe(false);

    await act(async () => {
      requests[2].deferred.resolve(bomResponse("Alpha refreshed"));
      await requests[2].deferred.promise;
    });
  });

  it("does not let a stale response paint over the newer search", async () => {
    const requests: Array<{ params: unknown; deferred: Deferred<any> }> = [];
    vi.spyOn(api, "getBomAdmin").mockImplementation((params) => {
      const request = { params, deferred: deferred<any>() };
      requests.push(request);
      return request.deferred.promise;
    });

    render(<BomAdminPanel />);
    await act(async () => {
      requests[0].deferred.resolve(bomResponse("Initial"));
      await requests[0].deferred.promise;
    });

    const input = screen.getByPlaceholderText(/Search model/);
    await act(async () => {
      fireEvent.change(input, { target: { value: "alpha" } });
      fireEvent.keyDown(input, { key: "Enter" });
      fireEvent.change(input, { target: { value: "beta" } });
      fireEvent.keyDown(input, { key: "Enter" });
    });

    await act(async () => {
      requests[1].deferred.resolve(bomResponse("Stale Alpha"));
      await requests[1].deferred.promise;
      await Promise.resolve();
    });
    expect(requests).toHaveLength(3);
    expect(requests[2].params).toEqual({ search: "beta" });

    await act(async () => {
      requests[2].deferred.resolve(bomResponse("Fresh Beta"));
      await requests[2].deferred.promise;
    });
    expect(document.body.textContent).toContain("Fresh Beta");
    expect(document.body.textContent).not.toContain("Stale Alpha");
  });

  it("shows the backend colour decision without substituting a frontend default", async () => {
    const response = bomResponse("Pricing sample");
    response.items[0].colourTier = "dual";
    const items = response.items.map((item) => ({
      ...item, colourPricing: { status: "matched_amount", amount: 475, source: "model_colour" },
    }));
    vi.spyOn(api, "getBomAdmin").mockResolvedValue({ ...response, items });
    vi.spyOn(api, "getOrderGeniusSpecialColourSurcharges").mockResolvedValue({ items: [] });
    await act(async () => { render(<BomAdminPanel />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /OMODA Pricing sample/ })); });
    expect(screen.getByTitle(/dual · rule \+475€/)).toBeTruthy();
    expect(screen.queryByTitle(/dual · rule \+200€/)).toBeNull();
  });

  it("saves product fields once and waits for BOM and matrix readback without inventing a version", async () => {
    const response = bomResponse("Product sample");
    const refreshed = { ...response, items: response.items.map((item) => ({ ...item, version: "7 seats", powertrain: "HEV" })) };
    const readback = deferred<typeof response>();
    const matrixReadback = deferred<void>();
    vi.spyOn(api, "getBomAdmin").mockResolvedValueOnce(response).mockReturnValueOnce(readback.promise).mockResolvedValue(refreshed);
    const save = vi.spyOn(api, "updateSkuMetadata").mockResolvedValue({
      materialCodes: ["T-Product sample"], updated: 1,
      productFields: { brand: "OMODA", modelName: "Product sample", version: "7 seats", powertrain: "HEV" },
    });
    const remarkSave = vi.spyOn(api, "updateSkuRemark");
    const changed = vi.fn(() => matrixReadback.promise);
    await act(async () => { render(<BomAdminPanel onFobChanged={changed} />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /OMODA Product sample/ })); });
    fireEvent.click(screen.getByRole("button", { name: "Edit" }));
    fireEvent.change(screen.getByPlaceholderText("Version"), { target: { value: "7 seats" } });
    await act(async () => { fireEvent.click(screen.getByText("Save Changes")); });
    expect(save).toHaveBeenCalledTimes(1);
    expect(save).toHaveBeenCalledWith("T-Product sample", expect.objectContaining({ rowVersions: { "T-Product sample": 1 }, remark: "" }));
    expect(remarkSave).not.toHaveBeenCalled();
    expect(screen.getByText("Saving...").hasAttribute("disabled")).toBe(true);
    expect(screen.queryByText("Saved product fields.")).toBeNull();
    await act(async () => { readback.resolve(refreshed); await readback.promise; });
    expect(changed).toHaveBeenCalledOnce();
    expect(screen.getByText("Saving...").hasAttribute("disabled")).toBe(true);
    await act(async () => { matrixReadback.resolve(); await matrixReadback.promise; });
    expect(screen.getAllByText("Saved product fields.").length).toBeGreaterThan(0);
    expect((screen.getByPlaceholderText("Version") as HTMLInputElement).value).toBe("7 seats");
    expect(document.body.textContent).toContain("HEV");
    await act(async () => { fireEvent.click(screen.getByText("Save Changes")); });
    expect(save).toHaveBeenLastCalledWith("T-Product sample", expect.objectContaining({ rowVersions: { "T-Product sample": 1 } }));
  });

  it("opens country periods outside the BOM card and keeps the draft after a save failure", async () => {
    vi.spyOn(api, "getBomAdmin").mockResolvedValue(bomResponse("Period sample"));
    vi.spyOn(api, "listBomTemplateFobPeriods").mockResolvedValue({ periods: [], usesPeriods: false });
    const save = vi.spyOn(api, "saveBomTemplateFobPeriod").mockRejectedValue(new Error("Country price conflict"));
    await act(async () => { render(<BomAdminPanel />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /OMODA Period sample/ })); });
    await act(async () => { fireEvent.click(screen.getByText("13,600")); });
    const dialog = screen.getByRole("dialog", { name: "Country template FOB periods" });
    expect(dialog.closest(".bom-admin-panel")).toBeNull();
    expect(api.listBomTemplateFobPeriods).toHaveBeenCalledWith({ bomTemplate: "T-Period sample-**", countryCode: "SE" });
    fireEvent.change(within(dialog).getByLabelText("From"), { target: { value: "2026-08-01" } });
    fireEvent.change(within(dialog).getByLabelText("To"), { target: { value: "2026-08-31" } });
    const base = within(dialog).getByLabelText("Single base EUR") as HTMLInputElement;
    fireEvent.change(base, { target: { value: "14000" } });
    await act(async () => { fireEvent.click(within(dialog).getByText("Add period")); });
    expect(save).toHaveBeenCalledWith(expect.objectContaining({ countryCode: "SE", bomTemplate: "T-Period sample-**", validFrom: "2026-08-01", validTo: "2026-08-31", baseFobEur: 14000 }));
    expect(base.value).toBe("14000");
    expect(within(dialog).getByText("Country price conflict")).toBeTruthy();
    fireEvent.keyDown(dialog, { key: "Escape" });
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("shows latest-start Single base with Pn without changing the default input", async () => {
    const response = bomResponse("Period summary");
    const periods = [
      { periodId: "past", countryCode: "SE", bomTemplate: "T-Period summary-**", validFrom: "2026-07-01", validTo: "2026-08-31", baseFobEur: 14000, remark: null, rowVersion: 1 },
      { periodId: "future", countryCode: "SE", bomTemplate: "T-Period summary-**", validFrom: "2027-01-01", validTo: null, baseFobEur: 15000, remark: null, rowVersion: 1 },
    ];
    vi.spyOn(api, "getBomAdmin").mockResolvedValue({ ...response, items: response.items.map((item) => ({ ...item, fobPeriodsByCountry: { SE: periods } })) });
    vi.spyOn(api, "listBomTemplateFobPeriods").mockResolvedValue({ periods, usesPeriods: true });
    await act(async () => { render(<BomAdminPanel />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /OMODA Period summary/ })); });
    expect(screen.getByText("P2")).toBeTruthy();
    const price = screen.getByText("15,000");
    expect(price.closest("td")?.title).toContain("Undated default: 13,600");
    expect(price.closest("td")?.title).toContain("Management summary, not a quote for today");
    await act(async () => { fireEvent.click(price); });
    expect((screen.getByLabelText("Base FOB EUR") as HTMLInputElement).value).toBe("13600");
    expect(document.body.textContent).toContain("T-Period summary-**");
  });

  it("confirms the server-priced default once before deleting the last period", async () => {
    vi.spyOn(api, "getBomAdmin").mockResolvedValue(bomResponse("Restore sample"));
    vi.spyOn(api, "listBomTemplateFobPeriods")
      .mockResolvedValueOnce({ periods: [{ periodId: "p1", countryCode: "SE", bomTemplate: "T-Restore sample-**", validFrom: "2026-08-01", validTo: "2026-08-31", baseFobEur: 14000, remark: null, rowVersion: 1 }], usesPeriods: true })
      .mockResolvedValue({ periods: [], usesPeriods: false });
    const impact = { periodId: "p1", fingerprint: "fp", lastPeriod: true, defaultBaseFobEur: 13000 };
    const remove = vi.spyOn(api, "deleteBomTemplateFobPeriod")
      .mockResolvedValueOnce({ ...impact, deleted: false })
      .mockResolvedValue({ ...impact, deleted: true });
    const changed = vi.fn();
    await act(async () => { render(<BomAdminPanel onFobChanged={changed} />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /OMODA Restore sample/ })); });
    await act(async () => { fireEvent.click(screen.getByText("13,600")); });
    await act(async () => { fireEvent.click(within(screen.getByRole("dialog")).getByText("Delete")); });
    expect(remove).toHaveBeenLastCalledWith("p1", 1, undefined);
    expect(screen.getByText(/undated Single base 13,000 EUR/)).toBeTruthy();
    expect(changed).not.toHaveBeenCalled();
    await act(async () => { fireEvent.click(screen.getByText("Confirm remove period")); });
    expect(remove).toHaveBeenLastCalledWith("p1", 1, "fp");
    expect(changed).toHaveBeenCalledOnce();
    expect(screen.getByText(/No periods configured/)).toBeTruthy();
  });

  it("searches the target country's rows and renders only NL plus that country; Clear restores all columns", async () => {
    const response = bomResponse("Country columns");
    const item = { ...response.items[0], fobByCountry: { NL: { finalFobEur: 12000 }, CH: { finalFobEur: 13600 }, SE: { finalFobEur: 14000 } } };
    const getBom = vi.spyOn(api, "getBomAdmin").mockImplementation(async (params) => ({
      ...response, items: [item], countries: params?.country ? ["NL", params.country] : ["NL", "CH", "SE"],
      activeFobCountries: ["NL", "CH", "SE"],
    }));
    await act(async () => { render(<BomAdminPanel />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /OMODA Country columns/ })); });
    const input = screen.getByPlaceholderText(/Search model/);
    await act(async () => {
      fireEvent.change(input, { target: { value: "CH" } });
      fireEvent.keyDown(input, { key: "Enter" });
    });
    expect(getBom).toHaveBeenLastCalledWith({ country: "CH" });
    expect(screen.getAllByRole("columnheader").map((cell) => cell.textContent).filter((text) => /^(NL|CH|SE)/.test(text || ""))).toEqual(["NL", "CH"]);
    expect(screen.getByText("12,000")).toBeTruthy();
    expect(screen.queryByText("14,000")).toBeNull();
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /^Clear$/ })); });
    expect(screen.getAllByRole("columnheader").map((cell) => cell.textContent).filter((text) => /^(NL|CH|SE)/.test(text || ""))).toEqual(["NL", "CH", "SE"]);
  });
});
