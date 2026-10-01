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

  it("requires a server-priced preview before restoring a cleared schedule", async () => {
    vi.spyOn(api, "getBomAdmin").mockResolvedValue(bomResponse("Restore sample"));
    vi.spyOn(api, "listBomTemplateFobPeriods")
      .mockResolvedValueOnce({ periods: [], usesPeriods: true })
      .mockResolvedValue({ periods: [], usesPeriods: false });
    const restore = vi.spyOn(api, "restoreBomTemplateDefaultFob")
      .mockResolvedValueOnce({ restored: false, baseFobEur: 13000 })
      .mockResolvedValue({ restored: true, baseFobEur: 13000 });
    const changed = vi.fn();
    await act(async () => { render(<BomAdminPanel onFobChanged={changed} />); });
    await act(async () => { fireEvent.click(screen.getByRole("button", { name: /OMODA Restore sample/ })); });
    await act(async () => { fireEvent.click(screen.getByText("13,600")); });
    await act(async () => { fireEvent.click(screen.getByText("Restore undated default FOB")); });
    expect(restore).toHaveBeenLastCalledWith("T-Restore sample-**", "SE", null);
    expect(screen.getByText(/Restore undated Single base 13,000 EUR/)).toBeTruthy();
    expect(changed).not.toHaveBeenCalled();
    await act(async () => { fireEvent.click(screen.getByText("Confirm restore default FOB")); });
    expect(restore).toHaveBeenLastCalledWith("T-Restore sample-**", "SE", 13000);
    expect(changed).toHaveBeenCalledOnce();
    expect(screen.getByText(/No periods configured/)).toBeTruthy();
  });
});
