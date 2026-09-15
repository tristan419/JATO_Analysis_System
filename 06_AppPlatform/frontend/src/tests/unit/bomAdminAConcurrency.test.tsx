// @vitest-environment jsdom

import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
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
});
