// @vitest-environment jsdom

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

import {
  FILTER_SNAPSHOT_FALLBACK_TIMEOUT_MS,
  SharedFilterScopeProvider,
  createSharedSelections,
  getInitialCascadeStartIndex,
  loadInitialFilterMetadata,
  shouldSyncDashboardSearchToLocation,
  useSharedFilterScope,
} from "../../contexts/SharedFilterScopeContext";
import type { FilterOptionsPayload } from "../../utils/filterOptions";

const apiMock = vi.hoisted(() => ({
  columns: vi.fn(),
  filterMetadataSnapshot: vi.fn(),
  filterOptionsBatch: vi.fn(),
  overview: vi.fn(),
}));

vi.mock("../../api/client", () => ({
  api: apiMock,
}));

vi.mock("../../contexts/AuthContext", () => ({
  useAuth: () => ({ user: { primaryCountry: null } }),
}));

function SharedScopeProbe() {
  const location = useLocation();
  const scope = useSharedFilterScope();
  return createElement(
    "div",
    null,
    createElement("span", { "data-testid": "search" }, location.search),
    createElement("span", { "data-testid": "ready" }, String(scope.filtersReady)),
  );
}

async function flushEffects(): Promise<void> {
  await act(async () => {
    for (let index = 0; index < 8; index += 1) {
      await Promise.resolve();
    }
  });
}

describe("shouldSyncDashboardSearchToLocation", () => {
  it("allows URL sync on dashboard routes that share the global filters", () => {
    expect(shouldSyncDashboardSearchToLocation("/")).toBe(true);
    expect(shouldSyncDashboardSearchToLocation("/dashboard")).toBe(true);
    expect(shouldSyncDashboardSearchToLocation("/specification")).toBe(true);
    expect(shouldSyncDashboardSearchToLocation("/data/spec-detail")).toBe(true);
  });

  it("prevents URL sync on self-managed routes like market scan and country copilot", () => {
    expect(shouldSyncDashboardSearchToLocation("/market-scan")).toBe(false);
    expect(shouldSyncDashboardSearchToLocation("/copilot")).toBe(false);
  });
});

describe("getInitialCascadeStartIndex", () => {
  it("skips the powertrain request when earlier filters use the full snapshot scope", () => {
    expect(getInitialCascadeStartIndex(createSharedSelections({
      country: ["丹麦", "德国"],
      powertrain: ["ICE", "BEV"],
    }), {
      country: ["丹麦", "德国"],
      body_type: ["SUV"],
      segment: ["C"],
      powertrain: ["ICE", "BEV"],
    })).toBe(4);
  });

  it("keeps powertrain validation when a URL narrows earlier filters", () => {
    expect(getInitialCascadeStartIndex(createSharedSelections({
      country: ["丹麦"],
      powertrain: ["ICE"],
    }), {
      country: ["丹麦", "德国"],
      powertrain: ["ICE", "BEV"],
    })).toBe(3);
  });
});

describe("SharedFilterScopeProvider boot", () => {
  beforeEach(() => {
    localStorage.clear();
    apiMock.filterMetadataSnapshot.mockResolvedValue({
      columns: ["国家", "Body type", "细分市场", "动总规整", "Make", "Model", "Version name"],
      options: {
        国家: ["丹麦"],
        "Body type": ["SUV"],
        细分市场: ["C"],
        动总规整: ["ICE", "BEV"],
      },
    });
    apiMock.filterOptionsBatch.mockImplementation((items: { column: string }[]) => {
      return Promise.resolve({
        items: items.map((item) => ({
          column: item.column,
          options: item.column === "动总规整" ? ["ICE", "BEV"] : [],
        })),
      });
    });
    apiMock.columns.mockResolvedValue({
      items: ["国家", "Body type", "细分市场", "动总规整", "Make", "Model", "Version name"],
    });
    apiMock.overview.mockResolvedValue({
      kpis: {
        totalRows: 12,
        brandCount: 2,
        modelCount: 3,
        versionCount: 4,
        cumulativeSales: 1200,
      },
      yearSeries: [{ time: "2025", value: 1200 }],
      monthSeries: [{ time: "2025-01", value: 100 }],
    });
  });

  afterEach(() => {
    cleanup();
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it("keeps the overview request after boot syncs filters into the URL", async () => {
    render(
      createElement(
        MemoryRouter,
        { initialEntries: ["/dashboard"] },
        createElement(
          Routes,
          null,
          createElement(Route, {
            path: "/dashboard",
            element: createElement(
              SharedFilterScopeProvider,
              null,
              createElement(SharedScopeProbe),
            ),
          }),
        ),
      ),
    );

    await flushEffects();

    expect(apiMock.filterMetadataSnapshot).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId("ready").textContent).toBe("true");
    expect(screen.getByTestId("search").textContent).toContain("country=");

    await waitFor(() => expect(apiMock.overview).toHaveBeenCalledTimes(1));
    expect(apiMock.filterOptionsBatch).toHaveBeenCalledTimes(1);
    expect(apiMock.filterOptionsBatch.mock.calls[0]?.[0]).toEqual([
      expect.objectContaining({ column: "Make" }),
    ]);
    expect(apiMock.overview).toHaveBeenCalledWith({
      filters: {
        国家: ["丹麦"],
        动总规整: ["ICE", "BEV"],
      },
      prefer_precomputed: true,
      top_n: 120,
    }, expect.objectContaining({ signal: expect.any(AbortSignal) }));
  });

  it("falls back to columns when the filter snapshot is too slow", async () => {
    vi.useFakeTimers();
    apiMock.filterMetadataSnapshot.mockImplementation((init?: RequestInit) => (
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => {
          reject(new DOMException("Aborted", "AbortError"));
        }, { once: true });
      })
    ));
    const loadBatch = vi.fn(async (items: FilterOptionsPayload[]) => (
      items.map((item) => (
        item.column === "国家" ? ["丹麦"]
          : item.column === "Body type" ? ["SUV"]
            : item.column === "细分市场" ? ["C"]
              : item.column === "动总规整" ? ["ICE", "BEV"]
                : []
      ))
    ));

    const metadataPromise = loadInitialFilterMetadata(loadBatch);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(FILTER_SNAPSHOT_FALLBACK_TIMEOUT_MS + 1);
    });

    await expect(metadataPromise).resolves.toMatchObject({
      columns: ["国家", "Body type", "细分市场", "动总规整", "Make", "Model", "Version name"],
      topLevelOptions: {
        country: ["丹麦"],
        body_type: ["SUV"],
        segment: ["C"],
        powertrain: ["ICE", "BEV"],
      },
    });
    expect(apiMock.columns).toHaveBeenCalledTimes(1);
    expect(loadBatch).toHaveBeenCalledTimes(1);
    vi.useRealTimers();
  });
});
