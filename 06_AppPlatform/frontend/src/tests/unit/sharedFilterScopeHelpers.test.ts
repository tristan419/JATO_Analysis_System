// @vitest-environment jsdom

import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { createElement } from "react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes, useLocation } from "react-router-dom";

import {
  SharedFilterScopeProvider,
  shouldSyncDashboardSearchToLocation,
  useSharedFilterScope,
} from "../../contexts/SharedFilterScopeContext";

const apiMock = vi.hoisted(() => ({
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
    expect(apiMock.overview).toHaveBeenCalledWith({
      filters: {
        国家: ["丹麦"],
        动总规整: ["ICE", "BEV"],
      },
      prefer_precomputed: true,
      top_n: 120,
    }, expect.objectContaining({ signal: expect.any(AbortSignal) }));
  });
});
