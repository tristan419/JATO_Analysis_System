import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { api } from "../../api/client";

function createStorageMock() {
  const store = new Map<string, string>();
  return {
    getItem(key: string) {
      return store.has(key) ? store.get(key) ?? null : null;
    },
    setItem(key: string, value: string) {
      store.set(key, value);
    },
    removeItem(key: string) {
      store.delete(key);
    },
    clear() {
      store.clear();
    },
  };
}

describe("data management api", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", createStorageMock());
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
  });

  it("returns airflow action payloads on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            item: {
              action: "start",
              detail: "Airflow 本地栈已启动。",
              status: {
                available: true,
                mode: "running",
                detail: "running",
                uiUrl: "http://127.0.0.1:8080",
                running: true,
                runningServices: 3,
                totalServices: 3,
                updatedAt: "2026-04-18T08:00:00+00:00",
                services: [],
                actions: {
                  canStart: false,
                  canStop: true,
                  canOpenUi: true,
                },
              },
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.startAirflow()).resolves.toMatchObject({
      action: "start",
      detail: "Airflow 本地栈已启动。",
      status: {
        mode: "running",
      },
    });
  });

  it("returns voc sync payloads on success", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({
            item: {
              root: "04_Processed_data/voc",
              countryCount: 8,
              sourceRunCount: 24,
              documentCount: 46,
              errorCount: 1,
            },
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.syncVocRawToStore()).resolves.toMatchObject({
      countryCount: 8,
      sourceRunCount: 24,
      documentCount: 46,
    });
  });

  it("passes country filters to voc overview endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          item: {
            generatedAt: "2026-04-20T12:00:00+00:00",
            selectedCountryCode: "NO",
            selectedCountryLabel: "Norway / 挪威",
            availableCountries: [],
            overallMetrics: [],
            countryMetrics: [],
            artifacts: [],
            sourceRuns: [],
            observedSections: [],
            inferredSections: [],
            topPainPoints: [],
            topProductSignals: [],
            evidenceCards: [],
            documentation: [],
            staging: {
              databaseConnected: false,
              sourceRunCount: 0,
              documentCount: 0,
              publishReadyCount: 0,
              latestCollectedAt: null,
            },
          },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.getVocManagementOverview("NO")).resolves.toMatchObject({
      selectedCountryCode: "NO",
    });
    expect(fetchMock).toHaveBeenCalledWith(
      expect.stringContaining("/data-management/voc/overview?country=NO"),
      expect.any(Object),
    );
  });

  it("builds Hermes proposals URLs without duplicate question marks", async () => {
    const fetchMock = vi.fn().mockImplementation(() => Promise.resolve(
      new Response(JSON.stringify([]), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    ));
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.hermesProposals()).resolves.toEqual([]);
    expect(fetchMock).toHaveBeenLastCalledWith(
      expect.stringContaining("/hermes/proposals"),
      expect.any(Object),
    );
    expect(String(fetchMock.mock.calls.at(-1)?.[0])).not.toContain("??");

    await expect(api.hermesProposals("implemented")).resolves.toEqual([]);
    expect(fetchMock).toHaveBeenLastCalledWith(
      expect.stringContaining("/hermes/proposals?status=implemented"),
      expect.any(Object),
    );
    expect(String(fetchMock.mock.calls.at(-1)?.[0])).not.toContain("??status=implemented");
  });

  it("preserves conflict detail for airflow stop errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ detail: "Airflow 当前已经停止。" }),
          { status: 409, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.stopAirflow()).rejects.toThrow(
      "409 Airflow 当前已经停止。",
    );
  });

  it("preserves runtime detail for airflow start errors", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ detail: "postgres failed" }),
          { status: 500, headers: { "Content-Type": "application/json" } },
        ),
      ),
    );

    await expect(api.startAirflow()).rejects.toThrow("500 postgres failed");
  });
});
