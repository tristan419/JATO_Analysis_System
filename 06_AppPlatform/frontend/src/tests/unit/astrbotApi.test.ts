import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { astrbotApiUrl, judgeExistingBusinessValidationRecords } from "../../features/astrbot/astrbotApi";

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

describe("astrbot api", () => {
  beforeEach(() => {
    vi.stubGlobal("localStorage", createStorageMock());
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  it("routes local AstrBot dev pages to their matching AstrBot backend port", () => {
    vi.stubEnv("VITE_API_BASE", "");
    vi.stubGlobal("window", {
      location: {
        hostname: "127.0.0.1",
        port: "5176",
        protocol: "http:",
      },
    });

    expect(astrbotApiUrl("/astrbot/eval/summary"))
      .toBe("http://127.0.0.1:8002/v1/astrbot/eval/summary");
  });

  it("judges existing business validation records without rerunning comparisons", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        JSON.stringify({
          status: "scored",
          category: "",
          limit: 30,
          latestPerQuestion: true,
          scoreReadyOnly: true,
          totalRecords: 30,
          candidateCount: 30,
          selectedCount: 30,
          attemptedCount: 30,
          judgedCount: 30,
          savedCount: 30,
          failedCount: 0,
          skippedCount: 0,
          statusCounts: { ok: 30 },
          results: [],
          summary: {
            replacementBaselineScoredCount: 30,
            pendingReplacementBaselineScoring: 0,
            replacementReadinessVerdict: "ready_for_limited_default_trial",
          },
        }),
        { status: 200, headers: { "Content-Type": "application/json" } },
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const response = await judgeExistingBusinessValidationRecords({
      limit: 30,
      latestPerQuestion: true,
      scoreReadyOnly: true,
    });

    expect(response.savedCount).toBe(30);
    expect(fetchMock).toHaveBeenCalledTimes(1);
    const requestUrl = new URL(String(fetchMock.mock.calls[0][0]), "http://localhost");
    expect(requestUrl.pathname).toBe("/v1/astrbot/eval/business/judge-existing");
    expect(fetchMock.mock.calls[0][1]).toMatchObject({
      method: "POST",
      body: JSON.stringify({
        category: undefined,
        limit: 30,
        latestPerQuestion: true,
        scoreReadyOnly: true,
      }),
    });
  });
});
