import { beforeAll, describe, expect, it } from "vitest";

interface PrewarmModule {
  buildDefaultCascadePayloads: (
    snapshot: unknown,
    configuredCountries: string[],
    configuredPowertrains: string[],
  ) => { column: string; filters: Record<string, string[]> }[];
  buildWarmupRequests: (
    snapshot: unknown,
    configuredCountries: string[],
    configuredPowertrains: string[],
  ) => { body?: unknown; label: string; method: string; path: string }[];
}

let prewarm: PrewarmModule;

beforeAll(async () => {
  prewarm = await import("../../../scripts/prewarm_intl_edge_cache.cjs") as unknown as PrewarmModule;
});

const snapshot = {
  columns: [
    "国家",
    "Body type",
    "细分市场",
    "动总规整",
    "Make",
    "Model",
    "Version name",
  ],
  options: {
    "国家": ["丹麦", "德国"],
    "Body type": ["SUV", "Hatchback"],
    "细分市场": ["C", "D"],
    "动总规整": ["ICE", "HEV", "BEV", "MHEV", "PHEV", "EV"],
  },
};

describe("prewarm intl edge cache", () => {
  it("builds the default dashboard cascade batch body from the filter snapshot", () => {
    expect(prewarm.buildDefaultCascadePayloads(snapshot, [], [])).toEqual([
      {
        column: "Make",
        filters: {
          "国家": ["丹麦", "德国"],
          "动总规整": ["ICE", "HEV", "BEV", "MHEV", "PHEV"],
        },
      },
    ]);
  });

  it("includes both fallback top-level and default cascade filter warmups", () => {
    const warmups = prewarm.buildWarmupRequests(snapshot, [], []);

    expect(warmups.map((request) => request.label)).toEqual([
      "filters-options-batch",
      "filters-options-default-cascade",
      "analysis-overview-default",
      "time-series-grouped-month-country",
      "time-series-grouped-year-country",
    ]);
    expect(warmups[1]).toMatchObject({
      body: {
        items: [
          {
            column: "Make",
            filters: {
              "国家": ["丹麦", "德国"],
              "动总规整": ["ICE", "HEV", "BEV", "MHEV", "PHEV"],
            },
          },
        ],
      },
      method: "POST",
      path: "/filters/options/batch",
    });
  });
});
