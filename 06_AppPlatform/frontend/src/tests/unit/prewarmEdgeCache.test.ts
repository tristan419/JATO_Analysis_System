import { beforeAll, describe, expect, it } from "vitest";

interface PrewarmModule {
  buildAdvancedAnalysisWarmupRequests: (
    configuredCountries?: string[],
  ) => { body?: unknown; label: string; method: string; path: string }[];
  buildDefaultCascadePayloads: (
    snapshot: unknown,
    configuredCountries: string[],
    configuredPowertrains: string[],
    configuredSelections?: Record<string, string[]>,
  ) => { column: string; filters: Record<string, string[]> }[];
  buildDefaultFilterPayload: (
    snapshot: unknown,
    configuredCountries: string[],
    configuredPowertrains: string[],
    configuredSelections?: Record<string, string[]>,
  ) => { columns: Record<string, string>; filters: Record<string, string[]> };
  buildWarmupRequests: (
    snapshot: unknown,
    configuredCountries: string[],
    configuredPowertrains: string[],
    configuredSelections?: Record<string, string[]>,
  ) => { body?: unknown; label: string; method: string; path: string }[];
  mergeConfiguredSelections: (
    configuredCountries: string[],
    configuredPowertrains: string[],
    dashboardUrl: string,
  ) => Record<string, string[]>;
  parseDashboardFilterParams: (dashboardUrl: string) => Record<string, string[]>;
  resolveWarmupRoles: (options: {
    configuredRoles?: string[];
    explicitRole?: string;
    loginRole?: string;
    token?: string;
  }) => string[];
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
      "filters-options-dashboard-segment",
      "filters-options-batch",
      "filters-options-default-cascade",
      "analysis-overview-default",
      "time-series-grouped-month-country",
      "time-series-grouped-year-country",
    ]);
    expect(warmups[0]).toMatchObject({
      body: {
        items: [
          {
            column: "细分市场",
            filters: {},
          },
        ],
      },
      method: "POST",
      path: "/filters/options/batch",
    });
    expect(warmups[2]).toMatchObject({
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

  it("builds bounded Advanced Analysis metadata warmups", () => {
    expect(prewarm.buildAdvancedAnalysisWarmupRequests()).toEqual([
      {
        label: "advanced-analysis-countries",
        method: "GET",
        path: "/advanced-analysis/countries",
      },
      {
        label: "advanced-analysis-profile-options-瑞典",
        method: "GET",
        path: "/advanced-analysis/profile-options?country=%E7%91%9E%E5%85%B8",
      },
    ]);

    expect(prewarm.buildAdvancedAnalysisWarmupRequests(["德国", "瑞典"])).toEqual([
      {
        label: "advanced-analysis-countries",
        method: "GET",
        path: "/advanced-analysis/countries",
      },
      {
        label: "advanced-analysis-profile-options-德国",
        method: "GET",
        path: "/advanced-analysis/profile-options?country=%E5%BE%B7%E5%9B%BD",
      },
      {
        label: "advanced-analysis-profile-options-瑞典",
        method: "GET",
        path: "/advanced-analysis/profile-options?country=%E7%91%9E%E5%85%B8",
      },
    ]);
  });

  it("uses dashboard URL query params for exact edge prewarm payloads", () => {
    const dashboardUrl = "/dashboard?country=%E4%B8%B9%E9%BA%A6%2C%E5%BE%B7%E5%9B%BD&powertrain=BEV%2CPHEV&make=BYD&model=SEAL";
    const configuredSelections = prewarm.mergeConfiguredSelections([], [], dashboardUrl);

    expect(configuredSelections).toEqual({
      country: ["丹麦", "德国"],
      make: ["BYD"],
      model: ["SEAL"],
      powertrain: ["BEV", "PHEV"],
    });
    expect(prewarm.buildDefaultFilterPayload(snapshot, [], [], configuredSelections).filters).toEqual({
      "国家": ["丹麦", "德国"],
      "动总规整": ["BEV", "PHEV"],
      Make: ["BYD"],
      Model: ["SEAL"],
    });
    expect(prewarm.buildDefaultCascadePayloads(snapshot, [], [], configuredSelections)).toEqual([
      {
        column: "Make",
        filters: {
          "国家": ["丹麦", "德国"],
          "动总规整": ["BEV", "PHEV"],
        },
      },
      {
        column: "Model",
        filters: {
          "国家": ["丹麦", "德国"],
          "动总规整": ["BEV", "PHEV"],
          Make: ["BYD"],
        },
      },
      {
        column: "Version name",
        filters: {
          "国家": ["丹麦", "德国"],
          "动总规整": ["BEV", "PHEV"],
          Make: ["BYD"],
          Model: ["SEAL"],
        },
      },
    ]);
  });

  it("lets explicit countries and powertrains override dashboard URL params", () => {
    const dashboardUrl = "/dashboard?country=%E4%B8%B9%E9%BA%A6&powertrain=BEV&make=BYD";

    expect(prewarm.mergeConfiguredSelections(["西班牙"], ["ICE"], dashboardUrl)).toEqual({
      country: ["西班牙"],
      make: ["BYD"],
      powertrain: ["ICE"],
    });
  });

  it("warms all common role cache scopes when no token is available", () => {
    expect(prewarm.resolveWarmupRoles({})).toEqual([
      "viewer",
      "order_filler",
      "editor",
      "admin",
    ]);
  });

  it("uses the authenticated role only when a token is available", () => {
    expect(prewarm.resolveWarmupRoles({ loginRole: "order_filler", token: "token-a" })).toEqual([
      "order_filler",
    ]);
  });

  it("lets explicit role lists override the default warmup scopes", () => {
    expect(prewarm.resolveWarmupRoles({
      configuredRoles: ["viewer", "admin", "viewer"],
      loginRole: "order_filler",
      token: "token-a",
    })).toEqual(["viewer", "admin"]);
  });
});
