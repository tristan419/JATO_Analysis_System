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
  buildGroupedTimeSeriesWarmups: (
    filters: Record<string, string[]>,
    groupBys?: string[],
    shareSplitBy?: string[],
  ) => { body?: unknown; label: string; method: string; path: string }[];
  buildWarmupRequests: (
    snapshot: unknown,
    configuredCountries: string[],
    configuredPowertrains: string[],
    configuredSelections?: Record<string, string[]>,
    options?: { groupBys?: string[]; shareSplitBy?: string[] },
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
      "time-series-grouped-month-动总规整",
      "time-series-grouped-month-国家",
      "time-series-grouped-month-四驱占比",
      "time-series-grouped-month-四驱占比-segment",
      "time-series-grouped-month-四驱占比-powertrain",
      "time-series-grouped-month-Business/Private 占比",
      "time-series-grouped-month-Business/Private 占比-segment",
      "time-series-grouped-month-Business/Private 占比-powertrain",
      "time-series-grouped-year-动总规整",
      "time-series-grouped-year-国家",
      "time-series-grouped-year-四驱占比",
      "time-series-grouped-year-四驱占比-segment",
      "time-series-grouped-year-四驱占比-powertrain",
      "time-series-grouped-year-Business/Private 占比",
      "time-series-grouped-year-Business/Private 占比-segment",
      "time-series-grouped-year-Business/Private 占比-powertrain",
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
    expect(warmups[7]).toMatchObject({
      body: {
        filters: {
          "国家": ["丹麦", "德国"],
          "动总规整": ["ICE", "HEV", "BEV", "MHEV", "PHEV"],
        },
        grain: "month",
        group_by: "四驱占比",
        share_split_by: "segment",
        include_others: false,
        top_n: 10,
      },
      method: "POST",
      path: "/analysis/time-series-grouped",
    });
    expect(warmups[19]).toMatchObject({
      body: {
        filters: {
          "国家": ["丹麦", "德国"],
          "动总规整": ["ICE", "HEV", "BEV", "MHEV", "PHEV"],
        },
        grain: "year",
        group_by: "Business/Private 占比",
        share_split_by: "powertrain",
        include_others: false,
        top_n: 10,
      },
      method: "POST",
      path: "/analysis/time-series-grouped",
    });
  });

  it("lets grouped time-series prewarm scope be narrowed for targeted warmups", () => {
    expect(prewarm.buildGroupedTimeSeriesWarmups(
      { "国家": ["瑞典"], "动总规整": ["BEV"] },
      ["国家", "四驱占比"],
      ["segment", "bad", "powertrain"],
    )).toEqual([
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "month",
          group_by: "国家",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-month-国家",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "month",
          group_by: "四驱占比",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-month-四驱占比",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "month",
          group_by: "四驱占比",
          share_split_by: "segment",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-month-四驱占比-segment",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "month",
          group_by: "四驱占比",
          share_split_by: "powertrain",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-month-四驱占比-powertrain",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "year",
          group_by: "国家",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-year-国家",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "year",
          group_by: "四驱占比",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-year-四驱占比",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "year",
          group_by: "四驱占比",
          share_split_by: "segment",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-year-四驱占比-segment",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
      {
        body: {
          filters: { "国家": ["瑞典"], "动总规整": ["BEV"] },
          grain: "year",
          group_by: "四驱占比",
          share_split_by: "powertrain",
          include_others: false,
          top_n: 10,
        },
        label: "time-series-grouped-year-四驱占比-powertrain",
        method: "POST",
        path: "/analysis/time-series-grouped",
      },
    ]);
  });

  it("builds bounded Advanced Analysis warmups", () => {
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
      {
        body: {
          country: "瑞典",
          fuel_types: [],
          sales_mode: "month",
          scope_filters: [],
          top_n: 25,
        },
        label: "advanced-analysis-transfer-mart-瑞典",
        method: "POST",
        path: "/advanced-analysis/transfer-mart",
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
      {
        body: {
          country: "德国",
          fuel_types: [],
          sales_mode: "month",
          scope_filters: [],
          top_n: 25,
        },
        label: "advanced-analysis-transfer-mart-德国",
        method: "POST",
        path: "/advanced-analysis/transfer-mart",
      },
      {
        body: {
          country: "瑞典",
          fuel_types: [],
          sales_mode: "month",
          scope_filters: [],
          top_n: 25,
        },
        label: "advanced-analysis-transfer-mart-瑞典",
        method: "POST",
        path: "/advanced-analysis/transfer-mart",
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
