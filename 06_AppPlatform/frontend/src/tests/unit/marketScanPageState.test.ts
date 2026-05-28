import { describe, expect, it } from "vitest";

import type {
  MarketScanDeckResponse,
  MarketScanDelta,
  MarketScanDrilldownPage,
  MarketScanOriginPage,
  MarketScanOverviewPage,
  MarketScanOverviewTrendItem,
  MarketScanRankingItem,
  MarketScanSegmentPage,
} from "../../types";
import {
  buildDefaultMarketScanSlideLayouts,
  buildMarketScanSlideFitAssessment,
  resetMarketScanActiveSlideLayout,
  toggleMarketScanFuelSelection,
  toggleMarketScanSlideEditModeState,
  updateMarketScanActiveSlideLayout,
} from "../../utils/marketScanPageState";

function makeDelta(
  value: number,
  tone: string = value > 0 ? "positive" : value < 0 ? "negative" : "neutral",
): MarketScanDelta {
  const sign = value > 0 ? "+" : value < 0 ? "" : "";
  return {
    value,
    display: `${sign}${(value * 100).toFixed(1)}%`,
    tone,
  };
}

function makeOverviewTrendItem(
  period: string,
  totalVolume: number,
  label: string,
  fuelMix: Record<string, number>,
): MarketScanOverviewTrendItem {
  return {
    period,
    label,
    totalVolume,
    fuelMix,
    suvTotalVolume: totalVolume,
    suvFuelMix: fuelMix,
    mom: makeDelta(0.02),
    yoy: makeDelta(0.04),
  };
}

function makeRankingItem(
  model: string,
  sharePct: number,
  rank: number,
): MarketScanRankingItem {
  return {
    rank,
    model,
    volume: 1000 - rank * 50,
    sharePct,
    shareDisplay: `${(sharePct * 100).toFixed(1)}%`,
    yoy: makeDelta(0.05),
    mom: makeDelta(0.01),
    barPct: 1 - rank * 0.08,
  };
}

function makeOverviewPage(): MarketScanOverviewPage {
  const items = [
    makeOverviewTrendItem("2026-01", 8000, "26.01", { ICE: 3000, HEV: 2600, BEV: 2400 }),
    makeOverviewTrendItem("2026-02", 8200, "26.02", { ICE: 3000, HEV: 2700, BEV: 2500 }),
    makeOverviewTrendItem("2026-03", 8400, "26.03", { ICE: 3000, HEV: 2800, BEV: 2600 }),
  ];
  return {
    summary: {
      headline: "headline",
      subheadline: "subheadline",
      currentMonthVolume: 8400,
      currentMonthYoY: makeDelta(0.04),
      rolling12Volume: 104000,
      rolling12YoY: makeDelta(0.06),
      ytdVolume: 24600,
      ytdYoY: makeDelta(0.05),
    },
    trend: {
      periods: items.map((item) => item.period),
      items,
    },
    monthlyBrandRanking: {
      title: "Monthly",
      items: [
        makeRankingItem("VOLVO", 0.2, 1),
        makeRankingItem("BMW", 0.16, 2),
        makeRankingItem("VW", 0.12, 3),
      ],
    },
    ytdBrandRanking: {
      title: "YTD",
      items: [
        makeRankingItem("VOLVO", 0.21, 1),
        makeRankingItem("BMW", 0.17, 2),
        makeRankingItem("VW", 0.11, 3),
      ],
    },
    rolling12BrandRanking: {
      title: "Rolling 12M",
      items: [
        makeRankingItem("VOLVO", 0.22, 1),
        makeRankingItem("BMW", 0.16, 2),
        makeRankingItem("VW", 0.12, 3),
      ],
    },
  };
}

function makeOriginPage(): MarketScanOriginPage {
  return {
    summaryText: "origin summary",
    trend: {
      series: [
        {
          origin: "欧系",
          points: [
            { period: "2026-03", label: "26.03", volume: 4000, sharePct: 0.4 },
            { period: "2026-04", label: "26.04", volume: 4200, sharePct: 0.42 },
          ],
        },
      ],
    },
    brandTrend: {
      groups: [
        {
          origin: "欧系",
          series: [
            {
              brand: "VOLVO",
              points: [
                { period: "2026-03", label: "26.03", volume: 1000, sharePct: 0.25 },
                { period: "2026-04", label: "26.04", volume: 1100, sharePct: 0.26 },
              ],
            },
          ],
        },
      ],
    },
    matrix: {
      columns: ["欧系", "中系"],
      rows: [
        {
          metricKey: "current_volume",
          label: "当月销量",
          cells: [
            { key: "欧系", value: 4200, display: "4,200", tone: "positive" },
            { key: "中系", value: 2400, display: "2,400", tone: "positive" },
          ],
        },
      ],
    },
  };
}

function makeSegmentPage(): MarketScanSegmentPage {
  return {
    summaryText: "segment summary",
    matrix: {
      columns: ["SUV-A0", "SUV-A"],
      rows: [
        {
          metricKey: "current_volume",
          label: "当月销量",
          cells: [
            { key: "SUV-A0", value: 2800, display: "2,800", tone: "positive" },
            { key: "SUV-A", value: 2200, display: "2,200", tone: "positive" },
          ],
        },
      ],
    },
    bodyShareTrend: {
      items: [
        { period: "2026-03", label: "26.03", totalVolume: 9600, suvSharePct: 0.6, sedanSharePct: 0.4 },
        { period: "2026-04", label: "26.04", totalVolume: 9800, suvSharePct: 0.62, sedanSharePct: 0.38 },
      ],
    },
    suvSegmentShareTrend: {
      items: [
        {
          period: "2026-04",
          label: "26.04",
          totalVolume: 9800,
          segmentSharePct: {
            "SUV-A00": 0.12,
            "SUV-A0": 0.29,
            "SUV-A": 0.21,
            "≥SUV-B": 0.1,
          },
        },
      ],
    },
  };
}

function makeDrilldownPage({
  rankingCount,
  fuelPanelCount,
  trendCount,
}: {
  rankingCount: number;
  fuelPanelCount: number;
  trendCount: number;
}): MarketScanDrilldownPage {
  return {
    segment: "SUV A0",
    segmentLabel: "SUV-A0",
    title: "SUV-A0",
    summaryText: "drilldown summary",
    monthTotalRanking: {
      title: "Monthly Ranking",
      items: Array.from({ length: rankingCount }, (_, index) =>
        makeRankingItem(`MONTH-MODEL-${index + 1}`, 0.16 - index * 0.003, index + 1),
      ),
    },
    totalRanking: {
      title: "Ranking",
      items: Array.from({ length: rankingCount }, (_, index) =>
        makeRankingItem(`MODEL-${index + 1}`, 0.18 - index * 0.003, index + 1),
      ),
    },
    rolling12TotalRanking: {
      title: "Rolling 12M Ranking",
      items: Array.from({ length: rankingCount }, (_, index) =>
        makeRankingItem(`ROLLING-MODEL-${index + 1}`, 0.2 - index * 0.003, index + 1),
      ),
    },
    monthFuelTrend: {
      items: Array.from({ length: trendCount }, (_, index) => ({
        label: `25.${String(index + 1).padStart(2, "0")}`,
        totalVolume: 2400 + index * 120,
        fuelMix: { ICE: 1000, HEV: 700, BEV: 780 },
      })),
    },
    rolling12FuelTrend: {
      items: Array.from({ length: trendCount }, (_, index) => ({
        label: `L12M 26.${String(index + 1).padStart(2, "0")}`,
        totalVolume: 12000 + index * 220,
        fuelMix: { ICE: 4200, HEV: 3200, BEV: 3800 },
      })),
    },
    ytdFuelTrend: {
      items: Array.from({ length: trendCount }, (_, index) => ({
        label: `26.${String(index + 1).padStart(2, "0")}`,
        totalVolume: 10000 + index * 200,
        fuelMix: { ICE: 4000, HEV: 3000, BEV: 3200 },
      })),
    },
    fuelPanels: Array.from({ length: fuelPanelCount }, (_, index) => ({
      fuelType: `Fuel-${index + 1}`,
      monthTitle: `Fuel-${index + 1} 26.04`,
      rolling12Title: `Fuel-${index + 1} 近12个月 · 截至 26.04`,
      ytdTitle: `Fuel-${index + 1} YTD`,
      monthRanking: [makeRankingItem(`MONTH-${index + 1}`, 0.1, 1)],
      rolling12Ranking: [makeRankingItem(`ROLLING-${index + 1}`, 0.13, 1)],
      ytdRanking: Array.from({ length: rankingCount }, (_, rankIndex) =>
        makeRankingItem(`FUEL-${index + 1}-${rankIndex + 1}`, 0.12, rankIndex + 1),
      ),
    })),
  };
}

function makeDeck(): MarketScanDeckResponse {
  const drilldown = makeDrilldownPage({
    rankingCount: 6,
    fuelPanelCount: 3,
    trendCount: 6,
  });
  return {
    metadata: {
      protocolVersion: "v1",
      requestedPeriod: null,
      resolvedPeriod: "2026-04",
      latestPeriod: "2026-04",
      priorPeriod: "2026-03",
      sameMonthLastYearPeriod: "2025-04",
      selectedCountry: "瑞典",
      selectedCountryLabel: "Sweden",
      selectedFuelTypes: ["ICE", "HEV", "BEV"],
      selectedDrilldownSegments: ["SUV A0"],
      selectedBodyTypes: [],
      availableCountries: [{ value: "瑞典", label: "Sweden" }],
      availablePeriods: [{ value: "2026-04", label: "2026-04" }],
      availableFuelTypes: ["ICE", "HEV", "BEV"],
      availableSegments: [{ value: "SUV A0", label: "SUV-A0" }],
      availableBodyTypes: [],
      labels: {
        pageTitle: "Market Scan Deck",
        currentMonthShort: "26.04",
        previousMonthShort: "26.03",
        sameMonthLastYearShort: "25.04",
        currentYtd: "26 YTD",
        priorYtd: "25 YTD",
        ytdWindow: "1-4月",
      },
    },
    results: {
      overview: makeOverviewPage(),
      origin: makeOriginPage(),
      segment: makeSegmentPage(),
      drilldown,
      suvAll: drilldown,
      suvA: drilldown,
      suvB: drilldown,
    },
  };
}

describe("toggleMarketScanSlideEditModeState", () => {
  it("forces the export drawer open when entering edit mode", () => {
    expect(
      toggleMarketScanSlideEditModeState({
        slideEditMode: false,
        exportToolsOpen: false,
      }),
    ).toEqual({
      slideEditMode: true,
      exportToolsOpen: true,
    });
  });

  it("keeps the existing drawer state when leaving edit mode", () => {
    expect(
      toggleMarketScanSlideEditModeState({
        slideEditMode: true,
        exportToolsOpen: true,
      }),
    ).toEqual({
      slideEditMode: false,
      exportToolsOpen: true,
    });
  });
});

describe("market scan slide layout state", () => {
  it("updates only the active page layout", () => {
    const layouts = buildDefaultMarketScanSlideLayouts();
    const updated = updateMarketScanActiveSlideLayout(layouts, "segment", {
      paddingX: 40,
      contentGap: 999,
    });

    expect(updated.segment.paddingX).toBe(40);
    expect(updated.segment.contentGap).toBe(24);
    expect(updated.overview).toEqual(layouts.overview);
  });

  it("resets only the active page layout back to defaults", () => {
    const layouts = updateMarketScanActiveSlideLayout(
      buildDefaultMarketScanSlideLayouts(),
      "segment",
      { paddingX: 40 },
    );
    const reset = resetMarketScanActiveSlideLayout(layouts, "segment");

    expect(reset.segment).toEqual(layouts.overview);
    expect(reset.origin).toEqual(layouts.origin);
  });
});

describe("toggleMarketScanFuelSelection", () => {
  it("keeps at least one active fuel chip", () => {
    expect(toggleMarketScanFuelSelection(["BEV"], "BEV")).toEqual(["BEV"]);
  });

  it("adds or removes fuels when more than one option is active", () => {
    expect(toggleMarketScanFuelSelection(["ICE", "BEV"], "ICE")).toEqual(["BEV"]);
    expect(toggleMarketScanFuelSelection(["ICE"], "BEV")).toEqual(["ICE", "BEV"]);
  });
});

describe("buildMarketScanSlideFitAssessment", () => {
  it("keeps a modest overview slide in safe mode", () => {
    const deck = makeDeck();
    const assessment = buildMarketScanSlideFitAssessment({
      deck,
      activePage: "overview",
      heroMetricCount: 4,
      narrative: "短摘要",
      exportWidth: 1920,
      exportHeight: 1080,
    });

    expect(assessment.status).toBe("safe");
    expect(assessment.recommendedActions).toHaveLength(0);
  });

  it("marks heavy drilldown slides for split mode", () => {
    const deck = makeDeck();
    deck.results.drilldown = makeDrilldownPage({
      rankingCount: 22,
      fuelPanelCount: 5,
      trendCount: 26,
    });
    const assessment = buildMarketScanSlideFitAssessment({
      deck,
      activePage: "drilldown",
      heroMetricCount: 7,
      narrative: "这是一个很长的摘要。".repeat(20),
      exportWidth: 1920,
      exportHeight: 1080,
    });

    expect(assessment.status).toBe("split");
    expect(assessment.splitSlides).toBeGreaterThan(1);
    expect(assessment.recommendedActions).toContain(
      "单页图表建议控制在 2-3 个，超出时拆页。",
    );
  });
});
