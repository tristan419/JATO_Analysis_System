import { describe, expect, it } from "vitest";

import type {
  MarketScanDrilldownPage,
  MarketScanOriginPage,
  MarketScanOverviewPage,
  MarketScanOverviewTrendItem,
  MarketScanRankingItem,
  MarketScanSegmentPage,
} from "../../types";
import {
  buildDrilldownInsight,
  buildOriginInsight,
  buildOverviewInsight,
  buildSegmentInsight,
} from "../../utils/marketScanInsights";

function makeDelta(value: number, tone: string = value > 0 ? "positive" : value < 0 ? "negative" : "neutral") {
  const sign = value > 0 ? "+" : value < 0 ? "" : "";
  return {
    value,
    display: `${sign}${(value * 100).toFixed(1)}%`,
    tone,
  };
}

function makeTrendItem(
  period: string,
  totalVolume: number,
  fuelMix: Record<string, number>,
  mom: number,
  yoy: number,
): MarketScanOverviewTrendItem {
  const [, monthText] = period.split("-");
  return {
    period,
    label: `${period.slice(2, 4)}.${monthText}`,
    totalVolume,
    fuelMix,
    mom: makeDelta(mom),
    yoy: makeDelta(yoy),
  };
}

function makeRankingItem(brand: string, sharePct: number, rank: number): MarketScanRankingItem {
  return {
    rank,
    brand,
    volume: 1000 - rank * 50,
    sharePct,
    shareDisplay: `${(sharePct * 100).toFixed(1)}%`,
    yoy: makeDelta(0.06),
    mom: makeDelta(0.02),
    barPct: 1 - rank * 0.1,
  };
}

function makeOverviewPage(
  trendItems: MarketScanOverviewTrendItem[],
  monthlyRanking: MarketScanRankingItem[],
  ytdRanking: MarketScanRankingItem[],
): MarketScanOverviewPage {
  return {
    summary: {
      headline: "headline",
      subheadline: "subheadline",
      currentMonthVolume: trendItems[trendItems.length - 1]?.totalVolume ?? 0,
      currentMonthYoY: makeDelta(0.08),
      ytdVolume: 100000,
      ytdYoY: makeDelta(0.05),
    },
    trend: {
      periods: trendItems.map((item) => item.period),
      items: trendItems,
    },
    monthlyBrandRanking: {
      title: "Monthly Brand Ranking",
      items: monthlyRanking,
    },
    ytdBrandRanking: {
      title: "YTD Brand Ranking",
      items: ytdRanking,
    },
  };
}

function insightCardValue(insight: { cards: Array<{ label: string; value: string }> }, label: string): string {
  return insight.cards.find((card) => card.label === label)?.value ?? "";
}

describe("buildOverviewInsight", () => {
  it("summarizes a strengthening market with a clear BEV driver", () => {
    const trendItems = [
      makeTrendItem("2025-09", 8300, { ICE: 3600, PHEV: 2200, BEV: 2500 }, 0.01, 0.03),
      makeTrendItem("2025-10", 8500, { ICE: 3600, PHEV: 2200, BEV: 2700 }, 0.02, 0.03),
      makeTrendItem("2025-11", 8700, { ICE: 3600, PHEV: 2200, BEV: 2900 }, 0.02, 0.04),
      makeTrendItem("2025-12", 8900, { ICE: 3500, PHEV: 2200, BEV: 3200 }, 0.03, 0.04),
      makeTrendItem("2026-01", 9100, { ICE: 3400, PHEV: 2200, BEV: 3500 }, 0.03, 0.05),
      makeTrendItem("2026-02", 9400, { ICE: 3400, PHEV: 2200, BEV: 3800 }, 0.03, 0.06),
      makeTrendItem("2026-03", 9700, { ICE: 3300, PHEV: 2200, BEV: 4200 }, 0.03, 0.07),
      makeTrendItem("2026-04", 10100, { ICE: 3200, PHEV: 2200, BEV: 4700 }, 0.04, 0.08),
    ];

    const overview = makeOverviewPage(
      trendItems,
      [
        makeRankingItem("VOLVO", 0.22, 1),
        makeRankingItem("BMW", 0.17, 2),
        makeRankingItem("VW", 0.13, 3),
      ],
      [
        makeRankingItem("VOLVO", 0.2, 1),
        makeRankingItem("BMW", 0.16, 2),
        makeRankingItem("VW", 0.12, 3),
      ],
    );

    const insight = buildOverviewInsight(overview);

    expect(insight.headline).toBe("市场进入上行通道");
    expect(insight.cards).toHaveLength(6);
    expect(insightCardValue(insight, "结构驱动")).toContain("BEV");
    expect(insightCardValue(insight, "竞争格局")).toContain("头部");
  });

  it("flags divergence when the monthly leader differs from YTD", () => {
    const trendItems = [
      makeTrendItem("2025-11", 9000, { ICE: 4200, PHEV: 2100, BEV: 2700 }, -0.01, 0.01),
      makeTrendItem("2025-12", 9050, { ICE: 4200, PHEV: 2050, BEV: 2800 }, 0.01, 0.02),
      makeTrendItem("2026-01", 9000, { ICE: 4100, PHEV: 2100, BEV: 2800 }, -0.01, 0.01),
      makeTrendItem("2026-02", 8980, { ICE: 4050, PHEV: 2100, BEV: 2830 }, -0.01, 0.01),
      makeTrendItem("2026-03", 9020, { ICE: 4000, PHEV: 2100, BEV: 2920 }, 0.00, 0.02),
      makeTrendItem("2026-04", 8990, { ICE: 3950, PHEV: 2080, BEV: 2960 }, -0.01, 0.02),
    ];

    const overview = makeOverviewPage(
      trendItems,
      [
        makeRankingItem("TESLA", 0.16, 1),
        makeRankingItem("VOLVO", 0.14, 2),
        makeRankingItem("BMW", 0.11, 3),
      ],
      [
        makeRankingItem("VOLVO", 0.19, 1),
        makeRankingItem("BMW", 0.15, 2),
        makeRankingItem("TESLA", 0.12, 3),
      ],
    );

    const insight = buildOverviewInsight(overview);

    expect(insightCardValue(insight, "竞争格局")).toContain("分化");
    expect(insightCardValue(insight, "下月观察")).toMatch(/观察|关注/);
  });
});

describe("buildOriginInsight", () => {
  it("summarizes current leader, growth leader, and watchout", () => {
    const page: MarketScanOriginPage = {
      summaryText: "欧系当月占比 41.0%，MoM +3.0%，YoY +5.0%。",
      trend: {
        series: [
          {
            origin: "欧系",
            points: [
              { period: "2026-03", label: "26.03", volume: 3800, sharePct: 0.39 },
              { period: "2026-04", label: "26.04", volume: 4100, sharePct: 0.41 },
            ],
          },
          {
            origin: "中系",
            points: [
              { period: "2026-03", label: "26.03", volume: 2100, sharePct: 0.22 },
              { period: "2026-04", label: "26.04", volume: 2600, sharePct: 0.26 },
            ],
          },
          {
            origin: "日系",
            points: [
              { period: "2026-03", label: "26.03", volume: 2400, sharePct: 0.25 },
              { period: "2026-04", label: "26.04", volume: 2200, sharePct: 0.22 },
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
                  { period: "2026-03", label: "26.03", volume: 900, sharePct: 0.24 },
                  { period: "2026-04", label: "26.04", volume: 1100, sharePct: 0.27 },
                ],
              },
              {
                brand: "BMW",
                points: [
                  { period: "2026-03", label: "26.03", volume: 850, sharePct: 0.22 },
                  { period: "2026-04", label: "26.04", volume: 900, sharePct: 0.22 },
                ],
              },
            ],
          },
        ],
      },
      matrix: {
        columns: ["欧系", "中系", "日系"],
        rows: [
          {
            metricKey: "current_volume",
            label: "当月销量",
            cells: [
              { key: "欧系", value: 4100, display: "4,100", tone: "positive" },
              { key: "中系", value: 2600, display: "2,600", tone: "positive" },
              { key: "日系", value: 2200, display: "2,200", tone: "negative" },
            ],
          },
          {
            metricKey: "yoy",
            label: "YoY",
            cells: [
              { key: "欧系", value: 0.05, display: "+5.0%", tone: "positive" },
              { key: "中系", value: 0.18, display: "+18.0%", tone: "positive" },
              { key: "日系", value: -0.04, display: "-4.0%", tone: "negative" },
            ],
          },
          {
            metricKey: "ytd",
            label: "YTD",
            cells: [
              { key: "欧系", value: 15000, display: "15,000", tone: "positive" },
              { key: "中系", value: 9800, display: "9,800", tone: "positive" },
              { key: "日系", value: 9100, display: "9,100", tone: "neutral" },
            ],
          },
          {
            metricKey: "ytd_yoy",
            label: "YTD YoY",
            cells: [
              { key: "欧系", value: 0.06, display: "+6.0%", tone: "positive" },
              { key: "中系", value: 0.14, display: "+14.0%", tone: "positive" },
              { key: "日系", value: -0.03, display: "-3.0%", tone: "negative" },
            ],
          },
        ],
      },
    };

    const insight = buildOriginInsight(page);

    expect(insight.headline).toContain("欧系");
    expect(insightCardValue(insight, "增长亮点")).toContain("中系");
    expect(insightCardValue(insight, "品牌牵引")).toContain("VOLVO");
  });
});

describe("buildSegmentInsight", () => {
  it("summarizes body mix and hottest segment bucket", () => {
    const page: MarketScanSegmentPage = {
      summaryText: "SUV市占率达 62.0%，市场容量 9,800 台，SUV-A0 同比 +11.0%。",
      matrix: {
        columns: ["SUV-A00", "SUV-A0", "SUV-A", "≥SUV-B"],
        rows: [
          {
            metricKey: "current_volume",
            label: "当月销量",
            cells: [
              { key: "SUV-A00", value: 1100, display: "1,100", tone: "neutral" },
              { key: "SUV-A0", value: 2800, display: "2,800", tone: "positive" },
              { key: "SUV-A", value: 2200, display: "2,200", tone: "positive" },
              { key: "≥SUV-B", value: 900, display: "900", tone: "neutral" },
            ],
          },
          {
            metricKey: "yoy",
            label: "YoY",
            cells: [
              { key: "SUV-A00", value: 0.02, display: "+2.0%", tone: "positive" },
              { key: "SUV-A0", value: 0.11, display: "+11.0%", tone: "positive" },
              { key: "SUV-A", value: 0.06, display: "+6.0%", tone: "positive" },
              { key: "≥SUV-B", value: -0.03, display: "-3.0%", tone: "negative" },
            ],
          },
          {
            metricKey: "ytd",
            label: "YTD",
            cells: [
              { key: "SUV-A00", value: 4200, display: "4,200", tone: "neutral" },
              { key: "SUV-A0", value: 12100, display: "12,100", tone: "positive" },
              { key: "SUV-A", value: 9700, display: "9,700", tone: "positive" },
              { key: "≥SUV-B", value: 3600, display: "3,600", tone: "neutral" },
            ],
          },
        ],
      },
      bodyShareTrend: {
        items: [
          { period: "2025-11", label: "25.11", totalVolume: 9000, suvSharePct: 0.56, sedanSharePct: 0.44 },
          { period: "2025-12", label: "25.12", totalVolume: 9100, suvSharePct: 0.57, sedanSharePct: 0.43 },
          { period: "2026-01", label: "26.01", totalVolume: 9400, suvSharePct: 0.58, sedanSharePct: 0.42 },
          { period: "2026-02", label: "26.02", totalVolume: 9500, suvSharePct: 0.59, sedanSharePct: 0.41 },
          { period: "2026-03", label: "26.03", totalVolume: 9700, suvSharePct: 0.61, sedanSharePct: 0.39 },
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

    const insight = buildSegmentInsight(page);

    expect(insight.headline).toContain("SUV");
    expect(insightCardValue(insight, "长度级别")).toContain("SUV-A0");
    expect(insightCardValue(insight, "SUV 内部拆分")).toContain("SUV-A0");
  });
});

describe("buildDrilldownInsight", () => {
  it("summarizes leader, dominant fuel, and concentration", () => {
    const page: MarketScanDrilldownPage = {
      segment: "SUV A0",
      segmentLabel: "SUV-A0",
      title: "SUV-A0",
      summaryText: "EX30 目前领跑 SUV-A0，累计同比 +9.0%。",
      totalRanking: {
        title: "YTD Total Model Ranking",
        items: [
          {
            rank: 1,
            model: "EX30",
            volume: 3200,
            sharePct: 0.18,
            shareDisplay: "18.0%",
            yoy: makeDelta(0.09),
            barPct: 1,
            fuelMix: { BEV: 3200, PHEV: 0, ICE: 0 },
            driveMix: { "4WD": 1200, "2WD": 2000, OTHER: 0 },
          },
          {
            rank: 2,
            model: "XC40",
            volume: 2500,
            sharePct: 0.14,
            shareDisplay: "14.0%",
            yoy: makeDelta(0.05),
            barPct: 0.8,
          },
          {
            rank: 3,
            model: "X1",
            volume: 1800,
            sharePct: 0.1,
            shareDisplay: "10.0%",
            yoy: makeDelta(-0.01),
            barPct: 0.6,
          },
        ],
      },
      ytdFuelTrend: {
        items: [
          { label: "24,1-04", totalVolume: 13000, fuelMix: { ICE: 5200, PHEV: 2600, BEV: 5200 } },
          { label: "25,1-04", totalVolume: 15000, fuelMix: { ICE: 5100, PHEV: 2700, BEV: 7200 } },
          { label: "26,1-04", totalVolume: 17800, fuelMix: { ICE: 5000, PHEV: 2800, BEV: 10000 } },
        ],
      },
      fuelPanels: [
        {
          fuelType: "BEV",
          monthTitle: "BEV 26.04",
          ytdTitle: "BEV YTD",
          ytdRanking: [
            { rank: 1, model: "EX30", volume: 3200, sharePct: 0.18, shareDisplay: "18.0%", yoy: makeDelta(0.09), barPct: 1 },
          ],
          monthRanking: [
            { rank: 1, model: "EX30", volume: 820, sharePct: 0.2, shareDisplay: "20.0%", yoy: makeDelta(0.12), barPct: 1 },
          ],
        },
        {
          fuelType: "PHEV",
          monthTitle: "PHEV 26.04",
          ytdTitle: "PHEV YTD",
          ytdRanking: [
            { rank: 1, model: "XC60", volume: 1100, sharePct: 0.06, shareDisplay: "6.0%", yoy: makeDelta(0.03), barPct: 1 },
          ],
          monthRanking: [
            { rank: 1, model: "GLC", volume: 260, sharePct: 0.05, shareDisplay: "5.0%", yoy: makeDelta(0.01), barPct: 1 },
          ],
        },
      ],
    };

    const insight = buildDrilldownInsight(page);

    expect(insight.headline).toContain("EX30");
    expect(insightCardValue(insight, "动力主线")).toContain("BEV");
    expect(insightCardValue(insight, "集中度")).toContain("Top3");
  });
});
