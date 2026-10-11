import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { SlideFitSummary } from "../../components/SlideFitSummary";
import { SlideLayoutEditor } from "../../components/SlideLayoutEditor";
import { DEFAULT_SLIDE_LAYOUT } from "../../utils/slideLayout";
import { DEFAULT_EXPORT } from "../../components/ExportPanelHelpers";
import { DrilldownSection } from "../../pages/MarketScanPage";
import type { MarketScanDrilldownPage } from "../../types";

vi.mock("../../components/LazyPlotlyChart", () => ({
  LazyPlotlyChart: ({ data }: { data: unknown }) => <div>{JSON.stringify(data)}</div>,
  preloadPlotlyChartRuntime: () => undefined,
}));

describe("DrilldownSection active window", () => {
  const page: MarketScanDrilldownPage = {
    segment: "SUV", segmentLabel: "SUV", title: "SUV", summaryText: "",
    monthTotalRanking: { title: "Monthly", items: [] },
    totalRanking: { title: "YTD", items: [] },
    rolling12TotalRanking: { title: "Rolling", items: [] },
    monthFuelTrend: { items: [] }, ytdFuelTrend: { items: [] },
    rolling12FuelTrend: { items: [] },
    customRangeTotalRanking: { title: "Custom Range Ranking", items: [{
      rank: 1, model: "Range model", volume: 100, sharePct: 1,
      shareDisplay: "100%", barPct: 1,
      yoy: { value: null, display: "—", tone: "neutral" },
    }] },
    customRangeFuelTrend: { items: [{ label: "26.08", totalVolume: 100, fuelMix: { BEV: 100 } }] },
    fuelPanels: [],
  };
  function markup(activePage: MarketScanDrilldownPage, customRangeActive: boolean) {
    return renderToStaticMarkup(<DrilldownSection
      page={activePage} fuelOrder={["BEV"]} salesMode="month"
      customRangeActive={customRangeActive} showDataLabels={false} exportSettings={DEFAULT_EXPORT}
    />);
  }
  it("renders populated custom range even when latest monthly ranking is empty", () => {
    const result = markup(page, true);
    expect(result).toContain("Custom Range Ranking");
    expect(result).toContain("Range model");
    expect(result).toContain("Custom Range Monthly Fuel Trend");
  });
  it.each(["ytd", "rolling12"] as const)("renders %s without a monthly ranking", (salesMode) => {
    const activePage = {
      ...page,
      totalRanking: page.customRangeTotalRanking!,
      rolling12TotalRanking: page.customRangeTotalRanking!,
    };
    expect(renderToStaticMarkup(<DrilldownSection
      page={activePage} fuelOrder={["BEV"]} salesMode={salesMode}
      showDataLabels={false} exportSettings={DEFAULT_EXPORT}
    />)).toContain("Range model");
  });
  it("shows an explicit empty state for an empty active monthly window", () => {
    expect(markup(page, false)).toContain("当前销量口径及筛选下暂无排行数据");
    expect(markup(page, false)).not.toContain("Range model");
  });
  it("uses custom range data for ordinary and China fuel panels", () => {
    const panel = {
      fuelType: "BEV", monthTitle: "BEV monthly", ytdTitle: "BEV YTD",
      rolling12Title: "BEV rolling", customRangeTitle: "BEV range 26.02–26.08",
      monthRanking: [{ ...page.customRangeTotalRanking!.items[0], model: "Monthly only" }],
      ytdRanking: [], rolling12Ranking: [],
      customRangeRanking: [{ ...page.customRangeTotalRanking!.items[0], model: "Range fuel" }],
    };
    const result = markup({ ...page, fuelPanels: [panel], originFuelPanels: [{
      origin: "China", originLabel: "中系", fuelPanels: [panel],
    }] }, true);
    expect(result.match(/BEV range 26.02–26.08/g)?.length).toBe(2);
    expect(result).toContain("Range fuel");
    expect(result).not.toContain("Monthly only");
  });
});

describe("SlideLayoutEditor", () => {
  it("renders all slide layout controls with current values", () => {
    const markup = renderToStaticMarkup(
      <SlideLayoutEditor
        value={{
          ...DEFAULT_SLIDE_LAYOUT,
          paddingX: 40,
          bodyGap: 18,
        }}
        onChange={() => undefined}
        onReset={() => undefined}
      />,
    );

    expect(markup).toContain("Layout Edit");
    expect(markup).toContain("Reset layout");
    expect(markup).toContain("左右边距");
    expect(markup).toContain("上下边距");
    expect(markup).toContain("内容块间距");
    expect(markup).toContain("40px");
    expect(markup.match(/type=\"range\"/g)?.length ?? 0).toBe(6);
  });
});

describe("SlideFitSummary", () => {
  it("caps action chips at three while keeping the status styling", () => {
    const markup = renderToStaticMarkup(
      <SlideFitSummary
        assessment={{
          status: "split",
          score: 98,
          summary: "当前页已超出推荐密度，建议至少拆成 2 页。",
          splitSlides: 2,
          issues: [],
          recommendedActions: [
            "动作一",
            "动作二",
            "动作三",
            "动作四",
          ],
        }}
      />,
    );

    expect(markup).toContain("slide-fit-summary--split");
    expect(markup).toContain("Need Split");
    expect(markup).toContain("动作一");
    expect(markup).toContain("动作二");
    expect(markup).toContain("动作三");
    expect(markup).not.toContain("动作四");
  });

  it("omits the action container when there are no recommendations", () => {
    const markup = renderToStaticMarkup(
      <SlideFitSummary
        assessment={{
          status: "safe",
          score: 0,
          summary: "当前页密度适合固定 1920×1080 导出。",
          splitSlides: 1,
          issues: [],
          recommendedActions: [],
        }}
      />,
    );

    expect(markup).toContain("slide-fit-summary--safe");
    expect(markup).not.toContain("slide-fit-actions");
  });
});
