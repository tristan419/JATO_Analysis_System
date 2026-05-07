import type {
  MarketScanDeckResponse,
  MarketScanDrilldownPage,
  MarketScanMatrix,
  MarketScanPageKey,
  MarketScanRankingItem,
} from "../types";

import {
  DEFAULT_SLIDE_LAYOUT,
  type SlideLayoutSettings,
  updateSlideLayout,
} from "./slideLayout";
import {
  assessSlideFit,
  measureLongestLabel,
  type SlideFitAssessment,
} from "./slideFit";

export function buildDefaultMarketScanSlideLayouts(): Record<
  MarketScanPageKey,
  SlideLayoutSettings
> {
  return {
    overview: { ...DEFAULT_SLIDE_LAYOUT },
    origin: { ...DEFAULT_SLIDE_LAYOUT },
    segment: { ...DEFAULT_SLIDE_LAYOUT },
    drilldown: { ...DEFAULT_SLIDE_LAYOUT },
    suvA: { ...DEFAULT_SLIDE_LAYOUT },
    suvB: { ...DEFAULT_SLIDE_LAYOUT },
  };
}

function rankingLabels(items: MarketScanRankingItem[]): string[] {
  return items
    .map((item) => item.brand?.trim() || item.model?.trim() || "")
    .filter(Boolean);
}

function matrixLabels(matrix: MarketScanMatrix): string[] {
  return [...matrix.columns, ...matrix.rows.map((row) => row.label)];
}

function narrativeBlockCount(text: string): number {
  const normalized = text.trim();
  if (!normalized) {
    return 0;
  }
  if (normalized.length > 160) {
    return 3;
  }
  if (normalized.length > 90) {
    return 2;
  }
  return 1;
}

export function buildMarketScanSlideFitAssessment({
  deck,
  activePage,
  heroMetricCount,
  narrative,
  exportWidth,
  exportHeight,
}: {
  deck: MarketScanDeckResponse;
  activePage: MarketScanPageKey;
  heroMetricCount: number;
  narrative: string;
  exportWidth: number;
  exportHeight: number;
}): SlideFitAssessment {
  let chartCount = 0;
  let primaryItemCount = 0;
  let secondaryItemCount = 0;
  let seriesCount = 0;
  let labels: string[] = [];

  if (activePage === "overview") {
    const page = deck.results.overview;
    chartCount = 3;
    primaryItemCount = Math.max(
      page.monthlyBrandRanking.items.length,
      page.ytdBrandRanking.items.length,
      page.rolling12BrandRanking.items.length,
    );
    secondaryItemCount = page.trend.items.length;
    seriesCount = deck.metadata.selectedFuelTypes.length;
    labels = [
      ...page.trend.items.map((item) => item.label),
      ...rankingLabels(page.monthlyBrandRanking.items),
      ...rankingLabels(page.ytdBrandRanking.items),
      ...rankingLabels(page.rolling12BrandRanking.items),
    ];
  } else if (activePage === "origin") {
    const page = deck.results.origin;
    chartCount = 2 + Math.min(2, page.brandTrend.groups.length);
    primaryItemCount = Math.max(
      page.trend.series.length,
      ...page.brandTrend.groups.map((group) => group.series.length),
      page.matrix.rows.length,
    );
    secondaryItemCount = page.matrix.columns.length + page.matrix.rows.length;
    seriesCount = Math.max(
      page.trend.series.length,
      ...page.brandTrend.groups.map((group) => group.series.length),
    );
    labels = [
      ...page.trend.series.map((item) => item.origin),
      ...page.brandTrend.groups.flatMap((group) => [
        group.origin,
        ...group.series.flatMap((item) => (item.brand ? [item.brand] : [])),
      ]),
      ...matrixLabels(page.matrix),
    ];
  } else if (activePage === "segment") {
    const page = deck.results.segment;
    chartCount = 3;
    primaryItemCount = Math.max(
      page.bodyShareTrend.items.length,
      page.suvSegmentShareTrend.items.length,
    );
    secondaryItemCount = page.matrix.rows.length + page.matrix.columns.length;
    seriesCount = 4;
    labels = [
      ...page.bodyShareTrend.items.map((item) => item.label),
      ...page.suvSegmentShareTrend.items.map((item) => item.label),
      ...matrixLabels(page.matrix),
    ];
  } else {
    const page = deck.results[activePage] as MarketScanDrilldownPage;
    chartCount = 2 + page.fuelPanels.length;
    primaryItemCount = Math.max(
      page.monthTotalRanking.items.length,
      page.totalRanking.items.length,
      page.rolling12TotalRanking.items.length,
      ...page.fuelPanels.map((panel) => panel.monthRanking.length),
      ...page.fuelPanels.map((panel) => panel.ytdRanking.length),
      ...page.fuelPanels.map((panel) => panel.rolling12Ranking.length),
    );
    secondaryItemCount = Math.max(
      page.monthFuelTrend.items.length,
      page.ytdFuelTrend.items.length,
      page.rolling12FuelTrend.items.length,
    );
    seriesCount = Math.max(
      deck.metadata.selectedFuelTypes.length,
      page.fuelPanels.length,
    );
    labels = [
      ...rankingLabels(page.monthTotalRanking.items),
      ...rankingLabels(page.totalRanking.items),
      ...rankingLabels(page.rolling12TotalRanking.items),
      ...page.monthFuelTrend.items.map((item) => item.label),
      ...page.ytdFuelTrend.items.map((item) => item.label),
      ...page.rolling12FuelTrend.items.map((item) => item.label),
      ...page.fuelPanels.flatMap((panel) => [
        panel.fuelType,
        ...rankingLabels(panel.monthRanking),
        ...rankingLabels(panel.ytdRanking),
        ...rankingLabels(panel.rolling12Ranking),
      ]),
    ];
  }

  return assessSlideFit({
    chartCount,
    metricCount: heroMetricCount,
    narrativeCount: narrativeBlockCount(narrative),
    primaryItemCount,
    secondaryItemCount,
    seriesCount,
    labelCount: labels.length,
    longestLabelLength: measureLongestLabel(labels),
    exportWidth,
    exportHeight,
  });
}

export function toggleMarketScanSlideEditModeState({
  slideEditMode,
  exportToolsOpen,
}: {
  slideEditMode: boolean;
  exportToolsOpen: boolean;
}): {
  slideEditMode: boolean;
  exportToolsOpen: boolean;
} {
  const nextSlideEditMode = !slideEditMode;
  return {
    slideEditMode: nextSlideEditMode,
    exportToolsOpen: nextSlideEditMode ? true : exportToolsOpen,
  };
}

export function updateMarketScanActiveSlideLayout<
  T extends Record<string, SlideLayoutSettings>,
>(
  current: T,
  activePage: keyof T,
  patch: Partial<SlideLayoutSettings>,
): T {
  return {
    ...current,
    [activePage]: updateSlideLayout(
      current[activePage] ?? DEFAULT_SLIDE_LAYOUT,
      patch,
    ),
  } as T;
}

export function resetMarketScanActiveSlideLayout<
  T extends Record<string, SlideLayoutSettings>,
>(
  current: T,
  activePage: keyof T,
): T {
  return {
    ...current,
    [activePage]: { ...DEFAULT_SLIDE_LAYOUT },
  } as T;
}

export function toggleMarketScanFuelSelection(
  current: string[],
  fuel: string,
): string[] {
  if (current.includes(fuel)) {
    return current.length > 1
      ? current.filter((item) => item !== fuel)
      : current;
  }
  return [...current, fuel];
}
