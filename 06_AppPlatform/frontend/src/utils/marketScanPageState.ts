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
    suvAll: { ...DEFAULT_SLIDE_LAYOUT },
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

function narrativeBlockCount(text: string | undefined): number {
  const normalized = (text ?? "").trim();
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
      page?.monthlyBrandRanking?.items?.length ?? 0,
      page?.ytdBrandRanking?.items?.length ?? 0,
      page?.rolling12BrandRanking?.items?.length ?? 0,
    );
    secondaryItemCount = page?.trend?.items?.length ?? 0;
    seriesCount = deck.metadata.selectedFuelTypes.length;
    labels = [
      ...(page?.trend?.items ?? []).map((item: { label: string }) => item.label),
      ...rankingLabels(page?.monthlyBrandRanking?.items ?? []),
      ...rankingLabels(page?.ytdBrandRanking?.items ?? []),
      ...rankingLabels(page?.rolling12BrandRanking?.items ?? []),
    ];
  } else if (activePage === "origin") {
    const page = deck.results.origin;
    const groups = page?.brandTrend?.groups ?? [];
    const series = page?.trend?.series ?? [];
    chartCount = 2 + Math.min(2, groups.length);
    primaryItemCount = Math.max(
      series.length,
      ...groups.map((g) => g.series?.length ?? 0),
      page?.matrix?.rows?.length ?? 0,
      0,
    );
    secondaryItemCount = (page?.matrix?.columns?.length ?? 0) + (page?.matrix?.rows?.length ?? 0);
    seriesCount = Math.max(
      series.length,
      ...groups.map((g) => g.series?.length ?? 0),
      0,
    );
    labels = [
      ...series.map((s) => s.origin ?? "").filter(Boolean),
      ...groups.flatMap((g) => [
        g.origin ?? "",
        ...(g.series ?? []).flatMap((s) => (s.brand ? [s.brand] : [])),
      ]),
      ...matrixLabels(page?.matrix ?? { columns: [], rows: [] } as MarketScanMatrix),
    ];
  } else if (activePage === "segment") {
    const page = deck.results.segment;
    chartCount = 3;
    primaryItemCount = Math.max(
      page?.bodyShareTrend?.items?.length ?? 0,
      page?.suvSegmentShareTrend?.items?.length ?? 0,
    );
    secondaryItemCount = (page?.matrix?.rows?.length ?? 0) + (page?.matrix?.columns?.length ?? 0);
    seriesCount = 4;
    labels = [
      ...(page?.bodyShareTrend?.items ?? []).map((item: { label: string }) => item.label),
      ...(page?.suvSegmentShareTrend?.items ?? []).map((item: { label: string }) => item.label),
      ...matrixLabels(page?.matrix ?? { columns: [], rows: [] } as MarketScanMatrix),
    ];
  } else {
    const page = deck.results[activePage] as MarketScanDrilldownPage | undefined;
    const panels = page?.fuelPanels ?? [];
    chartCount = 2 + panels.length;
    primaryItemCount = Math.max(
      page?.monthTotalRanking?.items?.length ?? 0,
      page?.totalRanking?.items?.length ?? 0,
      page?.rolling12TotalRanking?.items?.length ?? 0,
      ...panels.map((p) => p.monthRanking?.length ?? 0),
      ...panels.map((p) => p.ytdRanking?.length ?? 0),
      ...panels.map((p) => p.rolling12Ranking?.length ?? 0),
      0,
    );
    secondaryItemCount = Math.max(
      page?.monthFuelTrend?.items?.length ?? 0,
      page?.ytdFuelTrend?.items?.length ?? 0,
      page?.rolling12FuelTrend?.items?.length ?? 0,
    );
    seriesCount = Math.max(
      deck.metadata.selectedFuelTypes.length,
      panels.length,
    );
    labels = [
      ...rankingLabels(page?.monthTotalRanking?.items ?? []),
      ...rankingLabels(page?.totalRanking?.items ?? []),
      ...rankingLabels(page?.rolling12TotalRanking?.items ?? []),
      ...(page?.monthFuelTrend?.items ?? []).map((item: { label: string }) => item.label),
      ...(page?.ytdFuelTrend?.items ?? []).map((item: { label: string }) => item.label),
      ...(page?.rolling12FuelTrend?.items ?? []).map((item: { label: string }) => item.label),
      ...panels.flatMap((p) => [
        p.fuelType,
        ...rankingLabels(p.monthRanking ?? []),
        ...rankingLabels(p.ytdRanking ?? []),
        ...rankingLabels(p.rolling12Ranking ?? []),
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
