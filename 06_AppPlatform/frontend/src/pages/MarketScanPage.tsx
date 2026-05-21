import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type CSSProperties,
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
} from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { usePageTransition } from "../hooks/usePageTransition";
import { DeckPeriodTimeline } from "../components/DeckPeriodTimeline";
import {
  DEFAULT_EXPORT,
  ExportPanel,
  applyDataLabelsToTraces,
  applyExportToLayout,
  applySeriesColors,
  buildExportLabelModeOptions,
  getExportPalette,
  type ExportSettings,
} from "../components/ExportPanel";
import { DeckSubpageNav } from "../components/DeckSubpageNav";
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import { SlideLayoutEditor } from "../components/SlideLayoutEditor";
import {
  buildDrilldownInsight,
  buildOriginInsight,
  buildOverviewInsight,
  buildSegmentInsight,
  type MarketInsightSnapshot,
} from "../utils/marketScanInsights";
import { SERIES_COLORS, fuelColor, originColor } from "../utils/colors";
import {
  DEFAULT_SLIDE_LAYOUT,
  readStoredSlideLayouts,
  writeStoredSlideLayouts,
  type SlideLayoutSettings,
} from "../utils/slideLayout";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";
import { useArrowCountryNavigation } from "../utils/useArrowCountryNavigation";
import { useFixedCanvasPreview } from "../utils/useFixedCanvasPreview";
import { SlideFitSummary } from "../components/SlideFitSummary";
import { RankingTrendPopover } from "../components/RankingTrendDrawer";
import {
  buildDefaultMarketScanSlideLayouts,
  buildMarketScanSlideFitAssessment,
  resetMarketScanActiveSlideLayout,
  toggleMarketScanFuelSelection,
  toggleMarketScanSlideEditModeState,
  updateMarketScanActiveSlideLayout,
} from "../utils/marketScanPageState";
import type {
  MarketScanBodyShareTrendItem,
  MarketScanChannelMixItem,
  MarketScanChannelMixOption,
  MarketScanChannelMixWindow,
  MarketScanDeckResponse,
  MarketScanDrilldownPage,
  MarketScanFuelPanel,
  MarketScanFuelTrendItem,
  MarketScanOriginBrandGroup,
  MarketScanMatrix,
  MarketScanMatrixRow,
  MarketScanOverviewPage,
  MarketScanPeriodRange,
  MarketScanOverviewTrendItem,
  MarketScanPageKey,
  MarketScanRankingGroup,
  MarketScanRankingItem,
  MarketScanSegmentPage,
  MarketScanSuvSegmentShareTrendItem,
} from "../types";

type MarketScanSalesMode = "month" | "ytd" | "rolling12";

const DEFAULT_FUEL_TYPES = ["ICE", "MHEV", "HEV", "PHEV", "BEV", "LPG"];
const DEFAULT_MARKET_SCAN_COUNTRY = "瑞典";
const DEFAULT_MARKET_SCAN_SALES_MODE: MarketScanSalesMode = "month";
const MARKET_SCAN_SALES_MODE_OPTIONS: Array<{ value: MarketScanSalesMode; label: string }> = [
  { value: "month", label: "当月" },
  { value: "ytd", label: "YTD" },
  { value: "rolling12", label: "近12个月" },
];
const TAB_ITEMS: Array<{
  key: MarketScanPageKey;
  code: string;
  label: string;
  sublabel: string;
}> = [
  { key: "overview", code: "01", label: "Overview", sublabel: "市场总量" },
  { key: "origin", code: "02", label: "Origin", sublabel: "车系走势" },
  { key: "segment", code: "03", label: "Segment", sublabel: "级别结构" },
  { key: "suvAll", code: "04", label: "SUV", sublabel: "全SUV" },
  { key: "drilldown", code: "05", label: "Drilldown", sublabel: "A0级 SUV" },
  { key: "suvA", code: "06", label: "SUV-A", sublabel: "A级 SUV" },
  { key: "suvB", code: "07", label: "SUV-B", sublabel: "B级 SUV" },
];

const DEFAULT_MARKET_SCAN_EXPORT: ExportSettings = {
  ...DEFAULT_EXPORT,
  showXGrid: false,
  showYGrid: false,
  showAxisLine: false,
  exportWidth: 1920,
  exportHeight: 1080,
  dataLabelMode: "value",
  dataLabelPosition: "auto",
  decimalPlaces: 0,
  fontSize: 11,
};
const MARKET_SCAN_OVERVIEW_TREND_MARGIN = { l: 52, r: 24, t: 20, b: 48 } as const;
const MIN_MARKET_SCAN_RANKING_LIMIT = 10;
const MARKET_SCAN_RANKING_LIMIT_OPTIONS = [10, 15, 20, 30] as const;
const SUV_SEGMENT_SHARE_ORDER = ["SUV-A00", "SUV-A0", "SUV-A", "≥SUV-B"] as const;
const SUV_SEGMENT_SHARE_META: Record<(typeof SUV_SEGMENT_SHARE_ORDER)[number], { label: string; color: string }> = {
  "SUV-A00": { label: "SUV-A00", color: "#0f766e" },
  "SUV-A0": { label: "SUV-A0", color: "#14b8a6" },
  "SUV-A": { label: "SUV-A", color: "#84cc16" },
  "≥SUV-B": { label: "≥SUV-B", color: "#f59e0b" },
};
type MarketScanTextPosition =
  | "top center"
  | "middle left"
  | "middle right"
  | "bottom center";

const SUV_SEGMENT_SHARE_TEXT_POSITIONS: Record<(typeof SUV_SEGMENT_SHARE_ORDER)[number], MarketScanTextPosition> = {
  "SUV-A00": "bottom center",
  "SUV-A0": "top center",
  "SUV-A": "middle right",
  "≥SUV-B": "middle left",
};
const REGISTRATION_CHANNEL_ORDER = ["Business", "Private", "Other"] as const;
const REGISTRATION_CHANNEL_META: Record<(typeof REGISTRATION_CHANNEL_ORDER)[number], {
  label: string;
  color: string;
  textColor: string;
}> = {
  Business: { label: "Business", color: "#0f766e", textColor: "#ffffff" },
  Private: { label: "Private", color: "#2563eb", textColor: "#ffffff" },
  Other: { label: "Other", color: "#cbd5e1", textColor: "#0f172a" },
};
const DEFAULT_MARKET_SCAN_CHANNEL_VIEW = "origin";
const DEFAULT_MARKET_SCAN_CHANNEL_OPTIONS: MarketScanChannelMixOption[] = [
  { value: DEFAULT_MARKET_SCAN_CHANNEL_VIEW, label: "按车系" },
];
const MARKET_SCAN_SLIDE_LAYOUT_STORAGE_KEY = "market-scan";

function isMarketScanPageKey(value: string | null): value is MarketScanPageKey {
  return value !== null && TAB_ITEMS.some((item) => item.key === value);
}

function isMarketScanSalesMode(value: string | null): value is MarketScanSalesMode {
  return value === "month" || value === "ytd" || value === "rolling12";
}

function normalizeMarketScanRankingLimit(value: number | string | null | undefined): number {
  const numericValue = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numericValue)) {
    return MIN_MARKET_SCAN_RANKING_LIMIT;
  }
  return Math.max(MIN_MARKET_SCAN_RANKING_LIMIT, Math.round(numericValue));
}

function resolveMarketScanTextPosition(
  trace: Partial<Data>,
  requestedPosition: string,
  fallback: unknown,
): unknown {
  const traceRecord = trace as Record<string, unknown>;
  if (!requestedPosition || requestedPosition === "auto") {
    return fallback;
  }
  if (requestedPosition === "top") {
    return "top center";
  }
  if (requestedPosition === "middle") {
    return "middle center";
  }
  if (requestedPosition === "inside") {
    return trace.type === "bar" ? "inside" : "middle center";
  }
  if (requestedPosition === "outside") {
    if (trace.type === "bar") {
      return "outside";
    }
    return traceRecord.orientation === "h" ? "middle right" : "top center";
  }
  return fallback;
}

function marketScanSeriesKey(trace: Partial<Data>): string | null {
  const traceRecord = trace as Record<string, unknown>;
  if (typeof traceRecord.legendgroup === "string" && traceRecord.legendgroup.trim()) {
    return traceRecord.legendgroup;
  }
  if (typeof trace.name === "string" && trace.name.trim()) {
    return trace.name;
  }
  return null;
}

function buildMarketScanSeriesColors(traces: Data[], exportSettings: ExportSettings): Record<string, string> {
  const manualColors = exportSettings.seriesColors ?? {};
  const usePalette = exportSettings.colorScheme !== DEFAULT_MARKET_SCAN_EXPORT.colorScheme;
  if (!usePalette && Object.keys(manualColors).length === 0) {
    return manualColors;
  }

  const palette = getExportPalette(exportSettings.colorScheme);
  const resolved: Record<string, string> = { ...manualColors };
  const assigned = new Set(Object.keys(manualColors));
  let paletteIndex = 0;

  traces.forEach((trace) => {
    const key = marketScanSeriesKey(trace);
    if (!key || assigned.has(key) || key === "Labels" || key === "Total Labels") {
      return;
    }
    resolved[key] = palette[paletteIndex % palette.length];
    assigned.add(key);
    paletteIndex += 1;
  });

  return resolved;
}

interface MarketScanTraceExportOptions {
  rewriteDataLabels?: boolean;
}

function applyMarketScanExportToTraces(
  traces: Data[],
  exportSettings: ExportSettings,
  options: MarketScanTraceExportOptions = {},
): Data[] {
  const colorOverrides = buildMarketScanSeriesColors(traces, exportSettings);
  const labeled = options.rewriteDataLabels === false ? traces : applyDataLabelsToTraces(traces, exportSettings);
  const positioned = labeled.map((trace) => {
    const next = { ...trace } as Record<string, unknown> & Data & {
      textposition?: unknown;
      textfont?: { size?: number; color?: string };
    };
    const hasText =
      typeof next.text === "string"
      || (Array.isArray(next.text) && next.text.length > 0)
      || (typeof next.mode === "string" && next.mode.includes("text"));

    if (hasText) {
      next.textposition = resolveMarketScanTextPosition(
        next,
        exportSettings.dataLabelPosition,
        next.textposition,
      );
      if (next.textfont && typeof next.textfont === "object") {
        next.textfont = {
          ...next.textfont,
          size: Math.max(8, exportSettings.fontSize - 2),
        };
      }
    }

    return next as Data;
  });

  return applySeriesColors(positioned, colorOverrides);
}

function applyMarketScanExportToLayout(
  layout: Partial<PlotlyLayout>,
  exportSettings: ExportSettings,
): Partial<PlotlyLayout> {
  const baseLayout: Partial<PlotlyLayout> = {
    ...layout,
    xaxis: layout.xaxis && typeof layout.xaxis === "object"
      ? { ...(layout.xaxis as object) } as PlotlyLayout["xaxis"]
      : layout.xaxis,
    yaxis: layout.yaxis && typeof layout.yaxis === "object"
      ? { ...(layout.yaxis as object) } as PlotlyLayout["yaxis"]
      : layout.yaxis,
    legend: layout.legend && typeof layout.legend === "object"
      ? { ...(layout.legend as object) } as PlotlyLayout["legend"]
      : layout.legend,
    font: layout.font && typeof layout.font === "object"
      ? { ...(layout.font as object) } as PlotlyLayout["font"]
      : layout.font,
  };

  const exported = applyExportToLayout(baseLayout, { ...exportSettings, chartTitle: "" });
  const next: Partial<PlotlyLayout> = { ...exported };
  const baseXaxis = baseLayout.xaxis as PlotlyLayout["xaxis"] | undefined;
  const baseYaxis = baseLayout.yaxis as PlotlyLayout["yaxis"] | undefined;

  if (exportSettings.legendPosition === DEFAULT_MARKET_SCAN_EXPORT.legendPosition && baseLayout.legend) {
    next.legend = baseLayout.legend;
  }
  if (exportSettings.fontSize === DEFAULT_MARKET_SCAN_EXPORT.fontSize && baseLayout.font) {
    next.font = baseLayout.font;
  }
  if (exportSettings.paperBg === DEFAULT_MARKET_SCAN_EXPORT.paperBg && baseLayout.paper_bgcolor !== undefined) {
    next.paper_bgcolor = baseLayout.paper_bgcolor;
  }
  if (exportSettings.plotBg === DEFAULT_MARKET_SCAN_EXPORT.plotBg && baseLayout.plot_bgcolor !== undefined) {
    next.plot_bgcolor = baseLayout.plot_bgcolor;
  }

  next.xaxis = {
    ...(exported.xaxis as object ?? {}),
    ...(!exportSettings.xTickFormat && baseXaxis && "tickformat" in baseXaxis ? { tickformat: baseXaxis.tickformat } : {}),
    ...(!exportSettings.xTitle && baseXaxis && "title" in baseXaxis ? { title: baseXaxis.title } : {}),
  } as PlotlyLayout["xaxis"];
  next.yaxis = {
    ...(exported.yaxis as object ?? {}),
    ...(!exportSettings.yTickFormat && baseYaxis && "tickformat" in baseYaxis ? { tickformat: baseYaxis.tickformat } : {}),
    ...(!exportSettings.yTitle && baseYaxis && "title" in baseYaxis ? { title: baseYaxis.title } : {}),
  } as PlotlyLayout["yaxis"];

  return next;
}

interface HeroMetric {
  label: string;
  value: string;
  detail: string;
  tone?: string;
}

interface PanelProps {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  children: ReactNode;
  actions?: ReactNode;
  className?: string;
}

function MarketScanDeckSkeleton() {
  return (
    <section className="market-scan-state-card market-scan-state-card--skeleton" aria-hidden="true">
      <LoadingSurface
        mode="inline"
        kicker="Deck"
        label="正在生成市场扫描页面"
        detail="后端会按国家、月份和燃料组合动态聚合 Parquet 数据。"
      />
      <div className="market-scan-skeleton-hero">
        <div className="market-scan-skeleton-copy">
          <span className="market-scan-skeleton-block market-scan-skeleton-block--eyebrow" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--title" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--body" />
        </div>
        <div className="market-scan-skeleton-chip-row">
          {Array.from({ length: 4 }, (_, index) => (
            <span
              key={`skeleton-chip-${index}`}
              className="market-scan-skeleton-block market-scan-skeleton-block--chip"
            />
          ))}
        </div>
      </div>
      <div className="market-scan-skeleton-tabs">
        {Array.from({ length: 6 }, (_, index) => (
          <span
            key={`skeleton-tab-${index}`}
            className="market-scan-skeleton-block market-scan-skeleton-block--tab"
          />
        ))}
      </div>
      <div className="market-scan-skeleton-grid market-scan-skeleton-grid--metrics">
        {Array.from({ length: 4 }, (_, index) => (
          <div key={`skeleton-metric-${index}`} className="market-scan-skeleton-panel">
            <span className="market-scan-skeleton-block market-scan-skeleton-block--metric-label" />
            <span className="market-scan-skeleton-block market-scan-skeleton-block--metric-value" />
            <span className="market-scan-skeleton-block market-scan-skeleton-block--metric-detail" />
          </div>
        ))}
      </div>
      <div className="market-scan-skeleton-grid market-scan-skeleton-grid--content">
        <div className="market-scan-skeleton-panel market-scan-skeleton-panel--wide">
          <span className="market-scan-skeleton-block market-scan-skeleton-block--panel-title" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--chart" />
        </div>
        <div className="market-scan-skeleton-panel market-scan-skeleton-panel--stack">
          <span className="market-scan-skeleton-block market-scan-skeleton-block--panel-title" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
        </div>
      </div>
    </section>
  );
}

function formatVolume(value: number | null | undefined): string {
  return Number(value ?? 0).toLocaleString("en-US");
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function safeShareValue(value: number, total: number): number | null {
  if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) {
    return null;
  }
  return value / total;
}

function toneClassName(tone?: string): string {
  if (!tone) {
    return "is-neutral";
  }
  if (tone === "positive") {
    return "is-positive";
  }
  if (tone === "negative") {
    return "is-negative";
  }
  if (tone === "new") {
    return "is-new";
  }
  return "is-neutral";
}

function rankingItemLabel(item: MarketScanRankingItem): string {
  return item.brand ?? item.model ?? "-";
}

function topCell(row?: MarketScanMatrixRow): { key: string; value: number | null } | null {
  if (!row || row.cells.length === 0) {
    return null;
  }
  return row.cells.reduce<{ key: string; value: number | null } | null>((winner, cell) => {
    if (!winner || (cell.value ?? 0) > (winner.value ?? 0)) {
      return { key: cell.key, value: cell.value };
    }
    return winner;
  }, null);
}

function matrixRow(matrix: MarketScanMatrix, metricKey: string): MarketScanMatrixRow | undefined {
  return matrix.rows.find((row) => row.metricKey === metricKey);
}

function marketScanVolumeMetricKey(salesMode: MarketScanSalesMode, customRangeActive = false): string {
  if (customRangeActive) {
    return "custom_range";
  }
  if (salesMode === "ytd") {
    return "ytd";
  }
  if (salesMode === "rolling12") {
    return "rolling12";
  }
  return "current_volume";
}

function marketScanDeltaMetricKey(salesMode: MarketScanSalesMode, customRangeActive = false): string {
  if (customRangeActive) {
    return "custom_range_yoy";
  }
  if (salesMode === "ytd") {
    return "ytd_yoy";
  }
  if (salesMode === "rolling12") {
    return "rolling12_yoy";
  }
  return "yoy";
}

function marketScanWindowLabel(
  salesMode: MarketScanSalesMode,
  currentMonthShort: string,
  customRangeLabel?: string,
): string {
  if (customRangeLabel) {
    return customRangeLabel;
  }
  if (salesMode === "ytd") {
    return "YTD";
  }
  if (salesMode === "rolling12") {
    return "Rolling 12M";
  }
  return currentMonthShort;
}

function marketScanWindowDetail(
  salesMode: MarketScanSalesMode,
  currentMonthShort: string,
  customRangeActive = false,
): string {
  if (customRangeActive) {
    return "区间累计销量";
  }
  if (salesMode === "month") {
    return "车系总量";
  }
  return `截至 ${currentMonthShort}`;
}

function marketScanDeltaLabel(salesMode: MarketScanSalesMode, customRangeActive = false): string {
  if (customRangeActive) {
    return "自定义区间 YoY";
  }
  if (salesMode === "ytd") {
    return "YTD YoY";
  }
  if (salesMode === "rolling12") {
    return "Rolling 12M YoY";
  }
  return "YoY";
}

function marketScanVolumeSuffix(salesMode: MarketScanSalesMode, customRangeActive = false): string {
  if (customRangeActive) {
    return "台（自定义区间）";
  }
  if (salesMode === "ytd") {
    return "台（YTD）";
  }
  if (salesMode === "rolling12") {
    return "台（近12个月）";
  }
  return "台";
}

function marketScanOverviewRankingGroups(
  page: MarketScanOverviewPage,
  salesMode: MarketScanSalesMode,
  customRangeActive = false,
): MarketScanRankingGroup[] {
  if (customRangeActive && page.customRangeBrandRanking) {
    return [page.customRangeBrandRanking, page.rolling12BrandRanking];
  }
  if (salesMode === "ytd") {
    return [page.ytdBrandRanking, page.rolling12BrandRanking];
  }
  if (salesMode === "rolling12") {
    return [page.rolling12BrandRanking, page.monthlyBrandRanking];
  }
  return [page.monthlyBrandRanking, page.ytdBrandRanking];
}

function marketScanActiveFuelPanelWindow(
  panel: MarketScanFuelPanel,
  salesMode: MarketScanSalesMode,
  customRangeActive = false,
): { title: string; ranking: MarketScanRankingItem[] } {
  if (customRangeActive) {
    return {
      title: panel.customRangeTitle || panel.monthTitle,
      ranking: panel.customRangeRanking || panel.monthRanking,
    };
  }
  if (salesMode === "ytd") {
    return { title: panel.ytdTitle, ranking: panel.ytdRanking };
  }
  if (salesMode === "rolling12") {
    return { title: panel.rolling12Title, ranking: panel.rolling12Ranking };
  }
  return { title: panel.monthTitle, ranking: panel.monthRanking };
}

function marketScanActiveDrilldownWindow(
  page: MarketScanDrilldownPage,
  salesMode: MarketScanSalesMode,
  customRangeActive = false,
  customRangeLabel?: string,
): {
  ranking: MarketScanDrilldownPage["monthTotalRanking"];
  fuelTrend: MarketScanDrilldownPage["monthFuelTrend"];
  trendTitle: string;
  trendSubtitle: string;
  trendYAxisTitle: string;
  rankingSubtitle: string;
  heroWindowLabel: string;
  heroWindowValue: string;
} {
  if (customRangeActive && page.customRangeTotalRanking && page.customRangeFuelTrend) {
    return {
      ranking: page.customRangeTotalRanking,
      fuelTrend: page.customRangeFuelTrend,
      trendTitle: "Custom Range Monthly Fuel Trend",
      trendSubtitle: "观察所选时间轴区间内逐月的燃料路线结构。",
      trendYAxisTitle: "月度销量",
      rankingSubtitle: "按当前国家与细分市场自定义区间份额排序",
      heroWindowLabel: "Custom Range",
      heroWindowValue: customRangeLabel || "自定义区间",
    };
  }
  if (salesMode === "ytd") {
    return {
      ranking: page.totalRanking,
      fuelTrend: page.ytdFuelTrend,
      trendTitle: "YTD Fuel Trend",
      trendSubtitle: "观察同一年内累计窗口下各燃料路线的堆叠变化。",
      trendYAxisTitle: "YTD销量",
      rankingSubtitle: "按当前国家与细分市场 YTD 份额排序",
      heroWindowLabel: "YTD Window",
      heroWindowValue: "YTD",
    };
  }
  if (salesMode === "rolling12") {
    return {
      ranking: page.rolling12TotalRanking,
      fuelTrend: page.rolling12FuelTrend,
      trendTitle: "Rolling 12M Fuel Trend",
      trendSubtitle: "观察同一近12个月窗口下各燃料路线的堆叠变化。",
      trendYAxisTitle: "近12个月销量",
      rankingSubtitle: "按当前国家与细分市场近12个月份额排序",
      heroWindowLabel: "Rolling 12M Window",
      heroWindowValue: "",
    };
  }
  return {
    ranking: page.monthTotalRanking,
    fuelTrend: page.monthFuelTrend,
    trendTitle: "Monthly Fuel Trend",
    trendSubtitle: "观察同一月份跨年度的燃料路线结构。",
    trendYAxisTitle: "当月销量",
    rankingSubtitle: "按当前国家与细分市场当月份额排序",
    heroWindowLabel: "Month Window",
    heroWindowValue: "",
  };
}

function marketScanActiveSegmentChannelWindow(
  page: MarketScanSegmentPage,
  salesMode: MarketScanSalesMode,
  customRangeActive = false,
): MarketScanChannelMixWindow | null {
  const channelMix = page.channelMix;
  if (!channelMix) {
    return null;
  }
  if (customRangeActive && channelMix.customRange) {
    return channelMix.customRange;
  }
  if (salesMode === "ytd") {
    return channelMix.ytd;
  }
  if (salesMode === "rolling12") {
    return channelMix.rolling12;
  }
  return channelMix.month;
}

function marketScanChannelOptions(page: MarketScanSegmentPage): MarketScanChannelMixOption[] {
  const options = page.channelMix?.options ?? [];
  return options.length > 0 ? options : DEFAULT_MARKET_SCAN_CHANNEL_OPTIONS;
}

function marketScanActiveSegmentChannelView(
  window: MarketScanChannelMixWindow | null,
  requestedView: string,
): {
  viewKey: string;
  title: string;
  items: MarketScanChannelMixItem[];
} {
  if (!window) {
    return { viewKey: DEFAULT_MARKET_SCAN_CHANNEL_VIEW, title: "", items: [] };
  }
  const defaultView = window.defaultView || DEFAULT_MARKET_SCAN_CHANNEL_VIEW;
  const views = window.views ?? {};
  const viewKey = views[requestedView] ? requestedView : defaultView;
  const view = views[viewKey];
  return {
    viewKey,
    title: view?.title ?? window.title,
    items: view?.items ?? window.items,
  };
}

function filterMatrixBySalesMode(
  matrix: MarketScanMatrix,
  salesMode: MarketScanSalesMode,
  customRangeActive = false,
): MarketScanMatrix {
  const visibleMetricKeys = customRangeActive
    ? new Set(["custom_range", "custom_range_yoy"])
    : salesMode === "month"
    ? new Set(["current_volume", "mom", "yoy"])
    : salesMode === "ytd"
      ? new Set(["ytd", "ytd_yoy"])
      : new Set(["rolling12", "rolling12_yoy"]);
  return {
    ...matrix,
    rows: matrix.rows.filter((row) => visibleMetricKeys.has(row.metricKey)),
  };
}

function buildHeroMetrics(
  _deck: MarketScanDeckResponse,
  _pageKey: MarketScanPageKey,
  _salesMode: MarketScanSalesMode,
  _customRangeActive = false,
): HeroMetric[] {
  return [];
}


function pageNarrative(deck: MarketScanDeckResponse, pageKey: MarketScanPageKey): string {
  if (deck.metadata.customRangeActive) {
    return `当前页面已切换为 ${deck.results.overview.summary.customRangeLabel || "自定义区间"} 累计口径。`;
  }
  if (pageKey === "overview") {
    return deck.results.overview.summary.subheadline;
  }
  if (pageKey === "origin") {
    return deck.results.origin.summaryText;
  }
  if (pageKey === "segment") {
    return deck.results.segment.summaryText;
  }
  return (deck.results[pageKey] as MarketScanDrilldownPage).summaryText;
}

function buildSparseText(
  pointCount: number,
  render: (index: number) => string,
  stride = 4,
): string[] {
  return Array.from({ length: pointCount }, (_, index) => {
    if (index === pointCount - 1 || index === 0 || (index + 1) % stride === 0) {
      return render(index);
    }
    return "";
  });
}

function applyOverviewTrendExportToTraces(traces: Data[], exportSettings: ExportSettings): Data[] {
  const applied = applyMarketScanExportToTraces(traces, exportSettings);
  if (exportSettings.dataLabelMode === "off") {
    return applied;
  }
  if (exportSettings.dataLabelMode !== "value" && exportSettings.dataLabelMode !== "sales") {
    return applied;
  }

  return applied.map((trace) => {
    if (trace.type !== "scatter" || typeof trace.name !== "string" || !trace.name.endsWith("Total")) {
      return trace;
    }
    const pointCount = Array.isArray(trace.x)
      ? trace.x.length
      : Array.isArray(trace.y)
        ? trace.y.length
        : 0;
    if (pointCount === 0) {
      return trace;
    }
    const currentText = Array.isArray(trace.text)
      ? trace.text.map((value) => String(value ?? ""))
      : Array.from({ length: pointCount }, () => typeof trace.text === "string" ? trace.text : "");
    const isPriorTotal = typeof trace.line === "object" && trace.line !== null && "dash" in trace.line;
    return {
      ...trace,
      text: buildSparseText(pointCount, (index) => currentText[index] ?? "", 3),
      textposition: exportSettings.dataLabelPosition === "auto"
        ? (isPriorTotal ? "bottom right" : "top left")
        : trace.textposition,
    } as Data;
  });
}

function buildLastPointText(
  pointCount: number,
  render: (index: number) => string,
): string[] {
  return Array.from({ length: pointCount }, (_, index) => (
    index === pointCount - 1 ? render(index) : ""
  ));
}

function sanitizeFileNameSegment(value: string): string {
  return value.trim().replace(/[^a-zA-Z0-9\u4e00-\u9fff]+/g, "-").replace(/^-+|-+$/g, "") || "slide";
}

function shiftMonthPeriod(period: string, deltaMonths: number): string {
  const [yearText, monthText] = period.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    return period;
  }
  const shifted = new Date(Date.UTC(year, month - 1 + deltaMonths, 1));
  return `${shifted.getUTCFullYear()}-${String(shifted.getUTCMonth() + 1).padStart(2, "0")}`;
}

function readSearchTimeRange(searchParams: URLSearchParams): MarketScanPeriodRange | null {
  const start = searchParams.get("timeStart");
  const end = searchParams.get("timeEnd");
  if (start && end) {
    return { start, end };
  }
  return null;
}

function isCustomTimeRange(range: MarketScanPeriodRange | null | undefined): boolean {
  return Boolean(range && range.start !== range.end);
}

function periodWithinRange(period: string, range: MarketScanPeriodRange | null | undefined): boolean {
  if (!range) {
    return true;
  }
  return period >= range.start && period <= range.end;
}

function overviewTrendTrailingItems(items: MarketScanOverviewTrendItem[]): MarketScanOverviewTrendItem[] {
  return [...items].sort((left, right) => left.period.localeCompare(right.period)).slice(-12);
}

function readMarketScanFuelFromLegendName(traceName: string, fuelOrder: string[]): string | null {
  const normalizedName = traceName.trim();
  if (!normalizedName) {
    return null;
  }
  return fuelOrder.find((fuel) => normalizedName === fuel || normalizedName.endsWith(` ${fuel}`)) ?? null;
}

function readMarketScanLegendFuelFromTarget(target: EventTarget | null, fuelOrder: string[]): string | null {
  if (!(target instanceof Element)) {
    return null;
  }
  const legendTrace = target.closest("g.traces");
  const legendText = legendTrace?.querySelector("text.legendtext")?.textContent ?? "";
  return readMarketScanFuelFromLegendName(legendText, fuelOrder);
}

function buildOverviewTrendData(
  items: MarketScanOverviewTrendItem[],
  fuelOrder: string[],
  showDataLabels: boolean,
): Data[] {
  const ordered = [...items].sort((left, right) => left.period.localeCompare(right.period));
  const trailingItems = overviewTrendTrailingItems(items);
  if (trailingItems.length === 0) {
    return [];
  }

  const itemByPeriod = new Map(ordered.map((item) => [item.period, item]));
  const labels = trailingItems.map((item) => item.label);
  const currentYear = trailingItems[trailingItems.length - 1]?.period.slice(0, 4) ?? "Current";
  const priorYear = String(Number(currentYear) - 1);
  const priorItems = trailingItems.map((item) => itemByPeriod.get(shiftMonthPeriod(item.period, -12)));

  const traces: Data[] = [];
  fuelOrder.forEach((fuel) => {
    traces.push({
      type: "bar",
      name: `${priorYear} ${fuel}`,
      legendgroup: fuel,
      offsetgroup: priorYear,
      showlegend: false,
      opacity: 0.34,
      x: labels,
      y: priorItems.map((item) => item?.fuelMix[fuel] ?? 0),
      marker: { color: fuelColor(fuel) },
      hovertemplate: `%{x}<br>${priorYear} ${fuel}: %{y:,.0f} 台<extra></extra>`,
    } as Data);
    traces.push({
      type: "bar",
      name: `${currentYear} ${fuel}`,
      legendgroup: fuel,
      offsetgroup: currentYear,
      x: labels,
      y: trailingItems.map((item) => item.fuelMix[fuel] ?? 0),
      marker: { color: fuelColor(fuel) },
      hovertemplate: `%{x}<br>${currentYear} ${fuel}: %{y:,.0f} 台<extra></extra>`,
    } as Data);
  });

  traces.push({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: `${currentYear} Total`,
    x: labels,
    y: trailingItems.map((item) => item.totalVolume),
    line: { color: "#0f172a", width: 2.8 },
    marker: { color: "#0f172a", size: 6 },
    text: showDataLabels
      ? buildSparseText(trailingItems.length, (index) => formatVolume(trailingItems[index].totalVolume), 3)
      : undefined,
    textposition: "top center",
    textfont: { size: 10, color: "#0f172a" },
    hovertemplate: `%{x}<br>${currentYear} Total: %{y:,.0f} 台<extra></extra>`,
  });
  traces.push({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: `${priorYear} Total`,
    x: labels,
    y: priorItems.map((item) => item?.totalVolume ?? 0),
    line: { color: "#64748b", width: 2, dash: "dot" },
    marker: { color: "#64748b", size: 5 },
    text: showDataLabels
      ? buildSparseText(priorItems.length, (index) => formatVolume(priorItems[index]?.totalVolume ?? 0), 3)
      : undefined,
    textposition: "bottom center",
    textfont: { size: 9, color: "#64748b" },
    hovertemplate: `%{x}<br>${priorYear} Total: %{y:,.0f} 台<extra></extra>`,
  });

  return traces;
}

function buildOriginTrendData(
  series: MarketScanDeckResponse["results"]["origin"]["trend"]["series"],
  showDataLabels: boolean,
): Data[] {
  return series.map((entry) => ({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: entry.origin,
    x: entry.points.map((point) => point.label),
    y: entry.points.map((point) => point.volume),
    line: { color: originColor(entry.origin), width: 2.4 },
    marker: { color: originColor(entry.origin), size: 5 },
    text: showDataLabels
      ? buildLastPointText(
          entry.points.length,
          (index) => `${entry.origin} ${formatVolume(entry.points[index].volume)}`,
        )
      : undefined,
    textposition: "middle right",
    textfont: { size: 10 },
    hovertemplate: `%{x}<br>${entry.origin}: %{y:,.0f} 台<extra></extra>`,
  }));
}

function buildOriginBrandTrendData(
  group: MarketScanOriginBrandGroup,
  showDataLabels: boolean,
): Data[] {
  return group.series.map((entry, index) => ({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: entry.brand,
    x: entry.points.map((point) => point.label),
    y: entry.points.map((point) => point.volume),
    line: { color: SERIES_COLORS[index % SERIES_COLORS.length], width: 2.2 },
    marker: { color: SERIES_COLORS[index % SERIES_COLORS.length], size: 4 },
    text: showDataLabels
      ? buildLastPointText(
          entry.points.length,
          (pointIndex) => `${entry.brand} ${formatVolume(entry.points[pointIndex]?.volume ?? 0)}`,
        )
      : undefined,
    textposition: "middle right",
    textfont: { size: 9 },
    hovertemplate: `%{x}<br>${group.origin} · ${entry.brand}: %{y:,.0f} 台<extra></extra>`,
  }));
}

function buildBodyShareData(
  items: MarketScanBodyShareTrendItem[],
  showDataLabels: boolean,
  labelDigits = 1,
): Data[] {
  const labels = items.map((item) => item.label);
  return [
    {
      type: "scatter",
      mode: showDataLabels ? "text+lines+markers" : "lines",
      stackgroup: "share",
      fill: "tonexty",
      name: "SUV",
      x: labels,
      y: items.map((item) => item.suvSharePct),
      line: { color: "#0f766e", width: 2.2 },
      marker: { color: "#0f766e", size: 4 },
      text: showDataLabels
        ? items.map((item) => formatPercent(item.suvSharePct, labelDigits))
        : undefined,
      textposition: "top center",
      textfont: { size: 9 },
      hovertemplate: "%{x}<br>SUV: %{y:.1%}<extra></extra>",
    },
    {
      type: "scatter",
      mode: showDataLabels ? "text+lines+markers" : "lines",
      stackgroup: "share",
      fill: "tonexty",
      name: "Sedan",
      x: labels,
      y: items.map((item) => item.sedanSharePct),
      line: { color: "#b45309", width: 2.2 },
      marker: { color: "#b45309", size: 4 },
      text: showDataLabels
        ? items.map((item) => formatPercent(item.sedanSharePct, labelDigits))
        : undefined,
      textposition: "bottom center",
      textfont: { size: 9 },
      hovertemplate: "%{x}<br>Sedan: %{y:.1%}<extra></extra>",
    },
  ];
}

function buildSuvSegmentShareData(
  items: MarketScanSuvSegmentShareTrendItem[],
  showDataLabels: boolean,
  labelDigits = 1,
): Data[] {
  const labels = items.map((item) => item.label);
  return SUV_SEGMENT_SHARE_ORDER.map((segment) => ({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines",
    stackgroup: "suv-segment-share",
    fill: "tonexty",
    name: SUV_SEGMENT_SHARE_META[segment].label,
    x: labels,
    y: items.map((item) => item.segmentSharePct[segment] ?? 0),
    line: { color: SUV_SEGMENT_SHARE_META[segment].color, width: 2.2 },
    marker: { color: SUV_SEGMENT_SHARE_META[segment].color, size: 4 },
    text: showDataLabels
      ? buildSparseText(
          items.length,
          (index) => formatPercent(items[index]?.segmentSharePct[segment] ?? 0, labelDigits),
          6,
        )
      : undefined,
    textposition: SUV_SEGMENT_SHARE_TEXT_POSITIONS[segment],
    textfont: { size: 9 },
    hovertemplate: `%{x}<br>${SUV_SEGMENT_SHARE_META[segment].label}: %{y:.1%}<extra></extra>`,
  }));
}

function registrationChannelShare(item: MarketScanChannelMixItem, channel: (typeof REGISTRATION_CHANNEL_ORDER)[number]): number {
  const share = Number(item.channelSharePct?.[channel] ?? 0);
  if (!Number.isFinite(share) || share <= 0) {
    return 0;
  }
  return Math.max(0, Math.min(1, share));
}

function buildRegistrationChannelChartData(
  items: MarketScanChannelMixItem[],
  showDataLabels: boolean,
): Data[] {
  const ranked = items
    .filter((item) => {
      const channelMix = item.channelMix ?? {};
      return REGISTRATION_CHANNEL_ORDER.some((channel) => Number(channelMix[channel] ?? 0) > 0);
    })
    .slice(0, 6);
  const labels = ranked.map((item) => item.label);

  return REGISTRATION_CHANNEL_ORDER.map((channel) => ({
    type: "bar",
    orientation: "h",
    name: REGISTRATION_CHANNEL_META[channel].label,
    y: labels,
    x: ranked.map((item) => registrationChannelShare(item, channel)),
    marker: { color: REGISTRATION_CHANNEL_META[channel].color },
    text: showDataLabels
      ? ranked.map((item) => {
          const share = registrationChannelShare(item, channel);
          return share >= 0.08 ? formatPercent(share, 0) : "";
        })
      : undefined,
    texttemplate: showDataLabels ? "%{text}" : undefined,
    textposition: "inside",
    textfont: { size: 9, color: REGISTRATION_CHANNEL_META[channel].textColor },
    hovertemplate: `%{y}<br>${REGISTRATION_CHANNEL_META[channel].label}: %{x:.0%}<extra></extra>`,
  })) as Data[];
}

function buildFuelTrendData(
  items: MarketScanFuelTrendItem[],
  fuelOrder: string[],
  showDataLabels: boolean,
): Data[] {
  const labels = items.map((item) => item.label);
  const traces: Data[] = fuelOrder.map((fuel) => ({
    type: "bar",
    name: fuel,
    x: labels,
    y: items.map((item) => item.fuelMix[fuel] ?? 0),
    marker: { color: fuelColor(fuel) },
    hovertemplate: `%{x}<br>${fuel}: %{y:,.0f} 台<extra></extra>`,
  }));
  if (showDataLabels) {
    traces.push({
      type: "scatter",
      mode: "text",
      name: "Total Labels",
      x: labels,
      y: items.map((item) => item.totalVolume),
      text: items.map((item) => formatVolume(item.totalVolume)),
      textposition: "top center",
      textfont: { size: 10, color: "#0f172a" },
      cliponaxis: false,
      hoverinfo: "skip",
      showlegend: false,
    });
  }
  return traces;
}

function fuelTrendYAxisMax(items: MarketScanFuelTrendItem[], showDataLabels: boolean): number {
  const maxTotal = items.reduce((max, item) => Math.max(max, item.totalVolume || 0), 0);
  if (maxTotal <= 0) {
    return 1;
  }
  return maxTotal * (showDataLabels ? 1.16 : 1.04);
}

function dominantFuelForRanking(item: MarketScanRankingItem): string {
  const entries = Object.entries(item.fuelMix ?? {});
  if (entries.length === 0) {
    return "ICE";
  }
  entries.sort((left, right) => Number(right[1]) - Number(left[1]));
  return entries[0]?.[0] ?? "ICE";
}

function driveShareText(item: MarketScanRankingItem): string {
  return `4WD ${driveShareDisplay(item)}`;
}

function channelMixText(item: MarketScanRankingItem): string {
  const total = Math.max(item.volume || 0, 0);
  const registrationMix = item.registrationMix ?? {};
  if (total <= 0 || Object.keys(registrationMix).length === 0) {
    return "渠道占比 暂无";
  }
  const order = ["Business", "Private", "Other"];
  const labels: Record<string, string> = {
    Business: "Business",
    Private: "Private",
    Other: "Other",
  };
  const parts = order
    .map((key) => {
      const volume = Number(registrationMix[key] ?? 0);
      if (!Number.isFinite(volume) || volume <= 0) {
        return null;
      }
      return `${labels[key]} ${formatPercent(volume / total)}`;
    })
    .filter((value): value is string => Boolean(value));
  return parts.length > 0 ? `渠道占比 ${parts.join(" · ")}` : "渠道占比 暂无";
}

function businessSharePct(item: MarketScanRankingItem): number {
  const total = Math.max(item.volume || 0, 0);
  const businessVolume = Number(item.registrationMix?.Business ?? 0);
  if (total <= 0 || !Number.isFinite(businessVolume) || businessVolume <= 0) {
    return 0;
  }
  return Math.max(0, Math.min(1, businessVolume / total));
}

function businessShareDisplay(item: MarketScanRankingItem): string {
  return formatPercent(businessSharePct(item));
}

function driveSharePct(item: MarketScanRankingItem): number {
  const pct =
    typeof item.driveSharePct === "number"
      ? item.driveSharePct
      : Number(item.driveMix?.["4WD"] ?? 0) / Math.max(item.volume || 1, 1);
  return Number.isFinite(pct) ? Math.max(0, pct) : 0;
}

function driveShareDisplay(item: MarketScanRankingItem): string {
  return item.driveShareDisplay ?? formatPercent(driveSharePct(item));
}

function marketShareLabel(item: MarketScanRankingItem): string {
  return `MS ${item.shareDisplay ?? formatPercent(item.sharePct)}`;
}

function monthlyBrandBreakdown(item: MarketScanRankingItem) {
  if (Array.isArray(item.modelBreakdown) && item.modelBreakdown.length > 0) {
    return item.modelBreakdown;
  }
  return [{ model: rankingItemLabel(item), volume: item.volume, sharePct: 1, powertrain: "OTHER" }];
}

function normalizeBreakdownPowertrain(powertrain?: string): string {
  const normalized = String(powertrain ?? "").trim().toUpperCase();
  return normalized || "OTHER";
}

function hexToRgb(hex: string): [number, number, number] | null {
  const normalized = hex.replace("#", "").trim();
  const expanded = normalized.length === 3
    ? normalized.split("").map((chunk) => `${chunk}${chunk}`).join("")
    : normalized;
  if (!/^[\da-fA-F]{6}$/.test(expanded)) {
    return null;
  }
  return [
    Number.parseInt(expanded.slice(0, 2), 16),
    Number.parseInt(expanded.slice(2, 4), 16),
    Number.parseInt(expanded.slice(4, 6), 16),
  ];
}

function mixHexColor(color: string, ratio: number, target: number): string {
  const rgb = hexToRgb(color);
  if (!rgb) {
    return color;
  }
  const mixed = rgb.map((channel) => Math.round(channel + (target - channel) * ratio));
  return `#${mixed.map((channel) => channel.toString(16).padStart(2, "0")).join("")}`;
}

function shadeHexColor(color: string, amount: number): string {
  if (amount === 0) {
    return color;
  }
  return amount > 0
    ? mixHexColor(color, Math.min(amount, 0.55), 255)
    : mixHexColor(color, Math.min(Math.abs(amount), 0.45), 0);
}

function breakdownColor(entry: { powertrain?: string }, index: number, total: number): string {
  const powertrain = normalizeBreakdownPowertrain(entry.powertrain);
  const baseColor = powertrain === "OTHER" ? "#94a3b8" : fuelColor(powertrain);
  if (total <= 1) {
    return baseColor;
  }

  const shadeOffsets = [0.24, 0.12, 0, -0.1, -0.2, -0.28, -0.34, -0.4, -0.46, -0.52];
  const shadeAmount = shadeOffsets[Math.min(index, shadeOffsets.length - 1)];
  return shadeHexColor(baseColor, shadeAmount);
}

function buildBreakdownColorMap(
  breakdown: Array<{ model: string; volume: number; sharePct: number; powertrain?: string }>,
): Record<string, string> {
  const powertrainBuckets = new Map<string, Array<{ key: string; entry: (typeof breakdown)[number] }>>();

  breakdown.forEach((entry) => {
    const powertrain = normalizeBreakdownPowertrain(entry.powertrain);
    const bucket = powertrainBuckets.get(powertrain) ?? [];
    bucket.push({ key: `${entry.model}::${entry.volume}`, entry });
    powertrainBuckets.set(powertrain, bucket);
  });

  const colorMap: Record<string, string> = {};
  powertrainBuckets.forEach((bucket) => {
    bucket.forEach(({ key, entry }, index) => {
      colorMap[key] = breakdownColor(entry, index, bucket.length);
    });
  });
  return colorMap;
}

const STACKED_FUEL_ORDER = ["ICE", "MHEV", "HEV", "PHEV", "BEV", "LPG"];

function buildTotalRankingChartData(
  items: MarketScanRankingItem[],
  showDataLabels = true,
): Data[] {
  const ordered = [...items].reverse();
  const labels = ordered.map((item) => rankingItemLabel(item));
  const totalVolumeInSegment = items.reduce((sum, item) => sum + (item.volume || 0), 0) || 1;
  const itemShares = ordered.map((item) => (
    typeof item.sharePct === "number" && Number.isFinite(item.sharePct)
      ? Math.max(item.sharePct, 0)
      : (item.volume || 0) / totalVolumeInSegment
  ));

  const traces: Data[] = STACKED_FUEL_ORDER.map((fuel) => {
    const volumes = ordered.map((item) => (item.fuelMix?.[fuel] ?? 0));
    const sharePcts = volumes.map((vol) => vol / totalVolumeInSegment);
    return {
      type: "bar" as const,
      orientation: "h" as const,
      name: fuel,
      x: sharePcts,
      y: labels,
      marker: { color: fuelColor(fuel) },
      customdata: volumes.map((vol, i) => [vol, ordered[i]?.volume ?? 0, channelMixText(ordered[i])]),
      hovertemplate: `%{y}<br>${fuel}: %{customdata[0]:,.0f} 台<br>总销量 %{customdata[1]:,.0f} 台<br>%{customdata[2]}<extra></extra>`,
    };
  });

  if (showDataLabels) {
    traces.push({
      type: "scatter",
      mode: "text",
      name: "Labels",
      x: itemShares,
      y: labels,
      text: ordered.map(
        (item) => `${marketShareLabel(item)} · ${formatVolume(item.volume)} · ${driveShareText(item)}`,
      ),
      textposition: "middle right",
      textfont: { size: 10, color: "#0f172a" },
      cliponaxis: false,
      hoverinfo: "skip",
      showlegend: false,
    });
  }

  return traces;
}

function normalizeMarketScanExportDimension(
  value: number | undefined,
  fallback: number,
  min: number,
): number {
  if (typeof value !== "number" || !Number.isFinite(value)) {
    return fallback;
  }
  return Math.max(min, Math.round(value));
}

function totalRankingXAxisMax(items: MarketScanRankingItem[]): number {
  if (items.length === 0) {
    return 1;
  }
  const totalVolumeInSegment = items.reduce((sum, item) => sum + (item.volume || 0), 0) || 1;
  const maxShare = items.reduce((max, item) => {
    const share =
      typeof item.sharePct === "number" && Number.isFinite(item.sharePct)
        ? Math.max(item.sharePct, 0)
        : (item.volume || 0) / totalVolumeInSegment;
    return Math.max(max, share);
  }, 0);
  if (maxShare <= 0) {
    return 1;
  }
  return Math.min(1, maxShare + Math.max(0.12, maxShare * 0.65));
}

function Panel({ eyebrow, title, subtitle, children, actions, className }: PanelProps) {
  return (
    <section className={`market-scan-panel${className ? ` ${className}` : ""}`}>
      <header className="market-scan-panel-head">
        <div>
          {eyebrow ? <span className="market-scan-panel-eyebrow">{eyebrow}</span> : null}
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        {actions ? <div className="market-scan-panel-actions">{actions}</div> : null}
      </header>
      <div className="market-scan-panel-body">{children}</div>
    </section>
  );
}

function InsightContent({
  insight,
  inline = false,
  dense = false,
  stacked = false,
}: {
  insight: MarketInsightSnapshot;
  inline?: boolean;
  dense?: boolean;
  stacked?: boolean;
}) {
  return (
    <div
      className={`market-scan-insight${inline ? " market-scan-insight--inline" : ""}${dense ? " market-scan-insight--dense" : ""}${stacked ? " market-scan-insight--stacked" : ""}`}
    >
      <div className="market-scan-insight-hero">
        {inline ? <span className="market-scan-panel-eyebrow">Insight</span> : null}
        <h3 className={`market-scan-insight-headline is-${insight.tone}`}>{insight.headline}</h3>
        <p className="market-scan-insight-summary">{insight.summary}</p>
      </div>
      <div className="market-scan-insight-grid">
        {insight.cards.map((card) => (
          <article
            key={card.label}
            className={`market-scan-insight-card is-${card.tone}`}
            title={`${card.label} · ${card.value} · ${card.detail}`}
          >
            <span className="market-scan-insight-card-label">{card.label}</span>
            <strong className="market-scan-insight-card-value">{card.value}</strong>
            <p className="market-scan-insight-card-detail">{card.detail}</p>
          </article>
        ))}
      </div>
    </div>
  );
}

function ConclusionInsightPanel({
  title,
  insight,
  below,
}: {
  title: string;
  insight: MarketInsightSnapshot;
  below?: ReactNode;
}) {
  return (
    <Panel
      eyebrow="Conclusion"
      title={title}
      className="market-scan-panel--insight-compact market-scan-panel--insight-summary"
    >
      <div className="market-scan-insight-summary-layout">
        <InsightContent insight={insight} dense />
        {below ? <div className="market-scan-insight-channel-block">{below}</div> : null}
      </div>
    </Panel>
  );
}

function MetricCard({
  label,
  value,
  detail,
  tone,
}: {
  label: string;
  value: string;
  detail: string;
  tone?: string;
}) {
  return (
    <div className={`market-scan-metric-card ${toneClassName(tone)}`}>
      <span className="market-scan-metric-label">{label}</span>
      <strong className="market-scan-metric-value">{value}</strong>
      <span className="market-scan-metric-detail">{detail}</span>
    </div>
  );
}

function RankingGroup({
  group,
  compact = false,
  fuelType,
}: {
  group: MarketScanRankingGroup;
  compact?: boolean;
  fuelType?: string;
}) {
  if (group.items.length === 0) {
    return <div className="market-scan-empty">暂无排行数据。</div>;
  }

  const barColor = fuelType ? fuelColor(fuelType) : "#0f766e";
  const isFuelRanking = Boolean(fuelType);

  if (compact) {
    return (
      <div
        className={`market-scan-ranking-list market-scan-ranking-scrollable${isFuelRanking ? " market-scan-ranking-list--fuel" : ""}`}
      >
        {group.items.map((item) => {
          const currentDriveSharePct = driveSharePct(item);
          const currentBusinessSharePct = businessSharePct(item);
          const hasDriveShare = typeof item.driveSharePct === "number" || item.driveMix?.["4WD"] !== undefined;
          const driveShareLabel = driveShareDisplay(item);
          const shareLabel = marketShareLabel(item);
          const hoverTitle = [
            shareLabel,
            hasDriveShare ? `4WD ${driveShareLabel}` : null,
            `Business ${businessShareDisplay(item)}`,
            channelMixText(item),
          ]
            .filter((value): value is string => Boolean(value))
            .join(" · ");

          return (
            <article
              key={`${rankingItemLabel(item)}-${item.rank}`}
              className={`market-scan-ranking-row${isFuelRanking ? " market-scan-ranking-row--fuel" : ""}`}
            >
              <div className="market-scan-ranking-row-rank">{String(item.rank).padStart(2, "0")}</div>
              <div className="market-scan-ranking-row-info">
                <div className="market-scan-ranking-row-head">
                  <span className="market-scan-ranking-row-name">{rankingItemLabel(item)}</span>
                  <div className="market-scan-ranking-row-nums">
                    <span>{formatVolume(item.volume)}</span>
                    <span className="market-scan-tag">{shareLabel}</span>
                  </div>
                </div>
                <div
                  className="market-scan-ranking-row-bar"
                  title={hoverTitle}
                >
                  <span
                    className="market-scan-ranking-row-bar-fill"
                    style={{ width: `${Math.max(1, item.barPct * 100)}%`, background: barColor }}
                  >
                    {hasDriveShare ? (
                      <span
                        className="market-scan-4wd-fill"
                        style={{ width: `${currentDriveSharePct * 100}%` }}
                      />
                    ) : null}
                    <span
                      className="market-scan-business-marker"
                      style={{ left: `${Math.max(0, Math.min(100, currentBusinessSharePct * 100))}%` }}
                      title={`Business ${businessShareDisplay(item)}`}
                    />
                  </span>
                </div>
              </div>
              <div className="market-scan-ranking-row-side">
                <span className={`market-scan-tone-text ${toneClassName(item.yoy.tone)}`}>
                  YoY {item.yoy.tone === "positive" ? "▲ " : item.yoy.tone === "negative" ? "▼ " : ""}
                  {item.yoy.display}
                </span>
                {item.mom ? (
                  <span className={`market-scan-tone-text ${toneClassName(item.mom.tone)}`}>
                    MoM {item.mom.tone === "positive" ? "▲ " : item.mom.tone === "negative" ? "▼ " : ""}
                    {item.mom.display}
                  </span>
                ) : null}
                {hasDriveShare ? (
                  <span className="market-scan-ranking-row-drive-chip">
                    <span className="market-scan-ranking-row-drive-chip-label">4WD</span>
                    <strong className="market-scan-ranking-row-drive-chip-value">{driveShareLabel}</strong>
                  </span>
                ) : null}
              </div>
            </article>
          );
        })}
      </div>
    );
  }

  return (
    <div className="market-scan-ranking-stack market-scan-ranking-scrollable">
      {group.items.map((item) => (
        <article key={`${rankingItemLabel(item)}-${item.rank}`} className="market-scan-ranking-card">
          <div className="market-scan-ranking-main">
            <div className="market-scan-ranking-rank">{String(item.rank).padStart(2, "0")}</div>
            <div className="market-scan-ranking-copy">
              <strong>{rankingItemLabel(item)}</strong>
              <span>
                {group.currentLabel ? `${group.currentLabel} ` : ""}
                {formatVolume(item.volume)} 台
                {item.shareDisplay ? ` · ${item.shareDisplay}` : ""}
              </span>
            </div>
          </div>
          <div className="market-scan-ranking-side">
            <span className={`market-scan-tone-pill ${toneClassName(item.yoy.tone)}`}>
              YoY {item.yoy.display}
            </span>
            {item.mom ? (
              <span className={`market-scan-tone-pill ${toneClassName(item.mom.tone)}`}>
                MoM {item.mom.display}
              </span>
            ) : null}
          </div>
          <div className="market-scan-ranking-bar">
            <span style={{ width: `${Math.max(5, item.barPct * 100)}%`, background: barColor }} />
          </div>
        </article>
      ))}
    </div>
  );
}

function BrandModelRankingGroup({
  group,
  compact = false,
  onBrandClick,
}: {
  group: MarketScanRankingGroup;
  compact?: boolean;
  onBrandClick?: (brand: string, sourceTable: string) => void;
}) {
  if (group.items.length === 0) {
    return <div className="market-scan-empty">暂无排行数据。</div>;
  }

  return (
    <div
      className={`market-scan-ranking-list market-scan-ranking-scrollable market-scan-ranking-list--monthly${
        compact ? " market-scan-ranking-list--monthly-compact" : ""
      }`}
    >
      {group.items.map((item) => {
        const breakdown = monthlyBrandBreakdown(item);
        const breakdownColorMap = buildBreakdownColorMap(breakdown);
        const breakdownTitle = breakdown
          .map((entry) => {
            const powertrain = normalizeBreakdownPowertrain(entry.powertrain);
            return `${entry.model} · ${powertrain}: ${formatVolume(entry.volume)} 台 (${formatPercent(entry.sharePct)})`;
          })
          .join(" | ");

        const label = rankingItemLabel(item);
        return (
          <article
            key={`${label}-${item.rank}`}
            className="market-scan-ranking-row market-scan-ranking-row--monthly market-scan-ranking-row--clickable"
            title={`View ${label} trend →`}
            role="button"
            tabIndex={0}
            onClick={() => onBrandClick?.(label, group.title.includes("YTD") ? "ytd_brand_ranking" : "monthly_brand_ranking")}
            onKeyDown={(e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); onBrandClick?.(label, group.title.includes("YTD") ? "ytd_brand_ranking" : "monthly_brand_ranking"); } }}
          >
            <div className="market-scan-ranking-row-rank">{String(item.rank).padStart(2, "0")}</div>
            <div className="market-scan-ranking-row-info">
              <div className="market-scan-ranking-row-head">
                <span className="market-scan-ranking-row-name">{label}</span>
                <div className="market-scan-ranking-row-nums">
                  <span>{formatVolume(item.volume)}</span>
                  <span className="market-scan-tag">{marketShareLabel(item)}</span>
                </div>
              </div>
              <div className="market-scan-ranking-row-bar market-scan-ranking-row-bar--monthly" title={breakdownTitle}>
                <span
                  className="market-scan-ranking-row-bar-fill market-scan-ranking-row-bar-fill--monthly"
                  style={{ width: `${Math.max(1, item.barPct * 100)}%` }}
                >
                  {breakdown.map((entry) => (
                    <span
                      key={`${item.rank}-${entry.model}`}
                      className="market-scan-model-segment"
                      style={{
                        width: `${Math.max(entry.sharePct * 100, 0)}%`,
                        background: breakdownColorMap[`${entry.model}::${entry.volume}`],
                      }}
                    />
                  ))}
                </span>
              </div>
              <div className="market-scan-model-breakdown">
                {breakdown.map((entry) => (
                  <span
                    key={`${item.rank}-chip-${entry.model}`}
                    className="market-scan-model-breakdown-chip"
                    title={`${entry.model} · ${normalizeBreakdownPowertrain(entry.powertrain)} · ${formatVolume(entry.volume)} 台 · ${formatPercent(entry.sharePct)}`}
                  >
                    <span
                      className="market-scan-model-breakdown-swatch"
                      style={{ background: breakdownColorMap[`${entry.model}::${entry.volume}`] }}
                    />
                    <span className="market-scan-model-breakdown-label">{entry.model}</span>
                  </span>
                ))}
              </div>
            </div>
            <div className="market-scan-ranking-row-side">
              <span className={`market-scan-tone-text ${toneClassName(item.yoy.tone)}`}>
                YoY {item.yoy.tone === "positive" ? "▲ " : item.yoy.tone === "negative" ? "▼ " : ""}
                {item.yoy.display}
              </span>
              {item.mom ? (
                <span className={`market-scan-tone-text ${toneClassName(item.mom.tone)}`}>
                  MoM {item.mom.tone === "positive" ? "▲ " : item.mom.tone === "negative" ? "▼ " : ""}
                  {item.mom.display}
                </span>
              ) : null}
            </div>
          </article>
        );
      })}
    </div>
  );
}

function MatrixTable({ matrix }: { matrix: MarketScanMatrix }) {
  if (matrix.rows.length === 0) {
    return <div className="market-scan-empty">暂无矩阵数据。</div>;
  }

  return (
    <div className="market-scan-table-wrap">
      <table className="market-scan-matrix-table">
        <thead>
          <tr>
            <th>Metric</th>
            {matrix.columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {matrix.rows.map((row) => (
            <tr key={row.metricKey}>
              <th>{row.label}</th>
              {row.cells.map((cell) => (
                <td key={`${row.metricKey}-${cell.key}`} className={toneClassName(cell.tone)}>
                  {cell.display}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function OverviewTrendShareRows({
  items,
  fuel,
  fontSize,
}: {
  items: MarketScanOverviewTrendItem[];
  fuel: string | null;
  fontSize: number;
}) {
  if (!fuel) {
    return null;
  }

  const trailingItems = overviewTrendTrailingItems(items);
  if (trailingItems.length === 0) {
    return null;
  }

  const rows = [
    {
      key: "market",
      label: "全市场",
      values: trailingItems.map((item) => safeShareValue(item.fuelMix?.[fuel] ?? 0, item.totalVolume)),
    },
    {
      key: "suv",
      label: "SUV内",
      values: trailingItems.map((item) => safeShareValue(item.suvFuelMix?.[fuel] ?? Number.NaN, item.suvTotalVolume ?? 0)),
    },
  ];
  const style = {
    "--trend-left": `${MARKET_SCAN_OVERVIEW_TREND_MARGIN.l}px`,
    "--trend-right": `${MARKET_SCAN_OVERVIEW_TREND_MARGIN.r}px`,
    "--trend-font-size": `${Math.max(9, fontSize)}px`,
    "--trend-column-count": String(trailingItems.length),
  } as CSSProperties;

  return (
    <div className="market-scan-trend-share-table" style={style} aria-label={`${fuel} 动力占比`}>
      {rows.map((row) => (
        <div key={row.key} className="market-scan-trend-share-row">
          <span
            className="market-scan-trend-share-label"
            title={row.key === "market" ? `${fuel} 总市场占比` : `${fuel} SUV 内占比`}
          >
            {row.label}
          </span>
          {row.values.map((value, index) => (
            <span
              key={`${row.key}-${trailingItems[index].period}`}
              className="market-scan-trend-share-cell"
              style={{ gridColumn: index + 2 }}
            >
              {formatPercent(value)}
            </span>
          ))}
        </div>
      ))}
    </div>
  );
}

function OverviewSection({
  labels: _labels,
  page,
  fuelOrder,
  salesMode,
  customRangeActive = false,
  timeRange = null,
  showDataLabels,
  exportSettings,
  compact = false,
  onBrandClick,
}: {
  labels: MarketScanDeckResponse["metadata"]["labels"];
  page: MarketScanOverviewPage;
  fuelOrder: string[];
  salesMode: MarketScanSalesMode;
  customRangeActive?: boolean;
  timeRange?: MarketScanPeriodRange | null;
  showDataLabels: boolean;
  exportSettings: ExportSettings;
  compact?: boolean;
  onBrandClick?: (brand: string, sourceTable: string) => void;
}) {
  const insight = buildOverviewInsight(page);
  const rankingGroups = marketScanOverviewRankingGroups(page, salesMode, customRangeActive);
  const trendItems = customRangeActive
    ? page.trend.items.filter((item) => periodWithinRange(item.period, timeRange))
    : page.trend.items;
  const trendChartRef = useRef<HTMLDivElement | null>(null);
  const [focusedTrendFuel, setFocusedTrendFuel] = useState<string | null>(null);
  async function handleExportChartPng(ref: React.RefObject<HTMLDivElement | null>, filename: string) {
    const el = ref.current;
    if (!el) return;
    try {
      const { toPng } = await import("html-to-image");
      const dataUrl = await toPng(el, { cacheBust: true, pixelRatio: 2, backgroundColor: "#ffffff" });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = `${filename}.png`;
      link.click();
    } catch (err) { console.warn("Chart export failed", err); }
  }
  const activeTrendShareFuel = focusedTrendFuel && fuelOrder.includes(focusedTrendFuel)
    ? focusedTrendFuel
    : fuelOrder.length === 1
      ? fuelOrder[0]
      : null;
  const handleTrendLegendDoubleClick = (event: ReactMouseEvent<HTMLDivElement>): void => {
    const fuel = readMarketScanLegendFuelFromTarget(event.target, fuelOrder);
    if (fuel) {
      setFocusedTrendFuel(fuel);
    }
  };

  return (
    <div className="market-scan-grid market-scan-grid--three">
      <Panel
        eyebrow="Trend"
        title="Rolling 12M Volume / Powertrain"
        subtitle="上方保留 Rolling 12M 双柱趋势；下方直接汇总结论、结构驱动与下月观察点。"
        actions={<button type="button" className="btn btn-ghost btn-sm" onClick={() => { void handleExportChartPng(trendChartRef, "trend-rolling12m"); }}>Export PNG</button>}
      >
        <div className="market-scan-overview-trend-stack">
          <div className="market-scan-overview-trend-chart" ref={trendChartRef} onDoubleClickCapture={handleTrendLegendDoubleClick}>
            <PlotlyChart
              data={applyOverviewTrendExportToTraces(
                buildOverviewTrendData(trendItems, fuelOrder, showDataLabels),
                exportSettings,
              )}
              layout={applyMarketScanExportToLayout({
                ...CHART_LAYOUT,
                margin: MARKET_SCAN_OVERVIEW_TREND_MARGIN,
                barmode: "stack",
                xaxis: { type: "category" },
                yaxis: { title: { text: "销量" } },
              }, exportSettings)}
              height={compact ? 330 : 420}
            />
          </div>
          <OverviewTrendShareRows
            items={trendItems}
            fuel={activeTrendShareFuel}
            fontSize={exportSettings.fontSize}
          />
          <InsightContent insight={insight} inline />
        </div>
      </Panel>

      {rankingGroups.map((group, index) => (
        <Panel
          key={group.title}
          eyebrow={index === 0 ? "Ranking · Active" : "Ranking · Alt"}
          title={group.title}
        >
          <BrandModelRankingGroup group={group} compact={compact}
            onBrandClick={onBrandClick} />
        </Panel>
      ))}
    </div>
  );
}

function OriginSection({
  page,
  salesMode,
  customRangeActive = false,
  timeRange = null,
  showDataLabels,
  exportSettings,
  compact = false,
}: {
  page: MarketScanDeckResponse["results"]["origin"];
  salesMode: MarketScanSalesMode;
  customRangeActive?: boolean;
  timeRange?: MarketScanPeriodRange | null;
  showDataLabels: boolean;
  exportSettings: ExportSettings;
  compact?: boolean;
}) {
  const insight = buildOriginInsight(page);
  const filteredMatrix = customRangeActive
    ? {
        ...page.matrix,
        rows: [page.customRangeMatrixRow, page.customRangeYoYMatrixRow].filter(
          (row): row is MarketScanMatrixRow => Boolean(row),
        ),
      }
    : filterMatrixBySalesMode(page.matrix, salesMode);
  const trendSeries = customRangeActive
    ? page.trend.series.map((series) => ({
        ...series,
        points: series.points.filter((point) => periodWithinRange(point.period, timeRange)),
      }))
    : page.trend.series;
  const brandTrendGroups = customRangeActive
    ? page.brandTrend.groups.map((group) => ({
        ...group,
        series: group.series.map((series) => ({
          ...series,
          points: series.points.filter((point) => periodWithinRange(point.period, timeRange)),
        })),
      }))
    : page.brandTrend.groups;
  const trendPanel = (
    <Panel
      eyebrow="Trend"
      title="Origin Volume Trend"
      subtitle="欧系、日系、韩系、美系、中系与其他车系的月度走势。"
    >
      <PlotlyChart
        data={applyMarketScanExportToTraces(buildOriginTrendData(trendSeries, showDataLabels), exportSettings)}
        layout={applyMarketScanExportToLayout({
          ...CHART_LAYOUT,
          xaxis: { type: "category" },
          yaxis: { title: { text: "销量" } },
        }, exportSettings)}
        height={compact ? 282 : 420}
      />
      </Panel>
  );
  const selectedBrandTrendGroups = (() => {
    const rankedOrigins = [...trendSeries]
      .map((entry) => ({
        origin: entry.origin,
        latestVolume: entry.points[entry.points.length - 1]?.volume ?? 0,
      }))
      .sort((left, right) => right.latestVolume - left.latestVolume);
    const topOrigin = rankedOrigins[0]?.origin ?? null;
    const preferredOrigins = [topOrigin, "中系"].filter(
      (origin, index, items): origin is string => Boolean(origin) && items.indexOf(origin) === index,
    );
    const selected = preferredOrigins
      .map((origin) => brandTrendGroups.find((group) => group.origin === origin))
      .filter((group): group is NonNullable<typeof group> => Boolean(group));
    return selected.length > 0 ? selected : brandTrendGroups.slice(0, 2);
  })();
  const brandTrendPanel =
    selectedBrandTrendGroups.length > 0 ? (
      <Panel
        eyebrow="Trend"
        title="Origin Brand Trend"
        subtitle="默认展示当前第一名车系与中系品牌走势，各自保留最新月销量 Top 4 品牌。"
      >
        <div className="market-scan-grid market-scan-grid--two">
          {selectedBrandTrendGroups.map((group) => (
            <div key={group.origin} className="market-scan-subpanel">
              <h3>{group.origin}</h3>
              <PlotlyChart
                data={applyMarketScanExportToTraces(buildOriginBrandTrendData(group, showDataLabels), exportSettings)}
                layout={applyMarketScanExportToLayout({
                  ...CHART_LAYOUT,
                  xaxis: { type: "category" },
                  yaxis: { title: { text: "销量" } },
                  legend: {
                    orientation: "h",
                    y: -0.22,
                    x: 0.5,
                    xanchor: "center",
                    font: { size: 10 },
                  },
                }, exportSettings)}
                height={compact ? 220 : 260}
              />
            </div>
          ))}
        </div>
      </Panel>
    ) : null;
  const matrixPanel = (
    <Panel
      eyebrow="Matrix"
      title="Origin Scorecard"
      subtitle={
        customRangeActive
          ? "自定义区间累计与同比矩阵。"
          : salesMode === "month"
          ? "当月、环比、同比矩阵。"
          : salesMode === "ytd"
            ? "YTD、YTD同比矩阵。"
            : "近12个月、近12个月同比矩阵。"
      }
    >
      <MatrixTable matrix={filteredMatrix} />
    </Panel>
  );

  return (
    <>
      <ConclusionInsightPanel title="Origin Insight" insight={insight} />
      {compact ? (
        <>
          <div className="market-scan-grid market-scan-grid--two-wide">
            {trendPanel}
            {matrixPanel}
          </div>
          {brandTrendPanel}
        </>
      ) : (
        <>
          {trendPanel}
          {brandTrendPanel}
          {matrixPanel}
        </>
      )}
    </>
  );
}

function SegmentSection({
  page,
  salesMode,
  customRangeActive = false,
  timeRange = null,
  showDataLabels,
  labelDigits = 1,
  exportSettings,
  compact = false,
}: {
  page: MarketScanSegmentPage;
  salesMode: MarketScanSalesMode;
  customRangeActive?: boolean;
  timeRange?: MarketScanPeriodRange | null;
  showDataLabels: boolean;
  labelDigits?: number;
  exportSettings: ExportSettings;
  compact?: boolean;
}) {
  const insight = buildSegmentInsight(page);
  const filteredMatrix = filterMatrixBySalesMode(page.matrix, salesMode, customRangeActive);
  const bodyShareItems = customRangeActive
    ? page.bodyShareTrend.items.filter((item) => periodWithinRange(item.period, timeRange))
    : page.bodyShareTrend.items;
  const suvSegmentItems = customRangeActive
    ? page.suvSegmentShareTrend.items.filter((item) => periodWithinRange(item.period, timeRange))
    : page.suvSegmentShareTrend.items;
  const [selectedChannelView, setSelectedChannelView] = useState(DEFAULT_MARKET_SCAN_CHANNEL_VIEW);
  const channelOptions = marketScanChannelOptions(page);
  const channelWindow = marketScanActiveSegmentChannelWindow(page, salesMode, customRangeActive);
  const activeChannelView = marketScanActiveSegmentChannelView(channelWindow, selectedChannelView);
  const activeChannelOption =
    channelOptions.find((option) => option.value === activeChannelView.viewKey)
    ?? channelOptions[0]
    ?? DEFAULT_MARKET_SCAN_CHANNEL_OPTIONS[0];
  const channelMixItems = activeChannelView.items;
  const hasChannelMix = channelMixItems.some((item) =>
    REGISTRATION_CHANNEL_ORDER.some((channel) => Number(item.channelMix?.[channel] ?? 0) > 0),
  );
  const channelChartHeight = compact
    ? (activeChannelView.viewKey === "overall" ? 78 : 112)
    : (activeChannelView.viewKey === "overall" ? 132 : 180);
  const trendChartHeight = compact ? 226 : 400;
  return (
    <>
      <ConclusionInsightPanel
        title="Segment Insight"
        insight={insight}
        below={hasChannelMix ? (
          <>
            <div className="market-scan-insight-channel-head">
              <div>
                <span className="market-scan-panel-eyebrow">Channel</span>
                <h3>{activeChannelOption.label}渠道结构</h3>
                <p>{activeChannelView.title || "整体市场 Business / Private / Other 渠道结构。"}</p>
              </div>
              <label className="market-scan-channel-view-control">
                <span>View</span>
                <select
                  value={activeChannelView.viewKey}
                  onChange={(event) => setSelectedChannelView(event.target.value)}
                >
                  {channelOptions.map((option) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
              </label>
            </div>
            <PlotlyChart
              data={applyMarketScanExportToTraces(
                buildRegistrationChannelChartData(channelMixItems, showDataLabels),
                exportSettings,
                { rewriteDataLabels: false },
              )}
              layout={applyMarketScanExportToLayout({
                ...CHART_LAYOUT,
                barmode: "stack",
                margin: { l: 88, r: 10, t: 4, b: 22 },
                xaxis: {
                  title: { text: "" },
                  tickformat: ".0%",
                  range: [0, 1],
                  automargin: true,
                  fixedrange: true,
                },
                yaxis: {
                  automargin: true,
                  autorange: "reversed",
                  fixedrange: true,
                },
                legend: {
                  orientation: "h",
                  y: 1.14,
                  x: 0,
                  xanchor: "left",
                  font: { size: 9 },
                },
              }, exportSettings)}
              height={channelChartHeight}
            />
          </>
        ) : (
          <div className="market-scan-empty">当前筛选下暂无渠道拆分数据。</div>
        )}
      />
      <div className="market-scan-grid market-scan-grid--two market-scan-segment-trend-grid">
        <Panel
          eyebrow="Trend"
          title="SUV vs Sedan Share"
          subtitle="观察车身结构的月度切换。"
        >
          <PlotlyChart
            data={applyMarketScanExportToTraces(
              buildBodyShareData(bodyShareItems, showDataLabels, labelDigits),
              exportSettings,
              { rewriteDataLabels: false },
            )}
            layout={applyMarketScanExportToLayout({
              ...CHART_LAYOUT,
              xaxis: { type: "category" },
              yaxis: { title: { text: "占比" }, tickformat: ".0%", range: [0, 1] },
            }, exportSettings)}
            height={trendChartHeight}
          />
        </Panel>
        <Panel
          eyebrow="Trend"
          title="SUV Segment Share"
          subtitle="把 SUV 市占拆成 SUV-A00 / SUV-A0 / SUV-A / ≥SUV-B。"
        >
          <PlotlyChart
            data={applyMarketScanExportToTraces(
              buildSuvSegmentShareData(suvSegmentItems, showDataLabels, labelDigits),
              exportSettings,
              { rewriteDataLabels: false },
            )}
            layout={applyMarketScanExportToLayout({
              ...CHART_LAYOUT,
              xaxis: { type: "category" },
              yaxis: { title: { text: "占比" }, tickformat: ".0%", range: [0, 1] },
            }, exportSettings)}
            height={trendChartHeight}
          />
        </Panel>
      </div>
      <Panel
        eyebrow="Matrix"
        title="Segment Matrix"
        className="market-scan-panel--matrix-compact"
        subtitle={
          customRangeActive
            ? "不同长度级别的自定义区间累计与同比表现。"
            : salesMode === "month"
            ? "不同长度级别的当月、环比、同比表现。"
            : salesMode === "ytd"
              ? "不同长度级别的 YTD 与 YTD 同比表现。"
              : "不同长度级别的近12个月与近12个月同比表现。"
        }
      >
        <MatrixTable matrix={filteredMatrix} />
      </Panel>
    </>
  );
}

function FuelPanel({
  panel,
  salesMode,
  customRangeActive = false,
  compact = false,
}: {
  panel: MarketScanFuelPanel;
  salesMode: MarketScanSalesMode;
  customRangeActive?: boolean;
  compact?: boolean;
}) {
  const dense = true;
  const activeWindow = marketScanActiveFuelPanelWindow(panel, salesMode, customRangeActive);
  const activeTitle = activeWindow.title;
  const activeRanking = activeWindow.ranking;

  return (
    <Panel
      eyebrow={panel.fuelType}
      title={`${panel.fuelType} Share Ranking`}
      subtitle={activeTitle}
    >
      <RankingGroup
        group={{ title: activeTitle, currentLabel: activeTitle, items: activeRanking }}
        compact={dense || compact}
        fuelType={panel.fuelType}
      />
    </Panel>
  );
}

function DrilldownSection({
  page,
  fuelOrder,
  salesMode,
  customRangeActive = false,
  customRangeLabel,
  showDataLabels,
  exportSettings,
  compact = false,
  rankingLimit = 10,
  onRankingLimitChange,
}: {
  page: MarketScanDrilldownPage;
  fuelOrder: string[];
  salesMode: MarketScanSalesMode;
  customRangeActive?: boolean;
  customRangeLabel?: string;
  showDataLabels: boolean;
  exportSettings: ExportSettings;
  compact?: boolean;
  rankingLimit?: number;
  onRankingLimitChange?: (limit: number) => void;
}) {
  const normalizedRankingLimit = normalizeMarketScanRankingLimit(rankingLimit);
  const insight = buildDrilldownInsight(page);
  const activeWindow = marketScanActiveDrilldownWindow(page, salesMode, customRangeActive, customRangeLabel);
  const activeTotalRanking = activeWindow.ranking;
  const activeFuelTrend = activeWindow.fuelTrend;
  const activeFuelTrendTitle = activeWindow.trendTitle;
  const activeFuelTrendSubtitle = activeWindow.trendSubtitle;
  const activeFuelTrendYAxisTitle = activeWindow.trendYAxisTitle;

  return (
    <>
      <div className="market-scan-grid market-scan-grid--drilldown-top">
        <Panel
          eyebrow="Ranking"
          title={activeTotalRanking.title}
          subtitle={activeWindow.rankingSubtitle}
          actions={onRankingLimitChange ? (
            <label className="market-scan-ranking-limit-control">
              Top
              <select
                value={normalizedRankingLimit}
                onChange={(e) => onRankingLimitChange(normalizeMarketScanRankingLimit(e.target.value))}
              >
                {MARKET_SCAN_RANKING_LIMIT_OPTIONS.map((n) => (
                  <option key={n} value={n}>{n}</option>
                ))}
              </select>
            </label>
          ) : null}
        >
          {activeTotalRanking.items.length > 0 ? (
            <div className="market-scan-ranking-chart-shell">
              <PlotlyChart
                data={applyMarketScanExportToTraces(
                  buildTotalRankingChartData(activeTotalRanking.items, showDataLabels),
                  exportSettings,
                  { rewriteDataLabels: false },
                )}
                layout={applyMarketScanExportToLayout({
                  ...CHART_LAYOUT,
                  barmode: "stack",
                  margin: { l: 88, r: 96, t: 12, b: 26 },
                  xaxis: {
                    title: { text: "" },
                    tickformat: ".0%",
                    range: [0, totalRankingXAxisMax(activeTotalRanking.items)],
                    automargin: true,
                    fixedrange: true,
                  },
                  yaxis: { automargin: true, fixedrange: true },
                  legend: { orientation: "h", y: -0.1, x: 0.5, xanchor: "center", font: { size: 9 } },
                }, exportSettings)}
                height={compact ? 236 : 332}
              />
            </div>
          ) : (
            <div className="market-scan-empty">暂无车型排行。</div>
          )}
        </Panel>
        <Panel
          eyebrow="Trend"
          title={activeFuelTrendTitle}
          subtitle={activeFuelTrendSubtitle}
        >
          <PlotlyChart
            data={applyMarketScanExportToTraces(
              buildFuelTrendData(activeFuelTrend.items, fuelOrder, showDataLabels),
              exportSettings,
              { rewriteDataLabels: false },
            )}
            layout={applyMarketScanExportToLayout({
              ...CHART_LAYOUT,
              barmode: "stack",
                margin: { l: 54, r: 16, t: 22, b: 36 },
                xaxis: { type: "category", automargin: true, fixedrange: true },
                yaxis: {
                  title: { text: activeFuelTrendYAxisTitle },
                  range: [0, fuelTrendYAxisMax(activeFuelTrend.items, showDataLabels)],
                  automargin: true,
                  fixedrange: true,
                },
            }, exportSettings)}
            height={compact ? 236 : 332}
          />
        </Panel>

        <Panel
          eyebrow="Conclusion"
          title={`${page.segmentLabel} Insight`}
          className="market-scan-panel--insight-compact market-scan-panel--insight-fill"
        >
          <InsightContent insight={insight} dense stacked />
        </Panel>
      </div>
      <div className="market-scan-grid market-scan-grid--five market-scan-fuel-panel-row">
        {page.fuelPanels.map((panel) => (
          <FuelPanel
            key={`${page.segment}-${panel.fuelType}`}
            panel={panel}
            salesMode={salesMode}
            customRangeActive={customRangeActive}
            compact={compact}
          />
        ))}
      </div>
      {page.originFuelPanels?.map((og) => (
        <div
          key={`${page.segment}-${og.origin}`}
          className="market-scan-grid market-scan-grid--five market-scan-fuel-panel-row"
        >
          {og.fuelPanels.map((panel) => (
            <FuelPanel
              key={`${page.segment}-${og.origin}-${panel.fuelType}`}
              panel={panel}
              salesMode={salesMode}
              customRangeActive={customRangeActive}
              compact={compact}
            />
          ))}
        </div>
      ))}
    </>
  );
}

export function MarketScanPage({
  initialActivePage,
  initialDrilldownSegment,
}: {
  initialActivePage?: MarketScanPageKey;
  initialDrilldownSegment?: string;
} = {}) {
  const [searchParams, setSearchParams] = useSearchParams();
  const marketScanLabelModeOptions = useMemo(
    () => buildExportLabelModeOptions({ showValue: true, showSeries: false }),
    [],
  );
  const [deck, setDeck] = useState<MarketScanDeckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportSettings, setExportSettings] = useState<ExportSettings>({ ...DEFAULT_MARKET_SCAN_EXPORT });
  const [exportToolsOpen, setExportToolsOpen] = useState(false);
  const [trendDrawer, setTrendDrawer] = useState<{
    open: boolean; brand: string; model?: string; sourceTable: string;
  }>({ open: false, brand: "", sourceTable: "monthly_brand_ranking" });
  const [slideEditMode, setSlideEditMode] = useState(false);
  const [slideLayouts, setSlideLayouts] = useState<Record<MarketScanPageKey, SlideLayoutSettings>>(
    () => readStoredSlideLayouts(
      MARKET_SCAN_SLIDE_LAYOUT_STORAGE_KEY,
      buildDefaultMarketScanSlideLayouts(),
    ),
  );
  const [heroCollapsed, setHeroCollapsed] = useState(false);
  const [activePage, setActivePage] = useState<MarketScanPageKey>(
    () => {
      if (initialActivePage) return initialActivePage;
      const requestedPage = searchParams.get("activePage");
      return isMarketScanPageKey(requestedPage) ? requestedPage : "overview";
    },
  );
  const [selectedCountry, setSelectedCountry] = useState<string | null>(
    () => searchParams.get("country") || DEFAULT_MARKET_SCAN_COUNTRY,
  );
  const [selectedPeriod, setSelectedPeriod] = useState<string | null>(
    () => searchParams.get("period"),
  );
  const [selectedTimeRange, setSelectedTimeRange] = useState<MarketScanPeriodRange | null>(
    () => readSearchTimeRange(searchParams),
  );
  const [salesMode, setSalesMode] = useState<MarketScanSalesMode>(
    () => {
      const requestedMode = searchParams.get("salesMode");
      return isMarketScanSalesMode(requestedMode) ? requestedMode : DEFAULT_MARKET_SCAN_SALES_MODE;
    },
  );
  const [selectedFuelTypes, setSelectedFuelTypes] = useState<string[]>(
    () => {
      const ft = searchParams.get("fuelTypes");
      return ft ? ft.split(",") : DEFAULT_FUEL_TYPES;
    },
  );
  const [selectedDrilldownSegment, setSelectedDrilldownSegment] = useState(
    () => initialDrilldownSegment || searchParams.get("drilldownSegment") || "SUV A0",
  );
  const [rankingLimit, setRankingLimit] = useState(
    () => {
      const rl = searchParams.get("rankingLimit");
      return normalizeMarketScanRankingLimit(rl);
    },
  );
  const [reloadToken, setReloadToken] = useState(0);
  const requestRef = useRef(0);
  const slideRef = useRef<HTMLDivElement | null>(null);
  const countryOptions = deck?.metadata.availableCountries ?? [];

  // Sync filter state back to URL search params
  const syncUrlParams = useCallback(() => {
    const params = new URLSearchParams();
    if (selectedCountry) params.set("country", selectedCountry);
    if (selectedPeriod) params.set("period", selectedPeriod);
    if (selectedTimeRange) {
      params.set("timeStart", selectedTimeRange.start);
      params.set("timeEnd", selectedTimeRange.end);
    }
    if (activePage !== "overview") params.set("activePage", activePage);
    if (salesMode !== DEFAULT_MARKET_SCAN_SALES_MODE) params.set("salesMode", salesMode);
    if (selectedDrilldownSegment !== "SUV A0") params.set("drilldownSegment", selectedDrilldownSegment);
    if (rankingLimit !== MIN_MARKET_SCAN_RANKING_LIMIT) params.set("rankingLimit", String(rankingLimit));
    const ft = selectedFuelTypes.slice().sort().join(",");
    const defaultFt = DEFAULT_FUEL_TYPES.slice().sort().join(",");
    if (ft !== defaultFt) params.set("fuelTypes", selectedFuelTypes.join(","));
    setSearchParams(params, { replace: true });
  }, [activePage, rankingLimit, salesMode, selectedCountry, selectedDrilldownSegment, selectedFuelTypes, selectedPeriod, selectedTimeRange, setSearchParams]);

  useEffect(() => {
    syncUrlParams();
  }, [syncUrlParams]);

  // Defer Plotly preload to idle time — avoids blocking first-screen JS parse
  useEffect(() => {
    const id = window.setTimeout(() => {
      preloadPlotlyChartRuntime().catch(() => undefined);
    }, 2000);
    return () => window.clearTimeout(id);
  }, []);

  useEffect(() => {
    writeStoredSlideLayouts(MARKET_SCAN_SLIDE_LAYOUT_STORAGE_KEY, slideLayouts);
  }, [slideLayouts]);

  useArrowCountryNavigation({
    options: countryOptions,
    activeValue: selectedCountry || DEFAULT_MARKET_SCAN_COUNTRY,
    onSelect: (value) => setSelectedCountry(value || null),
  });

  useEffect(() => {
    let active = true;
    const requestId = ++requestRef.current;
    setLoading(true);
    setError("");

    api.marketScanDeck({
      country: selectedCountry || undefined,
      target_period: selectedPeriod || undefined,
      time_range: selectedTimeRange || undefined,
      fuel_types: selectedFuelTypes,
      trend_window_months: 24,
      origin_window_months: 24,
      body_window_months: 24,
      ranking_limit: normalizeMarketScanRankingLimit(rankingLimit),
      drilldown_segment: selectedDrilldownSegment || undefined,
    })
      .then((response) => {
        if (!active || requestId !== requestRef.current) {
          return;
        }
        setDeck(response);
      })
      .catch((reason: Error) => {
        if (!active || requestId !== requestRef.current) {
          return;
        }
        if (reason.name === "AbortError") {
          return;
        }
        setError(reason.message);
      })
      .finally(() => {
        if (active && requestId === requestRef.current) {
          setLoading(false);
        }
      });

    return () => {
      active = false;
    };
  }, [rankingLimit, reloadToken, selectedCountry, selectedDrilldownSegment, selectedFuelTypes, selectedPeriod, selectedTimeRange]);

  useEffect(() => {
    if (!deck) {
      return;
    }

    if (
      selectedCountry
      && !deck.metadata.availableCountries.some((option) => option.value === selectedCountry)
    ) {
      setSelectedCountry(deck.metadata.selectedCountry);
    }

    if (
      selectedPeriod
      && !deck.metadata.availablePeriods.some((option) => option.value === selectedPeriod)
    ) {
      setSelectedPeriod(deck.metadata.resolvedPeriod);
    }
    if (selectedTimeRange) {
      const availablePeriodSet = new Set(deck.metadata.availablePeriods.map((option) => option.value));
      const nextRange = deck.metadata.selectedTimeRange ?? null;
      const isCurrentRangeValid = availablePeriodSet.has(selectedTimeRange.start) && availablePeriodSet.has(selectedTimeRange.end);
      if (!isCurrentRangeValid) {
        setSelectedTimeRange(nextRange);
      } else if (
        nextRange
        && (nextRange.start !== selectedTimeRange.start || nextRange.end !== selectedTimeRange.end)
      ) {
        setSelectedTimeRange(nextRange);
      }
    }

    if (
      selectedDrilldownSegment
      && !deck.metadata.availableSegments.some(
        (option) => option.value === selectedDrilldownSegment,
      )
    ) {
      setSelectedDrilldownSegment(deck.metadata.selectedDrilldownSegment);
    }

    const availableFuelSet = new Set(deck.metadata.availableFuelTypes);
    const normalizedFuelTypes = selectedFuelTypes.filter((fuel) => availableFuelSet.has(fuel));
    if (normalizedFuelTypes.length !== selectedFuelTypes.length && deck.metadata.selectedFuelTypes.length > 0) {
      setSelectedFuelTypes(deck.metadata.selectedFuelTypes);
    }
  }, [deck, selectedCountry, selectedDrilldownSegment, selectedFuelTypes, selectedPeriod, selectedTimeRange]);

  const currentCountry = selectedCountry ?? deck?.metadata.selectedCountry ?? "";
  const resolvedTimeRange = selectedTimeRange ?? deck?.metadata.selectedTimeRange ?? null;
  const customRangeActive = isCustomTimeRange(resolvedTimeRange);
  const currentPeriod = resolvedTimeRange?.end ?? selectedPeriod ?? deck?.metadata.resolvedPeriod ?? "";
  const fuelOptions = deck?.metadata.availableFuelTypes ?? selectedFuelTypes;
  const activeFuelTypes = selectedFuelTypes.length > 0
    ? selectedFuelTypes
    : (deck?.metadata.selectedFuelTypes ?? DEFAULT_FUEL_TYPES);
  const showDataLabels = exportSettings.dataLabelMode !== "off";
  const labelDigits = Math.max(0, Math.min(4, exportSettings.decimalPlaces ?? 1));
  const heroMetrics = deck ? buildHeroMetrics(deck, activePage, salesMode, customRangeActive) : [];
  usePageTransition(activePage, ".market-scan-slide-content");
  const narrative = deck ? pageNarrative(deck, activePage) : "按国家、月份与动力组合切换市场扫描页。";
  const activeTab = TAB_ITEMS.find((item) => item.key === activePage) ?? TAB_ITEMS[0];
  const previewWidth = normalizeMarketScanExportDimension(exportSettings.exportWidth, 1920, 400);
  const previewHeight = normalizeMarketScanExportDimension(exportSettings.exportHeight, 1080, 300);
  const slidePreview = useFixedCanvasPreview({
    width: previewWidth,
    height: previewHeight,
    exporting: exportingSlide,
  });
  const slideTitle = exportSettings.chartTitle.trim() || deck?.metadata.labels.pageTitle || "Market Scan Deck";
  const activeSlideLayout = slideLayouts[activePage] ?? DEFAULT_SLIDE_LAYOUT;
  const slideFitAssessment = useMemo(
    () => (deck
      ? buildMarketScanSlideFitAssessment({
          deck,
          activePage,
          heroMetricCount: heroMetrics.length,
          narrative,
          exportWidth: previewWidth,
          exportHeight: previewHeight,
        })
      : null),
    [activePage, deck, heroMetrics.length, narrative, previewHeight, previewWidth],
  );
  const slideFrameStyle: CSSProperties = {
    ...slidePreview.frameStyle,
    background: exportSettings.paperBg,
    "--market-scan-slide-pad-x": `${activeSlideLayout.paddingX}px`,
    "--market-scan-slide-pad-y": `${activeSlideLayout.paddingY}px`,
    "--market-scan-slide-frame-gap": `${activeSlideLayout.frameGap}px`,
    "--market-scan-slide-head-gap": `${activeSlideLayout.headGap}px`,
    "--market-scan-slide-body-gap": `${activeSlideLayout.bodyGap}px`,
    "--market-scan-slide-content-gap": `${activeSlideLayout.contentGap}px`,
  } as CSSProperties;

  function handleToggleSlideEditMode() {
    const next = toggleMarketScanSlideEditModeState({
      slideEditMode,
      exportToolsOpen,
    });
    setSlideEditMode(next.slideEditMode);
    setExportToolsOpen(next.exportToolsOpen);
  }

  function handleSlideLayoutChange(patch: Partial<SlideLayoutSettings>) {
    setSlideLayouts((current) =>
      updateMarketScanActiveSlideLayout(current, activePage, patch),
    );
  }

  function handleSlideLayoutReset() {
    setSlideLayouts((current) =>
      resetMarketScanActiveSlideLayout(current, activePage),
    );
  }

  function toggleFuel(fuel: string) {
    setSelectedFuelTypes((current) => toggleMarketScanFuelSelection(current, fuel));
  }

  function renderActivePageContent(compact = false) {
    if (!deck) {
      return null;
    }
    if (activePage === "overview") {
      return (
        <OverviewSection
          labels={deck.metadata.labels}
          page={deck.results.overview}
          fuelOrder={deck.metadata.selectedFuelTypes}
          salesMode={salesMode}
          customRangeActive={customRangeActive}
          timeRange={resolvedTimeRange}
          showDataLabels={showDataLabels}
          exportSettings={exportSettings}
          compact={compact}
          onBrandClick={(brand, sourceTable) => setTrendDrawer({ open: true, brand, sourceTable })}
        />
      );
    }
    if (activePage === "origin") {
      return (
        <OriginSection
          page={deck.results.origin}
          salesMode={salesMode}
          customRangeActive={customRangeActive}
          timeRange={resolvedTimeRange}
          showDataLabels={showDataLabels}
          exportSettings={exportSettings}
          compact={compact}
        />
      );
    }
    if (activePage === "segment") {
      return (
        <SegmentSection
          page={deck.results.segment}
          salesMode={salesMode}
          customRangeActive={customRangeActive}
          timeRange={resolvedTimeRange}
          showDataLabels={showDataLabels}
          labelDigits={labelDigits}
          exportSettings={exportSettings}
          compact={compact}
        />
      );
    }
    if (activePage === "drilldown") {
      return (
        <DrilldownSection
          page={deck.results.drilldown}
          fuelOrder={deck.metadata.selectedFuelTypes}
          salesMode={salesMode}
          customRangeActive={customRangeActive}
          customRangeLabel={deck.results.overview.summary.customRangeLabel}
          showDataLabels={showDataLabels}
          exportSettings={exportSettings}
          compact={compact}
          rankingLimit={rankingLimit}
          onRankingLimitChange={compact ? undefined : setRankingLimit}
        />
      );
    }
    if (activePage === "suvAll") {
      return (
        <DrilldownSection
          page={deck.results.suvAll}
          fuelOrder={deck.metadata.selectedFuelTypes}
          salesMode={salesMode}
          customRangeActive={customRangeActive}
          customRangeLabel={deck.results.overview.summary.customRangeLabel}
          showDataLabels={showDataLabels}
          exportSettings={exportSettings}
          compact={compact}
          rankingLimit={rankingLimit}
          onRankingLimitChange={compact ? undefined : setRankingLimit}
        />
      );
    }
    if (activePage === "suvA") {
      return (
        <DrilldownSection
          page={deck.results.suvA}
          fuelOrder={deck.metadata.selectedFuelTypes}
          salesMode={salesMode}
          customRangeActive={customRangeActive}
          customRangeLabel={deck.results.overview.summary.customRangeLabel}
          showDataLabels={showDataLabels}
          exportSettings={exportSettings}
          compact={compact}
          rankingLimit={rankingLimit}
          onRankingLimitChange={compact ? undefined : setRankingLimit}
        />
      );
    }
    return (
        <DrilldownSection
          page={deck.results.suvB}
          fuelOrder={deck.metadata.selectedFuelTypes}
          salesMode={salesMode}
          customRangeActive={customRangeActive}
          customRangeLabel={deck.results.overview.summary.customRangeLabel}
          showDataLabels={showDataLabels}
          exportSettings={exportSettings}
        compact={compact}
        rankingLimit={rankingLimit}
        onRankingLimitChange={compact ? undefined : setRankingLimit}
      />
    );
  }

  async function handleExportSlide() {
    if (!slideRef.current || !deck) {
      return;
    }
    try {
      setExportError("");
      setExportingSlide(true);
      const exportWidth = normalizeMarketScanExportDimension(exportSettings.exportWidth, 1920, 400);
      const exportHeight = normalizeMarketScanExportDimension(exportSettings.exportHeight, 1080, 300);
      if ("fonts" in document) {
        await document.fonts.ready;
      }
      await new Promise<void>((resolve) => {
        requestAnimationFrame(() => requestAnimationFrame(() => resolve()));
      });
      const { toPng } = await import("html-to-image");
      const dataUrl = await toPng(slideRef.current, {
        cacheBust: true,
        pixelRatio: 2,
        backgroundColor: exportSettings.paperBg || "#eef4f7",
        width: exportWidth,
        height: exportHeight,
        canvasWidth: exportWidth,
        canvasHeight: exportHeight,
        style: {
          width: `${exportWidth}px`,
          height: `${exportHeight}px`,
        },
      });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = [
        "market-scan",
        sanitizeFileNameSegment(deck.metadata.selectedCountryLabel),
        deck.metadata.resolvedPeriod,
        activeTab.key,
      ].join("-") + ".png";
      link.click();
    } catch (reason) {
      setExportError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setExportingSlide(false);
    }
  }

  return (
    <>
    <div className="market-scan-shell">
      <div className="market-scan-main">
        <CollapsibleDeckHero
          collapsed={heroCollapsed}
          onToggle={() => setHeroCollapsed((current) => !current)}
          expandedLabel="展开市场扫描控制区"
          collapsedLabel="收起市场扫描控制区"
          expandedTitle="展开市场扫描控制区"
          collapsedTitle="收起市场扫描控制区"
          className="header-card dashboard-hero market-scan-hero"
          shellClassName="dashboard-hero-shell market-scan-hero-shell"
          head={(
            <div className="dashboard-hero-copy market-scan-hero-copy">
              <span className="page-kicker">Market Scan</span>
              <h1>{deck?.metadata.labels.pageTitle ?? "Market Scan Deck"}</h1>
              <p>{narrative}</p>
              <div className="market-scan-hero-ribbon">
                <span className="market-scan-hero-chip">
                  国家 {deck?.metadata.selectedCountryLabel ?? "Sweden"}
                </span>
                <span className="market-scan-hero-chip">
                  月份 {customRangeActive ? (deck?.results.overview.summary.customRangeLabel ?? deck?.metadata.labels.currentMonthShort ?? "Latest") : (deck?.metadata.labels.currentMonthShort ?? "Latest")}
                </span>
                <span className="market-scan-hero-chip">
                  口径 {customRangeActive ? "自定义区间累计" : (MARKET_SCAN_SALES_MODE_OPTIONS.find((option) => option.value === salesMode)?.label ?? "当月")}
                </span>
                <span className="market-scan-hero-chip">
                  动力 {activeFuelTypes.join(" / ")}
                </span>
                <span className="market-scan-hero-chip">
                  下钻 {deck?.metadata.selectedDrilldownSegment ?? selectedDrilldownSegment}
                </span>
                {loading && deck ? (
                  <span className="market-scan-hero-chip market-scan-hero-chip--live">Refreshing</span>
                ) : null}
              </div>
            </div>
          )}
          body={(
            <div className="market-scan-hero-body-grid">
              <div className="market-scan-controls-grid">
                <label className="market-scan-field">
                  <span>Country</span>
                  <select
                    value={currentCountry}
                    onChange={(event) => setSelectedCountry(event.target.value || null)}
                    disabled={!deck}
                  >
                    {(deck?.metadata.availableCountries ?? []).map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>

                <DeckPeriodTimeline
                  options={deck?.metadata.availablePeriods ?? []}
                  value={resolvedTimeRange ?? (selectedPeriod ? { start: selectedPeriod, end: selectedPeriod } : null)}
                  onChange={(value) => {
                    setSelectedTimeRange(isCustomTimeRange(value) ? value : null);
                    setSelectedPeriod(value?.end ?? null);
                  }}
                  disabled={!deck}
                />

                <div className="market-scan-field">
                  <span>销量口径</span>
                  <div className="btn-group">
                    {MARKET_SCAN_SALES_MODE_OPTIONS.map((option) => (
                      <button
                        key={option.value}
                        type="button"
                        className={`btn btn-sm ${!customRangeActive && salesMode === option.value ? "btn-primary" : "btn-ghost"}`}
                        onClick={() => {
                          setSelectedTimeRange(null);
                          setSalesMode(option.value);
                        }}
                      >
                        {option.label}
                      </button>
                    ))}
                    {customRangeActive ? (
                      <span className="btn btn-sm btn-primary">
                        {deck?.results.overview.summary.customRangeLabel ?? "自定义区间"}
                      </span>
                    ) : null}
                  </div>
                  {customRangeActive ? (
                    <small className="market-scan-field-hint">
                      当前时间轴就是激活中的销量口径；点击当月 / YTD / 近12个月会退出自定义区间。
                    </small>
                  ) : null}
                </div>

                <label className="market-scan-field">
                  <span>Drilldown</span>
                  <select
                    value={selectedDrilldownSegment}
                    onChange={(event) => setSelectedDrilldownSegment(event.target.value)}
                    disabled={!deck}
                  >
                    {(deck?.metadata.availableSegments ?? []).map((segment) => (
                      <option key={segment.value} value={segment.value}>
                        {segment.label}
                      </option>
                    ))}
                  </select>
                </label>

                <div className="market-scan-field market-scan-field-actions">
                  <span>Deck</span>
                  <div className="btn-group">
                    <button
                      type="button"
                      className="btn btn-secondary btn-sm"
                      onClick={() => setReloadToken((value) => value + 1)}
                    >
                      Refresh
                    </button>
                    <button
                      type="button"
                      className="btn btn-ghost btn-sm"
                      onClick={() => {
                        setSelectedCountry(null);
                        setSelectedPeriod(null);
                        setSalesMode(DEFAULT_MARKET_SCAN_SALES_MODE);
                        setSelectedFuelTypes(DEFAULT_FUEL_TYPES);
                        setSelectedDrilldownSegment("SUV A0");
                        setRankingLimit(MIN_MARKET_SCAN_RANKING_LIMIT);
                        setActivePage("overview");
                      }}
                    >
                      Reset
                    </button>
                  </div>
                </div>
              </div>

              <div className="market-scan-fuel-bank">
                <span className="market-scan-fuel-bank-label">Fuel Focus</span>
                <div className="market-scan-fuel-chip-row">
                  {fuelOptions.map((fuel) => {
                    const active = activeFuelTypes.includes(fuel);
                    return (
                      <button
                        key={fuel}
                        type="button"
                        className={`market-scan-fuel-chip${active ? " is-active" : ""}`}
                        onClick={() => toggleFuel(fuel)}
                        style={{
                          borderColor: active ? fuelColor(fuel) : undefined,
                          background: active ? `${fuelColor(fuel)}16` : undefined,
                        }}
                      >
                        <span
                          className="market-scan-fuel-dot"
                          style={{ backgroundColor: fuelColor(fuel) }}
                          aria-hidden="true"
                        />
                        {fuel}
                      </button>
                    );
                  })}
                </div>
              </div>

              <div className="market-scan-hero-metrics">
                {heroMetrics.map((metric) => (
                  <MetricCard
                    key={`${metric.label}-${metric.detail}`}
                    label={metric.label}
                    value={metric.value}
                    detail={metric.detail}
                    tone={metric.tone}
                  />
                ))}
              </div>
            </div>
          )}
        />

        <DeckSubpageNav
          items={TAB_ITEMS}
          activeKey={activePage}
          onSelect={setActivePage}
          ariaLabel="Market Scan Pages"
          tabsClassName="market-scan-tab-strip"
        />

        {error ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>Market Scan 加载失败</strong>
            <p>{error}</p>
            <button
              type="button"
              className="btn btn-secondary btn-sm"
              onClick={() => setReloadToken((value) => value + 1)}
            >
              重试
            </button>
          </section>
        ) : null}

        {loading && !deck ? <MarketScanDeckSkeleton /> : null}

        {exportError ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>PNG 导出失败</strong>
            <p>{exportError}</p>
          </section>
        ) : null}

        {deck ? (
          <div className="market-scan-content" aria-busy={loading}>
            {loading ? <MarketScanDeckSkeleton /> : null}
            <div className="market-scan-slide-shell-actions">
              <div className="market-scan-slide-shell-meta">
                <span className={`market-scan-toolbar-chip slide-edit-shell-chip${slideEditMode ? " is-active" : ""}`}>
                  {slideEditMode ? "Edit Mode" : "Preview"}
                </span>
                <span className="market-scan-slide-shell-note">
                  {slideEditMode
                    ? "正在调整固定画布版式参数。"
                    : "默认只做预览，需要时再进入一键 edit。"}
                </span>
              </div>
              <button
                type="button"
                className={`btn btn-sm ${slideEditMode ? "btn-secondary" : "btn-primary"}`}
                onClick={handleToggleSlideEditMode}
              >
                {slideEditMode ? "返回 Preview" : "一键 Edit"}
              </button>
            </div>
            <div ref={slidePreview.shellRef} className="market-scan-slide-shell">
              <div className="market-scan-slide-scale-box" style={slidePreview.scaleBoxStyle}>
                <div
                  ref={slideRef}
                  className={`market-scan-slide-frame market-scan-slide-frame--${activePage}${exportingSlide ? " is-exporting" : ""}${slideEditMode && !exportingSlide ? " is-editing" : ""}`}
                  style={slideFrameStyle}
                >
                <header className="market-scan-slide-head">
                  <div className="market-scan-slide-copy">
                    <span className="market-scan-slide-kicker">{activeTab.code} {activeTab.label}</span>
                    <h2>{slideTitle}</h2>
                    <p>{narrative}</p>
                  </div>
                  <div className="market-scan-slide-meta">
                    <span className="market-scan-slide-tag">国家 {deck.metadata.selectedCountryLabel}</span>
                    <span className="market-scan-slide-tag">月份 {deck.metadata.labels.currentMonthShort}</span>
                    <span className="market-scan-slide-tag">动力 {activeFuelTypes.join(" / ")}</span>
                    <span className="market-scan-slide-tag">下钻 {deck.metadata.selectedDrilldownSegment}</span>
                  </div>
                </header>
                <div className="market-scan-slide-body">
                  {heroMetrics.length > 0 ? (
                    <div className="market-scan-metric-grid market-scan-metric-grid--slide">
                      {heroMetrics.map((metric) => (
                        <MetricCard
                          key={`slide-${metric.label}-${metric.detail}`}
                          label={metric.label}
                          value={metric.value}
                          detail={metric.detail}
                          tone={metric.tone}
                        />
                      ))}
                    </div>
                  ) : null}
                  <div className="market-scan-slide-content">
                    {renderActivePageContent(true)}
                  </div>
                </div>
                </div>
              </div>
            </div>
            <section className="market-scan-export-drawer">
              <button
                type="button"
                className="market-scan-export-toggle"
                onClick={() => setExportToolsOpen((value) => !value)}
                aria-expanded={exportToolsOpen}
              >
                <span>导出当前页 / 导出图设置</span>
                <span>{exportToolsOpen ? "收起" : "展开"}</span>
              </button>
              {exportToolsOpen ? (
                <div className="market-scan-toolbar market-scan-toolbar--bottom">
                  <div className="market-scan-toolbar-group market-scan-toolbar-group--settings">
                    <div className="market-scan-toolbar-actions">
                      <button
                        type="button"
                        className="btn btn-primary btn-sm"
                        onClick={() => { void handleExportSlide(); }}
                        disabled={exportingSlide}
                      >
                        {exportingSlide ? "正在导出 PNG..." : "导出当前页 PNG"}
                      </button>
                      <button
                        type="button"
                        className={`btn btn-sm ${slideEditMode ? "btn-secondary" : "btn-ghost"}`}
                        onClick={handleToggleSlideEditMode}
                      >
                        {slideEditMode ? "退出 Edit" : "一键 Edit"}
                      </button>
                    </div>
                    {slideEditMode ? (
                      <SlideLayoutEditor
                        value={activeSlideLayout}
                        onChange={handleSlideLayoutChange}
                        onReset={handleSlideLayoutReset}
                      />
                    ) : null}
                    {slideFitAssessment ? <SlideFitSummary assessment={slideFitAssessment} /> : null}
                    <ExportPanel
                      value={exportSettings}
                      onChange={setExportSettings}
                      labelModeOptions={marketScanLabelModeOptions}
                      showExportButton={false}
                    />
                  </div>
                  <div className="market-scan-toolbar-meta">
                    <span className="market-scan-toolbar-chip">Slide Layout</span>
                    <span className="market-scan-toolbar-chip">{exportSettings.exportWidth} x {exportSettings.exportHeight}</span>
                    <span className="market-scan-toolbar-chip">标签 {showDataLabels ? "On" : "Off"}</span>
                    <span className="market-scan-toolbar-chip">{activeTab.label}</span>
                    {slideFitAssessment ? (
                      <span className={`market-scan-toolbar-chip slide-fit-chip slide-fit-chip--${slideFitAssessment.status}`}>
                        {slideFitAssessment.status === "safe"
                          ? "Fit Safe"
                          : slideFitAssessment.status === "compress"
                            ? "Need Trim"
                            : `Split ${slideFitAssessment.splitSlides}`}
                      </span>
                    ) : null}
                  </div>
                </div>
              ) : null}
            </section>
          </div>
        ) : null}
      </div>
    </div>
    <RankingTrendPopover
      open={trendDrawer.open}
      brand={trendDrawer.brand}
      model={trendDrawer.model}
      sourceTable={trendDrawer.sourceTable}
      country={selectedCountry || ""}
      segment={activePage === "drilldown" ? selectedDrilldownSegment : undefined}
      fuelTypes={selectedFuelTypes}
      onClose={() => setTrendDrawer({ open: false, brand: "", sourceTable: "monthly_brand_ranking" })}
      onBack={trendDrawer.model ? () => setTrendDrawer((p) => ({ ...p, model: undefined })) : undefined}
      onModelClick={(m) => setTrendDrawer((p) => ({ ...p, model: m }))}
    />
    </>
  );
}
