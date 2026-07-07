import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import type { Data, Layout, PlotMouseEvent } from "plotly.js";

import { dashboardApi } from "../api/dashboard";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { CollapsibleFilterSidebar } from "../components/CollapsibleFilterSidebar";
import { LoadingActionButton } from "../components/LoadingActionButton";
import { LoadingSurface } from "../components/LoadingSurface";
import { PageBannerStack } from "../components/PageFeedback";
import { SearchSelectFilter } from "../components/SearchSelectFilter";
import { DebouncedNumberInput } from "../components/deckControls/DebouncedNumberInput";
import { DeckControlTabs, type DeckControlTabItem } from "../components/deckControls/DeckControlTabs";
import { DeckExportDrawer } from "../components/deckControls/DeckExportDrawer";
import { DeckFloatingDrawer } from "../components/deckControls/DeckFloatingDrawer";
import { useSharedFilterScope } from "../contexts/SharedFilterScopeContext";
import { useResolvedCountry } from "../hooks/useResolvedCountry";
import {
  FILTER_ORDER,
} from "../dashboardFilters";
import type { FilterKey } from "../dashboardFilters";
import type { OverviewResponse, TimeSeriesPoint, GroupedTimeSeriesItem, ModelVersionItem, PositioningMapItem, PositioningPeerCorridor, OthersDetailItem, DataFreshnessItem } from "../types";
import { LazyPlotlyChart as PlotlyChart } from "../components/LazyPlotlyChart";
import { TimeAxis, type TimeRange } from "../components/TimeAxis";
import { DEFAULT_EXPORT, applyExportToLayout, getExportPalette, applyDataLabelsToTraces, applySeriesColors, buildExportLabelModeOptions, withExportLabels, type ExportLabelOverlapStrategy, type ExportSettings } from "../components/ExportPanelHelpers";
import { buildBubbleSizing } from "../utils/bubbleSizing";
import { DEFAULT_POWERTRAINS, fuelFamilyColor, normalizePowertrainName, seriesColor } from "../utils/colors";
import { getCachedPageValue, setCachedPageValue } from "../utils/pageCache";
import { buildCategoryAxis, formatCompactBarLabel } from "../utils/plotlyDefaults";
import { parseMonthLabel, toTimeOrdinal, compareTimeLabels } from "../utils/timeFormatting";
import { isAbortError } from "../utils/filterOptions";
import {
  type BubbleGroupDimension,
  type DashboardPageCache,
  type TimeSeriesShareSplitDimension,
  DASHBOARD_CACHE_KEY,
  PAGE_CACHE_TTL_MS,
  ADV_GROUPS,
  ADV_CHARTS,
  GROUP_BY_OPTIONS,
  TIME_SERIES_SHARE_SPLIT_OPTIONS,
  BUBBLE_GROUP_DIMENSIONS,
  SCATTER_CHARTS,
  STACKED_CHARTS,
  ensureArray,
  isPlainRecord,
  asMetaNumber,
  asMetaText,
  asMetaStringArray,
  asMetaRecordArray,
  formatDashboardSummaryMetric,
  getLoadingMetricValue,
  getDashboardLensSummary,
  isTimeSeriesShareGroupDimension,
  isDashboardBootstrapping,
  formatMetricValue,
  summarizeScopeValues,
  getMetricDensityClass,
  getUnifiedMetricDensityClass,
} from "./dashboardHelpers";
const RvFinanceDashboard = lazy(() =>
  import("../components/RvFinanceDashboard").then((module) => ({ default: module.RvFinanceDashboard }))
);
const DashboardExportPanel = lazy(() =>
  import("../components/ExportPanel").then((module) => ({ default: module.ExportPanel }))
);

const DASHBOARD_DATA_FRESHNESS_DELAY_MS = 10_000;
const DASHBOARD_CHART_RUNTIME_IDLE_TIMEOUT_MS = 4_000;
const DASHBOARD_CHART_RUNTIME_MIN_DELAY_MS = 12_000;
const DASHBOARD_HEAVY_QUERY_MIN_DELAY_MS = 10_000;
const DEFAULT_ADVANCED_EXPORT: ExportSettings = {
  ...DEFAULT_EXPORT,
  dataLabelOverlapStrategy: "smart_top",
};
const ADVANCED_BUBBLE_LABEL_OPTIONS: Array<{ value: ExportLabelOverlapStrategy; label: string }> = [
  { value: "smart_top", label: "Smart Top" },
  { value: "clean", label: "Clean" },
  { value: "selected", label: "Selected" },
  { value: "all", label: "All" },
];
type DashboardBubbleLabelMode = "all" | "smart_top" | "selected" | "clean";

interface DashboardBubbleLabelInfo {
  key: string;
  text: string;
  x: number;
  y: number;
  sales: number;
  series: string;
  priority: number;
  showLabel: boolean;
  jitterX: number;
  jitterY: number;
}

const DASHBOARD_BUBBLE_LABEL_STYLE: Record<number, { sizeOffset: number; color: string }> = {
  3: { sizeOffset: 0, color: "rgba(15,23,42,0.96)" },
  2: { sizeOffset: -1, color: "rgba(51,65,85,0.72)" },
  1: { sizeOffset: -1, color: "rgba(51,65,85,0.38)" },
  0: { sizeOffset: -2, color: "rgba(51,65,85,0.22)" },
};

function hashDashboardLabelKey(value: string): number {
  let hash = 0;
  for (let index = 0; index < value.length; index += 1) {
    hash = ((hash << 5) - hash) + value.charCodeAt(index);
    hash |= 0;
  }
  return Math.abs(hash);
}

function jitterDashboardLabel(key: string, amplitude: number): number {
  return ((hashDashboardLabelKey(key) % 1000) / 1000) * amplitude * 2 - amplitude;
}

function normalizeDashboardBubbleLabelMode(strategy: ExportLabelOverlapStrategy): DashboardBubbleLabelMode {
  if (strategy === "smart" || strategy === "smart_top") return "smart_top";
  if (strategy === "selected") return "selected";
  if (strategy === "all") return "all";
  return "clean";
}

function salesOpacity(value: number, minSales: number, maxSales: number): number {
  if (!Number.isFinite(value) || value <= 0) return 0.18;
  if (maxSales <= minSales) return 0.78;
  const normalized = Math.max(0, Math.min(1, (value - minSales) / (maxSales - minSales)));
  return 0.24 + Math.sqrt(normalized) * 0.58;
}

function buildSalesOpacityValues(values: number[]): number[] {
  const valid = values.filter((value) => Number.isFinite(value) && value > 0);
  const minSales = Math.min(...valid);
  const maxSales = Math.max(...valid);
  return values.map((value) => salesOpacity(value, minSales, maxSales));
}

function filterDashboardBubbleLabelOverlap(
  labels: DashboardBubbleLabelInfo[],
  xRange: number,
  yRange: number,
): Set<string> {
  const hidden = new Set<string>();
  if (labels.length <= 1) return hidden;
  const placed: Array<{ x: number; y: number }> = [];
  const sorted = [...labels].sort((a, b) => b.priority - a.priority || b.sales - a.sales);
  const xThreshold = xRange * 0.022;
  const yThreshold = yRange * 0.028;
  sorted.forEach((label) => {
    const x = label.x + label.jitterX;
    const y = label.y + label.jitterY;
    const overlaps = placed.some(
      (point) => Math.abs(point.x - x) < xThreshold && Math.abs(point.y - y) < yThreshold,
    );
    if (overlaps) {
      hidden.add(label.key);
      return;
    }
    placed.push({ x, y });
  });
  return hidden;
}

function buildDashboardBubbleLabelTraces(
  items: DashboardBubbleLabelInfo[],
  labelMode: DashboardBubbleLabelMode,
  fontSize: number,
): Data[] {
  if (labelMode === "clean") return [];
  const visibleCandidates = items.filter((item) => item.showLabel);
  if (visibleCandidates.length === 0) return [];
  const xValues = visibleCandidates.map((item) => item.x);
  const yValues = visibleCandidates.map((item) => item.y);
  const hidden = filterDashboardBubbleLabelOverlap(
    visibleCandidates,
    Math.max(...xValues) - Math.min(...xValues) || 1,
    Math.max(...yValues) - Math.min(...yValues) || 1,
  );
  const visible = visibleCandidates.filter((item) => !hidden.has(item.key));
  return [3, 2, 1, 0].flatMap((priority) => {
    const priorityItems = visible.filter((item) => item.priority === priority);
    if (priorityItems.length === 0) return [];
    const style = DASHBOARD_BUBBLE_LABEL_STYLE[priority];
    return [{
      type: "scatter",
      mode: "text",
      name: `label-p${priority}`,
      showlegend: false,
      x: priorityItems.map((item) => item.x + item.jitterX),
      y: priorityItems.map((item) => item.y + item.jitterY),
      text: priorityItems.map((item) => item.text),
      textposition: "top center",
      textfont: {
        size: Math.max(7, fontSize + style.sizeOffset),
        color: style.color,
      },
      cliponaxis: false,
      customdata: priorityItems.map((item) => [item.series, item.sales, item.key]),
      hoverinfo: "skip",
    } as Data];
  });
}

let dashboardAnimationPromise: Promise<typeof import("animejs")> | null = null;

function loadDashboardAnimation() {
  if (!dashboardAnimationPromise) {
    dashboardAnimationPromise = import("animejs").catch((error) => {
      dashboardAnimationPromise = null;
      throw error;
    });
  }
  return dashboardAnimationPromise;
}

type DashboardIdleWindow = Window & typeof globalThis & {
  requestIdleCallback?: (callback: () => void, options?: { timeout?: number }) => number;
  cancelIdleCallback?: (handle: number) => void;
};

function scheduleDashboardIdlePreload(callback: () => void): () => void {
  const idleWindow = window as DashboardIdleWindow;
  if (typeof idleWindow.requestIdleCallback === "function") {
    const handle = idleWindow.requestIdleCallback(callback, {
      timeout: DASHBOARD_CHART_RUNTIME_IDLE_TIMEOUT_MS,
    });
    return () => idleWindow.cancelIdleCallback?.(handle);
  }
  const handle = window.setTimeout(callback, DASHBOARD_CHART_RUNTIME_IDLE_TIMEOUT_MS);
  return () => window.clearTimeout(handle);
}

function scheduleDashboardDelayedIdlePreload(callback: () => void, delayMs: number): () => void {
  let cancelIdlePreload: (() => void) | null = null;
  const timer = window.setTimeout(() => {
    cancelIdlePreload = scheduleDashboardIdlePreload(callback);
  }, delayMs);
  return () => {
    window.clearTimeout(timer);
    cancelIdlePreload?.();
  };
}

function resolveTimeSeriesSeriesColor(
  name: string,
  index: number,
  palette: string[],
  isPowertrain: boolean,
  manualColors: Record<string, string>,
  focusedPowertrain: string | null,
  total: number,
): string {
  const manualColor = manualColors[name];
  if (manualColor) return manualColor;
  const safeIndex = index >= 0 ? index : 0;
  if (focusedPowertrain && !isPowertrain) {
    return fuelFamilyColor(focusedPowertrain, safeIndex, total);
  }
  return seriesColor(name, safeIndex, palette, isPowertrain);
}

function uniqueNonEmptyStrings(values: readonly unknown[]): string[] {
  return Array.from(
    new Set(
      values
        .map((value) => String(value ?? "").trim())
        .filter(Boolean),
    ),
  );
}

function getAdvancedScatterColorKey(chart: string): string {
  switch (chart) {
    case "powertrain_bubble":
    case "nev_capacity_vs_msrp":
    case "nev_length_vs_range":
    case "estimated_tco":
      return "Powertrain";
    case "length_vs_price":
    case "sales_vs_price":
      return "Segment";
    case "price_per_meter":
      return "Brand";
    default:
      return "";
  }
}

const NEV_POWERTRAIN_FILTER_CHARTS = new Set([
  "nev_range_distribution",
  "nev_capacity_vs_msrp",
  "nev_length_vs_range",
]);

function usesNevPowertrainFilter(chart: string): boolean {
  return NEV_POWERTRAIN_FILTER_CHARTS.has(chart);
}

function readSinglePowertrain(values: readonly string[]): string | null {
  const normalized = uniqueNonEmptyStrings(values.map((value) => normalizePowertrainName(String(value))));
  return normalized.length === 1 ? normalized[0] : null;
}

function resolveAdvancedSeriesColor(
  name: string,
  index: number,
  total: number,
  palette: string[],
  isPowertrainSeries: boolean,
  manualColors: Record<string, string>,
  focusedPowertrain: string | null,
): string {
  const manualColor = manualColors[name];
  if (manualColor) return manualColor;
  if (focusedPowertrain && !isPowertrainSeries) {
    return fuelFamilyColor(focusedPowertrain, index, total);
  }
  return seriesColor(name, index, palette, isPowertrainSeries);
}

type DeckSectionKey = "timeSeries" | "advanced" | "modelVersion" | "positioning";

interface DashboardDeckLayoutSettings {
  height: number;
  width: number;
}

type DashboardDeckLayouts = Record<DeckSectionKey, DashboardDeckLayoutSettings>;

const DECK_SECTION_TABS: { key: DeckSectionKey; label: string }[] = [
  { key: "timeSeries", label: "03 Time-Series" },
  { key: "advanced", label: "04 Advanced" },
  { key: "modelVersion", label: "05 Model Version" },
  { key: "positioning", label: "06 Positioning" },
];

const DEFAULT_DASHBOARD_DECK_LAYOUTS: DashboardDeckLayouts = {
  timeSeries: { height: 500, width: 0 },
  advanced: { height: 520, width: 0 },
  modelVersion: { height: 520, width: 0 },
  positioning: { height: 520, width: 0 },
};

function normalizeDeckLayoutValue(
  value: unknown,
  fallback: DashboardDeckLayoutSettings,
): DashboardDeckLayoutSettings {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return fallback;
  }
  const record = value as Record<string, unknown>;
  const height = Number(record.height);
  const width = Number(record.width);
  return {
    height: Number.isFinite(height) && height >= 300 && height <= 900
      ? height
      : fallback.height,
    width: Number.isFinite(width) && width >= 0 && width <= 2000
      ? width
      : fallback.width,
  };
}

function readDashboardDeckLayouts(): DashboardDeckLayouts {
  const fallback = { ...DEFAULT_DASHBOARD_DECK_LAYOUTS };
  try {
    const saved = localStorage.getItem("dashboard-deck-layouts");
    if (saved) {
      const parsed = JSON.parse(saved) as Partial<Record<DeckSectionKey, unknown>>;
      return {
        timeSeries: normalizeDeckLayoutValue(parsed.timeSeries, fallback.timeSeries),
        advanced: normalizeDeckLayoutValue(parsed.advanced, fallback.advanced),
        modelVersion: normalizeDeckLayoutValue(parsed.modelVersion, fallback.modelVersion),
        positioning: normalizeDeckLayoutValue(parsed.positioning, fallback.positioning),
      };
    }

    const legacyHeight = Number(localStorage.getItem("dashboard-deck-chart-height"));
    const legacyWidth = Number(localStorage.getItem("dashboard-deck-chart-width"));
    if (Number.isFinite(legacyHeight) || Number.isFinite(legacyWidth)) {
      const migrated = normalizeDeckLayoutValue(
        {
          height: Number.isFinite(legacyHeight) ? legacyHeight : fallback.timeSeries.height,
          width: Number.isFinite(legacyWidth) ? legacyWidth : fallback.timeSeries.width,
        },
        fallback.timeSeries,
      );
      return {
        timeSeries: migrated,
        advanced: migrated,
        modelVersion: migrated,
        positioning: migrated,
      };
    }
  } catch {
    return fallback;
  }
  return fallback;
}

function getDeckLayoutStyle(
  layout: DashboardDeckLayoutSettings,
): React.CSSProperties {
  return {
    "--deck-chart-max-width": layout.width > 0 ? `${layout.width}px` : "100%",
  } as React.CSSProperties;
}

function DeferredDashboardDecksPlaceholder({ onActivate }: { onActivate: () => void }) {
  return (
    <div className="card analysis-deck-card dashboard-deck-card--compact-hero">
      <div className="dashboard-hero-head dashboard-deck-hero-head">
        <div className="dashboard-hero-copy dashboard-deck-hero-copy">
          <span className="panel-kicker">04-06 / Deferred Analysis</span>
          <h3>Advanced decks preparing</h3>
          <p>高级分析、单车型版型和竞品定位模块正在准备。</p>
        </div>
        <div className="dashboard-hero-actions dashboard-deck-hero-actions dashboard-deck-hero-actions--pair">
          <div className="hero-meta-block dashboard-deck-hero-stat">
            <span className="hero-meta-label">Startup</span>
            <strong className="hero-meta-value">FAST</strong>
            <span className="hero-meta-subvalue">首屏优先展示筛选和趋势</span>
          </div>
          <div className="hero-meta-block dashboard-deck-hero-stat">
            <span className="hero-meta-label">Heavy decks</span>
            <strong className="hero-meta-value">DEFERRED</strong>
            <span className="hero-meta-subvalue">空闲后自动挂载</span>
          </div>
        </div>
      </div>
      <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
        <div className="dashboard-cta-row">
          <button type="button" className="btn btn-secondary" onClick={onActivate}>
            立即加载高级分析模块
          </button>
        </div>
      </div>
    </div>
  );
}

function DeferredDashboardChartPlaceholder({ onActivate }: { onActivate: () => void }) {
  return (
    <div className="dashboard-chart-runtime-placeholder">
      <div>
        <span className="panel-kicker">Chart Runtime</span>
        <strong>趋势图正在后台准备</strong>
        <p>筛选、概览和控制区已可操作，图表运行时会在浏览器空闲后加载。</p>
      </div>
      <button type="button" className="btn btn-secondary" onClick={onActivate}>
        立即加载趋势图
      </button>
    </div>
  );
}

/* ── filter component ──────────────────────────────── */
/* ── Main Dashboard ────────────────────────────────── */
export function DashboardPage() {
  const currentSearch = typeof window !== "undefined" ? window.location.search : "";
  const cachedPageRef = useRef<DashboardPageCache | null>(null);
  if (cachedPageRef.current === null) {
    const cached = getCachedPageValue<DashboardPageCache>(DASHBOARD_CACHE_KEY);
    cachedPageRef.current = cached && (cached.search === currentSearch || currentSearch === "") ? cached : null;
  }
  const cachedPage = cachedPageRef.current;
  const {
    columns,
    selections,
    optionsMap,
    filteredRowCount,
    overview,
    yearSeries,
    monthSeries,
    filtersReady,
    loading,
    optionsSyncPending,
    error: sharedError,
    activeFilters: rawActiveFilters,
    activeFilterSummary,
    specificationHref,
    dashboardSearch,
    heroCollapsed,
    sidebarCollapsed,
    setHeroCollapsed,
    setSidebarCollapsed,
    buildFilterPayload,
    filterPayloadStr,
    onFilterChange,
    resetFilters,
  } = useSharedFilterScope();
  const activeFilters = rawActiveFilters ?? [];
  const hasFilterSearchParams = useMemo(() => {
    const params = new URLSearchParams(currentSearch);
    return FILTER_ORDER.some(({ key }) => params.has(key));
  }, [currentSearch]);

  /* auto-select the user's primary country on first load (fallback: all countries) */
  const { country: defaultCountryZh } = useResolvedCountry("zh");
  const countryAutoApplied = useRef(false);
  useEffect(() => {
    if (!filtersReady || countryAutoApplied.current || hasFilterSearchParams) return;
    const countryOptions = optionsMap.country ?? [];
    if (countryOptions.length > 0 && selections.country.length === 0) {
      countryAutoApplied.current = true;
      const preferred =
        defaultCountryZh && countryOptions.includes(defaultCountryZh)
          ? [defaultCountryZh]
          : countryOptions;
      void onFilterChange("country", preferred);
    }
  }, [filtersReady, hasFilterSearchParams, optionsMap.country, selections.country, onFilterChange, defaultCountryZh]);

  /* time-series controls */
  const [activeTab, setActiveTab] = useState<"year"|"month">(() => cachedPage?.activeTab ?? "month");
  const [chartType, setChartType] = useState<"line"|"bar"|"rank">(() => cachedPage?.chartType ?? "line");
  const [rankLimit, setRankLimit] = useState(() => cachedPage?.rankLimit ?? 20);
  const [tsMode, setTsMode] = useState<"\u603b\u548c"|"\u5206\u7ec4">(() => cachedPage?.tsMode ?? "\u603b\u548c");
  const [tsGroupDim, setTsGroupDim] = useState(() => cachedPage?.tsGroupDim ?? "\u56fd\u5bb6");
  const [tsShareSplit, setTsShareSplit] = useState<TimeSeriesShareSplitDimension>(
    () => cachedPage?.tsShareSplit ?? "total",
  );
  const [tsTopN, setTsTopN] = useState(() => cachedPage?.tsTopN ?? 10);
  const [tsTopNEnabled, setTsTopNEnabled] = useState(() => cachedPage?.tsTopNEnabled ?? true);
  const [tsIncludeOthers, setTsIncludeOthers] = useState(() => cachedPage?.tsIncludeOthers ?? false);
  const [groupedItems, setGroupedItems] = useState<GroupedTimeSeriesItem[]>(
    () => ensureArray(cachedPage?.groupedItems),
  );
  const [groupedLoading, setGroupedLoading] = useState(false);
  const [chartRuntimeReady, setChartRuntimeReady] = useState(false);
  const [heavyQueriesReady, setHeavyQueriesReady] = useState(false);
  const [hiddenSeries, setHiddenSeries] = useState<Set<string>>(() => new Set(cachedPage?.hiddenSeries ?? []));
  const [othersDetail, setOthersDetail] = useState<OthersDetailItem[]>(() => cachedPage?.othersDetail ?? []);
  const releaseChartRuntime = useCallback(() => {
    setChartRuntimeReady(true);
  }, []);
  const releaseHeavyQueries = useCallback(() => {
    setHeavyQueriesReady(true);
  }, []);
  const releaseDashboardChartWork = useCallback(() => {
    releaseChartRuntime();
    releaseHeavyQueries();
  }, [releaseChartRuntime, releaseHeavyQueries]);
  const selectTsMode = useCallback((nextMode: "总和" | "分组") => {
    releaseDashboardChartWork();
    setTsMode(nextMode);
  }, [releaseDashboardChartWork]);
  const selectChartType = useCallback((nextType: "line" | "bar" | "rank") => {
    releaseDashboardChartWork();
    setChartType(nextType);
    if (nextType === "rank" && tsMode === "总和") {
      setTsMode("分组");
    }
  }, [releaseDashboardChartWork, tsMode]);

  /* advanced charts — honor URL params for deep-links from Copilot */
  const urlParams = useMemo(() => new URLSearchParams(currentSearch), [currentSearch]);
  const [advGroup, setAdvGroup] = useState(() => urlParams.get("advGroup") ?? cachedPage?.advGroup ?? "market_structure");
  const [advChart, setAdvChart] = useState(() => urlParams.get("advChart") ?? cachedPage?.advChart ?? "powertrain_bubble");
  const [advItems, setAdvItems] = useState<Record<string, string|number>[]>(() => cachedPage?.advItems ?? []);
  const [advMeta, setAdvMeta] = useState<Record<string, unknown> | null>(() => cachedPage?.advMeta ?? null);
  const [advLoading, setAdvLoading] = useState(false);
  const [advBandSize, setAdvBandSize] = useState(() => cachedPage?.advBandSize ?? 1000);
  const [advTopN, setAdvTopN] = useState(() => cachedPage?.advTopN ?? 30);
  const [advMigrationMode, setAdvMigrationMode] = useState<"area"|"line">(() => cachedPage?.advMigrationMode ?? "area");
  const [advBubbleScale, setAdvBubbleScale] = useState(() => cachedPage?.advBubbleScale ?? 2);
  const [advBubbleGrain, setAdvBubbleGrain] = useState<"model"|"version">(() => cachedPage?.advBubbleGrain ?? "model");
  const [advBubbleLabelDimension, setAdvBubbleLabelDimension] = useState<"model"|"version">(() => cachedPage?.advBubbleLabelDimension ?? "model");
  /* 7a: brand faceting for powertrain_bubble */
  const [advBubbleFacet, setAdvBubbleFacet] = useState(() => cachedPage?.advBubbleFacet ?? false);
  const [advBubbleFacetMax, setAdvBubbleFacetMax] = useState(() => cachedPage?.advBubbleFacetMax ?? 4);
  const [advBubbleShowYoy, setAdvBubbleShowYoy] = useState(() => cachedPage?.advBubbleShowYoy ?? false);
  const [advBubbleYoyYear, setAdvBubbleYoyYear] = useState(() => cachedPage?.advBubbleYoyYear ?? "");
  const [advBubbleGroupTopN, setAdvBubbleGroupTopN] = useState(() => cachedPage?.advBubbleGroupTopN ?? false);
  const [advBubbleGroupDimension, setAdvBubbleGroupDimension] = useState<BubbleGroupDimension>(() => cachedPage?.advBubbleGroupDimension ?? "segment");
  const [advBubbleGroupValues, setAdvBubbleGroupValues] = useState<string[]>(() => cachedPage?.advBubbleGroupValues ?? []);
  const [advBubbleGroupTopNMap, setAdvBubbleGroupTopNMap] = useState<Record<string, number>>(() => cachedPage?.advBubbleGroupTopNMap ?? {});
  /* NEV-specific controls */
  const [advPowertrains, setAdvPowertrains] = useState<string[]>(() => cachedPage?.advPowertrains ?? ["BEV","PHEV"]);
  const [advNevTopNEnabled, setAdvNevTopNEnabled] = useState(() => cachedPage?.advNevTopNEnabled ?? true);
  const [advNevAxisMax, setAdvNevAxisMax] = useState(() => cachedPage?.advNevAxisMax ?? 1000);
  const [advNevMetricMode, setAdvNevMetricMode] = useState<"window_sales"|"net_change">(() => cachedPage?.advNevMetricMode ?? "window_sales");
  const [advNevStackByModel, setAdvNevStackByModel] = useState(() => cachedPage?.advNevStackByModel ?? false);
  const [advNevFacetBrand, setAdvNevFacetBrand] = useState(() => cachedPage?.advNevFacetBrand ?? false);
  const [advNevMaxBrandFacets, setAdvNevMaxBrandFacets] = useState(() => cachedPage?.advNevMaxBrandFacets ?? 4);
  const [advNevRangeQuery, setAdvNevRangeQuery] = useState(() => cachedPage?.advNevRangeQuery ?? "");
  const [advRangeStep, setAdvRangeStep] = useState(() => cachedPage?.advRangeStep ?? 50);
  const [advHeatmapScale, setAdvHeatmapScale] = useState(() => cachedPage?.advHeatmapScale ?? "Blues");
  /* TCO parameter sliders */
  const [tcoYears, setTcoYears] = useState(() => cachedPage?.tcoYears ?? 5);
  const [tcoAnnualKm, setTcoAnnualKm] = useState(() => cachedPage?.tcoAnnualKm ?? 15000);
  const [tcoDepreciation, setTcoDepreciation] = useState(() => cachedPage?.tcoDepreciation ?? 0.5);
  const [tcoMaintenance, setTcoMaintenance] = useState(() => cachedPage?.tcoMaintenance ?? 0.018);
  const [tcoTaxInsurance, setTcoTaxInsurance] = useState(() => cachedPage?.tcoTaxInsurance ?? 0.02);
  const [tcoEnergyCost, setTcoEnergyCost] = useState(() => cachedPage?.tcoEnergyCost ?? 0.1);

  /* model version bubble (Bug 2) */
  const [mvModelName, setMvModelName] = useState(() => cachedPage?.mvModelName ?? "");
  const [mvTopN, setMvTopN] = useState(() => cachedPage?.mvTopN ?? 50);
  const [mvItems, setMvItems] = useState<ModelVersionItem[]>(() => cachedPage?.mvItems ?? []);
  const [mvLoading, setMvLoading] = useState(false);
  const [mvColorBy, setMvColorBy] = useState<"Powertrain"|"Trim">(() => cachedPage?.mvColorBy ?? "Powertrain");

  /* OJ positioning map (Bug 3) */
  const [pmTargetLength, setPmTargetLength] = useState(() => cachedPage?.pmTargetLength ?? "");
  const [pmTargetMsrp, setPmTargetMsrp] = useState(() => cachedPage?.pmTargetMsrp ?? "");
  const [pmLengthRange, setPmLengthRange] = useState(() => cachedPage?.pmLengthRange ?? 600);
  const [pmManualInput, setPmManualInput] = useState("");
  const [pmManualCompetitors, setPmManualCompetitors] = useState<string[]>(() => cachedPage?.pmManualCompetitors ?? []);
  const [pmTopN, setPmTopN] = useState(() => cachedPage?.pmTopN ?? 80);
  const [pmNClusters, setPmNClusters] = useState(() => cachedPage?.pmNClusters ?? 4);
  const [pmItems, setPmItems] = useState<PositioningMapItem[]>(() => cachedPage?.pmItems ?? []);
  const [pmTarget, setPmTarget] = useState<{ Length: number; MSRP: number } | null>(() => cachedPage?.pmTarget ?? null);
  const [pmClusterTop3, setPmClusterTop3] = useState<string[]>(() => cachedPage?.pmClusterTop3 ?? []);
  const [pmPeerCorridor, setPmPeerCorridor] = useState<PositioningPeerCorridor | null>(() => cachedPage?.pmPeerCorridor ?? null);
  const [pmLoading, setPmLoading] = useState(false);

  /* global time axis */
  const [timeRange, setTimeRange] = useState<TimeRange | null>(() => cachedPage?.timeRange ?? null);
  const [monthGrain, setMonthGrain] = useState<"month"|"quarter"|"year">(() => cachedPage?.monthGrain ?? "month");
  const timeRangePayload = useMemo(
    () => (timeRange ? { start: timeRange.start, end: timeRange.end } : undefined),
    [timeRange],
  );

  /* export settings (one per chart section) */
  const [tsExport, setTsExport] = useState<ExportSettings>(() => cachedPage?.tsExport ?? { ...DEFAULT_EXPORT });
  const [deckExportDrawerOpen, setDeckExportDrawerOpen] = useState(false);
  const [deckControlDrawerOpen, setDeckControlDrawerOpen] = useState(false);
  const [deckLayouts, setDeckLayouts] = useState<DashboardDeckLayouts>(
    readDashboardDeckLayouts,
  );
  const handleControlDrawerOpen = (open: boolean) => {
    if (open) setDeckExportDrawerOpen(false);
    setDeckControlDrawerOpen(open);
  };
  const handleExportDrawerOpen = (open: boolean) => {
    if (open) setDeckControlDrawerOpen(false);
    setDeckExportDrawerOpen(open);
  };
  useEffect(() => {
    try { localStorage.setItem("dashboard-deck-layouts", JSON.stringify(deckLayouts)); } catch {}
  }, [deckLayouts]);
  const [advExport, setAdvExport] = useState<ExportSettings>(() => cachedPage?.advExport ?? { ...DEFAULT_ADVANCED_EXPORT });
  const [mvExport, setMvExport] = useState<ExportSettings>(() => cachedPage?.mvExport ?? { ...DEFAULT_EXPORT });
  const [pmExport, setPmExport] = useState<ExportSettings>(() => cachedPage?.pmExport ?? { ...DEFAULT_EXPORT });
  const tsChartRef = useRef<HTMLDivElement | null>(null);
  const advChartRef = useRef<HTMLDivElement | null>(null);
  const advRequestAbortRef = useRef<AbortController | null>(null);
  const mvRequestAbortRef = useRef<AbortController | null>(null);
  const pmRequestAbortRef = useRef<AbortController | null>(null);
  useEffect(() => () => {
    advRequestAbortRef.current?.abort();
    advRequestAbortRef.current = null;
    mvRequestAbortRef.current?.abort();
    mvRequestAbortRef.current = null;
    pmRequestAbortRef.current?.abort();
    pmRequestAbortRef.current = null;
  }, []);
  // --- interactive "selected" label strategy for Advanced scatter charts ---
  const [selectedAdvKeys, setSelectedAdvKeys] = useState<Set<string>>(new Set());
  useEffect(() => {
    if (advExport.dataLabelOverlapStrategy !== "selected") {
      setSelectedAdvKeys(new Set());
    }
  }, [advExport.dataLabelOverlapStrategy]);
  const handleAdvClick = useCallback((event: Readonly<PlotMouseEvent>) => {
    if (advExport.dataLabelOverlapStrategy !== "selected") return;
    const pts = event.points ?? [];
    if (pts.length === 0) return;
    const point = pts[0];
    const series = typeof point.data?.name === "string" ? point.data.name : "";
    const text = typeof point.text === "string" ? point.text : "";
    if (!series || !text) return;
    const x = Number(point.x);
    const y = Number(point.y);
    if (!Number.isFinite(x) || !Number.isFinite(y)) return;
    setSelectedAdvKeys((prev) => {
      const next = new Set(prev);
      const pointKey = `${series}|${text}|${x}|${y}`;
      if (next.has(pointKey)) { next.delete(pointKey); } else { next.add(pointKey); }
      return next;
    });
  }, [advExport.dataLabelOverlapStrategy]);
  const initialAdvBubbleLabelStrategySyncDoneRef = useRef(false);
  useEffect(() => {
    if (initialAdvBubbleLabelStrategySyncDoneRef.current) return;
    if (advChart !== "powertrain_bubble") return;
    initialAdvBubbleLabelStrategySyncDoneRef.current = true;
    if (advExport.dataLabelOverlapStrategy === "all") {
      setAdvExport((previous) => (
        previous.dataLabelOverlapStrategy === "all"
          ? { ...previous, dataLabelOverlapStrategy: "smart_top" }
          : previous
      ));
    }
  }, [advChart, advExport.dataLabelOverlapStrategy]);
  const mvChartRef = useRef<HTMLDivElement | null>(null);
  const pmChartRef = useRef<HTMLDivElement | null>(null);

  const [error, setError] = useState("");
  const combinedError = sharedError || error;
  const [heroLoadingTick, setHeroLoadingTick] = useState(0);
  const filterTimeScopeKey = useMemo(
    () => `${filterPayloadStr}::${activeTab}::${timeRange?.start ?? ""}::${timeRange?.end ?? ""}`,
    [activeTab, filterPayloadStr, timeRange],
  );

  /* data freshness per country */
  const [freshnessItems, setFreshnessItems] = useState<DataFreshnessItem[]>([]);
  useEffect(() => {
    let cancelled = false;
    const controller = new AbortController();
    const cancelPreload = scheduleDashboardDelayedIdlePreload(() => {
      dashboardApi.dataFreshness({ signal: controller.signal }).then((res) => {
        if (!cancelled) setFreshnessItems(res.items ?? []);
      }).catch(() => {});
    }, DASHBOARD_DATA_FRESHNESS_DELAY_MS);
    return () => {
      cancelled = true;
      controller.abort();
      cancelPreload();
    };
  }, []);

  /* deck section selector for global drawers */
  const [activeDeckSection, setActiveDeckSection] = useState<DeckSectionKey>("timeSeries");
  const activeDeckLayout = deckLayouts[activeDeckSection];
  const timeSeriesDeckLayout = deckLayouts.timeSeries;
  const advancedDeckLayout = deckLayouts.advanced;
  const modelVersionDeckLayout = deckLayouts.modelVersion;
  const positioningDeckLayout = deckLayouts.positioning;
  const updateActiveDeckLayout = (
    patch: Partial<DashboardDeckLayoutSettings>,
  ) => {
    setDeckLayouts((current) => ({
      ...current,
      [activeDeckSection]: {
        ...current[activeDeckSection],
        ...patch,
      },
    }));
  };
  const resetActiveDeckLayout = () => {
    setDeckLayouts((current) => ({
      ...current,
      [activeDeckSection]: DEFAULT_DASHBOARD_DECK_LAYOUTS[activeDeckSection],
    }));
  };

  /* control drawer tabs */
  const [deckControlTab, setDeckControlTab] = useState<"window" | "chart" | "layout">("window");
  const DECK_CONTROL_TABS: DeckControlTabItem<"window" | "chart" | "layout">[] = [
    { key: "window", label: "窗口", caption: "时间范围" },
    { key: "chart", label: "图表", caption: "切换与分组" },
    { key: "layout", label: "版式", caption: "图表高度" },
  ];

  useEffect(() => {
    if (!loading) {
      setHeroLoadingTick(0);
      return;
    }
    const timer = window.setInterval(() => {
      setHeroLoadingTick((tick) => tick + 1);
    }, 80);
    return () => window.clearInterval(timer);
  }, [loading]);

  useEffect(() => {
    if (chartRuntimeReady || !filtersReady || loading || columns.length === 0) return;
    return scheduleDashboardDelayedIdlePreload(
      () => {
        setChartRuntimeReady(true);
      },
      DASHBOARD_CHART_RUNTIME_MIN_DELAY_MS,
    );
  }, [chartRuntimeReady, columns.length, filtersReady, loading]);

  useEffect(() => {
    if (heavyQueriesReady || !filtersReady || loading || columns.length === 0) return;
    return scheduleDashboardDelayedIdlePreload(
      () => {
        setHeavyQueriesReady(true);
      },
      DASHBOARD_HEAVY_QUERY_MIN_DELAY_MS,
    );
  }, [columns.length, filtersReady, heavyQueriesReady, loading]);

  useEffect(() => {
    if (selections.model.length !== 1) return;
    setMvModelName((current) => current || selections.model[0]);
  }, [selections.model]);

  const initialBubbleLabelSyncDoneRef = useRef(false);
  useEffect(() => {
    if (initialBubbleLabelSyncDoneRef.current) return;
    if (advChart !== "powertrain_bubble") return;
    initialBubbleLabelSyncDoneRef.current = true;
    if (advBubbleGrain === "version" && advBubbleLabelDimension !== "version") {
      setAdvBubbleLabelDimension("version");
    }
  }, [advBubbleGrain, advBubbleLabelDimension, advChart]);

  /* B3: auto-reload advanced chart when filters change */
  const prevAdvPayloadRef = useRef(filterTimeScopeKey);
  const prevMvScopeRef = useRef(filterTimeScopeKey);
  const prevPmScopeRef = useRef(filterTimeScopeKey);
  useEffect(() => {
    if (!heavyQueriesReady || optionsSyncPending || prevAdvPayloadRef.current === filterTimeScopeKey || advItems.length === 0 || columns.length === 0) return;
    prevAdvPayloadRef.current = filterTimeScopeKey;
    loadAdvChart();
  }, [advItems.length, columns.length, filterTimeScopeKey, heavyQueriesReady, loadAdvChart, optionsSyncPending]);

  useEffect(() => {
    if (!heavyQueriesReady || optionsSyncPending || prevMvScopeRef.current === filterTimeScopeKey || mvItems.length === 0 || !mvModelName.trim()) return;
    prevMvScopeRef.current = filterTimeScopeKey;
    loadModelVersions();
  }, [filterTimeScopeKey, heavyQueriesReady, loadModelVersions, mvItems.length, mvModelName, optionsSyncPending]);

  useEffect(() => {
    if (!heavyQueriesReady || optionsSyncPending || prevPmScopeRef.current === filterTimeScopeKey || pmItems.length === 0) return;
    prevPmScopeRef.current = filterTimeScopeKey;
    loadPositioningMap();
  }, [filterTimeScopeKey, heavyQueriesReady, loadPositioningMap, optionsSyncPending, pmItems.length]);

  /* auto-fetch grouped time series */
  useEffect(() => {
    if (!heavyQueriesReady || tsMode !== "\u5206\u7ec4" || columns.length === 0) return;
    const filters = JSON.parse(filterPayloadStr) as Record<string, string[]>;
    setGroupedLoading(true);
    let cancelled = false;
    const controller = new AbortController();
      const timer = setTimeout(async () => {
        setError("");
        try {
          const shareSplitBy = tsMode === "\u5206\u7ec4" && isTimeSeriesShareGroupDimension(tsGroupDim) && tsShareSplit !== "total"
            ? tsShareSplit
            : undefined;
          const r = await dashboardApi.groupedTimeSeries({
            filters,
            grain: activeTab,
            group_by: tsGroupDim,
            share_split_by: shareSplitBy,
            top_n: chartType === "rank" ? rankLimit : (tsTopNEnabled ? tsTopN : 9999),
            include_others: tsIncludeOthers,
            time_range: timeRangePayload,
          }, { signal: controller.signal });
          if (!cancelled) {
            setGroupedItems(ensureArray(r.items));
            setHiddenSeries(new Set());
            setOthersDetail(ensureArray(r.others_detail));
          }
        } catch (e) { if (!cancelled && !isAbortError(e)) setError((e as Error).message); }
        finally { if (!cancelled) setGroupedLoading(false); }
      }, 300);
      return () => {
        cancelled = true;
        controller.abort();
        clearTimeout(timer);
        setGroupedLoading(false);
      };
  }, [tsMode, tsGroupDim, tsShareSplit, activeTab, tsTopN, tsTopNEnabled, tsIncludeOthers, chartType, rankLimit, filterPayloadStr, columns.length, heavyQueriesReady, timeRangePayload]);

  /* advanced chart */
  async function loadAdvChart() {
    prevAdvPayloadRef.current = filterTimeScopeKey;
    advRequestAbortRef.current?.abort();
    const controller = new AbortController();
    advRequestAbortRef.current = controller;
    setAdvLoading(true); setError("");
    try {
      const opts: Record<string, unknown> = { band_size: advBandSize };
      if (timeRangePayload) opts.time_range = timeRangePayload;
      if (advChart === "powertrain_bubble") {
        opts.grain = advBubbleGrain;
        opts.show_yoy = advBubbleShowYoy;
        if (advBubbleShowYoy && advBubbleYoyYear) opts.yoy_compare_year = advBubbleYoyYear;
        opts.group_top_n = advBubbleGroupTopN;
        opts.group_dimension = advBubbleGroupDimension;
        opts.group_values = advBubbleGroupValues;
        opts.group_top_n_map = advBubbleGroupTopNMap;
      }
      if (usesNevPowertrainFilter(advChart)) {
        opts.powertrains = advPowertrains;
      }
      if (advChart === "nev_range_distribution") {
        opts.range_step = advRangeStep;
        opts.top_n_enabled = advNevTopNEnabled;
        opts.axis_max = advNevAxisMax;
        opts.metric_mode = advNevMetricMode;
        opts.stack_by_model = advNevStackByModel;
        opts.facet_brand = advNevFacetBrand;
        opts.max_brand_facets = advNevMaxBrandFacets;
      }
      if (advChart === "estimated_tco") {
        opts.years = tcoYears; opts.annual_km = tcoAnnualKm;
        opts.depreciation_rate = tcoDepreciation; opts.maintenance_rate = tcoMaintenance;
        opts.tax_insurance_rate = tcoTaxInsurance; opts.energy_cost_base = tcoEnergyCost;
      }
      const r = await dashboardApi.advancedChart({
        group: advGroup,
        chart: advChart,
        filters: buildFilterPayload(),
        top_n: advTopN,
        options: opts,
        time_range: timeRangePayload,
      }, { signal: controller.signal });
      if (advRequestAbortRef.current !== controller || controller.signal.aborted) return;
      setAdvItems(ensureArray(r.items));
      setAdvMeta(r.meta ?? null);
    } catch (e) {
      if (advRequestAbortRef.current === controller && !isAbortError(e)) {
        setError((e as Error).message);
      }
    }
    finally {
      if (advRequestAbortRef.current === controller) {
        advRequestAbortRef.current = null;
        setAdvLoading(false);
      }
    }
  }

  /* model version bubble */
  async function loadModelVersions() {
    mvRequestAbortRef.current?.abort();
    if (!mvModelName.trim()) {
      setMvLoading(false);
      return;
    }
    prevMvScopeRef.current = filterTimeScopeKey;
    const controller = new AbortController();
    mvRequestAbortRef.current = controller;
    setMvLoading(true); setError("");
    try {
      const r = await dashboardApi.modelVersions({
        filters: buildFilterPayload(),
        model_name: mvModelName.trim(),
        top_n: mvTopN,
        time_range: timeRangePayload,
      }, { signal: controller.signal });
      if (mvRequestAbortRef.current !== controller || controller.signal.aborted) return;
      setMvItems(ensureArray(r.items));
    } catch (e) {
      if (mvRequestAbortRef.current === controller && !isAbortError(e)) {
        setError((e as Error).message);
      }
    }
    finally {
      if (mvRequestAbortRef.current === controller) {
        mvRequestAbortRef.current = null;
        setMvLoading(false);
      }
    }
  }

  /* OJ positioning map */
  function addCompetitor() {
    const v = pmManualInput.trim();
    if (v && !pmManualCompetitors.includes(v)) setPmManualCompetitors(p => [...p, v]);
    setPmManualInput("");
  }
  function toggleAdvBubbleGroupValue(value: string) {
    setAdvBubbleGroupValues((current) => (
      current.includes(value)
        ? current.filter((item) => item !== value)
        : [...current, value]
    ));
  }
  async function loadPositioningMap() {
    prevPmScopeRef.current = filterTimeScopeKey;
    pmRequestAbortRef.current?.abort();
    const controller = new AbortController();
    pmRequestAbortRef.current = controller;
    setPmLoading(true); setError("");
    try {
      const r = await dashboardApi.positioningMap({
        filters: buildFilterPayload(),
        target_length: pmTargetLength ? Number(pmTargetLength) : null,
        target_msrp: pmTargetMsrp ? Number(pmTargetMsrp) : null,
        length_range: pmLengthRange,
        manual_competitors: pmManualCompetitors,
        top_n: pmTopN,
        n_clusters: pmNClusters,
        time_range: timeRangePayload,
      }, { signal: controller.signal });
      if (pmRequestAbortRef.current !== controller || controller.signal.aborted) return;
      setPmItems(ensureArray(r.items)); setPmTarget(r.target ?? null); setPmClusterTop3(ensureArray(r.cluster_top3)); setPmPeerCorridor(r.peerCorridor ?? null);
    } catch (e) {
      if (pmRequestAbortRef.current === controller && !isAbortError(e)) {
        setError((e as Error).message);
      }
    }
    finally {
      if (pmRequestAbortRef.current === controller) {
        pmRequestAbortRef.current = null;
        setPmLoading(false);
      }
    }
  }

  /* ── derived chart data ──────────────────────────── */
  const kpis = overview?.kpis;
  const dashboardBootstrapping = useMemo(
    () => isDashboardBootstrapping(filtersReady, loading, overview),
    [filtersReady, loading, overview],
  );
  const timeWindowLabel = timeRange ? `${timeRange.start} ~ ${timeRange.end}` : "Full timeline";
  const activeFilterSummaryText = useMemo(
    () => getDashboardLensSummary(activeFilterSummary, activeFilters.length, dashboardBootstrapping),
    [activeFilterSummary, activeFilters.length, dashboardBootstrapping],
  );
  const activeLensTokens = useMemo(() => {
    const filterTokens = dashboardBootstrapping
      ? [activeFilterSummaryText]
      : activeFilters.length === 0
      ? ["Default powertrain lens"]
      : activeFilters.map(({ key, label }) => `${label}: ${summarizeScopeValues(selections[key] ?? [])}`);
    return [...filterTokens, `Time window: ${timeWindowLabel}`];
  }, [activeFilterSummaryText, activeFilters, dashboardBootstrapping, selections, timeWindowLabel]);
  const activeFilterCount = activeFilters.length;
  const isGrouped = tsMode === "\u5206\u7ec4";
  const isShareGrouped = isGrouped && isTimeSeriesShareGroupDimension(tsGroupDim);
  const singleSeries = activeTab === "year" ? yearSeries : monthSeries;

  /* all series names (stable order for consistent colours) */
  const allSeriesNames = useMemo(() => {
    if (!isGrouped || groupedItems.length === 0) return [] as string[];
    const s = new Set<string>();
    for (const item of groupedItems) s.add(item.series);
    return Array.from(s);
  }, [isGrouped, groupedItems]);

  const visibleSeries = useMemo(() =>
    allSeriesNames.filter(s => !hiddenSeries.has(s)),
    [allSeriesNames, hiddenSeries],
  );

  /* stacked chart pivot */
  const { stackData, stackKeys } = useMemo(() => {
    if (!STACKED_CHARTS.has(advChart) || advItems.length === 0 || advChart === "nev_range_distribution")
      return { stackData: [] as Record<string,unknown>[], stackKeys: [] as string[] };
    let xKey = "PriceBand"; let stackKey = "Powertrain";
    if (advChart === "segment_share_by_length") { xKey = "LengthBand"; stackKey = "Segment"; }
    const xMap = new Map<number, Record<string,unknown>>();
    const sSet = new Set<string>();
    for (const item of advItems) {
      const x = Number(item[xKey] ?? 0); const s = String(item[stackKey] ?? "");
      sSet.add(s);
      let row = xMap.get(x);
      if (!row) { row = { [xKey]: x }; xMap.set(x, row); }
      row[s] = (Number(row[s] ?? 0)) + Number(item.Sales ?? 0);
    }
    return { stackData: Array.from(xMap.values()).sort((a,b)=>Number(a[xKey])-Number(b[xKey])), stackKeys: Array.from(sSet) };
  }, [advChart, advItems]);

  /* price migration pivot */
  const { migrationData, migrationYears } = useMemo(() => {
    if (advChart !== "price_migration" || advItems.length === 0)
      return { migrationData: [] as Record<string,unknown>[], migrationYears: [] as string[] };
    const bandMap = new Map<number, Record<string,unknown>>();
    const yearSet = new Set<string>();
    for (const item of advItems) {
      const band = Number(item.priceBand ?? 0); const yr = String(item.year ?? "");
      yearSet.add(yr);
      let row = bandMap.get(band); if (!row) { row = { priceBand: band }; bandMap.set(band, row); }
      row[yr] = Number(item.sales ?? 0);
    }
    return { migrationData: Array.from(bandMap.values()).sort((a,b)=>Number(a.priceBand)-Number(b.priceBand)), migrationYears: Array.from(yearSet).sort() };
  }, [advChart, advItems]);

  /* heatmap */
  const months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];
  const hmYears = [...new Set(advItems.filter(()=>advChart==="seasonality_heatmap").map(r=>String(r.year??"")))].filter(Boolean).sort();
  const hmMax = Math.max(1,...advItems.filter(()=>advChart==="seasonality_heatmap").map(r=>Number(r.value??0)));
  function hmVal(y:string,m:string){return Number(advItems.find(r=>String(r.year)===y&&String(r.month)===m)?.value??0);}
  function hmColor(v:number){return "rgba(7,89,133,"+(0.1+(v/hmMax)*0.8).toFixed(3)+")";}
  const advBubbleYearOptions = useMemo(() => {
    const years = Array.from(new Set(
      yearSeries
        .map((point) => String(point.time ?? "").trim())
        .filter((value) => /^\d{4}$/.test(value)),
    ));
    return years.sort(compareTimeLabels);
  }, [yearSeries]);
  const advBubbleGroupFilterKey: FilterKey = advBubbleGroupDimension === "segment" ? "segment" : "powertrain";
  const advBubbleGroupOptions = useMemo(() => {
    const source = selections[advBubbleGroupFilterKey].length > 0
      ? selections[advBubbleGroupFilterKey]
      : optionsMap[advBubbleGroupFilterKey] ?? [];
    const values = Array.from(new Set(source.map((value) => value.trim()).filter(Boolean)));
    if (advBubbleGroupDimension === "powertrain") {
      return values.sort((a, b) => {
        const ai = DEFAULT_POWERTRAINS.findIndex((name) => name === normalizePowertrainName(a));
        const bi = DEFAULT_POWERTRAINS.findIndex((name) => name === normalizePowertrainName(b));
        if (ai !== -1 || bi !== -1) return (ai === -1 ? 999 : ai) - (bi === -1 ? 999 : bi);
        return a.localeCompare(b);
      });
    }
    return values.sort((a, b) => a.localeCompare(b, "zh-Hans"));
  }, [advBubbleGroupDimension, advBubbleGroupFilterKey, optionsMap, selections]);
  useEffect(() => {
    if (!advBubbleShowYoy) return;
    if (advBubbleYearOptions.length < 2) {
      if (advBubbleYoyYear) setAdvBubbleYoyYear("");
      return;
    }
    if (advBubbleYoyYear && advBubbleYearOptions.includes(advBubbleYoyYear)) return;
    setAdvBubbleYoyYear(advBubbleYearOptions[advBubbleYearOptions.length - 1] ?? "");
  }, [advBubbleShowYoy, advBubbleYoyYear, advBubbleYearOptions]);
  useEffect(() => {
    setAdvBubbleGroupValues((current) => {
      const next = current.filter((value) => advBubbleGroupOptions.includes(value));
      if (!advBubbleGroupTopN) return next;
      if (next.length > 0) return next;
      return advBubbleGroupOptions.slice(0, Math.min(2, advBubbleGroupOptions.length));
    });
  }, [advBubbleGroupDimension, advBubbleGroupOptions, advBubbleGroupTopN]);

  /* time axis labels */
  const timeLabels = useMemo(() => {
    const src = activeTab === "year" ? yearSeries : monthSeries;
    return src.map(s => s.time);
  }, [activeTab, yearSeries, monthSeries]);

  /* filter series by time range */
  function filterByTimeRange<T extends { time: string }>(items: T[]): T[] {
    if (!timeRange) return items;
    const start = toTimeOrdinal(timeRange.start);
    const end = toTimeOrdinal(timeRange.end);
    return items.filter(it => {
      const current = toTimeOrdinal(it.time);
      if (start !== null && end !== null && current !== null) return current >= start && current <= end;
      return compareTimeLabels(it.time, timeRange.start) >= 0 && compareTimeLabels(it.time, timeRange.end) <= 0;
    });
  }

  const filteredSingle = useMemo(() => filterByTimeRange(singleSeries), [singleSeries, timeRange]);

  /* B6: aggregate monthly data by quarter/year when monthGrain is set */
  const aggregatedSingle = useMemo(() => {
    if (activeTab !== "month" || monthGrain === "month") return filteredSingle;
    const groups = new Map<string, number>();
    for (const s of filteredSingle) {
      const month = parseMonthLabel(s.time);
      if (!month) continue;
      let key: string;
      if (monthGrain === "quarter") {
        const q = Math.ceil(month.month / 3);
        key = `${month.year}-Q${q}`;
      } else {
        key = String(month.year);
      }
      groups.set(key, (groups.get(key) ?? 0) + s.value);
    }
    return Array.from(groups.entries()).map(([time, value]) => ({ time, value })).sort((a, b) => compareTimeLabels(a.time, b.time));
  }, [activeTab, monthGrain, filteredSingle]);
  const singleTimeLabels = useMemo(
    () => aggregatedSingle.map(point => point.time),
    [aggregatedSingle],
  );
  const filteredGrouped = useMemo(() => filterByTimeRange(groupedItems), [groupedItems, timeRange]);
  const groupedTimeLabels = useMemo(
    () => Array.from(new Set(filteredGrouped.map(point => point.time))).sort(compareTimeLabels),
    [filteredGrouped],
  );

  /* ranking data: aggregate across time window, rank by total volume */
  const rankingData = useMemo(() => {
    if (chartType !== "rank" || !isGrouped) return [] as { name: string; volume: number; share: number }[];
    const totals = new Map<string, number>();
    for (const item of filteredGrouped) {
      if (!hiddenSeries.has(item.series)) {
        totals.set(item.series, (totals.get(item.series) ?? 0) + item.value);
      }
    }
    const grandTotal = Array.from(totals.values()).reduce((s, v) => s + v, 0) || 1;
    return Array.from(totals.entries())
      .map(([name, volume]) => ({ name, volume, share: volume / grandTotal }))
      .sort((a, b) => b.volume - a.volume);
  }, [chartType, isGrouped, filteredGrouped, hiddenSeries]);
  const rankingSliced = useMemo(
    () => rankingData.slice(0, rankLimit),
    [rankingData, rankLimit],
  );
  /* B7: time-window KPI — compute sales from filtered time series */
  const timeWindowSales = useMemo(() => {
    if (!timeRange) return kpis?.cumulativeSales;
    return filteredSingle.reduce((sum, s) => sum + s.value, 0);
  }, [timeRange, filteredSingle, kpis]);
  const heroTotalSales = useMemo(() => {
    const target = Number(timeWindowSales ?? 0);
    return loading ? getLoadingMetricValue(target, heroLoadingTick, 120000) : target;
  }, [heroLoadingTick, loading, timeWindowSales]);
  const heroVersionCount = useMemo(() => {
    const target = Number(kpis?.versionCount ?? 0);
    return loading ? getLoadingMetricValue(target, heroLoadingTick + 5, 240) : target;
  }, [heroLoadingTick, kpis, loading]);
  const heroTotalSalesText = useMemo(() => formatMetricValue(heroTotalSales), [heroTotalSales]);
  const heroVersionCountText = useMemo(() => formatMetricValue(heroVersionCount), [heroVersionCount]);
  const sidebarSummaryItems = useMemo(
    () => [
      { key: "rows", label: "筛选后记录数", value: formatDashboardSummaryMetric(kpis?.totalRows, dashboardBootstrapping) },
      { key: "brands", label: "品牌数", value: formatDashboardSummaryMetric(kpis?.brandCount, dashboardBootstrapping) },
      { key: "models", label: "Model 数", value: formatDashboardSummaryMetric(kpis?.modelCount, dashboardBootstrapping) },
      { key: "versions", label: "Version 数", value: formatDashboardSummaryMetric(kpis?.versionCount, dashboardBootstrapping) },
    ],
    [dashboardBootstrapping, kpis],
  );
  const sidebarSummaryDensityClass = useMemo(
    () => getUnifiedMetricDensityClass(sidebarSummaryItems.map((item) => item.value)),
    [sidebarSummaryItems],
  );
  const dashboardCacheSnapshot = useMemo<DashboardPageCache>(() => ({
    search: dashboardSearch,
    columns,
    selections,
    optionsMap,
    heroCollapsed,
    sidebarCollapsed,
    filteredRowCount,
    overview,
    yearSeries,
    monthSeries,
    tsExport,
    activeTab,
    chartType,
    rankLimit,
    tsMode,
    tsGroupDim,
    tsShareSplit,
    tsTopN,
    tsTopNEnabled,
    tsIncludeOthers,
    groupedItems,
    hiddenSeries: Array.from(hiddenSeries),
    othersDetail,
    advGroup,
    advChart,
    advItems,
    advMeta,
    advBandSize,
    advTopN,
    advMigrationMode,
    advBubbleScale,
    advBubbleGrain,
    advBubbleLabelDimension,
    advBubbleFacet,
    advBubbleFacetMax,
    advBubbleShowYoy,
    advBubbleYoyYear,
    advBubbleGroupTopN,
    advBubbleGroupDimension,
    advBubbleGroupValues,
    advBubbleGroupTopNMap,
    advPowertrains,
    advNevTopNEnabled,
    advNevAxisMax,
    advNevMetricMode,
    advNevStackByModel,
    advNevFacetBrand,
    advNevMaxBrandFacets,
    advNevRangeQuery,
    advRangeStep,
    advHeatmapScale,
    tcoYears,
    tcoAnnualKm,
    tcoDepreciation,
    tcoMaintenance,
    tcoTaxInsurance,
    tcoEnergyCost,
    mvModelName,
    mvTopN,
    mvItems,
    mvColorBy,
    pmTargetLength,
    pmTargetMsrp,
    pmLengthRange,
    pmManualCompetitors,
    pmTopN,
    pmNClusters,
    pmItems,
    pmTarget,
    pmClusterTop3,
    pmPeerCorridor,
    timeRange,
    monthGrain,
    advExport,
    mvExport,
    pmExport,
  }), [
    activeTab,
    advBandSize,
    advBubbleFacet,
    advBubbleFacetMax,
    advBubbleGroupDimension,
    advBubbleGroupTopN,
    advBubbleGroupTopNMap,
    advBubbleGroupValues,
    advBubbleGrain,
    advBubbleLabelDimension,
    advBubbleScale,
    advBubbleShowYoy,
    advBubbleYoyYear,
    advChart,
    advExport,
    advGroup,
    advHeatmapScale,
    advItems,
    advMeta,
    advMigrationMode,
    advNevAxisMax,
    advNevFacetBrand,
    advNevMaxBrandFacets,
    advNevMetricMode,
    advNevRangeQuery,
    advNevStackByModel,
    advNevTopNEnabled,
    advPowertrains,
    advRangeStep,
    advTopN,
    chartType,
    rankLimit,
    columns,
    dashboardSearch,
    filteredRowCount,
    heroCollapsed,
    sidebarCollapsed,
    groupedItems,
    hiddenSeries,
    monthGrain,
    monthSeries,
    mvColorBy,
    mvExport,
    mvItems,
    mvModelName,
    mvTopN,
    optionsMap,
    othersDetail,
    overview,
    pmClusterTop3,
    pmExport,
    pmItems,
    pmLengthRange,
    pmManualCompetitors,
    pmNClusters,
    pmPeerCorridor,
    pmTarget,
    pmTargetLength,
    pmTargetMsrp,
    pmTopN,
    selections,
    tcoAnnualKm,
    tcoDepreciation,
    tcoEnergyCost,
    tcoMaintenance,
    tcoTaxInsurance,
    tcoYears,
    timeRange,
    tsExport,
    tsGroupDim,
    tsIncludeOthers,
    tsMode,
    tsShareSplit,
    tsTopN,
    tsTopNEnabled,
    yearSeries,
  ]);
  useEffect(() => {
    if (columns.length === 0) return;
    setCachedPageValue(DASHBOARD_CACHE_KEY, dashboardCacheSnapshot, PAGE_CACHE_TTL_MS);
  }, [columns.length, dashboardCacheSnapshot]);

  /* palette helper */
  const tsPalette = useMemo(() => getExportPalette(tsExport.colorScheme), [tsExport.colorScheme]);
  const advPalette = useMemo(() => getExportPalette(advExport.colorScheme), [advExport.colorScheme]);
  const mvPalette = useMemo(() => getExportPalette(mvExport.colorScheme), [mvExport.colorScheme]);
  const pmPalette = useMemo(() => getExportPalette(pmExport.colorScheme), [pmExport.colorScheme]);
  const filterFocusedPowertrain = useMemo(
    () => readSinglePowertrain(selections.powertrain),
    [selections.powertrain],
  );
  const advancedFocusedPowertrain = useMemo(
    () => filterFocusedPowertrain ?? (usesNevPowertrainFilter(advChart) ? readSinglePowertrain(advPowertrains) : null),
    [advChart, advPowertrains, filterFocusedPowertrain],
  );
  const tsLabelModeOptions = useMemo(
    () => buildExportLabelModeOptions({ showValue: true, showSeries: isGrouped }),
    [isGrouped],
  );
  const advLabelModeOptions = useMemo(() => {
    if (advChart === "seasonality_heatmap") {
      return buildExportLabelModeOptions({ showValue: false, showSeries: false });
    }
    if (SCATTER_CHARTS.has(advChart)) {
      const hasModel = advItems.some(item => String(item.Model ?? "").trim());
      const hasSales = advItems.some(item => item.Sales !== undefined);
      return buildExportLabelModeOptions({ showValue: true, showSeries: true, showModel: hasModel, showSales: hasSales });
    }
    if (STACKED_CHARTS.has(advChart) || advChart === "price_migration") {
      return buildExportLabelModeOptions({ showValue: true, showSeries: true });
    }
    return buildExportLabelModeOptions({ showValue: true, showSeries: false });
  }, [advChart, advItems]);
  const mvLabelModeOptions = useMemo(
    () => buildExportLabelModeOptions({ showValue: true, showSeries: true, showSales: mvItems.length > 0 }),
    [mvItems.length],
  );
  const pmLabelModeOptions = useMemo(
    () => buildExportLabelModeOptions({ showValue: true, showSeries: true, showModel: pmItems.some(item => item.Model.trim()), showSales: pmItems.length > 0 }),
    [pmItems],
  );
  /* simple bar */
  const isSimpleBar = !SCATTER_CHARTS.has(advChart) && !STACKED_CHARTS.has(advChart) && advChart !== "price_migration" && advChart !== "seasonality_heatmap" && advChart !== "rv_finance_dashboard";
  const maxBar = Math.max(1,...advItems.map(r=>Number(r.value??0)));
  const bubbleMetaRecord = advChart === "powertrain_bubble" && advMeta ? advMeta : null;
  const bubbleYoyEnabled = Boolean(bubbleMetaRecord?.yoyEnabled);
  const bubbleYoyCompareYear = asMetaText(bubbleMetaRecord?.yoyCompareYear);
  const bubbleYoyBaseYear = asMetaText(bubbleMetaRecord?.yoyBaseYear);
  const bubbleGroupTopNApplied = Boolean(bubbleMetaRecord?.groupTopNApplied);
  const bubbleGroupDimensionLabel = asMetaText(bubbleMetaRecord?.groupDimensionLabel);
  const bubbleSelectedGroups = asMetaStringArray(bubbleMetaRecord?.groupValues);
  const bubbleWarnings = asMetaStringArray(bubbleMetaRecord?.warnings);
  const nevMetaRecord = advChart === "nev_range_distribution" && advMeta ? advMeta : null;
  const nevMetricMode = asMetaText(nevMetaRecord?.metricMode) || "window_sales";
  const nevMetricTitle = asMetaText(nevMetaRecord?.metricTitle) || "销量";
  const nevRangeColumn = asMetaText(nevMetaRecord?.rangeColumn) || "Battery range";
  const nevStackKey = asMetaText(nevMetaRecord?.stackKey) || "Powertrain";
  const nevSplitByBrand = Boolean(nevMetaRecord?.splitByBrand);
  const nevBrands = asMetaStringArray(nevMetaRecord?.brands);
  const nevWarnings = asMetaStringArray(nevMetaRecord?.warnings);
  const nevRangeStep = asMetaNumber(nevMetaRecord?.rangeStep) ?? 50;
  const nevAxisMaxMeta = asMetaNumber(nevMetaRecord?.axisMax) ?? advNevAxisMax;
  const nevAnnualSales = asMetaRecordArray(nevMetaRecord?.annualSales);
  const nevPowertrainSummary = asMetaRecordArray(nevMetaRecord?.powertrainSummary);
  const nevBucketSummary = asMetaRecordArray(nevMetaRecord?.bucketSummary);
  const nevBucketPositive = asMetaRecordArray(nevMetaRecord?.bucketPositive);
  const nevBucketNegative = asMetaRecordArray(nevMetaRecord?.bucketNegative);
  const nevModelMovers = asMetaRecordArray(nevMetaRecord?.modelMovers);
  const nevModelGains = asMetaRecordArray(nevMetaRecord?.modelGains);
  const nevModelDeclines = asMetaRecordArray(nevMetaRecord?.modelDeclines);
  const nevGrowthSpanLabel = asMetaText(nevMetaRecord?.growthSpanLabel);
  const nevKpis = isPlainRecord(nevMetaRecord?.kpis) ? nevMetaRecord.kpis : null;
  const nevTopModelLimit = asMetaNumber(nevMetaRecord?.topModelLimit);
  const nevTopModelAbsShare = asMetaNumber(nevMetaRecord?.topModelAbsShare);
  const nevRangeSamples = asMetaRecordArray(nevMetaRecord?.rangeSamples);
  const nevRangeSampleUnit = asMetaText(nevMetaRecord?.rangeSampleUnit) || "Model";
  const nevRangeQueryStats = useMemo(() => {
    const target = Number(advNevRangeQuery);
    if (!Number.isFinite(target) || target <= 0 || nevRangeSamples.length === 0) {
      return null;
    }
    const samples = nevRangeSamples
      .map((sample) => ({
        range: asMetaNumber(sample.BatteryRange),
        sales: Math.max(0, asMetaNumber(sample.Sales) ?? 0),
      }))
      .filter((sample): sample is { range: number; sales: number } => sample.range !== null && sample.range > 0);
    if (samples.length === 0) return null;
    const below = samples.filter((sample) => sample.range < target);
    const totalSales = samples.reduce((sum, sample) => sum + sample.sales, 0);
    const belowSales = below.reduce((sum, sample) => sum + sample.sales, 0);
    return {
      target,
      belowCount: below.length,
      totalCount: samples.length,
      belowShare: below.length / samples.length,
      belowSales,
      totalSales,
      belowSalesShare: totalSales > 0 ? belowSales / totalSales : null,
    };
  }, [advNevRangeQuery, nevRangeSamples]);
  const nevStackSeries = useMemo(() => {
    if (advChart !== "nev_range_distribution") return [] as string[];
    const series = new Set<string>();
    for (const item of advItems) {
      const name = String(item[nevStackKey] ?? "").trim();
      if (name) series.add(name);
    }
    const values = Array.from(series);
    if (nevStackKey === "Powertrain") {
      return values.sort((a, b) => {
        const ai = DEFAULT_POWERTRAINS.findIndex(name => name === normalizePowertrainName(a));
        const bi = DEFAULT_POWERTRAINS.findIndex(name => name === normalizePowertrainName(b));
        const av = ai === -1 ? Number.MAX_SAFE_INTEGER : ai;
        const bv = bi === -1 ? Number.MAX_SAFE_INTEGER : bi;
        return av - bv || a.localeCompare(b);
      });
    }
    return values.sort((a, b) => a.localeCompare(b));
  }, [advChart, advItems, nevStackKey]);
  const nevPowertrainTokens = useMemo(() => {
    if (advChart !== "nev_range_distribution" || nevMetricMode !== "net_change") {
      return [] as string[];
    }
    const netChangeTotal = asMetaNumber(nevKpis?.netChangeTotal);
    return nevPowertrainSummary.map((row) => {
      const powertrain = String(row.Powertrain ?? "-");
      const growth = asMetaNumber(row.GrowthWindow) ?? 0;
      const shareText = netChangeTotal && netChangeTotal !== 0
        ? `${((growth / netChangeTotal) * 100).toFixed(1)}%`
        : "N/A";
      return `${powertrain} ${growth.toLocaleString(undefined, { maximumFractionDigits: 0 })} (${shareText})`;
    });
  }, [advChart, nevKpis, nevMetricMode, nevPowertrainSummary]);
  const nevStartYearLabel = useMemo(() => {
    if (!nevGrowthSpanLabel.includes("-")) return "首年";
    const tokens = nevGrowthSpanLabel.split("-");
    return tokens[tokens.length - 1] || "首年";
  }, [nevGrowthSpanLabel]);
  const nevChartTitle = useMemo(() => {
    if (advChart !== "nev_range_distribution") return "";
    if (nevMetricMode === "net_change") {
      return nevGrowthSpanLabel
        ? `NEV 续航分布变化（${nevGrowthSpanLabel}）`
        : "NEV 续航分布变化（末年-首年）";
    }
    return nevStackKey === "Model"
      ? "NEV 续航分布（Model堆叠）"
      : "NEV 续航分布（BEV/PHEV）";
  }, [advChart, nevGrowthSpanLabel, nevMetricMode, nevStackKey]);
  const nevFacetPlot = useMemo(() => {
    if (advChart !== "nev_range_distribution" || advItems.length === 0) {
      return {
        traces: [] as Data[],
        layout: {} as Partial<Layout>,
        height: 420,
      };
    }

    const isPowertrainStack = nevStackKey === "Powertrain";
    const rangeBands = Array.from(new Set(
      advItems
        .map(item => Number(item.RangeBand ?? 0))
        .filter(value => Number.isFinite(value)),
    )).sort((a, b) => a - b);
    const selectedBrands = nevSplitByBrand && nevBrands.length > 0
      ? nevBrands.filter(brand => advItems.some(item => String(item.Brand ?? "") === brand))
      : [];
    const facetBrands = selectedBrands.length > 0 ? selectedBrands : [null];
    const gridColumns = facetBrands.length > 1 ? Math.min(3, facetBrands.length) : 1;
    const gridRows = Math.max(1, Math.ceil(facetBrands.length / gridColumns));
    const traces: Data[] = [];
    const annotations: NonNullable<Layout["annotations"]> = [];
    const rangeQueryValue = Number(advNevRangeQuery);
    const hasRangeQueryLine = Number.isFinite(rangeQueryValue) && rangeQueryValue > 0;

    facetBrands.forEach((brand, facetIndex) => {
      const axisIndex = facetIndex + 1;
      const axisRef = axisIndex === 1 ? "" : String(axisIndex);
      const xAxisName = axisRef ? `x${axisRef}` : undefined;
      const yAxisName = axisRef ? `y${axisRef}` : undefined;
      const facetRows = brand === null
        ? advItems
        : advItems.filter(item => String(item.Brand ?? "") === brand);
      if (facetRows.length === 0) return;

      nevStackSeries.forEach((series, seriesIndex) => {
        const valueMap = new Map<number, number>();
        for (const item of facetRows) {
          const currentSeries = String(item[nevStackKey] ?? "").trim();
          if (currentSeries !== series) continue;
          const rangeBand = Number(item.RangeBand ?? 0);
          const value = Number(item.Value ?? 0);
          valueMap.set(rangeBand, (valueMap.get(rangeBand) ?? 0) + value);
        }
        if (valueMap.size === 0) return;

        traces.push({
          type: "bar",
          orientation: "h",
          name: series,
          x: rangeBands.map(rangeBand => valueMap.get(rangeBand) ?? 0),
          y: rangeBands,
          marker: {
            color: resolveAdvancedSeriesColor(
              series,
              seriesIndex,
              nevStackSeries.length,
              advPalette,
              isPowertrainStack,
              advExport.seriesColors,
              advancedFocusedPowertrain,
            ),
          },
          hovertemplate: `${brand ? `${brand}<br>` : ""}%{y:.0f} km<br>${series}: %{x:,.0f}<extra></extra>`,
          showlegend: facetIndex === 0,
          ...(xAxisName ? { xaxis: xAxisName } : {}),
          ...(yAxisName ? { yaxis: yAxisName } : {}),
        } as Data);
      });

      if (brand) {
        const columnIndex = facetIndex % gridColumns;
        const rowIndex = Math.floor(facetIndex / gridColumns);
        annotations.push({
          text: brand,
          x: (columnIndex + 0.5) / gridColumns,
          y: 1 - rowIndex / gridRows + 0.04,
          xref: "paper",
          yref: "paper",
          showarrow: false,
          font: { size: 12 },
        });
      }
    });

    const layout: Partial<Layout> = {
      title: { text: nevChartTitle },
      barmode: "stack",
      showlegend: true,
      margin: { t: facetBrands.length > 1 ? 76 : 48, b: 48, l: 78, r: 18 },
    };
    if (hasRangeQueryLine) {
      type PlotlyShapeYRef = NonNullable<NonNullable<Layout["shapes"]>[number]["yref"]>;
      layout.shapes = facetBrands.map((_, facetIndex) => ({
        type: "line",
        xref: "paper",
        x0: 0,
        x1: 1,
        yref: (facetIndex === 0 ? "y" : `y${facetIndex + 1}`) as PlotlyShapeYRef,
        y0: rangeQueryValue,
        y1: rangeQueryValue,
        line: { color: "#dc2626", width: 2, dash: "dash" },
      }));
      annotations.push({
        text: `${Math.round(rangeQueryValue).toLocaleString("en-US")} km`,
        x: 1,
        y: rangeQueryValue,
        xref: "paper",
        yref: "y",
        showarrow: false,
        xanchor: "right",
        yanchor: "bottom",
        font: { size: 11, color: "#dc2626" },
      });
      layout.annotations = annotations;
    }
    if (facetBrands.length > 1) {
      layout.grid = {
        rows: gridRows,
        columns: gridColumns,
        pattern: "independent",
      };
      layout.annotations = annotations;
    }

    facetBrands.forEach((_, facetIndex) => {
      const axisIndex = facetIndex + 1;
      const xaxisKey = axisIndex === 1 ? "xaxis" : `xaxis${axisIndex}`;
      const yaxisKey = axisIndex === 1 ? "yaxis" : `yaxis${axisIndex}`;
      const isLeftEdge = facetIndex % gridColumns === 0;
      (layout as Record<string, unknown>)[xaxisKey] = {
        title: { text: nevMetricTitle },
        automargin: true,
      };
      (layout as Record<string, unknown>)[yaxisKey] = {
        title: { text: isLeftEdge ? `Battery range（${nevRangeColumn}）` : "" },
        range: [0, nevAxisMaxMeta],
        dtick: nevRangeStep,
        automargin: true,
      };
    });

    return {
      traces,
      layout,
      height: Math.max(420, gridRows * 300),
    };
  }, [
    advChart,
    advItems,
    advNevRangeQuery,
    advExport.seriesColors,
    advPalette,
    advancedFocusedPowertrain,
    nevAxisMaxMeta,
    nevBrands,
    nevChartTitle,
    nevMetricTitle,
    nevRangeColumn,
    nevRangeStep,
    nevSplitByBrand,
    nevStackKey,
    nevStackSeries,
  ]);

  /* scatter axes config */
  function scatterAxes() {
    switch (advChart) {
      case "powertrain_bubble": return { x:"Length", y:"MSRP", z:"Sales", color:"Powertrain", xLabel:"\u8f66\u957f(mm)", yLabel:"MSRP" };
      case "nev_capacity_vs_msrp": return { x:"BatteryCapacity", y:"MSRP", z:"Sales", color:"Powertrain", xLabel:"\u7535\u6c60\u5bb9\u91cf(kWh)", yLabel:"MSRP" };
      case "nev_length_vs_range": return { x:"Length", y:"BatteryRange", z:"Sales", color:"Powertrain", xLabel:"\u8f66\u957f(mm)", yLabel:"\u7eaf\u7535\u7eed\u822a(km)" };
      case "length_vs_price": return { x:"Length", y:"MSRP", z:"Sales", color:"Segment", xLabel:"\u8f66\u957f(mm)", yLabel:"MSRP" };
      case "price_per_meter": return { x:"PricePerMeter", y:"Sales", z:"Sales", color:"Brand", xLabel:"\u6bcf\u7c73\u4ef7\u683c", yLabel:"\u9500\u91cf" };
      case "sales_vs_price": return { x:"MSRP", y:"Sales", z:"SegmentSharePct", color:"Segment", xLabel:"MSRP", yLabel:"\u9500\u91cf" };
      case "estimated_tco": return { x:"MSRP", y:"EstimatedTCO", z:"Sales", color:"Powertrain", xLabel:"MSRP", yLabel:"\u4f30\u7b97TCO" };
      default: return { x:"x", y:"y", z:"z", color:"", xLabel:"X", yLabel:"Y" };
    }
  }

  const chartOpts = ADV_CHARTS[advGroup] ?? [];
  const selectedAdvGroupLabel = ADV_GROUPS.find((group) => group.v === advGroup)?.l ?? "";
  const selectedAdvChartLabel = chartOpts.find((chart) => chart.v === advChart)?.l ?? "";
  const timeAxisModeValue = activeTab === "year" ? "YEAR" : "MONTH";
  const timeAxisModeDetail = activeTab === "month"
    ? `聚合口径：${monthGrain === "month" ? "月" : monthGrain === "quarter" ? "季" : "年"}`
    : "聚焦年度对比区间";
  const timeWindowStateValue = timeRange ? "CUSTOM" : "FULL";
  const timeSeriesDeckState = isGrouped
    ? (groupedLoading ? "SYNCING" : filteredGrouped.length > 0 ? "READY" : "IDLE")
    : (loading ? "SYNCING" : aggregatedSingle.length > 0 ? "READY" : "IDLE");
  const timeSeriesDeckVolume = isGrouped
    ? `${visibleSeries.length} / ${allSeriesNames.length || 0}`
    : String(aggregatedSingle.length);
  const timeSeriesExportSeriesNames = useMemo(
    () => isGrouped ? visibleSeries : ["Sales"],
    [isGrouped, visibleSeries],
  );
  const advancedExportSeriesNames = useMemo(() => {
    if (advChart === "seasonality_heatmap") return [] as string[];
    if (advChart === "price_migration") return migrationYears;
    if (advChart === "nev_range_distribution") return nevStackSeries;
    if (STACKED_CHARTS.has(advChart)) return uniqueNonEmptyStrings(stackKeys);
    if (SCATTER_CHARTS.has(advChart)) {
      const colorKey = getAdvancedScatterColorKey(advChart);
      return colorKey ? uniqueNonEmptyStrings(advItems.map((item) => item[colorKey])) : [];
    }
    if (isSimpleBar) return uniqueNonEmptyStrings(advItems.map((item) => item.label));
    return [];
  }, [advChart, advItems, isSimpleBar, migrationYears, nevStackSeries, stackKeys]);
  const modelVersionExportSeriesNames = useMemo(
    () => uniqueNonEmptyStrings(mvItems.map((item) => item[mvColorBy])),
    [mvColorBy, mvItems],
  );
  const positioningExportSeriesNames = useMemo(() => {
    const names = uniqueNonEmptyStrings(
      pmItems.map((item) => `Cluster ${item.cluster}`),
    );
    if (pmTarget) names.push("目标车型");
    return names;
  }, [pmItems, pmTarget]);
  const advancedDeckState = advChart === "rv_finance_dashboard"
    ? "EMBEDDED"
    : advLoading ? "LOADING" : advItems.length > 0 ? "READY" : "IDLE";
  const advancedDeckVolume = advChart === "rv_finance_dashboard"
    ? "Inline dashboard"
    : `${advItems.length} rows`;

  return (
    <div className="dashboard-layout">
      <CollapsibleFilterSidebar
        collapsed={sidebarCollapsed}
        onToggle={() => setSidebarCollapsed((current) => !current)}
        kicker="01 / Filter Stack"
        title="全维度筛选"
        summary={activeFilterSummaryText}
        expandedLabel="展开筛选面板"
        collapsedLabel="收起筛选面板"
        expandedTitle="Expand filters"
        collapsedTitle="Collapse filters"
      >
          <div className="filter-sidebar-header">
            <div className="filter-sidebar-hint">当前筛选会同步到 URL，也可直接带到 Specification Page。</div>
          </div>
          <div className="filter-card filter-summary-card">
            {sidebarSummaryItems.map((item) => (
              <div key={item.key} className="kpi-card">
                <div className="kpi-label">{item.label}</div>
                <div className={`kpi-value${sidebarSummaryDensityClass}`} title={item.value}>{item.value}</div>
              </div>
            ))}
          </div>
          <div className="dashboard-sidebar-caption">{activeFilterSummaryText}</div>

          <div className="dashboard-sidebar-toolbar">
            <button className="btn btn-sm btn-secondary" onClick={resetFilters}>{"\u91cd\u7f6e\u7b5b\u9009"}</button>
            <Link className="btn btn-sm btn-primary" to={specificationHref}>{"\u89c4\u683c\u9875"}</Link>
          </div>

          {FILTER_ORDER.map(({ key, label }) => (
            <SearchSelectFilter
              key={key}
              label={label}
              options={optionsMap[key] ?? []}
              selected={selections[key]}
              onChange={(values) => void onFilterChange(key, values)}
              showSuvShortcut={key === "segment"}
              shortcuts={key === "origin" ? [{ label: "中国品牌 / 中系车", values: ["中系", "中系2", "中国", "China", "Chinese", "CN"] }] : []}
            />
          ))}
      </CollapsibleFilterSidebar>

      <section className="dashboard-main">
        <PageBannerStack
          items={[
            ...(combinedError ? [{ id: "dashboard-error", tone: "error" as const, title: "Dashboard 加载失败", message: combinedError }] : []),
          ]}
        />

        <CollapsibleDeckHero
          collapsed={heroCollapsed}
          onToggle={() => setHeroCollapsed((current) => !current)}
          expandedLabel="展开概览面板"
          collapsedLabel="收起概览面板"
          expandedTitle="Expand overview"
          collapsedTitle="Collapse overview"
          head={(
            <>
              <div className="dashboard-hero-copy">
                <span className="page-kicker">01 / Market Overview</span>
                <h1>Dashboard Control View</h1>
                <div className="dashboard-hero-inline-summary">
                  <span className="selection-ribbon-label">Active lens</span>
                  <span className="selection-ribbon-value">{activeFilterSummaryText}</span>
                </div>
              </div>

              <div className="dashboard-hero-actions">
                <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                  <span className="hero-meta-label">Total sales</span>
                  <strong className={`hero-meta-value hero-meta-animated-value${getMetricDensityClass(heroTotalSalesText)}`} title={heroTotalSalesText}>{heroTotalSalesText}</strong>
                  <span className="hero-meta-subvalue">{timeWindowLabel}</span>
                  {loading && <span className="hero-meta-loader">LOADING LIVE SCOPE</span>}
                </div>
                <div className={`hero-meta-block hero-meta-block-immersive${loading ? " is-loading" : ""}`}>
                  <span className="hero-meta-label">Version count</span>
                  <strong className={`hero-meta-value hero-meta-animated-value${getMetricDensityClass(heroVersionCountText)}`} title={heroVersionCountText}>{heroVersionCountText}</strong>
                  <span className="hero-meta-subvalue">
                    {dashboardBootstrapping
                      ? "Bootstrapping default lens"
                      : activeFilterCount
                      ? `${activeFilterCount} filter dimensions active`
                      : "Default powertrain lens"}
                  </span>
                  {loading && <span className="hero-meta-loader">SYNCING FILTER STATE</span>}
                </div>
              </div>
            </>
          )}
          body={(
            <div className="dashboard-hero-rail">
              <div className="dashboard-hero-chip-row">
                {activeLensTokens.map((token, index) => (
                  <span key={`${index}-${token}`} className="dashboard-hero-chip">{token}</span>
                ))}
              </div>
              {freshnessItems.length > 0 && (() => {
                /* group countries by latestMonth, sort months chronologically descending */
                const byMonth = new Map<string, string[]>();
                for (const item of freshnessItems) {
                  const list = byMonth.get(item.latestMonth) ?? [];
                  list.push(item.country);
                  byMonth.set(item.latestMonth, list);
                }
                const sortedMonths = [...byMonth.keys()].sort((a, b) => {
                  const oa = toTimeOrdinal(a);
                  const ob = toTimeOrdinal(b);
                  if (oa != null && ob != null) return ob - oa;
                  return a < b ? 1 : -1;
                });
                return (
                  <div className="dashboard-hero-freshness-table">
                    <span className="dashboard-hero-freshness-label">数据覆盖</span>
                    <div className="freshness-month-groups">
                      {sortedMonths.map((month) => (
                        <div key={month} className="freshness-month-col">
                          <div className="freshness-month-header">{month}</div>
                          <div className="freshness-country-list">
                            {(byMonth.get(month) ?? []).map((c) => (
                              <span key={c} className="freshness-country-tag">{c}</span>
                            ))}
                          </div>
                        </div>
                      ))}
                    </div>
                  </div>
                );
              })()}
            </div>
          )}
        />

        {/* ── Global Time Axis ────────────────────────── */}
        <div className="card analysis-deck-card dashboard-time-axis-card dashboard-deck-card--compact-hero">
          <div className="dashboard-hero-head dashboard-deck-hero-head">
            <div className="dashboard-hero-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">02 / Global Time Axis</span>
              <h3>Global Time Axis</h3>
              <p>统一年度与月度时间窗，控制趋势对比与后续高级分析的观察区间。</p>
            </div>
            <div className="dashboard-hero-actions dashboard-deck-hero-actions dashboard-deck-hero-actions--pair">
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Axis Mode</span>
                <strong className="hero-meta-value">{timeAxisModeValue}</strong>
                <span className="hero-meta-subvalue">{timeAxisModeDetail}</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Window State</span>
                <strong className="hero-meta-value">{timeWindowStateValue}</strong>
                <span className="hero-meta-subvalue">{timeWindowLabel}</span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
            <TimeAxis
              labels={timeLabels}
              value={timeRange}
              onChange={setTimeRange}
              grain={activeTab}
              onGrainChange={setActiveTab}
              showTitle={false}
              monthGrain={monthGrain}
              onMonthGrainChange={setMonthGrain}
            />
          </div>
        </div>

        {/* ── Time series ─────────────────────────────── */}
        <div
          className="card analysis-deck-card chart-section dashboard-time-series-card dashboard-deck-card--compact-hero"
          style={getDeckLayoutStyle(timeSeriesDeckLayout)}
        >
          <div className="dashboard-hero-head dashboard-deck-hero-head">
            <div className="dashboard-hero-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">03 / Time-Series Lens</span>
              <h3>Sales Time Series</h3>
              <p>在同一筛选边界下切换年度、月度与分组序列，保持趋势分析和图例交互语义一致。</p>
            </div>
            <div className="dashboard-hero-actions dashboard-deck-hero-actions dashboard-deck-hero-actions--quad">
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">View Mode</span>
                <strong className="hero-meta-value">{activeTab === "year" ? "YEAR" : "MONTH"}</strong>
                <span className="hero-meta-subvalue">{activeTab === "month" ? `聚合：${monthGrain}` : "年度对比"}</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Series Mode</span>
                <strong className="hero-meta-value">{isGrouped ? "GROUPED" : "TOTAL"}</strong>
                <span className="hero-meta-subvalue">{isGrouped ? `${tsGroupDim}${isShareGrouped && tsShareSplit !== "total" ? ` · ${TIME_SERIES_SHARE_SPLIT_OPTIONS.find(option => option.value === tsShareSplit)?.label ?? tsShareSplit}` : ""}` : "单序列汇总视图"}</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Data State</span>
                <strong className="hero-meta-value">{timeSeriesDeckState}</strong>
                <span className="hero-meta-subvalue">{isGrouped ? `${filteredGrouped.length} 个分组点` : `${aggregatedSingle.length} 个时间点`}</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Visible Series</span>
                <strong className="hero-meta-value">{isGrouped ? timeSeriesDeckVolume : "1"}</strong>
                <span className="hero-meta-subvalue">{isGrouped ? `可见 ${visibleSeries.length} / ${allSeriesNames.length}` : "单序列展示"}</span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
          <div className="chart-header">
            <div className="tab-bar">
              <button className={"tab-btn"+(activeTab==="year"?" active":"")} onClick={()=>{releaseChartRuntime(); setActiveTab("year");}}>{"\u5e74\u5ea6\u5bf9\u6bd4"}</button>
              <button className={"tab-btn"+(activeTab==="month"?" active":"")} onClick={()=>{releaseChartRuntime(); setActiveTab("month");}}>{"\u6708\u5ea6\u660e\u7ec6"}</button>
            </div>
            <div className="chart-controls">
              <div className="tab-bar">
                <button className={"tab-btn"+(tsMode==="\u603b\u548c"?" active":"")} onClick={()=>selectTsMode("\u603b\u548c")}>{"\u603b\u548c"}</button>
                <button className={"tab-btn"+(tsMode==="\u5206\u7ec4"?" active":"")} onClick={()=>selectTsMode("\u5206\u7ec4")}>{"\u5206\u7ec4"}</button>
              </div>
              <span className="chart-controls-sep" />
              <label className="chart-mode-label"><input type="radio" name="chartType" value="line" checked={chartType==="line"} onChange={()=>selectChartType("line")} />{" \u6298\u7ebf"}</label>
              <label className="chart-mode-label"><input type="radio" name="chartType" value="bar" checked={chartType==="bar"} onChange={()=>selectChartType("bar")} />{" \u7d2f\u79ef\u67f1\u72b6"}</label>
              <label className="chart-mode-label"><input type="radio" name="chartType" value="rank" checked={chartType==="rank"} onChange={()=>selectChartType("rank")} />{" \u6392\u540d"}</label>
            </div>
          </div>

          {groupedLoading && (
            <LoadingSurface mode="inline" label="正在刷新分组序列" detail={tsGroupDim} />
          )}

          {/* series pills (click to toggle visibility) */}
          {isGrouped && allSeriesNames.length > 0 && (() => {
            const isPt = tsGroupDim === "\u52a8\u603b\u89c4\u6574";
            return (
            <div className="ts-series-pills">
              {allSeriesNames.map((name, i) => (
                <button key={name} className={"ts-pill"+(hiddenSeries.has(name)?" ts-pill-hidden":"")}
                  style={{
                    "--pill-color": resolveTimeSeriesSeriesColor(
                      name,
                      i,
                      tsPalette,
                      isPt,
                      tsExport.seriesColors,
                      filterFocusedPowertrain,
                      allSeriesNames.length,
                    ),
                  } as React.CSSProperties}
                  onClick={()=>setHiddenSeries(prev=>{const n=new Set(prev);n.has(name)?n.delete(name):n.add(name);return n;})}>
                  <span className="ts-pill-dot" />{name}
                </button>
              ))}
              <span className="ts-series-count">{visibleSeries.length+" / "+allSeriesNames.length+" \u7cfb\u5217"}</span>
            </div>
            );
          })()}

          {!chartRuntimeReady && (
            <DeferredDashboardChartPlaceholder onActivate={releaseDashboardChartWork} />
          )}

          {/* single-series */}
          {chartRuntimeReady && !isGrouped && chartType !== "rank" && aggregatedSingle.length > 0 && (
            <div ref={el => { tsChartRef.current = el; }}>
              <PlotlyChart
                data={(() => {
                  const salesColor = resolveTimeSeriesSeriesColor(
                    "Sales",
                    0,
                    tsPalette,
                    false,
                    tsExport.seriesColors,
                    filterFocusedPowertrain,
                    1,
                  );
                  const trace = chartType === "line" ? {
                    x: aggregatedSingle.map(s => s.time),
                    y: aggregatedSingle.map(s => s.value),
                    type: "scatter", mode: "lines+markers", name: "Sales",
                    line: { color: salesColor, width: 2 },
                    marker: { size: 5, color: salesColor },
                  } as Data : {
                    x: aggregatedSingle.map(s => s.time),
                    y: aggregatedSingle.map(s => s.value),
                    type: "bar", name: "Sales",
                    marker: { color: salesColor },
                  } as Data;
                  return applyDataLabelsToTraces([trace], tsExport);
                })()}
                layout={applyExportToLayout({
                  xaxis: buildCategoryAxis(singleTimeLabels, { tickangle: -45 }),
                  yaxis: { title: { text: "Sales" } },
                }, tsExport)}
                height={timeSeriesDeckLayout.height}
              />
            </div>
          )}

          {/* multi-series grouped */}
          {chartRuntimeReady && isGrouped && chartType !== "rank" && filteredGrouped.length > 0 && (() => {
            const isPt = tsGroupDim === "\u52a8\u603b\u89c4\u6574";
            let traces: Data[] = visibleSeries.map((name) => {
              const seriesData = filteredGrouped.filter(g => g.series === name);
              const seriesIndex = allSeriesNames.indexOf(name);
              const c = resolveTimeSeriesSeriesColor(
                name,
                seriesIndex,
                tsPalette,
                isPt,
                tsExport.seriesColors,
                filterFocusedPowertrain,
                allSeriesNames.length,
              );
              const base: Partial<Data> = {
                x: seriesData.map(d => d.time),
                y: seriesData.map(d => d.value),
                name,
              };
              if (chartType === "line") {
                return { ...base, type: "scatter", mode: "lines+markers", line: { color: c, width: 2 }, marker: { size: 4 } } as Data;
              }
              return { ...base, type: "bar", marker: { color: c } } as Data;
            });
            traces = applyDataLabelsToTraces(traces, tsExport);
            return (
              <div ref={el => { tsChartRef.current = el; }}>
                <PlotlyChart
                  data={traces}
                  layout={applyExportToLayout({
                      barmode: chartType === "bar" ? (isShareGrouped ? "group" : "relative") : undefined,
                    xaxis: buildCategoryAxis(groupedTimeLabels, { tickangle: -45 }),
                    yaxis: isShareGrouped
                      ? { title: { text: "Share (%)" }, range: [0, 100], ticksuffix: "%" }
                      : { title: { text: "Sales" } },
                  }, tsExport)}
                  height={timeSeriesDeckLayout.height}
                />
              </div>
            );
          })()}

          {chartRuntimeReady && !isGrouped && chartType !== "rank" && aggregatedSingle.length===0 && !loading && <div className="chart-empty">{"\u6682\u65e0\u8d8b\u52bf\u6570\u636e"}</div>}
          {chartRuntimeReady && isGrouped && chartType !== "rank" && filteredGrouped.length===0 && !groupedLoading && <div className="chart-empty">{"\u5207\u6362\u5206\u7ec4\u7ef4\u5ea6\u6216\u8c03\u6574\u7b5b\u9009\u6761\u4ef6"}</div>}

          {/* ranking horizontal bar chart */}
          {chartRuntimeReady && chartType === "rank" && isGrouped && rankingSliced.length > 0 && (() => {
            const reversed = [...rankingSliced].reverse();
            const maxVol = rankingSliced[0]?.volume ?? 1;
            const chartHeight = Math.max(timeSeriesDeckLayout.height, Math.min(1200, rankingSliced.length * 26 + 50));
            const trace: Data = {
              type: "bar",
              orientation: "h",
              name: tsGroupDim,
              showlegend: false,
              x: reversed.map((item) => item.volume),
              y: reversed.map((item) => item.name),
              marker: {
                color: reversed.map((item) => {
                  const idx = allSeriesNames.indexOf(item.name);
                  return resolveTimeSeriesSeriesColor(
                    item.name,
                    idx,
                    tsPalette,
                    tsGroupDim === "\u52a8\u603b\u89c4\u6574",
                    tsExport.seriesColors,
                    filterFocusedPowertrain,
                    allSeriesNames.length,
                  );
                }),
              },
              hovertemplate: "%{y}<br>\u9500\u91cf %{x:,.0f} \u53f0<br>\u5360\u6bd4 %{customdata:.1%}<extra></extra>",
              customdata: reversed.map((item) => item.share),
            };
            const labelTrace: Data = {
              type: "scatter",
              mode: "text",
              name: "Labels",
              x: reversed.map((item) => item.volume + maxVol * 0.03),
              y: reversed.map((item) => item.name),
              text: reversed.map((item) => formatCompactBarLabel(item.volume, item.share)),
              textposition: "middle right",
              textfont: { size: tsExport.labelFontSize ?? tsExport.fontSize, color: "#334155" },
              cliponaxis: false,
              hoverinfo: "skip",
              showlegend: false,
            };
            const shareValues = trace.customdata as number[];
            const processedBar = applyDataLabelsToTraces([trace], tsExport)[0];
            if (shareValues && tsExport.dataLabelMode !== "off") {
              (processedBar as Record<string, unknown>).customdata = shareValues;
            }
            const hasBarLabels = tsExport.dataLabelMode !== "off";
            const traces = hasBarLabels ? [processedBar] : [processedBar, labelTrace];
            return (
              <div className="ts-ranking-chart-shell" ref={el => { tsChartRef.current = el; }}>
                <PlotlyChart
                  data={traces}
                  layout={applyExportToLayout({
                    barmode: "relative",
                    bargap: 0.15,
                    margin: { r: 180, t: 24, b: 30 },
                    xaxis: { title: { text: "Sales" }, automargin: true, fixedrange: true, showgrid: false, rangemode: "tozero" as const },
                    yaxis: { automargin: true, fixedrange: true, showgrid: false, ticks: "" as const },
                  }, tsExport)}
                  height={chartHeight}
                />
              </div>
            );
          })()}

          {chartRuntimeReady && chartType === "rank" && isGrouped && rankingData.length === 0 && !groupedLoading && (
            <div className="chart-empty">{"\u5207\u6362\u5206\u7ec4\u7ef4\u5ea6\u6216\u8c03\u6574\u7b5b\u9009\u6761\u4ef6"}</div>
          )}

          {!isGrouped && chartType === "rank" && (
            <div className="ts-mode-hint">\u5207\u6362\u5230\u300c\u5206\u7ec4\u300d\u6a21\u5f0f\u540e\u53ef\u67e5\u770b\u6392\u540d\u6761\u5f62\u56fe\u3002</div>
          )}

          {/* B11: "其他"明细表 — 展示被合并进"其他"的各分组明细 */}
          {isGrouped && tsIncludeOthers && othersDetail.length > 0 && (
            <details className="analysis-disclosure">
              <summary>
                {`“其他”包含 ${othersDetail.length} 个分组`}
              </summary>
              <div className="analysis-table-wrap">
                <table className="data-table">
                  <thead><tr><th>{"\u540d\u79f0"}</th><th>{"\u9500\u91cf"}</th><th>{"\u5360\u6bd4"}</th></tr></thead>
                  <tbody>{othersDetail.map(d=>(
                    <tr key={d.name}>
                      <td>{d.name}</td>
                      <td>{d.sales.toLocaleString()}</td>
                      <td>{(d.share*100).toFixed(1)}%</td>
                    </tr>
                  ))}</tbody>
                </table>
              </div>
            </details>
          )}

          </div>
        </div>

        {heavyQueriesReady ? (
          <>
        {/* ── Advanced analysis ───────────────────────── */}
        <div
          className="card analysis-deck-card dashboard-advanced-card dashboard-deck-card--compact-hero"
          style={getDeckLayoutStyle(advancedDeckLayout)}
        >
          <div className="dashboard-hero-head dashboard-deck-hero-head">
            <div className="dashboard-hero-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">04 / Advanced Analysis</span>
              <h3>Advanced Control Deck</h3>
              <p>在同一筛选与时间窗口下切换分析域、图层和参数，承接主看板的深度分析与嵌入式 RV Finance 视图。</p>
            </div>
            <div className="dashboard-hero-actions dashboard-deck-hero-actions dashboard-deck-hero-actions--quad">
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Analysis Domain</span>
                <strong className="hero-meta-value">{selectedAdvGroupLabel || "-"}</strong>
                <span className="hero-meta-subvalue">当前分析域</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Chart Layer</span>
                <strong className="hero-meta-value">{selectedAdvChartLabel || "-"}</strong>
                <span className="hero-meta-subvalue">当前图层</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Data State</span>
                <strong className="hero-meta-value">{advancedDeckState}</strong>
                <span className="hero-meta-subvalue">{advancedDeckVolume}</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Active Filters</span>
                <strong className="hero-meta-value">{String(activeFilters.length)}</strong>
                <span className="hero-meta-subvalue">
                  {dashboardBootstrapping
                    ? "Bootstrapping default lens"
                    : activeFilters.length
                    ? activeFilterSummaryText
                    : "Default powertrain lens"}
                </span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
          {/* group button row */}
          <div className="adv-console">
            <div className="adv-console-row">
              <div className="adv-console-kicker">分析域</div>
              <div className="adv-console-buttons">
                {ADV_GROUPS.map(o=>(
                  <button key={o.v} type="button" className={"adv-console-btn"+(advGroup===o.v?" is-active":"")}
                    onClick={()=>{setAdvGroup(o.v); setAdvChart((ADV_CHARTS[o.v]??[])[0]?.v??""); setAdvItems([]); setAdvMeta(null);}}>{o.l}</button>
                ))}
              </div>
            </div>
            <div className="adv-console-row">
              <div className="adv-console-kicker">分析图层</div>
              <div className="adv-console-buttons adv-console-buttons--wrap">
                {chartOpts.map(o=>(
                  <button key={o.v} type="button" className={"adv-console-btn"+(advChart===o.v?" is-active":"")}
                    onClick={()=>{setAdvChart(o.v); setAdvItems([]); setAdvMeta(null);}}>{o.l}</button>
                ))}
              </div>
            </div>
          </div>
          <div className="adv-controls adv-controls-panel">
            {advChart !== "rv_finance_dashboard" && (<>
            {advChart !== "nev_range_distribution" && (
              <div className="filter-group"><label>Top N</label>
                <input type="number" value={advTopN} min={5} max={200} style={{width:60}} onChange={e=>setAdvTopN(Number(e.target.value)||30)} />
              </div>
            )}
            {(STACKED_CHARTS.has(advChart) || advChart==="price_migration" || advChart==="powertrain_vs_price") && (
              <div className="filter-group"><label>{"\u5e26\u5bbd"}</label>
                <input type="number" value={advBandSize} min={50} max={50000} step={100} style={{width:80}} onChange={e=>setAdvBandSize(Number(e.target.value)||1000)} />
              </div>
            )}
            {advChart==="price_migration" && (
              <div className="filter-group"><label>{"\u7c7b\u578b"}</label>
                <select value={advMigrationMode} onChange={e=>setAdvMigrationMode(e.target.value as "area"|"line")}>
                  <option value="area">{"\u9762\u79ef\u56fe"}</option>
                  <option value="line">{"\u6298\u7ebf\u56fe"}</option>
                </select>
              </div>
            )}
            {SCATTER_CHARTS.has(advChart) && (
              <div className="filter-group"><label>{"\u6c14\u6ce1\u500d\u7387"}</label>
                <select value={advBubbleScale} onChange={e=>setAdvBubbleScale(Number(e.target.value))}>
                  <option value={1}>{"\u00d71"}</option>
                  <option value={2}>{"\u00d72"}</option>
                  <option value={3}>{"\u00d73"}</option>
                  <option value={4}>{"\u00d74"}</option>
                </select>
              </div>
            )}
            {advChart==="powertrain_bubble" && (
              <div className="adv-bubble-deck">
                <div className="adv-bubble-main">
                  <div className="filter-group adv-control-unit"><label>粒度</label>
                    <select
                      value={advBubbleGrain}
                      onChange={(e) => {
                        const nextGrain = e.target.value as "model" | "version";
                        setAdvBubbleGrain(nextGrain);
                        setAdvBubbleLabelDimension(nextGrain);
                        setAdvItems([]);
                        setAdvMeta(null);
                      }}
                    >
                      <option value="model">Model</option>
                      <option value="version">Version</option>
                    </select>
                  </div>
                  <label className="adv-toggle-chip">
                    <input type="checkbox" checked={advBubbleFacet} onChange={e=>setAdvBubbleFacet(e.target.checked)} />
                    <span>按品牌分面</span>
                  </label>
                  {advBubbleFacet && (
                    <div className="filter-group adv-control-unit"><label>最多品牌数</label>
                      <input type="number" min={2} max={12} value={advBubbleFacetMax}
                        onChange={e=>setAdvBubbleFacetMax(Number(e.target.value)||4)} />
                    </div>
                  )}
                </div>
                <details className="adv-disclosure">
                  <summary>7a 高级设置</summary>
                  <div className="adv-bubble-advanced">
                    <div className="adv-inline-strip">
                      <label className="adv-toggle-chip">
                        <input type="checkbox" checked={advBubbleShowYoy}
                          onChange={e=>setAdvBubbleShowYoy(e.target.checked)} />
                        <span>hover 显示 YoY</span>
                      </label>
                      {advBubbleShowYoy && advBubbleYearOptions.length >= 2 && (
                        <div className="filter-group adv-control-unit"><label>YoY 年份</label>
                          <select value={advBubbleYoyYear} onChange={e=>setAdvBubbleYoyYear(e.target.value)}>
                            {advBubbleYearOptions.slice(1).map((year) => <option key={year} value={year}>{year}</option>)}
                          </select>
                        </div>
                      )}
                      {advBubbleShowYoy && advBubbleYearOptions.length < 2 && (
                        <div className="adv-state-note">当前年度列不足两年，无法显示 YoY。</div>
                      )}
                    </div>

                    <div className="adv-inline-strip">
                      <label className="adv-toggle-chip">
                        <input type="checkbox" checked={advBubbleGroupTopN}
                          onChange={e=>setAdvBubbleGroupTopN(e.target.checked)} />
                        <span>启用分组 TopN</span>
                      </label>
                      {advBubbleGroupTopN && (
                        <div className="filter-group adv-control-unit"><label>分组维度</label>
                          <select value={advBubbleGroupDimension} onChange={e=>setAdvBubbleGroupDimension(e.target.value as BubbleGroupDimension)}>
                            {BUBBLE_GROUP_DIMENSIONS.map((option) => <option key={option.v} value={option.v}>{option.l}</option>)}
                          </select>
                        </div>
                      )}
                    </div>

                    {advBubbleGroupTopN && advBubbleGroupOptions.length > 0 && (
                      <>
                        <div className="adv-chip-grid">
                          {advBubbleGroupOptions.map((value) => {
                            const active = advBubbleGroupValues.includes(value);
                            return (
                              <button
                                key={value}
                                type="button"
                                className={"adv-chip"+(active?" is-active":"")}
                                onClick={() => toggleAdvBubbleGroupValue(value)}
                              >
                                {value}
                              </button>
                            );
                          })}
                        </div>
                        {advBubbleGroupValues.length > 0 ? (
                          <div className="adv-topn-grid">
                            {advBubbleGroupValues.map((value) => (
                              <div key={value} className="filter-group adv-control-unit"><label>{`${value} TopN`}</label>
                                <input
                                  type="number"
                                  min={1}
                                  max={300}
                                  value={advBubbleGroupTopNMap[value] ?? advTopN}
                                  onChange={e=>setAdvBubbleGroupTopNMap((current) => ({
                                    ...current,
                                    [value]: Math.max(1, Math.min(300, Number(e.target.value) || advTopN)),
                                  }))}
                                />
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div className="adv-state-note">至少选择一个分组后，才能为每组设置独立 TopN。</div>
                        )}
                      </>
                    )}
                  </div>
                </details>
              </div>
            )}
            {/* NEV动总筛选 */}
            {usesNevPowertrainFilter(advChart) && (
              <div className="filter-group adv-control-unit adv-control-unit--wide"><label>{"\u52a8\u603b\u7c7b\u578b"}</label>
                <div className="adv-powertrain-strip">
                  {["BEV","PHEV","HEV","MHEV","ICE"].map((pt, index)=>(
                    <label key={pt} className={"adv-powertrain-chip"+(advPowertrains.includes(pt)?" is-active":"")}>
                      <input type="checkbox" checked={advPowertrains.includes(pt)}
                        onChange={e=>{const next=e.target.checked?[...advPowertrains,pt]:advPowertrains.filter(x=>x!==pt);setAdvPowertrains(next);}} />
                      <span className="adv-powertrain-chip-swatch" style={{"--pt-color": fuelFamilyColor(pt, 0, 1)} as React.CSSProperties} />
                      <span>{pt}</span>
                    </label>
                  ))}
                </div>
              </div>
            )}
            {advChart==="nev_range_distribution" && (
              <>
                <label className="chart-mode-label" style={{gap:6}}>
                  <input type="checkbox" checked={advNevTopNEnabled} onChange={e=>setAdvNevTopNEnabled(e.target.checked)} />
                  {"\u542f\u7528 TopN"}
                </label>
                {advNevTopNEnabled && <div className="filter-group"><label>Top N</label>
                  <input type="number" value={advTopN} min={10} max={300} step={5} style={{width:72}} onChange={e=>setAdvTopN(Number(e.target.value)||80)} />
                </div>}
                <div className="filter-group"><label>{"\u7eed\u822a\u8f74\u4e0a\u9650"}</label>
                  <input type="number" value={advNevAxisMax} min={200} max={1500} step={50} style={{width:72}} onChange={e=>setAdvNevAxisMax(Number(e.target.value)||1000)} />
                </div>
                <div className="filter-group"><label>{"\u7eed\u822a\u67e5\u8be2(km)"}</label>
                  <input type="number" value={advNevRangeQuery} min={0} max={2000} step={10} style={{width:84}} onChange={e=>setAdvNevRangeQuery(e.target.value)} />
                </div>
                <div className="filter-group"><label>{"\u5206\u5e03\u53e3\u5f84"}</label>
                  <select value={advNevMetricMode} onChange={e=>setAdvNevMetricMode(e.target.value as "window_sales"|"net_change")}>
                    <option value="window_sales">{"\u5f53\u524d\u65f6\u95f4\u7a97\u9500\u91cf"}</option>
                    <option value="net_change">{"\u51c0\u53d8\u5316\uff08\u672b\u5e74-\u9996\u5e74\uff09"}</option>
                  </select>
                </div>
              </>
            )}
            {advChart==="nev_range_distribution" && (
              <div className="filter-group"><label>{"\u7eed\u822a\u6b65\u957f(km)"}</label>
                <input type="number" value={advRangeStep} min={10} max={200} step={10} style={{width:60}} onChange={e=>setAdvRangeStep(Number(e.target.value)||50)} />
              </div>
            )}
            {/* NEV参数重置 */}
            {usesNevPowertrainFilter(advChart) && (
              <button className="btn btn-sm btn-secondary" onClick={()=>{setAdvPowertrains(["BEV","PHEV"]);setAdvTopN(advChart==="nev_range_distribution"?80:120);setAdvRangeStep(50);setAdvNevTopNEnabled(true);setAdvNevAxisMax(1000);setAdvNevMetricMode("window_sales");setAdvNevStackByModel(false);setAdvNevFacetBrand(false);setAdvNevMaxBrandFacets(4);setAdvNevRangeQuery("");}}>{"\u91cd\u7f6e\u53c2\u6570"}</button>
            )}
            {advChart==="nev_range_distribution" && (
              <details className="adv-disclosure adv-disclosure--panel">
                <summary>NEV 高级设置</summary>
                <div className="adv-bubble-advanced">
                  <div className="adv-inline-strip">
                  <label className="chart-mode-label" style={{gap:6}}>
                    <input type="checkbox" checked={advNevStackByModel} onChange={e=>setAdvNevStackByModel(e.target.checked)} />
                    {"\u6309 Model \u5806\u53e0"}
                  </label>
                  <label className="chart-mode-label" style={{gap:6}}>
                    <input type="checkbox" checked={advNevFacetBrand} onChange={e=>setAdvNevFacetBrand(e.target.checked)} />
                    {"\u6309\u54c1\u724c\u5206\u9762"}
                  </label>
                  {advNevFacetBrand && <div className="filter-group adv-control-unit"><label>{"\u6700\u591a\u54c1\u724c\u6570"}</label>
                    <input type="number" value={advNevMaxBrandFacets} min={2} max={12} step={1} style={{width:60}} onChange={e=>setAdvNevMaxBrandFacets(Number(e.target.value)||4)} />
                  </div>}
                  </div>
                </div>
              </details>
            )}
            {/* 热力图色阶 */}
            {advChart==="seasonality_heatmap" && (
              <div className="filter-group"><label>{"\u8272\u9636"}</label>
                <select value={advHeatmapScale} onChange={e=>setAdvHeatmapScale(e.target.value)}>
                  {["Blues","Viridis","YlOrRd","RdBu","Greens","Hot"].map(s=><option key={s} value={s}>{s}</option>)}
                </select>
              </div>
            )}
            <LoadingActionButton loading={advLoading} loadingLabel="加载中…" disabled={!columns.length} onClick={()=>{ releaseHeavyQueries(); void loadAdvChart(); }}>加载图表</LoadingActionButton>
            </>)}
          </div>

          {/* TCO参数面板 */}
          {advChart==="estimated_tco" && (
            <div className="adv-controls adv-controls-panel adv-controls-panel-secondary">
              <div className="filter-group adv-control-unit adv-slider-unit"><label>{"\u4f7f\u7528\u5e74\u9650"}</label>
                <input type="range" min={1} max={10} step={1} value={tcoYears} onChange={e=>setTcoYears(Number(e.target.value))} />
                <span className="adv-slider-readout">{tcoYears}{"\u5e74"}</span>
              </div>
              <div className="filter-group adv-control-unit adv-slider-unit"><label>{"\u5e74\u91cc\u7a0b(km)"}</label>
                <input type="range" min={5000} max={50000} step={1000} value={tcoAnnualKm} onChange={e=>setTcoAnnualKm(Number(e.target.value))} />
                <span className="adv-slider-readout">{tcoAnnualKm.toLocaleString()}</span>
              </div>
              <div className="filter-group adv-control-unit adv-slider-unit"><label>{"\u6298\u65e7\u7387"}</label>
                <input type="range" min={0.1} max={0.9} step={0.05} value={tcoDepreciation} onChange={e=>setTcoDepreciation(Number(e.target.value))} />
                <span className="adv-slider-readout">{(tcoDepreciation*100).toFixed(0)}%</span>
              </div>
              <div className="filter-group adv-control-unit adv-slider-unit"><label>{"\u7ef4\u4fdd\u7387"}</label>
                <input type="range" min={0.005} max={0.05} step={0.002} value={tcoMaintenance} onChange={e=>setTcoMaintenance(Number(e.target.value))} />
                <span className="adv-slider-readout">{(tcoMaintenance*100).toFixed(1)}%</span>
              </div>
              <div className="filter-group adv-control-unit adv-slider-unit"><label>{"\u7a0e\u8d39\u4fdd\u9669"}</label>
                <input type="range" min={0.005} max={0.06} step={0.005} value={tcoTaxInsurance} onChange={e=>setTcoTaxInsurance(Number(e.target.value))} />
                <span className="adv-slider-readout">{(tcoTaxInsurance*100).toFixed(1)}%</span>
              </div>
              <div className="filter-group adv-control-unit adv-slider-unit"><label>{"\u80fd\u6e90\u6210\u672c\u57fa\u7840(\u20ac/km)"}</label>
                <input type="range" min={0.02} max={0.3} step={0.01} value={tcoEnergyCost} onChange={e=>setTcoEnergyCost(Number(e.target.value))} />
                <span className="adv-slider-readout">{tcoEnergyCost.toFixed(2)}</span>
              </div>
            </div>
          )}

          {advChart === "powertrain_bubble" && (bubbleWarnings.length > 0 || bubbleYoyEnabled || bubbleGroupTopNApplied) && (
            <div className="adv-bubble-status">
              {bubbleYoyEnabled && bubbleYoyBaseYear && bubbleYoyCompareYear && (
                <span>{`YoY ${bubbleYoyBaseYear} -> ${bubbleYoyCompareYear}`}</span>
              )}
              {bubbleGroupTopNApplied && bubbleGroupDimensionLabel && bubbleSelectedGroups.length > 0 && (
                <span>{`${bubbleGroupDimensionLabel} TopN: ${bubbleSelectedGroups.join(" / ")}`}</span>
              )}
              {bubbleWarnings.map((warning) => <span key={warning}>{warning}</span>)}
            </div>
          )}

          {/* simple bar chart */}
          {isSimpleBar && advItems.length > 0 && (
            <div className="bar-chart">
              {advItems.map((row, index)=>{
                const lb=String(row.label??"-"); const val=Number(row.value??0);
                const pct=Math.max(1,Math.round((val/maxBar)*100));
                const fillColor = resolveAdvancedSeriesColor(
                  lb,
                  index,
                  advItems.length,
                  advPalette,
                  false,
                  advExport.seriesColors,
                  advancedFocusedPowertrain,
                );
                return (<div className="bar-row" key={lb+"-"+val}>
                  <span className="bar-label">{lb}</span>
                  <div className="bar-track"><div className="bar-fill" style={{width:pct+"%", background: fillColor}} /></div>
                  <span className="bar-value">{val.toLocaleString()}</span>
                </div>);
              })}
            </div>
          )}

          {/* scatter / bubble */}
          {SCATTER_CHARTS.has(advChart) && advItems.length > 0 && (() => {
            const ax = scatterAxes();
            const isPtScatter = ax.color === "Powertrain";
            const cats = [...new Set(advItems.map(r=>String(r[ax.color]??"")))];
            const scatterBubbleSizing = buildBubbleSizing(
              advItems.map(r => Number(r[ax.z] ?? 0)),
              { maxDiameter: 24 * advBubbleScale, minDiameter: 4 },
            );
            const pointLabelText = (row: Record<string, string | number>): string => {
              if (advChart !== "powertrain_bubble") {
                return String(row.DisplayName ?? row.Version ?? row.Model ?? row.Brand ?? "");
              }
              if (advBubbleLabelDimension === "version") {
                return String(row.Version ?? row.DisplayName ?? row.Model ?? row.Brand ?? "");
              }
              return String(row.Model ?? row.DisplayName ?? row.Version ?? row.Brand ?? "");
            };

            function buildTraces(items: Record<string, string|number>[]) {
              const localCats = [...new Set(items.map(r=>String(r[ax.color]??"")))];
              return localCats.map((cat, i) => {
                const subset = items.filter(r=>String(r[ax.color]??"")=== cat);
                const isBubbleMsrp = advChart === "powertrain_bubble";
                const salesValues = subset.map(r => Math.max(0, Number(r[ax.z] ?? 0)));
                const bubbleYoyTemplate = isBubbleMsrp && bubbleYoyEnabled && bubbleYoyCompareYear && bubbleYoyBaseYear
                  ? `<br>${bubbleYoyBaseYear} Sales: %{customdata[4]:,.0f}<br>${bubbleYoyCompareYear} Sales: %{customdata[5]:,.0f}<br>YoY: %{customdata[6]:+.1f}%`
                  : "";
                return withExportLabels({
                  x: subset.map(r => Number(r[ax.x] ?? 0)),
                  y: subset.map(r => Number(r[ax.y] ?? 0)),
                  text: subset.map(pointLabelText),
                  customdata: subset.map(r => isBubbleMsrp
                    ? [
                        Number(r[ax.z] ?? 0),
                        Number(r.MsrpMin ?? r[ax.y] ?? 0),
                        Number(r.MsrpMax ?? r[ax.y] ?? 0),
                        Number(r.VariantCount ?? 1),
                        Number(r.SalesBase ?? r[ax.z] ?? 0),
                        Number(r.SalesCurrent ?? r[ax.z] ?? 0),
                        Number(r.YoYPct ?? 0),
                      ]
                    : [Number(r[ax.z] ?? 0)]),
                  type: "scatter",
                  mode: "markers",
                  name: cat,
                  marker: {
                    color: resolveAdvancedSeriesColor(
                      cat,
                      i,
                      localCats.length,
                      advPalette,
                      isPtScatter,
                      advExport.seriesColors,
                      advancedFocusedPowertrain,
                    ),
                    size: salesValues,
                    sizemode: scatterBubbleSizing.sizemode,
                    sizeref: scatterBubbleSizing.sizeref,
                    sizemin: scatterBubbleSizing.sizemin,
                    opacity: isBubbleMsrp ? buildSalesOpacityValues(salesValues) : 0.7,
                  },
                  hovertemplate: isBubbleMsrp
                    ? advBubbleGrain === "version"
                      ? "%{text}<br>" + ax.xLabel + ": %{x:,.0f}<br>MSRP（组内中位数）: %{y:,.0f}<br>MSRP范围: %{customdata[1]:,.0f} - %{customdata[2]:,.0f}<br>Sales: %{customdata[0]:,.0f}" + bubbleYoyTemplate + "<extra>%{fullData.name}</extra>"
                      : "%{text}<br>" + ax.xLabel + ": %{x:,.0f}<br>MSRP（组内中位数）: %{y:,.0f}<br>MSRP范围: %{customdata[1]:,.0f} - %{customdata[2]:,.0f}<br>聚合版型数: %{customdata[3]:,.0f}<br>Sales: %{customdata[0]:,.0f}" + bubbleYoyTemplate + "<extra>%{fullData.name}</extra>"
                    : "%{text}<br>" + ax.xLabel + ": %{x:,.0f}<br>" + ax.yLabel + ": %{y:,.0f}<br>Sales: %{customdata[0]:,.0f}<extra>%{fullData.name}</extra>",
                } as Data, {
                  ...(subset.some(r => pointLabelText(r).trim()) ? { model: subset.map(pointLabelText) } : {}),
                  ...(subset.some(r => r.Sales !== undefined) ? { sales: subset.map(r => Number(r.Sales ?? 0)) } : {}),
                  value: subset.map(r => Number(r[ax.y] ?? 0)),
                  series: subset.map(() => cat),
                }) as Data;
              });
            }

            function buildPowertrainBubbleLabels(items: Record<string, string | number>[]): Data[] {
              if (advChart !== "powertrain_bubble" || advExport.dataLabelMode === "off") return [];
              const labelMode = normalizeDashboardBubbleLabelMode(advExport.dataLabelOverlapStrategy);
              if (labelMode === "clean") return [];
              const candidates = items.flatMap((row): DashboardBubbleLabelInfo[] => {
                const text = pointLabelText(row).trim();
                const x = Number(row[ax.x] ?? 0);
                const y = Number(row[ax.y] ?? 0);
                const sales = Math.max(0, Number(row[ax.z] ?? 0));
                const series = String(row[ax.color] ?? "");
                if (!text || !series || !Number.isFinite(x) || !Number.isFinite(y)) return [];
                const key = `${series}|${text}|${x}|${y}`;
                return [{
                  key,
                  text,
                  x,
                  y,
                  sales,
                  series,
                  priority: 1,
                  showLabel: true,
                  jitterX: 0,
                  jitterY: 0,
                }];
              });
              if (candidates.length === 0) return [];
              const xValues = candidates.map((item) => item.x);
              const yValues = candidates.map((item) => item.y);
              const xRange = Math.max(...xValues) - Math.min(...xValues) || 1;
              const yRange = Math.max(...yValues) - Math.min(...yValues) || 1;
              const rankedKeys = [...candidates]
                .sort((left, right) => right.sales - left.sales)
                .map((item) => item.key);
              const highSalesCutoff = Math.max(1, Math.ceil(candidates.length * 0.08));
              const midSalesCutoff = Math.max(highSalesCutoff + 1, Math.ceil(candidates.length * 0.24));
              const longTailCutoff = Math.max(0, Math.floor(candidates.length * 0.2));
              const highSalesKeys = new Set(rankedKeys.slice(0, highSalesCutoff));
              const midSalesKeys = new Set(rankedKeys.slice(0, midSalesCutoff));
              const longTailKeys = new Set(rankedKeys.slice(candidates.length - longTailCutoff));
              const topBySeries = new Map<string, Set<string>>();
              Array.from(new Set(candidates.map((item) => item.series))).forEach((series) => {
                const seriesTop = candidates
                  .filter((item) => item.series === series)
                  .sort((left, right) => right.sales - left.sales)
                  .slice(0, 2)
                  .map((item) => item.key);
                topBySeries.set(series, new Set(seriesTop));
              });
              const labelInfos = candidates.map((candidate) => {
                const seriesTop = topBySeries.get(candidate.series);
                let priority = 1;
                if (highSalesKeys.has(candidate.key)) {
                  priority = 3;
                } else if (midSalesKeys.has(candidate.key) || seriesTop?.has(candidate.key)) {
                  priority = 2;
                } else if (longTailKeys.has(candidate.key)) {
                  priority = 0;
                }
                const showLabel =
                  labelMode === "all" ||
                  (labelMode === "smart_top" && priority >= 2) ||
                  (labelMode === "selected" && selectedAdvKeys.has(candidate.key));
                return {
                  ...candidate,
                  priority,
                  showLabel,
                  jitterX: jitterDashboardLabel(candidate.key, xRange * 0.008),
                  jitterY: jitterDashboardLabel(`${candidate.key}_y`, yRange * 0.01),
                };
              });
              return buildDashboardBubbleLabelTraces(
                labelInfos,
                labelMode,
                advExport.labelFontSize ?? advExport.fontSize,
              );
            }

            function applyAdvancedScatterData(sourceItems: Record<string, string | number>[], traces: Data[]): Data[] {
              if (advChart !== "powertrain_bubble") {
                return applySeriesColors(applyDataLabelsToTraces(traces, advExport), advExport.seriesColors);
              }
              return [
                ...applySeriesColors(traces, advExport.seriesColors),
                ...buildPowertrainBubbleLabels(sourceItems),
              ];
            }

            /* 7a: brand faceting */
            if (advChart === "powertrain_bubble" && advBubbleFacet) {
              /* group by brand, take top N brands by total Sales */
              const brandTotals = new Map<string, number>();
              for (const r of advItems) {
                const b = String(r.Brand ?? "");
                brandTotals.set(b, (brandTotals.get(b) ?? 0) + Number(r.Sales ?? 0));
              }
              const topBrands = [...brandTotals.entries()]
                .sort((a, b) => b[1] - a[1])
                .slice(0, advBubbleFacetMax)
                .map(e => e[0]);
              return (
                <div ref={el => { advChartRef.current = el; }} className="facet-plot-grid">
                  {topBrands.map(brand => {
                    const subset = advItems.filter(r => String(r.Brand ?? "") === brand);
                    const traces = buildTraces(subset);
                    return (
                      <div key={brand} className="facet-plot-card">
                        <div className="facet-plot-title">{brand}</div>
                        <PlotlyChart
                          data={applyAdvancedScatterData(subset, traces)}
                          layout={applyExportToLayout({
                                      xaxis: { title: { text: ax.xLabel } },
                            yaxis: { title: { text: ax.yLabel } },
                            showlegend: false,
                            margin: { t: 18, b: 40, l: 50, r: 10 },
                          }, advExport)}
                          height={advancedDeckLayout.height}
                        />
                      </div>
                    );
                  })}
                </div>
              );
            }

            const traces = buildTraces(advItems);
            return (
              <div ref={el => { advChartRef.current = el; }}>
                <PlotlyChart
                  key={`adv-scatter-${selectedAdvKeys.size}`}
                  data={applyAdvancedScatterData(advItems, traces)}
                  layout={applyExportToLayout({
                      xaxis: { title: { text: ax.xLabel } },
                    yaxis: { title: { text: ax.yLabel } },
                    ...(advChart === "length_vs_price" ? {
                      shapes: [
                        { type: "line", x0: 4550, x1: 4550, y0: 0, y1: 1, yref: "paper", line: { color: "#94a3b8", width: 1.5, dash: "dash" } },
                        { type: "line", x0: 4700, x1: 4700, y0: 0, y1: 1, yref: "paper", line: { color: "#64748b", width: 1.5, dash: "dash" } },
                      ],
                      annotations: [
                        { x: 4550, y: 1.02, yref: "paper", text: "C-SUV 4550mm", showarrow: false, font: { size: 10, color: "#94a3b8" } },
                        { x: 4700, y: 1.02, yref: "paper", text: "D-SUV 4700mm", showarrow: false, font: { size: 10, color: "#64748b" } },
                        /* §9b value detection: models ≥4700mm below median MSRP */
                        ...(() => {
                          const prices = advItems.map(r => Number(r.MSRP ?? 0)).filter(v => v > 0).sort((a, b) => a - b);
                          const med = prices[Math.floor(prices.length / 2)] ?? 0;
                          return advItems
                            .filter(r => Number(r.Length ?? 0) >= 4700 && Number(r.MSRP ?? 0) > 0 && Number(r.MSRP ?? 0) < med)
                            .slice(0, 5)
                            .map(r => ({
                              x: Number(r.Length), y: Number(r.MSRP),
                              text: String(r.Model ?? ""), showarrow: true, arrowhead: 2, arrowcolor: "#ef4444",
                              font: { size: 9, color: "#ef4444" }, ax: 30, ay: -20,
                            }));
                        })(),
                      ],
                    } : {}),
                  }, advExport)}
                  onClick={handleAdvClick}
                  height={advancedDeckLayout.height}
                />
              </div>
            );
          })()}

          {/* correlation coefficient for nev_capacity_vs_msrp */}
          {advChart==="nev_capacity_vs_msrp" && advItems.length > 2 && (() => {
            const xs = advItems.map(r=>Number(r.BatteryCapacity??0));
            const ys = advItems.map(r=>Number(r.MSRP??0));
            const n = xs.length;
            const mx = xs.reduce((a,b)=>a+b,0)/n; const my = ys.reduce((a,b)=>a+b,0)/n;
            let sxy=0,sxx=0,syy=0;
            for(let i=0;i<n;i++){const dx=xs[i]-mx;const dy=ys[i]-my;sxy+=dx*dy;sxx+=dx*dx;syy+=dy*dy;}
            const r = sxx>0&&syy>0 ? sxy/Math.sqrt(sxx*syy) : 0;
            return <div style={{fontSize:12,color:"var(--c-text-muted)",padding:"4px 8px"}}>Pearson r = <strong>{r.toFixed(3)}</strong>{" (n="+n+")"}</div>;
          })()}

          {/* 8a: metadata-driven range distribution */}
          {advChart==="nev_range_distribution" && advItems.length > 0 && (() => {
            const weightedRangeEnd = asMetaNumber(nevKpis?.weightedRangeEnd);
            const weightedRangeDelta = asMetaNumber(nevKpis?.weightedRangeDelta);
            const offsetRatio = asMetaNumber(nevKpis?.offsetRatio) ?? 0;
            return (
              <>
                <PageBannerStack
                  items={nevWarnings.map((warning, idx) => ({
                    id: `dashboard-nev-warning-${idx}`,
                    tone: "info" as const,
                    message: warning,
                  }))}
                />

                {nevFacetPlot.traces.length > 0 && (
                  <div ref={el => { advChartRef.current = el; }} style={{marginBottom:12}}>
                    <PlotlyChart
                      data={applySeriesColors(
                        applyDataLabelsToTraces(nevFacetPlot.traces, advExport),
                        advExport.seriesColors,
                      )}
                      layout={applyExportToLayout(nevFacetPlot.layout, advExport)}
                      height={advancedDeckLayout.height}
                    />
                  </div>
                )}

                {nevRangeQueryStats && (
                  <div className="kpi-caption" style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:12}}>
                    {`${Math.round(nevRangeQueryStats.target).toLocaleString("en-US")} km 以下：${nevRangeQueryStats.belowCount.toLocaleString("en-US")} / ${nevRangeQueryStats.totalCount.toLocaleString("en-US")} ${nevRangeSampleUnit}，占 ${(nevRangeQueryStats.belowShare * 100).toFixed(1)}%`}
                    {nevRangeQueryStats.belowSalesShare !== null
                      ? `｜按销量占 ${(nevRangeQueryStats.belowSalesShare * 100).toFixed(1)}%（${nevRangeQueryStats.belowSales.toLocaleString(undefined, { maximumFractionDigits: 0 })} / ${nevRangeQueryStats.totalSales.toLocaleString(undefined, { maximumFractionDigits: 0 })}）`
                      : ""}
                  </div>
                )}

                {nevMetricMode === "net_change" && nevAnnualSales.length > 0 && (
                  <div className="kpi-caption" style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:8}}>
                    {"NEV 年度销量：" + nevAnnualSales.map((row) => {
                      const year = String(row.year ?? "-");
                      const sales = (asMetaNumber(row.sales) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 });
                      return `${year} ${sales}`;
                    }).join("｜")}
                  </div>
                )}

                {nevMetricMode === "net_change" && nevKpis && (
                  <div className="kpi-grid" style={{marginBottom:16}}>
                    <div className="kpi-card kpi-primary">
                      <div className="kpi-label">{"时间窗净变化"}</div>
                      <div className="kpi-value">{(asMetaNumber(nevKpis.netChangeTotal) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                      <div className="kpi-sub">{nevGrowthSpanLabel || "末年-首年"}</div>
                    </div>
                    <div className="kpi-card">
                      <div className="kpi-label">{"|净变化|总量"}</div>
                      <div className="kpi-value">{(asMetaNumber(nevKpis.absChangeTotal) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
                      <div className="kpi-sub">{"用于判断结构对冲强度"}</div>
                    </div>
                    <div className="kpi-card">
                      <div className="kpi-label">{"结构对冲率"}</div>
                      <div className="kpi-value">{((asMetaNumber(nevKpis.offsetRatio) ?? 0) * 100).toFixed(1)}%</div>
                      <div className="kpi-sub">{"越高表示桶间此消彼长越明显"}</div>
                    </div>
                    <div className="kpi-card">
                      <div className="kpi-label">{"销量加权平均续航(末年)"}</div>
                      <div className="kpi-value">{weightedRangeEnd !== null ? `${weightedRangeEnd.toFixed(1)} km` : "N/A"}</div>
                      <div className="kpi-sub">{weightedRangeDelta !== null ? `${weightedRangeDelta >= 0 ? "+" : ""}${weightedRangeDelta.toFixed(1)} km vs ${nevStartYearLabel}` : "N/A"}</div>
                    </div>
                  </div>
                )}

                {nevMetricMode === "net_change" && nevPowertrainTokens.length > 0 && (
                  <div className="kpi-caption" style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:8}}>
                    {"净变化贡献：" + nevPowertrainTokens.join("｜")}
                  </div>
                )}

                {nevMetricMode === "net_change" && nevTopModelAbsShare !== null && nevTopModelLimit !== null && nevTopModelLimit > 0 && (
                  <div className="kpi-caption" style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:8}}>
                    {`Top${nevTopModelLimit} Model 贡献了 ${(nevTopModelAbsShare * 100).toFixed(1)}% 的 |净变化|。`}
                  </div>
                )}

                {nevMetricMode === "net_change" && nevTopModelAbsShare !== null && nevTopModelLimit !== null && nevTopModelLimit > 0 && nevTopModelAbsShare >= 0.7 && (
                  <PageBannerStack
                    items={[{
                      id: "dashboard-nev-concentration-warning",
                      tone: "warning" as const,
                      message: `Top${nevTopModelLimit} |净变化|集中度 ${(nevTopModelAbsShare * 100).toFixed(1)}% >= 70%，结构风险较高，建议关注头部车型波动。`,
                    }]}
                  />
                )}

                {nevMetricMode === "net_change" && offsetRatio >= 0.85 && (
                  <PageBannerStack
                    items={[{
                      id: "dashboard-nev-offset-info",
                      tone: "info" as const,
                      message: "对冲率较高：净增背后存在较强的车型结构迁移，建议结合分桶与 Top 车型明细一起看。",
                    }]}
                  />
                )}

                {nevMetricMode === "net_change" && (nevBucketSummary.length > 0 || nevModelMovers.length > 0) && (
                  <details style={{marginBottom:4}}>
                    <summary style={{cursor:"pointer",fontSize:13,color:"var(--c-text-secondary)"}}>{"查看净变化结构拆解"}</summary>

                    {nevBucketSummary.length > 0 && (
                      <div className="table-wrapper" style={{marginTop:8}}>
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>{"续航分桶(km)"}</th>
                              <th>{`${nevStartYearLabel}销量`}</th>
                              <th>{"末年销量"}</th>
                              <th>{`净变化(${nevGrowthSpanLabel || "末年-首年"})`}</th>
                              <th>{"净变化贡献"}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {nevBucketSummary.map((row, idx) => (
                              <tr key={String(row.RangeBandLabel ?? idx)}>
                                <td>{String(row.RangeBandLabel ?? "-")}</td>
                                <td>{(asMetaNumber(row.SalesWindowStartYear) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                <td>{(asMetaNumber(row.SalesWindowEndYear) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                <td>{(asMetaNumber(row.GrowthWindow) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                <td>{`${((asMetaNumber(row.NetShare) ?? 0) * 100).toFixed(1)}%`}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}

                    {(nevBucketPositive.length > 0 || nevBucketNegative.length > 0) && (
                      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit, minmax(260px, 1fr))",gap:12,marginTop:8}}>
                        {nevBucketPositive.length > 0 && (
                          <div className="table-wrapper">
                            <div style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:6}}>{"续航分桶净变化 Top 正向"}</div>
                            <table className="data-table">
                              <thead><tr><th>{"续航分桶(km)"}</th><th>{`净变化(${nevGrowthSpanLabel || "末年-首年"})`}</th><th>{"净变化贡献"}</th></tr></thead>
                              <tbody>
                                {nevBucketPositive.map((row, idx) => (
                                  <tr key={`pos-${String(row.RangeBandLabel ?? idx)}`}>
                                    <td>{String(row.RangeBandLabel ?? "-")}</td>
                                    <td>{(asMetaNumber(row.GrowthWindow) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                    <td>{`${((asMetaNumber(row.NetShare) ?? 0) * 100).toFixed(1)}%`}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                        {nevBucketNegative.length > 0 && (
                          <div className="table-wrapper">
                            <div style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:6}}>{"续航分桶净变化 Top 负向"}</div>
                            <table className="data-table">
                              <thead><tr><th>{"续航分桶(km)"}</th><th>{`净变化(${nevGrowthSpanLabel || "末年-首年"})`}</th><th>{"净变化贡献"}</th></tr></thead>
                              <tbody>
                                {nevBucketNegative.map((row, idx) => (
                                  <tr key={`neg-${String(row.RangeBandLabel ?? idx)}`}>
                                    <td>{String(row.RangeBandLabel ?? "-")}</td>
                                    <td>{(asMetaNumber(row.GrowthWindow) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                    <td>{`${((asMetaNumber(row.NetShare) ?? 0) * 100).toFixed(1)}%`}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                      </div>
                    )}

                    {nevModelMovers.length > 0 && (
                      <div className="table-wrapper" style={{marginTop:8}}>
                        <div style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:6}}>{"Top 车型（按 |净变化| 排序）"}</div>
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>{"Model"}</th>
                              <th>{`${nevStartYearLabel}销量`}</th>
                              <th>{"末年销量"}</th>
                              <th>{`净变化(${nevGrowthSpanLabel || "末年-首年"})`}</th>
                              <th>{"|净变化|"}</th>
                            </tr>
                          </thead>
                          <tbody>
                            {nevModelMovers.map((row, idx) => (
                              <tr key={String(row.Model ?? idx)}>
                                <td>{String(row.Model ?? "-")}</td>
                                <td>{(asMetaNumber(row.SalesWindowStartYear) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                <td>{(asMetaNumber(row.SalesWindowEndYear) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                <td>{(asMetaNumber(row.GrowthWindow) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                <td>{(asMetaNumber(row.GrowthAbsWindow) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}

                    {(nevModelGains.length > 0 || nevModelDeclines.length > 0) && (
                      <div style={{display:"grid",gridTemplateColumns:"repeat(auto-fit, minmax(260px, 1fr))",gap:12,marginTop:8}}>
                        {nevModelGains.length > 0 && (
                          <div className="table-wrapper">
                            <div style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:6}}>{`Top${nevTopModelLimit ?? nevModelGains.length} 正向车型`}</div>
                            <table className="data-table">
                              <thead><tr><th>{"Model"}</th><th>{`净变化(${nevGrowthSpanLabel || "末年-首年"})`}</th></tr></thead>
                              <tbody>
                                {nevModelGains.map((row, idx) => (
                                  <tr key={`gain-${String(row.Model ?? idx)}`}>
                                    <td>{String(row.Model ?? "-")}</td>
                                    <td>{(asMetaNumber(row.GrowthWindow) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                        {nevModelDeclines.length > 0 && (
                          <div className="table-wrapper">
                            <div style={{fontSize:12,color:"var(--c-text-secondary)",marginBottom:6}}>{`Top${nevTopModelLimit ?? nevModelDeclines.length} 负向车型`}</div>
                            <table className="data-table">
                              <thead><tr><th>{"Model"}</th><th>{`净变化(${nevGrowthSpanLabel || "末年-首年"})`}</th></tr></thead>
                              <tbody>
                                {nevModelDeclines.map((row, idx) => (
                                  <tr key={`decline-${String(row.Model ?? idx)}`}>
                                    <td>{String(row.Model ?? "-")}</td>
                                    <td>{(asMetaNumber(row.GrowthWindow) ?? 0).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        )}
                      </div>
                    )}
                  </details>
                )}
              </>
            );
          })()}

          {/* stacked bar */}
          {STACKED_CHARTS.has(advChart) && advChart !== "nev_range_distribution" && stackData.length > 0 && (() => {
            const xKey = advChart==="segment_share_by_length"?"LengthBand":"PriceBand";
            const isPtStack = advChart === "powertrain_vs_price";
            const isHorizontal = false;
            /* compute per-xKey totals for percentage labels */
            const xTotals = new Map<unknown, number>();
            for (const r of stackData) {
              const x = r[xKey];
              let total = 0;
              for (const k of stackKeys) total += Number(r[k] ?? 0);
              xTotals.set(x, total);
            }
            const traces: Data[] = stackKeys.map((k, i) => ({
              ...(isHorizontal
                ? { y: stackData.map(r => r[xKey] as number), x: stackData.map(r => Number(r[k] ?? 0)), orientation: "h" as const }
                : { x: stackData.map(r => r[xKey] as number), y: stackData.map(r => Number(r[k] ?? 0)) }),
              type: "bar" as const,
              name: k,
              marker: {
                color: resolveAdvancedSeriesColor(
                  k,
                  i,
                  stackKeys.length,
                  advPalette,
                  isPtStack,
                  advExport.seriesColors,
                  advancedFocusedPowertrain,
                ),
              },
              ...(advChart === "segment_share_by_length" ? {
                text: stackData.map(r => { const v = Number(r[k] ?? 0); const t = xTotals.get(r[xKey]) ?? 1; return t > 0 ? (v / t * 100).toFixed(0) + "%" : ""; }),
                textposition: "inside" as const,
                textfont: { size: 10, color: "#fff" },
              } : {}),
            }));
            return (
              <div ref={el => { advChartRef.current = el; }}>
                <PlotlyChart
                  data={applySeriesColors(applyDataLabelsToTraces(traces, advExport), advExport.seriesColors)}
                  layout={applyExportToLayout({
                    barmode: "stack",
                    ...(isHorizontal
                      ? { yaxis: { title: { text: xKey }, autorange: "reversed" as const }, xaxis: { title: { text: "Sales" } } }
                      : { xaxis: { title: { text: xKey }, tickangle: -45 }, yaxis: { title: { text: "Sales" } } }),
                  }, advExport)}
                  height={advancedDeckLayout.height}
                />
              </div>
            );
          })()}

          {/* price migration area/line */}
          {advChart==="price_migration" && migrationData.length > 0 && (() => {
            const isArea = advMigrationMode === "area";
            const traces: Data[] = migrationYears.map((yr, i) => {
              const color = resolveAdvancedSeriesColor(
                yr,
                i,
                migrationYears.length,
                advPalette,
                false,
                advExport.seriesColors,
                advancedFocusedPowertrain,
              );
              return {
                x: migrationData.map(r => r.priceBand as number),
                y: migrationData.map(r => Number(r[yr] ?? 0)),
                type: "scatter" as const,
                mode: "lines" as const,
                ...(isArea ? { fill: "tozeroy" as const, fillcolor: `${color}26` } : {}),
                name: yr,
                line: { color, width: 2 },
              };
            });
            return (
              <div ref={el => { advChartRef.current = el; }}>
                <PlotlyChart
                  data={applySeriesColors(applyDataLabelsToTraces(traces, advExport), advExport.seriesColors)}
                  layout={applyExportToLayout({
                      xaxis: { title: { text: "\u4ef7\u683c\u5e26" }, tickangle: -45 },
                    yaxis: { title: { text: "Sales" } },
                  }, advExport)}
                  height={advancedDeckLayout.height}
                />
              </div>
            );
          })()}

          {/* heatmap — B8: Plotly heatmap instead of HTML table */}
          {advChart==="seasonality_heatmap" && hmYears.length > 0 && (() => {
            const z = hmYears.map(y => months.map(m => hmVal(y, m)));
            const heatTraces: Data[] = [{
              z, x: months, y: hmYears,
              type: "heatmap" as const,
              colorscale: advHeatmapScale,
              hoverongaps: false,
              hovertemplate: "Year: %{y}<br>Month: %{x}<br>Sales: %{z:,.0f}<extra></extra>",
            } as Data];
            return (
              <div ref={el => { advChartRef.current = el; }}>
                <PlotlyChart
                  data={applyDataLabelsToTraces(heatTraces, advExport)}
                  layout={applyExportToLayout({
                      xaxis: { title: { text: "Month" } },
                    yaxis: { title: { text: "Year" }, autorange: "reversed" as const },
                  }, advExport)}
                  height={advancedDeckLayout.height}
                />
              </div>
            );
          })()}

          {/* RV Finance Dashboard (inline within advanced analysis) */}
          {advChart==="rv_finance_dashboard" && (
            <Suspense fallback={<div className="analysis-inline-note">正在加载 RV Finance 仪表盘...</div>}>
              <RvFinanceDashboard />
            </Suspense>
          )}

          {advChart!=="rv_finance_dashboard" && advItems.length===0 && !advLoading && <div className="chart-empty">{"\u70b9\u51fb\u300c\u52a0\u8f7d\u56fe\u8868\u300d\u67e5\u770b\u5206\u6790\u7ed3\u679c"}</div>}
          </div>
        </div>

        {/* ── Bug 2: Model Version Bubble ─────────────── */}
        <div
          className="card analysis-deck-card dashboard-deck-card--compact-hero"
          style={getDeckLayoutStyle(modelVersionDeckLayout)}
        >
          <div className="dashboard-hero-head dashboard-deck-hero-head">
            <div className="dashboard-hero-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">05 / Single Model Lens</span>
              <h3>Model Version Bubble</h3>
              <p>锁定单一 Model，查看版本在车长与 MSRP 平面上的分布，并沿用当前 Dashboard 的筛选范围。</p>
            </div>
            <div className="dashboard-hero-actions dashboard-deck-hero-actions dashboard-deck-hero-actions--pair">
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Data state</span>
                <strong className="hero-meta-value">{mvLoading ? "SYNC" : mvItems.length ? "READY" : "IDLE"}</strong>
                <span className="hero-meta-subvalue">{mvItems.length ? `${mvItems.length} 个版本` : "等待加载版型"}</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Quick picks</span>
                <strong className="hero-meta-value">{String(selections.model.length).padStart(2, "0")}</strong>
                <span className="hero-meta-subvalue">来自共享筛选</span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
          <div className="adv-controls adv-controls-panel">
            <div className="filter-group adv-control-unit adv-control-unit--wide"><label>Model</label>
              <input type="text" placeholder="\u8f93\u5165 Model \u540d\u79f0" value={mvModelName}
                onChange={e=>setMvModelName(e.target.value)}
                style={{width:180}} />
            </div>
            <div className="filter-group adv-control-unit"><label>Top N</label>
              <input type="number" value={mvTopN} min={5} max={200} style={{width:60}}
                onChange={e=>setMvTopN(Number(e.target.value)||50)} />
            </div>
            <div className="filter-group adv-control-unit"><label>{"\u7740\u8272"}</label>
              <select value={mvColorBy} onChange={e=>setMvColorBy(e.target.value as "Powertrain"|"Trim")}>
                <option value="Powertrain">{"\u52a8\u529b\u603b\u6210"}</option>
                <option value="Trim">Trim</option>
              </select>
            </div>
            <LoadingActionButton loading={mvLoading} loadingLabel="加载中…" disabled={!mvModelName.trim()} onClick={()=>{ releaseHeavyQueries(); void loadModelVersions(); }}>
              加载版型
            </LoadingActionButton>
          </div>
          {/* Model filter quick pick */}
          {selections.model.length > 0 && (
            <div className="mv-quick-pick analysis-chip-row">
              <span className="analysis-chip-label">{"\u5feb\u9009"}</span>
              {selections.model.map(m=>(
                <button key={m} type="button" className={"analysis-chip-button"+(mvModelName===m?" is-active":"")}
                  onClick={()=>setMvModelName(m)}>{m}</button>
              ))}
            </div>
          )}
          {mvItems.length > 0 && (
            <div className="analysis-chip-row analysis-chip-row--compact">
              <span className="analysis-chip">Visible versions {mvItems.length}</span>
              <span className="analysis-chip">Shared filter scope active</span>
            </div>
          )}
          {mvItems.length > 0 && (() => {
            const isPtBubble = mvColorBy === "Powertrain";
            const cats = [...new Set(mvItems.map(r=>r[mvColorBy]))];
            const mvBubbleSizing = buildBubbleSizing(
              mvItems.map(r => r.Sales),
              { maxDiameter: 24 * advBubbleScale, minDiameter: 4 },
            );
            const traces: Data[] = cats.map((cat, i) => {
              const subset = mvItems.filter(r=>r[mvColorBy]===cat);
              return withExportLabels({
                x: subset.map(r => r.Length),
                y: subset.map(r => r.MSRP),
                text: subset.map(r => r.Version),
                customdata: subset.map(r => [r.Trim, r.Powertrain, Math.round(r.Sales)]),
                type: "scatter",
                mode: "markers",
                name: cat,
                marker: {
                  color: resolveAdvancedSeriesColor(
                    cat,
                    i,
                    cats.length,
                    mvPalette,
                    isPtBubble,
                    mvExport.seriesColors,
                    filterFocusedPowertrain,
                  ),
                  size: subset.map(r => Math.max(0, r.Sales)),
                  sizemode: mvBubbleSizing.sizemode,
                  sizeref: mvBubbleSizing.sizeref,
                  sizemin: mvBubbleSizing.sizemin,
                  opacity: 0.7,
                },
                hovertemplate: "<b>%{text}</b><br>Trim: %{customdata[0]}<br>动力: %{customdata[1]}<br>车长: %{x} mm<br>MSRP: %{y:,.0f}<br>销量: %{customdata[2]}<extra>%{fullData.name}</extra>",
              } as Data, {
                sales: subset.map(r => Math.round(r.Sales)),
                value: subset.map(r => r.MSRP),
                series: subset.map(() => cat),
              }) as Data;
            });
            return (
              <div ref={el => { mvChartRef.current = el; }}>
                <PlotlyChart
                  data={applySeriesColors(applyDataLabelsToTraces(traces, mvExport), mvExport.seriesColors)}
                  layout={applyExportToLayout({
                      xaxis: { title: { text: "车长(mm)" } },
                    yaxis: { title: { text: "MSRP" } },
                  }, mvExport)}
                  height={modelVersionDeckLayout.height}
                />
              </div>
            );
          })()}
          {mvItems.length===0 && !mvLoading && <div className="chart-empty">{"\u8f93\u5165 Model \u540d\u79f0\u5e76\u70b9\u51fb\u300c\u52a0\u8f7d\u7248\u578b\u300d"}</div>}
          </div>
        </div>

        {/* ── Bug 3: OJ Positioning Map ───────────────── */}
        <div
          className="card analysis-deck-card dashboard-deck-card--compact-hero"
          style={getDeckLayoutStyle(positioningDeckLayout)}
        >
          <div className="dashboard-hero-head dashboard-deck-hero-head">
            <div className="dashboard-hero-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">06 / Competitive Positioning</span>
              <h3>OJ Positioning Map</h3>
              <p>基于当前筛选边界生成竞品聚类，并支持叠加手动竞品与目标车型坐标，保持与主分析区一致的控件语言。</p>
            </div>
            <div className="dashboard-hero-actions dashboard-deck-hero-actions dashboard-deck-hero-actions--pair">
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Map state</span>
                <strong className="hero-meta-value">{pmLoading ? "SYNC" : pmItems.length ? "READY" : "IDLE"}</strong>
                <span className="hero-meta-subvalue">{pmItems.length ? `${pmItems.length} 个候选点` : "等待加载定位图"}</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Manual rivals</span>
                <strong className="hero-meta-value">{String(pmManualCompetitors.length).padStart(2, "0")}</strong>
                <span className="hero-meta-subvalue">{pmPeerCorridor ? `${pmPeerCorridor.peerCount} 个 peer` : pmClusterTop3.length ? `${pmClusterTop3.length} 个Top3标签` : "尚未生成聚类代表"}</span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
          <div className="adv-controls adv-controls-panel">
            <div className="filter-group adv-control-unit"><label>{"\u76ee\u6807\u8f66\u957f(mm)"}</label>
              <input type="number" placeholder="4500" value={pmTargetLength}
                onChange={e=>setPmTargetLength(e.target.value)} style={{width:100}} />
            </div>
            <div className="filter-group adv-control-unit"><label>{"\u76ee\u6807 MSRP"}</label>
              <input type="number" placeholder="30000" value={pmTargetMsrp}
                onChange={e=>setPmTargetMsrp(e.target.value)} style={{width:100}} />
            </div>
            <div className="filter-group adv-control-unit"><label>{"\u8f66\u957f\u7a97\u53e3(mm)"}</label>
              <input type="number" value={pmLengthRange} min={100} max={2000} step={100} style={{width:80}}
                onChange={e=>setPmLengthRange(Number(e.target.value)||600)} />
            </div>
            <div className="filter-group adv-control-unit"><label>{"\u805a\u7c7b\u6570"}</label>
              <input type="number" value={pmNClusters} min={2} max={10} style={{width:56}}
                onChange={e=>setPmNClusters(Number(e.target.value)||4)} />
            </div>
            <div className="filter-group adv-control-unit"><label>Top N</label>
              <input type="number" value={pmTopN} min={10} max={300} style={{width:60}}
                onChange={e=>setPmTopN(Number(e.target.value)||80)} />
            </div>
            <LoadingActionButton loading={pmLoading} loadingLabel="加载中…" onClick={()=>{ releaseHeavyQueries(); void loadPositioningMap(); }}>
              加载定位图
            </LoadingActionButton>
          </div>
          {/* manual competitor input */}
          <div className="pm-competitor-bar">
            <span className="analysis-chip-label">{"\u624b\u52a8\u7ade\u54c1"}</span>
            <input type="text" placeholder="\u8f93\u5165\u54c1\u724c\u540d\u79f0\u2026" value={pmManualInput}
              onChange={e=>setPmManualInput(e.target.value)}
              onKeyDown={e=>{if(e.key==="Enter") addCompetitor();}}
              style={{width:150}} />
            <button className="btn btn-sm btn-secondary" onClick={addCompetitor}>{"\u6dfb\u52a0"}</button>
            {pmManualCompetitors.map(c=>(
              <span key={c} className="pm-competitor-chip">
                {c}
                <button className="pm-chip-remove" onClick={()=>setPmManualCompetitors(p=>p.filter(x=>x!==c))}>{"\u00d7"}</button>
              </span>
            ))}
          </div>
          {/* cluster top 3 */}
          {pmClusterTop3.length > 0 && (
            <div className="pm-cluster-top3">
              <span className="analysis-chip-label">{"KMeans Top3"}</span>
              {pmClusterTop3.map(c=><span key={c} className="pm-top3-label">{c}</span>)}
            </div>
          )}
          {pmPeerCorridor && (
            <div className="pm-cluster-top3">
              <span className="analysis-chip-label">Peer corridor</span>
              {pmPeerCorridor.stanceLabel ? (
                <span className="pm-top3-label">{pmPeerCorridor.stanceLabel}</span>
              ) : null}
              <span className="pm-top3-label">{`${Math.round(pmPeerCorridor.msrpP25).toLocaleString("en-US")} - ${Math.round(pmPeerCorridor.msrpP75).toLocaleString("en-US")}`}</span>
              <span className="pm-top3-label">{`Median ${Math.round(pmPeerCorridor.msrpMedian).toLocaleString("en-US")}`}</span>
              <span className="pm-top3-label">{`${Math.round(pmPeerCorridor.lengthMin).toLocaleString("en-US")} - ${Math.round(pmPeerCorridor.lengthMax).toLocaleString("en-US")} mm`}</span>
              {typeof pmPeerCorridor.targetMsrp === "number" ? (
                <span className="pm-top3-label">
                  {`Target ${Math.round(pmPeerCorridor.targetMsrp).toLocaleString("en-US")} / ${typeof pmPeerCorridor.targetResidualPct === "number" ? `${pmPeerCorridor.targetResidualPct > 0 ? "+" : ""}${pmPeerCorridor.targetResidualPct.toFixed(1)}%` : "-"}`}
                </span>
              ) : null}
            </div>
          )}
          {pmItems.length > 0 && (() => {
            const clusterIds = [...new Set(pmItems.map(r=>r.cluster))].sort((a,b)=>a-b);
            const pmBubbleSizing = buildBubbleSizing(
              pmItems.map(item => item.Sales),
              { maxDiameter: 24 * advBubbleScale, minDiameter: 4 },
            );
            const traces: Data[] = clusterIds.map((cid, i) => {
              const subset = pmItems.filter(r=>r.cluster===cid);
              return withExportLabels({
                x: subset.map(r => r.Length),
                y: subset.map(r => r.MSRP),
                text: subset.map(r => r.Brand + " " + r.Model),
                customdata: subset.map(r => [r.Segment, Math.round(r.Sales), r.cluster]),
                type: "scatter",
                mode: "markers",
                name: "Cluster " + cid,
                marker: {
                  color: resolveAdvancedSeriesColor(
                    "Cluster " + cid,
                    i,
                    clusterIds.length,
                    pmPalette,
                    false,
                    pmExport.seriesColors,
                    filterFocusedPowertrain,
                  ),
                  size: subset.map(r => Math.max(0, r.Sales)),
                  sizemode: pmBubbleSizing.sizemode,
                  sizeref: pmBubbleSizing.sizeref,
                  sizemin: pmBubbleSizing.sizemin,
                },
                hovertemplate: "<b>%{text}</b><br>\u7ec6\u5206: %{customdata[0]}<br>\u8f66\u957f: %{x} mm<br>MSRP: %{y:,.0f}<br>\u9500\u91cf: %{customdata[1]}<br>\u805a\u7c7b: %{customdata[2]}<extra>%{fullData.name}</extra>",
              } as Data, {
                model: subset.map(r => r.Model),
                sales: subset.map(r => Math.round(r.Sales)),
                value: subset.map(r => r.MSRP),
                series: subset.map(() => `聚类 ${cid}`),
              }) as Data;
            });
            if (pmTarget) {
              traces.push(withExportLabels({
                x: [pmTarget.Length],
                y: [pmTarget.MSRP],
                text: ["\u76ee\u6807\u8f66\u578b"],
                type: "scatter",
                mode: "markers",
                name: "\u76ee\u6807\u8f66\u578b",
                marker: { color: "#ef4444", size: 16, symbol: "diamond" },
                hovertemplate: "<b>\u76ee\u6807\u8f66\u578b</b><br>\u8f66\u957f: %{x} mm<br>MSRP: %{y:,.0f}<extra>%{fullData.name}</extra>",
              } as Data, {
                model: ["\u76ee\u6807\u8f66\u578b"],
                sales: [""],
                value: [pmTarget.MSRP],
                series: ["\u76ee\u6807\u8f66\u578b"],
              }) as Data);
            }
            return (
              <div ref={el => { pmChartRef.current = el; }}>
                <PlotlyChart
                  data={applySeriesColors(applyDataLabelsToTraces(traces, pmExport), pmExport.seriesColors)}
                  layout={applyExportToLayout({
                      xaxis: { title: { text: "\u8f66\u957f(mm)" } },
                    yaxis: { title: { text: "MSRP" } },
                  }, pmExport)}
                  height={positioningDeckLayout.height}
                />
              </div>
            );
          })()}
          {pmItems.length===0 && !pmLoading && <div className="chart-empty">{"\u8f93\u5165\u76ee\u6807\u8f66\u578b\u53c2\u6570\u6216\u76f4\u63a5\u70b9\u51fb\u300c\u52a0\u8f7d\u5b9a\u4f4d\u56fe\u300d\u67e5\u770b\u5f53\u524d\u7b5b\u9009\u8fb9\u754c\u5185\u5b9a\u4ef7"}</div>}
          </div>
        </div>
          </>
        ) : (
          <DeferredDashboardDecksPlaceholder onActivate={releaseHeavyQueries} />
        )}

        <div className="card analysis-deck-card analysis-route-card dashboard-deck-card--compact-hero">
          <div className="dashboard-hero-head dashboard-deck-hero-head">
            <div className="dashboard-hero-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">07 / Specification Route</span>
              <h3>Specification Entry</h3>
              <p>明细表、列选择、分页和 CSV 导出已经迁到独立 Specification Page，Dashboard 只保留 KPI 与图表交互。</p>
            </div>
            <div className="dashboard-hero-actions dashboard-deck-hero-actions dashboard-deck-hero-actions--pair">
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Route State</span>
                <strong className="hero-meta-value">READY</strong>
                <span className="hero-meta-subvalue">与 Dashboard 共享筛选 query</span>
              </div>
              <div className="hero-meta-block dashboard-deck-hero-stat">
                <span className="hero-meta-label">Active Filters</span>
                <strong className="hero-meta-value">{String(activeFilters.length)}</strong>
                <span className="hero-meta-subvalue">{activeFilters.length ? "带当前筛选进入" : "使用默认筛选进入"}</span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
            <div className="dashboard-cta-row">
              <Link className="btn btn-primary" to={specificationHref}>{"\u6253\u5f00 Specification Page"}</Link>
              <Link className="btn btn-ghost" to={specificationHref}>{"\u5e26\u5f53\u524d\u7b5b\u9009\u8fdb\u5165"}</Link>
            </div>
          </div>
        </div>
      </section>

      {/* global control drawer — Time-Series quick controls + layout */}
      <DeckFloatingDrawer
          className="dashboard-control-drawer"
          open={deckControlDrawerOpen}
          onOpenChange={handleControlDrawerOpen}
          triggerPrimary="筛选 / 版式"
          triggerSecondaryOpen="收起控制"
          triggerSecondaryClosed="打开控制"
          eyebrow="Dashboard"
          title="控制与版式"
          ariaLabel="Dashboard deck controls"
        >
          <DeckControlTabs
            tabs={DECK_CONTROL_TABS}
            activeKey={deckControlTab}
            onChange={setDeckControlTab}
            ariaLabel="Dashboard control tabs"
          />

          <div className="deck-export-section-tabs">
            {DECK_SECTION_TABS.map((tab) => (
              <button
                key={tab.key}
                type="button"
                className={`tab-btn${activeDeckSection === tab.key ? " active" : ""}`}
                onClick={() => setActiveDeckSection(tab.key)}
              >
                {tab.label}
              </button>
            ))}
          </div>

          {deckControlTab === "window" ? (
            <div>
              <TimeAxis
                labels={timeLabels}
                value={timeRange}
                onChange={setTimeRange}
                grain={activeTab}
                onGrainChange={setActiveTab}
                showTitle={false}
                monthGrain={monthGrain}
                onMonthGrainChange={setMonthGrain}
              />
            </div>
          ) : deckControlTab === "chart" ? (
            <div>
              <div className="positioning-pricing-control-grid">
                {activeDeckSection === "timeSeries" && (
                  <>
                    <label className="market-scan-field">
                      <span>Year / Month</span>
                      <div className="tab-bar">
                        <button className={`tab-btn${activeTab==="year"?" active":""}`} onClick={()=>setActiveTab("year")}>年度</button>
                        <button className={`tab-btn${activeTab==="month"?" active":""}`} onClick={()=>setActiveTab("month")}>月度</button>
                      </div>
                    </label>
                    <label className="market-scan-field">
                      <span>Series</span>
                      <div className="tab-bar">
                        <button className={`tab-btn${tsMode==="总和"?" active":""}`} onClick={()=>selectTsMode("总和")}>总和</button>
                        <button className={`tab-btn${tsMode==="分组"?" active":""}`} onClick={()=>selectTsMode("分组")}>分组</button>
                      </div>
                    </label>
                    <label className="market-scan-field positioning-pricing-control-field--wide">
                      <span>Chart</span>
                      <div className="tab-bar">
                        <button className={`tab-btn${chartType==="line"?" active":""}`} onClick={()=>selectChartType("line")}>折线</button>
                        <button className={`tab-btn${chartType==="bar"?" active":""}`} onClick={()=>selectChartType("bar")}>柱状</button>
                        <button className={`tab-btn${chartType==="rank"?" active":""}`} onClick={()=>selectChartType("rank")}>排名</button>
                      </div>
                    </label>
                    {isGrouped && (
                      <label className="market-scan-field">
                        <span>分组维度</span>
                        <select value={tsGroupDim} onChange={e=>setTsGroupDim(e.target.value)}>
                          {GROUP_BY_OPTIONS.map(o=><option key={o.v} value={o.v}>{o.l}</option>)}
                        </select>
                      </label>
                    )}
                    {isGrouped && !isShareGrouped && (
                      <label className="market-scan-field">
                        <span>{chartType === "rank" ? "Rank" : "Top N"}</span>
                        {chartType === "rank" ? (
                          <DebouncedNumberInput value={rankLimit} onCommit={(v) => v !== null && setRankLimit(v)} min={5} max={100} delayMs={1200} />
                        ) : (
                          <DebouncedNumberInput value={tsTopN} onCommit={(v) => v !== null && setTsTopN(v)} min={3} max={30} delayMs={1200} />
                        )}
                      </label>
                    )}
                  </>
                )}
                {activeDeckSection === "advanced" && (
                  <>
                    <label className="market-scan-field">
                      <span>分析组</span>
                      <select value={advGroup} onChange={e=>setAdvGroup(e.target.value)}>
                        {ADV_GROUPS.map(o=><option key={o.v} value={o.v}>{o.l}</option>)}
                      </select>
                    </label>
                    <label className="market-scan-field">
                      <span>图表</span>
                      <select value={advChart} onChange={e=>setAdvChart(e.target.value)}>
                        {(ADV_CHARTS[advGroup]??[]).map(o=><option key={o.v} value={o.v}>{o.l}</option>)}
                      </select>
                    </label>
                    <label className="market-scan-field">
                      <span>Top N</span>
                      <DebouncedNumberInput value={advTopN} onCommit={(v) => v !== null && setAdvTopN(v)} min={5} max={100} delayMs={1200} />
                    </label>
                  </>
                )}
                {activeDeckSection === "modelVersion" && (
                  <label className="market-scan-field positioning-pricing-control-field--wide">
                    <span>Model Name</span>
                    <input type="text" value={mvModelName} onChange={e=>setMvModelName(e.target.value)} placeholder="输入 Model 名称" className="market-scan-field-input" />
                  </label>
                )}
                {activeDeckSection === "positioning" && (
                  <>
                    <label className="market-scan-field">
                      <span>Target Length</span>
                      <DebouncedNumberInput value={pmTargetLength ? Number(pmTargetLength) : null} onCommit={(v) => v !== null && setPmTargetLength(String(v))} min={3000} max={6000} step={100} allowEmpty delayMs={1200} />
                    </label>
                    <label className="market-scan-field">
                      <span>Target MSRP</span>
                      <DebouncedNumberInput value={pmTargetMsrp ? Number(pmTargetMsrp) : null} onCommit={(v) => v !== null && setPmTargetMsrp(String(v))} min={0} max={500000} step={1000} allowEmpty delayMs={1200} />
                    </label>
                    <label className="market-scan-field">
                      <span>Top N</span>
                      <DebouncedNumberInput value={pmTopN} onCommit={(v) => v !== null && setPmTopN(v)} min={10} max={200} delayMs={1200} />
                    </label>
                  </>
                )}
              </div>
            </div>
          ) : (
            <div className="positioning-pricing-control-grid">
              <label className="market-scan-field positioning-pricing-control-field--wide">
                <span>图表高度 {activeDeckLayout.height}px</span>
                <input
                  type="range"
                  min={300}
                  max={900}
                  step={10}
                  value={activeDeckLayout.height}
                  onChange={(e) => updateActiveDeckLayout({ height: Number(e.target.value) })}
                />
              </label>
              <label className="market-scan-field positioning-pricing-control-field--wide">
                <span>图表宽度 {activeDeckLayout.width === 0 ? "auto" : `${activeDeckLayout.width}px`}</span>
                <input
                  type="range"
                  min={0}
                  max={2000}
                  step={20}
                  value={activeDeckLayout.width}
                  onChange={(e) => updateActiveDeckLayout({ width: Number(e.target.value) })}
                />
                {activeDeckLayout.width === 0 && <span className="ts-mode-hint">填满容器</span>}
              </label>
              <div className="market-scan-field market-scan-field-actions positioning-pricing-control-field--wide">
                <button type="button" className="btn btn-secondary btn-sm" onClick={resetActiveDeckLayout}>
                  Reset 尺寸
                </button>
              </div>
            </div>
          )}
        </DeckFloatingDrawer>

      {/* global export drawer — user picks which section to configure */}
      <DeckExportDrawer
          className="dashboard-export-drawer"
          open={deckExportDrawerOpen}
          onOpenChange={handleExportDrawerOpen}
          triggerPrimary="导出图设置"
          triggerSecondaryOpen="收起设置"
          triggerSecondaryClosed="打开设置"
          eyebrow={DECK_SECTION_TABS.find(t => t.key === activeDeckSection)?.label ?? "Deck Export"}
          title="导出与图表样式"
          ariaLabel="Dashboard deck export settings"
        >
          <div className="tab-bar deck-export-section-tabs">
            {DECK_SECTION_TABS.map((tab) => (
              <button
                key={tab.key}
                type="button"
                className={`tab-btn${activeDeckSection === tab.key ? " active" : ""}`}
                onClick={() => setActiveDeckSection(tab.key)}
              >
                {tab.label}
              </button>
            ))}
          </div>
          <Suspense fallback={<LoadingSurface mode="inline" label="正在加载导出设置" detail="Export panel" />}>
            {activeDeckSection === "timeSeries" && (
              <DashboardExportPanel value={tsExport} onChange={setTsExport} graphDiv={tsChartRef.current} seriesNames={timeSeriesExportSeriesNames} labelModeOptions={tsLabelModeOptions} showExportButton={false} showDimensionControls={false} collapsible={false} />
            )}
            {activeDeckSection === "advanced" && (
              <>
                <DashboardExportPanel value={advExport} onChange={setAdvExport} graphDiv={advChartRef.current} seriesNames={advancedExportSeriesNames} labelModeOptions={advLabelModeOptions} showExportButton={false} showDimensionControls={false} showLabelStrategyControl={false} collapsible={false} />
                <div className="positioning-pricing-control-grid">
                  <label className="market-scan-field">
                    <span>标签字段</span>
                    <select
                      value={advBubbleLabelDimension}
                      onChange={(e) => {
                        const nextLabel = e.target.value as "model" | "version";
                        setAdvBubbleLabelDimension(nextLabel);
                        if (nextLabel === "version" && advBubbleGrain !== "version") {
                          setAdvBubbleGrain("version");
                          setAdvItems([]);
                          setAdvMeta(null);
                        }
                      }}
                    >
                      <option value="model">Model</option>
                      <option value="version">Version</option>
                    </select>
                    {advBubbleLabelDimension === "version" && advBubbleGrain !== "version" && (
                      <span className="ts-mode-hint">Version 标签会同步切换 04 图粒度并重新加载。</span>
                    )}
                  </label>
                  <label className="market-scan-field">
                    <span>气泡标签</span>
                    <select
                      value={advExport.dataLabelOverlapStrategy}
                      onChange={(e) => setAdvExport((previous) => ({
                        ...previous,
                        dataLabelOverlapStrategy: e.target.value as ExportLabelOverlapStrategy,
                      }))}
                    >
                      {ADVANCED_BUBBLE_LABEL_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>
                </div>
              </>
            )}
            {activeDeckSection === "modelVersion" && (
              <DashboardExportPanel value={mvExport} onChange={setMvExport} graphDiv={mvChartRef.current} seriesNames={modelVersionExportSeriesNames} labelModeOptions={mvLabelModeOptions} showExportButton={false} showDimensionControls={false} collapsible={false} />
            )}
            {activeDeckSection === "positioning" && (
              <DashboardExportPanel value={pmExport} onChange={setPmExport} graphDiv={pmChartRef.current} seriesNames={positioningExportSeriesNames} labelModeOptions={pmLabelModeOptions} showExportButton={false} showDimensionControls={false} collapsible={false} />
            )}
          </Suspense>
        </DeckExportDrawer>
    </div>
  );
}
