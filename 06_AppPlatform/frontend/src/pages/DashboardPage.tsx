import { Suspense, lazy, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";
import type { Data, Layout } from "plotly.js";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { CollapsibleFilterSidebar } from "../components/CollapsibleFilterSidebar";
import { LoadingActionButton } from "../components/LoadingActionButton";
import { LoadingSurface } from "../components/LoadingSurface";
import { SearchSelectFilter } from "../components/SearchSelectFilter";
import { useSharedFilterScope } from "../contexts/SharedFilterScopeContext";
import {
  FILTER_ORDER,
} from "../dashboardFilters";
import type { FilterKey } from "../dashboardFilters";
import type { OverviewResponse, TimeSeriesPoint, GroupedTimeSeriesItem, ModelVersionItem, PositioningMapItem, OthersDetailItem } from "../types";
import { LazyPlotlyChart as PlotlyChart } from "../components/LazyPlotlyChart";
import { TimeAxis, type TimeRange } from "../components/TimeAxis";
import { ExportPanel, DEFAULT_EXPORT, applyExportToLayout, getExportPalette, applyDataLabelsToTraces, applySeriesColors, buildExportLabelModeOptions, withExportLabels, type ExportSettings } from "../components/ExportPanel";
import { buildBubbleSizing } from "../utils/bubbleSizing";
import { getCachedPageValue, setCachedPageValue } from "../utils/pageCache";
const RvFinanceDashboard = lazy(() =>
  import("../components/RvFinanceDashboard").then((module) => ({ default: module.RvFinanceDashboard }))
);

/* ── constants ──────────────────────────────────────── */
const COLORS = [
  "#2563eb","#16a34a","#f59e0b","#ef4444","#8b5cf6","#ec4899",
  "#14b8a6","#f97316","#6366f1","#0ea5e9","#84cc16","#e11d48",
];

/** 动力总成固定配色 — 全局所有图表统一使用 */
const POWERTRAIN_COLORS: Record<string, string> = {
  ICE:  "#6b7280",  // 灰色
  MHEV: "#f97316",  // 橙色
  HEV:  "#eab308",  // 黄色
  PHEV: "#3b82f6",  // 蓝色
  BEV:  "#22c55e",  // 绿色
};
const DEFAULT_POWERTRAINS = ["ICE", "HEV", "BEV", "MHEV", "PHEV"] as const;
const MONTH_INDEX: Record<string, number> = {
  Jan: 1, Feb: 2, Mar: 3, Apr: 4, May: 5, Jun: 6,
  Jul: 7, Aug: 8, Sep: 9, Oct: 10, Nov: 11, Dec: 12,
};
function normalizePowertrainName(value: string): string {
  return value.trim().toUpperCase();
}
function parseMonthLabel(label: string): { year: number; month: number } | null {
  const text = label.trim();
  const monthNameMatch = text.match(/^(\d{4})\s+([A-Za-z]{3})$/);
  if (monthNameMatch) {
    return { year: Number(monthNameMatch[1]), month: MONTH_INDEX[monthNameMatch[2]] ?? 1 };
  }
  const shortYearMatch = text.match(/^(\d{2})[.\/-](\d{1,2})$/);
  if (shortYearMatch) {
    return { year: 2000 + Number(shortYearMatch[1]), month: Number(shortYearMatch[2]) };
  }
  const numericMatch = text.match(/^(\d{4})[-\/.](\d{1,2})$/);
  if (numericMatch) {
    return { year: Number(numericMatch[1]), month: Number(numericMatch[2]) };
  }
  return null;
}
function toTimeOrdinal(label: string): number | null {
  const text = label.trim();
  if (/^\d{4}$/.test(text)) return Number(text) * 100 + 12;
  const month = parseMonthLabel(text);
  if (month) return month.year * 100 + month.month;
  const quarter = text.match(/^(\d{4})-Q([1-4])$/);
  if (quarter) return Number(quarter[1]) * 100 + Number(quarter[2]) * 3;
  return null;
}
function compareTimeLabels(a: string, b: string): number {
  const ao = toTimeOrdinal(a);
  const bo = toTimeOrdinal(b);
  if (ao !== null && bo !== null && ao !== bo) return ao - bo;
  return a.localeCompare(b);
}

function ensureArray<T>(value: T[] | null | undefined): T[] {
  return Array.isArray(value) ? value : [];
}

function buildCategoryAxis(
  labels: string[],
  extra: Partial<Layout["xaxis"]> = {},
): Partial<Layout["xaxis"]> {
  const ordered = Array.from(new Set(labels));
  return {
    type: "category",
    categoryorder: "array",
    categoryarray: ordered,
    ...extra,
  };
}
/** 判断给定系列名/分组维度是否属于动力总成，返回固定色 */
function ptColor(name: string, fallback: string): string {
  return POWERTRAIN_COLORS[name] ?? POWERTRAIN_COLORS[name.toUpperCase()] ?? fallback;
}
/** 当分组维度是动力总成时为系列分配固定颜色，否则沿用 palette */
function seriesColor(name: string, idx: number, palette: string[], isPowertrain: boolean): string {
  if (isPowertrain) return ptColor(name, palette[idx % palette.length]);
  return palette[idx % palette.length];
}

const ADV_GROUPS: { v: string; l: string }[] = [
  { v: "market_structure", l: "\u5e02\u573a\u7ed3\u6784" },
  { v: "nev_analysis", l: "NEV\u5206\u6790" },
  { v: "price_value", l: "\u4ef7\u683c\u4ef7\u503c" },
  { v: "cost_analysis", l: "\u52a8\u529b\u6210\u672c" },
];
const ADV_CHARTS: Record<string, { v: string; l: string }[]> = {
  market_structure: [
    { v: "powertrain_bubble", l: "\u52a8\u529b\u6c14\u6ce1\u56fe" },
    { v: "seasonality_heatmap", l: "\u5b63\u8282\u6027\u70ed\u529b\u56fe" },
    { v: "segment_share_by_length", l: "\u8f66\u957f\u00d7\u7ec6\u5206\u5e02\u573a" },
  ],
  nev_analysis: [
    { v: "nev_range_distribution", l: "\u7eed\u822a\u5206\u5e03" },
    { v: "nev_capacity_vs_msrp", l: "\u7535\u6c60\u5bb9\u91cf vs MSRP" },
  ],
  price_value: [
    { v: "price_migration", l: "\u4ef7\u683c\u8fc1\u79fb" },
    { v: "length_vs_price", l: "\u8f66\u957f vs \u4ef7\u683c" },
    { v: "price_per_meter", l: "\u6bcf\u7c73\u4ef7\u683c" },
    { v: "sales_vs_price", l: "\u9500\u91cf vs \u4ef7\u683c" },
  ],
  cost_analysis: [
    { v: "rv_finance_dashboard", l: "RV\u91d1\u878d\u6760\u6746\u770b\u677f" },
    { v: "estimated_tco", l: "\u4f30\u7b97TCO vs MSRP" },
    { v: "powertrain_vs_price", l: "\u52a8\u529b\u00d7\u4ef7\u683c\u5e26" },
  ],
};
const GROUP_BY_OPTIONS = [
  { v: "\u52a8\u603b\u89c4\u6574", l: "\u52a8\u529b\u603b\u6210" },
  { v: "\u7ec6\u5206\u5e02\u573a\uff08\u6309\u8f66\u957f\uff09", l: "\u7ec6\u5206\u5e02\u573a" },
  { v: "Make", l: "\u54c1\u724c" },
  { v: "Model", l: "Model" },
  { v: "Version name", l: "Version" },
  { v: "\u56fd\u5bb6", l: "\u56fd\u5bb6" },
];

type BubbleGroupDimension = "segment" | "powertrain";

const BUBBLE_GROUP_DIMENSIONS: { v: BubbleGroupDimension; l: string; dataKey: "Segment" | "Powertrain" }[] = [
  { v: "segment", l: "\u7ec6\u5206\u5e02\u573a", dataKey: "Segment" },
  { v: "powertrain", l: "\u52a8\u603b\u89c4\u6574", dataKey: "Powertrain" },
];

const SCATTER_CHARTS = new Set([
  "powertrain_bubble","nev_capacity_vs_msrp",
  "length_vs_price","price_per_meter","sales_vs_price","estimated_tco",
]);
const STACKED_CHARTS = new Set([
  "segment_share_by_length","nev_range_distribution","powertrain_vs_price",
]);

/* ── helpers ────────────────────────────────────────── */
function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function asMetaNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function asMetaText(value: unknown): string {
  return typeof value === "string" ? value : "";
}

function asMetaStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map(item => String(item)).filter(Boolean) : [];
}

function asMetaRecordArray(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter(isPlainRecord) : [];
}

const DASHBOARD_CACHE_KEY = "dashboard-page";
const PAGE_CACHE_TTL_MS = 30 * 60 * 1000;

interface DashboardPageCache {
  search: string;
  columns: string[];
  selections: Record<string, string[]>;
  optionsMap: Record<string, string[]>;
  heroCollapsed: boolean;
  sidebarCollapsed: boolean;
  filteredRowCount: number | null;
  overview: OverviewResponse | null;
  yearSeries: TimeSeriesPoint[];
  monthSeries: TimeSeriesPoint[];
  activeTab: "year" | "month";
  chartType: "line" | "bar";
  tsMode: "总和" | "分组";
  tsGroupDim: string;
  tsTopN: number;
  tsTopNEnabled: boolean;
  tsIncludeOthers: boolean;
  groupedItems: GroupedTimeSeriesItem[];
  hiddenSeries: string[];
  othersDetail: OthersDetailItem[];
  advGroup: string;
  advChart: string;
  advItems: Record<string, string | number>[];
  advMeta: Record<string, unknown> | null;
  advBandSize: number;
  advTopN: number;
  advMigrationMode: "area" | "line";
  advBubbleScale: number;
  advBubbleGrain: "model" | "version";
  advBubbleFacet: boolean;
  advBubbleFacetMax: number;
  advBubbleShowYoy: boolean;
  advBubbleYoyYear: string;
  advBubbleGroupTopN: boolean;
  advBubbleGroupDimension: BubbleGroupDimension;
  advBubbleGroupValues: string[];
  advBubbleGroupTopNMap: Record<string, number>;
  advPowertrains: string[];
  advNevTopNEnabled: boolean;
  advNevAxisMax: number;
  advNevMetricMode: "window_sales" | "net_change";
  advNevStackByModel: boolean;
  advNevFacetBrand: boolean;
  advNevMaxBrandFacets: number;
  advRangeStep: number;
  advHeatmapScale: string;
  tcoYears: number;
  tcoAnnualKm: number;
  tcoDepreciation: number;
  tcoMaintenance: number;
  tcoTaxInsurance: number;
  tcoEnergyCost: number;
  mvModelName: string;
  mvTopN: number;
  mvItems: ModelVersionItem[];
  mvColorBy: "Powertrain" | "Trim";
  pmTargetLength: string;
  pmTargetMsrp: string;
  pmLengthRange: number;
  pmManualCompetitors: string[];
  pmTopN: number;
  pmNClusters: number;
  pmItems: PositioningMapItem[];
  pmTarget: { Length: number; MSRP: number } | null;
  pmClusterTop3: string[];
  timeRange: TimeRange | null;
  monthGrain: "month" | "quarter" | "year";
}

function getLoadingMetricValue(target: number, tick: number, fallback: number): number {
  const base = target > 0 ? target : fallback;
  const phase = (tick % 18) / 18;
  return Math.round(base * (0.3 + phase * 0.95));
}

function formatMetricValue(value: number): string {
  return Math.max(0, Math.round(value)).toLocaleString();
}

function summarizeScopeValues(values: string[]): string {
  if (values.length === 0) return "-";
  if (values.length <= 2) return values.join(" · ");
  return `${values.slice(0, 2).join(" · ")} +${values.length - 2}`;
}

function getMetricDensityClass(valueText: string): string {
  const digits = valueText.replace(/\D/g, "").length;
  if (digits >= 7) return " metric-value--ultra";
  if (digits >= 6) return " metric-value--compact";
  return "";
}

function getUnifiedMetricDensityClass(values: string[]): string {
  const maxDigits = Math.max(0, ...values.map((value) => value.replace(/\D/g, "").length));
  if (maxDigits >= 7) return " metric-value--ultra";
  if (maxDigits >= 6) return " metric-value--compact";
  return "";
}

/* ── filter component ──────────────────────────────── */
/* ── Main Dashboard ────────────────────────────────── */
export function DashboardPage() {
  const currentSearch = typeof window !== "undefined" ? window.location.search : "";
  const cachedPageRef = useRef<DashboardPageCache | null>(null);
  if (cachedPageRef.current === null) {
    const cached = getCachedPageValue<DashboardPageCache>(DASHBOARD_CACHE_KEY);
    cachedPageRef.current = cached && cached.search === currentSearch ? cached : null;
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
    loading,
    optionsSyncPending,
    error: sharedError,
    activeFilters,
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

  /* time-series controls */
  const [activeTab, setActiveTab] = useState<"year"|"month">(() => cachedPage?.activeTab ?? "year");
  const [chartType, setChartType] = useState<"line"|"bar">(() => cachedPage?.chartType ?? "line");
  const [tsMode, setTsMode] = useState<"\u603b\u548c"|"\u5206\u7ec4">(() => cachedPage?.tsMode ?? "\u603b\u548c");
  const [tsGroupDim, setTsGroupDim] = useState(() => cachedPage?.tsGroupDim ?? "\u52a8\u603b\u89c4\u6574");
  const [tsTopN, setTsTopN] = useState(() => cachedPage?.tsTopN ?? 10);
  const [tsTopNEnabled, setTsTopNEnabled] = useState(() => cachedPage?.tsTopNEnabled ?? true);
  const [tsIncludeOthers, setTsIncludeOthers] = useState(() => cachedPage?.tsIncludeOthers ?? false);
  const [groupedItems, setGroupedItems] = useState<GroupedTimeSeriesItem[]>(() => cachedPage?.groupedItems ?? []);
  const [groupedLoading, setGroupedLoading] = useState(false);
  const [hiddenSeries, setHiddenSeries] = useState<Set<string>>(() => new Set(cachedPage?.hiddenSeries ?? []));
  const [othersDetail, setOthersDetail] = useState<OthersDetailItem[]>(() => cachedPage?.othersDetail ?? []);

  /* advanced charts */
  const [advGroup, setAdvGroup] = useState(() => cachedPage?.advGroup ?? "market_structure");
  const [advChart, setAdvChart] = useState(() => cachedPage?.advChart ?? "powertrain_bubble");
  const [advItems, setAdvItems] = useState<Record<string, string|number>[]>(() => cachedPage?.advItems ?? []);
  const [advMeta, setAdvMeta] = useState<Record<string, unknown> | null>(() => cachedPage?.advMeta ?? null);
  const [advLoading, setAdvLoading] = useState(false);
  const [advBandSize, setAdvBandSize] = useState(() => cachedPage?.advBandSize ?? 1000);
  const [advTopN, setAdvTopN] = useState(() => cachedPage?.advTopN ?? 30);
  const [advMigrationMode, setAdvMigrationMode] = useState<"area"|"line">(() => cachedPage?.advMigrationMode ?? "area");
  const [advBubbleScale, setAdvBubbleScale] = useState(() => cachedPage?.advBubbleScale ?? 2);
  const [advBubbleGrain, setAdvBubbleGrain] = useState<"model"|"version">(() => cachedPage?.advBubbleGrain ?? "model");
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
  const [pmLoading, setPmLoading] = useState(false);

  /* global time axis */
  const [timeRange, setTimeRange] = useState<TimeRange | null>(() => cachedPage?.timeRange ?? null);
  const [monthGrain, setMonthGrain] = useState<"month"|"quarter"|"year">(() => cachedPage?.monthGrain ?? "month");

  /* export settings (one per chart section) */
  const [tsExport, setTsExport] = useState<ExportSettings>({ ...DEFAULT_EXPORT });
  const [advExport, setAdvExport] = useState<ExportSettings>({ ...DEFAULT_EXPORT });
  const [mvExport, setMvExport] = useState<ExportSettings>({ ...DEFAULT_EXPORT });
  const [pmExport, setPmExport] = useState<ExportSettings>({ ...DEFAULT_EXPORT });
  const tsChartRef = useRef<HTMLDivElement | null>(null);
  const advChartRef = useRef<HTMLDivElement | null>(null);
  const mvChartRef = useRef<HTMLDivElement | null>(null);
  const pmChartRef = useRef<HTMLDivElement | null>(null);

  const [error, setError] = useState("");
  const combinedError = sharedError || error;
  const [heroLoadingTick, setHeroLoadingTick] = useState(0);
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
    if (selections.model.length !== 1) return;
    setMvModelName((current) => current || selections.model[0]);
  }, [selections.model]);

  /* B3: auto-reload advanced chart when filters change */
  const prevAdvPayloadRef = useRef(filterPayloadStr);
  useEffect(() => {
    if (optionsSyncPending || prevAdvPayloadRef.current === filterPayloadStr || advItems.length === 0 || columns.length === 0) return;
    prevAdvPayloadRef.current = filterPayloadStr;
    loadAdvChart();
  }, [advItems.length, columns.length, filterPayloadStr, loadAdvChart, optionsSyncPending]);

  /* B12: lazy auto-load — when filteredRowCount < 200 000, auto-trigger first advanced chart */
  const advAutoLoaded = useRef(false);
  useEffect(() => {
    if (advAutoLoaded.current) return;
    if (filteredRowCount !== null && filteredRowCount < 200_000 && columns.length > 0 && advItems.length === 0 && advChart !== "rv_finance_dashboard") {
      advAutoLoaded.current = true;
      loadAdvChart();
    }
  }, [filteredRowCount, columns.length, advItems.length, advChart]);

  /* auto-fetch grouped time series */
  useEffect(() => {
    if (tsMode !== "\u5206\u7ec4" || columns.length === 0) return;
    const filters = JSON.parse(filterPayloadStr) as Record<string, string[]>;
    setGroupedLoading(true);
    let cancelled = false;
    const timer = setTimeout(async () => {
      setError("");
      try {
        const r = await api.groupedTimeSeries({ filters, grain: activeTab, group_by: tsGroupDim, top_n: tsTopNEnabled ? tsTopN : 9999, include_others: tsIncludeOthers });
        if (!cancelled) { setGroupedItems(r.items); setHiddenSeries(new Set()); setOthersDetail(r.others_detail ?? []); }
      } catch (e) { if (!cancelled) setError((e as Error).message); }
      finally { if (!cancelled) setGroupedLoading(false); }
    }, 300);
    return () => { cancelled = true; clearTimeout(timer); setGroupedLoading(false); };
  }, [tsMode, tsGroupDim, activeTab, tsTopN, tsTopNEnabled, tsIncludeOthers, filterPayloadStr, columns.length]);

  /* advanced chart */
  async function loadAdvChart() {
    setAdvLoading(true); setError("");
    try {
      const opts: Record<string, unknown> = { band_size: advBandSize };
      if (advChart === "powertrain_bubble") {
        opts.grain = advBubbleGrain;
        opts.show_yoy = advBubbleShowYoy;
        if (advBubbleShowYoy && advBubbleYoyYear) opts.yoy_compare_year = advBubbleYoyYear;
        opts.group_top_n = advBubbleGroupTopN;
        opts.group_dimension = advBubbleGroupDimension;
        opts.group_values = advBubbleGroupValues;
        opts.group_top_n_map = advBubbleGroupTopNMap;
      }
      if (advChart === "nev_range_distribution" || advChart === "nev_capacity_vs_msrp") {
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
      const r = await api.advancedChart({ group: advGroup, chart: advChart, filters: buildFilterPayload(), top_n: advTopN, options: opts });
      setAdvItems(ensureArray(r.items));
      setAdvMeta(r.meta ?? null);
    } catch (e) { setError((e as Error).message); }
    finally { setAdvLoading(false); }
  }

  /* model version bubble */
  async function loadModelVersions() {
    if (!mvModelName.trim()) return;
    setMvLoading(true); setError("");
    try {
      const r = await api.modelVersions({ filters: buildFilterPayload(), model_name: mvModelName.trim(), top_n: mvTopN });
      setMvItems(ensureArray(r.items));
    } catch (e) { setError((e as Error).message); }
    finally { setMvLoading(false); }
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
    setPmLoading(true); setError("");
    try {
      const r = await api.positioningMap({
        filters: buildFilterPayload(),
        target_length: pmTargetLength ? Number(pmTargetLength) : null,
        target_msrp: pmTargetMsrp ? Number(pmTargetMsrp) : null,
        length_range: pmLengthRange,
        manual_competitors: pmManualCompetitors,
        top_n: pmTopN,
        n_clusters: pmNClusters,
      });
      setPmItems(ensureArray(r.items)); setPmTarget(r.target ?? null); setPmClusterTop3(ensureArray(r.cluster_top3));
    } catch (e) { setError((e as Error).message); }
    finally { setPmLoading(false); }
  }

  /* ── derived chart data ──────────────────────────── */
  const kpis = overview?.kpis;
  const timeWindowLabel = timeRange ? `${timeRange.start} ~ ${timeRange.end}` : "Full timeline";
  const activeLensTokens = useMemo(() => {
    const filterTokens = activeFilters.length === 0
      ? ["Default powertrain lens"]
      : activeFilters.map(({ key, label }) => `${label}: ${summarizeScopeValues(selections[key])}`);
    return [...filterTokens, `Time window: ${timeWindowLabel}`];
  }, [activeFilters, selections, timeWindowLabel]);
  const activeFilterCount = activeFilters.length;
  const isGrouped = tsMode === "\u5206\u7ec4";
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
      { key: "rows", label: "筛选后记录数", value: (kpis?.totalRows ?? 0).toLocaleString() },
      { key: "brands", label: "品牌数", value: (kpis?.brandCount ?? 0).toLocaleString() },
      { key: "models", label: "Model 数", value: (kpis?.modelCount ?? 0).toLocaleString() },
      { key: "versions", label: "Version 数", value: (kpis?.versionCount ?? 0).toLocaleString() },
    ],
    [kpis],
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
    activeTab,
    chartType,
    tsMode,
    tsGroupDim,
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
    timeRange,
    monthGrain,
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
    advBubbleScale,
    advBubbleShowYoy,
    advBubbleYoyYear,
    advChart,
    advGroup,
    advHeatmapScale,
    advItems,
    advMeta,
    advMigrationMode,
    advNevAxisMax,
    advNevFacetBrand,
    advNevMaxBrandFacets,
    advNevMetricMode,
    advNevStackByModel,
    advNevTopNEnabled,
    advPowertrains,
    advRangeStep,
    advTopN,
    chartType,
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
    mvItems,
    mvModelName,
    mvTopN,
    optionsMap,
    othersDetail,
    overview,
    pmClusterTop3,
    pmItems,
    pmLengthRange,
    pmManualCompetitors,
    pmNClusters,
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
    tsGroupDim,
    tsIncludeOthers,
    tsMode,
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
          marker: { color: seriesColor(series, seriesIndex, advPalette, isPowertrainStack) },
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
    advPalette,
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
        summary={activeFilterSummary}
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
          <div className="dashboard-sidebar-caption">{activeFilterSummary}</div>

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
            />
          ))}
      </CollapsibleFilterSidebar>

      <section className="dashboard-main">
        {combinedError && <div className="alert alert-error">{combinedError}</div>}

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
                  <span className="selection-ribbon-value">{activeFilterSummary}</span>
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
                  <span className="hero-meta-subvalue">{activeFilterCount ? `${activeFilterCount} filter dimensions active` : "Default powertrain lens"}</span>
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
              <div className="dashboard-hero-rail-actions">
                <button type="button" className="btn btn-sm btn-secondary" onClick={resetFilters}>
                  Reset filters
                </button>
                <Link className="btn btn-sm btn-primary" to={specificationHref}>
                  Open Specification
                </Link>
              </div>
            </div>
          )}
        />

        {/* ── Global Time Axis ────────────────────────── */}
        <div className="card analysis-deck-card dashboard-time-axis-card dashboard-deck-card--compact-hero">
          <div className="analysis-deck-head dashboard-deck-hero-head">
            <div className="analysis-deck-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">02 / Global Time Axis</span>
              <h3>Global Time Axis</h3>
              <p>统一年度与月度时间窗，控制趋势对比与后续高级分析的观察区间。</p>
              <div className="analysis-chip-row analysis-chip-row--compact">
                <span className="analysis-chip">{activeFilterSummary}</span>
                <span className="analysis-chip">{timeWindowLabel}</span>
              </div>
            </div>
            <div className="analysis-deck-meta dashboard-deck-hero-meta">
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Axis Mode</span>
                <strong className="analysis-deck-stat-value">{timeAxisModeValue}</strong>
                <span className="analysis-deck-stat-subvalue">{timeAxisModeDetail}</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Window State</span>
                <strong className="analysis-deck-stat-value">{timeWindowStateValue}</strong>
                <span className="analysis-deck-stat-subvalue">{timeWindowLabel}</span>
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
        <div className="card analysis-deck-card chart-section dashboard-time-series-card dashboard-deck-card--compact-hero">
          <div className="analysis-deck-head dashboard-deck-hero-head">
            <div className="analysis-deck-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">03 / Time-Series Lens</span>
              <h3>Sales Time Series</h3>
              <p>在同一筛选边界下切换年度、月度与分组序列，保持趋势分析和图例交互语义一致。</p>
              <div className="analysis-chip-row analysis-chip-row--compact">
                <span className="analysis-chip">{activeFilterSummary}</span>
                <span className="analysis-chip">{timeWindowLabel}</span>
              </div>
            </div>
            <div className="analysis-deck-meta dashboard-deck-hero-meta">
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">View Mode</span>
                <strong className="analysis-deck-stat-value">{activeTab === "year" ? "YEAR" : "MONTH"}</strong>
                <span className="analysis-deck-stat-subvalue">{activeTab === "month" ? `聚合：${monthGrain}` : "年度对比"}</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Series Mode</span>
                <strong className="analysis-deck-stat-value">{isGrouped ? "GROUPED" : "TOTAL"}</strong>
                <span className="analysis-deck-stat-subvalue">{isGrouped ? tsGroupDim : "单序列汇总视图"}</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Data State</span>
                <strong className="analysis-deck-stat-value">{timeSeriesDeckState}</strong>
                <span className="analysis-deck-stat-subvalue">{isGrouped ? `${filteredGrouped.length} 个分组点` : `${aggregatedSingle.length} 个时间点`}</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Visible Series</span>
                <strong className="analysis-deck-stat-value">{isGrouped ? timeSeriesDeckVolume : "1"}</strong>
                <span className="analysis-deck-stat-subvalue">{isGrouped ? `可见 ${visibleSeries.length} / ${allSeriesNames.length}` : "单序列展示"}</span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact dashboard-deck-hero-surface">
          <div className="chart-header">
            <div className="tab-bar">
              <button className={"tab-btn"+(activeTab==="year"?" active":"")} onClick={()=>setActiveTab("year")}>{"\u5e74\u5ea6\u5bf9\u6bd4"}</button>
              <button className={"tab-btn"+(activeTab==="month"?" active":"")} onClick={()=>setActiveTab("month")}>{"\u6708\u5ea6\u660e\u7ec6"}</button>
            </div>
            <div className="chart-controls">
              <div className="tab-bar">
                <button className={"tab-btn"+(tsMode==="\u603b\u548c"?" active":"")} onClick={()=>setTsMode("\u603b\u548c")}>{"\u603b\u548c"}</button>
                <button className={"tab-btn"+(tsMode==="\u5206\u7ec4"?" active":"")} onClick={()=>setTsMode("\u5206\u7ec4")}>{"\u5206\u7ec4"}</button>
              </div>
              <span className="chart-controls-sep" />
              <label className="chart-mode-label"><input type="radio" name="chartType" value="line" checked={chartType==="line"} onChange={()=>setChartType("line")} />{" \u6298\u7ebf"}</label>
              <label className="chart-mode-label"><input type="radio" name="chartType" value="bar" checked={chartType==="bar"} onChange={()=>setChartType("bar")} />{" \u7d2f\u79ef\u6761\u5f62"}</label>
            </div>
          </div>

          {/* grouped controls */}
          {isGrouped && (
            <div className="ts-group-bar">
              <div className="filter-group"><label>{"\u5206\u7ec4\u7ef4\u5ea6"}</label>
                <select value={tsGroupDim} onChange={e=>setTsGroupDim(e.target.value)}>
                  {GROUP_BY_OPTIONS.map(o=><option key={o.v} value={o.v}>{o.l}</option>)}
                </select>
              </div>
              <label className="chart-mode-label" style={{gap:6}}>
                <input type="checkbox" checked={tsTopNEnabled} onChange={e=>setTsTopNEnabled(e.target.checked)} />
                {"\u542f\u7528 Top N"}
              </label>
              {tsTopNEnabled && <div className="filter-group"><label>Top N</label>
                <input type="number" value={tsTopN} min={3} max={30} style={{width:56}} onChange={e=>setTsTopN(Math.max(3,Math.min(30,Number(e.target.value)||10)))} />
              </div>}
              <label className="chart-mode-label" style={{gap:6}}>
                <input type="checkbox" checked={tsIncludeOthers} onChange={e=>setTsIncludeOthers(e.target.checked)} />
                {"\u56fe\u4e2d\u663e\u793a\u201c\u5176\u4ed6\u201d"}
              </label>
              {groupedLoading && (
                <LoadingSurface
                  mode="inline"
                  label="正在刷新分组序列"
                  detail={tsTopNEnabled ? `${tsGroupDim} · Top ${tsTopN}` : tsGroupDim}
                />
              )}
            </div>
          )}
          {!isGrouped && <div className="ts-mode-hint">{"\u5207\u6362\u5230\u201c\u5206\u7ec4\u201d\u540e\uff0c\u53ef\u6309\u52a8\u603b/\u7ec6\u5206/\u54c1\u724c/Model \u5206\u8272\u663e\u793a\u3002"}</div>}

          {/* series pills (click to toggle visibility) */}
          {isGrouped && allSeriesNames.length > 0 && (() => {
            const isPt = tsGroupDim === "\u52a8\u603b\u89c4\u6574";
            return (
            <div className="ts-series-pills">
              {allSeriesNames.map((name, i) => (
                <button key={name} className={"ts-pill"+(hiddenSeries.has(name)?" ts-pill-hidden":"")}
                  style={{"--pill-color": seriesColor(name, i, COLORS, isPt)} as React.CSSProperties}
                  onClick={()=>setHiddenSeries(prev=>{const n=new Set(prev);n.has(name)?n.delete(name):n.add(name);return n;})}>
                  <span className="ts-pill-dot" />{name}
                </button>
              ))}
              <span className="ts-series-count">{visibleSeries.length+" / "+allSeriesNames.length+" \u7cfb\u5217"}</span>
            </div>
            );
          })()}

          {/* single-series */}
          {!isGrouped && aggregatedSingle.length > 0 && (
            <div ref={el => { tsChartRef.current = el; }}>
              <PlotlyChart
                data={applyDataLabelsToTraces([chartType === "line" ? {
                  x: aggregatedSingle.map(s => s.time),
                  y: aggregatedSingle.map(s => s.value),
                  type: "scatter", mode: "lines+markers", name: "Sales",
                  line: { color: tsPalette[0], width: 2 },
                  marker: { size: 5 },
                } as Data : {
                  x: aggregatedSingle.map(s => s.time),
                  y: aggregatedSingle.map(s => s.value),
                  type: "bar", name: "Sales",
                  marker: { color: tsPalette[0] },
                } as Data], tsExport)}
                layout={applyExportToLayout({
                  xaxis: buildCategoryAxis(singleTimeLabels, { tickangle: -45 }),
                  yaxis: { title: { text: "Sales" } },
                }, tsExport)}
                height={400}
              />
            </div>
          )}

          {/* multi-series grouped */}
          {isGrouped && filteredGrouped.length > 0 && (() => {
            const isPt = tsGroupDim === "\u52a8\u603b\u89c4\u6574";
            let traces: Data[] = visibleSeries.map((name, i) => {
              const seriesData = filteredGrouped.filter(g => g.series === name);
              const c = seriesColor(name, i, tsPalette, isPt);
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
            traces = applySeriesColors(applyDataLabelsToTraces(traces, tsExport), tsExport.seriesColors);
            return (
              <div ref={el => { tsChartRef.current = el; }}>
                <PlotlyChart
                  data={traces}
                  layout={applyExportToLayout({
                    barmode: chartType === "bar" ? "relative" : undefined,
                    xaxis: buildCategoryAxis(groupedTimeLabels, { tickangle: -45 }),
                    yaxis: { title: { text: "Sales" } },
                  }, tsExport)}
                  height={450}
                />
              </div>
            );
          })()}

          {!isGrouped && aggregatedSingle.length===0 && !loading && <div className="chart-empty">{"\u6682\u65e0\u8d8b\u52bf\u6570\u636e"}</div>}
          {isGrouped && filteredGrouped.length===0 && !groupedLoading && <div className="chart-empty">{"\u5207\u6362\u5206\u7ec4\u7ef4\u5ea6\u6216\u8c03\u6574\u7b5b\u9009\u6761\u4ef6"}</div>}

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

          <ExportPanel value={tsExport} onChange={setTsExport} graphDiv={tsChartRef.current} seriesNames={isGrouped ? visibleSeries : undefined} labelModeOptions={tsLabelModeOptions} />
          </div>
        </div>

        {/* ── Advanced analysis ───────────────────────── */}
        <div className="card analysis-deck-card dashboard-advanced-card dashboard-deck-card--compact-hero">
          <div className="analysis-deck-head dashboard-deck-hero-head">
            <div className="analysis-deck-copy dashboard-deck-hero-copy">
              <span className="panel-kicker">04 / Advanced Analysis</span>
              <h3>Advanced Control Deck</h3>
              <p>在同一筛选与时间窗口下切换分析域、图层和参数，承接主看板的深度分析与嵌入式 RV Finance 视图。</p>
              <div className="analysis-chip-row analysis-chip-row--compact">
                <span className="analysis-chip">{activeFilterSummary}</span>
                <span className="analysis-chip">{timeWindowLabel}</span>
              </div>
            </div>
            <div className="analysis-deck-meta dashboard-deck-hero-meta">
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Analysis Domain</span>
                <strong className="analysis-deck-stat-value">{selectedAdvGroupLabel || "-"}</strong>
                <span className="analysis-deck-stat-subvalue">当前分析域</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Chart Layer</span>
                <strong className="analysis-deck-stat-value">{selectedAdvChartLabel || "-"}</strong>
                <span className="analysis-deck-stat-subvalue">当前图层</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Data State</span>
                <strong className="analysis-deck-stat-value">{advancedDeckState}</strong>
                <span className="analysis-deck-stat-subvalue">{advancedDeckVolume}</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Active Filters</span>
                <strong className="analysis-deck-stat-value">{String(activeFilters.length)}</strong>
                <span className="analysis-deck-stat-subvalue">{activeFilters.length ? activeFilterSummary : "Default powertrain lens"}</span>
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
            <div className="adv-console-path">
              <span>BMW Control Deck</span>
              <strong>{ADV_GROUPS.find(g=>g.v===advGroup)?.l??""}</strong>
              <span>/</span>
              <strong>{chartOpts.find(c=>c.v===advChart)?.l??""}</strong>
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
                    <select value={advBubbleGrain} onChange={e=>{setAdvBubbleGrain(e.target.value as "model"|"version"); setAdvItems([]); setAdvMeta(null);}}>
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
            {(advChart==="nev_range_distribution"||advChart==="nev_capacity_vs_msrp") && (
              <div className="filter-group adv-control-unit adv-control-unit--wide"><label>{"\u52a8\u603b\u7c7b\u578b"}</label>
                <div className="adv-powertrain-strip">
                  {["BEV","PHEV","HEV","MHEV","ICE"].map(pt=>(
                    <label key={pt} className={"adv-powertrain-chip"+(advPowertrains.includes(pt)?" is-active":"")}>
                      <input type="checkbox" checked={advPowertrains.includes(pt)}
                        onChange={e=>{const next=e.target.checked?[...advPowertrains,pt]:advPowertrains.filter(x=>x!==pt);setAdvPowertrains(next);}} />
                      <span className="adv-powertrain-chip-swatch" style={{"--pt-color": POWERTRAIN_COLORS[pt]} as React.CSSProperties} />
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
            {(advChart==="nev_range_distribution"||advChart==="nev_capacity_vs_msrp") && (
              <button className="btn btn-sm btn-secondary" onClick={()=>{setAdvPowertrains(["BEV","PHEV"]);setAdvTopN(advChart==="nev_range_distribution"?80:120);setAdvRangeStep(50);setAdvNevTopNEnabled(true);setAdvNevAxisMax(1000);setAdvNevMetricMode("window_sales");setAdvNevStackByModel(false);setAdvNevFacetBrand(false);setAdvNevMaxBrandFacets(4);}}>{"\u91cd\u7f6e\u53c2\u6570"}</button>
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
            <LoadingActionButton loading={advLoading} loadingLabel="加载中…" disabled={!columns.length} onClick={loadAdvChart}>加载图表</LoadingActionButton>
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
              {advItems.map(row=>{
                const lb=String(row.label??"-"); const val=Number(row.value??0);
                const pct=Math.max(1,Math.round((val/maxBar)*100));
                return (<div className="bar-row" key={lb+"-"+val}>
                  <span className="bar-label">{lb}</span>
                  <div className="bar-track"><div className="bar-fill" style={{width:pct+"%"}} /></div>
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

            function buildTraces(items: Record<string, string|number>[]) {
              const localCats = [...new Set(items.map(r=>String(r[ax.color]??"")))];
              return localCats.map((cat, i) => {
                const subset = items.filter(r=>String(r[ax.color]??"")=== cat);
                const isBubbleMsrp = advChart === "powertrain_bubble";
                const bubbleYoyTemplate = isBubbleMsrp && bubbleYoyEnabled && bubbleYoyCompareYear && bubbleYoyBaseYear
                  ? `<br>${bubbleYoyBaseYear} Sales: %{customdata[4]:,.0f}<br>${bubbleYoyCompareYear} Sales: %{customdata[5]:,.0f}<br>YoY: %{customdata[6]:+.1f}%`
                  : "";
                return withExportLabels({
                  x: subset.map(r => Number(r[ax.x] ?? 0)),
                  y: subset.map(r => Number(r[ax.y] ?? 0)),
                  text: subset.map(r => String(r.DisplayName ?? r.Version ?? r.Model ?? r.Brand ?? "")),
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
                    color: seriesColor(cat, i, advPalette, isPtScatter),
                    size: subset.map(r => Math.max(0, Number(r[ax.z] ?? 0))),
                    sizemode: scatterBubbleSizing.sizemode,
                    sizeref: scatterBubbleSizing.sizeref,
                    sizemin: scatterBubbleSizing.sizemin,
                    opacity: 0.7,
                  },
                  hovertemplate: isBubbleMsrp
                    ? advBubbleGrain === "version"
                      ? "%{text}<br>" + ax.xLabel + ": %{x:,.0f}<br>MSRP（组内中位数）: %{y:,.0f}<br>MSRP范围: %{customdata[1]:,.0f} - %{customdata[2]:,.0f}<br>Sales: %{customdata[0]:,.0f}" + bubbleYoyTemplate + "<extra>%{fullData.name}</extra>"
                      : "%{text}<br>" + ax.xLabel + ": %{x:,.0f}<br>MSRP（组内中位数）: %{y:,.0f}<br>MSRP范围: %{customdata[1]:,.0f} - %{customdata[2]:,.0f}<br>聚合版型数: %{customdata[3]:,.0f}<br>Sales: %{customdata[0]:,.0f}" + bubbleYoyTemplate + "<extra>%{fullData.name}</extra>"
                    : "%{text}<br>" + ax.xLabel + ": %{x:,.0f}<br>" + ax.yLabel + ": %{y:,.0f}<br>Sales: %{customdata[0]:,.0f}<extra>%{fullData.name}</extra>",
                } as Data, {
                  ...(subset.some(r => String(r.Model ?? "").trim()) ? { model: subset.map(r => String(r.Model ?? "")) } : {}),
                  ...(subset.some(r => r.Sales !== undefined) ? { sales: subset.map(r => Number(r.Sales ?? 0)) } : {}),
                  value: subset.map(r => Number(r[ax.y] ?? 0)),
                  series: subset.map(() => cat),
                }) as Data;
              });
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
                          data={applySeriesColors(applyDataLabelsToTraces(traces, advExport), advExport.seriesColors)}
                          layout={applyExportToLayout({
                            xaxis: { title: { text: ax.xLabel } },
                            yaxis: { title: { text: ax.yLabel } },
                            showlegend: false,
                            margin: { t: 18, b: 40, l: 50, r: 10 },
                          }, advExport)}
                          height={320}
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
                  data={applySeriesColors(applyDataLabelsToTraces(traces, advExport), advExport.seriesColors)}
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
                  height={500}
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
                {nevWarnings.map((warning, idx) => (
                  <div key={warning+idx} className="alert alert-info">{warning}</div>
                ))}

                {nevFacetPlot.traces.length > 0 && (
                  <div ref={el => { advChartRef.current = el; }} style={{marginBottom:12}}>
                    <PlotlyChart
                      data={applySeriesColors(
                        applyDataLabelsToTraces(nevFacetPlot.traces, advExport),
                        advExport.seriesColors,
                      )}
                      layout={applyExportToLayout(nevFacetPlot.layout, advExport)}
                      height={nevFacetPlot.height}
                    />
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
                  <div className="alert alert-warning">
                    {`Top${nevTopModelLimit} |净变化|集中度 ${(nevTopModelAbsShare * 100).toFixed(1)}% >= 70%，结构风险较高，建议关注头部车型波动。`}
                  </div>
                )}

                {nevMetricMode === "net_change" && offsetRatio >= 0.85 && (
                  <div className="alert alert-info">{"对冲率较高：净增背后存在较强的车型结构迁移，建议结合分桶与 Top 车型明细一起看。"}</div>
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
              marker: { color: seriesColor(k, i, advPalette, isPtStack) },
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
                  height={450}
                />
              </div>
            );
          })()}

          {/* price migration area/line */}
          {advChart==="price_migration" && migrationData.length > 0 && (() => {
            const isArea = advMigrationMode === "area";
            const traces: Data[] = migrationYears.map((yr, i) => ({
              x: migrationData.map(r => r.priceBand as number),
              y: migrationData.map(r => Number(r[yr] ?? 0)),
              type: "scatter" as const,
              mode: "lines" as const,
              ...(isArea ? { fill: "tozeroy" as const, fillcolor: advPalette[i % advPalette.length] + "26" } : {}),
              name: yr,
              line: { color: advPalette[i % advPalette.length], width: 2 },
            }));
            return (
              <div ref={el => { advChartRef.current = el; }}>
                <PlotlyChart
                  data={applyDataLabelsToTraces(traces, advExport)}
                  layout={applyExportToLayout({
                    xaxis: { title: { text: "\u4ef7\u683c\u5e26" }, tickangle: -45 },
                    yaxis: { title: { text: "Sales" } },
                  }, advExport)}
                  height={450}
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
                  height={400}
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
          <ExportPanel value={advExport} onChange={setAdvExport} graphDiv={advChartRef.current} labelModeOptions={advLabelModeOptions} />
          </div>
        </div>

        {/* ── Bug 2: Model Version Bubble ─────────────── */}
        <div className="card analysis-deck-card">
          <div className="analysis-deck-head">
            <div className="analysis-deck-copy">
              <span className="panel-kicker">05 / Single Model Lens</span>
              <h3>Model Version Bubble</h3>
              <p>锁定单一 Model，查看版本在车长与 MSRP 平面上的分布，并沿用当前 Dashboard 的筛选范围。</p>
              <div className="analysis-chip-row">
                <span className="analysis-chip">{activeFilterSummary}</span>
                <span className="analysis-chip">{mvColorBy === "Powertrain" ? "Color by powertrain" : "Color by trim"}</span>
              </div>
            </div>
            <div className="analysis-deck-meta">
              <div className={`analysis-deck-stat${mvLoading ? " is-loading" : ""}`}>
                <span className="analysis-deck-stat-label">Data state</span>
                <strong className="analysis-deck-stat-value">{mvLoading ? "SYNC" : mvItems.length ? "READY" : "IDLE"}</strong>
                <span className="analysis-deck-stat-subvalue">{mvItems.length ? `${mvItems.length} 个版本` : "等待加载版型"}</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Quick picks</span>
                <strong className="analysis-deck-stat-value">{String(selections.model.length).padStart(2, "0")}</strong>
                <span className="analysis-deck-stat-subvalue">来自共享筛选</span>
              </div>
            </div>
          </div>
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
            <LoadingActionButton loading={mvLoading} loadingLabel="加载中…" disabled={!mvModelName.trim()} onClick={loadModelVersions}>
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
                  color: seriesColor(cat, i, mvPalette, isPtBubble),
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
                  data={traces}
                  layout={applyExportToLayout({
                    xaxis: { title: { text: "车长(mm)" } },
                    yaxis: { title: { text: "MSRP" } },
                  }, mvExport)}
                  height={500}
                />
              </div>
            );
          })()}
          {mvItems.length===0 && !mvLoading && <div className="chart-empty">{"\u8f93\u5165 Model \u540d\u79f0\u5e76\u70b9\u51fb\u300c\u52a0\u8f7d\u7248\u578b\u300d"}</div>}
          <ExportPanel value={mvExport} onChange={setMvExport} graphDiv={mvChartRef.current} labelModeOptions={mvLabelModeOptions} />
        </div>

        {/* ── Bug 3: OJ Positioning Map ───────────────── */}
        <div className="card analysis-deck-card">
          <div className="analysis-deck-head">
            <div className="analysis-deck-copy">
              <span className="panel-kicker">06 / Competitive Positioning</span>
              <h3>OJ Positioning Map</h3>
              <p>基于当前筛选边界生成竞品聚类，并支持叠加手动竞品与目标车型坐标，保持与主分析区一致的控件语言。</p>
              <div className="analysis-chip-row">
                <span className="analysis-chip">{activeFilterSummary}</span>
                <span className="analysis-chip">Target ready {pmTarget ? "YES" : "NO"}</span>
              </div>
            </div>
            <div className="analysis-deck-meta">
              <div className={`analysis-deck-stat${pmLoading ? " is-loading" : ""}`}>
                <span className="analysis-deck-stat-label">Map state</span>
                <strong className="analysis-deck-stat-value">{pmLoading ? "SYNC" : pmItems.length ? "READY" : "IDLE"}</strong>
                <span className="analysis-deck-stat-subvalue">{pmItems.length ? `${pmItems.length} 个候选点` : "等待加载定位图"}</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Manual rivals</span>
                <strong className="analysis-deck-stat-value">{String(pmManualCompetitors.length).padStart(2, "0")}</strong>
                <span className="analysis-deck-stat-subvalue">{pmClusterTop3.length ? `${pmClusterTop3.length} 个Top3标签` : "尚未生成聚类代表"}</span>
              </div>
            </div>
          </div>
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
            <LoadingActionButton loading={pmLoading} loadingLabel="加载中…" onClick={loadPositioningMap}>
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
                  color: pmPalette[i % pmPalette.length],
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
                  data={traces}
                  layout={applyExportToLayout({
                    xaxis: { title: { text: "\u8f66\u957f(mm)" } },
                    yaxis: { title: { text: "MSRP" } },
                  }, pmExport)}
                  height={520}
                />
              </div>
            );
          })()}
          {pmItems.length===0 && !pmLoading && <div className="chart-empty">{"\u8f93\u5165\u76ee\u6807\u8f66\u578b\u53c2\u6570\u6216\u76f4\u63a5\u70b9\u51fb\u300c\u52a0\u8f7d\u5b9a\u4f4d\u56fe\u300d\u67e5\u770b\u5f53\u524d\u7b5b\u9009\u8fb9\u754c\u5185\u5b9a\u4ef7"}</div>}
          <ExportPanel value={pmExport} onChange={setPmExport} graphDiv={pmChartRef.current} labelModeOptions={pmLabelModeOptions} />
        </div>

        <div className="card analysis-deck-card analysis-route-card">
          <div className="analysis-deck-head">
            <div className="analysis-deck-copy">
              <span className="panel-kicker">07 / Specification Route</span>
              <h3>Specification Entry</h3>
              <p>明细表、列选择、分页和 CSV 导出已经迁到独立 Specification Page，Dashboard 只保留 KPI 与图表交互。</p>
              <div className="analysis-chip-row">
                <span className="analysis-chip">{activeFilterSummary}</span>
                <span className="analysis-chip">React Router /specification</span>
              </div>
            </div>
            <div className="analysis-deck-meta">
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Route State</span>
                <strong className="analysis-deck-stat-value">READY</strong>
                <span className="analysis-deck-stat-subvalue">与 Dashboard 共享筛选 query</span>
              </div>
              <div className="analysis-deck-stat">
                <span className="analysis-deck-stat-label">Active Filters</span>
                <strong className="analysis-deck-stat-value">{String(activeFilters.length)}</strong>
                <span className="analysis-deck-stat-subvalue">{activeFilters.length ? "带当前筛选进入" : "使用默认筛选进入"}</span>
              </div>
            </div>
          </div>
          <div className="analysis-chart-block analysis-chart-block--compact">
            <div className="dashboard-cta-row">
              <Link className="btn btn-primary" to={specificationHref}>{"\u6253\u5f00 Specification Page"}</Link>
              <Link className="btn btn-ghost" to={specificationHref}>{"\u5e26\u5f53\u524d\u7b5b\u9009\u8fdb\u5165"}</Link>
            </div>
          </div>
        </div>
      </section>
    </div>
  );
}
