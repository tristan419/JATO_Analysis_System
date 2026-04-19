/**
 * Pure helper functions, constants, and type definitions extracted from DashboardPage.
 *
 * Keeps the main DashboardPage component focused on rendering logic.
 */

import type { TimeRange } from "../components/TimeAxis";
import type { ExportSettings } from "../components/ExportPanel";
import type { FilterKey } from "../dashboardFilters";
import type {
  OverviewResponse,
  TimeSeriesPoint,
  GroupedTimeSeriesItem,
  ModelVersionItem,
  PositioningMapItem,
  PositioningPeerCorridor,
  OthersDetailItem,
} from "../types";

/* ── types ──────────────────────────────────────────── */

export type BubbleGroupDimension = "segment" | "powertrain";

export interface DashboardPageCache {
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
  pmPeerCorridor: PositioningPeerCorridor | null;
  timeRange: TimeRange | null;
  monthGrain: "month" | "quarter" | "year";
}

/* ── constants ──────────────────────────────────────── */

export const DASHBOARD_CACHE_KEY = "dashboard-page";
export const PAGE_CACHE_TTL_MS = 30 * 60 * 1000;

export const ADV_GROUPS: { v: string; l: string }[] = [
  { v: "market_structure", l: "市场结构" },
  { v: "nev_analysis", l: "NEV分析" },
  { v: "price_value", l: "价格价值" },
  { v: "cost_analysis", l: "动力成本" },
];

export const ADV_CHARTS: Record<string, { v: string; l: string }[]> = {
  market_structure: [
    { v: "powertrain_bubble", l: "动力气泡图" },
    { v: "seasonality_heatmap", l: "季节性热力图" },
    { v: "segment_share_by_length", l: "车长×细分市场" },
  ],
  nev_analysis: [
    { v: "nev_range_distribution", l: "续航分布" },
    { v: "nev_capacity_vs_msrp", l: "电池容量 vs MSRP" },
  ],
  price_value: [
    { v: "price_migration", l: "价格迁移" },
    { v: "length_vs_price", l: "车长 vs 价格" },
    { v: "price_per_meter", l: "每米价格" },
    { v: "sales_vs_price", l: "销量 vs 价格" },
  ],
  cost_analysis: [
    { v: "rv_finance_dashboard", l: "RV金融杠杆看板" },
    { v: "estimated_tco", l: "估算TCO vs MSRP" },
    { v: "powertrain_vs_price", l: "动力×价格带" },
  ],
};

export const GROUP_BY_OPTIONS = [
  { v: "动总规整", l: "动力总成" },
  { v: "细分市场（按车长）", l: "细分市场" },
  { v: "Make", l: "品牌" },
  { v: "Model", l: "Model" },
  { v: "Version name", l: "Version" },
  { v: "国家", l: "国家" },
];

export const BUBBLE_GROUP_DIMENSIONS: { v: BubbleGroupDimension; l: string; dataKey: "Segment" | "Powertrain" }[] = [
  { v: "segment", l: "细分市场", dataKey: "Segment" },
  { v: "powertrain", l: "动总规整", dataKey: "Powertrain" },
];

export const SCATTER_CHARTS = new Set([
  "powertrain_bubble", "nev_capacity_vs_msrp",
  "length_vs_price", "price_per_meter", "sales_vs_price", "estimated_tco",
]);

export const STACKED_CHARTS = new Set([
  "segment_share_by_length", "nev_range_distribution", "powertrain_vs_price",
]);

/* ── pure helper functions ──────────────────────────── */

export function ensureArray<T>(value: T[] | null | undefined): T[] {
  return Array.isArray(value) ? value : [];
}

export function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function asMetaNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string" && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

export function asMetaText(value: unknown): string {
  return typeof value === "string" ? value : "";
}

export function asMetaStringArray(value: unknown): string[] {
  return Array.isArray(value) ? value.map((item) => String(item)).filter(Boolean) : [];
}

export function asMetaRecordArray(value: unknown): Record<string, unknown>[] {
  return Array.isArray(value) ? value.filter(isPlainRecord) : [];
}

export function getLoadingMetricValue(target: number, tick: number, fallback: number): number {
  const base = target > 0 ? target : fallback;
  const phase = (tick % 18) / 18;
  return Math.round(base * (0.3 + phase * 0.95));
}

export function formatMetricValue(value: number): string {
  return Math.max(0, Math.round(value)).toLocaleString();
}

export function isDashboardBootstrapping(
  filtersReady: boolean,
  loading: boolean,
  overview: OverviewResponse | null,
): boolean {
  return !filtersReady && loading && overview === null;
}

export function formatDashboardSummaryMetric(
  value: number | null | undefined,
  bootstrapping: boolean,
): string {
  if (bootstrapping) return "...";
  return Math.max(0, Math.round(Number(value ?? 0))).toLocaleString();
}

export function getDashboardLensSummary(
  activeFilterSummary: string,
  activeFilterCount: number,
  bootstrapping: boolean,
): string {
  if (bootstrapping && activeFilterCount === 0) {
    return "Loading default powertrain lens...";
  }
  return activeFilterSummary;
}

export function summarizeScopeValues(values: string[]): string {
  if (values.length === 0) return "-";
  if (values.length <= 2) return values.join(" · ");
  return `${values.slice(0, 2).join(" · ")} +${values.length - 2}`;
}

export function getMetricDensityClass(valueText: string): string {
  const digits = valueText.replace(/\D/g, "").length;
  if (digits >= 7) return " metric-value--ultra";
  if (digits >= 6) return " metric-value--compact";
  return "";
}

export function getUnifiedMetricDensityClass(values: string[]): string {
  const maxDigits = Math.max(0, ...values.map((value) => value.replace(/\D/g, "").length));
  if (maxDigits >= 7) return " metric-value--ultra";
  if (maxDigits >= 6) return " metric-value--compact";
  return "";
}
