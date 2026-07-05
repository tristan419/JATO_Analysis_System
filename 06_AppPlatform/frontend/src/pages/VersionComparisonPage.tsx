import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout, PlotMouseEvent, PlotSelectionEvent } from "plotly.js";

import { api } from "../api/client";
import {
  DebouncedNumberInput,
  DeckControlTabs,
  DeckExportDrawer,
  DeckFloatingDrawer,
  type DeckControlTabItem,
} from "../components/deckControls";
import {
  DEFAULT_EXPORT,
  ExportPanel,
  applyDataLabelsToTraces,
  applyExportToLayout,
  applySeriesColors,
  buildExportLabelModeOptions,
  getExportPalette,
  withExportLabels,
  type ExportSettings,
} from "../components/ExportPanel";
import { DeckPeriodTimeline } from "../components/DeckPeriodTimeline";
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import { PageBannerStack, PageLoadingShell } from "../components/PageFeedback";
import type {
  MarketScanPeriodRange,
  PositioningPricingMetric,
  PositioningPricingPriceBandItem,
  PositioningPricingSalesMode,
  VersionComparisonBubbleItem,
  VersionComparisonDeckResponse,
  VersionComparisonMode,
  VersionComparisonModelOption,
} from "../types";
import { buildBubbleSizing } from "../utils/bubbleSizing";
import { fuelColor } from "../utils/colors";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";
import { compactSearchText, optionMatchesCompactSearch } from "../utils/searchMatching";
import { useArrowCountryNavigation } from "../utils/useArrowCountryNavigation";
import { useFixedCanvasPreview } from "../utils/useFixedCanvasPreview";
import { useDeckLayoutControls, type DeckLayoutDirection } from "../hooks/useDeckLayoutControls";
import { useFuelChipClick } from "../hooks/useFuelChipClick";
import { useResolvedCountry } from "../hooks/useResolvedCountry";

const DEFAULT_FUEL_TYPES = ["BEV", "HEV", "PHEV", "MHEV", "ICE"];
const DEFAULT_SALES_MODE: PositioningPricingSalesMode = "month";
const DEFAULT_PRICE_BAND_SIZE = 1000;
const MIN_PRICE_BAND_SIZE = 500;
const MAX_PRICE_BAND_SIZE = 200000;
const MAX_MSRP_INPUT = 1000000;
const DEFAULT_EXPORT_PRESET = "fhd";
const MAX_SELECTED_MODELS = 10;
const SALES_MODE_OPTIONS: Array<{ value: PositioningPricingSalesMode; label: string }> = [
  { value: "month", label: "当月" },
  { value: "ytd", label: "YTD" },
  { value: "rolling12", label: "近12个月" },
];
const COMPARISON_MODE_OPTIONS: Array<{ value: VersionComparisonMode; label: string }> = [
  { value: "same_segment", label: "同级别对比" },
  { value: "free_comparison", label: "自由对比" },
];

type LabelMode = "clean" | "smart_top" | "selected" | "all";
const LABEL_MODE_OPTIONS: Array<{ value: LabelMode; label: string; hint: string }> = [
  { value: "smart_top", label: "Smart Top", hint: "重点版本" },
  { value: "clean", label: "Clean", hint: "仅Model" },
  { value: "selected", label: "Selected", hint: "已选版本" },
  { value: "all", label: "All", hint: "全部版本" },
];

function isLabelMode(value: string | null): value is LabelMode {
  return LABEL_MODE_OPTIONS.some((item) => item.value === value);
}

type VersionComparisonControlPanel = "filters" | "range" | "layout";
type VersionComparisonExportPanel = "priceBands" | "bubble";

const VC_CONTROL_TABS: Array<DeckControlTabItem<VersionComparisonControlPanel>> = [
  { key: "filters", label: "筛选", caption: "模式 / 国家 / 车型" },
  { key: "range", label: "范围", caption: "MSRP / 步长 / 动力" },
  { key: "layout", label: "版式", caption: "布局 / 比例 / 高度" },
];

const VC_EXPORT_TABS: Array<DeckControlTabItem<VersionComparisonExportPanel>> = [
  { key: "priceBands", label: "Price Bands", caption: "累计价格带" },
  { key: "bubble", label: "Powertrain Bubble", caption: "动力气泡图" },
];

const DEFAULT_VC_LAYOUT_DIRECTION: DeckLayoutDirection = "row";
const DEFAULT_VC_SPLIT_RATIO = 20;
const DEFAULT_VC_CHART_HEIGHT = 430;
const MIN_VC_SPLIT_RATIO = 1;
const MAX_VC_SPLIT_RATIO = 99;
const MIN_VC_CHART_HEIGHT = 280;
const MAX_VC_CHART_HEIGHT = 800;
const VC_LAYOUT_DIRECTION_STORAGE_KEY = "vc_layout_dir";
const VC_SPLIT_RATIO_STORAGE_KEY = "vc_layout_split_v2";
const VC_CHART_HEIGHT_STORAGE_KEY = "vc_layout_height";
const VC_LAYOUT_STORAGE_KEYS = {
  direction: VC_LAYOUT_DIRECTION_STORAGE_KEY,
  splitRatio: VC_SPLIT_RATIO_STORAGE_KEY,
  chartHeight: VC_CHART_HEIGHT_STORAGE_KEY,
} as const;
const VC_LAYOUT_DEFAULTS = {
  direction: DEFAULT_VC_LAYOUT_DIRECTION,
  splitRatio: DEFAULT_VC_SPLIT_RATIO,
  chartHeight: DEFAULT_VC_CHART_HEIGHT,
} as const;
const VC_LAYOUT_RANGES = {
  splitRatio: { min: MIN_VC_SPLIT_RATIO, max: MAX_VC_SPLIT_RATIO },
  chartHeight: { min: MIN_VC_CHART_HEIGHT, max: MAX_VC_CHART_HEIGHT },
} as const;
const VC_LAYOUT_CSS_VARIABLES = {
  chartHeight: "--vc-chart-height",
  splitRatio: "--vc-split-ratio",
  remainderRatio: "--vc-remainder-ratio",
} as const;

const DEFAULT_VC_PRICE_BAND_EXPORT: ExportSettings = {
  ...DEFAULT_EXPORT,
  showXGrid: false,
  showYGrid: false,
  showAxisLine: false,
  legendPosition: "right",
  fontSize: 11,
  labelFontSize: 9,
  xTickFormat: "d",
  yTickFormat: "d",
  exportWidth: 960,
  exportHeight: 430,
  decimalPlaces: 0,
  dataLabelMode: "off",
};

const DEFAULT_VC_BUBBLE_EXPORT: ExportSettings = {
  ...DEFAULT_VC_PRICE_BAND_EXPORT,
  dataLabelMode: "off",
};

const VC_LABEL_MODE_OPTIONS = buildExportLabelModeOptions({
  showValue: true,
  showSeries: true,
  showModel: true,
  showSales: true,
});

function vcBubbleSeriesKey(trace: Data): string | null {
  const name = (trace as { name?: unknown }).name;
  return typeof name === "string" && name.trim() && !name.startsWith("label-p") ? name : null;
}

function buildVcSeriesColors(traces: Data[], exportSettings: ExportSettings): Record<string, string> {
  const manualColors = exportSettings.seriesColors ?? {};
  if (exportSettings.colorScheme === DEFAULT_VC_PRICE_BAND_EXPORT.colorScheme) {
    return manualColors;
  }
  const palette = getExportPalette(exportSettings.colorScheme);
  const resolved: Record<string, string> = { ...manualColors };
  const assigned = new Set(Object.keys(manualColors));
  let paletteIndex = 0;
  traces.forEach((trace) => {
    const key = vcBubbleSeriesKey(trace);
    if (!key || assigned.has(key)) return;
    resolved[key] = palette[paletteIndex % palette.length];
    assigned.add(key);
    paletteIndex += 1;
  });
  return resolved;
}

function applyVcExportToTraces(traces: Data[], exportSettings: ExportSettings): Data[] {
  const labeled = applyDataLabelsToTraces(traces, exportSettings);
  const colorOverrides = buildVcSeriesColors(traces, exportSettings);
  return applySeriesColors(labeled, colorOverrides);
}

function applyVcExportToLayout(
  layout: Partial<PlotlyLayout>,
  exportSettings: ExportSettings,
): Partial<PlotlyLayout> {
  return applyExportToLayout(layout, { ...exportSettings, chartTitle: "" });
}

const VC_ROW_HEIGHT_CHROME = 650;
const VC_COLUMN_HEIGHT_CHROME = 830;

function resolveVcCanvasHeight(
  presetHeight: number,
  layoutDirection: DeckLayoutDirection,
  chartHeight: number,
): number {
  const contentHeight = layoutDirection === "column"
    ? chartHeight * 2 + VC_COLUMN_HEIGHT_CHROME
    : chartHeight + VC_ROW_HEIGHT_CHROME;
  return Math.max(presetHeight, contentHeight);
}

const EXPORT_PRESETS = [
  { key: "hd+", label: "1600 x 900", width: 1600, height: 900 },
  { key: "fhd", label: "1920 x 1080", width: 1920, height: 1080 },
  { key: "qhd", label: "2560 x 1440", width: 2560, height: 1440 },
] as const;

function isSalesMode(value: string | null): value is PositioningPricingSalesMode {
  return SALES_MODE_OPTIONS.some((item) => item.value === value);
}

function isComparisonMode(value: string | null): value is VersionComparisonMode {
  return COMPARISON_MODE_OPTIONS.some((item) => item.value === value);
}

function formatMetricValue(value: number | string): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString("en-US");
  }
  return String(value ?? "-");
}

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i++) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function jitter(key: string, amplitude: number): number {
  return ((hashString(key) % 1000) / 1000) * amplitude * 2 - amplitude;
}

interface LabelPos {
  key: string;
  x: number;
  y: number;
  priority: number;
}

function filterOverlappingLabels(labels: LabelPos[], xRange: number, yRange: number): Set<string> {
  const hidden = new Set<string>();
  if (labels.length <= 1) return hidden;
  const sorted = [...labels].sort((a, b) => b.priority - a.priority);
  const placed: Array<{ x: number; y: number }> = [];
  const xThreshold = xRange * 0.022;
  const yThreshold = yRange * 0.028;
  for (const label of sorted) {
    const overlaps = placed.some(
      (p) => Math.abs(p.x - label.x) < xThreshold && Math.abs(p.y - label.y) < yThreshold,
    );
    if (overlaps) {
      hidden.add(label.key);
    } else {
      placed.push({ x: label.x, y: label.y });
    }
  }
  return hidden;
}

function sanitizeFileNameSegment(value: string): string {
  return value
    .trim()
    .replace(/[\\/:*?"<>|]+/g, "-")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
}

function readSearchTimeRange(searchParams: URLSearchParams): MarketScanPeriodRange | null {
  const start = searchParams.get("timeStart");
  const end = searchParams.get("timeEnd");
  return start && end ? { start, end } : null;
}

function isCustomTimeRange(range: MarketScanPeriodRange | null | undefined): boolean {
  return Boolean(range && range.start !== range.end);
}

function Panel({
  eyebrow,
  title,
  subtitle,
  children,
}: {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  children: React.ReactNode;
}) {
  return (
    <section className="market-scan-panel">
      <header className="market-scan-panel-head">
        <div>
          {eyebrow ? <span className="market-scan-panel-eyebrow">{eyebrow}</span> : null}
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </header>
      <div className="market-scan-panel-body">{children}</div>
    </section>
  );
}

function MetricCard({ metric }: { metric: PositioningPricingMetric }) {
  return (
    <article className="market-scan-metric-card">
      <span className="market-scan-metric-label">{metric.label}</span>
      <strong className="market-scan-metric-value">{formatMetricValue(metric.value)}</strong>
      <span className="market-scan-metric-detail">{metric.detail}</span>
    </article>
  );
}

function buildPriceBandTraces(items: PositioningPricingPriceBandItem[], fuelOrder: string[]): Data[] {
  return fuelOrder.map((fuel) => {
    const salesValues = items.map((item) => item.fuelMix[fuel] ?? 0);
    return withExportLabels({
      type: "bar",
      orientation: "h",
      name: fuel,
      y: items.map((item) => item.bandMid),
      x: salesValues,
      width: items.map((item) => Math.max(item.bandWidth * 0.84, 500)),
      customdata: items.map((item) => [item.label]),
      marker: { color: fuelColor(fuel) },
      hovertemplate: `%{customdata[0]}<br>${fuel}: %{x:,.0f} 台<extra></extra>`,
    } as Data, {
      model: items.map((item) => item.label),
      sales: salesValues,
      value: salesValues,
      series: items.map(() => fuel),
    });
  });
}

interface BuildBubbleTracesOpts {
  labelMode: LabelMode;
  selectedKeys: Set<string>;
}

interface LabelInfo {
  item: VersionComparisonBubbleItem;
  key: string;
  priority: number;
  showLabel: boolean;
  jitterX: number;
  jitterY: number;
}

const PRIORITY_STYLE: Record<number, { opacity: number; size: number; color: string }> = {
  3: { opacity: 0.97, size: 10, color: "rgba(15,23,42,0.97)" },
  2: { opacity: 0.70, size: 9,  color: "rgba(51,65,85,0.72)" },
  1: { opacity: 0.35, size: 9,  color: "rgba(51,65,85,0.38)" },
  0: { opacity: 0.20, size: 8,  color: "rgba(51,65,85,0.22)" },
};

function buildVersionBubbleTraces(items: VersionComparisonBubbleItem[], opts: BuildBubbleTracesOpts): Data[] {
  const { labelMode, selectedKeys } = opts;
  if (items.length === 0) return [];

  const sizing = buildBubbleSizing(items.map((item) => item.sales), {
    maxDiameter: 58,
    minDiameter: 10,
  });

  const seenPowertrains = Array.from(new Set(items.map((item) => item.powertrain)));
  const powertrains = [
    ...DEFAULT_FUEL_TYPES.filter((fuel) => seenPowertrains.includes(fuel)),
    ...seenPowertrains.filter((fuel) => !DEFAULT_FUEL_TYPES.includes(fuel)),
  ];

  // --- Key helpers ---
  const itemKey = (item: VersionComparisonBubbleItem) => `${item.modelKey || item.model}||${item.version}||${item.trim}`;
  const asCustomdata = (item: VersionComparisonBubbleItem, key: string) => [
    item.model, item.version, item.trim, item.powertrain,
    item.sales, item.msrpMin, item.msrpMax, item.length, key, item.brand,
  ];

  // --- Ranges for jitter & overlap ---
  const xVals = items.map((i) => i.length);
  const yVals = items.map((i) => i.msrp);
  const xRange = Math.max(...xVals) - Math.min(...xVals) || 1;
  const yRange = Math.max(...yVals) - Math.min(...yVals) || 1;

  // --- Model top-3 sales ---
  const modelGroups = new Map<string, VersionComparisonBubbleItem[]>();
  items.forEach((item) => {
    const modelKey = item.modelKey || item.model;
    const arr = modelGroups.get(modelKey) || [];
    arr.push(item);
    modelGroups.set(modelKey, arr);
  });
  const modelTopKeys = new Map<string, Set<string>>(); // model -> set of top-2/3 keys (excludes top-1)
  const modelTop1Key = new Map<string, string>(); // model -> top-1 key
  modelGroups.forEach((group, model) => {
    const sorted = [...group].sort((a, b) => b.sales - a.sales);
    modelTop1Key.set(model, itemKey(sorted[0]));
    modelTopKeys.set(model, new Set(sorted.slice(1, 3).map(itemKey)));
  });

  // --- Powertrain top-1 sales ---
  const ptGroups = new Map<string, VersionComparisonBubbleItem[]>();
  items.forEach((item) => {
    const arr = ptGroups.get(item.powertrain) || [];
    arr.push(item);
    ptGroups.set(item.powertrain, arr);
  });
  const ptTopKey = new Map<string, string>(); // powertrain -> key of top-1
  ptGroups.forEach((group, pt) => {
    const top = group.reduce((a, b) => (a.sales > b.sales ? a : b));
    ptTopKey.set(pt, itemKey(top));
  });

  // --- Global MSRP extremes ---
  const maxMsrpItem = items.reduce((a, b) => (a.msrp > b.msrp ? a : b));
  const minMsrpItem = items.reduce((a, b) => (a.msrp < b.msrp ? a : b));
  const maxMsrpKey = itemKey(maxMsrpItem);
  const minMsrpKey = itemKey(minMsrpItem);

  // --- Long-tail threshold (bottom 20% by sales) ---
  const salesSorted = [...items].map((i) => i.sales).sort((a, b) => a - b);
  const longTailCutoff = salesSorted[Math.floor(salesSorted.length * 0.2)];

  // --- Priority computation ---
  const labelInfos: LabelInfo[] = items.map((item) => {
    const key = itemKey(item);
    let priority = 1;

    const isModelTop1 = modelTop1Key.get(item.modelKey || item.model) === key;

    if (selectedKeys.has(key)) {
      priority = 3;
    } else if (isModelTop1 || key === maxMsrpKey || key === minMsrpKey) {
      priority = 3;
    } else if (modelTopKeys.get(item.modelKey || item.model)?.has(key)) {
      priority = 2;
    } else if (ptTopKey.get(item.powertrain) === key) {
      priority = 2;
    } else if (item.sales <= longTailCutoff) {
      priority = 0;
    }

    let showLabel = true;
    if (labelMode === "clean") {
      showLabel = false;
    } else if (labelMode === "smart_top") {
      showLabel = priority >= 2;
    } else if (labelMode === "selected") {
      showLabel = selectedKeys.has(key);
    } // "all" → showLabel stays true

    const jx = jitter(key, xRange * 0.008);
    const jy = jitter(key + "_y", yRange * 0.01);

    return { item, key, priority, showLabel, jitterX: jx, jitterY: jy };
  });

  // --- Overlap filter ---
  const visibleLabels = labelInfos.filter((l) => l.showLabel);
  const hiddenByOverlap = filterOverlappingLabels(
    visibleLabels.map((l) => ({
      key: l.key,
      x: l.item.length + l.jitterX,
      y: l.item.msrp + l.jitterY,
      priority: l.priority,
    })),
    xRange,
    yRange,
  );
  labelInfos.forEach((l) => {
    if (hiddenByOverlap.has(l.key)) l.showLabel = false;
  });

  // --- Build traces ---
  const traces: Data[] = [];

  // 1. Marker traces (bubbles — one per powertrain, in legend)
  powertrains.forEach((powertrain) => {
    const subset = items.filter((item) => item.powertrain === powertrain);
    traces.push({
      type: "scatter",
      mode: "markers",
      name: powertrain,
      x: subset.map((item) => item.length),
      y: subset.map((item) => item.msrp),
      cliponaxis: false,
      customdata: subset.map((item) => asCustomdata(item, itemKey(item))),
      marker: {
        color: fuelColor(powertrain),
        opacity: 0.82,
        line: { color: "rgba(15, 23, 42, 0.28)", width: 1 },
        size: subset.map((item) => Math.max(0, item.sales)),
        sizemode: sizing.sizemode,
        sizeref: sizing.sizeref,
        sizemin: sizing.sizemin,
      },
      hovertemplate:
        "Brand: %{customdata[9]}<br>Model: %{customdata[0]}<br>Version: %{customdata[1]}<br>Trim: %{customdata[2]}<br>动力: %{customdata[3]}"
        + "<br>Length: %{customdata[7]:,.0f} mm<br>MSRP: %{y:,.0f}<br>MSRP范围: %{customdata[5]:,.0f}-%{customdata[6]:,.0f}"
        + "<br>Sales: %{customdata[4]:,.0f}<extra></extra>",
    } as Data);
  });

  // 2. Label traces — one per priority level across all powertrains
  [3, 2, 1, 0].forEach((priority) => {
    const labelItems = labelInfos.filter((l) => l.priority === priority && l.showLabel);
    if (labelItems.length === 0) return;

    const style = PRIORITY_STYLE[priority];
    traces.push({
      type: "scatter",
      mode: "text",
      name: `label-p${priority}`,
      showlegend: false,
      x: labelItems.map((l) => l.item.length + l.jitterX),
      y: labelItems.map((l) => l.item.msrp + l.jitterY),
      text: labelItems.map((l) => l.item.version),
      textposition: "top center",
      textfont: { size: style.size, color: style.color, family: "Inter, sans-serif" },
      cliponaxis: false,
      customdata: labelItems.map((l) => asCustomdata(l.item, l.key)),
      hoverinfo: "skip",
    } as Data);
  });

  return traces;
}

const MODEL_LENGTH_LABEL_YSHIFTS = [-28, -52] as const;
const BUBBLE_MODEL_LABEL_BOTTOM_MARGIN = 118;

function buildModelLengthAnnotations(items: VersionComparisonBubbleItem[]): NonNullable<Partial<PlotlyLayout>["annotations"]> {
  const modelLengthMap = new Map<string, { label: string; length: number }>();
  items.forEach((item) => {
    const key = item.modelKey || item.model;
    if (!modelLengthMap.has(key)) {
      modelLengthMap.set(key, { label: item.model, length: item.length });
    }
  });
  const overlapThreshold = 70;
  let previousLength: number | null = null;
  let currentRow = 0;
  return Array.from(modelLengthMap.values())
    .sort((left, right) => left.length - right.length)
    .map(({ label, length }) => {
      if (previousLength !== null && Math.abs(length - previousLength) <= overlapThreshold) {
        currentRow = (currentRow + 1) % MODEL_LENGTH_LABEL_YSHIFTS.length;
      } else {
        currentRow = 0;
      }
      previousLength = length;
      return {
        x: length,
        y: 0,
        xref: "x",
        yref: "paper",
        yshift: MODEL_LENGTH_LABEL_YSHIFTS[currentRow],
        text: label,
        showarrow: false,
        xanchor: "center",
        yanchor: "top",
        align: "center",
        font: {
          size: 10,
          color: "#475569",
        },
      };
    });
}

function priceBandLayout(
  rangeMin: number,
  rangeMax: number,
  step: number,
): Partial<PlotlyLayout> {
  return {
    ...CHART_LAYOUT,
    barmode: "stack",
    margin: { l: 96, r: 80, t: 16, b: 62 },
    legend: {
      orientation: "v",
      x: 1.02,
      xanchor: "left",
      y: 1,
      yanchor: "top",
      font: { size: 9 },
    },
    xaxis: {
      title: { text: "Sales" },
      automargin: true,
      showgrid: false,
      zeroline: false,
    },
    yaxis: {
      title: { text: "MSRP" },
      range: [rangeMin, rangeMax],
      tick0: rangeMin,
      dtick: step,
      tickformat: ",d",
      automargin: true,
      showgrid: false,
      zeroline: false,
    },
  };
}

function versionBubbleLayout(
  rangeMin: number,
  rangeMax: number,
  step: number,
  annotations: NonNullable<Partial<PlotlyLayout>["annotations"]>,
): Partial<PlotlyLayout> {
  const bottomMargin = annotations.length > 0 ? BUBBLE_MODEL_LABEL_BOTTOM_MARGIN : 62;
  return {
    ...CHART_LAYOUT,
    margin: { l: 96, r: 80, t: 16, b: bottomMargin },
    legend: {
      orientation: "v",
      x: 1.02,
      xanchor: "left",
      y: 1,
      yanchor: "top",
      font: { size: 9 },
    },
    xaxis: {
      tickformat: ",d",
      automargin: true,
      showgrid: false,
      zeroline: false,
    },
    yaxis: {
      title: { text: "Version MSRP" },
      range: [rangeMin, rangeMax],
      tick0: rangeMin,
      dtick: step,
      tickformat: ",d",
      automargin: true,
      showgrid: false,
      zeroline: false,
    },
    annotations,
  };
}

function compactSearchToken(value: string): string {
  return compactSearchText(value);
}

function searchModelOptions<T extends VersionComparisonModelOption>(options: T[], query: string): T[] {
  const q = query.trim().toLowerCase();
  const compactQuery = compactSearchToken(query);
  if (!q) return options;
  return options.filter((m) => {
    const fields = [
      m.label,
      m.brand,
      m.segment,
      m.powertrain,
      m.bodyType,
      m.driveType,
      String(m.lengthMm),
      String(m.msrpMedian),
    ].filter(Boolean).join(" ").toLowerCase();
    return fields.includes(q) || (compactQuery !== "" && compactSearchToken(fields).includes(compactQuery));
  });
}

function modelOptionIdentity(option: VersionComparisonModelOption): string {
  const brand = compactSearchToken(option.brand || "");
  const label = compactSearchToken(option.label || option.value || option.modelKey || "");
  return `${brand}::${label}`;
}

function searchSegmentOptions(options: { value: string; label: string }[], query: string): { value: string; label: string }[] {
  const q = query.trim().toLowerCase();
  if (!q) return options;
  return options.filter((s) => optionMatchesCompactSearch(s, query));
}

function searchCountryOptions(options: { value: string; label: string }[], query: string): { value: string; label: string }[] {
  const q = query.trim().toLowerCase();
  if (!q) return options;
  return options.filter((c) => optionMatchesCompactSearch(c, query));
}

type VersionComparisonPickerOption = VersionComparisonModelOption & {
  availability: "current" | "global";
  availabilityReason?: string;
};

export function VersionComparisonPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { country: defaultCountry } = useResolvedCountry("zh");
  const [deck, setDeck] = useState<VersionComparisonDeckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportToolsOpen, setExportToolsOpen] = useState(false);
  const [controlToolsOpen, setControlToolsOpen] = useState(false);
  const [activeControlPanel, setActiveControlPanel] = useState<VersionComparisonControlPanel>("filters");
  const [activeExportSettingsPanel, setActiveExportSettingsPanel] = useState<VersionComparisonExportPanel>("bubble");
  const [priceBandExport, setPriceBandExport] = useState<ExportSettings>(DEFAULT_VC_PRICE_BAND_EXPORT);
  const [bubbleExport, setBubbleExport] = useState<ExportSettings>(DEFAULT_VC_BUBBLE_EXPORT);
  const [exportPresetKey, setExportPresetKey] = useState<(typeof EXPORT_PRESETS)[number]["key"]>(DEFAULT_EXPORT_PRESET);
  const [reloadToken, setReloadToken] = useState(0);
  const [modelSearchQuery, setModelSearchQuery] = useState("");
  const [modelPickerOpen, setModelPickerOpen] = useState(false);
  const [segmentSearchQuery, setSegmentSearchQuery] = useState("");
  const [segmentPickerOpen, setSegmentPickerOpen] = useState(false);
  const [countrySearchQuery, setCountrySearchQuery] = useState("");
  const [countryPickerOpen, setCountryPickerOpen] = useState(false);
  const requestRef = useRef(0);
  const slideRef = useRef<HTMLDivElement | null>(null);
  const modelPickerRef = useRef<HTMLDivElement | null>(null);
  const segmentPickerRef = useRef<HTMLDivElement | null>(null);
  const countryPickerRef = useRef<HTMLDivElement | null>(null);
  const skipResolvedModelFetchRef = useRef<string | null>(null);
  const modelScopeKeyRef = useRef<string | null>(null);

  const [selectedCountry, setSelectedCountry] = useState<string | null>(() => searchParams.get("country") || defaultCountry);
  const [selectedPeriod, setSelectedPeriod] = useState<string | null>(() => searchParams.get("period"));
  const [selectedTimeRange, setSelectedTimeRange] = useState<MarketScanPeriodRange | null>(
    () => readSearchTimeRange(searchParams),
  );
  const [salesMode, setSalesMode] = useState<PositioningPricingSalesMode>(() => {
    const requested = searchParams.get("salesMode");
    return isSalesMode(requested) ? requested : DEFAULT_SALES_MODE;
  });
  const [comparisonMode, setComparisonMode] = useState<VersionComparisonMode>(() => {
    const requested = searchParams.get("comparisonMode");
    return isComparisonMode(requested) ? requested : "same_segment";
  });
  const [selectedSegment, setSelectedSegment] = useState<string | null>(() => searchParams.get("segment"));
  const [selectedModels, setSelectedModels] = useState<string[]>(() => {
    const raw = searchParams.get("models");
    return raw ? raw.split("||").filter(Boolean).slice(0, MAX_SELECTED_MODELS) : [];
  });
  const [selectedFuelTypes, setSelectedFuelTypes] = useState<string[]>(() => {
    const raw = searchParams.get("fuelTypes");
    return raw ? raw.split(",") : DEFAULT_FUEL_TYPES;
  });
  const [modelToAdd, setModelToAdd] = useState("");
  const [priceControlsTouched, setPriceControlsTouched] = useState<boolean>(
    () => searchParams.has("msrpMin") || searchParams.has("msrpMax") || searchParams.has("priceBandSize"),
  );
  const [msrpMin, setMsrpMin] = useState<number | null>(() => {
    const raw = searchParams.get("msrpMin");
    return raw ? Number(raw) : null;
  });
  const [msrpMax, setMsrpMax] = useState<number | null>(() => {
    const raw = searchParams.get("msrpMax");
    return raw ? Number(raw) : null;
  });
  const [priceBandSize, setPriceBandSize] = useState<number | null>(() => {
    const raw = searchParams.get("priceBandSize");
    return raw ? Number(raw) : DEFAULT_PRICE_BAND_SIZE;
  });
  const [bodyType, setBodyType] = useState<string | null>(() => searchParams.get("bodyType"));
  const [driveTypes, setDriveTypes] = useState<string[]>(() => {
    const raw = searchParams.get("driveTypes");
    return raw ? raw.split(",").filter(Boolean) : [];
  });
  const [lengthMin, setLengthMin] = useState<number | null>(() => {
    const raw = searchParams.get("lengthMin");
    return raw ? Number(raw) : null;
  });
  const [lengthMax, setLengthMax] = useState<number | null>(() => {
    const raw = searchParams.get("lengthMax");
    return raw ? Number(raw) : null;
  });
  const [selectedSegments, setSelectedSegments] = useState<string[]>(() => {
    const raw = searchParams.get("segments");
    return raw ? raw.split(",").filter(Boolean) : [];
  });
  const [labelMode, setLabelMode] = useState<LabelMode>(() => {
    const raw = searchParams.get("labelMode");
    return isLabelMode(raw) ? raw : "all";
  });
  const [selectedBubbles, setSelectedBubbles] = useState<Set<string>>(new Set());
  const [hoveredBubble, setHoveredBubble] = useState<string | null>(null);
  const {
    layoutDirection,
    splitRatio,
    chartHeight,
    gridStyle: vcGridStyle,
    setLayoutDirection,
    setSplitRatio,
    setChartHeight,
    resetLayout: resetVcLayoutControls,
  } = useDeckLayoutControls({
    storageKeys: VC_LAYOUT_STORAGE_KEYS,
    defaults: VC_LAYOUT_DEFAULTS,
    ranges: VC_LAYOUT_RANGES,
    cssVariables: VC_LAYOUT_CSS_VARIABLES,
  });

  const countryOptions = deck?.metadata.availableCountries ?? [];
  const modelScopeKey = useMemo(() => JSON.stringify({
    bodyType: comparisonMode === "free_comparison" ? (bodyType ?? "") : "",
    comparisonMode,
    country: selectedCountry || "",
    driveTypes: comparisonMode === "free_comparison" ? driveTypes.slice().sort() : [],
    fuelTypes: selectedFuelTypes.slice().sort(),
    lengthMax: comparisonMode === "free_comparison" ? lengthMax : null,
    lengthMin: comparisonMode === "free_comparison" ? lengthMin : null,
    msrpMax: priceControlsTouched ? msrpMax : null,
    msrpMin: priceControlsTouched ? msrpMin : null,
    period: selectedPeriod || "",
    salesMode,
    segment: comparisonMode === "same_segment" ? (selectedSegment || "") : "",
    segments: comparisonMode === "free_comparison" ? selectedSegments.slice().sort() : [],
    timeEnd: selectedTimeRange?.end ?? "",
    timeStart: selectedTimeRange?.start ?? "",
  }), [
    bodyType,
    comparisonMode,
    driveTypes,
    lengthMax,
    lengthMin,
    msrpMax,
    msrpMin,
    priceControlsTouched,
    salesMode,
    selectedCountry,
    selectedFuelTypes,
    selectedPeriod,
    selectedSegment,
    selectedSegments,
    selectedTimeRange,
  ]);

  const syncUrlParams = useCallback(() => {
    const params = new URLSearchParams();
    if (selectedCountry) params.set("country", selectedCountry);
    if (selectedPeriod) params.set("period", selectedPeriod);
    if (selectedTimeRange) {
      params.set("timeStart", selectedTimeRange.start);
      params.set("timeEnd", selectedTimeRange.end);
    }
    if (salesMode !== DEFAULT_SALES_MODE) params.set("salesMode", salesMode);
    if (comparisonMode !== "same_segment") params.set("comparisonMode", comparisonMode);
    if (selectedSegment && comparisonMode === "same_segment") params.set("segment", selectedSegment);
    if (selectedModels.length > 0) params.set("models", selectedModels.join("||"));
    const fuels = selectedFuelTypes.slice().sort().join(",");
    const defaultFuels = DEFAULT_FUEL_TYPES.slice().sort().join(",");
    if (fuels && fuels !== defaultFuels) params.set("fuelTypes", selectedFuelTypes.join(","));
    if (priceControlsTouched) {
      if (msrpMin !== null) params.set("msrpMin", String(msrpMin));
      if (msrpMax !== null) params.set("msrpMax", String(msrpMax));
      if (priceBandSize !== null && priceBandSize !== DEFAULT_PRICE_BAND_SIZE) params.set("priceBandSize", String(priceBandSize));
    }
    if (bodyType && comparisonMode !== "same_segment") params.set("bodyType", bodyType);
    if (driveTypes.length > 0 && comparisonMode !== "same_segment") params.set("driveTypes", driveTypes.join(","));
    if (lengthMin !== null && comparisonMode !== "same_segment") params.set("lengthMin", String(lengthMin));
    if (lengthMax !== null && comparisonMode !== "same_segment") params.set("lengthMax", String(lengthMax));
    if (selectedSegments.length > 0 && comparisonMode !== "same_segment") params.set("segments", selectedSegments.join(","));
    if (labelMode !== "smart_top") params.set("labelMode", labelMode);
    setSearchParams(params, { replace: true });
  }, [msrpMax, msrpMin, priceBandSize, priceControlsTouched, salesMode, comparisonMode, selectedCountry, selectedFuelTypes, selectedModels, selectedPeriod, selectedSegment, selectedTimeRange, bodyType, driveTypes, lengthMin, lengthMax, selectedSegments, labelMode, setSearchParams]);

  useEffect(() => {
    syncUrlParams();
  }, [syncUrlParams]);

  useEffect(() => {
    preloadPlotlyChartRuntime().catch(() => undefined);
  }, []);

  useArrowCountryNavigation({
    options: countryOptions,
    activeValue: selectedCountry || defaultCountry,
    onSelect: (value) => setSelectedCountry(value || null),
  });

  useEffect(() => {
    const selectedModelKey = selectedModels.join("||");
    const previousModelScopeKey = modelScopeKeyRef.current;
    const modelScopeChanged = previousModelScopeKey !== null && previousModelScopeKey !== modelScopeKey;
    modelScopeKeyRef.current = modelScopeKey;
    if (!modelScopeChanged && skipResolvedModelFetchRef.current === selectedModelKey) {
      skipResolvedModelFetchRef.current = null;
      return;
    }
    if (modelScopeChanged) {
      skipResolvedModelFetchRef.current = null;
    }
    const requestModels = modelScopeChanged ? [] : selectedModels;
    if (modelScopeChanged && selectedModels.length > 0) {
      skipResolvedModelFetchRef.current = "";
      setSelectedModels([]);
      setModelSearchQuery("");
      setModelPickerOpen(false);
      setModelToAdd("");
      setSelectedBubbles(new Set());
    }
    const requestId = ++requestRef.current;
    setLoading(true);
    setError("");
    api.versionComparisonDeck({
      country: selectedCountry || undefined,
      target_period: selectedPeriod || undefined,
      time_range: selectedTimeRange || undefined,
      fuel_types: selectedFuelTypes,
      sales_mode: salesMode,
      comparison_mode: comparisonMode,
      segment: selectedSegment || undefined,
      models: requestModels,
      msrp_min: priceControlsTouched ? (msrpMin ?? undefined) : undefined,
      msrp_max: priceControlsTouched ? (msrpMax ?? undefined) : undefined,
      price_band_size: priceControlsTouched ? (priceBandSize ?? undefined) : undefined,
      body_type: bodyType || undefined,
      drive_types: driveTypes.length > 0 ? driveTypes : undefined,
      segments: selectedSegments.length > 0 ? selectedSegments : undefined,
      length_min: lengthMin ?? undefined,
      length_max: lengthMax ?? undefined,
    })
      .then((response) => {
        if (requestId !== requestRef.current) {
          return;
        }
        setDeck(response);
      })
      .catch((reason: Error) => {
        if (requestId !== requestRef.current) {
          return;
        }
        setError(reason.message);
      })
      .finally(() => {
        if (requestId === requestRef.current) {
          setLoading(false);
        }
      });
  }, [msrpMax, msrpMin, modelScopeKey, priceBandSize, priceControlsTouched, reloadToken, salesMode, comparisonMode, selectedCountry, selectedFuelTypes, selectedModels, selectedPeriod, selectedSegment, selectedTimeRange, bodyType, driveTypes, selectedSegments, lengthMin, lengthMax]);

  useEffect(() => {
    if (!deck) {
      return;
    }
    if (selectedCountry && !deck.metadata.availableCountries.some((item) => item.value === selectedCountry)) {
      setSelectedCountry(deck.metadata.selectedCountry);
    }
    if (selectedPeriod && !deck.metadata.availablePeriods.some((item) => item.value === selectedPeriod)) {
      setSelectedPeriod(deck.metadata.resolvedPeriod);
    }
    if (selectedTimeRange) {
      const availablePeriodSet = new Set(deck.metadata.availablePeriods.map((item) => item.value));
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
    if (comparisonMode === "same_segment" && selectedSegment !== deck.metadata.selectedSegment) {
      setSelectedSegment(deck.metadata.selectedSegment);
    }
    const availableFuelSet = new Set(deck.metadata.availableFuelTypes);
    const normalizedFuelTypes = selectedFuelTypes.filter((fuel) => availableFuelSet.has(fuel));
    if (normalizedFuelTypes.length !== selectedFuelTypes.length) {
      setSelectedFuelTypes(deck.metadata.selectedFuelTypes);
    }
    if (
      selectedModels.length !== deck.metadata.selectedModels.length
      || selectedModels.some((model, index) => model !== deck.metadata.selectedModels[index])
    ) {
      skipResolvedModelFetchRef.current = deck.metadata.selectedModels.join("||");
      setSelectedModels(deck.metadata.selectedModels);
    }
  }, [deck, selectedTimeRange]);

  // Auto-detect free_comparison mode when models span multiple segments
  useEffect(() => {
    if (!deck || comparisonMode !== "same_segment") return;
    const modelDetails = deck.metadata.availableModels.filter((m) => selectedModels.includes(m.value));
    const segments = new Set(modelDetails.map((m) => m.segment).filter(Boolean));
    if (segments.size > 1 && !searchParams.get("comparisonMode")) {
      setComparisonMode("free_comparison");
    }
  }, [deck?.metadata.availableModels, selectedModels, comparisonMode]);

  // Close pickers on outside click
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (modelPickerRef.current && !modelPickerRef.current.contains(event.target as Node)) {
        setModelPickerOpen(false);
      }
      if (segmentPickerRef.current && !segmentPickerRef.current.contains(event.target as Node)) {
        setSegmentPickerOpen(false);
      }
      if (countryPickerRef.current && !countryPickerRef.current.contains(event.target as Node)) {
        setCountryPickerOpen(false);
      }
    }
    if (modelPickerOpen || segmentPickerOpen || countryPickerOpen) {
      document.addEventListener("mousedown", handleClickOutside);
    }
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, [modelPickerOpen, segmentPickerOpen, countryPickerOpen]);

  const currentCountry = selectedCountry ?? deck?.metadata.selectedCountry ?? defaultCountry;
  const resolvedTimeRange = selectedTimeRange ?? deck?.metadata.selectedTimeRange ?? null;
  const customRangeActive = isCustomTimeRange(resolvedTimeRange);
  const currentPeriod = resolvedTimeRange?.end ?? selectedPeriod ?? deck?.metadata.resolvedPeriod ?? "";
  const currentSegment = selectedSegment ?? deck?.metadata.selectedSegment ?? "";
  const fuelOptions = deck?.metadata.availableFuelTypes ?? DEFAULT_FUEL_TYPES;
  const activeFuelTypes = selectedFuelTypes.length > 0
    ? selectedFuelTypes
    : (deck?.metadata.selectedFuelTypes ?? DEFAULT_FUEL_TYPES);
  const activeModels = selectedModels.length > 0
    ? selectedModels
    : (deck?.metadata.selectedModels ?? []);
  const maxModelsReached = activeModels.length >= MAX_SELECTED_MODELS;
  const page = deck?.page;
  const isMixedSegment = deck?.metadata.isMixedSegment ?? false;
  const bodyTypeOptions = deck?.metadata.availableBodyTypes ?? [];
  const driveTypeOptions = deck?.metadata.availableDriveTypes ?? [];
  const activeModeLabel = comparisonMode === "free_comparison" ? "自由对比" : "同级别对比";
  const activeSegmentLabel = comparisonMode === "free_comparison"
    ? (selectedSegments.length > 0 ? selectedSegments.join(" + ") : "全部 Segment")
    : (currentSegment || "当前 Segment");
  const globalUnavailableContext = [
    deck?.metadata.selectedCountryLabel ?? currentCountry,
    deck?.metadata.labels.salesModeLabel,
    activeFuelTypes.join(" / "),
    activeSegmentLabel,
  ].filter(Boolean).join(" · ");

  useEffect(() => {
    if (!page || priceControlsTouched) {
      return;
    }
    if (msrpMin !== page.priceBands.range.min) {
      setMsrpMin(page.priceBands.range.min);
    }
    if (msrpMax !== page.priceBands.range.max) {
      setMsrpMax(page.priceBands.range.max);
    }
    if (priceBandSize !== DEFAULT_PRICE_BAND_SIZE) {
      setPriceBandSize(DEFAULT_PRICE_BAND_SIZE);
    }
  }, [msrpMax, msrpMin, page, priceBandSize, priceControlsTouched]);

  const exportPreset = EXPORT_PRESETS.find((item) => item.key === exportPresetKey) ?? EXPORT_PRESETS[1];
  const vcCanvasHeight = resolveVcCanvasHeight(exportPreset.height, layoutDirection, chartHeight);
  const slidePreview = useFixedCanvasPreview({
    width: exportPreset.width,
    height: vcCanvasHeight,
    exporting: exportingSlide,
  });

  // Candidate pool: current-country options plus global-discovery options shown in the picker.
  const candidateOptions = deck?.metadata.availableModels ?? [];
  const globalCandidateOptions = deck?.metadata.globalAvailableModels ?? [];
  const pickerOptions = useMemo<VersionComparisonPickerOption[]>(() => {
    const currentValues = new Set(candidateOptions.map((option) => option.value));
    const currentIdentities = new Set(candidateOptions.map(modelOptionIdentity));
    const currentOptions = candidateOptions.map((option) => ({
      ...option,
      availability: "current" as const,
    }));
    const globalOnlyOptions = globalCandidateOptions
      .filter((option) => !currentValues.has(option.value) && !currentIdentities.has(modelOptionIdentity(option)))
      .map((option) => ({
        ...option,
        availability: "global" as const,
        availabilityReason: "当前筛选无销量",
      }));
    return [...currentOptions, ...globalOnlyOptions];
  }, [candidateOptions, globalCandidateOptions]);
  // Filtered by search query
  const searchedOptions = useMemo(
    () => searchModelOptions(pickerOptions, modelSearchQuery),
    [modelSearchQuery, pickerOptions],
  );
  const segmentOptions = deck?.metadata.availableSegments ?? [];
  const searchedSegmentOptions = useMemo(
    () => searchSegmentOptions(segmentOptions, segmentSearchQuery),
    [segmentOptions, segmentSearchQuery],
  );
  const searchedCountryOptions = useMemo(
    () => searchCountryOptions(countryOptions, countrySearchQuery),
    [countryOptions, countrySearchQuery],
  );
  // Models the user has selected, with full metadata
  const selectedModelDetails = useMemo(() => {
    const detailMap = new Map(candidateOptions.map((m) => [m.value, m]));
    return activeModels.map((modelName) => detailMap.get(modelName)).filter(Boolean) as VersionComparisonModelOption[];
  }, [activeModels, candidateOptions]);

  // Backward compat: unselected model options for the old plain select (unused but kept for data)
  const unselectedModelOptions = candidateOptions.filter((item) => !activeModels.includes(item.value));

  useEffect(() => {
    if (!modelToAdd && unselectedModelOptions.length > 0) {
      setModelToAdd(unselectedModelOptions[0].value);
      return;
    }
    if (modelToAdd && !unselectedModelOptions.some((item) => item.value === modelToAdd)) {
      setModelToAdd(unselectedModelOptions[0]?.value ?? "");
    }
  }, [modelToAdd, unselectedModelOptions]);

  // Auto-select modelToAdd from search results
  useEffect(() => {
    if (searchedOptions.length > 0) {
      const unselected = searchedOptions.filter((m) => m.availability === "current" && !activeModels.includes(m.value));
      setModelToAdd(unselected[0]?.value ?? "");
    } else {
      setModelToAdd("");
    }
  }, [searchedOptions, activeModels]);

  const barTraces = useMemo(
    () => (page ? applyVcExportToTraces(buildPriceBandTraces(page.priceBands.items, activeFuelTypes), priceBandExport) : []),
    [activeFuelTypes, priceBandExport, page],
  );
  const bubbleTraces = useMemo(
    () => (page ? applyVcExportToTraces(buildVersionBubbleTraces(page.bubbleChart.items, { labelMode, selectedKeys: selectedBubbles }), bubbleExport) : []),
    [bubbleExport, labelMode, page, selectedBubbles],
  );
  const bubbleAnnotations = useMemo(
    () => (page ? buildModelLengthAnnotations(page.bubbleChart.items) : []),
    [page],
  );

  const { toggle, isolate } = useFuelChipClick(fuelOptions, setSelectedFuelTypes);

  function handleControlDrawerOpenChange(open: boolean): void {
    setControlToolsOpen(open);
    if (open) {
      setExportToolsOpen(false);
      setActiveControlPanel("filters");
    }
  }

  function handleExportDrawerOpenChange(open: boolean): void {
    setExportToolsOpen(open);
    if (open) {
      setControlToolsOpen(false);
      setActiveExportSettingsPanel("bubble");
    }
  }

  function handleAddModel() {
    if (!modelToAdd) {
      return;
    }
    setSelectedModels((current) => {
      if (current.includes(modelToAdd) || current.length >= MAX_SELECTED_MODELS) {
        return current;
      }
      return [...current, modelToAdd];
    });
    setModelSearchQuery("");
    setModelPickerOpen(false);
  }

  function handleRemoveModel(model: string) {
    setSelectedModels((current) => current.filter((item) => item !== model));
  }

  function handleSelectAllVisible() {
    setSelectedModels((current) => {
      const existing = new Set(current);
      const toAdd = searchedOptions
        .filter((m) => m.availability === "current" && !existing.has(m.value))
        .slice(0, MAX_SELECTED_MODELS - current.length)
        .map((m) => m.value);
      return [...current, ...toAdd].slice(0, MAX_SELECTED_MODELS);
    });
  }

  function handleDeselectAllVisible() {
    const visibleValues = new Set(searchedOptions.map((m) => m.value));
    setSelectedModels((current) => current.filter((m) => !visibleValues.has(m)));
  }

  function handleClearAll() {
    setSelectedModels([]);
  }

  function handleToggleModel(modelValue: string) {
    setSelectedModels((current) => {
      if (current.includes(modelValue)) {
        return current.filter((item) => item !== modelValue);
      }
      if (current.length >= MAX_SELECTED_MODELS) {
        return current;
      }
      return [...current, modelValue];
    });
  }

  function handleBubbleHover(event: Readonly<PlotMouseEvent>) {
    const pt = event.points?.[0] as unknown as Record<string, unknown> | undefined;
    const cd = pt?.customdata as unknown[] | undefined;
    if (cd && cd.length >= 9) {
      setHoveredBubble(String(cd[8]));
    }
  }

  function handleBubbleUnhover() {
    setHoveredBubble(null);
  }

  function handleBubbleClick(event: Readonly<PlotMouseEvent>) {
    const pt = event.points?.[0] as unknown as Record<string, unknown> | undefined;
    const cd = pt?.customdata as unknown[] | undefined;
    if (!cd || cd.length < 9) return;
    const key = String(cd[8]);
    setSelectedBubbles((prev) => {
      const next = new Set(prev);
      if (next.has(key)) {
        next.delete(key);
      } else {
        next.add(key);
      }
      return next;
    });
  }

  function handleBubbleSelect(event: Readonly<PlotSelectionEvent>) {
    if (!event.points) return;
    const keys: string[] = [];
    for (const p of event.points) {
      const cd = (p as unknown as Record<string, unknown>).customdata as unknown[] | undefined;
      if (cd && cd.length >= 9) {
        keys.push(String(cd[8]));
      }
    }
    if (keys.length === 0) return;
    setSelectedBubbles((prev) => new Set([...prev, ...keys]));
  }

  async function handleExportSlide() {
    if (!slideRef.current || !deck || !page) {
      return;
    }
    try {
      setExportError("");
      setExportingSlide(true);
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
        backgroundColor: "#eef4f7",
        width: exportPreset.width,
        height: vcCanvasHeight,
        canvasWidth: exportPreset.width,
        canvasHeight: vcCanvasHeight,
        style: {
          width: `${exportPreset.width}px`,
          height: `${vcCanvasHeight}px`,
        },
      });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = [
        "version-comparison",
        sanitizeFileNameSegment(deck.metadata.selectedCountryLabel),
        sanitizeFileNameSegment(comparisonMode),
        deck.metadata.resolvedPeriod,
      ].join("-") + ".png";
      link.click();
    } catch (reason) {
      setExportError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setExportingSlide(false);
    }
  }

  return (
    <div className="positioning-pricing-shell">
      <div className="positioning-pricing-main">
        <section className="header-card dashboard-hero market-scan-hero positioning-pricing-hero">
          <div className="dashboard-hero-head positioning-pricing-summary-head">
            <div className="dashboard-hero-copy market-scan-hero-copy">
              <div style={{ display: "flex", alignItems: "flex-start", justifyContent: "space-between", gap: 16 }}>
                <div>
                  <span className="page-kicker">Version Comparison</span>
                  <h1>{deck?.metadata.labels.pageTitle ?? "版型对比"}</h1>
                  <p>{page?.summaryText ?? "按 segment 和 model 组合，对比不同 version/trim 的定位分布。"}</p>
                </div>
                <div className="btn-group" style={{ flexShrink: 0 }}>
                  <button type="button" className="btn btn-secondary btn-sm"
                    onClick={() => setReloadToken((v) => v + 1)}>Refresh</button>
                  <button type="button" className="btn btn-primary btn-sm"
                    onClick={() => { void handleExportSlide(); }}
                    disabled={!deck || !page || exportingSlide}>
                    {exportingSlide ? "导出中..." : "Export PNG"}
                  </button>
                  <button type="button" className="btn btn-ghost btn-sm"
                    onClick={() => {
                      setSelectedCountry(defaultCountry); setSelectedPeriod(null);
                      setSalesMode(DEFAULT_SALES_MODE); setComparisonMode("same_segment");
                      setSelectedSegment(null); setSelectedModels([]);
                      setSelectedFuelTypes(DEFAULT_FUEL_TYPES); setPriceControlsTouched(false);
                      setMsrpMin(null); setMsrpMax(null); setPriceBandSize(DEFAULT_PRICE_BAND_SIZE);
                      setBodyType(null); setDriveTypes([]); setSelectedSegments([]);
                      setModelSearchQuery(""); setSegmentSearchQuery("");
                      resetVcLayoutControls();
                    }}>Reset</button>
                </div>
              </div>
              <div className="market-scan-hero-ribbon">
                <span className="market-scan-hero-chip">国家 {deck?.metadata.selectedCountryLabel ?? currentCountry}</span>
                <span className="market-scan-hero-chip">月份 {customRangeActive ? (resolvedTimeRange ? `${resolvedTimeRange.start}~${resolvedTimeRange.end}` : (deck?.metadata.labels.currentMonthShort ?? "Latest")) : (deck?.metadata.labels.currentMonthShort ?? "Latest")}</span>
                <span className="market-scan-hero-chip">口径 {customRangeActive ? "自定义区间累计" : (deck?.metadata.labels.salesModeLabel ?? "当月")}</span>
                <span className="market-scan-hero-chip">模式 {activeModeLabel}</span>
                <span className="market-scan-hero-chip">Models {activeModels.length}/{MAX_SELECTED_MODELS}</span>
                {loading && deck ? <span className="market-scan-hero-chip market-scan-hero-chip--live">Refreshing</span> : null}
                {isMixedSegment ? <span className="market-scan-hero-chip market-scan-hero-chip--warn">跨Segment</span> : null}
              </div>
            </div>
          </div>
        </section>

        <DeckFloatingDrawer
          open={controlToolsOpen}
          onOpenChange={handleControlDrawerOpenChange}
          triggerPrimary="筛选 / 布局"
          triggerSecondaryOpen="收起控制"
          triggerSecondaryClosed="打开控制"
          eyebrow="Controls"
          title="筛选与版式"
          ariaLabel="Version Comparison controls"
          footer={(
            <>
              <span className="market-scan-toolbar-chip">{deck?.metadata.selectedCountryLabel ?? currentCountry}</span>
              <span className="market-scan-toolbar-chip">{activeModeLabel}</span>
              <span className="market-scan-toolbar-chip">Models {activeModels.length}/{MAX_SELECTED_MODELS}</span>
              <span className="market-scan-toolbar-chip">{layoutDirection === "row" ? `并排 ${splitRatio}/${100 - splitRatio}` : "上下"}</span>
            </>
          )}
        >
          <DeckControlTabs
            tabs={VC_CONTROL_TABS}
            activeKey={activeControlPanel}
            onChange={setActiveControlPanel}
            ariaLabel="版型对比控制"
          />

          {activeControlPanel === "filters" ? (
            <div className="deck-panel-grid">
              <div className="market-scan-field deck-panel-grid__wide">
                <span>对比模式</span>
                <div className="btn-group">
                  {COMPARISON_MODE_OPTIONS.map((option) => (
                    <button
                      key={option.value}
                      type="button"
                      className={`btn btn-sm ${comparisonMode === option.value ? "btn-primary" : "btn-ghost"}`}
                      onClick={() => setComparisonMode(option.value)}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>
              </div>

              <div className="market-scan-field version-comparison-model-picker-field" ref={countryPickerRef}>
                <span>Country</span>
                <div className="version-comparison-model-picker">
                  <div className="version-comparison-model-picker-input-row">
                    <input
                      type="text"
                      className="version-comparison-model-search"
                      placeholder="搜索国家..."
                      value={countrySearchQuery || deck?.metadata.selectedCountryLabel || currentCountry}
                      onChange={(event) => {
                        setCountrySearchQuery(event.target.value);
                        setCountryPickerOpen(true);
                      }}
                      onFocus={() => {
                        setCountrySearchQuery("");
                        setCountryPickerOpen(true);
                      }}
                      disabled={!deck}
                    />
                  </div>
                  {countryPickerOpen && searchedCountryOptions.length > 0 ? (
                    <div className="version-comparison-model-dropdown">
                      {searchedCountryOptions.slice(0, 40).map((option) => {
                        const isSelected = option.value === (selectedCountry || defaultCountry);
                        return (
                          <button
                            key={option.value}
                            type="button"
                            className={`version-comparison-model-option${isSelected ? " is-selected" : ""}`}
                            onClick={() => {
                              setSelectedCountry(option.value);
                              setCountrySearchQuery("");
                              setCountryPickerOpen(false);
                            }}
                          >
                            <span className={`version-comparison-model-checkbox${isSelected ? " is-checked" : ""}`}>
                              {isSelected ? "✓" : ""}
                            </span>
                            <span className="version-comparison-model-option-name">{option.label}</span>
                          </button>
                        );
                      })}
                    </div>
                  ) : null}
                  {countryPickerOpen && searchedCountryOptions.length === 0 && countrySearchQuery.trim() ? (
                    <div className="version-comparison-model-dropdown">
                      <div className="version-comparison-model-empty">无匹配国家</div>
                    </div>
                  ) : null}
                </div>
              </div>

              <div className="market-scan-field version-comparison-model-picker-field" ref={segmentPickerRef}>
                <span>{comparisonMode === "same_segment" ? "Segment" : `Segment${selectedSegments.length > 0 ? ` (${selectedSegments.length})` : ""}`}</span>
                <div className="version-comparison-model-picker">
                  <div className="version-comparison-model-picker-input-row">
                    <input
                      type="text"
                      className="version-comparison-model-search"
                      placeholder={comparisonMode === "same_segment" ? (currentSegment || "搜索 Segment...") : "多选 Segment..."}
                      value={segmentSearchQuery}
                      onChange={(event) => {
                        setSegmentSearchQuery(event.target.value);
                        setSegmentPickerOpen(true);
                      }}
                      onFocus={() => {
                        setSegmentSearchQuery("");
                        setSegmentPickerOpen(true);
                      }}
                      disabled={!deck}
                    />
                  </div>
                  {segmentPickerOpen && searchedSegmentOptions.length > 0 ? (
                    <div className="version-comparison-model-dropdown">
                      {comparisonMode !== "same_segment" ? (
                        <div className="version-comparison-model-dropdown-actions">
                          <button type="button" className="version-comparison-batch-btn"
                            onClick={() => setSelectedSegments(searchedSegmentOptions.map(s => s.value))}>全选</button>
                          <button type="button" className="version-comparison-batch-btn"
                            onClick={() => { const v = new Set(searchedSegmentOptions.map(s => s.value)); setSelectedSegments(c => c.filter(s => !v.has(s))); }}>取消</button>
                          <button type="button" className="version-comparison-batch-btn"
                            onClick={() => setSelectedSegments([])}>清空</button>
                          <span className="version-comparison-dropdown-count">
                            {searchedSegmentOptions.length} 项 · {selectedSegments.length} 已选
                          </span>
                        </div>
                      ) : null}
                      {searchedSegmentOptions.slice(0, 30).map((seg) => {
                        const active = comparisonMode === "same_segment"
                          ? seg.value === currentSegment
                          : selectedSegments.includes(seg.value);
                        return (
                          <button
                            key={seg.value}
                            type="button"
                            className={`version-comparison-model-option${active ? " is-selected" : ""}`}
                            onClick={() => {
                              if (comparisonMode === "same_segment") {
                                setSelectedSegment(seg.value);
                                setSelectedModels([]);
                                setSegmentSearchQuery("");
                                setSegmentPickerOpen(false);
                              } else {
                                setSelectedSegments((current) =>
                                  current.includes(seg.value)
                                    ? current.filter((s) => s !== seg.value)
                                    : [...current, seg.value]
                                );
                              }
                            }}
                          >
                            <span className={`version-comparison-model-checkbox${active ? " is-checked" : ""}`}>
                              {active ? "✓" : ""}
                            </span>
                            <span className="version-comparison-model-option-name">{seg.label}</span>
                          </button>
                        );
                      })}
                    </div>
                  ) : null}
                  {segmentPickerOpen && searchedSegmentOptions.length === 0 && segmentSearchQuery.trim() ? (
                    <div className="version-comparison-model-dropdown">
                      <div className="version-comparison-model-empty">无匹配 Segment</div>
                    </div>
                  ) : null}
                </div>
              </div>

              <div className="market-scan-field deck-panel-grid__wide">
                <span>时间与口径</span>
                <div className="btn-group" style={{ marginBottom: 8 }}>
                  {SALES_MODE_OPTIONS.map((option) => (
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
                      {resolvedTimeRange ? `${resolvedTimeRange.start} - ${resolvedTimeRange.end}` : "自定义区间"}
                    </span>
                  ) : null}
                </div>
                <DeckPeriodTimeline
                  options={deck?.metadata.availablePeriods ?? []}
                  value={resolvedTimeRange ?? (selectedPeriod ? { start: selectedPeriod, end: selectedPeriod } : null)}
                  onChange={(value) => {
                    setSelectedTimeRange(isCustomTimeRange(value) ? value : null);
                    setSelectedPeriod(value?.end ?? null);
                  }}
                  disabled={!deck}
                />
                {customRangeActive ? (
                  <small className="market-scan-field-hint">已切换自定义区间；点击当月/YTD/近12个月退出。</small>
                ) : null}
              </div>

              <div className="market-scan-field version-comparison-model-picker-field deck-panel-grid__wide" ref={modelPickerRef}>
                <span>Add Model {maxModelsReached ? `(${MAX_SELECTED_MODELS}/${MAX_SELECTED_MODELS})` : `(${activeModels.length}/${MAX_SELECTED_MODELS})`}</span>
                <div className="version-comparison-model-picker">
                  <div className="version-comparison-model-picker-input-row">
                    <input
                      type="text"
                      className="version-comparison-model-search"
                      placeholder={maxModelsReached ? `最多 ${MAX_SELECTED_MODELS} 个` : "搜索品牌或车型名称..."}
                      value={modelSearchQuery}
                      onChange={(event) => {
                        setModelSearchQuery(event.target.value);
                        setModelPickerOpen(true);
                      }}
                      onFocus={() => setModelPickerOpen(true)}
                      disabled={!deck || maxModelsReached}
                    />
                  </div>
                  {modelPickerOpen && searchedOptions.length > 0 ? (
                    <div className="version-comparison-model-dropdown">
                      <div className="version-comparison-model-dropdown-actions">
                        <button type="button" className="version-comparison-batch-btn"
                          onClick={handleSelectAllVisible} disabled={maxModelsReached}>全选</button>
                        <button type="button" className="version-comparison-batch-btn"
                          onClick={handleDeselectAllVisible}>取消</button>
                        <span className="version-comparison-dropdown-count">
                          {searchedOptions.length} 项 · {activeModels.length}/{MAX_SELECTED_MODELS} 已选
                        </span>
                      </div>
                      {searchedOptions.slice(0, 50).map((option) => {
                        const isSelected = activeModels.includes(option.value);
                        const isGlobalOnly = option.availability === "global";
                        return (
                          <button
                            key={option.value}
                            type="button"
                            className={`version-comparison-model-option${option.value === modelToAdd ? " is-active" : ""}${isSelected ? " is-selected" : ""}${isGlobalOnly ? " is-global-only" : ""}`}
                            disabled={isGlobalOnly}
                            title={isGlobalOnly ? `${option.availabilityReason ?? "当前筛选无销量"}：${globalUnavailableContext}` : undefined}
                            onClick={() => { handleToggleModel(option.value); setModelToAdd(option.value); }}
                            onMouseEnter={() => { if (!isGlobalOnly) setModelToAdd(option.value); }}
                          >
                            <span className={`version-comparison-model-checkbox${isSelected ? " is-checked" : ""}`}>
                              {isSelected ? "✓" : ""}
                            </span>
                            <div className="version-comparison-model-option-body">
                              <div className="version-comparison-model-option-main">
                                <span className="version-comparison-model-option-name">{option.label}</span>
                                {isSelected ? <span className="version-comparison-model-option-added">已添加</span> : null}
                                {isGlobalOnly ? <span className="version-comparison-model-option-added is-muted">{`全局有车 · ${option.availabilityReason ?? "当前筛选无销量"}`}</span> : null}
                              </div>
                              <div className="version-comparison-model-option-meta">
                                {option.brand ? <span>{option.brand}</span> : null}
                                {option.segment ? <span>{option.segment}</span> : null}
                                {option.powertrain ? <span>{option.powertrain}</span> : null}
                                {option.lengthMm > 0 ? <span>{option.lengthMm} mm</span> : null}
                                {option.driveType ? <span>{option.driveType}</span> : null}
                              </div>
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  ) : null}
                  {modelPickerOpen && searchedOptions.length === 0 && modelSearchQuery.trim() ? (
                    <div className="version-comparison-model-dropdown">
                      <div className="version-comparison-model-empty">无匹配车型</div>
                    </div>
                  ) : null}
                </div>
              </div>
            </div>
          ) : null}

          {activeControlPanel === "range" ? (
            <div className="deck-panel-grid">
              <label className="market-scan-field">
                <span>MSRP Min</span>
                <DebouncedNumberInput
                  value={msrpMin}
                  min={0}
                  max={MAX_MSRP_INPUT}
                  step={1000}
                  allowEmpty
                  inputMode="numeric"
                  className="version-comparison-number-input"
                  placeholder={String(page?.priceBands.range.min ?? "")}
                  onDraftChange={() => setPriceControlsTouched(true)}
                  onCommit={setMsrpMin}
                />
              </label>
              <label className="market-scan-field">
                <span>MSRP Max</span>
                <DebouncedNumberInput
                  value={msrpMax}
                  min={0}
                  max={MAX_MSRP_INPUT}
                  step={1000}
                  allowEmpty
                  inputMode="numeric"
                  className="version-comparison-number-input"
                  placeholder={String(page?.priceBands.range.max ?? "")}
                  onDraftChange={() => setPriceControlsTouched(true)}
                  onCommit={setMsrpMax}
                />
              </label>
              <label className="market-scan-field">
                <span>Step</span>
                <DebouncedNumberInput
                  value={priceBandSize}
                  min={MIN_PRICE_BAND_SIZE}
                  max={MAX_PRICE_BAND_SIZE}
                  step={500}
                  allowEmpty
                  inputMode="numeric"
                  className="version-comparison-number-input"
                  placeholder={String(page?.priceBands.bandSize ?? "")}
                  onDraftChange={() => setPriceControlsTouched(true)}
                  onCommit={setPriceBandSize}
                />
              </label>

              <div className="market-scan-fuel-bank deck-panel-grid__wide">
                <span className="market-scan-fuel-bank-label">Fuel Focus</span>
                <div className="market-scan-fuel-chip-row">
                  {fuelOptions.map((fuel) => {
                    const active = activeFuelTypes.includes(fuel);
                    return (
                      <button
                        key={fuel}
                        type="button"
                        className={`market-scan-fuel-chip${active ? " is-active" : ""}`}
                        onClick={(e) => { if (e.detail >= 2) { isolate(fuel); } else { toggle(fuel); } }}
                        title="双击只看此动力"
                        style={{
                          borderColor: active ? fuelColor(fuel) : undefined,
                          background: active ? `${fuelColor(fuel)}16` : undefined,
                        }}
                      >
                        <span className="market-scan-fuel-dot"
                          style={{ backgroundColor: fuelColor(fuel) }} aria-hidden="true" />
                        {fuel}
                      </button>
                    );
                  })}
                  {isMixedSegment ? (
                    <span className="market-scan-hero-chip market-scan-hero-chip--warn">跨Segment对比</span>
                  ) : null}
                </div>
              </div>

              <div className="version-comparison-selection-bank deck-panel-grid__wide">
                <div className="version-comparison-selection-header">
                  <span className="market-scan-fuel-bank-label">Selected Models ({activeModels.length}/{MAX_SELECTED_MODELS})</span>
                  {activeModels.length > 0 ? (
                    <button type="button" className="version-comparison-clear-btn" onClick={handleClearAll}>清空全部</button>
                  ) : null}
                </div>
                <div className="version-comparison-chip-row">
                  {selectedModelDetails.length > 0 ? selectedModelDetails.map((model) => (
                    <button
                      key={model.value}
                      type="button"
                      className="version-comparison-chip version-comparison-chip--detailed"
                      onClick={() => handleRemoveModel(model.value)}
                    >
                      <div className="version-comparison-chip-content">
                        <span className="version-comparison-chip-name">{model.label}</span>
                        <span className="version-comparison-chip-meta">
                          {[model.brand, model.segment, model.powertrain, model.lengthMm > 0 ? `${model.lengthMm}mm` : ""].filter(Boolean).join(" · ")}
                        </span>
                      </div>
                      <span className="version-comparison-chip-remove" aria-hidden="true">×</span>
                    </button>
                  )) : (
                    <span className="version-comparison-empty">
                      {comparisonMode === "free_comparison" ? "搜索车型开始对比" : "暂无可对比 Model"}
                    </span>
                  )}
                </div>
                {maxModelsReached ? (
                  <span className="version-comparison-empty">已达到最多 {MAX_SELECTED_MODELS} 个 Model</span>
                ) : null}
              </div>
            </div>
          ) : null}

          {activeControlPanel === "layout" ? (
            <div className="deck-panel-grid">
              <label className="market-scan-field">
                <span>布局</span>
                <select
                  value={layoutDirection}
                  onChange={(event) => setLayoutDirection(event.target.value === "column" ? "column" : "row")}
                >
                  <option value="row">并排</option>
                  <option value="column">上下</option>
                </select>
              </label>
              {layoutDirection === "row" ? (
                <label className="market-scan-field">
                  <span>比例 {splitRatio}/{100 - splitRatio}</span>
                  <input
                    type="range"
                    min={MIN_VC_SPLIT_RATIO}
                    max={MAX_VC_SPLIT_RATIO}
                    value={splitRatio}
                    onChange={(event) => setSplitRatio(Number(event.target.value))}
                  />
                </label>
              ) : null}
              <label className="market-scan-field deck-panel-grid__wide">
                <span>高度 {chartHeight}px</span>
                <input
                  type="range"
                  min={MIN_VC_CHART_HEIGHT}
                  max={MAX_VC_CHART_HEIGHT}
                  step={10}
                  value={chartHeight}
                  onChange={(event) => setChartHeight(Number(event.target.value))}
                />
              </label>
            </div>
          ) : null}
        </DeckFloatingDrawer>

        <PageBannerStack
          items={[
            ...(error ? [{ id: "version-comparison-error", tone: "error" as const, title: "版型对比加载失败", message: error }] : []),
            ...(exportError ? [{ id: "version-comparison-export-error", tone: "error" as const, title: "PNG 导出失败", message: exportError }] : []),
          ]}
        />

        {loading && !deck ? (
          <PageLoadingShell
            kicker="Deck"
            label="正在生成版型对比页面"
            detail="按 segment / model / 时间口径实时聚合版型明细。"
          />
        ) : null}

        {deck && page ? (
          <div className="market-scan-content" aria-busy={loading}>
            {loading ? (
              <div className="market-scan-refresh-layer">
                <LoadingSurface
                  mode="overlay"
                  kicker="Refreshing"
                  label="正在刷新版型对比结果"
                  detail="会随 segment、model、时间口径同步重算左侧价格带和右侧 version drilldown。"
                />
              </div>
            ) : null}

            <div ref={slidePreview.shellRef} className="market-scan-slide-shell">
              <div className="market-scan-slide-scale-box" style={slidePreview.scaleBoxStyle}>
                <div
                  ref={slideRef}
                  className="market-scan-slide-frame positioning-pricing-slide-frame"
                  style={slidePreview.frameStyle}
                >
                <header className="market-scan-slide-head">
                  <div className="market-scan-slide-copy">
                    <span className="market-scan-slide-kicker">09 {page.title}</span>
                    <h2>{deck.metadata.labels.pageTitle}</h2>
                    <p>{page.summaryText}</p>
                  </div>
                  <div className="market-scan-slide-meta">
                    <span className="market-scan-slide-tag">国家 {deck.metadata.selectedCountryLabel}</span>
                    <span className="market-scan-slide-tag">月份 {deck.metadata.labels.currentMonthShort}</span>
                    <span className="market-scan-slide-tag">口径 {deck.metadata.labels.salesModeLabel}</span>
                    <span className="market-scan-slide-tag">模式 {activeModeLabel}</span>
                    {deck.metadata.selectedSegment ? (
                      <span className="market-scan-slide-tag">Segment {deck.metadata.selectedSegment}</span>
                    ) : null}
                    <span className="market-scan-slide-tag">Model {deck.metadata.selectedModels.length}/{MAX_SELECTED_MODELS}</span>
                  </div>
                </header>

                <div className="market-scan-slide-body">
                  <div className="market-scan-metric-grid market-scan-metric-grid--slide">
                    {page.metrics.map((metric) => (
                      <MetricCard key={`${metric.label}-${metric.detail}`} metric={metric} />
                    ))}
                  </div>

                  <div className="market-scan-slide-content">
                    <div className="market-scan-callout positioning-pricing-summary">
                      {page.subtitle}：左侧按 MSRP 价格带看累计销量，右侧下钻到选中 Model 的 version / trim 粒度。
                    </div>

                    <div className={`market-scan-grid market-scan-grid--two-wide version-comparison-grid version-comparison-grid--${layoutDirection}`} style={vcGridStyle}>
                      <div className="positioning-pricing-panel-slot">
                        <Panel
                          eyebrow="Price Bands"
                          title="累计价格带"
                          subtitle="纵轴为 MSRP 区间，横轴为销量，按动力堆叠。"
                        >
                          <div className="positioning-pricing-chart">
                            {barTraces.length > 0 ? (
                              <PlotlyChart
                                data={barTraces}
                                layout={applyVcExportToLayout(priceBandLayout(page.priceBands.range.min, page.priceBands.range.max, page.priceBands.bandSize), priceBandExport)}
                                height={chartHeight}
                              />
                            ) : (
                              <LoadingSurface
                                mode="inline"
                                kicker="Bands"
                                label="暂无价格带数据"
                                detail="当前 segment / model 组合下没有可堆叠的价格带销量。"
                              />
                            )}
                          </div>
                        </Panel>
                      </div>

                      <div className="positioning-pricing-panel-slot">
                        <Panel
                          eyebrow="Version Drilldown"
                          title="版型细分气泡图"
                          subtitle="横轴为车长，轴下标出对应 Model，气泡文字显示 version，颜色按动总区分。"
                        >
                          <div className="positioning-pricing-chart">
                            {bubbleTraces.length > 0 ? (
                              <PlotlyChart
                                data={bubbleTraces}
                                layout={applyVcExportToLayout(versionBubbleLayout(
                                  page.priceBands.range.min,
                                  page.priceBands.range.max,
                                  page.priceBands.bandSize,
                                  bubbleAnnotations,
                                ), bubbleExport)}
                                height={chartHeight}
                                onHover={handleBubbleHover}
                                onUnhover={handleBubbleUnhover}
                                onClick={handleBubbleClick}
                                onSelected={handleBubbleSelect}
                              />
                          ) : (
                            <LoadingSurface
                              mode="inline"
                              kicker="Versions"
                              label="暂无版型气泡图数据"
                              detail="当前 segment / model / 时间口径下没有满足条件的 version。"
                            />
                          )}
                        </div>
                      </Panel>
                    </div>
                  </div>
                </div>
                </div>
              </div>
            </div>
            </div>

            <DeckExportDrawer
              open={exportToolsOpen}
              onOpenChange={handleExportDrawerOpenChange}
              triggerPrimary="导出当前页 / 图表设置"
              triggerSecondaryOpen="收起设置"
              triggerSecondaryClosed="打开设置"
              eyebrow="Export Settings"
              title="导出与图表样式"
              ariaLabel="Version comparison export settings"
              footer={(
                <>
                  <span className="market-scan-toolbar-chip">{exportPreset.width} x {vcCanvasHeight}</span>
                  <span className="market-scan-toolbar-chip">{deck.metadata.labels.salesModeLabel}</span>
                  <span className="market-scan-toolbar-chip">Bands 标签 {priceBandExport.dataLabelMode}</span>
                  <span className="market-scan-toolbar-chip">Bubble 标签 {bubbleExport.dataLabelMode}</span>
                  <span className="market-scan-toolbar-chip">{deck.metadata.selectedModels.length}/{MAX_SELECTED_MODELS} Models</span>
                </>
              )}
            >
              <div className="deck-export-quick-grid">
                <label className="market-scan-field">
                  <span>导出尺寸</span>
                  <select
                    value={exportPresetKey}
                    onChange={(event) => setExportPresetKey(event.target.value as (typeof EXPORT_PRESETS)[number]["key"])}
                  >
                    {EXPORT_PRESETS.map((preset) => (
                      <option key={preset.key} value={preset.key}>
                        {preset.label}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="market-scan-field">
                  <span>气泡标签</span>
                  <select value={labelMode} onChange={(event) => setLabelMode(event.target.value as LabelMode)}>
                    {LABEL_MODE_OPTIONS.map((opt) => (
                      <option key={opt.value} value={opt.value}>{opt.label}</option>
                    ))}
                  </select>
                </label>
              </div>
              <button
                type="button"
                className={`btn btn-primary btn-liquid deck-export-primary${exportingSlide ? " is-loading" : ""}`}
                onClick={() => { void handleExportSlide(); }}
                disabled={exportingSlide}
              >
                <span className="btn-liquid-label">{exportingSlide ? "正在导出 PNG..." : "导出当前页 PNG"}</span>
                {exportingSlide ? <span className="btn-liquid-loader" aria-hidden="true" /> : null}
              </button>

              <DeckControlTabs
                tabs={VC_EXPORT_TABS}
                activeKey={activeExportSettingsPanel}
                onChange={setActiveExportSettingsPanel}
                ariaLabel="图表导出设置"
              />

              {selectedBubbles.size > 0 ? (
                <button
                  type="button"
                  className="btn btn-sm btn-ghost"
                  style={{ marginTop: 8 }}
                  onClick={() => setSelectedBubbles(new Set())}
                >
                  清除已选气泡 ({selectedBubbles.size})
                </button>
              ) : null}

              <div className="positioning-pricing-export-settings-card">
                {activeExportSettingsPanel === "priceBands" ? (
                  <ExportPanel
                    value={priceBandExport}
                    onChange={setPriceBandExport}
                    seriesNames={activeFuelTypes}
                    labelModeOptions={VC_LABEL_MODE_OPTIONS}
                    showExportButton={false}
                    showDimensionControls={false}
                    collapsible={false}
                  />
                ) : (
                  <ExportPanel
                    value={bubbleExport}
                    onChange={setBubbleExport}
                    seriesNames={activeFuelTypes}
                    labelModeOptions={VC_LABEL_MODE_OPTIONS}
                    showExportButton={false}
                    showDimensionControls={false}
                    collapsible={false}
                  />
                )}
              </div>
            </DeckExportDrawer>
          </div>
        ) : null}
      </div>
    </div>
  );
}
