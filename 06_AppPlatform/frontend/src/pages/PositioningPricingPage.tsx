import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { DeckPeriodTimeline } from "../components/DeckPeriodTimeline";
import { DeckSubpageNav } from "../components/DeckSubpageNav";
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
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  MarketScanPeriodRange,
  PositioningPricingBubbleItem,
  PositioningPricingDeckResponse,
  PositioningPricingMetric,
  PositioningPricingPage,
  PositioningPricingPageKey,
  PositioningPricingPriceOverlay,
  PositioningPricingSalesMode,
} from "../types";
import { buildBubbleSizing } from "../utils/bubbleSizing";
import { fuelColor } from "../utils/colors";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";
import { useArrowCountryNavigation } from "../utils/useArrowCountryNavigation";
import { useFixedCanvasPreview } from "../utils/useFixedCanvasPreview";
import { useDeckLayoutControls, type DeckLayoutDirection } from "../hooks/useDeckLayoutControls";
import { useFuelChipClick } from "../hooks/useFuelChipClick";
import { useResolvedCountry } from "../hooks/useResolvedCountry";

const DEFAULT_FUEL_TYPES = ["BEV", "HEV", "PHEV", "MHEV", "ICE"];
const DEFAULT_SALES_MODE: PositioningPricingSalesMode = "month";
const DEFAULT_MSRP_MIN = 20000;
const DEFAULT_MSRP_MAX = 60000;
const DEFAULT_PRICE_BAND_SIZE = 1000;
const DEFAULT_LENGTH_MIN = 4000;
const DEFAULT_LENGTH_MAX = 5000;
const DEFAULT_LENGTH_STEP = 50;
const DEFAULT_TOP_N = 30;
const MIN_TOP_N = 1;
const MAX_TOP_N = 200;
const MAX_MSRP_INPUT = 10000000;
const MAX_LENGTH_INPUT = 10000;
const MAX_PRICE_BAND_SIZE = 200000;
const DEFAULT_BUBBLE_SCALE = 2;
type PositioningExportSettingsPanel = "priceBands" | "bubble";
type PositioningControlPanel = "filters" | "range" | "layout";

const DEFAULT_POSITIONING_LAYOUT_DIRECTION: DeckLayoutDirection = "row";
const DEFAULT_POSITIONING_SPLIT_RATIO = 20;
const DEFAULT_POSITIONING_CHART_HEIGHT = 430;
const MIN_POSITIONING_SPLIT_RATIO = 1;
const MAX_POSITIONING_SPLIT_RATIO = 99;
const MIN_POSITIONING_CHART_HEIGHT = 280;
const MAX_POSITIONING_CHART_HEIGHT = 800;
const POSITIONING_LAYOUT_DIRECTION_STORAGE_KEY = "pos_layout_dir";
const POSITIONING_SPLIT_RATIO_STORAGE_KEY = "pos_layout_split_v2";
const POSITIONING_CHART_HEIGHT_STORAGE_KEY = "pos_layout_height";
const POSITIONING_LAYOUT_STORAGE_KEYS = {
  direction: POSITIONING_LAYOUT_DIRECTION_STORAGE_KEY,
  splitRatio: POSITIONING_SPLIT_RATIO_STORAGE_KEY,
  chartHeight: POSITIONING_CHART_HEIGHT_STORAGE_KEY,
} as const;
const POSITIONING_LAYOUT_DEFAULTS = {
  direction: DEFAULT_POSITIONING_LAYOUT_DIRECTION,
  splitRatio: DEFAULT_POSITIONING_SPLIT_RATIO,
  chartHeight: DEFAULT_POSITIONING_CHART_HEIGHT,
} as const;
const POSITIONING_LAYOUT_RANGES = {
  splitRatio: {
    min: MIN_POSITIONING_SPLIT_RATIO,
    max: MAX_POSITIONING_SPLIT_RATIO,
  },
  chartHeight: {
    min: MIN_POSITIONING_CHART_HEIGHT,
    max: MAX_POSITIONING_CHART_HEIGHT,
  },
} as const;
const POSITIONING_LAYOUT_CSS_VARIABLES = {
  chartHeight: "--positioning-chart-height",
  splitRatio: "--positioning-split-ratio",
  remainderRatio: "--positioning-remainder-ratio",
} as const;
const POSITIONING_ROW_HEIGHT_CHROME = 650;
const POSITIONING_COLUMN_HEIGHT_CHROME = 830;
const BUBBLE_SCALE_OPTIONS = [1, 2, 3, 4] as const;
const POSITIONING_EXPORT_SETTINGS_TABS: Array<DeckControlTabItem<PositioningExportSettingsPanel>> = [
  { key: "priceBands", label: "Price Bands", caption: "累计价格带" },
  { key: "bubble", label: "Powertrain Bubble", caption: "动力气泡图" },
];
const POSITIONING_CONTROL_TABS: Array<DeckControlTabItem<PositioningControlPanel>> = [
  { key: "filters", label: "筛选", caption: "国家 / 月份 / 口径" },
  { key: "range", label: "范围", caption: "价格 / 车长 / 步长" },
  { key: "layout", label: "版式", caption: "布局 / 高度 / 动力" },
];
const POSITIONING_CHART_MARGIN = { l: 96, r: 24, t: 16, b: 62 } as const;
const POSITIONING_AXIS_TITLE_STANDOFF = 12;
const DEFAULT_POSITIONING_EXPORT: ExportSettings = {
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
};
const DEFAULT_PRICE_BAND_EXPORT: ExportSettings = {
  ...DEFAULT_POSITIONING_EXPORT,
  dataLabelMode: "off",
  dataLabelPosition: "outside",
};
const DEFAULT_BUBBLE_EXPORT: ExportSettings = {
  ...DEFAULT_POSITIONING_EXPORT,
  dataLabelMode: "model",
  dataLabelPosition: "top",
  dataLabelOverlapStrategy: "all",
};
const PRICE_BAND_LABEL_MODE_OPTIONS = buildExportLabelModeOptions({
  showValue: true,
  showSeries: true,
  showSales: true,
});
const BUBBLE_LABEL_MODE_OPTIONS = buildExportLabelModeOptions({
  showValue: true,
  showSeries: true,
  showModel: true,
  showSales: true,
});
const EXPORT_PRESETS = [
  { key: "hd+", label: "1600 x 900", width: 1600, height: 900 },
  { key: "fhd", label: "1920 x 1080", width: 1920, height: 1080 },
  { key: "qhd", label: "2560 x 1440", width: 2560, height: 1440 },
] as const;
const SALES_MODE_OPTIONS: Array<{ value: PositioningPricingSalesMode; label: string }> = [
  { value: "month", label: "当月" },
  { value: "ytd", label: "YTD" },
  { value: "rolling12", label: "近12个月" },
];
const PRICE_OVERLAY_REASON_LABELS: Record<string, string> = {
  "country-unresolved": "国家字段未解析",
  "duckdb-unavailable": "DuckDB 不可用",
  "database-unavailable": "应用数据库不可用",
  "duckdb-postgres-attach-failed": "DuckDB 无法挂载 PostgreSQL",
  "no-current-prices": "PG current_prices 暂无候选",
  "no-current-price-candidates": "当前页没有可覆盖的 current price 候选",
  "no-overlay-matches": "当前页未命中 reviewed price",
  "duckdb-overlay-failed": "DuckDB overlay 执行失败",
};

function clampNumber(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function readTopNInput(value: string | null): number {
  if (!value) {
    return DEFAULT_TOP_N;
  }
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return DEFAULT_TOP_N;
  }
  return Math.trunc(clampNumber(parsed, MIN_TOP_N, MAX_TOP_N));
}

function resolvePositioningCanvasHeight(
  presetHeight: number,
  layoutDirection: DeckLayoutDirection,
  chartHeight: number,
): number {
  const contentHeight = layoutDirection === "column"
    ? chartHeight * 2 + POSITIONING_COLUMN_HEIGHT_CHROME
    : chartHeight + POSITIONING_ROW_HEIGHT_CHROME;
  return Math.max(presetHeight, contentHeight);
}

const TAB_ITEMS: Array<{
  key: PositioningPricingPageKey;
  code: string;
  label: string;
  sublabel: string;
}> = [
  { key: "overview", code: "01", label: "Overview", sublabel: "全市场" },
  { key: "suvAll", code: "02", label: "SUV", sublabel: "全 SUV" },
  { key: "suvA0", code: "03", label: "SUV-A0", sublabel: "入门 SUV" },
  { key: "suvA", code: "04", label: "SUV-A", sublabel: "A级 SUV" },
  { key: "suvBPlus", code: "05", label: "SUV-B+", sublabel: "B 级及以上 SUV" },
];

function isPageKey(value: string | null): value is PositioningPricingPageKey {
  return TAB_ITEMS.some((item) => item.key === value);
}

function isSalesMode(value: string | null): value is PositioningPricingSalesMode {
  return SALES_MODE_OPTIONS.some((item) => item.value === value);
}

function formatMetricValue(value: number | string): string {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString("en-US");
  }
  return String(value ?? "-");
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

function bubbleTextPosition(index: number): string {
  const positions = ["top center", "middle right", "bottom center", "middle left"] as const;
  return positions[index % positions.length];
}

type PositioningPricingTruthLayer = {
  chipLabel: string;
  title: string;
  detail: string;
};

function formatPositioningPriceOverlayReason(reason?: string | null): string | null {
  const value = String(reason ?? "").trim();
  if (!value) {
    return null;
  }
  const [prefix, ...rest] = value.split(":");
  const label = PRICE_OVERLAY_REASON_LABELS[prefix.trim()] ?? prefix.trim();
  const suffix = rest.join(":").trim();
  return suffix ? `${label}（${suffix}）` : label;
}

function buildPositioningTruthLayer(overlay?: PositioningPricingPriceOverlay | null): PositioningPricingTruthLayer | null {
  if (!overlay) {
    return null;
  }
  const sourceMode = String(overlay.mode ?? overlay.sourceMode ?? "").trim();
  const matchedRows = overlay.matchedRows ?? 0;
  const matchedModels = overlay.matchedModels ?? 0;
  const linkMatches = overlay.linkMatches ?? 0;
  const directMatches = overlay.directMatches ?? 0;
  const candidateRows = overlay.candidateRows ?? 0;
  const linkCandidateRows = overlay.linkCandidateRows ?? 0;
  const reason = formatPositioningPriceOverlayReason(overlay.reason);

  if (sourceMode === "duckdb-overlay" && matchedRows > 0) {
    return {
      chipLabel: "Reviewed MSRP overlay 已命中",
      title: "价格真值层",
      detail: `当前页已命中 reviewed PG current price，覆盖 ${matchedRows.toLocaleString("en-US")} 行 / ${matchedModels.toLocaleString("en-US")} 个车型；link 命中 ${linkMatches.toLocaleString("en-US")} 行，direct 命中 ${directMatches.toLocaleString("en-US")} 行。`,
    };
  }

  if (sourceMode === "duckdb-postgres-attach") {
    return {
      chipLabel: "Reviewed MSRP overlay 部分命中",
      title: "价格真值层",
      detail: `已连接 PG reviewed price layer，但当前页并非全部命中；current price 候选 ${candidateRows.toLocaleString("en-US")} 行，link 候选 ${linkCandidateRows.toLocaleString("en-US")} 行。${reason ? `原因：${reason}。` : "未命中的车型仍会回落到 parquet MSRP。"}`,
    };
  }

  return {
    chipLabel: "Parquet MSRP fallback",
    title: "价格真值层",
    detail: `当前页未命中 reviewed PG current price，仍使用 parquet MSRP。${reason ? `原因：${reason}。` : ""}`,
  };
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

function buildPriceBandTraces(
  page: PositioningPricingPage,
  fuelOrder: string[],
): Data[] {
  return fuelOrder.map((fuel) => {
    const salesValues = page.priceBands.items.map((item) => item.fuelMix[fuel] ?? 0);
    return withExportLabels({
      type: "bar",
      orientation: "h",
      name: fuel,
      y: page.priceBands.items.map((item) => item.bandMid),
      x: salesValues,
      width: page.priceBands.items.map((item) => Math.max(item.bandWidth * 0.84, 500)),
      customdata: page.priceBands.items.map((item) => [item.label]),
      marker: { color: fuelColor(fuel) },
      hovertemplate: `%{customdata[0]}<br>${fuel}: %{x:,.0f} 台<extra></extra>`,
    } as Data, {
      model: page.priceBands.items.map((item) => item.label),
      sales: salesValues,
      value: salesValues,
      series: page.priceBands.items.map(() => fuel),
    });
  });
}

function buildBubbleTraces(
  items: PositioningPricingBubbleItem[],
  fuelOrder: string[],
  bubbleScale: number,
  labelFontSize = 9,
  msrpMode: "min" | "median" = "min",
): Data[] {
  const sizing = buildBubbleSizing(items.map((item) => item.sales), {
    maxDiameter: 28 * bubbleScale,
    minDiameter: 4,
  });
  const msrpField = msrpMode === "median" ? "msrp" as const : "msrpMin" as const;
  const msrpLabel = msrpMode === "median" ? "中位 MSRP" : "最低 MSRP";
  const otherLabel = msrpMode === "median" ? "最低 MSRP" : "中位 MSRP";
  return fuelOrder.flatMap((fuel) => {
    const fuelItems = items.filter((item) => item.powertrain === fuel);
    if (fuelItems.length === 0) {
      return [];
    }
    const labelPosition = bubbleTextPosition(fuelOrder.indexOf(fuel));
    return [withExportLabels({
      type: "scatter",
      mode: "markers",
      name: fuel,
      x: fuelItems.map((item) => item.length),
      y: fuelItems.map((item) => item[msrpField]),
      textposition: labelPosition,
      textfont: { size: labelFontSize, color: "#334155" },
      cliponaxis: false,
      customdata: fuelItems.map((item) => [
        item.model,
        item.brand,
        item.segment,
        msrpMode === "median" ? item.msrpMin : item.msrp,
        item.msrpMax,
        item.sales,
        item.variantCount,
      ]),
      marker: {
        color: fuelColor(fuel),
        opacity: 0.82,
        line: { color: "rgba(15, 23, 42, 0.28)", width: 1 },
        size: fuelItems.map((item) => Math.max(0, item.sales)),
        sizemode: sizing.sizemode,
        sizeref: sizing.sizeref,
        sizemin: sizing.sizemin,
      },
      hovertemplate:
        `Model: %{customdata[0]}<br>Brand: %{customdata[1]}<br>Segment: %{customdata[2]}<br>Length: %{x:,.0f} mm`
        + `<br>${msrpLabel}: %{y:,.0f}<br>${otherLabel}: %{customdata[3]:,.0f}<br>最高 MSRP: %{customdata[4]:,.0f}`
        + "<br>Sales: %{customdata[5]:,.0f}<br>聚合版型数: %{customdata[6]:,.0f}<extra>%{fullData.name}</extra>",
    } as Data, {
      model: fuelItems.map((item) => item.model.trim()),
      sales: fuelItems.map((item) => item.sales),
      value: fuelItems.map((item) => item[msrpField]),
      series: fuelItems.map(() => fuel),
    })];
  });
}

function priceBandLayout(
  page: PositioningPricingPage,
  exportSettings: ExportSettings,
): Partial<PlotlyLayout> {
  const showDataLabels = exportSettings.dataLabelMode !== "off";
  const maxSales = Math.max(0, ...page.priceBands.items.map((item) => item.sales));
  return {
    ...CHART_LAYOUT,
    barmode: "stack",
    margin: { ...POSITIONING_CHART_MARGIN, r: showDataLabels ? 70 : POSITIONING_CHART_MARGIN.r },
    xaxis: {
      title: { text: "Sales", standoff: POSITIONING_AXIS_TITLE_STANDOFF },
      automargin: true,
      exponentformat: "none",
      zeroline: false,
      ...(showDataLabels && maxSales > 0 ? { range: [0, maxSales * 1.18] } : {}),
    },
    yaxis: msrpYAxisLayout(page),
  };
}

function bubbleLayout(page: PositioningPricingPage): Partial<PlotlyLayout> {
  const lengthMin = page.lengthRange.min;
  const lengthMax = page.lengthRange.max;
  return {
    ...CHART_LAYOUT,
    margin: POSITIONING_CHART_MARGIN,
    xaxis: {
      title: { text: "Length (mm)", standoff: POSITIONING_AXIS_TITLE_STANDOFF },
      automargin: true,
      range: [lengthMin, lengthMax],
      tickformat: "d",
      exponentformat: "none",
      zeroline: false,
    },
    yaxis: msrpYAxisLayout(page),
  };
}

function msrpYAxisLayout(page: PositioningPricingPage): Partial<PlotlyLayout>["yaxis"] {
  const rangeMin = page.priceBands.range.min;
  const lowerPadding = Math.max(page.priceBands.bandSize * 0.6, 400);
  return {
    title: { text: "MSRP", standoff: POSITIONING_AXIS_TITLE_STANDOFF },
    automargin: true,
    range: [Math.max(0, rangeMin - lowerPadding), page.priceBands.range.max],
    tick0: rangeMin,
    dtick: page.priceBands.bandSize,
    tickformat: "d",
    exponentformat: "none",
    zeroline: false,
  };
}

function positioningSeriesKey(trace: Data): string | null {
  const name = (trace as { name?: unknown }).name;
  return typeof name === "string" && name.trim() ? name : null;
}

function buildPositioningSeriesColors(
  traces: Data[],
  exportSettings: ExportSettings,
): Record<string, string> {
  const manualColors = exportSettings.seriesColors ?? {};
  if (exportSettings.colorScheme === DEFAULT_POSITIONING_EXPORT.colorScheme) {
    return manualColors;
  }
  const palette = getExportPalette(exportSettings.colorScheme);
  const resolved: Record<string, string> = { ...manualColors };
  const assigned = new Set(Object.keys(manualColors));
  let paletteIndex = 0;

  traces.forEach((trace) => {
    const key = positioningSeriesKey(trace);
    if (!key || assigned.has(key)) {
      return;
    }
    resolved[key] = palette[paletteIndex % palette.length];
    assigned.add(key);
    paletteIndex += 1;
  });
  return resolved;
}

function applyPositioningExportToTraces(
  traces: Data[],
  exportSettings: ExportSettings,
): Data[] {
  const labeled = applyDataLabelsToTraces(traces, exportSettings);
  const colorOverrides = buildPositioningSeriesColors(traces, exportSettings);
  return applySeriesColors(labeled, colorOverrides);
}

function applyPositioningExportToLayout(
  layout: Partial<PlotlyLayout>,
  exportSettings: ExportSettings,
): Partial<PlotlyLayout> {
  return applyExportToLayout(layout, { ...exportSettings, chartTitle: "" });
}

export function PositioningPricingPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const { country: defaultCountry } = useResolvedCountry("zh");
  const [deck, setDeck] = useState<PositioningPricingDeckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportToolsOpen, setExportToolsOpen] = useState(false);
  const [activeExportSettingsPanel, setActiveExportSettingsPanel] = useState<PositioningExportSettingsPanel>("bubble");
  const [controlToolsOpen, setControlToolsOpen] = useState(false);
  const [activeControlPanel, setActiveControlPanel] = useState<PositioningControlPanel>("filters");
  const [exportPresetKey, setExportPresetKey] = useState<(typeof EXPORT_PRESETS)[number]["key"]>("fhd");
  const [priceBandExport, setPriceBandExport] = useState<ExportSettings>(DEFAULT_PRICE_BAND_EXPORT);
  const [bubbleExport, setBubbleExport] = useState<ExportSettings>(DEFAULT_BUBBLE_EXPORT);
  const [bubbleScale, setBubbleScale] = useState<number>(DEFAULT_BUBBLE_SCALE);
  const [reloadToken, setReloadToken] = useState(0);
  const slideRef = useRef<HTMLDivElement | null>(null);
  const [activePage, setActivePage] = useState<PositioningPricingPageKey>(
    () => {
      const requested = searchParams.get("activePage");
      return isPageKey(requested) ? requested : "overview";
    },
  );
  const [selectedCountry, setSelectedCountry] = useState<string | null>(
    () => searchParams.get("country") || defaultCountry,
  );
  const [selectedPeriod, setSelectedPeriod] = useState<string | null>(
    () => searchParams.get("period"),
  );
  const [selectedTimeRange, setSelectedTimeRange] = useState<MarketScanPeriodRange | null>(
    () => readSearchTimeRange(searchParams),
  );
  const [salesMode, setSalesMode] = useState<PositioningPricingSalesMode>(
    () => {
      const requested = searchParams.get("salesMode");
      return isSalesMode(requested) ? requested : DEFAULT_SALES_MODE;
    },
  );
  const [selectedFuelTypes, setSelectedFuelTypes] = useState<string[]>(
    () => {
      const raw = searchParams.get("fuelTypes");
      return raw ? raw.split(",") : DEFAULT_FUEL_TYPES;
    },
  );
  const [topN, setTopN] = useState<number>(() => readTopNInput(searchParams.get("topN")));
  const [msrpMin, setMsrpMin] = useState<number | null>(() => {
    const raw = searchParams.get("msrpMin");
    return raw ? Number(raw) : DEFAULT_MSRP_MIN;
  });
  const [msrpMax, setMsrpMax] = useState<number | null>(() => {
    const raw = searchParams.get("msrpMax");
    return raw ? Number(raw) : DEFAULT_MSRP_MAX;
  });
  const [priceBandSize, setPriceBandSize] = useState<number | null>(() => {
    const raw = searchParams.get("priceBandSize");
    return raw ? Number(raw) : DEFAULT_PRICE_BAND_SIZE;
  });
  const [msrpMode, setMsrpMode] = useState<"min" | "median">(() => {
    const raw = searchParams.get("msrpMode");
    return raw === "median" ? "median" : "min";
  });
  const [lengthMin, setLengthMin] = useState<number | null>(() => {
    const raw = searchParams.get("lengthMin");
    return raw ? Number(raw) : DEFAULT_LENGTH_MIN;
  });
  const [lengthMax, setLengthMax] = useState<number | null>(() => {
    const raw = searchParams.get("lengthMax");
    return raw ? Number(raw) : DEFAULT_LENGTH_MAX;
  });
  const {
    layoutDirection,
    splitRatio,
    chartHeight,
    gridStyle: positioningGridStyle,
    setLayoutDirection,
    setSplitRatio,
    setChartHeight,
    resetLayout: resetPositioningLayoutControls,
  } = useDeckLayoutControls({
    storageKeys: POSITIONING_LAYOUT_STORAGE_KEYS,
    defaults: POSITIONING_LAYOUT_DEFAULTS,
    ranges: POSITIONING_LAYOUT_RANGES,
    cssVariables: POSITIONING_LAYOUT_CSS_VARIABLES,
  });
  const countryOptions = deck?.metadata.availableCountries ?? [];

  const syncUrlParams = useCallback(() => {
    const params = new URLSearchParams();
    if (selectedCountry) params.set("country", selectedCountry);
    if (selectedPeriod) params.set("period", selectedPeriod);
    if (selectedTimeRange) {
      params.set("timeStart", selectedTimeRange.start);
      params.set("timeEnd", selectedTimeRange.end);
    }
    if (activePage !== "overview") params.set("activePage", activePage);
    if (salesMode !== DEFAULT_SALES_MODE) params.set("salesMode", salesMode);
    if (topN !== DEFAULT_TOP_N) params.set("topN", String(topN));
    if (msrpMin !== null) params.set("msrpMin", String(msrpMin));
    if (msrpMax !== null) params.set("msrpMax", String(msrpMax));
    if (lengthMin !== null) params.set("lengthMin", String(lengthMin));
    if (lengthMax !== null) params.set("lengthMax", String(lengthMax));
    if (priceBandSize !== null) params.set("priceBandSize", String(priceBandSize));
    if (msrpMode !== "min") params.set("msrpMode", msrpMode);
    const fuels = selectedFuelTypes.slice().sort().join(",");
    const defaultFuels = DEFAULT_FUEL_TYPES.slice().sort().join(",");
    if (fuels && fuels !== defaultFuels) {
      params.set("fuelTypes", selectedFuelTypes.join(","));
    }
    setSearchParams(params, { replace: true });
  }, [activePage, lengthMax, lengthMin, msrpMax, msrpMin, msrpMode, priceBandSize, salesMode, selectedCountry, selectedFuelTypes, selectedPeriod, selectedTimeRange, setSearchParams, topN]);

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
    setLoading(true);
    setError("");
    api.positioningPricingDeck({
      country: selectedCountry || undefined,
      target_period: selectedPeriod || undefined,
      time_range: selectedTimeRange || undefined,
      fuel_types: selectedFuelTypes,
      sales_mode: salesMode,
      top_n: topN,
      msrp_min: msrpMin,
      msrp_max: msrpMax,
      length_min: lengthMin,
      length_max: lengthMax,
      price_band_size: priceBandSize,
      msrp_mode: msrpMode,
    })
      .then((response) => {
        setDeck(response);
      })
      .catch((reason: Error) => {
        setError(reason.message);
      })
      .finally(() => {
        setLoading(false);
      });
  }, [lengthMax, lengthMin, msrpMax, msrpMin, priceBandSize, reloadToken, salesMode, selectedCountry, selectedFuelTypes, selectedPeriod, selectedTimeRange, topN]);

  useEffect(() => {
    if (!deck) {
      return;
    }
    if (
      selectedCountry
      && !deck.metadata.availableCountries.some((item) => item.value === selectedCountry)
    ) {
      setSelectedCountry(deck.metadata.selectedCountry);
    }
    if (
      selectedPeriod
      && !deck.metadata.availablePeriods.some((item) => item.value === selectedPeriod)
    ) {
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
    const availableFuelSet = new Set(deck.metadata.availableFuelTypes);
    const normalized = selectedFuelTypes.filter((fuel) => availableFuelSet.has(fuel));
    if (normalized.length !== selectedFuelTypes.length && deck.metadata.selectedFuelTypes.length > 0) {
      setSelectedFuelTypes(deck.metadata.selectedFuelTypes);
    }
  }, [deck, selectedCountry, selectedFuelTypes, selectedPeriod, selectedTimeRange]);

  const currentCountry = selectedCountry ?? deck?.metadata.selectedCountry ?? defaultCountry;
  const resolvedTimeRange = selectedTimeRange ?? deck?.metadata.selectedTimeRange ?? null;
  const customRangeActive = isCustomTimeRange(resolvedTimeRange);
  const fuelOptions = deck?.metadata.availableFuelTypes ?? DEFAULT_FUEL_TYPES;
  const activeFuelTypes = selectedFuelTypes.length > 0
    ? selectedFuelTypes
    : (deck?.metadata.selectedFuelTypes ?? DEFAULT_FUEL_TYPES);
  const page = deck?.pages[activePage];
  const activeTab = TAB_ITEMS.find((item) => item.key === activePage) ?? TAB_ITEMS[0];
  const exportPreset = EXPORT_PRESETS.find((item) => item.key === exportPresetKey) ?? EXPORT_PRESETS[1];
  const positioningCanvasHeight = resolvePositioningCanvasHeight(exportPreset.height, layoutDirection, chartHeight);
  const slidePreview = useFixedCanvasPreview({
    width: exportPreset.width,
    height: positioningCanvasHeight,
    exporting: exportingSlide,
  });
  const positioningGridClassName = [
    "market-scan-grid",
    "market-scan-grid--two-wide",
    "positioning-pricing-grid",
    `positioning-pricing-grid--${layoutDirection}`,
  ].join(" ");
  const barTraces = useMemo(
    () => (page ? applyPositioningExportToTraces(buildPriceBandTraces(page, activeFuelTypes), priceBandExport) : []),
    [activeFuelTypes, page, priceBandExport],
  );
  const bubbleTraces = useMemo(
    () => (
      page
        ? applyPositioningExportToTraces(
            buildBubbleTraces(page.bubbleChart.items, activeFuelTypes, bubbleScale, bubbleExport.labelFontSize ?? bubbleExport.fontSize, msrpMode),
            bubbleExport,
          )
        : []
    ),
    [activeFuelTypes, bubbleExport, bubbleScale, msrpMode, page],
  );
  const priceBandChartKey = [
    "price",
    priceBandExport.dataLabelMode,
    priceBandExport.dataLabelPosition,
    priceBandExport.dataLabelOverlapStrategy,
    priceBandExport.fontSize,
    priceBandExport.labelFontSize ?? priceBandExport.fontSize,
    priceBandExport.decimalPlaces,
  ].join("-");
  const bubbleChartKey = [
    "priceBands",
    bubbleExport.dataLabelMode,
    bubbleExport.dataLabelPosition,
    bubbleExport.dataLabelOverlapStrategy,
    bubbleExport.fontSize,
    bubbleExport.labelFontSize ?? bubbleExport.fontSize,
    bubbleExport.decimalPlaces,
  ].join("-");
  const priceTruthLayer = useMemo(
    () => buildPositioningTruthLayer(deck?.metadata.priceOverlay),
    [deck],
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
      const exportHeight = Math.max(positioningCanvasHeight, Math.ceil(slideRef.current.scrollHeight));
      const dataUrl = await toPng(slideRef.current, {
        cacheBust: true,
        pixelRatio: 2,
        backgroundColor: "#eef4f7",
        width: exportPreset.width,
        height: exportHeight,
        canvasWidth: exportPreset.width,
        canvasHeight: exportHeight,
        style: {
          width: `${exportPreset.width}px`,
          height: `${exportHeight}px`,
        },
      });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = [
        "positioning-pricing",
        sanitizeFileNameSegment(deck.metadata.selectedCountryLabel),
        deck.metadata.resolvedPeriod,
        page.key,
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
        <section className="header-card dashboard-hero market-scan-hero positioning-pricing-hero positioning-pricing-summary-hero">
          <div className="dashboard-hero-head positioning-pricing-summary-head">
            <div className="dashboard-hero-copy market-scan-hero-copy">
              <span className="page-kicker">Positioning Pricing</span>
              <h1>{deck?.metadata.labels.pageTitle ?? "定位定价"}</h1>
              <p>{page?.summaryText ?? "按国家、月份与动力筛选固定版式的定位定价页。"}</p>
              <div className="market-scan-hero-ribbon">
                <span className="market-scan-hero-chip">国家 {deck?.metadata.selectedCountryLabel ?? currentCountry}</span>
                <span className="market-scan-hero-chip">月份 {customRangeActive ? (resolvedTimeRange ? `${resolvedTimeRange.start}~${resolvedTimeRange.end}` : (deck?.metadata.labels.currentMonthShort ?? "Latest")) : (deck?.metadata.labels.currentMonthShort ?? "Latest")}</span>
                <span className="market-scan-hero-chip">动力 {activeFuelTypes.join(" / ")}</span>
                <span className="market-scan-hero-chip">页面 {activeTab.label}</span>
                <span className="market-scan-hero-chip">口径 {customRangeActive ? "自定义区间累计" : (deck?.metadata.labels.salesModeLabel ?? SALES_MODE_OPTIONS.find((item) => item.value === salesMode)?.label ?? "当月")}</span>
                <span className="market-scan-hero-chip">Top {topN}</span>
                {priceTruthLayer ? <span className="market-scan-hero-chip">{priceTruthLayer.chipLabel}</span> : null}
                <span className="market-scan-hero-chip">
                  MSRP {formatMetricValue(page?.priceBands.range.min ?? msrpMin ?? 0)}-{formatMetricValue(page?.priceBands.range.max ?? msrpMax ?? 0)}
                </span>
                <span className="market-scan-hero-chip">
                  Length {formatMetricValue(page?.lengthRange.min ?? lengthMin ?? 0)}-{formatMetricValue(page?.lengthRange.max ?? lengthMax ?? 0)} mm
                </span>
                {loading && deck ? <span className="market-scan-hero-chip market-scan-hero-chip--live">Refreshing</span> : null}
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
          ariaLabel="Positioning Pricing controls"
          footer={(
            <>
              <span className="market-scan-toolbar-chip">{deck?.metadata.selectedCountryLabel ?? currentCountry}</span>
              <span className="market-scan-toolbar-chip">{customRangeActive ? "自定义区间" : (deck?.metadata.labels.salesModeLabel ?? "当月")}</span>
              <span className="market-scan-toolbar-chip">Top {topN}</span>
              <span className="market-scan-toolbar-chip">{layoutDirection === "row" ? `并排 ${splitRatio}/${100 - splitRatio}` : "上下"}</span>
            </>
          )}
        >
          <DeckControlTabs
            tabs={POSITIONING_CONTROL_TABS}
            activeKey={activeControlPanel}
            onChange={setActiveControlPanel}
            ariaLabel="定位定价控制"
            className="positioning-pricing-control-tabs"
            tabClassName="positioning-pricing-control-tab"
          />

          {activeControlPanel === "filters" ? (
            <div className="positioning-pricing-control-grid">
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
              <label className="market-scan-field">
                <span>Top N</span>
                <DebouncedNumberInput
                  value={topN}
                  onCommit={(value) => {
                    if (value !== null) {
                      setTopN(value);
                    }
                  }}
                  min={MIN_TOP_N}
                  max={MAX_TOP_N}
                  step={1}
                  placeholder={String(DEFAULT_TOP_N)}
                />
              </label>
              <div className="market-scan-field positioning-pricing-control-field--wide">
                <DeckPeriodTimeline
                  options={deck?.metadata.availablePeriods ?? []}
                  value={resolvedTimeRange ?? (selectedPeriod ? { start: selectedPeriod, end: selectedPeriod } : null)}
                  onChange={(value) => {
                    setSelectedTimeRange(isCustomTimeRange(value) ? value : null);
                    setSelectedPeriod(value?.end ?? null);
                  }}
                  disabled={!deck}
                />
              </div>
              <div className="market-scan-field positioning-pricing-control-field--wide">
                <span>销量口径</span>
                <div className="btn-group">
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
                {customRangeActive ? (
                  <small className="market-scan-field-hint">当前时间轴就是激活中的销量口径；点击三档按钮会退出自定义区间。</small>
                ) : null}
              </div>
            </div>
          ) : null}

          {activeControlPanel === "range" ? (
            <div className="positioning-pricing-control-grid">
              <label className="market-scan-field positioning-pricing-control-field--wide">
                <span>MSRP 口径</span>
                <div className="btn-group" style={{ width: "100%" }}>
                  <button
                    type="button"
                    className={`btn btn-sm${msrpMode === "min" ? " btn-primary" : " btn-ghost"}`}
                    style={{ flex: 1, fontSize: 11 }}
                    onClick={() => setMsrpMode("min")}
                  >
                    最低 MSRP
                  </button>
                  <button
                    type="button"
                    className={`btn btn-sm${msrpMode === "median" ? " btn-primary" : " btn-ghost"}`}
                    style={{ flex: 1, fontSize: 11 }}
                    onClick={() => setMsrpMode("median")}
                  >
                    中位 MSRP
                  </button>
                </div>
              </label>
              <label className="market-scan-field">
                <span>MSRP Min</span>
                <DebouncedNumberInput
                  value={msrpMin}
                  min={0}
                  max={MAX_MSRP_INPUT}
                  step={1000}
                  allowEmpty
                  placeholder={String(page?.priceBands.range.min ?? "")}
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
                  placeholder={String(page?.priceBands.range.max ?? "")}
                  onCommit={setMsrpMax}
                />
              </label>
              <label className="market-scan-field">
                <span>Length Min</span>
                <DebouncedNumberInput
                  value={lengthMin}
                  min={0}
                  max={MAX_LENGTH_INPUT}
                  step={DEFAULT_LENGTH_STEP}
                  allowEmpty
                  placeholder={String(page?.lengthRange.min ?? "")}
                  onCommit={setLengthMin}
                />
              </label>
              <label className="market-scan-field">
                <span>Length Max</span>
                <DebouncedNumberInput
                  value={lengthMax}
                  min={0}
                  max={MAX_LENGTH_INPUT}
                  step={DEFAULT_LENGTH_STEP}
                  allowEmpty
                  placeholder={String(page?.lengthRange.max ?? "")}
                  onCommit={setLengthMax}
                />
              </label>
              <label className="market-scan-field positioning-pricing-control-field--wide">
                <span>Step</span>
                <DebouncedNumberInput
                  value={priceBandSize}
                  min={500}
                  max={MAX_PRICE_BAND_SIZE}
                  step={500}
                  allowEmpty
                  placeholder={String(page?.priceBands.bandSize ?? "")}
                  onCommit={setPriceBandSize}
                />
              </label>
            </div>
          ) : null}

          {activeControlPanel === "layout" ? (
            <div className="positioning-pricing-control-grid">
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
                    min={MIN_POSITIONING_SPLIT_RATIO}
                    max={MAX_POSITIONING_SPLIT_RATIO}
                    value={splitRatio}
                    onChange={(event) => setSplitRatio(Number(event.target.value))}
                  />
                </label>
              ) : null}
              <label className="market-scan-field positioning-pricing-control-field--wide">
                <span>高度 {chartHeight}px</span>
                <input
                  type="range"
                  min={MIN_POSITIONING_CHART_HEIGHT}
                  max={MAX_POSITIONING_CHART_HEIGHT}
                  step={10}
                  value={chartHeight}
                  onChange={(event) => setChartHeight(Number(event.target.value))}
                />
              </label>
              <div className="market-scan-fuel-bank positioning-pricing-control-field--wide">
                <span className="market-scan-fuel-bank-label">Fuel Focus</span>
                <div className="market-scan-fuel-chip-row">
                  {fuelOptions.map((fuel) => {
                    const active = activeFuelTypes.includes(fuel);
                    return (
                      <button
                        key={fuel}
                        type="button"
                        className={`market-scan-fuel-chip${active ? " is-active" : ""}`}
                        onClick={() => toggle(fuel)}
                        onDoubleClick={() => isolate(fuel)}
                        title="双击只看此动力"
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
              <div className="market-scan-field market-scan-field-actions positioning-pricing-control-field--wide">
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
                    className="btn btn-primary btn-sm"
                    onClick={() => { void handleExportSlide(); }}
                    disabled={!deck || !page || exportingSlide}
                  >
                    {exportingSlide ? "正在导出 PNG..." : "导出当前页 PNG"}
                  </button>
                  <button
                    type="button"
                    className="btn btn-ghost btn-sm"
                    onClick={() => {
                      setSelectedCountry(defaultCountry);
                      setSelectedPeriod(null);
                      setSalesMode(DEFAULT_SALES_MODE);
                      setSelectedFuelTypes(DEFAULT_FUEL_TYPES);
                      setTopN(DEFAULT_TOP_N);
                      setMsrpMin(DEFAULT_MSRP_MIN);
                      setMsrpMax(DEFAULT_MSRP_MAX);
                      setMsrpMode("min");
                      setPriceBandSize(DEFAULT_PRICE_BAND_SIZE);
                      setLengthMin(DEFAULT_LENGTH_MIN);
                      setLengthMax(DEFAULT_LENGTH_MAX);
                      setPriceBandExport(DEFAULT_PRICE_BAND_EXPORT);
                      setBubbleExport(DEFAULT_BUBBLE_EXPORT);
                      setBubbleScale(DEFAULT_BUBBLE_SCALE);
                      setActivePage("overview");
                      resetPositioningLayoutControls();
                    }}
                  >
                    Reset
                  </button>
                </div>
              </div>
            </div>
          ) : null}
        </DeckFloatingDrawer>

        <DeckSubpageNav
          items={TAB_ITEMS}
          activeKey={activePage}
          onSelect={setActivePage}
          ariaLabel="Positioning Pricing Pages"
          tabsClassName="positioning-pricing-tab-strip"
        />

        {error ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>定位定价加载失败</strong>
            <p>{error}</p>
          </section>
        ) : null}

        {loading && !deck ? (
          <section className="market-scan-state-card">
            <LoadingSurface
              mode="inline"
              kicker="Deck"
              label="正在生成定位定价页面"
              detail="按国家、月份与动力实时聚合价格带与气泡定位数据。"
            />
          </section>
        ) : null}

        {exportError ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>PNG 导出失败</strong>
            <p>{exportError}</p>
          </section>
        ) : null}

        {deck && page ? (
          <div className="market-scan-content" aria-busy={loading}>
            {loading ? (
              <div className="market-scan-refresh-layer">
                <LoadingSurface
                  mode="overlay"
                  kicker="Refreshing"
                  label="正在刷新定位定价结果"
                  detail="新页面沿用 market scan 固定版式，但价格定位改用最低 MSRP。"
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
                    <span className="market-scan-slide-kicker">{activeTab.code} {page.title}</span>
                    <h2>{deck.metadata.labels.pageTitle}</h2>
                    <p>{page.summaryText}</p>
                  </div>
                  <div className="market-scan-slide-meta">
                    <span className="market-scan-slide-tag">国家 {deck.metadata.selectedCountryLabel}</span>
                    <span className="market-scan-slide-tag">月份 {deck.metadata.labels.currentMonthShort}</span>
                    <span className="market-scan-slide-tag">口径 {deck.metadata.labels.salesModeLabel}</span>
                    <span className="market-scan-slide-tag">动力 {activeFuelTypes.join(" / ")}</span>
                    <span className="market-scan-slide-tag">Top {topN}</span>
                    <span className="market-scan-slide-tag">
                      MSRP {page.priceBands.range.min.toLocaleString("en-US")}-{page.priceBands.range.max.toLocaleString("en-US")}
                    </span>
                    <span className="market-scan-slide-tag">
                      Length {page.lengthRange.min.toLocaleString("en-US")}-{page.lengthRange.max.toLocaleString("en-US")} mm
                    </span>
                    <span className="market-scan-slide-tag">价格带步长 {page.priceBands.bandSize.toLocaleString("en-US")}</span>
                  </div>
                </header>

                <div className="market-scan-slide-body">
                  <div className="market-scan-metric-grid market-scan-metric-grid--slide">
                    {page.metrics.map((metric) => (
                      <MetricCard key={`${page.key}-${metric.label}`} metric={metric} />
                    ))}
                  </div>

                  <div className="market-scan-slide-content">
                    <div
                      className={positioningGridClassName}
                      style={positioningGridStyle}
                    >
                      <div className="positioning-pricing-panel-slot">
                        <Panel
                          eyebrow="Price Bands"
                          title="累计价格带"
                        >
                          <div className="positioning-pricing-chart">
                            {barTraces.length > 0 ? (
                              <PlotlyChart
                                key={priceBandChartKey}
                                data={barTraces}
                                layout={applyPositioningExportToLayout(priceBandLayout(page, priceBandExport), priceBandExport)}
                                height={chartHeight}
                              />
                            ) : (
                              <LoadingSurface
                                mode="inline"
                                kicker="Bands"
                                label="暂无价格带数据"
                                detail="当前国家 / 月份 / 动力条件下没有可堆叠的价格带销量。"
                              />
                            )}
                          </div>
                        </Panel>
                      </div>

                      <div className="positioning-pricing-panel-slot">
                        <Panel
                          eyebrow="Powertrain Bubble"
                          title="动力气泡图"
                        >
                          <div className="positioning-pricing-chart">
                            {bubbleTraces.length > 0 ? (
                              <PlotlyChart
                                key={bubbleChartKey}
                                data={bubbleTraces}
                                layout={applyPositioningExportToLayout(bubbleLayout(page), bubbleExport)}
                                height={chartHeight}
                              />
                            ) : (
                              <LoadingSurface
                                mode="inline"
                                kicker="Bubble"
                                label="暂无气泡图数据"
                                detail="当前页没有符合条件的销量气泡。"
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
              ariaLabel="Positioning Pricing export settings"
              footer={(
                <>
                  <span className="market-scan-toolbar-chip">{exportPreset.width} x {positioningCanvasHeight}</span>
                  <span className="market-scan-toolbar-chip">{deck.metadata.labels.salesModeLabel}</span>
                  <span className="market-scan-toolbar-chip">{activeTab.label}</span>
                  <span className="market-scan-toolbar-chip">Bands 标签 {priceBandExport.dataLabelMode}</span>
                  <span className="market-scan-toolbar-chip">Bubble 标签 {bubbleExport.dataLabelMode}</span>
                  <span className="market-scan-toolbar-chip">气泡 ×{bubbleScale}</span>
                  <span className="market-scan-toolbar-chip">{deck.metadata.selectedCountryLabel}</span>
                  <span className="market-scan-toolbar-chip">{deck.metadata.resolvedPeriod}</span>
                </>
              )}
            >
                    <div className="positioning-pricing-export-quick-grid">
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
                        <span>气泡倍率</span>
                        <select
                          value={bubbleScale}
                          onChange={(event) => setBubbleScale(Number(event.target.value))}
                        >
                          {BUBBLE_SCALE_OPTIONS.map((option) => (
                            <option key={option} value={option}>
                              ×{option}
                            </option>
                          ))}
                        </select>
                      </label>
                    </div>

                    <button
                      type="button"
                      className={`btn btn-primary btn-liquid positioning-pricing-export-primary${exportingSlide ? " is-loading" : ""}`}
                      onClick={() => { void handleExportSlide(); }}
                      disabled={exportingSlide}
                    >
                      <span className="btn-liquid-label">{exportingSlide ? "正在导出 PNG..." : "导出当前页 PNG"}</span>
                      {exportingSlide ? <span className="btn-liquid-loader" aria-hidden="true" /> : null}
                    </button>

                    <DeckControlTabs
                      tabs={POSITIONING_EXPORT_SETTINGS_TABS}
                      activeKey={activeExportSettingsPanel}
                      onChange={setActiveExportSettingsPanel}
                      ariaLabel="图表导出设置"
                      className="positioning-pricing-export-tabs"
                      tabClassName="positioning-pricing-export-tab"
                    />

                    <div className="positioning-pricing-export-settings-card">
                      {activeExportSettingsPanel === "priceBands" ? (
                        <ExportPanel
                          value={priceBandExport}
                          onChange={setPriceBandExport}
                          seriesNames={activeFuelTypes}
                          labelModeOptions={PRICE_BAND_LABEL_MODE_OPTIONS}
                          showExportButton={false}
                          showDimensionControls={false}
                          collapsible={false}
                        />
                      ) : (
                        <ExportPanel
                          value={bubbleExport}
                          onChange={setBubbleExport}
                          seriesNames={activeFuelTypes}
                          labelModeOptions={BUBBLE_LABEL_MODE_OPTIONS}
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
