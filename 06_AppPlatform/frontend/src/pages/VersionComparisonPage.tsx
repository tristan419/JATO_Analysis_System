import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { DeckPeriodTimeline } from "../components/DeckPeriodTimeline";
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
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
import { useArrowCountryNavigation } from "../utils/useArrowCountryNavigation";
import { useFixedCanvasPreview } from "../utils/useFixedCanvasPreview";

const DEFAULT_FUEL_TYPES = ["BEV", "HEV", "PHEV", "MHEV", "ICE"];
const DEFAULT_COUNTRY = "瑞典";
const DEFAULT_SALES_MODE: PositioningPricingSalesMode = "month";
const DEFAULT_PRICE_BAND_SIZE = 1000;
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
  return fuelOrder.map((fuel) => ({
    type: "bar",
    orientation: "h",
    name: fuel,
    y: items.map((item) => item.bandMid),
    x: items.map((item) => item.fuelMix[fuel] ?? 0),
    width: items.map((item) => Math.max(item.bandWidth * 0.84, 500)),
    customdata: items.map((item) => [item.label]),
    marker: { color: fuelColor(fuel) },
    hovertemplate: `%{customdata[0]}<br>${fuel}: %{x:,.0f} 台<extra></extra>`,
  }) as Data);
}

function buildVersionBubbleTraces(items: VersionComparisonBubbleItem[]): Data[] {
  const sizing = buildBubbleSizing(items.map((item) => item.sales), {
    maxDiameter: 58,
    minDiameter: 10,
  });
  const seenPowertrains = Array.from(new Set(items.map((item) => item.powertrain)));
  const powertrains = [
    ...DEFAULT_FUEL_TYPES.filter((fuel) => seenPowertrains.includes(fuel)),
    ...seenPowertrains.filter((fuel) => !DEFAULT_FUEL_TYPES.includes(fuel)),
  ];
  return powertrains.map((powertrain) => {
    const subset = items.filter((item) => item.powertrain === powertrain);
    return {
      type: "scatter",
      mode: "text+markers",
      name: powertrain,
      x: subset.map((item) => item.length),
      y: subset.map((item) => item.msrp),
      text: subset.map((item) => item.version),
      textposition: "top center",
      textfont: { size: 9, color: "#334155" },
      cliponaxis: false,
      customdata: subset.map((item) => [
        item.model,
        item.version,
        item.trim,
        item.powertrain,
        item.sales,
        item.msrpMin,
        item.msrpMax,
        item.length,
      ]),
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
        "Model: %{customdata[0]}<br>Version: %{customdata[1]}<br>Trim: %{customdata[2]}<br>动力: %{customdata[3]}"
        + "<br>Length: %{customdata[7]:,.0f} mm<br>MSRP: %{y:,.0f}<br>MSRP范围: %{customdata[5]:,.0f}-%{customdata[6]:,.0f}"
        + "<br>Sales: %{customdata[4]:,.0f}<extra></extra>",
    } as Data;
  });
}

function buildModelLengthAnnotations(items: VersionComparisonBubbleItem[]): NonNullable<Partial<PlotlyLayout>["annotations"]> {
  const modelLengthMap = new Map<string, number>();
  items.forEach((item) => {
    if (!modelLengthMap.has(item.model)) {
      modelLengthMap.set(item.model, item.length);
    }
  });
  const rowOffsets = [-0.14, -0.24];
  const overlapThreshold = 70;
  let previousLength: number | null = null;
  let currentRow = 0;
  return Array.from(modelLengthMap.entries())
    .sort((left, right) => left[1] - right[1])
    .map(([model, length]) => {
      if (previousLength !== null && Math.abs(length - previousLength) <= overlapThreshold) {
        currentRow = (currentRow + 1) % rowOffsets.length;
      } else {
        currentRow = 0;
      }
      previousLength = length;
      return {
        x: length,
        y: rowOffsets[currentRow],
        xref: "x",
        yref: "paper",
        text: model,
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
    margin: { l: 84, r: 20, t: 16, b: 118 },
    legend: {
      orientation: "h",
      x: 0,
      xanchor: "left",
      y: -0.18,
      yanchor: "top",
      font: { size: 9 },
      itemwidth: 30,
      itemsizing: "constant",
    },
    xaxis: { title: { text: "Sales" }, zeroline: false },
    yaxis: {
      title: { text: "MSRP" },
      range: [rangeMin, rangeMax],
      tick0: rangeMin,
      dtick: step,
      tickformat: ",d",
      automargin: true,
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
  return {
    ...CHART_LAYOUT,
    margin: { l: 58, r: 24, t: 16, b: 118 },
    xaxis: {
      tickformat: ",d",
      automargin: true,
      zeroline: false,
    },
    yaxis: {
      title: { text: "Version MSRP" },
      range: [rangeMin, rangeMax],
      tick0: rangeMin,
      dtick: step,
      tickformat: ",d",
      zeroline: false,
    },
    annotations,
  };
}

function searchModelOptions(options: VersionComparisonModelOption[], query: string): VersionComparisonModelOption[] {
  const q = query.trim().toLowerCase();
  if (!q) return options;
  return options.filter((m) => {
    const fields = [
      m.label,
      m.segment,
      m.powertrain,
      m.bodyType,
      m.driveType,
      String(m.lengthMm),
      String(m.msrpMedian),
    ].filter(Boolean).join(" ").toLowerCase();
    return fields.includes(q);
  });
}

function searchSegmentOptions(options: { value: string; label: string }[], query: string): { value: string; label: string }[] {
  const q = query.trim().toLowerCase();
  if (!q) return options;
  return options.filter((s) => s.label.toLowerCase().includes(q) || s.value.toLowerCase().includes(q));
}

function searchCountryOptions(options: { value: string; label: string }[], query: string): { value: string; label: string }[] {
  const q = query.trim().toLowerCase();
  if (!q) return options;
  return options.filter((c) => c.label.toLowerCase().includes(q) || c.value.toLowerCase().includes(q));
}

export function VersionComparisonPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [deck, setDeck] = useState<VersionComparisonDeckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportToolsOpen, setExportToolsOpen] = useState(false);
  const [exportPresetKey, setExportPresetKey] = useState<(typeof EXPORT_PRESETS)[number]["key"]>(DEFAULT_EXPORT_PRESET);
  const [heroCollapsed, setHeroCollapsed] = useState(false);
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

  const [selectedCountry, setSelectedCountry] = useState<string | null>(() => searchParams.get("country") || DEFAULT_COUNTRY);
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
  const countryOptions = deck?.metadata.availableCountries ?? [];

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
    if (msrpMin !== null) params.set("msrpMin", String(msrpMin));
    if (msrpMax !== null) params.set("msrpMax", String(msrpMax));
    if (priceBandSize !== null && priceBandSize !== DEFAULT_PRICE_BAND_SIZE) params.set("priceBandSize", String(priceBandSize));
    if (bodyType && comparisonMode !== "same_segment") params.set("bodyType", bodyType);
    if (driveTypes.length > 0 && comparisonMode !== "same_segment") params.set("driveTypes", driveTypes.join(","));
    if (lengthMin !== null && comparisonMode !== "same_segment") params.set("lengthMin", String(lengthMin));
    if (lengthMax !== null && comparisonMode !== "same_segment") params.set("lengthMax", String(lengthMax));
    if (selectedSegments.length > 0 && comparisonMode !== "same_segment") params.set("segments", selectedSegments.join(","));
    setSearchParams(params, { replace: true });
  }, [msrpMax, msrpMin, priceBandSize, salesMode, comparisonMode, selectedCountry, selectedFuelTypes, selectedModels, selectedPeriod, selectedSegment, selectedTimeRange, bodyType, driveTypes, lengthMin, lengthMax, selectedSegments, setSearchParams]);

  useEffect(() => {
    syncUrlParams();
  }, [syncUrlParams]);

  useEffect(() => {
    preloadPlotlyChartRuntime().catch(() => undefined);
  }, []);

  useArrowCountryNavigation({
    options: countryOptions,
    activeValue: selectedCountry || DEFAULT_COUNTRY,
    onSelect: (value) => setSelectedCountry(value || null),
  });

  useEffect(() => {
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
      models: selectedModels,
      msrp_min: msrpMin,
      msrp_max: msrpMax,
      price_band_size: priceBandSize,
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
  }, [msrpMax, msrpMin, priceBandSize, reloadToken, salesMode, comparisonMode, selectedCountry, selectedFuelTypes, selectedModels, selectedPeriod, selectedSegment, selectedTimeRange, bodyType, driveTypes, selectedSegments, lengthMin, lengthMax]);

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

  const currentCountry = selectedCountry ?? deck?.metadata.selectedCountry ?? DEFAULT_COUNTRY;
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

  // Auto-populate length range from segment suggestion (same_segment) or full range (free_comparison)
  const [lengthTouched, setLengthTouched] = useState(false);
  useEffect(() => {
    if (!deck?.metadata.suggestedLengthMin && !deck?.metadata.suggestedLengthMax) return;
    if (lengthTouched) return;
    const slm = deck.metadata.suggestedLengthMin;
    const slx = deck.metadata.suggestedLengthMax;
    if (slm != null && lengthMin !== slm) setLengthMin(slm);
    if (slx != null && lengthMax !== slx) setLengthMax(slx);
  }, [deck?.metadata.suggestedLengthMin, deck?.metadata.suggestedLengthMax]);

  const exportPreset = EXPORT_PRESETS.find((item) => item.key === exportPresetKey) ?? EXPORT_PRESETS[1];
  const slidePreview = useFixedCanvasPreview({
    width: exportPreset.width,
    height: exportPreset.height,
    exporting: exportingSlide,
  });

  // Candidate pool: filtered model options shown in the picker
  const candidateOptions = deck?.metadata.availableModels ?? [];
  // Filtered by search query
  const searchedOptions = useMemo(
    () => searchModelOptions(candidateOptions, modelSearchQuery),
    [candidateOptions, modelSearchQuery],
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
      const unselected = searchedOptions.filter((m) => !activeModels.includes(m.value));
      setModelToAdd(unselected[0]?.value ?? "");
    } else {
      setModelToAdd("");
    }
  }, [searchedOptions, activeModels]);

  const barTraces = useMemo(
    () => (page ? buildPriceBandTraces(page.priceBands.items, activeFuelTypes) : []),
    [activeFuelTypes, page],
  );
  const bubbleTraces = useMemo(
    () => (page ? buildVersionBubbleTraces(page.bubbleChart.items) : []),
    [page],
  );
  const bubbleAnnotations = useMemo(
    () => (page ? buildModelLengthAnnotations(page.bubbleChart.items) : []),
    [page],
  );

  function toggleFuel(fuel: string) {
    setSelectedFuelTypes((current) => {
      if (current.includes(fuel)) {
        return current.length > 1 ? current.filter((item) => item !== fuel) : current;
      }
      return [...current, fuel];
    });
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
    const visibleValues = new Set(searchedOptions.map((m) => m.value));
    setSelectedModels((current) => {
      const existing = new Set(current);
      const toAdd = searchedOptions
        .filter((m) => !existing.has(m.value))
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
        height: exportPreset.height,
        canvasWidth: exportPreset.width,
        canvasHeight: exportPreset.height,
        style: {
          width: `${exportPreset.width}px`,
          height: `${exportPreset.height}px`,
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
        <CollapsibleDeckHero
          collapsed={heroCollapsed}
          onToggle={() => setHeroCollapsed((current) => !current)}
          expandedLabel="展开版型对比控制区"
          collapsedLabel="收起版型对比控制区"
          expandedTitle="展开版型对比控制区"
          collapsedTitle="收起版型对比控制区"
          className="header-card dashboard-hero market-scan-hero positioning-pricing-hero"
          shellClassName="dashboard-hero-shell market-scan-hero-shell"
          head={(
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
                      setSelectedCountry(DEFAULT_COUNTRY); setSelectedPeriod(null);
                      setSalesMode(DEFAULT_SALES_MODE); setComparisonMode("same_segment");
                      setSelectedSegment(null); setSelectedModels([]);
                      setSelectedFuelTypes(DEFAULT_FUEL_TYPES); setPriceControlsTouched(false);
                      setMsrpMin(null); setMsrpMax(null); setPriceBandSize(DEFAULT_PRICE_BAND_SIZE);
                      setBodyType(null); setDriveTypes([]); setSelectedSegments([]);
                      setLengthMin(null); setLengthMax(null); setLengthTouched(false);
                      setModelSearchQuery(""); setSegmentSearchQuery("");
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
          )}
          body={(
            <div className="market-scan-hero-body-grid">
              <div className="version-comparison-filter-grid">
                {/* Mode selector */}
                <div className="vc-col-14">
                  <div className="market-scan-field">
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
                </div>

                {/* Filter Row 1: Country(2) + Period(5) + Segment(2) + Add Model(5) */}
                <div className="vc-col-2 market-scan-field version-comparison-model-picker-field" ref={countryPickerRef}>
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
                          const isSelected = option.value === (selectedCountry || DEFAULT_COUNTRY);
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
                <div className="vc-col-5 market-scan-field">
                  <div className="version-comparison-period-row">
                    <DeckPeriodTimeline
                      options={deck?.metadata.availablePeriods ?? []}
                      value={resolvedTimeRange ?? (selectedPeriod ? { start: selectedPeriod, end: selectedPeriod } : null)}
                      onChange={(value) => {
                        setSelectedTimeRange(isCustomTimeRange(value) ? value : null);
                        setSelectedPeriod(value?.end ?? null);
                      }}
                      disabled={!deck}
                    />
                    <div className="btn-group version-comparison-sales-mode-group">
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
                  </div>
                  {customRangeActive ? (
                    <small className="market-scan-field-hint">已切换自定义区间；点击当月/YTD/近12个月退出。</small>
                  ) : null}
                </div>

                {/* Row 2: Segment (col-3) + Add Model (col-9) */}
                <div className="vc-col-2 market-scan-field version-comparison-model-picker-field" ref={segmentPickerRef}>
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
                <div className="vc-col-5 market-scan-field version-comparison-model-picker-field" ref={modelPickerRef}>
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
                          return (
                            <button
                              key={option.value}
                              type="button"
                              className={`version-comparison-model-option${option.value === modelToAdd ? " is-active" : ""}${isSelected ? " is-selected" : ""}`}
                              onClick={() => { handleToggleModel(option.value); setModelToAdd(option.value); }}
                              onMouseEnter={() => setModelToAdd(option.value)}
                            >
                              <span className={`version-comparison-model-checkbox${isSelected ? " is-checked" : ""}`}>
                                {isSelected ? "✓" : ""}
                              </span>
                              <div className="version-comparison-model-option-body">
                                <div className="version-comparison-model-option-main">
                                  <span className="version-comparison-model-option-name">{option.label}</span>
                                  {isSelected ? <span className="version-comparison-model-option-added">已添加</span> : null}
                                </div>
                                <div className="version-comparison-model-option-meta">
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

                {/* Filter Row 2: MSRP Min(2) + MSRP Max(2) + Step(2) + Length Range(8) */}
                <label className="vc-col-2 market-scan-field">
                  <span>MSRP Min</span>
                  <input type="text" inputMode="numeric" className="version-comparison-number-input"
                    value={msrpMin ?? ""} placeholder={String(page?.priceBands.range.min ?? "")}
                    onChange={(event) => { setPriceControlsTouched(true); const raw = event.target.value.replace(/[^0-9]/g, ""); setMsrpMin(raw ? Number(raw) : null); }} />
                </label>
                <label className="vc-col-2 market-scan-field">
                  <span>MSRP Max</span>
                  <input type="text" inputMode="numeric" className="version-comparison-number-input"
                    value={msrpMax ?? ""} placeholder={String(page?.priceBands.range.max ?? "")}
                    onChange={(event) => { setPriceControlsTouched(true); const raw = event.target.value.replace(/[^0-9]/g, ""); setMsrpMax(raw ? Number(raw) : null); }} />
                </label>
                <label className="vc-col-2 market-scan-field">
                  <span>Step</span>
                  <input type="text" inputMode="numeric" className="version-comparison-number-input"
                    value={priceBandSize ?? ""} placeholder={String(page?.priceBands.bandSize ?? "")}
                    onChange={(event) => { setPriceControlsTouched(true); const raw = event.target.value.replace(/[^0-9]/g, ""); setPriceBandSize(raw ? Number(raw) : null); }} />
                </label>
                <div className="vc-col-8 market-scan-field">
                  <span>Length Range (mm)</span>
                  <div className="version-comparison-length-slider-row">
                    <input type="text" inputMode="numeric"
                      className="version-comparison-length-input"
                      value={lengthMin ?? ""} placeholder={String(deck?.metadata.suggestedLengthMin ?? "3500")}
                      onChange={(event) => { setLengthTouched(true); const raw = event.target.value.replace(/[^0-9]/g, ""); setLengthMin(raw ? Number(raw) : null); }} />
                    <div className="version-comparison-length-bar">
                      {lengthMin && lengthMax ? (
                        <div className="version-comparison-length-bar-fill"
                          style={{
                            left: `${Math.max(0, ((lengthMin - 3500) / (5500 - 3500)) * 100)}%`,
                            right: `${Math.max(0, ((5500 - lengthMax) / (5500 - 3500)) * 100)}%`,
                          }} />
                      ) : null}
                    </div>
                    <input type="text" inputMode="numeric"
                      className="version-comparison-length-input"
                      value={lengthMax ?? ""} placeholder={String(deck?.metadata.suggestedLengthMax ?? "5000")}
                      onChange={(event) => { setLengthTouched(true); const raw = event.target.value.replace(/[^0-9]/g, ""); setLengthMax(raw ? Number(raw) : null); }} />
                    <span className="version-comparison-length-unit">mm</span>
                  </div>
                </div>

                {/* Corridor extra filters: Body Type + Drive Type */}
                {comparisonMode !== "same_segment" ? (
                  <>
                    {bodyTypeOptions.length > 0 ? (
                      <label className="vc-col-2 market-scan-field">
                        <span>Body Type</span>
                        <select value={bodyType ?? ""}
                          onChange={(event) => setBodyType(event.target.value || null)} disabled={!deck}>
                          <option value="">全部</option>
                          {bodyTypeOptions.map((option) => (<option key={option} value={option}>{option}</option>))}
                        </select>
                      </label>
                    ) : null}
                    {driveTypeOptions.length > 0 ? (
                      <div className="vc-col-5 market-scan-field">
                        <span>Drive Type</span>
                        <div className="market-scan-fuel-chip-row">
                          {driveTypeOptions.map((dt) => {
                            const active = driveTypes.includes(dt);
                            return (
                              <button key={dt} type="button"
                                className={`market-scan-fuel-chip${active ? " is-active" : ""}`}
                                onClick={() => setDriveTypes((c) => c.includes(dt) ? c.filter(d => d !== dt) : [...c, dt])}>
                                {dt}
                              </button>
                            );
                          })}
                        </div>
                      </div>
                    ) : null}
                  </>
                ) : null}
              </div>

              {/* Fuel Focus */}
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

              {/* Selected Models */}
              <div className="version-comparison-selection-bank">
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
                          {model.segment}{model.powertrain ? ` · ${model.powertrain}` : ""}{model.lengthMm > 0 ? ` · ${model.lengthMm}mm` : ""}
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
          )}
        />

        {error ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>版型对比加载失败</strong>
            <p>{error}</p>
          </section>
        ) : null}

        {loading && !deck ? (
          <section className="market-scan-state-card">
            <LoadingSurface
              mode="inline"
              kicker="Deck"
              label="正在生成版型对比页面"
              detail="按 segment / model / 时间口径实时聚合版型明细。"
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

                    <div className="market-scan-grid market-scan-grid--two-wide positioning-pricing-grid">
                      <Panel
                        eyebrow="Price Bands"
                        title="累计价格带"
                        subtitle="纵轴为 MSRP 区间，横轴为销量，按动力堆叠。"
                      >
                        <div className="positioning-pricing-chart">
                          {barTraces.length > 0 ? (
                            <PlotlyChart
                              data={barTraces}
                              layout={priceBandLayout(page.priceBands.range.min, page.priceBands.range.max, page.priceBands.bandSize)}
                              height={430}
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

                      <Panel
                        eyebrow="Version Drilldown"
                        title="版型细分气泡图"
                        subtitle="横轴为车长，轴下标出对应 Model，气泡文字显示 version，颜色按动总区分。"
                      >
                        <div className="positioning-pricing-chart">
                          {bubbleTraces.length > 0 ? (
                            <PlotlyChart
                              data={bubbleTraces}
                              layout={versionBubbleLayout(
                                page.priceBands.range.min,
                                page.priceBands.range.max,
                                page.priceBands.bandSize,
                                bubbleAnnotations,
                              )}
                              height={430}
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

            <section className="market-scan-export-drawer">
              <button
                type="button"
                className="market-scan-export-toggle"
                onClick={() => setExportToolsOpen((value) => !value)}
                aria-expanded={exportToolsOpen}
              >
                <span>导出当前页 PNG</span>
                <span>{exportToolsOpen ? "收起" : "展开"}</span>
              </button>
              {exportToolsOpen ? (
                <div className="market-scan-toolbar market-scan-toolbar--bottom">
                  <div className="market-scan-toolbar-group market-scan-toolbar-group--settings">
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
                    <button
                      type="button"
                      className="btn btn-primary btn-sm"
                      onClick={() => { void handleExportSlide(); }}
                      disabled={exportingSlide}
                    >
                      {exportingSlide ? "正在导出 PNG..." : "导出当前页 PNG"}
                    </button>
                  </div>
                  <div className="market-scan-toolbar-meta">
                    <span className="market-scan-toolbar-chip">{exportPreset.width} x {exportPreset.height}</span>
                    <span className="market-scan-toolbar-chip">{deck.metadata.labels.salesModeLabel}</span>
                    <span className="market-scan-toolbar-chip">{deck.metadata.selectedSegment || "-"}</span>
                    <span className="market-scan-toolbar-chip">{deck.metadata.selectedModels.length}/{MAX_SELECTED_MODELS} Models</span>
                  </div>
                </div>
              ) : null}
            </section>
          </div>
        ) : null}
      </div>
    </div>
  );
}