import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import type {
  PositioningPricingBubbleItem,
  PositioningPricingDeckResponse,
  PositioningPricingMetric,
  PositioningPricingPage,
  PositioningPricingPageKey,
  PositioningPricingSalesMode,
} from "../types";
import { buildBubbleSizing } from "../utils/bubbleSizing";
import { fuelColor } from "../utils/colors";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";

const DEFAULT_FUEL_TYPES = ["BEV", "HEV", "PHEV", "MHEV", "ICE"];
const DEFAULT_COUNTRY = "瑞典";
const DEFAULT_SALES_MODE: PositioningPricingSalesMode = "month";
const DEFAULT_TOP_N = 50;
const TOP_N_OPTIONS = [30, 50, 100] as const;
const EXPORT_PRESETS = [
  { key: "hd+", label: "1600 x 900", width: 1600, height: 900 },
  { key: "fhd", label: "1920 x 1080", width: 1920, height: 1080 },
  { key: "qhd", label: "2560 x 1440", width: 2560, height: 1440 },
] as const;
const SALES_MODE_OPTIONS: Array<{ value: PositioningPricingSalesMode; label: string }> = [
  { value: "month", label: "当月" },
  { value: "rolling12", label: "近12个月" },
];
const TAB_ITEMS: Array<{
  key: PositioningPricingPageKey;
  code: string;
  label: string;
  sublabel: string;
}> = [
  { key: "overview", code: "01", label: "Overview", sublabel: "全市场" },
  { key: "suvA0", code: "02", label: "SUV-A0", sublabel: "入门 SUV" },
  { key: "suvA", code: "03", label: "SUV-A", sublabel: "A级 SUV" },
  { key: "suvBPlus", code: "04", label: "SUV-B+", sublabel: "B 级及以上 SUV" },
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

function bubbleTextPosition(index: number): string {
  const positions = ["top center", "middle right", "bottom center", "middle left"] as const;
  return positions[index % positions.length];
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

function buildPriceBandTraces(page: PositioningPricingPage, fuelOrder: string[]): Data[] {
  return fuelOrder.map((fuel) => ({
    type: "bar",
    orientation: "h",
    name: fuel,
    y: page.priceBands.items.map((item) => item.bandMid),
    x: page.priceBands.items.map((item) => item.fuelMix[fuel] ?? 0),
    width: page.priceBands.items.map((item) => Math.max(item.bandWidth * 0.84, 500)),
    customdata: page.priceBands.items.map((item) => [item.label]),
    marker: { color: fuelColor(fuel) },
    hovertemplate: `%{customdata[0]}<br>${fuel}: %{x:,.0f} 台<extra></extra>`,
  }) as Data);
}

function buildBubbleTraces(items: PositioningPricingBubbleItem[], fuelOrder: string[]): Data[] {
  return fuelOrder.flatMap((fuel) => {
    const fuelItems = items.filter((item) => item.powertrain === fuel);
    if (fuelItems.length === 0) {
      return [];
    }
    const labelPosition = bubbleTextPosition(fuelOrder.indexOf(fuel));
    const sizing = buildBubbleSizing(fuelItems.map((item) => item.sales), {
      maxDiameter: 58,
      minDiameter: 10,
    });
    return [{
      type: "scatter",
      mode: "text+markers",
      name: fuel,
      x: fuelItems.map((item) => item.length),
      y: fuelItems.map((item) => item.msrpMin),
      text: fuelItems.map((item) => item.model.trim()),
      textposition: labelPosition,
      textfont: { size: 9, color: "#334155" },
      cliponaxis: false,
      customdata: fuelItems.map((item) => [
        item.model,
        item.brand,
        item.segment,
        item.msrp,
        item.msrpMax,
        item.sales,
        item.variantCount,
      ]),
      marker: {
        color: fuelColor(fuel),
        opacity: 0.82,
        line: { color: "rgba(15, 23, 42, 0.28)", width: 1 },
        size: sizing.values,
        sizemode: sizing.sizemode,
        sizeref: sizing.sizeref,
        sizemin: sizing.sizemin,
      },
      hovertemplate:
        "Model: %{customdata[0]}<br>Brand: %{customdata[1]}<br>Segment: %{customdata[2]}<br>Length: %{x:,.0f} mm"
        + "<br>最低 MSRP: %{y:,.0f}<br>组内中位 MSRP: %{customdata[3]:,.0f}<br>最高 MSRP: %{customdata[4]:,.0f}"
        + "<br>Sales: %{customdata[5]:,.0f}<br>聚合版型数: %{customdata[6]:,.0f}<extra>%{fullData.name}</extra>",
    } as Data];
  });
}

function priceBandLayout(page: PositioningPricingPage): Partial<PlotlyLayout> {
  const rangeMin = page.priceBands.range.min;
  const rangeMax = page.priceBands.range.max;
  const step = page.priceBands.bandSize;
  return {
    ...CHART_LAYOUT,
    barmode: "stack",
    margin: { l: 84, r: 20, t: 16, b: 48 },
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

function bubbleLayout(page: PositioningPricingPage): Partial<PlotlyLayout> {
  return {
    ...CHART_LAYOUT,
    margin: { l: 58, r: 24, t: 16, b: 52 },
    xaxis: { title: { text: "Length (mm)" }, zeroline: false },
    yaxis: {
      title: { text: "最低 MSRP" },
      range: [page.priceBands.range.min, page.priceBands.range.max],
      tick0: page.priceBands.range.min,
      dtick: page.priceBands.bandSize,
      tickformat: ",d",
      zeroline: false,
    },
  };
}

export function PositioningPricingPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [deck, setDeck] = useState<PositioningPricingDeckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportToolsOpen, setExportToolsOpen] = useState(false);
  const [exportPresetKey, setExportPresetKey] = useState<(typeof EXPORT_PRESETS)[number]["key"]>("fhd");
  const [heroCollapsed, setHeroCollapsed] = useState(false);
  const [reloadToken, setReloadToken] = useState(0);
  const slideRef = useRef<HTMLDivElement | null>(null);
  const [activePage, setActivePage] = useState<PositioningPricingPageKey>(
    () => {
      const requested = searchParams.get("activePage");
      return isPageKey(requested) ? requested : "overview";
    },
  );
  const [selectedCountry, setSelectedCountry] = useState<string | null>(
    () => searchParams.get("country") || DEFAULT_COUNTRY,
  );
  const [selectedPeriod, setSelectedPeriod] = useState<string | null>(
    () => searchParams.get("period"),
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
  const [topN, setTopN] = useState<number>(() => {
    const raw = Number(searchParams.get("topN") || DEFAULT_TOP_N);
    return TOP_N_OPTIONS.includes(raw as typeof TOP_N_OPTIONS[number]) ? raw : DEFAULT_TOP_N;
  });
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
    return raw ? Number(raw) : null;
  });

  const syncUrlParams = useCallback(() => {
    const params = new URLSearchParams();
    if (selectedCountry) params.set("country", selectedCountry);
    if (selectedPeriod) params.set("period", selectedPeriod);
    if (activePage !== "overview") params.set("activePage", activePage);
    if (salesMode !== DEFAULT_SALES_MODE) params.set("salesMode", salesMode);
    if (topN !== DEFAULT_TOP_N) params.set("topN", String(topN));
    if (msrpMin !== null) params.set("msrpMin", String(msrpMin));
    if (msrpMax !== null) params.set("msrpMax", String(msrpMax));
    if (priceBandSize !== null) params.set("priceBandSize", String(priceBandSize));
    const fuels = selectedFuelTypes.slice().sort().join(",");
    const defaultFuels = DEFAULT_FUEL_TYPES.slice().sort().join(",");
    if (fuels && fuels !== defaultFuels) {
      params.set("fuelTypes", selectedFuelTypes.join(","));
    }
    setSearchParams(params, { replace: true });
  }, [activePage, msrpMax, msrpMin, priceBandSize, salesMode, selectedCountry, selectedFuelTypes, selectedPeriod, setSearchParams, topN]);

  useEffect(() => {
    syncUrlParams();
  }, [syncUrlParams]);

  useEffect(() => {
    preloadPlotlyChartRuntime().catch(() => undefined);
  }, []);

  useEffect(() => {
    setLoading(true);
    setError("");
    api.positioningPricingDeck({
      country: selectedCountry || undefined,
      target_period: selectedPeriod || undefined,
      fuel_types: selectedFuelTypes,
      sales_mode: salesMode,
      top_n: topN,
      msrp_min: msrpMin,
      msrp_max: msrpMax,
      price_band_size: priceBandSize,
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
  }, [msrpMax, msrpMin, priceBandSize, reloadToken, salesMode, selectedCountry, selectedFuelTypes, selectedPeriod, topN]);

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
    const availableFuelSet = new Set(deck.metadata.availableFuelTypes);
    const normalized = selectedFuelTypes.filter((fuel) => availableFuelSet.has(fuel));
    if (normalized.length !== selectedFuelTypes.length && deck.metadata.selectedFuelTypes.length > 0) {
      setSelectedFuelTypes(deck.metadata.selectedFuelTypes);
    }
  }, [deck, selectedCountry, selectedFuelTypes, selectedPeriod]);

  const currentCountry = selectedCountry ?? deck?.metadata.selectedCountry ?? DEFAULT_COUNTRY;
  const currentPeriod = selectedPeriod ?? deck?.metadata.resolvedPeriod ?? "";
  const fuelOptions = deck?.metadata.availableFuelTypes ?? DEFAULT_FUEL_TYPES;
  const activeFuelTypes = selectedFuelTypes.length > 0
    ? selectedFuelTypes
    : (deck?.metadata.selectedFuelTypes ?? DEFAULT_FUEL_TYPES);
  const page = deck?.pages[activePage];
  const activeTab = TAB_ITEMS.find((item) => item.key === activePage) ?? TAB_ITEMS[0];
  const exportPreset = EXPORT_PRESETS.find((item) => item.key === exportPresetKey) ?? EXPORT_PRESETS[1];
  const barTraces = useMemo(
    () => (page ? buildPriceBandTraces(page, activeFuelTypes) : []),
    [activeFuelTypes, page],
  );
  const bubbleTraces = useMemo(
    () => (page ? buildBubbleTraces(page.bubbleChart.items, activeFuelTypes) : []),
    [activeFuelTypes, page],
  );

  function toggleFuel(fuel: string) {
    setSelectedFuelTypes((current) => {
      if (current.includes(fuel)) {
        return current.length > 1 ? current.filter((item) => item !== fuel) : current;
      }
      return [...current, fuel];
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
        <CollapsibleDeckHero
          collapsed={heroCollapsed}
          onToggle={() => setHeroCollapsed((current) => !current)}
          expandedLabel="展开定位定价控制区"
          collapsedLabel="收起定位定价控制区"
          expandedTitle="展开定位定价控制区"
          collapsedTitle="收起定位定价控制区"
          className="header-card dashboard-hero market-scan-hero positioning-pricing-hero"
          shellClassName="dashboard-hero-shell market-scan-hero-shell"
          head={(
            <div className="dashboard-hero-copy market-scan-hero-copy">
              <span className="page-kicker">Positioning Pricing</span>
              <h1>{deck?.metadata.labels.pageTitle ?? "定位定价"}</h1>
              <p>{page?.summaryText ?? "按国家、月份与动力筛选固定版式的定位定价页。"}</p>
              <div className="market-scan-hero-ribbon">
                <span className="market-scan-hero-chip">国家 {deck?.metadata.selectedCountryLabel ?? currentCountry}</span>
                <span className="market-scan-hero-chip">月份 {deck?.metadata.labels.currentMonthShort ?? "Latest"}</span>
                <span className="market-scan-hero-chip">动力 {activeFuelTypes.join(" / ")}</span>
                <span className="market-scan-hero-chip">页面 {activeTab.label}</span>
                <span className="market-scan-hero-chip">口径 {deck?.metadata.labels.salesModeLabel ?? SALES_MODE_OPTIONS.find((item) => item.value === salesMode)?.label ?? "当月"}</span>
                <span className="market-scan-hero-chip">Top {topN}</span>
                <span className="market-scan-hero-chip">
                  MSRP {formatMetricValue(page?.priceBands.range.min ?? msrpMin ?? 0)}-{formatMetricValue(page?.priceBands.range.max ?? msrpMax ?? 0)}
                </span>
                {loading && deck ? <span className="market-scan-hero-chip market-scan-hero-chip--live">Refreshing</span> : null}
              </div>
            </div>
          )}
          body={(
            <div className="market-scan-hero-body-grid">
              <div className="market-scan-controls-grid positioning-pricing-controls-grid">
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
                  <span>Period</span>
                  <select
                    value={currentPeriod}
                    onChange={(event) => setSelectedPeriod(event.target.value || null)}
                    disabled={!deck}
                  >
                    {(deck?.metadata.availablePeriods ?? []).map((option) => (
                      <option key={option.value} value={option.value}>
                        {option.label}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="market-scan-field">
                  <span>销量口径</span>
                  <div className="btn-group">
                    {SALES_MODE_OPTIONS.map((option) => (
                      <button
                        key={option.value}
                        type="button"
                        className={`btn btn-sm ${salesMode === option.value ? "btn-primary" : "btn-ghost"}`}
                        onClick={() => setSalesMode(option.value)}
                      >
                        {option.label}
                      </button>
                    ))}
                  </div>
                </div>
                <label className="market-scan-field">
                  <span>Top N</span>
                  <select value={topN} onChange={(event) => setTopN(Number(event.target.value))}>
                    {TOP_N_OPTIONS.map((option) => (
                      <option key={option} value={option}>
                        Top {option}
                      </option>
                    ))}
                  </select>
                </label>
                <label className="market-scan-field">
                  <span>MSRP Min</span>
                  <input
                    type="number"
                    min={0}
                    step={1000}
                    value={msrpMin ?? ""}
                    placeholder={String(page?.priceBands.range.min ?? "")}
                    onChange={(event) => setMsrpMin(event.target.value ? Number(event.target.value) : null)}
                  />
                </label>
                <label className="market-scan-field">
                  <span>MSRP Max</span>
                  <input
                    type="number"
                    min={0}
                    step={1000}
                    value={msrpMax ?? ""}
                    placeholder={String(page?.priceBands.range.max ?? "")}
                    onChange={(event) => setMsrpMax(event.target.value ? Number(event.target.value) : null)}
                  />
                </label>
                <label className="market-scan-field">
                  <span>Step</span>
                  <input
                    type="number"
                    min={500}
                    step={500}
                    value={priceBandSize ?? ""}
                    placeholder={String(page?.priceBands.bandSize ?? "")}
                    onChange={(event) => setPriceBandSize(event.target.value ? Number(event.target.value) : null)}
                  />
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
                        setSelectedCountry(DEFAULT_COUNTRY);
                        setSelectedPeriod(null);
                        setSalesMode(DEFAULT_SALES_MODE);
                        setSelectedFuelTypes(DEFAULT_FUEL_TYPES);
                        setTopN(DEFAULT_TOP_N);
                        setMsrpMin(null);
                        setMsrpMax(null);
                        setPriceBandSize(null);
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
            </div>
          )}
        />

        <nav className="positioning-pricing-tab-strip" aria-label="Positioning Pricing Pages">
          {TAB_ITEMS.map((item) => (
            <button
              key={item.key}
              type="button"
              className={`market-scan-tab${activePage === item.key ? " is-active" : ""}`}
              onClick={() => setActivePage(item.key)}
            >
              <span className="market-scan-tab-code">{item.code}</span>
              <span className="market-scan-tab-copy">
                <strong>{item.label}</strong>
                <span>{item.sublabel}</span>
              </span>
            </button>
          ))}
        </nav>

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

            <div className="market-scan-slide-shell">
              <div
                ref={slideRef}
                className="market-scan-slide-frame positioning-pricing-slide-frame"
                style={{
                  width: exportingSlide ? `${exportPreset.width}px` : undefined,
                  height: exportingSlide ? `${exportPreset.height}px` : undefined,
                  aspectRatio: exportingSlide ? "auto" : undefined,
                }}
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
                    <div className="market-scan-callout positioning-pricing-summary">
                      {page.subtitle}：左侧按 MSRP 区间看销量堆叠，右侧按最低 MSRP 看动力气泡定位。
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
                              layout={priceBandLayout(page)}
                              height={430}
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

                      <Panel
                        eyebrow="Powertrain Bubble"
                        title="动力气泡图"
                        subtitle="与 Dashboard 不同，这里的 MSRP 使用组内最低值。"
                      >
                        <div className="positioning-pricing-chart">
                          {bubbleTraces.length > 0 ? (
                            <PlotlyChart
                              data={bubbleTraces}
                              layout={bubbleLayout(page)}
                              height={430}
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
                    <span className="market-scan-toolbar-chip">{activeTab.label}</span>
                    <span className="market-scan-toolbar-chip">{deck.metadata.selectedCountryLabel}</span>
                    <span className="market-scan-toolbar-chip">{deck.metadata.resolvedPeriod}</span>
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
