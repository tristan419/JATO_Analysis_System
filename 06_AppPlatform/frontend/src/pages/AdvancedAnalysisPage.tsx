import { Fragment, Suspense, lazy, useCallback, useEffect, useMemo, useState } from "react";
import type { CSSProperties, ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { LazyPlotlyChart, type PlotlyChartProps } from "../components/LazyPlotlyChart";
import { PageBannerStack, PageLoadingShell } from "../components/PageFeedback";
import { SearchSelectFilter } from "../components/SearchSelectFilter";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";
import { SERIES_COLORS } from "../utils/colors";
import { compactSearchText } from "../utils/searchMatching";
import type { ExportSettings } from "../components/ExportPanelHelpers";
import { DeckExportDrawer, DeckFloatingDrawer } from "../components/deckControls";
import { JATO_COUNTRIES, formatJatoCountryOption } from "../utils/jatoCountries";
import "./AdvancedAnalysisPage.css";
import type {
  AdvancedAnalysisCompetitorSetRequest,
  AdvancedAnalysisCountriesResponse,
  AdvancedAnalysisProfileDimension,
  AdvancedAnalysisProfileOptions,
  AdvancedAnalysisProfileOptionsResponse,
  AdvancedAnalysisSalesMode,
  AdvancedAnalysisTransferMartRequest,
  CompetitorProductSpecKey,
  CompetitorProfileSpecs,
  CompetitorSetResponse,
  ModelChannelTimeseriesItem,
  TransferMartModel,
  TransferMartResponse,
} from "../types/advancedAnalysis";

/* ── Constants ── */

const DEFAULT_COUNTRY = "瑞典";
const STATIC_COUNTRY_OPTIONS = JATO_COUNTRIES.map((country) => ({
  value: country.marketScanCountry,
  label: formatJatoCountryOption(country),
}));
const CHART_MARGIN = { l: 52, r: 24, t: 20, b: 48 } as const;
const DEFAULT_AA_EXPORT: ExportSettings = {
  showXGrid: false,
  showYGrid: false,
  showAxisLine: true,
  showLegend: true,
  legendPosition: "right",
  colorScheme: "default",
  fontSize: 11,
  labelFontSize: 12,
  gridColor: "#E5E7EB",
  axisColor: "#6B7280",
  xTickFormat: "",
  yTickFormat: "",
  paperBg: "#FFFFFF",
  plotBg: "#FFFFFF",
  chartTitle: "",
  xTitle: "",
  yTitle: "",
  exportWidth: 1920,
  exportHeight: 1080,
  dataLabelMode: "value",
  dataLabelPosition: "auto",
  dataLabelOverlapStrategy: "all",
  decimalPlaces: 0,
  seriesColors: {},
};
const ADVANCED_ANALYSIS_PLOTLY_DEFER_MS = 6_000;
const COLORS = { growth: "#10b981", decline: "#ef4444", stable: "#94a3b8", market: "#3b82f6", share: "#10b981", mix: "#f59e0b", interaction: "#8b5cf6", winner: "#10b981", loser: "#ef4444" };
const RESPONSIVE_TWO_COL: CSSProperties = { display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(360px, 1fr))", gap: 16, marginBottom: 16 };
const PROFILE_DIMENSIONS: Array<{ key: AdvancedAnalysisProfileDimension; label: string; showSuvShortcut?: boolean }> = [
  { key: "segment", label: "Segment", showSuvShortcut: true },
  { key: "body_type", label: "Body Type", showSuvShortcut: true },
  { key: "powertrain", label: "Powertrain" },
  { key: "registration_type", label: "Channel" },
  { key: "drive_type", label: "Drive" },
  { key: "origin", label: "Origin" },
  { key: "make", label: "Make" },
];
const PRODUCT_SPEC_INPUTS: Array<{ key: CompetitorProductSpecKey; label: string; placeholder: string; suffix: string }> = [
  { key: "length_mm", label: "Length", placeholder: "e.g. 4424", suffix: "mm" },
  { key: "msrp", label: "MSRP", placeholder: "e.g. 36000", suffix: "" },
  { key: "ev_range", label: "EV Range", placeholder: "e.g. 430", suffix: "km" },
  { key: "fuel_consumption", label: "Fuel Cons.", placeholder: "e.g. 6.8", suffix: "L/100km" },
  { key: "co2_emission", label: "CO2", placeholder: "e.g. 135", suffix: "g/km" },
  { key: "battery_kwh", label: "Battery", placeholder: "e.g. 61", suffix: "kWh" },
];
const EMPTY_PROFILE_OPTIONS: AdvancedAnalysisProfileOptions = {
  segment: [],
  body_type: [],
  powertrain: [],
  registration_type: [],
  drive_type: [],
  origin: [],
  make: [],
  model: [],
};

const HeroProductAnalysisView = lazy(() =>
  import("./HeroProductAnalysisView").then((module) => ({ default: module.HeroProductAnalysisView })),
);
const AdvancedAnalysisExportPanel = lazy(() =>
  import("../components/ExportPanel").then((module) => ({ default: module.ExportPanel })),
);

type DecompositionKey = "market_carryover" | "channel_mix" | "drive_mix" | "powertrain_mix" | "pure_share_shift" | "interaction";
type SortKey = "model" | "dV" | "pure_share_shift" | DecompositionKey;
type RoleColorKey = "likely_source" | "likely_recipient" | "co_winner" | "co_loser" | "adjacent" | "target";
type CountryOption = { value: string; label: string };
type RankedModelOption = { model: string; score: number; index: number };

/* ── Helpers ── */

function pt(text: string): Partial<PlotlyLayout>["title"] { return { text }; }
function PlotlyChart(props: PlotlyChartProps) {
  return <LazyPlotlyChart {...props} deferMs={ADVANCED_ANALYSIS_PLOTLY_DEFER_MS} />;
}
function stateColor(s: string): string { return s === "growth" ? COLORS.growth : s === "decline" ? COLORS.decline : COLORS.stable; }
function getGraphDiv(): HTMLElement | null { return document.querySelector(".chart-card .js-plotly-plot") as HTMLElement | null; }
function fmtNum(n: number): string { return n.toLocaleString(undefined, { maximumFractionDigits: 0 }); }
function fmtPct(n: number): string { return `${(n * 100).toFixed(1)}%`; }
function fmtBp(n: number): string { return `${(n * 10000).toFixed(0)} bp`; }
function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
}
function normalizeLookupText(value: string): string {
  return value
    .normalize("NFKD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9\u4e00-\u9fa5]+/g, " ")
    .trim();
}
function compactLookupText(value: string): string {
  return compactSearchText(normalizeLookupText(value));
}
function tokenizeModelQuery(query: string): string[] {
  return normalizeLookupText(query).split(/\s+/).filter(Boolean);
}
function scoreModelOption(model: string, query: string): number | null {
  const normalizedModel = normalizeLookupText(model);
  const normalizedQuery = normalizeLookupText(query);
  if (!normalizedQuery) return 1;
  const tokens = tokenizeModelQuery(query);
  if (tokens.length === 0) return 1;
  const compactModel = compactLookupText(model);
  const compactQuery = compactLookupText(query);
  if (!tokens.every((token) => normalizedModel.includes(token)) && !compactModel.includes(compactQuery)) return null;
  let score = 10;
  if (normalizedModel === normalizedQuery) score += 100;
  if (normalizedModel.startsWith(normalizedQuery)) score += 45;
  if (compactModel === compactQuery) score += 90;
  if (compactModel.startsWith(compactQuery)) score += 35;
  score += tokens.reduce((sum, token) => {
    if (normalizedModel.startsWith(token)) return sum + 18;
    if (normalizedModel.split(" ").some((word) => word.startsWith(token))) return sum + 12;
    return sum + Math.max(1, 8 - normalizedModel.indexOf(token));
  }, 0);
  return score;
}
function mergeCountryOptions(datasetCountries: string[], activeCountry: string): CountryOption[] {
  const byValue = new Map<string, CountryOption>();
  for (const option of STATIC_COUNTRY_OPTIONS) {
    byValue.set(option.value, option);
  }
  for (const country of datasetCountries) {
    const value = country.trim();
    if (!value || byValue.has(value)) continue;
    byValue.set(value, { value, label: value });
  }
  if (activeCountry && !byValue.has(activeCountry)) {
    byValue.set(activeCountry, { value: activeCountry, label: activeCountry });
  }
  return Array.from(byValue.values());
}
function dominantComponent(model: TransferMartModel): string {
  const components: Array<{ label: string; value: number }> = [
    { label: "market carryover", value: model.market_carryover },
    { label: "channel mix", value: model.channel_mix },
    { label: "drive mix", value: model.drive_mix },
    { label: "powertrain mix", value: model.powertrain_mix },
    { label: "competitive share shift", value: model.pure_share_shift },
    { label: "interaction", value: model.interaction },
  ];
  return components.sort((a, b) => Math.abs(b.value) - Math.abs(a.value))[0]?.label ?? "net volume change";
}
function roleColor(role: RoleColorKey): string {
  if (role === "likely_source") return COLORS.loser;
  if (role === "likely_recipient") return COLORS.winner;
  if (role === "co_winner") return "#0ea5e9";
  if (role === "co_loser") return "#f97316";
  if (role === "target") return "#1e293b";
  return "#64748b";
}
function emptyProfileSelections(): Record<AdvancedAnalysisProfileDimension, string[]> {
  return {
    segment: [],
    body_type: [],
    powertrain: [],
    registration_type: [],
    drive_type: [],
    origin: [],
    make: [],
  };
}
function emptyProfileSpecs(): Record<CompetitorProductSpecKey, string> {
  return {
    length_mm: "",
    msrp: "",
    ev_range: "",
    fuel_consumption: "",
    co2_emission: "",
    battery_kwh: "",
  };
}
function buildProfileSpecs(values: Record<CompetitorProductSpecKey, string>): CompetitorProfileSpecs {
  return PRODUCT_SPEC_INPUTS.reduce<CompetitorProfileSpecs>((acc, item) => {
    const numeric = Number(values[item.key]);
    if (Number.isFinite(numeric) && (numeric > 0 || item.key === "co2_emission")) {
      acc[item.key] = numeric;
    }
    return acc;
  }, {});
}
function selectedSpecCount(values: Record<CompetitorProductSpecKey, string>): number {
  return Object.keys(buildProfileSpecs(values)).length;
}
function readCsvParam(searchParams: URLSearchParams, key: string): string[] {
  const raw = searchParams.get(key);
  return raw ? raw.split(",").map(item => item.trim()).filter(Boolean) : [];
}
function selectedProfileCount(selections: Record<AdvancedAnalysisProfileDimension, string[]>): number {
  return PROFILE_DIMENSIONS.reduce((sum, dim) => sum + selections[dim.key].length, 0);
}
function selectedProfileLabel(selections: Record<AdvancedAnalysisProfileDimension, string[]>): string {
  const parts = PROFILE_DIMENSIONS
    .map(dim => {
      const values = selections[dim.key];
      if (values.length === 0) return "";
      return `${dim.label}: ${values.slice(0, 3).join("/")}${values.length > 3 ? "+" : ""}`;
    })
    .filter(Boolean);
  return parts.length > 0 ? parts.join(" · ") : "All market profile";
}
function formatProfileSpecValue(field: CompetitorProductSpecKey, value?: number): string {
  if (value === undefined || value === null || Number.isNaN(value)) return "-";
  if (field === "length_mm") return `${value.toFixed(0)} mm`;
  if (field === "msrp") return value.toLocaleString(undefined, { maximumFractionDigits: 0 });
  if (field === "ev_range") return `${value.toFixed(0)} km`;
  if (field === "fuel_consumption") return value.toFixed(1);
  if (field === "co2_emission") return `${value.toFixed(0)} g/km`;
  if (field === "battery_kwh") return `${value.toFixed(1)} kWh`;
  return `${value}`;
}

/* ── Page ── */

export function AdvancedAnalysisPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const analysisMode = searchParams.get("mode") === "hero-product" ? "hero-product" : "transfer";
  const [country, setCountry] = useState(() => searchParams.get("country") || (() => { try { return sessionStorage.getItem("aa_country"); } catch { return null; } })() || DEFAULT_COUNTRY);
  const [availableCountries, setAvailableCountries] = useState<string[]>([]);
  const [period, setPeriod] = useState(() => searchParams.get("period") || "");
  const [timeMode, setTimeMode] = useState<AdvancedAnalysisSalesMode>("month");
  const [compareMode, setCompareMode] = useState(false);
  const [periodB, setPeriodB] = useState("");
  const TIME_MODES: Array<{ value: AdvancedAnalysisSalesMode; label: string }> = [
    { value: "month", label: "当月" },
    { value: "ytd", label: "YTD" },
    { value: "rolling12", label: "近12月" },
  ];
  const [profileSelections, setProfileSelections] = useState<Record<AdvancedAnalysisProfileDimension, string[]>>(() => ({
    ...emptyProfileSelections(),
    segment: readCsvParam(searchParams, "seg"),
    body_type: readCsvParam(searchParams, "body"),
    powertrain: readCsvParam(searchParams, "powertrain"),
    registration_type: readCsvParam(searchParams, "channel"),
    drive_type: readCsvParam(searchParams, "drive"),
    origin: readCsvParam(searchParams, "origin"),
    make: readCsvParam(searchParams, "make"),
  }));
  const [profileSpecs, setProfileSpecs] = useState<Record<CompetitorProductSpecKey, string>>(() => emptyProfileSpecs());
  const [profileOptions, setProfileOptions] = useState<AdvancedAnalysisProfileOptions>(EMPTY_PROFILE_OPTIONS);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<TransferMartResponse | null>(null);
  const [competitorData, setCompetitorData] = useState<CompetitorSetResponse | null>(null);
  const [competitorLoading, setCompetitorLoading] = useState(false);
  const [competitorError, setCompetitorError] = useState<string | null>(null);
  const [targetModel, setTargetModel] = useState(() => searchParams.get("model") || "");
  const [targetModelSearch, setTargetModelSearch] = useState(() => searchParams.get("model") || "");
  const [filterOpen, setFilterOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [exportSettings, setExportSettings] = useState<ExportSettings>(DEFAULT_AA_EXPORT);
  const countryOptions = useMemo(() => mergeCountryOptions(availableCountries, country), [availableCountries, country]);
  const targetModelCandidates = useMemo(() => {
    const candidates = [
      ...(competitorData?.model_options || []),
      ...profileOptions.model,
      ...(data?.models.map(model => model.model) || []),
      competitorData?.target_model || "",
      targetModel,
    ];
    return Array.from(new Set(candidates.map(model => model.trim()).filter(Boolean)));
  }, [competitorData?.model_options, competitorData?.target_model, data?.models, profileOptions.model, targetModel]);
  const targetModelMatches = useMemo(() => {
    const ranked: RankedModelOption[] = [];
    targetModelCandidates.forEach((model, index) => {
      const score = scoreModelOption(model, targetModelSearch);
      if (score === null) return;
      ranked.push({ model, score: model === targetModel ? score + 25 : score, index });
    });
    return ranked
      .sort((a, b) => b.score - a.score || a.index - b.index || a.model.localeCompare(b.model))
      .slice(0, 16)
      .map(option => option.model);
  }, [targetModel, targetModelCandidates, targetModelSearch]);
  const targetSearchTrimmed = targetModelSearch.trim();
  const exactTargetModel = useMemo(() => {
    if (!targetSearchTrimmed) return "";
    const normalizedSearch = normalizeLookupText(targetSearchTrimmed);
    const compactSearch = compactLookupText(targetSearchTrimmed);
    return targetModelCandidates.find(model => normalizeLookupText(model) === normalizedSearch || compactLookupText(model) === compactSearch) || "";
  }, [targetModelCandidates, targetSearchTrimmed]);
  const suggestedTargetModel = exactTargetModel || targetModelMatches[0] || targetSearchTrimmed;
  const canApplyTargetModel = Boolean(targetSearchTrimmed && suggestedTargetModel !== targetModel);
  const switchAnalysisMode = useCallback((mode: "transfer" | "hero-product") => {
    const params = new URLSearchParams(searchParams);
    if (mode === "hero-product") {
      params.set("mode", "hero-product");
    } else {
      params.delete("mode");
    }
    setSearchParams(params, { replace: true });
  }, [searchParams, setSearchParams]);

  // URL sync
  useEffect(() => {
    if (analysisMode !== "transfer") return;
    const p = new URLSearchParams();
    if (country) p.set("country", country);
    if (period) p.set("period", period);
    if (profileSelections.segment.length > 0) p.set("seg", profileSelections.segment.join(","));
    if (profileSelections.body_type.length > 0) p.set("body", profileSelections.body_type.join(","));
    if (profileSelections.powertrain.length > 0) p.set("powertrain", profileSelections.powertrain.join(","));
    if (profileSelections.registration_type.length > 0) p.set("channel", profileSelections.registration_type.join(","));
    if (profileSelections.drive_type.length > 0) p.set("drive", profileSelections.drive_type.join(","));
    if (profileSelections.origin.length > 0) p.set("origin", profileSelections.origin.join(","));
    if (profileSelections.make.length > 0) p.set("make", profileSelections.make.join(","));
    if (targetModel) p.set("model", targetModel);
    setSearchParams(p, { replace: true });
    try { sessionStorage.setItem("aa_country", country); } catch { /* ignore */ }
  }, [analysisMode, country, period, profileSelections, targetModel, setSearchParams]);

  useEffect(() => {
    if (analysisMode !== "transfer") return;
    const controller = new AbortController();
    api.get<AdvancedAnalysisCountriesResponse>("/advanced-analysis/countries", { signal: controller.signal })
      .then(response => setAvailableCountries(response.countries || []))
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        if (isAbortError(error)) return;
        setAvailableCountries([]);
    });
    return () => controller.abort();
  }, [analysisMode]);

  useEffect(() => {
    setTargetModelSearch(targetModel);
  }, [targetModel]);

  // Load profile options when country changes
  useEffect(() => {
    if (analysisMode !== "transfer") return;
    const controller = new AbortController();
    setProfileOptions(EMPTY_PROFILE_OPTIONS);
    const timeoutId = window.setTimeout(() => {
      api.get<AdvancedAnalysisProfileOptionsResponse>(
        `/advanced-analysis/profile-options?country=${encodeURIComponent(country)}`,
        { signal: controller.signal },
      )
      .then(r => setProfileOptions({ ...EMPTY_PROFILE_OPTIONS, ...(r.options || {}) }))
      .catch((error: unknown) => {
        if (controller.signal.aborted) return;
        if (isAbortError(error)) return;
        setProfileOptions(EMPTY_PROFILE_OPTIONS);
      });
    }, 250);
    return () => {
      window.clearTimeout(timeoutId);
      controller.abort();
    };
  }, [analysisMode, country]);

  useEffect(() => {
    if (analysisMode !== "transfer") return;
    if (!targetModel || profileOptions.model.length === 0) return;
    if (profileOptions.model.includes(targetModel)) return;
    setTargetModel("");
  }, [analysisMode, profileOptions.model, targetModel]);

  // Fetch data
  const buildScope = useCallback(() => {
    const s: Array<{ dim: string; value: string }> = [];
    for (const dim of PROFILE_DIMENSIONS) {
      for (const value of profileSelections[dim.key]) {
        s.push({ dim: dim.key, value });
      }
    }
    return s;
  }, [profileSelections]);

  const fetchData = useCallback(async (signal?: AbortSignal) => {
    if (analysisMode !== "transfer") return;
    setLoading(true); setError(null);
    setCompetitorLoading(false);
    setCompetitorError(null);
    setCompetitorData(null);
    try {
      const scope = buildScope();
      const targetPeriod = compareMode && periodB ? periodB : period;
      const basePeriod = compareMode && period && periodB ? period : undefined;
      const payload: AdvancedAnalysisTransferMartRequest = {
        country,
        target_period: targetPeriod || undefined,
        base_period: basePeriod,
        sales_mode: timeMode,
        scope_filters: scope,
        fuel_types: [],
        top_n: 25,
      };
      const competitorPayload: AdvancedAnalysisCompetitorSetRequest = {
        ...payload,
        target_model: targetModel || undefined,
        profile_specs: buildProfileSpecs(profileSpecs),
        top_n: 12,
      };
      const martResult = await api.post<TransferMartResponse>(
        "/advanced-analysis/transfer-mart",
        payload,
        { signal },
      );
      if (signal?.aborted) return;
      setData(martResult);
      setLoading(false);
      setCompetitorLoading(true);
      try {
        const competitorResult = await api.post<CompetitorSetResponse>(
          "/advanced-analysis/competitor-set",
          competitorPayload,
          { signal },
        );
        if (signal?.aborted) return;
        setCompetitorData(competitorResult);
      } catch (competitorFetchError: unknown) {
        if (signal?.aborted || isAbortError(competitorFetchError)) return;
        setCompetitorError(competitorFetchError instanceof Error ? competitorFetchError.message : "Competitor set failed");
      } finally {
        if (!signal?.aborted) setCompetitorLoading(false);
      }
    } catch (e: unknown) {
      if (signal?.aborted) return;
      if (isAbortError(e)) return;
      setError(e instanceof Error ? e.message : "Failed");
    }
    finally {
      if (!signal?.aborted) {
        setLoading(false);
        setCompetitorLoading(false);
      }
    }
  }, [analysisMode, country, period, timeMode, buildScope, compareMode, periodB, targetModel, profileSpecs]);
  useEffect(() => {
    if (analysisMode !== "transfer") return;
    const controller = new AbortController();
    const timeoutId = window.setTimeout(() => {
      void fetchData(controller.signal);
    }, 500);
    return () => {
      window.clearTimeout(timeoutId);
      controller.abort();
    };
  }, [analysisMode, fetchData]);

  // Drawer mutual exclusion
  const hFO = useCallback((o: boolean) => { setFilterOpen(o); if (o) setExportOpen(false); }, []);
  const hEO = useCallback((o: boolean) => { setExportOpen(o); if (o) setFilterOpen(false); }, []);
  const handleExportPng = useCallback(() => {
    const graphDiv = getGraphDiv();
    if (!graphDiv) return;
    void import("../components/ExportPanelHelpers").then((module) => {
      void module.downloadPng(graphDiv, exportSettings);
    });
  }, [exportSettings]);

  // ── Synthesized conclusion narrative ──
  const s = data?.scope_summary;
  const narrative = useMemo(() => {
    if (!s || !data) return null;
    const topW = data.winners?.[0];
    const topL = data.losers?.[0];
    const scopeDesc = selectedProfileLabel(profileSelections);
    return {
      state: s.market_state,
      dM: s.dM,
      yoy: s.yoy_pct,
      topW: topW ? { model: topW.model, gain: topW.dV, driver: dominantComponent(topW) } : null,
      topL: topL ? { model: topL.model, loss: topL.dV, driver: dominantComponent(topL) } : null,
      scope: scopeDesc,
      modelCount: data.models.length,
    };
  }, [data, s, profileSelections]);

  if (analysisMode === "hero-product") {
    return (
      <Suspense fallback={<PageLoadingShell kicker="Advanced" label="Loading Hero Product analysis..." />}>
        <HeroProductAnalysisView onSwitchToTransfer={() => switchAnalysisMode("transfer")} />
      </Suspense>
    );
  }

  return (
    <div className="market-scan-page">
      {/* Hero */}
      <section className="header-card dashboard-hero market-scan-hero">
        <div className="dashboard-hero-head">
          <div className="dashboard-hero-copy market-scan-hero-copy">
            <span className="page-kicker">Advanced Analysis</span>
            <h1>Profile-Based Competitive Transfer</h1>
            <p>Describe a product profile with reusable filters, then read share transfer, channel quality, and the closest competitive battlefield.</p>
            <div className="market-scan-hero-ribbon aa-mode-ribbon">
              <button type="button" className="btn btn-primary btn-sm aa-mode-btn" onClick={() => switchAnalysisMode("transfer")}>
                Share Transfer
              </button>
              <button type="button" className="btn btn-ghost btn-sm aa-mode-btn" onClick={() => switchAnalysisMode("hero-product")}>
                Hero Product 分析
              </button>
              {s ? (
                <>
                <span className="market-scan-hero-chip" style={{ color: stateColor(s.market_state), fontWeight: 700 }}>
                  {s.market_state === "growth" ? "Growth" : s.market_state === "decline" ? "Decline" : "Stable"}
                </span>
                <span className="market-scan-hero-chip">ΔM {s.dM > 0 ? "+" : ""}{fmtNum(s.dM)}</span>
                <span className="market-scan-hero-chip">YoY {fmtPct(s.yoy_pct)}</span>
                <span className="market-scan-hero-chip">{country}</span>
                <span className="market-scan-hero-chip">{compareMode && periodB ? `${period || "Auto base"} -> ${periodB}` : period || "Latest"}</span>
                <span className="market-scan-hero-chip" style={{ background: "#e0f2fe" }}>Profile {selectedProfileCount(profileSelections)}</span>
                {selectedSpecCount(profileSpecs) > 0 && <span className="market-scan-hero-chip" style={{ background: "#ecfdf5" }}>Specs {selectedSpecCount(profileSpecs)}</span>}
                {(targetModel || competitorData?.target_model) && (
                  <span className="market-scan-hero-chip" style={{ background: "#e0f2fe" }}>
                    {competitorData?.analysis_mode === "profile"
                      ? competitorData.target_model
                      : `Target: ${targetModel || competitorData?.target_model}`}
                  </span>
                )}
                </>
              ) : null}
            </div>
          </div>
        </div>
      </section>

      {/* Synthesized narrative */}
      {narrative && (
        <div style={{ padding: "12px 16px", background: "linear-gradient(90deg, #f0fdf4, #f8fafc, #fef2f2)", borderBottom: "1px solid #e2e8f0", fontSize: 13, lineHeight: 1.6 }}>
          <strong style={{ color: stateColor(narrative.state) }}>
            {narrative.state === "growth" ? "Growth market" : narrative.state === "decline" ? "Declining market" : "Stable market"}
          </strong>
          {" - "}In <strong>{narrative.scope}</strong>, total volume changed by <strong>{narrative.dM > 0 ? "+" : ""}{fmtNum(narrative.dM)}</strong> units (YoY {fmtPct(narrative.yoy)}).
          {narrative.topW && <> <strong style={{ color: COLORS.winner }}>{narrative.topW.model}</strong> gained <strong>+{fmtNum(narrative.topW.gain)}</strong>, driven primarily by <strong>{narrative.topW.driver}</strong>.</>}
          {narrative.topL && <> <strong style={{ color: COLORS.loser }}>{narrative.topL.model}</strong> lost <strong>{fmtNum(narrative.topL.loss)}</strong>, hurt by <strong>{narrative.topL.driver}</strong>.</>}
          {" "}<span style={{ color: "#94a3b8" }}>({narrative.modelCount} models analyzed)</span>
        </div>
      )}

      {/* Floating drawers */}
      <DeckFloatingDrawer open={filterOpen} onOpenChange={hFO} triggerPrimary="分析筛选" triggerSecondaryOpen="收起" triggerSecondaryClosed="打开"
        eyebrow="Controls" title="筛选条件" ariaLabel="Filters"
        footer={<div className="market-scan-toolbar-meta"><span className="market-scan-toolbar-chip">{country}</span><span className="market-scan-toolbar-chip">{period || "Latest"}</span><span className="market-scan-toolbar-chip">Profile {selectedProfileCount(profileSelections)}</span><span className="market-scan-toolbar-chip">Specs {selectedSpecCount(profileSpecs)}</span></div>}
      >
        <div className="deck-panel-grid">
          <label className="market-scan-field"><span>Country</span>
            <select value={country} onChange={e => setCountry(e.target.value)}>
              {countryOptions.map(option => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </label>
          <label className="market-scan-field"><span>{compareMode ? "Period A" : "Period"}</span>
            <input type="month" value={period} onChange={e => { setPeriod(e.target.value); setTimeMode("month"); }}
              style={{ minHeight: 40, width: "100%", padding: "10px 12px", border: "1px solid rgba(15,23,42,0.12)", borderRadius: 6, fontSize: 13 }} />
          </label>
          <label className="market-scan-field"><span>Time Range</span>
            <div style={{ display: "flex", gap: 2, background: "#f1f5f9", borderRadius: 6, padding: 2 }}>
              {TIME_MODES.map(tm => (
                <button key={tm.value} type="button"
                  onClick={() => setTimeMode(tm.value)}
                  style={{ flex: 1, padding: "6px 8px", fontSize: 11, fontWeight: 600, border: "none", borderRadius: 4, cursor: "pointer",
                    background: timeMode === tm.value ? "#fff" : "transparent",
                    color: timeMode === tm.value ? "#1e293b" : "#64748b",
                    boxShadow: timeMode === tm.value ? "0 1px 3px rgba(0,0,0,0.1)" : "none",
                  }}>{tm.label}</button>
              ))}
            </div>
          </label>
          <div className="market-scan-field deck-panel-grid__wide">
            <span>Target Model</span>
            <div className="aa-target-model-picker">
              <input
                type="search"
                value={targetModelSearch}
                onChange={e => setTargetModelSearch(e.target.value)}
                onKeyDown={event => {
                  if (event.key !== "Enter") return;
                  event.preventDefault();
                  setTargetModel(targetSearchTrimmed ? suggestedTargetModel : "");
                }}
                placeholder="Search target model"
                className="aa-target-model-input"
              />
              <div className="aa-target-model-actions">
                <span className="aa-target-model-status">{targetModel ? `Target: ${targetModel}` : "Profile mode"}</span>
                {canApplyTargetModel ? (
                  <button type="button" className="aa-target-model-action" onClick={() => setTargetModel(suggestedTargetModel)}>Apply</button>
                ) : null}
                {targetModel || targetModelSearch ? (
                  <button type="button" className="aa-target-model-action" onClick={() => { setTargetModel(""); setTargetModelSearch(""); }}>Clear</button>
                ) : null}
              </div>
              <div className="aa-target-model-options">
                {targetModelMatches.map(model => (
                  <button
                    key={model}
                    type="button"
                    className={`aa-target-model-option${model === targetModel ? " is-active" : ""}`}
                    onClick={() => setTargetModel(model)}
                  >
                    <span>{model}</span>
                  </button>
                ))}
                {targetModelMatches.length === 0 && targetSearchTrimmed ? (
                  <div className="aa-target-model-empty">No matching model</div>
                ) : null}
              </div>
            </div>
          </div>
          <div className="market-scan-field deck-panel-grid__wide">
            <span>Known Product Specs</span>
            <div className="aa-spec-input-grid">
              {PRODUCT_SPEC_INPUTS.map(item => (
                <label key={item.key} className="aa-spec-input">
                  <span>{item.label}</span>
                  <div className="aa-spec-input-control">
                    <input
                      type="number"
                      min={item.key === "co2_emission" ? 0 : 1}
                      step={item.key === "fuel_consumption" || item.key === "battery_kwh" ? 0.1 : 1}
                      inputMode="decimal"
                      value={profileSpecs[item.key]}
                      onChange={e => setProfileSpecs(current => ({ ...current, [item.key]: e.target.value }))}
                      placeholder={item.placeholder}
                    />
                    {item.suffix && <em>{item.suffix}</em>}
                  </div>
                </label>
              ))}
            </div>
          </div>
          <div className="deck-panel-grid__wide" style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: 10 }}>
            {PROFILE_DIMENSIONS.map(dim => (
              <SearchSelectFilter
                key={dim.key}
                label={dim.label}
                options={profileOptions[dim.key] || []}
                selected={profileSelections[dim.key]}
                showSuvShortcut={dim.showSuvShortcut}
                onChange={values => setProfileSelections(current => ({ ...current, [dim.key]: values }))}
              />
            ))}
          </div>
          <label className="market-scan-field deck-panel-grid__wide" style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <span>Compare A vs B</span>
            <input type="checkbox" checked={compareMode} onChange={e => setCompareMode(e.target.checked)} />
          </label>
          {compareMode && (
            <label className="market-scan-field"><span>Period B</span>
              <input type="month" value={periodB} onChange={e => setPeriodB(e.target.value)}
                style={{ minHeight: 40, width: "100%", padding: "10px 12px", border: "1px solid rgba(15,23,42,0.12)", borderRadius: 6, fontSize: 13 }} />
            </label>
          )}
          <div className="market-scan-field market-scan-field-actions deck-panel-grid__wide">
            <span>Deck</span>
            <div className="btn-group">
              <button type="button" className="btn btn-secondary btn-sm" onClick={() => void fetchData()}>Refresh</button>
              <button type="button" className="btn btn-ghost btn-sm" onClick={() => { setProfileSelections(emptyProfileSelections()); setProfileSpecs(emptyProfileSpecs()); setTargetModel(""); setPeriodB(""); setCompareMode(false); setError(null); }}>Reset All</button>
            </div>
          </div>
        </div>
      </DeckFloatingDrawer>

      <DeckExportDrawer open={exportOpen} onOpenChange={hEO} triggerPrimary="导出图表 PNG" triggerSecondaryOpen="收起" triggerSecondaryClosed="展开"
        eyebrow="Export" title="导出与图表样式" ariaLabel="Export"
        footer={<div className="market-scan-toolbar-meta"><span className="market-scan-toolbar-chip">{exportSettings.exportWidth}×{exportSettings.exportHeight}</span></div>}
      >
        <button type="button" className="btn btn-primary btn-sm" onClick={handleExportPng} style={{ marginBottom: 12, width: "100%" }}>Export Current Chart PNG</button>
        <Suspense fallback={<div className="market-scan-toolbar-meta">Loading export settings...</div>}>
          <AdvancedAnalysisExportPanel value={exportSettings} onChange={setExportSettings} showExportButton={false} collapsible={false} />
        </Suspense>
      </DeckExportDrawer>

      <PageBannerStack
        items={[
          ...(error ? [{ id: "advanced-analysis-error", tone: "error" as const, title: "Advanced Analysis 加载失败", message: error }] : []),
          ...(loading && data ? [{ id: "advanced-analysis-refreshing", tone: "info" as const, title: "Refreshing", message: "正在刷新分析结果" }] : []),
          ...(competitorLoading && data ? [{ id: "advanced-analysis-competitor-loading", tone: "info" as const, title: "Competitor analysis", message: "主分析已可用，竞品战场正在后台补齐。" }] : []),
          ...(competitorError ? [{ id: "advanced-analysis-competitor-error", tone: "warning" as const, title: "竞品分析加载失败", message: competitorError }] : []),
        ]}
      />
      {loading && !data && <PageLoadingShell kicker="Advanced" label="Analyzing..." />}

      {data && !data.error && (
        <div style={{ padding: "12px 16px 24px" }}>
          {competitorData && !competitorData.error && (
            <>
              <div style={RESPONSIVE_TWO_COL}>
                <ChartCard title={`${competitorData.target_model} Sales x Channel Quality`} subtitle="Business/Private volume bars with total sales and channel share overlays">
                  <ModelChannelOverlayChart data={competitorData.model_channel_timeseries} targetModel={competitorData.target_model} />
                </ChartCard>
                <ChartCard title="Competitive Battlefield" subtitle="Similar products ranked by likely gain/loss pressure">
                  <CompetitorBattleChart data={competitorData} />
                </ChartCard>
              </div>
              <div style={{ marginBottom: 16 }}>
                <ChartCard title="Competitor Match Matrix" subtitle="Field-level match: product profile, specs, sales shift, and channel pressure">
                  <CompetitorMatrix data={competitorData} />
                </ChartCard>
              </div>
            </>
          )}

          {/* Market context */}
          <div style={RESPONSIVE_TWO_COL}>
            <ChartCard title="Market Decomposition Waterfall" subtitle={`${data.base_period} -> ${data.target_period}`}>
              <MarketWaterfallChart data={data} />
            </ChartCard>
            <ChartCard title="Winner / Loser Butterfly" subtitle="By pure share shift">
              <ButterflyChart winners={data.winners} losers={data.losers} />
            </ChartCard>
          </div>

          {/* Market channel context */}
          <div style={RESPONSIVE_TWO_COL}>
            <ChartCard title="Channel Volume (Stacked)" subtitle="Business / Private absolute volume">
              <ChannelVolumeChart ts={data.channel_timeseries} />
            </ChartCard>
            <ChartCard title="Channel Share (Indexed, base=100)" subtitle="Relative share change without dual-axis distortion">
              <ChannelShareChart ts={data.channel_timeseries} />
            </ChartCard>
          </div>

          {/* Transfer Ledger */}
          <div style={{ marginBottom: 16 }}>
            <ChartCard title="Transfer Ledger" subtitle={`${data.models.length} models ranked by |dV|`}>
              <TransferLedger models={data.models} basePeriod={data.base_period} targetPeriod={data.target_period} tsData={data.model_timeseries} />
            </ChartCard>
          </div>

          {/* Structure context */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16, marginBottom: 16 }}>
            <ChartCard title="Powertrain Cumulative" subtitle="BEV / HEV / PHEV / ICE stacked volume">
              <PowertrainStackedChart ts={data.powertrain_timeseries} />
            </ChartCard>
          </div>

          {/* Cell pressure and persistence */}
          <div style={RESPONSIVE_TWO_COL}>
            <ChartCard title="Channel × Drive Heatmap" subtitle="Net volume shift per cell">
              <HeatmapChart cells={data.channel_drive_heatmap} />
            </ChartCard>
            <ChartCard title="Share Momentum" subtitle="Recent 3-period share slope (top 12 models)">
              <MomentumChart momentum={data.momentum} />
            </ChartCard>
          </div>

          {/* Row 5: Powertrain × Origin Breakdown */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr", gap: 16, marginBottom: 16 }}>
            <ChartCard title="Powertrain × Origin Breakdown" subtitle="Share shift by powertrain and origin">
              <PowertrainOriginChart data={data.powertrain_origin_breakdown} />
            </ChartCard>
          </div>

          {/* Compare mode: Period A vs Period B direct decomposition */}
          {compareMode && period && periodB && (
            <div style={{ marginBottom: 16, padding: 12, background: "#f8fafc", borderRadius: 8, border: "1px solid #e2e8f0" }}>
              <h4 style={{ margin: "0 0 8px", fontSize: 13 }}>
                Period Comparison: {data.base_period}{" -> "}{data.target_period}
              </h4>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, fontSize: 12 }}>
                <div><strong>Market ΔM:</strong> <span style={{ color: data.scope_summary.dM >= 0 ? COLORS.winner : COLORS.loser }}>{data.scope_summary.dM > 0 ? "+" : ""}{fmtNum(data.scope_summary.dM)}</span></div>
                <div><strong>Top winners:</strong> {data.winners.slice(0, 3).map(w => w.model).join(", ") || "-"}</div>
                <div><strong>Top losers:</strong> {data.losers.slice(0, 3).map(l => l.model).join(", ") || "-"}</div>
              </div>
            </div>
          )}

        </div>
      )}

      {!loading && !error && !data && (
        <div style={{ padding: 60, textAlign: "center", color: "#64748b" }}>
          <h3>Share Transfer Analysis</h3>
          <p>No analysis result for the current scope.</p>
        </div>
      )}
    </div>
  );
}

/* ── ChartCard wrapper ── */

function ChartCard({ title, subtitle, note, children }: { title: string; subtitle?: string; note?: string; children: ReactNode }) {
  return (
    <div className="chart-card" style={{ padding: 12 }}>
      <div style={{ marginBottom: 8 }}>
        <h4 style={{ margin: 0, fontSize: 13, fontWeight: 700 }}>{title}</h4>
        {subtitle && <span style={{ fontSize: 11, color: "#94a3b8" }}>{subtitle}</span>}
      </div>
      {children}
      {note && <div style={{ marginTop: 8, fontSize: 10, color: "#94a3b8", fontStyle: "italic" }}>{note}</div>}
    </div>
  );
}

/* Product competitor view */

function ModelChannelOverlayChart({ data, targetModel }: { data: ModelChannelTimeseriesItem[]; targetModel: string }) {
  const chart = useMemo(() => {
    const rows = data.filter(row => row.model === targetModel);
    if (rows.length === 0) return null;
    const periods = [...new Set(rows.map(row => row.period))].sort();
    const channels = [...new Set(rows.map(row => row.channel))].sort((a, b) => {
      const order = ["Business", "Private", "Other"];
      return (order.indexOf(a) < 0 ? 99 : order.indexOf(a)) - (order.indexOf(b) < 0 ? 99 : order.indexOf(b));
    });
    const channelColors: Record<string, string> = { Business: "#2563eb", Private: "#16a34a", Other: "#94a3b8" };
    const traces: Data[] = channels.map(channel => ({
      x: periods,
      y: periods.map(period => rows.find(row => row.period === period && row.channel === channel)?.volume || 0),
      type: "bar" as const,
      name: `${channel} volume`,
      marker: { color: channelColors[channel] || "#94a3b8" },
      hovertemplate: `%{x}<br>${channel}: %{y:,.0f}<extra></extra>`,
    }));
    traces.push({
      x: periods,
      y: periods.map(period => rows.find(row => row.period === period)?.total_volume || 0),
      type: "scatter",
      mode: "lines+markers",
      name: "Total sales",
      line: { color: "#0f172a", width: 2.5 },
      marker: { color: "#0f172a", size: 5 },
      hovertemplate: "%{x}<br>Total: %{y:,.0f}<extra></extra>",
    });
    for (const channel of ["Business", "Private"]) {
      if (!channels.includes(channel)) continue;
      traces.push({
        x: periods,
        y: periods.map(period => rows.find(row => row.period === period && row.channel === channel)?.share || 0),
        type: "scatter",
        mode: "lines",
        name: `${channel} share`,
        yaxis: "y2",
        line: { color: channelColors[channel], width: 2, dash: "dot" },
        hovertemplate: `%{x}<br>${channel} share: %{y:.1%}<extra></extra>`,
      });
    }
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT,
      margin: { l: 54, r: 58, t: 20, b: 72 },
      barmode: "stack" as const,
      yaxis: { title: { text: "Sales volume" } },
      yaxis2: { title: { text: "Channel share" }, overlaying: "y", side: "right", range: [0, 1], tickformat: ".0%" },
      legend: { orientation: "h", y: 1.22, x: 0 },
      hovermode: "x unified",
    };
    return { traces, layout };
  }, [data, targetModel]);

  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No channel series for target model</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 330 }} />;
}

function CompetitorBattleChart({ data }: { data: CompetitorSetResponse }) {
  const chart = useMemo(() => {
    const rows = data.competitors.slice(0, 12);
    if (rows.length === 0) return null;
    const hasFlow = rows.some(row => row.estimated_flow > 0);
    const sorted = [...rows].sort((a, b) => {
      const av = hasFlow ? a.estimated_flow : Math.abs(a.pure_share_shift);
      const bv = hasFlow ? b.estimated_flow : Math.abs(b.pure_share_shift);
      return av - bv;
    });
    const values = sorted.map(row => hasFlow ? row.estimated_flow : row.pure_share_shift);
    const traces: Data[] = [{
      y: sorted.map(row => row.model),
      x: values,
      type: "bar",
      orientation: "h",
      marker: { color: sorted.map(row => roleColor(row.role)) },
      text: sorted.map(row => `${row.similarity_score.toFixed(0)}% match`),
      textposition: "auto" as const,
      customdata: sorted.map(row => [row.pure_share_shift, row.dV, row.match_evidence?.slice(0, 3).map(item => item.detail).join("<br>") || row.shared_dims.join(", ")]),
      hovertemplate: [
        "%{y}",
        hasFlow ? "<br>Estimated flow: %{x:,.0f}" : "<br>Share shift: %{x:+,.0f}",
        "<br>Pure share shift: %{customdata[0]:+,.0f}",
        "<br>dV: %{customdata[1]:+,.0f}",
        "<br>%{customdata[2]}",
        "<extra></extra>",
      ].join(""),
    }];
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT,
      margin: { l: 112, r: 24, t: 20, b: 44 },
      xaxis: { title: { text: hasFlow ? "Estimated competitive flow" : "Pure share shift" }, zeroline: true },
      yaxis: { automargin: true },
      showlegend: false,
    };
    return { traces, layout };
  }, [data]);

  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No competitor set for target model</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 330 }} />;
}

function CompetitorMatrix({ data }: { data: CompetitorSetResponse }) {
  const categoricalDims = ["powertrain", "segment", "body_type", "registration_type", "drive_type", "origin"] as const;
  const specDims = PRODUCT_SPEC_INPUTS.filter(item => ["length_mm", "msrp", "ev_range", "fuel_consumption", "co2_emission", "battery_kwh"].includes(item.key));
  const targetProfile = data.target.profile;
  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
        <thead>
          <tr style={{ background: "#f8fafc", borderBottom: "2px solid #e2e8f0" }}>
            <th style={thStyle}>Competitor</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Match</th>
            <th style={{ ...thStyle, textAlign: "right" }}>dV</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Share Shift</th>
            <th style={{ ...thStyle, textAlign: "right" }}>Flow</th>
            <th style={{ ...thStyle, minWidth: 220 }}>Evidence</th>
            {categoricalDims.map(dim => <th key={dim} style={thStyle}>{dim.replace("_", " ")}</th>)}
            {specDims.map(item => <th key={item.key} style={thStyle}>{item.label}</th>)}
          </tr>
        </thead>
        <tbody>
          {data.competitors.slice(0, 12).map(row => {
            const evidenceByField = new Map((row.match_evidence || []).map(item => [item.field, item]));
            const topEvidence = (row.match_evidence || []).slice(0, 3);
            return (
              <tr key={row.model} style={{ borderBottom: "1px solid #eef2f7" }}>
                <td style={{ ...tdStyle, fontWeight: 700, color: roleColor(row.role), minWidth: 132 }}>
                  <div>{row.model}</div>
                  {row.make && <div style={{ color: "#94a3b8", fontSize: 10, fontWeight: 600 }}>{row.make}</div>}
                </td>
                <td style={{ ...tdStyle, textAlign: "right" }}>{row.similarity_score.toFixed(0)}%</td>
                <td style={{ ...tdStyle, textAlign: "right", color: row.dV >= 0 ? COLORS.winner : COLORS.loser }}>{row.dV > 0 ? "+" : ""}{fmtNum(row.dV)}</td>
                <td style={{ ...tdStyle, textAlign: "right", color: row.pure_share_shift >= 0 ? COLORS.winner : COLORS.loser }}>{row.pure_share_shift > 0 ? "+" : ""}{fmtNum(row.pure_share_shift)}</td>
                <td style={{ ...tdStyle, textAlign: "right" }}>{row.estimated_flow > 0 ? fmtNum(row.estimated_flow) : "-"}</td>
                <td style={{ ...tdStyle, minWidth: 220, color: "#334155" }}>
                  {topEvidence.length > 0
                    ? topEvidence.map(item => (
                      <span key={`${row.model}-${item.field}`} style={{ display: "block", lineHeight: 1.45 }}>
                        {item.detail} <strong>{item.score.toFixed(0)}%</strong>
                      </span>
                    ))
                    : row.shared_dims.join(", ") || "-"}
                </td>
                {categoricalDims.map(dim => {
                  const targetValues = String(targetProfile[dim] || "").split(" / ").map(value => value.trim()).filter(Boolean);
                  const same = Boolean(row.profile[dim] && (targetValues.length === 0 || targetValues.includes(String(row.profile[dim]))));
                  return (
                    <td key={dim} style={{ ...tdStyle, background: same ? "#ecfdf5" : "transparent", color: same ? "#166534" : "#64748b" }}>
                      {row.profile[dim] || "-"}
                    </td>
                  );
                })}
                {specDims.map(item => {
                  const evidence = evidenceByField.get(item.key);
                  const same = Boolean(evidence && evidence.score >= 65);
                  return (
                    <td key={item.key} style={{ ...tdStyle, background: same ? "#ecfeff" : "transparent", color: same ? "#155e75" : "#64748b", whiteSpace: "nowrap" }}>
                      {formatProfileSpecValue(item.key, row.profile[item.key])}
                    </td>
                  );
                })}
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

/* ── Chart 1: Market Waterfall ── */

function MarketWaterfallChart({ data }: { data: TransferMartResponse }) {
  const chart = useMemo(() => {
    const items = data.market_waterfall || [];
    // Build waterfall: running total with relative bars
    const labels = items.map(i => i.label);
    const values = items.map(i => i.value);
    const colors = items.map(i => COLORS[i.kind] || COLORS.stable);

    const traces: Data[] = [{
      x: labels, y: values, type: "bar",
      marker: { color: colors },
      text: values.map(v => `${v > 0 ? "+" : ""}${fmtNum(v)}`),
      textposition: "outside" as const,
      hovertemplate: "%{x}: %{y:+,.0f}<extra></extra>",
    }];
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { ...CHART_MARGIN, b: 80 },
      title: pt("Δ Volume Decomposition"), yaxis: { title: { text: "Volume" } },
    };
    return { traces, layout };
  }, [data]);

  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 280 }} />;
}

/* ── Chart 2: Butterfly ── */

function ButterflyChart({ winners, losers }: { winners: TransferMartModel[]; losers: TransferMartModel[] }) {
  const chart = useMemo(() => {
    const merged = [
      ...losers.slice(0, 10).map(m => ({ model: m.model, val: m.pure_share_shift, kind: "Loser" })),
      ...winners.slice(0, 10).map(m => ({ model: m.model, val: m.pure_share_shift, kind: "Winner" })),
    ].sort((a, b) => b.val - a.val);
    const traces: Data[] = [{
      x: merged.map(m => m.model), y: merged.map(m => m.val), type: "bar",
      marker: { color: merged.map(m => m.kind === "Winner" ? COLORS.winner : COLORS.loser) },
      text: merged.map(m => `${m.val > 0 ? "+" : ""}${fmtNum(m.val)}`),
      textposition: "outside" as const,
    }];
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { ...CHART_MARGIN, b: 100 },
      yaxis: { title: { text: "Pure Share Shift" } },
      showlegend: false,
    };
    return { traces, layout };
  }, [winners, losers]);

  if (winners.length === 0 && losers.length === 0) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No data</div>;

  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 300 }} />;
}

/* ── Chart 3: Sankey ── */

function SankeyChart({ winners, losers }: { winners: TransferMartModel[]; losers: TransferMartModel[] }) {
  const chart = useMemo(() => {
    const topW = winners.slice(0, 8);
    const topL = losers.slice(0, 8);
    if (topW.length === 0 || topL.length === 0) return null;

    const nodes: Array<{ label: string; color: string }> = [];
    const nodeMap: Record<string, number> = {};
    const links: Array<{ source: number; target: number; value: number }> = [];

    for (const l of topL) { nodeMap[l.model] = nodes.length; nodes.push({ label: `${l.model} loss`, color: COLORS.loser }); }
    for (const w of topW) { nodeMap[w.model] = nodes.length; nodes.push({ label: `${w.model} gain`, color: COLORS.winner }); }

    const totalLoss = topL.reduce((s, l) => s + Math.abs(l.pure_share_shift), 0);
    if (totalLoss > 0) {
      for (const l of topL) {
        for (const w of topW) {
          const flow = w.pure_share_shift * Math.abs(l.pure_share_shift) / totalLoss;
          if (flow > 0.05) links.push({ source: nodeMap[l.model], target: nodeMap[w.model], value: Math.max(0.1, Math.round(flow * 10) / 10) });
        }
      }
    }

    const traces: Data[] = [{
      type: "sankey" as const, orientation: "h" as const,
      node: { pad: 12, thickness: 16, line: { color: "#94a3b8", width: 0.3 }, label: nodes.map(n => n.label), color: nodes.map(n => n.color) },
      link: { source: links.map(l => l.source), target: links.map(l => l.target), value: links.map(l => l.value), color: links.map(() => "rgba(148,163,184,0.25)") },
    }];
    const layout: Partial<PlotlyLayout> = { ...CHART_LAYOUT, margin: { l: 8, r: 8, t: 8, b: 8 } };
    return { traces, layout };
  }, [winners, losers]);

  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>Insufficient data for Sankey</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 320 }} />;
}

/* ── Chart 4: Channel × Drive Heatmap ── */

function HeatmapChart({ cells }: { cells: TransferMartResponse["channel_drive_heatmap"] }) {
  const chart = useMemo(() => {
    if (!cells || cells.length === 0) return null;
    const channels = [...new Set(cells.map(c => c.channel))].sort();
    const drives = [...new Set(cells.map(c => c.drive))].sort();
    const z = channels.map(ch => drives.map(dr => cells.find(c => c.channel === ch && c.drive === dr)?.net_shift || 0));
    const maxAbs = Math.max(...z.flat().map(v => Math.abs(v)), 1);

    const traces: Data[] = [{
      x: drives, y: channels, z, type: "heatmap" as const,
      colorscale: [[0, COLORS.loser], [0.5, "#f8fafc"], [1, COLORS.winner]],
      zmin: -maxAbs, zmax: maxAbs,
      hovertemplate: "%{y} × %{x}<br>Net Shift: %{z:+,.0f}<extra></extra>",
    }];
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { l: 80, r: 24, t: 20, b: 48 },
      xaxis: { title: { text: "Drive Type" }, side: "bottom" },
      yaxis: { title: { text: "Channel" } },
    };
    return { traces, layout };
  }, [cells]);

  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No heatmap data</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 250 }} />;
}

/* ── Chart 5: Momentum ── */

function MomentumChart({ momentum }: { momentum: TransferMartResponse["momentum"] }) {
  const chart = useMemo(() => {
    if (!momentum || momentum.length === 0) return null;
    const items = momentum.slice(0, 12);
    const labels = items.map(m => m.model);
    const values = items.map(m => m.share_slope * 10000); // Scale for readability (basis points per period)
    const colors = items.map(m => m.trend === "rising" ? COLORS.winner : m.trend === "falling" ? COLORS.loser : COLORS.stable);

    const traces: Data[] = [{
      x: labels, y: values, type: "bar",
      marker: { color: colors },
      hovertemplate: "%{x}<br>Slope: %{y:.1f} bp/period<extra></extra>",
    }];
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { ...CHART_MARGIN, b: 100 },
      yaxis: { title: { text: "Share Momentum (bp/period)" } },
      showlegend: false,
    };
    return { traces, layout };
  }, [momentum]);

  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No momentum data</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 250 }} />;
}

/* ── Chart 6: Powertrain × Origin ── */

function PowertrainOriginChart({ data: items }: { data: TransferMartResponse["powertrain_origin_breakdown"] }) {
  const chart = useMemo(() => {
    if (!items || items.length === 0) return null;
    const top = items.filter(i => Math.abs(i.shift) > 1).sort((a, b) => b.shift - a.shift).slice(0, 20);
    const labels = top.map(i => `${i.origin}/${i.powertrain}`);
    const values = top.map(i => i.shift);

    const traces: Data[] = [{
      x: labels, y: values, type: "bar",
      marker: { color: values.map(v => v >= 0 ? COLORS.winner : COLORS.loser) },
      hovertemplate: "%{x}<br>Shift: %{y:+,.0f}<extra></extra>",
    }];
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { ...CHART_MARGIN, b: 120 },
      yaxis: { title: { text: "Δ Volume" } },
      showlegend: false,
    };
    return { traces, layout };
  }, [items]);

  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No breakdown data</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 280 }} />;
}

/* ── Chart 7a: Channel Volume (stacked bar, single axis) ── */

function ChannelVolumeChart({ ts }: { ts: TransferMartResponse["channel_timeseries"] }) {
  const chart = useMemo(() => {
    if (!ts || ts.length === 0) return null;
    const periods = [...new Set(ts.map(d => d.period))].sort();
    const channels = [...new Set(ts.map(d => d.channel!))].sort();
    const CH_COLORS: Record<string, string> = { Business: "#3b82f6", Private: "#10b981", Other: "#94a3b8" };
    const traces: Data[] = channels.map(ch => ({
      x: periods,
      y: periods.map(p => ts.find(d => d.period === p && d.channel === ch)?.volume || 0),
      type: "bar" as const, name: ch, marker: { color: CH_COLORS[ch] || "#94a3b8" },
      hovertemplate: `%{x}<br>${ch}: %{y:,.0f}<extra></extra>`,
    }));
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { ...CHART_MARGIN, b: 80 },
      barmode: "stack" as const, yaxis: { title: { text: "Volume" } },
      legend: { orientation: "h", y: 1.15 },
    };
    return { traces, layout };
  }, [ts]);
  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No channel timeseries data</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 280 }} />;
}

/* ── Chart 7b: Channel Share (indexed, base=100) ── */

function ChannelShareChart({ ts }: { ts: TransferMartResponse["channel_timeseries"] }) {
  const chart = useMemo(() => {
    if (!ts || ts.length === 0) return null;
    const periods = [...new Set(ts.map(d => d.period))].sort();
    const channels = [...new Set(ts.map(d => d.channel!))].sort();
    const CH_COLORS: Record<string, string> = { Business: "#3b82f6", Private: "#10b981", Other: "#94a3b8" };
    // Indexed: first period = 100
    const traces: Data[] = channels.map(ch => {
      const vals = periods.map(p => ts.find(d => d.period === p && d.channel === ch)?.volume || 0);
      const base = vals[0] || 1;
      const indexed = vals.map(v => (v / base) * 100);
      return {
        x: periods, y: indexed, type: "scatter", mode: "lines+markers", name: ch,
        marker: { color: CH_COLORS[ch] || "#94a3b8", size: 4 }, line: { width: 2 },
        hovertemplate: `%{x}<br>${ch}: %{y:.1f} (base=100)<extra></extra>`,
      };
    });
    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { ...CHART_MARGIN, b: 80 },
      yaxis: { title: { text: "Index (base=100)" } },
      legend: { orientation: "h", y: 1.15 },
    };
    return { traces, layout };
  }, [ts]);
  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No channel timeseries data</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 280 }} />;
}

/* ── Chart 8: Powertrain Stacked Bar ── */

function PowertrainStackedChart({ ts }: { ts: TransferMartResponse["powertrain_timeseries"] }) {
  const chart = useMemo(() => {
    if (!ts || ts.length === 0) return null;
    const periods = [...new Set(ts.map(d => d.period))].sort();
    const pts = [...new Set(ts.map(d => d.powertrain!))].sort();
    const PT_COLORS: Record<string, string> = { BEV: "#10b981", HEV: "#f59e0b", PHEV: "#3b82f6", ICE: "#94a3b8", MHEV: "#8b5cf6", OTHER: "#64748b" };

    const traces: Data[] = pts.map(pt => {
      const vals = periods.map(p => ts.find(d => d.period === p && d.powertrain === pt)?.volume || 0);
      return { x: periods, y: vals, type: "bar" as const, name: pt, marker: { color: PT_COLORS[pt] || "#94a3b8" }, hovertemplate: `%{x}<br>${pt}: %{y:,.0f}<extra></extra>` };
    });

    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { ...CHART_MARGIN, b: 80 },
      barmode: "stack" as const,
      yaxis: { title: { text: "Volume" } },
      legend: { orientation: "h", y: 1.15 },
    };
    return { traces, layout };
  }, [ts]);
  if (!chart) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No powertrain timeseries data</div>;
  return <PlotlyChart data={chart.traces} layout={chart.layout} style={{ width: "100%", height: 300 }} />;
}

/* ── Bottom: Transfer Ledger ── */

function TransferLedger({ models, basePeriod, targetPeriod, tsData }: { models: TransferMartModel[]; basePeriod: string; targetPeriod: string; tsData: TransferMartResponse["model_timeseries"] }) {
  const [sortKey, setSortKey] = useState<SortKey>("dV");
  const [sortDir, setSortDir] = useState<-1 | 1>(-1);
  const [expanded, setExpanded] = useState<string | null>(null);
  const tsMap = useMemo(() => {
    const m = new Map<string, number[]>();
    for (const item of tsData || []) m.set(item.model, item.shares.map(s => s.share));
    return m;
  }, [tsData]);

  const sorted = useMemo(() => {
    const arr = [...models];
    arr.sort((a, b) => {
      if (sortKey === "model") {
        return a.model.localeCompare(b.model) * sortDir;
      }
      return (a[sortKey] - b[sortKey]) * sortDir;
    });
    return arr;
  }, [models, sortKey, sortDir]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) setSortDir(d => (d * -1) as -1 | 1);
    else { setSortKey(key); setSortDir(-1); }
  };

  const cols: Array<{ key: SortKey; label: string }> = [
    { key: "model", label: "Model" }, { key: "dV", label: "Δ Vol" }, { key: "pure_share_shift", label: "Share Shift" },
    { key: "market_carryover", label: "Market" }, { key: "channel_mix", label: "Channel" },
    { key: "drive_mix", label: "Drive" }, { key: "powertrain_mix", label: "PWT" },
    { key: "interaction", label: "Int." },
  ];

  const decompOrder: Array<{ key: DecompositionKey; label: string; color: string }> = [
    { key: "market_carryover", label: "Market Carryover", color: COLORS.market },
    { key: "channel_mix", label: "Channel Mix", color: COLORS.mix },
    { key: "drive_mix", label: "Drive Mix", color: COLORS.mix },
    { key: "powertrain_mix", label: "Powertrain Mix", color: COLORS.mix },
    { key: "pure_share_shift", label: "Pure Share Shift", color: COLORS.share },
    { key: "interaction", label: "Interaction", color: COLORS.interaction },
  ];

  return (
    <div style={{ overflowX: "auto" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11 }}>
        <thead>
          <tr style={{ background: "#f8fafc", borderBottom: "2px solid #e2e8f0" }}>
            <th style={{ ...thStyle, width: 24 }}></th>
            {cols.map(c => (
              <th key={c.key} onClick={() => handleSort(c.key)}
                style={{ ...thStyle, cursor: "pointer", color: sortKey === c.key ? "#1e293b" : "#64748b" }}>
                {c.label}{sortKey === c.key ? (sortDir === -1 ? " desc" : " asc") : ""}
              </th>
            ))}
            <th style={{ ...thStyle, width: 80 }}>Trend</th>
            <th style={thStyle}>Donors</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((m, i) => {
            const isExp = expanded === m.model;
            const total = Math.abs(m.market_carryover) + Math.abs(m.channel_mix) + Math.abs(m.drive_mix) + Math.abs(m.powertrain_mix) + Math.abs(m.pure_share_shift) + Math.abs(m.interaction);
            return (
              <Fragment key={m.model}>
                <tr onClick={() => setExpanded(isExp ? null : m.model)}
                  style={{ borderBottom: "1px solid #f1f5f9", background: m.dV > 0 ? "#f0fdf4" : m.dV < 0 ? "#fef2f2" : "transparent", cursor: "pointer" }}>
                  <td style={{ ...tdStyle, textAlign: "center", color: "#94a3b8" }}>{isExp ? "v" : ">"}</td>
                  <td style={{ ...tdStyle, fontWeight: 600 }}>{m.model}</td>
                  <td style={{ ...tdStyle, textAlign: "right", color: m.dV >= 0 ? COLORS.winner : COLORS.loser }}>{m.dV > 0 ? "+" : ""}{fmtNum(m.dV)}</td>
                  <td style={{ ...tdStyle, textAlign: "right", color: m.pure_share_shift >= 0 ? COLORS.winner : COLORS.loser }}>{m.pure_share_shift > 0 ? "+" : ""}{fmtNum(m.pure_share_shift)}</td>
                  <td style={{ ...tdStyle, textAlign: "right" }}>{fmtNum(m.market_carryover)}</td>
                  <td style={{ ...tdStyle, textAlign: "right", color: m.channel_mix >= 0 ? COLORS.winner : COLORS.loser }}>{m.channel_mix > 0 ? "+" : ""}{fmtNum(m.channel_mix)}</td>
                  <td style={{ ...tdStyle, textAlign: "right" }}>{fmtNum(m.drive_mix)}</td>
                  <td style={{ ...tdStyle, textAlign: "right" }}>{fmtNum(m.powertrain_mix)}</td>
                  <td style={{ ...tdStyle, textAlign: "right" }}>{fmtNum(m.interaction)}</td>
                  <td style={{ ...tdStyle, width: 80 }}>
                    {(() => {
                      const vals = tsMap.get(m.model);
                      if (!vals || vals.length < 2) return <span style={{ color: "#cbd5e1" }}>-</span>;
                      const max = Math.max(...vals, 1);
                      const min = Math.min(...vals, 0);
                      const range = max - min || 1;
                      const points = vals.map((v, i) => `${(i / (vals.length - 1)) * 100},${100 - ((v - min) / range) * 100}`).join(" ");
                      const color = (vals[vals.length - 1] || 0) >= (vals[0] || 0) ? COLORS.winner : COLORS.loser;
                      return (
                        <svg width="72" height="20" style={{ display: "block" }}>
                          <polyline points={points} fill="none" stroke={color} strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
                        </svg>
                      );
                    })()}
                  </td>
                  <td style={{ ...tdStyle, fontSize: 10, color: "#94a3b8", maxWidth: 120, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                    {m.donors?.slice(0, 3).map(d => d.model).join(", ") || "-"}
                  </td>
                </tr>
                {isExp && (
                  <tr>
                    <td colSpan={12} style={{ padding: "8px 24px", background: "#fafafa" }}>
                      <div style={{ fontSize: 11, fontWeight: 600, marginBottom: 6 }}>
                        Why {m.model} {m.dV >= 0 ? "gained" : "lost"} {m.dV > 0 ? "+" : ""}{fmtNum(m.dV)}?
                      </div>
                      <div style={{ display: "flex", gap: 6, flexWrap: "wrap", alignItems: "center" }}>
                        {decompOrder.map(d => {
                          const val = m[d.key];
                          const pct = total > 0 ? Math.abs(val) / total * 100 : 0;
                          if (Math.abs(val) < 0.5) return null;
                          return (
                            <span key={d.key} style={{ display: "inline-flex", alignItems: "center", gap: 4, padding: "3px 8px", borderRadius: 4, background: d.color + "18", border: `1px solid ${d.color}40`, fontSize: 10 }}>
                              <span style={{ color: d.color, fontWeight: 600 }}>{d.label}</span>
                              <span style={{ color: val >= 0 ? COLORS.winner : COLORS.loser, fontWeight: 600 }}>{val > 0 ? "+" : ""}{fmtNum(val)}</span>
                              <span style={{ color: "#94a3b8" }}>({fmtNum(pct)}%)</span>
                              <div style={{ width: 60, height: 4, background: "#e2e8f0", borderRadius: 2, overflow: "hidden" }}>
                                <div style={{ width: `${Math.min(pct, 100)}%`, height: "100%", background: d.color, borderRadius: 2 }} />
                              </div>
                            </span>
                          );
                        })}
                      </div>
                      {m.donors && m.donors.length > 0 && (
                        <div style={{ marginTop: 6, fontSize: 10, color: "#64748b" }}>
                          Estimated share sources: {m.donors.map(d => `${d.model} (${fmtNum(d.estimated_flow)})`).join(", ")}
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </Fragment>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const thStyle: CSSProperties = { padding: "7px 10px", textAlign: "left", fontWeight: 600, fontSize: 11, textTransform: "uppercase", whiteSpace: "nowrap" };
const tdStyle: CSSProperties = { padding: "5px 10px", fontSize: 11, whiteSpace: "nowrap" };
