import { Fragment, useCallback, useEffect, useMemo, useState } from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { LazyPlotlyChart as PlotlyChart } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";
import { SERIES_COLORS } from "../utils/colors";
import { DEFAULT_EXPORT, ExportPanel, downloadPng, type ExportSettings } from "../components/ExportPanel";
import { DeckExportDrawer, DeckFloatingDrawer } from "../components/deckControls";
import type {
  AdvancedAnalysisSalesMode,
  AdvancedAnalysisTransferMartRequest,
  TransferMartModel,
  TransferMartResponse,
} from "../types/advancedAnalysis";

/* ── Constants ── */

const DEFAULT_COUNTRY = "瑞典";
const COUNTRY_OPTIONS = ["瑞典","挪威","丹麦","芬兰","德国","英国","法国","荷兰","比利时","意大利","西班牙"];
const CHART_MARGIN = { l: 52, r: 24, t: 20, b: 48 } as const;
const DEFAULT_AA_EXPORT: ExportSettings = { ...DEFAULT_EXPORT, exportWidth: 1920, exportHeight: 1080, dataLabelMode: "value", fontSize: 11 };
const COLORS = { growth: "#10b981", decline: "#ef4444", stable: "#94a3b8", market: "#3b82f6", share: "#10b981", mix: "#f59e0b", interaction: "#8b5cf6", winner: "#10b981", loser: "#ef4444" };

/* ── Helpers ── */

function pt(text: string): Partial<PlotlyLayout>["title"] { return { text }; }
function stateColor(s: string): string { return s === "growth" ? COLORS.growth : s === "decline" ? COLORS.decline : COLORS.stable; }
function getGraphDiv(): HTMLElement | null { return document.querySelector(".chart-card .js-plotly-plot") as HTMLElement | null; }
function fmtNum(n: number): string { return n.toLocaleString(undefined, { maximumFractionDigits: 0 }); }
function fmtPct(n: number): string { return `${(n * 100).toFixed(1)}%`; }

/* ── Page ── */

export function AdvancedAnalysisPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [country, setCountry] = useState(() => searchParams.get("country") || (() => { try { return sessionStorage.getItem("aa_country"); } catch { return null; } })() || DEFAULT_COUNTRY);
  const [period, setPeriod] = useState(() => searchParams.get("period") || "");
  const [timeMode, setTimeMode] = useState<AdvancedAnalysisSalesMode>("month");
  const [compareMode, setCompareMode] = useState(false);
  const [periodB, setPeriodB] = useState("");
  const TIME_MODES: Array<{ value: AdvancedAnalysisSalesMode; label: string }> = [
    { value: "month", label: "当月" },
    { value: "ytd", label: "YTD" },
    { value: "rolling12", label: "近12月" },
  ];
  const [powertrainFilter, setPowertrainFilter] = useState<string[]>([]);
  const [channelFilter, setChannelFilter] = useState("");
  const [driveFilter, setDriveFilter] = useState("");
  const [segmentFilter, setSegmentFilter] = useState(() => searchParams.get("seg") || "");
  const [availableSegments, setAvailableSegments] = useState<string[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [data, setData] = useState<TransferMartResponse | null>(null);
  const [dataB, setDataB] = useState<TransferMartResponse | null>(null);
  const [filterOpen, setFilterOpen] = useState(false);
  const [exportOpen, setExportOpen] = useState(false);
  const [exportSettings, setExportSettings] = useState<ExportSettings>(DEFAULT_AA_EXPORT);

  const PT_OPTIONS = ["BEV","HEV","PHEV","ICE","MHEV","REEV","FCV"];
  const CH_OPTIONS = ["","Business","Private"];
  const DR_OPTIONS = ["","4WD","2WD"];

  // URL sync
  useEffect(() => {
    const p = new URLSearchParams();
    if (country) p.set("country", country);
    if (period) p.set("period", period);
    if (segmentFilter) p.set("seg", segmentFilter);
    setSearchParams(p, { replace: true });
    try { sessionStorage.setItem("aa_country", country); } catch { /* ignore */ }
  }, [country, period, segmentFilter, setSearchParams]);

  // Load available segments when country changes
  useEffect(() => {
    api.get<{ segments: string[] }>(`/advanced-analysis/segments?country=${encodeURIComponent(country)}`)
      .then(r => setAvailableSegments(r.segments || []))
      .catch(() => setAvailableSegments([]));
  }, [country]);

  // Fetch data
  const buildScope = useCallback(() => {
    const s: Array<{ dim: string; value: string }> = [];
    if (segmentFilter) s.push({ dim: "segment", value: segmentFilter });
    if (channelFilter) s.push({ dim: "registration_type", value: channelFilter });
    if (driveFilter) s.push({ dim: "drive_type", value: driveFilter });
    for (const pt of powertrainFilter) s.push({ dim: "powertrain", value: pt });
    return s;
  }, [segmentFilter, channelFilter, driveFilter, powertrainFilter]);

  const fetchData = useCallback(async () => {
    setLoading(true); setError(null);
    try {
      const scope = buildScope();
      const payload: AdvancedAnalysisTransferMartRequest = { country, target_period: period || undefined, sales_mode: timeMode, scope_filters: scope, top_n: 25 };
      setData(await api.post<TransferMartResponse>("/advanced-analysis/transfer-mart", payload));
      if (compareMode && periodB) {
        const payloadB: AdvancedAnalysisTransferMartRequest = { country, target_period: periodB, sales_mode: timeMode, scope_filters: scope, top_n: 25 };
        setDataB(await api.post<TransferMartResponse>("/advanced-analysis/transfer-mart", payloadB));
      } else { setDataB(null); }
    } catch (e: unknown) { setError(e instanceof Error ? e.message : "Failed"); }
    finally { setLoading(false); }
  }, [country, period, timeMode, buildScope, compareMode, periodB]);
  useEffect(() => { fetchData(); }, [fetchData]);

  // Drawer mutual exclusion
  const hFO = useCallback((o: boolean) => { setFilterOpen(o); if (o) setExportOpen(false); }, []);
  const hEO = useCallback((o: boolean) => { setExportOpen(o); if (o) setFilterOpen(false); }, []);
  const handleExportPng = useCallback(() => { const d = getGraphDiv(); if (d) downloadPng(d, exportSettings); }, [exportSettings]);

  // ── Synthesized conclusion narrative ──
  const s = data?.scope_summary;
  const narrative = useMemo(() => {
    if (!s || !data) return null;
    const topW = data.winners?.[0];
    const topL = data.losers?.[0];
    // Find the dominant decomposition component for the top winner
    const wComp = topW ? (
      Math.abs(topW.market_carryover) > Math.abs(topW.pure_share_shift) && Math.abs(topW.market_carryover) > Math.abs(topW.channel_mix) ? "market growth" :
      Math.abs(topW.channel_mix) > Math.abs(topW.drive_mix) && Math.abs(topW.channel_mix) > Math.abs(topW.powertrain_mix) ? "channel mix shift" :
      Math.abs(topW.pure_share_shift) > Math.abs(topW.powertrain_mix) ? "competitive share gain" : "powertrain mix shift"
    ) : null;
    const lComp = topL ? (
      Math.abs(topL.market_carryover) > Math.abs(topL.pure_share_shift) ? "market contraction" :
      Math.abs(topL.channel_mix) < topL.channel_mix ? "channel mix headwind" : "competitive share loss"
    ) : null;
    const scopeDesc = [channelFilter, driveFilter, ...powertrainFilter, segmentFilter].filter(Boolean).join("+") || "total market";
    return {
      state: s.market_state,
      dM: s.dM,
      yoy: s.yoy_pct,
      topW: topW ? { model: topW.model, gain: topW.dV, driver: wComp } : null,
      topL: topL ? { model: topL.model, loss: topL.dV, driver: lComp } : null,
      scope: scopeDesc,
      modelCount: data.models.length,
    };
  }, [data, s, channelFilter, driveFilter, powertrainFilter, segmentFilter]);

  return (
    <div className="market-scan-page">
      {/* Hero */}
      <section className="header-card dashboard-hero market-scan-hero">
        <div className="dashboard-hero-head">
          <div className="dashboard-hero-copy market-scan-hero-copy">
            <span className="page-kicker">Advanced Analysis</span>
            <h1>Share Transfer &amp; Gain/Loss Attribution</h1>
            <p>One-page conclusion: where growth comes from, who gains share, who loses it, and why — decomposed into market carryover, channel/drive/powertrain mix effects, and pure competitive shift.</p>
            {s && (
              <div className="market-scan-hero-ribbon">
                <span className="market-scan-hero-chip" style={{ color: stateColor(s.market_state), fontWeight: 700 }}>
                  {s.market_state === "growth" ? "↑ Growth" : s.market_state === "decline" ? "↓ Decline" : "→ Stable"}
                </span>
                <span className="market-scan-hero-chip">ΔM {s.dM > 0 ? "+" : ""}{fmtNum(s.dM)}</span>
                <span className="market-scan-hero-chip">YoY {fmtPct(s.yoy_pct)}</span>
                <span className="market-scan-hero-chip">{country}</span>
                <span className="market-scan-hero-chip">{period || "Latest"}</span>
                {channelFilter && <span className="market-scan-hero-chip" style={{ background: "#dbeafe" }}>Ch: {channelFilter}</span>}
                {driveFilter && <span className="market-scan-hero-chip" style={{ background: "#dbeafe" }}>Dr: {driveFilter}</span>}
                {powertrainFilter.length > 0 && <span className="market-scan-hero-chip" style={{ background: "#dbeafe" }}>PT: {powertrainFilter.join("+")}</span>}
                {segmentFilter && <span className="market-scan-hero-chip" style={{ background: "#fef3c7" }}>Seg: {segmentFilter}</span>}
              </div>
            )}
          </div>
        </div>
      </section>

      {/* Synthesized narrative */}
      {narrative && (
        <div style={{ padding: "12px 16px", background: "linear-gradient(90deg, #f0fdf4, #f8fafc, #fef2f2)", borderBottom: "1px solid #e2e8f0", fontSize: 13, lineHeight: 1.6 }}>
          <strong style={{ color: stateColor(narrative.state) }}>
            {narrative.state === "growth" ? "📈 Growth market" : narrative.state === "decline" ? "📉 Declining market" : "📊 Stable market"}
          </strong>
          {" — "}In <strong>{narrative.scope}</strong>, total volume changed by <strong>{narrative.dM > 0 ? "+" : ""}{fmtNum(narrative.dM)}</strong> units (YoY {fmtPct(narrative.yoy)}).
          {narrative.topW && <> <strong style={{ color: COLORS.winner }}>{narrative.topW.model}</strong> gained <strong>+{fmtNum(narrative.topW.gain)}</strong>, driven primarily by <strong>{narrative.topW.driver}</strong>.</>}
          {narrative.topL && <> <strong style={{ color: COLORS.loser }}>{narrative.topL.model}</strong> lost <strong>{fmtNum(narrative.topL.loss)}</strong>, hurt by <strong>{narrative.topL.driver}</strong>.</>}
          {" "}<span style={{ color: "#94a3b8" }}>({narrative.modelCount} models analyzed)</span>
        </div>
      )}

      {/* Floating drawers */}
      <DeckFloatingDrawer open={filterOpen} onOpenChange={hFO} triggerPrimary="分析筛选" triggerSecondaryOpen="收起" triggerSecondaryClosed="打开"
        eyebrow="Controls" title="筛选条件" ariaLabel="Filters"
        footer={<div className="market-scan-toolbar-meta"><span className="market-scan-toolbar-chip">{country}</span><span className="market-scan-toolbar-chip">{period || "Latest"}</span>{segmentFilter && <span className="market-scan-toolbar-chip">{segmentFilter}</span>}</div>}
      >
        <div className="deck-panel-grid">
          <label className="market-scan-field"><span>Country</span>
            <select value={country} onChange={e => setCountry(e.target.value)}>{COUNTRY_OPTIONS.map(c => <option key={c} value={c}>{c}</option>)}</select>
          </label>
          <label className="market-scan-field"><span>Period</span>
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
          <label className="market-scan-field"><span>Channel</span>
            <select value={channelFilter} onChange={e => setChannelFilter(e.target.value)}>
              <option value="">All Channels</option>
              {CH_OPTIONS.filter(c => c).map(c => <option key={c} value={c}>{c}</option>)}
            </select>
          </label>
          <label className="market-scan-field"><span>Drive</span>
            <select value={driveFilter} onChange={e => setDriveFilter(e.target.value)}>
              <option value="">All Drive Types</option>
              {DR_OPTIONS.filter(d => d).map(d => <option key={d} value={d}>{d}</option>)}
            </select>
          </label>
          <label className="market-scan-field deck-panel-grid__wide"><span>Powertrain</span>
            <div style={{ display: "flex", gap: 4, flexWrap: "wrap" }}>
              {PT_OPTIONS.map(pt => (
                <label key={pt} style={{ fontSize: 11, display: "flex", alignItems: "center", gap: 3, cursor: "pointer", padding: "2px 6px", borderRadius: 4, background: powertrainFilter.includes(pt) ? "#dbeafe" : "#f1f5f9" }}>
                  <input type="checkbox" checked={powertrainFilter.includes(pt)} onChange={() => setPowertrainFilter(prev => prev.includes(pt) ? prev.filter(p => p !== pt) : [...prev, pt])} style={{ margin: 0 }} />
                  {pt}
                </label>
              ))}
            </div>
          </label>
          <label className="market-scan-field deck-panel-grid__wide"><span>Segment</span>
            <select value={segmentFilter} onChange={e => setSegmentFilter(e.target.value)}
              style={{ minHeight: 40, width: "100%", padding: "10px 12px", border: "1px solid rgba(15,23,42,0.12)", borderRadius: 6, fontSize: 13 }}>
              <option value="">All Segments (total market)</option>
              {availableSegments.map(s => <option key={s} value={s}>{s}</option>)}
            </select>
          </label>
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
              <button type="button" className="btn btn-secondary btn-sm" onClick={fetchData}>Refresh</button>
              <button type="button" className="btn btn-ghost btn-sm" onClick={() => { setSegmentFilter(""); setChannelFilter(""); setDriveFilter(""); setPowertrainFilter([]); setPeriodB(""); setCompareMode(false); setError(null); }}>Reset All</button>
            </div>
          </div>
        </div>
      </DeckFloatingDrawer>

      <DeckExportDrawer open={exportOpen} onOpenChange={hEO} triggerPrimary="导出图表 PNG" triggerSecondaryOpen="收起" triggerSecondaryClosed="展开"
        eyebrow="Export" title="导出与图表样式" ariaLabel="Export"
        footer={<div className="market-scan-toolbar-meta"><span className="market-scan-toolbar-chip">{exportSettings.exportWidth}×{exportSettings.exportHeight}</span></div>}
      >
        <button type="button" className="btn btn-primary btn-sm" onClick={handleExportPng} style={{ marginBottom: 12, width: "100%" }}>Export Current Chart PNG</button>
        <ExportPanel value={exportSettings} onChange={setExportSettings} showExportButton={false} collapsible={false} />
      </DeckExportDrawer>

      {error && <div style={{ padding: 16, color: "#b91c1c", background: "#fef2f2", borderRadius: 8, margin: "0 16px" }}>{error}</div>}
      {loading && <LoadingSurface mode="overlay" label="Analyzing..." />}

      {data && !data.error && (
        <div style={{ padding: "12px 16px 24px" }}>
          {/* Row 1: Market Waterfall + Winner/Loser Butterfly */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
            <ChartCard title="Market Decomposition Waterfall" subtitle={`${data.base_period} → ${data.target_period}`}>
              <MarketWaterfallChart data={data} />
            </ChartCard>
            <ChartCard title="Winner / Loser Butterfly" subtitle="By pure share shift">
              <ButterflyChart winners={data.winners} losers={data.losers} />
            </ChartCard>
          </div>

          {/* Row 2: Channel Volume + Indexed Share (side-by-side, no dual axis) */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
            <ChartCard title="Channel Volume (Stacked)" subtitle="Business / Private absolute volume">
              <ChannelVolumeChart ts={data.channel_timeseries} />
            </ChartCard>
            <ChartCard title="Channel Share (Indexed, base=100)" subtitle="Relative share change — avoids dual-axis distortion">
              <ChannelShareChart ts={data.channel_timeseries} />
            </ChartCard>
          </div>

          {/* Row 3: Transfer Ledger — page center, answers "who exactly" */}
          <div style={{ marginBottom: 16 }}>
            <ChartCard title="Transfer Ledger" subtitle={`${data.models.length} models ranked by |ΔV| — click row to see decomposition`}>
              <TransferLedger models={data.models} basePeriod={data.base_period} targetPeriod={data.target_period} tsData={data.model_timeseries} />
            </ChartCard>
          </div>

          {/* Row 4: Powertrain + Sankey */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
            <ChartCard title="Powertrain Cumulative" subtitle="BEV / HEV / PHEV / ICE stacked volume">
              <PowertrainStackedChart ts={data.powertrain_timeseries} />
            </ChartCard>
            <ChartCard title="Model Transfer Sankey" subtitle={`Estimated share flows (top donors → recipients)`} note="Estimated transfer, not observed switching">
              <SankeyChart winners={data.winners} losers={data.losers} />
            </ChartCard>
          </div>

          {/* Row 5: Heatmap + Momentum */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 16, marginBottom: 16 }}>
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

          {/* Compare mode: Period A vs Period B delta */}
          {compareMode && dataB && !dataB.error && (
            <div style={{ marginBottom: 16, padding: 12, background: "#f8fafc", borderRadius: 8, border: "1px solid #e2e8f0" }}>
              <h4 style={{ margin: "0 0 8px", fontSize: 13 }}>
                Δ Comparison: {data.target_period} → {dataB.target_period}
              </h4>
              <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr", gap: 12, fontSize: 12 }}>
                <div><strong>Market ΔM:</strong> <span style={{ color: (dataB.scope_summary.dM - data.scope_summary.dM) >= 0 ? COLORS.winner : COLORS.loser }}>{(dataB.scope_summary.dM - data.scope_summary.dM) > 0 ? "+" : ""}{fmtNum(dataB.scope_summary.dM - data.scope_summary.dM)}</span></div>
                <div><strong>Winners in B:</strong> {dataB.winners.slice(0, 3).map(w => w.model).join(", ") || "—"}</div>
                <div><strong>Losers in B:</strong> {dataB.losers.slice(0, 3).map(l => l.model).join(", ") || "—"}</div>
              </div>
            </div>
          )}

        </div>
      )}

      {!loading && !error && !data && (
        <div style={{ padding: 60, textAlign: "center", color: "#64748b" }}>
          <div style={{ fontSize: 48, marginBottom: 12 }}>📊</div>
          <h3>Share Transfer Analysis</h3>
          <p>Open 分析筛选 → select country + period → click Refresh</p>
        </div>
      )}
    </div>
  );
}

/* ── ChartCard wrapper ── */

function ChartCard({ title, subtitle, note, children }: { title: string; subtitle?: string; note?: string; children: React.ReactNode }) {
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
    const topW = winners.slice(0, 10);
    const topL = losers.slice(0, 10).reverse(); // most negative last → display bottom-to-top

    // Winners (right side, positive)
    const wTraces: Data[] = [{
      y: topW.map(m => m.model), x: topW.map(m => m.pure_share_shift), type: "bar", orientation: "h",
      name: "Winners", marker: { color: COLORS.winner },
      xaxis: "x", hovertemplate: "%{y}: +%{x:,.0f}<extra></extra>",
    }];
    // Losers (left side, negative)
    const lTraces: Data[] = [{
      y: topL.map(m => m.model), x: topL.map(m => m.pure_share_shift), type: "bar", orientation: "h",
      name: "Losers", marker: { color: COLORS.loser },
      xaxis: "x2", hovertemplate: "%{y}: %{x:,.0f}<extra></extra>",
    }];

    const layout: Partial<PlotlyLayout> = {
      ...CHART_LAYOUT, margin: { l: 100, r: 24, t: 20, b: 32 },
      grid: { rows: 1, columns: 2, roworder: "top to bottom" },
      xaxis: { title: { text: "Share Shift" }, domain: [0, 0.45], autorange: "reversed" },
      xaxis2: { title: { text: "Share Shift" }, domain: [0.55, 1] },
      yaxis: { autorange: "reversed" },
      yaxis2: { anchor: "x2", autorange: "reversed" },
      showlegend: false,
    };
    return { traces: [...lTraces, ...wTraces], layout };
  }, [winners, losers]);

  if (winners.length === 0 && losers.length === 0) return <div style={{ padding: 20, textAlign: "center", color: "#94a3b8" }}>No data</div>;

  // Fallback: simple grouped bar if subplots are tricky
  const merged = [
    ...losers.slice(0, 10).map(m => ({ model: m.model, val: m.pure_share_shift, kind: "Loser" })),
    ...winners.slice(0, 10).map(m => ({ model: m.model, val: m.pure_share_shift, kind: "Winner" })),
  ].sort((a, b) => b.val - a.val);

  const sChart = useMemo(() => {
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

  return <PlotlyChart data={sChart.traces} layout={sChart.layout} style={{ width: "100%", height: 300 }} />;
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

    for (const l of topL) { nodeMap[l.model] = nodes.length; nodes.push({ label: l.model + " ↘", color: COLORS.loser }); }
    for (const w of topW) { nodeMap[w.model] = nodes.length; nodes.push({ label: "↗ " + w.model, color: COLORS.winner }); }

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
      text: z.map(row => row.map(v => `${v > 0 ? "+" : ""}${fmtNum(v)}`)),
    }] as unknown as Data[];
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
  const [sortKey, setSortKey] = useState<string>("dV");
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
      const av = (a as unknown as Record<string, number>)[sortKey] || 0;
      const bv = (b as unknown as Record<string, number>)[sortKey] || 0;
      return (av - bv) * sortDir;
    });
    return arr;
  }, [models, sortKey, sortDir]);

  const handleSort = (key: string) => {
    if (sortKey === key) setSortDir(d => (d * -1) as -1 | 1);
    else { setSortKey(key); setSortDir(-1); }
  };

  const cols: Array<{ key: string; label: string }> = [
    { key: "model", label: "Model" }, { key: "dV", label: "Δ Vol" }, { key: "pure_share_shift", label: "Share Shift" },
    { key: "market_carryover", label: "Market" }, { key: "channel_mix", label: "Channel" },
    { key: "drive_mix", label: "Drive" }, { key: "powertrain_mix", label: "PWT" },
    { key: "interaction", label: "Int." },
  ];

  const decompOrder: Array<{ key: string; label: string; color: string }> = [
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
                {c.label}{sortKey === c.key ? (sortDir === -1 ? " ↓" : " ↑") : ""}
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
                  <td style={{ ...tdStyle, textAlign: "center", color: "#94a3b8" }}>{isExp ? "▼" : "▶"}</td>
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
                      if (!vals || vals.length < 2) return <span style={{ color: "#cbd5e1" }}>—</span>;
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
                          const val = (m as any)[d.key] || 0;
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

const thStyle: React.CSSProperties = { padding: "7px 10px", textAlign: "left", fontWeight: 600, fontSize: 11, textTransform: "uppercase", whiteSpace: "nowrap" };
const tdStyle: React.CSSProperties = { padding: "5px 10px", fontSize: 11, whiteSpace: "nowrap" };
