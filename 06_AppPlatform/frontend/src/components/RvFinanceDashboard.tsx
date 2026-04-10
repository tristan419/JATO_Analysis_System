import { useCallback, useMemo, useRef, useState } from "react";
import type { Data } from "plotly.js";

import { api } from "../api/client";
import type {
  RvContourMatrix,
  RvFinanceResult,
  RvFinanceVehicle,
  RvSensitivityPoint,
  RvSensitivitySummaryRow,
  RvWaterfallStep,
} from "../types";
import { LazyPlotlyChart as PlotlyChart } from "./LazyPlotlyChart";
import { LoadingActionButton } from "./LoadingActionButton";
import {
  ExportPanel,
  DEFAULT_EXPORT,
  applyDataLabelsToTraces,
  applyExportToLayout,
  applySeriesColors,
  buildExportLabelModeOptions,
  getExportPalette,
  type ExportSettings,
} from "./ExportPanel";

const CURRENCIES = ["EUR", "SEK", "NOK", "DKK", "GBP", "USD"];
const DEFAULT_RATES: Record<string, number> = {
  EUR: 1.0,
  SEK: 11.5,
  NOK: 11.3,
  DKK: 7.46,
  GBP: 0.86,
  USD: 1.09,
};
const FALLBACK_PRESETS: Record<string, Partial<RvFinanceVehicle>> = {
  "瑞典": { down_pct: 20, rv_pct: 45, apr_pct: 3.5, term: 36 },
  "挪威": { down_pct: 15, rv_pct: 50, apr_pct: 3.0, term: 36 },
  "德国": { down_pct: 20, rv_pct: 48, apr_pct: 3.9, term: 36 },
  "法国": { down_pct: 20, rv_pct: 40, apr_pct: 4.0, term: 48 },
  "英国": { down_pct: 10, rv_pct: 42, apr_pct: 5.9, term: 48 },
  "荷兰": { down_pct: 15, rv_pct: 45, apr_pct: 3.5, term: 36 },
};
const EMPTY_VEHICLE: RvFinanceVehicle = {
  vehicle: "",
  msrp: 0,
  down_pct: 10,
  rv_pct: 45,
  apr_pct: 3.5,
  term: 36,
};

function buildDefaultVehicles(): RvFinanceVehicle[] {
  return [
    { ...EMPTY_VEHICLE, vehicle: "方案 A", msrp: 35000 },
    { ...EMPTY_VEHICLE, vehicle: "方案 B", msrp: 42000 },
    { ...EMPTY_VEHICLE, vehicle: "方案 C", msrp: 50000, down_pct: 15, rv_pct: 50, term: 48 },
  ];
}

function clampPreset(preset: Partial<RvFinanceVehicle>): Partial<RvFinanceVehicle> {
  return {
    down_pct: Math.min(Math.max(Number(preset.down_pct ?? 10), 0), 50),
    rv_pct: Math.min(Math.max(Number(preset.rv_pct ?? 45), 30), 70),
    apr_pct: Math.min(Math.max(Number(preset.apr_pct ?? 3.5), 0), 10),
    term: Math.min(Math.max(Number(preset.term ?? 36), 12), 84),
  };
}

export function RvFinanceDashboard() {
  const [vehicles, setVehicles] = useState<RvFinanceVehicle[]>(buildDefaultVehicles());
  const [currency, setCurrency] = useState("EUR");
  const [fxMode, setFxMode] = useState<"preset" | "manual">("preset");
  const [manualRate, setManualRate] = useState(DEFAULT_RATES.EUR);
  const [selectedPreset, setSelectedPreset] = useState(Object.keys(FALLBACK_PRESETS)[0] ?? "瑞典");
  const [sensitivityIdx, setSensitivityIdx] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const [results, setResults] = useState<RvFinanceResult[]>([]);
  const [waterfall, setWaterfall] = useState<RvWaterfallStep[]>([]);
  const [sensitivity, setSensitivity] = useState<RvSensitivityPoint[]>([]);
  const [contour, setContour] = useState<RvContourMatrix>({ apr_values: [], rv_values: [], z: [] });
  const [presetOptions, setPresetOptions] = useState<Record<string, Partial<RvFinanceVehicle>>>(FALLBACK_PRESETS);
  const [effectiveRate, setEffectiveRate] = useState(DEFAULT_RATES.EUR);

  const [exportSettings, setExportSettings] = useState<ExportSettings>({ ...DEFAULT_EXPORT });
  const chartRef = useRef<HTMLDivElement | null>(null);
  const palette = getExportPalette(exportSettings.colorScheme);
  const rvLabelModeOptions = useMemo(() => buildExportLabelModeOptions({ showValue: true, showSeries: true }), []);
  const safeSensitivityIdx = Math.max(0, Math.min(sensitivityIdx, Math.max(vehicles.length - 1, 0)));
  const activePreset = clampPreset(presetOptions[selectedPreset] ?? FALLBACK_PRESETS[selectedPreset] ?? FALLBACK_PRESETS[Object.keys(FALLBACK_PRESETS)[0] ?? "瑞典"] ?? EMPTY_VEHICLE);

  const updateVehicle = useCallback(
    (idx: number, field: keyof RvFinanceVehicle, value: string | number) => {
      setVehicles((prev) => {
        const next = [...prev];
        next[idx] = { ...next[idx], [field]: value };
        return next;
      });
    },
    [],
  );

  const addVehicle = useCallback(() => {
    setVehicles((prev) => [
      ...prev,
      { ...EMPTY_VEHICLE, vehicle: `方案 ${String.fromCharCode(65 + prev.length)}`, ...activePreset },
    ]);
  }, [activePreset]);

  const removeVehicle = useCallback((idx: number) => {
    setVehicles((prev) => prev.filter((_, i) => i !== idx));
    setSensitivityIdx((prev) => Math.max(0, Math.min(prev, Math.max(vehicles.length - 2, 0))));
  }, [vehicles.length]);

  const applyPresetToAll = useCallback(() => {
    setVehicles((prev) => prev.map((vehicle) => ({ ...vehicle, ...activePreset })));
  }, [activePreset]);

  const resetVehicles = useCallback(() => {
    setVehicles(buildDefaultVehicles());
    setResults([]);
    setWaterfall([]);
    setSensitivity([]);
    setContour({ apr_values: [], rv_values: [], z: [] });
    setCurrency("EUR");
    setFxMode("preset");
    setManualRate(DEFAULT_RATES.EUR);
    setEffectiveRate(DEFAULT_RATES.EUR);
    setSensitivityIdx(0);
  }, []);

  const calculate = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const response = await api.rvFinance({
        vehicles,
        currency,
        fx_rate: fxMode === "manual" ? manualRate : undefined,
        sensitivity_vehicle_idx: safeSensitivityIdx,
      });
      setResults(response.results);
      setWaterfall(response.waterfall);
      setSensitivity(response.sensitivity);
      setContour(response.contour);
      setEffectiveRate(response.rate ?? (fxMode === "manual" ? manualRate : DEFAULT_RATES[currency] ?? 1));
      if (response.presets && Object.keys(response.presets).length > 0) {
        setPresetOptions(response.presets);
        if (!response.presets[selectedPreset]) {
          setSelectedPreset(Object.keys(response.presets)[0]);
        }
      }
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  }, [vehicles, currency, fxMode, manualRate, safeSensitivityIdx, selectedPreset]);

  const baseMonthly = results[safeSensitivityIdx]?.monthly ?? 0;
  const summaryKpis = useMemo(() => {
    if (results.length === 0) return null;
    const monthlies = results.map((row) => row.monthly);
    return {
      count: results.length,
      avgMonthly: monthlies.reduce((sum, value) => sum + value, 0) / monthlies.length,
      maxMonthly: Math.max(...monthlies),
      minMonthly: Math.min(...monthlies),
    };
  }, [results]);

  const sensitivitySummary = useMemo<RvSensitivitySummaryRow[]>(() => {
    if (sensitivity.length === 0) return [];
    const map = new Map<string, RvSensitivitySummaryRow>();
    for (const point of sensitivity) {
      const existing = map.get(point.param) ?? {
        param: point.param,
        low: baseMonthly,
        base: baseMonthly,
        high: baseMonthly,
      };
      if (point.scenario === "low") existing.low = point.monthly;
      if (point.scenario === "high") existing.high = point.monthly;
      map.set(point.param, existing);
    }
    return Array.from(map.values()).sort((a, b) => {
      const ad = Math.max(Math.abs(a.low - a.base), Math.abs(a.high - a.base));
      const bd = Math.max(Math.abs(b.low - b.base), Math.abs(b.high - b.base));
      return bd - ad;
    });
  }, [baseMonthly, sensitivity]);

  const waterfallTrace: Data[] = waterfall.length > 0
    ? [{
        type: "waterfall",
        orientation: "v",
        x: waterfall.map((step) => step.label),
        y: waterfall.map((step) => step.value),
        measure: waterfall.map((step) => step.type === "relative" ? "relative" : "total"),
        connector: { line: { color: "rgb(99,110,250)" } },
        decreasing: { marker: { color: "#ef4444" } },
        increasing: { marker: { color: "#16a34a" } },
        totals: { marker: { color: "#2563eb" } },
        textposition: "outside",
        text: waterfall.map((step) => step.value.toLocaleString(undefined, { maximumFractionDigits: 0 })),
      } as Data]
    : [];

  const comparisonTraces: Data[] = results.length > 0
    ? [
        {
          type: "bar",
          x: results.map((row) => row.vehicle),
          y: results.map((row) => row.monthly),
          name: "月供",
          marker: { color: palette[0] },
          text: results.map((row) => row.monthly.toFixed(0)),
          textposition: "auto",
        } as Data,
        {
          type: "bar",
          x: results.map((row) => row.vehicle),
          y: results.map((row) => row.total_payments),
          name: "总支付",
          marker: { color: palette[1] },
          yaxis: "y2",
        } as Data,
      ]
    : [];

  const tornadoTraces: Data[] = sensitivitySummary.length > 0
    ? [
        {
          type: "bar",
          orientation: "h",
          y: sensitivitySummary.map((row) => row.param),
          x: sensitivitySummary.map((row) => row.low - row.base),
          name: "低值",
          marker: { color: "#3b82f6" },
          hovertemplate: "%{y}<br>变化: %{x:,.0f}<extra>低值</extra>",
        } as Data,
        {
          type: "bar",
          orientation: "h",
          y: sensitivitySummary.map((row) => row.param),
          x: sensitivitySummary.map((row) => row.high - row.base),
          name: "高值",
          marker: { color: "#ef4444" },
          hovertemplate: "%{y}<br>变化: %{x:,.0f}<extra>高值</extra>",
        } as Data,
      ]
    : [];

  const contourTrace: Data[] = contour.apr_values.length > 0 && contour.rv_values.length > 0 && contour.z.length > 0
    ? [{
        type: "contour",
        x: contour.apr_values,
        y: contour.rv_values,
        z: contour.z,
        colorscale: "Viridis",
        contours: { coloring: "heatmap" },
        colorbar: { title: { text: "月供" } },
        hovertemplate: "APR%: %{x:.1f}<br>RV%: %{y:.0f}<br>月供: %{z:,.0f}<extra></extra>",
      } as Data]
    : [];

  const exportSeriesNames = useMemo(() => {
    const names = new Set<string>();
    [...comparisonTraces, ...tornadoTraces, ...waterfallTrace].forEach((trace) => {
      if (typeof trace.name === "string" && trace.name.trim()) names.add(trace.name);
    });
    return Array.from(names);
  }, [comparisonTraces, tornadoTraces, waterfallTrace]);

  const applyRvExport = useCallback((traces: Data[]) => {
    return applyDataLabelsToTraces(applySeriesColors(traces, exportSettings.seriesColors), exportSettings);
  }, [exportSettings]);

  return (
    <div className="rv-finance analysis-deck-card">
      <div className="analysis-deck-head rv-finance-head">
        <div className="analysis-deck-copy">
          <span className="panel-kicker">04 / Powertrain Cost</span>
          <h3>RV Finance Leverage Deck</h3>
          <p>用统一的 control deck 组织模板、汇率、方案矩阵、敏感性和等高线，让金融测算区也保持与主分析一致的操作节奏。</p>
          <div className="analysis-chip-row">
            <span className="analysis-chip">{`1 EUR = ${effectiveRate.toFixed(4)} ${currency}`}</span>
            <span className="analysis-chip">{fxMode === "manual" ? "Manual FX" : "Preset FX"}</span>
            <span className="analysis-chip">MSRP in EUR</span>
          </div>
        </div>
        <div className="analysis-deck-meta">
          <div className={`analysis-deck-stat${loading ? " is-loading" : ""}`}>
            <span className="analysis-deck-stat-label">Calc state</span>
            <strong className="analysis-deck-stat-value">{loading ? "SYNC" : results.length ? "READY" : "IDLE"}</strong>
            <span className="analysis-deck-stat-subvalue">{results.length ? `${results.length} 个方案已计算` : "等待执行计算"}</span>
          </div>
          <div className="analysis-deck-stat">
            <span className="analysis-deck-stat-label">Vehicles</span>
            <strong className="analysis-deck-stat-value">{String(vehicles.length).padStart(2, "0")}</strong>
            <span className="analysis-deck-stat-subvalue">{`Sensitivity #${safeSensitivityIdx + 1}`}</span>
          </div>
        </div>
      </div>

      {error && <div className="alert alert-error">{error}</div>}

      <div className="analysis-inline-note">
        {`汇率口径：1 EUR = ${effectiveRate.toFixed(4)} ${currency} · ${fxMode === "manual" ? "手动输入" : "预设汇率"} · MSRP 输入始终按 EUR`}
      </div>

      <div className="adv-controls adv-controls-panel rv-finance-actions">
        <div className="filter-group adv-control-unit">
          <label>{"参数模板"}</label>
          <select value={selectedPreset} onChange={(e) => setSelectedPreset(e.target.value)}>
            {Object.keys(presetOptions).map((name) => (
              <option key={name} value={name}>{name}</option>
            ))}
          </select>
        </div>
        <button className="btn btn-sm btn-secondary" onClick={applyPresetToAll}>{"应用模板到全部方案"}</button>
        <button className="btn btn-sm btn-secondary" onClick={resetVehicles}>{"重置参数"}</button>
      </div>

      <div className="adv-controls adv-controls-panel adv-controls-panel-secondary rv-finance-actions">
        <div className="filter-group adv-control-unit">
          <label>{"展示币种"}</label>
          <select
            value={currency}
            onChange={(e) => {
              const nextCurrency = e.target.value;
              setCurrency(nextCurrency);
              setManualRate(DEFAULT_RATES[nextCurrency] ?? 1);
              if (fxMode === "preset") setEffectiveRate(DEFAULT_RATES[nextCurrency] ?? 1);
            }}
          >
            {CURRENCIES.map((code) => (
              <option key={code} value={code}>{code}</option>
            ))}
          </select>
        </div>
        <div className="filter-group adv-control-unit">
          <label>{"汇率来源"}</label>
          <select value={fxMode} onChange={(e) => setFxMode(e.target.value as "preset" | "manual") }>
            <option value="preset">{"预设汇率"}</option>
            <option value="manual">{"手动输入"}</option>
          </select>
        </div>
        {fxMode === "manual" && (
          <div className="filter-group adv-control-unit">
            <label>{`手动汇率（1 EUR = ? ${currency}）`}</label>
            <input
              type="number"
              min={0.0001}
              step={0.0001}
              value={manualRate}
              style={{width:120}}
              onChange={(e) => setManualRate(Number(e.target.value) || 1)}
            />
          </div>
        )}
        <div className="filter-group adv-control-unit">
          <label>{"敏感性分析对象"}</label>
          <select value={safeSensitivityIdx} onChange={(e) => setSensitivityIdx(Number(e.target.value))}>
            {vehicles.map((vehicle, idx) => (
              <option key={vehicle.vehicle || idx} value={idx}>{vehicle.vehicle || `方案 ${idx + 1}`}</option>
            ))}
          </select>
        </div>
        <button className="btn btn-sm btn-secondary" onClick={addVehicle}>{"+ 添加方案"}</button>
        <LoadingActionButton loading={loading} loadingLabel="计算中…" disabled={vehicles.length === 0} onClick={calculate}>
          计算
        </LoadingActionButton>
      </div>

      <div className="rv-finance-table-wrap analysis-table-wrap">
        <table className="data-table rv-finance-table">
          <thead>
            <tr>
              <th>{"方案"}</th>
              <th>{"MSRP (EUR)"}</th>
              <th>{"首付%"}</th>
              <th>{"RV%"}</th>
              <th>{"APR%"}</th>
              <th>{"期数(月)"}</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {vehicles.map((vehicle, idx) => (
              <tr key={`${vehicle.vehicle}-${idx}`}>
                <td>
                  <input type="text" value={vehicle.vehicle} style={{width:90}} onChange={(e) => updateVehicle(idx, "vehicle", e.target.value)} />
                </td>
                <td>
                  <input type="number" value={vehicle.msrp} min={0} step={1000} style={{width:100}} onChange={(e) => updateVehicle(idx, "msrp", Number(e.target.value))} />
                </td>
                <td>
                  <input type="number" value={vehicle.down_pct} min={0} max={50} step={1} style={{width:64}} onChange={(e) => updateVehicle(idx, "down_pct", Number(e.target.value))} />
                </td>
                <td>
                  <input type="number" value={vehicle.rv_pct} min={30} max={70} step={1} style={{width:64}} onChange={(e) => updateVehicle(idx, "rv_pct", Number(e.target.value))} />
                </td>
                <td>
                  <input type="number" value={vehicle.apr_pct} min={0} max={10} step={0.1} style={{width:72}} onChange={(e) => updateVehicle(idx, "apr_pct", Number(e.target.value))} />
                </td>
                <td>
                  <input type="number" value={vehicle.term} min={12} max={84} step={12} style={{width:64}} onChange={(e) => updateVehicle(idx, "term", Number(e.target.value))} />
                </td>
                <td>
                  {vehicles.length > 1 && (
                    <button className="btn btn-sm btn-danger" onClick={() => removeVehicle(idx)}>{"×"}</button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {summaryKpis && (
        <div className="kpi-grid rv-finance-kpi-grid">
          <div className="kpi-card kpi-primary">
            <div className="kpi-label">{"方案数"}</div>
            <div className="kpi-value">{summaryKpis.count}</div>
            <div className="kpi-sub">{`敏感性对象：${results[safeSensitivityIdx]?.vehicle ?? "-"}`}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">{"月供均值"}</div>
            <div className="kpi-value">{summaryKpis.avgMonthly.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
            <div className="kpi-sub">{`${currency}/月`}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">{"月供最高"}</div>
            <div className="kpi-value">{summaryKpis.maxMonthly.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
            <div className="kpi-sub">{`${currency}/月`}</div>
          </div>
          <div className="kpi-card">
            <div className="kpi-label">{"月供最低"}</div>
            <div className="kpi-value">{summaryKpis.minMonthly.toLocaleString(undefined, { maximumFractionDigits: 0 })}</div>
            <div className="kpi-sub">{`${currency}/月`}</div>
          </div>
        </div>
      )}

      {results.length > 0 && (
        <div className="rv-finance-results analysis-subsection">
          <div className="analysis-subsection-head">
            <div>
              <div className="analysis-subsection-title">计算结果</div>
              <p className="section-note">以当前汇率与模板设定输出 Down、Balloon、净贷款和月供对比。</p>
            </div>
            <div className="analysis-chip-row analysis-chip-row--compact">
              <span className="analysis-chip">{`Baseline ${results[safeSensitivityIdx]?.vehicle ?? "-"}`}</span>
              <span className="analysis-chip">{`${currency} settled`}</span>
            </div>
          </div>
          <div className="table-wrapper analysis-table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>{"方案"}</th>
                  <th>{"MSRP"}</th>
                  <th>{"首付"}</th>
                  <th>{"Balloon"}</th>
                  <th>{"净贷款"}</th>
                  <th>{"月供"}</th>
                  <th>{"相对基准"}</th>
                  <th>{"总支付"}</th>
                </tr>
              </thead>
              <tbody>
                {results.map((row) => (
                  <tr key={row.vehicle}>
                    <td>{row.vehicle}</td>
                    <td>{row.msrp.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td>{row.down.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td>{row.balloon.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td>{row.net_financed.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td><strong>{row.monthly.toLocaleString(undefined, { maximumFractionDigits: 0 })}</strong></td>
                    <td>{(row.monthly - baseMonthly).toLocaleString(undefined, { maximumFractionDigits: 0, signDisplay: "always" })}</td>
                    <td>{row.total_payments.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <div ref={(el) => { chartRef.current = el; }}>
        {comparisonTraces.length > 0 && (
          <div className="rv-finance-chart analysis-chart-block">
            <div className="analysis-subsection-title">多方案月供对比</div>
            <PlotlyChart
              data={applyRvExport(comparisonTraces)}
              layout={applyExportToLayout({
                barmode: "group",
                xaxis: { title: { text: "方案" } },
                yaxis: { title: { text: `月供 (${currency}/月)` } },
                yaxis2: { title: { text: `总支付 (${currency})` }, overlaying: "y", side: "right" },
              }, exportSettings)}
              height={360}
            />
          </div>
        )}

        {waterfallTrace.length > 0 && (
          <div className="rv-finance-chart analysis-chart-block">
            <div className="analysis-subsection-title">资金流瀑布图</div>
            <PlotlyChart
              data={applyRvExport(waterfallTrace)}
              layout={applyExportToLayout({ yaxis: { title: { text: currency } } }, exportSettings)}
              height={380}
            />
          </div>
        )}

        {tornadoTraces.length > 0 && (
          <div className="rv-finance-chart analysis-chart-block">
            <div className="analysis-subsection-title">敏感性分析（龙卷风图）</div>
            <PlotlyChart
              data={applyRvExport(tornadoTraces)}
              layout={applyExportToLayout({
                barmode: "relative",
                xaxis: { title: { text: `月供变化 (${currency}/月)` } },
              }, exportSettings)}
              height={320}
            />
          </div>
        )}

        {contourTrace.length > 0 && (
          <div className="rv-finance-chart analysis-chart-block">
            <div className="analysis-subsection-title">APR × RV 月供等高线</div>
            <PlotlyChart
              data={applyRvExport(contourTrace)}
              layout={applyExportToLayout({
                xaxis: { title: { text: "APR %" } },
                yaxis: { title: { text: "RV %" } },
              }, exportSettings)}
              height={420}
            />
          </div>
        )}
      </div>

      {sensitivitySummary.length > 0 && (
        <details className="adv-disclosure analysis-disclosure" open>
          <summary>{"敏感性汇总"}</summary>
          <div className="table-wrapper analysis-table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>{"参数"}</th>
                  <th>{"低值月供"}</th>
                  <th>{"基准月供"}</th>
                  <th>{"高值月供"}</th>
                  <th>{"最大波动"}</th>
                </tr>
              </thead>
              <tbody>
                {sensitivitySummary.map((row) => (
                  <tr key={row.param}>
                    <td>{row.param}</td>
                    <td>{row.low.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td>{row.base.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td>{row.high.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td>{Math.max(Math.abs(row.low - row.base), Math.abs(row.high - row.base)).toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}

      {sensitivity.length > 0 && (
        <details className="adv-disclosure analysis-disclosure">
          <summary>{"敏感性场景明细"}</summary>
          <div className="table-wrapper analysis-table-wrap">
            <table className="data-table">
              <thead>
                <tr>
                  <th>{"参数"}</th>
                  <th>{"场景"}</th>
                  <th>{"参数值"}</th>
                  <th>{"月供"}</th>
                  <th>{"相对基准"}</th>
                </tr>
              </thead>
              <tbody>
                {sensitivity.map((row, idx) => (
                  <tr key={`${row.param}-${row.scenario}-${idx}`}>
                    <td>{row.param}</td>
                    <td>{row.scenario === "low" ? "低值" : "高值"}</td>
                    <td>{row.param_value}</td>
                    <td>{row.monthly.toLocaleString(undefined, { maximumFractionDigits: 0 })}</td>
                    <td>{row.delta.toLocaleString(undefined, { maximumFractionDigits: 0, signDisplay: "always" })}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </details>
      )}

      <ExportPanel
        value={exportSettings}
        onChange={setExportSettings}
        graphDiv={chartRef.current}
        seriesNames={exportSeriesNames}
        labelModeOptions={rvLabelModeOptions}
      />

      {results.length === 0 && !loading && (
        <div className="chart-empty">{"填入多方案参数后点击“计算”查看 RV 金融结果、敏感性和等高线。"}</div>
      )}
    </div>
  );
}
