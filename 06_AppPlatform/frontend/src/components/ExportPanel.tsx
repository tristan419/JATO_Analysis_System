import { useEffect, useState } from "react";
import type { Data, Layout } from "plotly.js";

import { POWERTRAIN_COLORS as FIXED_POWERTRAIN_COLORS } from "../utils/colors";

export type ExportLabelMode =
  | "off"
  | "value"
  | "series"
  | "model"
  | "sales"
  | "model+value"
  | "model+sales";

export interface ExportLabelModeAvailability {
  showValue?: boolean;
  showSeries?: boolean;
  showModel?: boolean;
  showSales?: boolean;
}

type ExportLabelValue = string | number | null | undefined;

export interface ExportTraceLabelFields {
  model?: ExportLabelValue[];
  sales?: ExportLabelValue[];
  value?: ExportLabelValue[];
  series?: ExportLabelValue[];
}

export interface ExportSettings {
  showXGrid: boolean;
  showYGrid: boolean;
  showAxisLine: boolean;
  showLegend: boolean;
  legendPosition: "right" | "top" | "bottom" | "left";
  colorScheme: string;
  fontSize: number;
  gridColor: string;
  axisColor: string;
  xTickFormat: string;
  yTickFormat: string;
  paperBg: string;
  plotBg: string;
  chartTitle: string;
  xTitle: string;
  yTitle: string;
  exportWidth: number;
  exportHeight: number;
  dataLabelMode: ExportLabelMode;
  dataLabelPosition: string;
  decimalPlaces: number;
  seriesColors: Record<string, string>;
}

export const DEFAULT_EXPORT: ExportSettings = {
  showXGrid: true, showYGrid: true, showAxisLine: true, showLegend: true,
  legendPosition: "right", colorScheme: "default", fontSize: 12,
  gridColor: "#E5E7EB", axisColor: "#6B7280",
  xTickFormat: "", yTickFormat: "",
  paperBg: "#FFFFFF", plotBg: "#FFFFFF",
  chartTitle: "", xTitle: "", yTitle: "",
  exportWidth: 1200, exportHeight: 800,
  dataLabelMode: "off", dataLabelPosition: "auto",
  decimalPlaces: 0,
  seriesColors: {},
};

const LEGEND_MAP: Record<string, Partial<Layout["legend"]>> = {
  right: { x: 1.02, y: 1, xanchor: "left", orientation: "v" },
  top: { x: 0.5, y: 1.12, xanchor: "center", orientation: "h" },
  bottom: { x: 0.5, y: -0.15, xanchor: "center", orientation: "h" },
  left: { x: -0.15, y: 1, xanchor: "right", orientation: "v" },
};

const PALETTES: Record<string, string[]> = {
  default: ["#2563eb","#16a34a","#f59e0b","#ef4444","#8b5cf6","#ec4899","#14b8a6","#f97316","#6366f1","#0ea5e9"],
  plotly: ["#636EFA","#EF553B","#00CC96","#AB63FA","#FFA15A","#19D3F3","#FF6692","#B6E880","#FF97FF","#FECB52"],
  safe: ["#88CCEE","#CC6677","#DDCC77","#117733","#332288","#AA4499","#44AA99","#999933","#882255","#661100"],
  set2: ["#66C2A5","#FC8D62","#8DA0CB","#E78AC3","#A6D854","#FFD92F","#E5C494","#B3B3B3"],
  pastel: ["#FBB4AE","#B3CDE3","#CCEBC5","#DECBE4","#FED9A6","#FFFFCC","#E5D8BD","#FDDAEC"],
  dark24: ["#2E91E5","#E15F99","#1CA71C","#FB0D0D","#DA16FF","#222A2A","#B68100","#750D86","#EB663B","#511CFB"],
};

const TICK_FORMATS: { v: string; l: string }[] = [
  { v: "", l: "保留原始" }, { v: "d", l: "整数" }, { v: ",.0f", l: "千分位整数" },
  { v: ",.1f", l: "千分位1位" }, { v: ".0%", l: "百分比(0-1)" }, { v: ".2s", l: "科学计数" },
];
const DEFAULT_LABEL_MODES: ExportLabelMode[] = ["off","value","series","model","sales","model+value","model+sales"];
const LABEL_MODE_LABELS: Record<ExportLabelMode, string> = {
  off: "关闭",
  value: "value",
  series: "series",
  model: "model",
  sales: "sales",
  "model+value": "model+value",
  "model+sales": "model+sales",
};
const LABEL_POSITIONS = ["auto","inside","outside","top","middle"];

export function buildExportLabelModeOptions({
  showValue = true,
  showSeries = true,
  showModel = false,
  showSales = false,
}: ExportLabelModeAvailability = {}): ExportLabelMode[] {
  const modes: ExportLabelMode[] = ["off"];
  if (showValue) modes.push("value");
  if (showSeries) modes.push("series");
  if (showModel) modes.push("model");
  if (showSales) modes.push("sales");
  if (showModel && showValue) modes.push("model+value");
  if (showModel && showSales) modes.push("model+sales");
  return modes;
}

export function withExportLabels<T extends Data>(trace: T, labels: ExportTraceLabelFields): T {
  const next = { ...trace } as T & { meta?: Record<string, unknown> };
  const meta = next.meta && typeof next.meta === "object" && !Array.isArray(next.meta)
    ? next.meta
    : {};
  next.meta = { ...meta, __exportLabels: labels };
  return next;
}

function inferPointCount(trace: Partial<Data> & Record<string, unknown>): number {
  if (Array.isArray(trace.x)) return trace.x.length;
  if (Array.isArray(trace.y)) return trace.y.length;
  if (Array.isArray(trace.text)) return trace.text.length;
  if (Array.isArray(trace.customdata)) return trace.customdata.length;
  if (Array.isArray(trace.z)) return trace.z.length;
  return 0;
}

function normalizeCustomdataRows(raw: unknown, pointCount: number): unknown[][] {
  if (!Array.isArray(raw)) {
    return Array.from({ length: pointCount }, () => [] as unknown[]);
  }
  return Array.from({ length: pointCount }, (_, index) => {
    const row = raw[index];
    if (Array.isArray(row)) return [...row];
    if (row === undefined) return [];
    return [row];
  });
}

function normalizeFieldValues(
  raw: ExportLabelValue[] | undefined,
  pointCount: number,
  fallback: (index: number) => ExportLabelValue,
): ExportLabelValue[] {
  return Array.from({ length: pointCount }, (_, index) => raw?.[index] ?? fallback(index));
}

function formatLabelValue(value: ExportLabelValue, decimalPlaces: number): string {
  if (value === null || value === undefined || value === "") return "";
  if (typeof value === "number" && Number.isFinite(value)) {
    return value.toLocaleString(undefined, {
      minimumFractionDigits: decimalPlaces,
      maximumFractionDigits: decimalPlaces,
    });
  }
  return String(value);
}

function inferNumericValue(arrayValue: unknown, index: number): ExportLabelValue {
  if (Array.isArray(arrayValue)) return arrayValue[index] as ExportLabelValue;
  return arrayValue as ExportLabelValue;
}

function inferTraceValue(trace: Record<string, unknown>, index: number): ExportLabelValue {
  if (trace.orientation === "h") return inferNumericValue(trace.x, index);
  return inferNumericValue(trace.y, index);
}

function inferTraceSales(trace: Record<string, unknown>, index: number): ExportLabelValue {
  const custom = trace.customdata;
  if (Array.isArray(custom)) {
    const row = custom[index];
    if (Array.isArray(row) && row.length > 0) return row[0] as ExportLabelValue;
    if (!Array.isArray(row) && row !== undefined) return row as ExportLabelValue;
  }
  return inferTraceValue(trace, index);
}

function resolveExportLabelMetadata(trace: Record<string, unknown>): ExportTraceLabelFields {
  const meta = trace.meta;
  if (meta && typeof meta === "object" && !Array.isArray(meta)) {
    const labels = (meta as Record<string, unknown>).__exportLabels;
    if (labels && typeof labels === "object" && !Array.isArray(labels)) {
      return labels as ExportTraceLabelFields;
    }
  }
  return {};
}

export function applyExportToLayout(
  layout: Partial<Layout>,
  s: ExportSettings,
): Partial<Layout> {
  const leg = LEGEND_MAP[s.legendPosition] ?? LEGEND_MAP.right;
  const existingTitle = layout.title;
  return {
    ...layout,
    showlegend: s.showLegend,
    legend: s.showLegend ? leg as Layout["legend"] : undefined,
    title: s.chartTitle
      ? {
          ...(typeof existingTitle === "object" && existingTitle !== null ? existingTitle as object : {}),
          text: s.chartTitle,
          font: { size: s.fontSize + 2 },
        }
      : existingTitle,
    font: { size: s.fontSize },
    paper_bgcolor: s.paperBg,
    plot_bgcolor: s.plotBg,
    xaxis: {
      ...(layout.xaxis as object ?? {}),
      showgrid: s.showXGrid,
      gridcolor: s.gridColor,
      showline: s.showAxisLine,
      linecolor: s.axisColor,
      tickformat: s.xTickFormat || undefined,
      ...(s.xTitle ? { title: { text: s.xTitle } } : {}),
    } as Layout["xaxis"],
    yaxis: {
      ...(layout.yaxis as object ?? {}),
      showgrid: s.showYGrid,
      gridcolor: s.gridColor,
      showline: s.showAxisLine,
      linecolor: s.axisColor,
      tickformat: s.yTickFormat || undefined,
      ...(s.yTitle ? { title: { text: s.yTitle } } : {}),
    } as Layout["yaxis"],
  };
}

export function getExportPalette(scheme: string): string[] {
  return PALETTES[scheme] ?? PALETTES.default;
}

function getSeriesDefaultColor(name: string, fallback: string): string {
  return FIXED_POWERTRAIN_COLORS[name.toUpperCase()] ?? fallback;
}

/* ── B9: apply data labels to trace-level properties ── */
export function applyDataLabelsToTraces(traces: Data[], s: ExportSettings): Data[] {
  if (s.dataLabelMode === "off") return traces;
  const pos = s.dataLabelPosition === "auto" ? undefined : s.dataLabelPosition;
  const decimalPlaces = s.decimalPlaces ?? 0;
  return traces.map(tr => {
    const t = { ...tr } as any;
    if (t.type === "heatmap" || t.type === "contour") return t as Data;
    const pointCount = inferPointCount(t);
    if (pointCount === 0) return t as Data;

    const labels = resolveExportLabelMetadata(t);
    const originalRows = normalizeCustomdataRows(t.customdata, pointCount);
    const baseOffset = originalRows.reduce((max, row) => Math.max(max, row.length), 0);
    const modelValues = normalizeFieldValues(labels.model, pointCount, index => {
      if (Array.isArray(t.text)) return t.text[index] as ExportLabelValue;
      return typeof t.text === "string" ? t.text : "";
    });
    const salesValues = normalizeFieldValues(labels.sales, pointCount, index => inferTraceSales(t, index));
    const valueValues = normalizeFieldValues(labels.value, pointCount, index => inferTraceValue(t, index));
    const seriesValues = normalizeFieldValues(labels.series, pointCount, () => String(t.name ?? ""));

    t.customdata = originalRows.map((row, index) => [
      ...row,
      modelValues[index],
      salesValues[index],
      valueValues[index],
      seriesValues[index],
    ]);

    const fieldIndex = {
      model: baseOffset,
      sales: baseOffset + 1,
      value: baseOffset + 2,
      series: baseOffset + 3,
    };
    const formatAt = (index: number) => `%{customdata[${index}]}`;
    const numericAt = (index: number) => `%{customdata[${index}]}`;

    switch (s.dataLabelMode) {
      case "value":
        t.text = valueValues.map(value => formatLabelValue(value, decimalPlaces));
        t.texttemplate = "%{text}";
        t.textposition = pos || (t.type === "bar" ? "outside" : "top");
        break;
      case "series":
        t.texttemplate = formatAt(fieldIndex.series);
        t.textposition = pos || "top";
        break;
      case "model":
        t.texttemplate = formatAt(fieldIndex.model);
        t.textposition = pos || "top";
        break;
      case "sales":
        t.text = salesValues.map(value => formatLabelValue(value, decimalPlaces));
        t.texttemplate = "%{text}";
        t.textposition = pos || "top";
        break;
      case "model+value":
        t.texttemplate = `${formatAt(fieldIndex.model)}: ${numericAt(fieldIndex.value)}`;
        t.textposition = pos || "top";
        break;
      case "model+sales":
        t.texttemplate = `${formatAt(fieldIndex.model)}: ${numericAt(fieldIndex.sales)}`;
        t.textposition = pos || "top";
        break;
      default: break;
    }
    if (t.type === "bar") {
      t.textangle = 0;
    } else if (t.mode && typeof t.mode === "string") {
      const parts = new Set(t.mode.split("+"));
      parts.add("text");
      t.mode = [...parts].join("+");
    }
    return t as Data;
  });
}

/* ── B10: apply per-series manual colors ── */
export function applySeriesColors(traces: Data[], colors: Record<string, string>): Data[] {
  if (!colors || Object.keys(colors).length === 0) return traces;
  return traces.map(tr => {
    const name = (tr as any).name as string | undefined;
    if (name && colors[name]) {
      const t = { ...tr } as any;
      if (t.marker) t.marker = { ...t.marker, color: colors[name] };
      else if (t.line) t.line = { ...t.line, color: colors[name] };
      else t.marker = { color: colors[name] };
      return t as Data;
    }
    return tr;
  });
}

export async function downloadPng(graphDiv: HTMLElement | null, settings: ExportSettings) {
  if (!graphDiv) return;
  const { default: Plotly } = await import("plotly.js-cartesian-dist-min");
  Plotly.downloadImage(graphDiv, {
    format: "png", width: settings.exportWidth, height: settings.exportHeight,
    filename: settings.chartTitle || "jato_export",
  });
}

interface Props {
  value: ExportSettings;
  onChange: (s: ExportSettings) => void;
  graphDiv?: HTMLElement | null;
  seriesNames?: string[];
  labelModeOptions?: ExportLabelMode[];
  showExportButton?: boolean;
}

export function ExportPanel({
  value: s,
  onChange,
  graphDiv,
  seriesNames,
  labelModeOptions,
  showExportButton = true,
}: Props) {
  const [open, setOpen] = useState(false);
  const set = <K extends keyof ExportSettings>(k: K, v: ExportSettings[K]) => onChange({ ...s, [k]: v });
  const resolvedLabelModes = labelModeOptions && labelModeOptions.length > 0 ? labelModeOptions : DEFAULT_LABEL_MODES;
  const safeLabelMode = resolvedLabelModes.includes(s.dataLabelMode) ? s.dataLabelMode : "off";

  useEffect(() => {
    if (safeLabelMode !== s.dataLabelMode) {
      onChange({ ...s, dataLabelMode: safeLabelMode });
    }
  }, [onChange, s, safeLabelMode]);

  return (
    <div className="export-panel">
      <button className="btn btn-sm btn-secondary" onClick={() => setOpen(!open)}>
        {open ? "▾ 收起导出设置" : "▸ 导出图设置"}
      </button>
      {open && (
        <div className="export-panel-body">
          <div className="export-row">
            <label><input type="checkbox" checked={s.showXGrid} onChange={e => set("showXGrid", e.target.checked)} /> X网格线</label>
            <label><input type="checkbox" checked={s.showYGrid} onChange={e => set("showYGrid", e.target.checked)} /> Y网格线</label>
            <label><input type="checkbox" checked={s.showAxisLine} onChange={e => set("showAxisLine", e.target.checked)} /> 坐标轴线</label>
            <label><input type="checkbox" checked={s.showLegend} onChange={e => set("showLegend", e.target.checked)} /> 图例</label>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>图例位置</label>
              <select value={s.legendPosition} onChange={e => set("legendPosition", e.target.value as ExportSettings["legendPosition"])}>
                <option value="right">右侧</option><option value="top">顶部</option>
                <option value="bottom">底部</option><option value="left">左侧</option>
              </select>
            </div>
            <div className="filter-group"><label>配色</label>
              <select value={s.colorScheme} onChange={e => set("colorScheme", e.target.value)}>
                {Object.keys(PALETTES).map(k => <option key={k} value={k}>{k}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>字号</label>
              <input type="number" value={s.fontSize} min={8} max={24} style={{ width: 50 }}
                onChange={e => set("fontSize", Number(e.target.value) || 12)} />
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>X轴格式</label>
              <select value={s.xTickFormat} onChange={e => set("xTickFormat", e.target.value)}>
                {TICK_FORMATS.map(f => <option key={f.v} value={f.v}>{f.l}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>Y轴格式</label>
              <select value={s.yTickFormat} onChange={e => set("yTickFormat", e.target.value)}>
                {TICK_FORMATS.map(f => <option key={f.v} value={f.v}>{f.l}</option>)}
              </select>
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>背景色</label>
              <input type="color" value={s.paperBg} onChange={e => set("paperBg", e.target.value)} />
            </div>
            <div className="filter-group"><label>绘图区背景</label>
              <input type="color" value={s.plotBg} onChange={e => set("plotBg", e.target.value)} />
            </div>
            <div className="filter-group"><label>网格色</label>
              <input type="color" value={s.gridColor} onChange={e => set("gridColor", e.target.value)} />
            </div>
            <div className="filter-group"><label>轴线色</label>
              <input type="color" value={s.axisColor} onChange={e => set("axisColor", e.target.value)} />
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>标题</label>
              <input type="text" value={s.chartTitle} placeholder="图表标题" style={{ width: 140 }}
                onChange={e => set("chartTitle", e.target.value)} />
            </div>
            <div className="filter-group"><label>X轴标题</label>
              <input type="text" value={s.xTitle} placeholder="X轴" style={{ width: 100 }}
                onChange={e => set("xTitle", e.target.value)} />
            </div>
            <div className="filter-group"><label>Y轴标题</label>
              <input type="text" value={s.yTitle} placeholder="Y轴" style={{ width: 100 }}
                onChange={e => set("yTitle", e.target.value)} />
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>导出宽度</label>
              <input type="number" value={s.exportWidth} min={400} max={2400} step={100} style={{ width: 70 }}
                onChange={e => set("exportWidth", Number(e.target.value) || 1200)} />
            </div>
            <div className="filter-group"><label>导出高度</label>
              <input type="number" value={s.exportHeight} min={300} max={1800} step={100} style={{ width: 70 }}
                onChange={e => set("exportHeight", Number(e.target.value) || 800)} />
            </div>
          </div>
          <div className="export-row">
            <div className="filter-group"><label>数据标签</label>
              <select value={safeLabelMode} onChange={e => set("dataLabelMode", e.target.value as ExportLabelMode)}>
                {resolvedLabelModes.map(m => <option key={m} value={m}>{LABEL_MODE_LABELS[m] ?? m}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>标签位置</label>
              <select value={s.dataLabelPosition} onChange={e => set("dataLabelPosition", e.target.value)}>
                {LABEL_POSITIONS.map(p => <option key={p} value={p}>{p === "auto" ? "自动" : p}</option>)}
              </select>
            </div>
            <div className="filter-group"><label>小数位</label>
              <input type="number" value={s.decimalPlaces} min={0} max={4} style={{ width: 50 }}
                onChange={e => set("decimalPlaces", Math.max(0, Math.min(4, Number(e.target.value) || 0)))} />
            </div>
          </div>
          {seriesNames && seriesNames.length > 0 && seriesNames.length <= 30 && (
            <div className="export-row" style={{flexWrap:"wrap",gap:4}}>
              <span style={{width:"100%",fontSize:12,color:"var(--c-text-muted)"}}>逐系列配色</span>
              {seriesNames.map((name, i) => (
                <label key={name} style={{display:"inline-flex",alignItems:"center",gap:2,fontSize:11}}>
                  <input type="color"
                    value={s.seriesColors[name] ?? getSeriesDefaultColor(name, getExportPalette(s.colorScheme)[i % getExportPalette(s.colorScheme).length])}
                    onChange={e => set("seriesColors", { ...s.seriesColors, [name]: e.target.value })}
                    style={{width:20,height:20,padding:0,border:"none"}} />
                  {name}
                </label>
              ))}
            </div>
          )}
          {showExportButton ? (
            <div className="export-row">
              <button className="btn btn-primary" onClick={() => { void downloadPng(graphDiv ?? null, s); }}
                disabled={!graphDiv}>📷 导出 PNG</button>
            </div>
          ) : null}
        </div>
      )}
    </div>
  );
}
