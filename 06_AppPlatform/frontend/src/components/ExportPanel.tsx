import { useEffect, useState } from "react";
import type { Data, Layout } from "plotly.js";

import { POWERTRAIN_COLORS as FIXED_POWERTRAIN_COLORS } from "../utils/colors";
import { DebouncedNumberInput } from "./deckControls";

export type ExportLabelMode =
  | "off"
  | "value"
  | "series"
  | "model"
  | "sales"
  | "model+value"
  | "model+sales";
export type ExportLabelOverlapStrategy = "all" | "smart_top" | "selected" | "clean" | "none" | "smart";

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
  labelFontSize?: number;
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
  dataLabelOverlapStrategy: ExportLabelOverlapStrategy;
  decimalPlaces: number;
  seriesColors: Record<string, string>;
}

export const DEFAULT_EXPORT: ExportSettings = {
  showXGrid: true, showYGrid: true, showAxisLine: true, showLegend: true,
  legendPosition: "right", colorScheme: "default", fontSize: 12, labelFontSize: 12,
  gridColor: "#E5E7EB", axisColor: "#6B7280",
  xTickFormat: "", yTickFormat: "",
  paperBg: "#FFFFFF", plotBg: "#FFFFFF",
  chartTitle: "", xTitle: "", yTitle: "",
  exportWidth: 1200, exportHeight: 800,
  dataLabelMode: "off", dataLabelPosition: "auto",
  dataLabelOverlapStrategy: "all",
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
const LABEL_POSITIONS = ["auto","inside","outside","top","top center","middle","bottom center"];
const LABEL_OVERLAP_STRATEGIES: Array<{ value: ExportLabelOverlapStrategy; label: string }> = [
  { value: "all", label: "All" },
  { value: "smart_top", label: "Smart Top" },
  { value: "selected", label: "Selected" },
  { value: "clean", label: "Clean" },
];

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

function normalizeExportLabelStrategy(strategy: ExportLabelOverlapStrategy | undefined): ExportLabelOverlapStrategy {
  if (strategy === "smart") return "smart_top";
  if (strategy === "none" || !strategy) return "all";
  return strategy;
}

function resolveLabelFontSize(settings: ExportSettings): number {
  return settings.labelFontSize ?? settings.fontSize;
}

function hashString(str: string): number {
  let hash = 0;
  for (let i = 0; i < str.length; i += 1) {
    hash = ((hash << 5) - hash) + str.charCodeAt(i);
    hash |= 0;
  }
  return Math.abs(hash);
}

function jitter(key: string, amplitude: number): number {
  return ((hashString(key) % 1000) / 1000) * amplitude * 2 - amplitude;
}

interface PreparedExportLabels {
  customdata: unknown[][];
  modelValues: ExportLabelValue[];
  salesValues: ExportLabelValue[];
  valueValues: ExportLabelValue[];
  seriesValues: ExportLabelValue[];
  fieldIndex: {
    model: number;
    sales: number;
    value: number;
    series: number;
  };
  text: string[];
}

function buildLabelText(
  mode: ExportLabelMode,
  modelValues: ExportLabelValue[],
  salesValues: ExportLabelValue[],
  valueValues: ExportLabelValue[],
  seriesValues: ExportLabelValue[],
  decimalPlaces: number,
): string[] {
  return modelValues.map((model, index) => {
    const formattedModel = formatLabelValue(model, decimalPlaces);
    const formattedSales = formatLabelValue(salesValues[index], decimalPlaces);
    const formattedValue = formatLabelValue(valueValues[index], decimalPlaces);
    switch (mode) {
      case "value":
        return formattedValue;
      case "series":
        return formatLabelValue(seriesValues[index], decimalPlaces);
      case "model":
        return formattedModel;
      case "sales":
        return formattedSales;
      case "model+value":
        return formattedModel ? `${formattedModel}: ${formattedValue}` : formattedValue;
      case "model+sales":
        return formattedModel ? `${formattedModel}: ${formattedSales}` : formattedSales;
      case "off":
      default:
        return "";
    }
  });
}

function prepareExportLabels(
  trace: Record<string, unknown>,
  pointCount: number,
  settings: ExportSettings,
): PreparedExportLabels {
  const labels = resolveExportLabelMetadata(trace);
  const originalRows = normalizeCustomdataRows(trace.customdata, pointCount);
  const baseOffset = originalRows.reduce((max, row) => Math.max(max, row.length), 0);
  const modelValues = normalizeFieldValues(labels.model, pointCount, index => {
    if (Array.isArray(trace.text)) return trace.text[index] as ExportLabelValue;
    return typeof trace.text === "string" ? trace.text : "";
  });
  const salesValues = normalizeFieldValues(labels.sales, pointCount, index => inferTraceSales(trace, index));
  const valueValues = normalizeFieldValues(labels.value, pointCount, index => inferTraceValue(trace, index));
  const seriesValues = normalizeFieldValues(labels.series, pointCount, () => String(trace.name ?? ""));
  const customdata = originalRows.map((row, index) => [
    ...row,
    modelValues[index],
    salesValues[index],
    valueValues[index],
    seriesValues[index],
  ]);

  return {
    customdata,
    modelValues,
    salesValues,
    valueValues,
    seriesValues,
    fieldIndex: {
      model: baseOffset,
      sales: baseOffset + 1,
      value: baseOffset + 2,
      series: baseOffset + 3,
    },
    text: buildLabelText(
      settings.dataLabelMode,
      modelValues,
      salesValues,
      valueValues,
      seriesValues,
      settings.decimalPlaces ?? 0,
    ),
  };
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

interface SmartLabelCandidate {
  key: string;
  x: number;
  y: number;
  text: string;
  sales: number;
  series: string;
  customdata: unknown[];
  priority: number;
  jitterX: number;
  jitterY: number;
}

const SMART_LABEL_STYLE: Record<number, { sizeOffset: number; color: string }> = {
  3: { sizeOffset: 0, color: "rgba(15,23,42,0.96)" },
  2: { sizeOffset: -1, color: "rgba(51,65,85,0.72)" },
  1: { sizeOffset: -2, color: "rgba(51,65,85,0.44)" },
  0: { sizeOffset: -3, color: "rgba(51,65,85,0.26)" },
};

function toFiniteNumberArray(raw: unknown, pointCount: number): number[] | null {
  if (!Array.isArray(raw) || raw.length < pointCount) return null;
  const values = raw.slice(0, pointCount).map((value) => Number(value));
  return values.every((value) => Number.isFinite(value)) ? values : null;
}

function canUseSmartScatterLabels(trace: Record<string, unknown>): boolean {
  if (trace.type !== "scatter") return false;
  const mode = typeof trace.mode === "string" ? trace.mode : "";
  return mode.split("+").includes("markers");
}

function clearTraceLabels(trace: Record<string, unknown>): Record<string, unknown> {
  const next = { ...trace };
  delete next.text;
  delete next.texttemplate;
  delete next.textposition;
  delete next.textfont;
  if (typeof next.mode === "string") {
    const modeParts = next.mode.split("+").filter((part) => part !== "text");
    next.mode = modeParts.length > 0 ? modeParts.join("+") : next.mode;
  }
  return next;
}

function resolveSmartTextPosition(position: string): string {
  switch (position) {
    case "bottom center":
    case "top center":
      return position;
    case "middle":
    case "inside":
      return "middle center";
    case "auto":
    case "outside":
    case "top":
    default:
      return "top center";
  }
}

function collectSmartLabelCandidates(
  trace: Record<string, unknown>,
  traceIndex: number,
  prepared: PreparedExportLabels,
  xValues: number[],
  yValues: number[],
): SmartLabelCandidate[] {
  const xRange = Math.max(...xValues) - Math.min(...xValues) || 1;
  const yRange = Math.max(...yValues) - Math.min(...yValues) || 1;
  return prepared.text.flatMap((text, index) => {
    if (!text.trim()) return [];
    const key = [
      traceIndex,
      index,
      prepared.seriesValues[index] ?? "",
      prepared.modelValues[index] ?? "",
      xValues[index],
      yValues[index],
    ].join("|");
    const sales = Number(prepared.salesValues[index]);
    return [{
      key,
      x: xValues[index],
      y: yValues[index],
      text,
      sales: Number.isFinite(sales) ? sales : 0,
      series: formatLabelValue(prepared.seriesValues[index], 0),
      customdata: prepared.customdata[index],
      priority: 1,
      jitterX: jitter(key, xRange * 0.008),
      jitterY: jitter(`${key}_y`, yRange * 0.01),
    }];
  });
}

function rankSmartLabelCandidates(candidates: SmartLabelCandidate[]): SmartLabelCandidate[] {
  if (candidates.length === 0) return candidates;
  const salesSorted = [...candidates].map((item) => item.sales).sort((a, b) => a - b);
  const longTailCutoff = salesSorted[Math.floor(salesSorted.length * 0.2)] ?? -Infinity;
  const maxSales = Math.max(...candidates.map((item) => item.sales));
  const maxY = Math.max(...candidates.map((item) => item.y));
  const minY = Math.min(...candidates.map((item) => item.y));
  const topBySeries = new Map<string, Set<string>>();

  Array.from(new Set(candidates.map((item) => item.series))).forEach((series) => {
    const ranked = candidates
      .filter((item) => item.series === series)
      .sort((a, b) => b.sales - a.sales);
    topBySeries.set(series, new Set(ranked.slice(0, 3).map((item) => item.key)));
  });

  return candidates.map((candidate) => {
    let priority = 1;
    const seriesTop = topBySeries.get(candidate.series);
    if (candidate.sales === maxSales || candidate.y === maxY || candidate.y === minY) {
      priority = 3;
    } else if (seriesTop?.has(candidate.key)) {
      const seriesRank = candidates
        .filter((item) => item.series === candidate.series)
        .sort((a, b) => b.sales - a.sales)
        .findIndex((item) => item.key === candidate.key);
      priority = seriesRank === 0 ? 3 : 2;
    } else if (candidate.sales <= longTailCutoff) {
      priority = 0;
    }
    return { ...candidate, priority };
  });
}

function filterOverlappingSmartLabels(
  candidates: SmartLabelCandidate[],
  xRange: number,
  yRange: number,
): Set<string> {
  const hidden = new Set<string>();
  if (candidates.length <= 1) return hidden;
  const sorted = [...candidates].sort((a, b) => b.priority - a.priority);
  const placed: Array<{ x: number; y: number }> = [];
  const xThreshold = xRange * 0.022;
  const yThreshold = yRange * 0.028;
  sorted.forEach((candidate) => {
    const x = candidate.x + candidate.jitterX;
    const y = candidate.y + candidate.jitterY;
    const overlaps = placed.some(
      (point) => Math.abs(point.x - x) < xThreshold && Math.abs(point.y - y) < yThreshold,
    );
    if (overlaps) {
      hidden.add(candidate.key);
    } else {
      placed.push({ x, y });
    }
  });
  return hidden;
}

function buildSmartLabelTraces(
  candidates: SmartLabelCandidate[],
  settings: ExportSettings,
  strategy: ExportLabelOverlapStrategy,
): Data[] {
  const minPriority = strategy === "selected" ? 3 : 2;
  const visibleCandidates = candidates.filter((candidate) => candidate.priority >= minPriority);
  if (visibleCandidates.length === 0) return [];
  const xValues = visibleCandidates.map((item) => item.x);
  const yValues = visibleCandidates.map((item) => item.y);
  const hidden = filterOverlappingSmartLabels(
    visibleCandidates,
    Math.max(...xValues) - Math.min(...xValues) || 1,
    Math.max(...yValues) - Math.min(...yValues) || 1,
  );
  const visible = visibleCandidates.filter((candidate) => !hidden.has(candidate.key));
  const textposition = resolveSmartTextPosition(settings.dataLabelPosition);
  const labelFontSize = resolveLabelFontSize(settings);
  return [3, 2, 1, 0].flatMap((priority) => {
    const items = visible.filter((candidate) => candidate.priority === priority);
    if (items.length === 0) return [];
    const style = SMART_LABEL_STYLE[priority];
    return [{
      type: "scatter",
      mode: "text",
      name: `label-p${priority}`,
      showlegend: false,
      x: items.map((item) => item.x + item.jitterX),
      y: items.map((item) => item.y + item.jitterY),
      text: items.map((item) => item.text),
      textposition,
      textfont: {
        size: Math.max(7, labelFontSize + style.sizeOffset),
        color: style.color,
      },
      cliponaxis: false,
      customdata: items.map((item) => item.customdata),
      hoverinfo: "skip",
    } as Data];
  });
}

function applyDataLabelsToTrace(trace: Data, settings: ExportSettings): Data {
  const pos = settings.dataLabelPosition === "auto" ? undefined : settings.dataLabelPosition;
  const decimalPlaces = settings.decimalPlaces ?? 0;
  const t = { ...trace } as Record<string, unknown>;
  if (t.type === "heatmap" || t.type === "contour") return t as Data;
  const pointCount = inferPointCount(t);
  if (pointCount === 0) return t as Data;

  const prepared = prepareExportLabels(t, pointCount, settings);
  t.customdata = prepared.customdata;
  t.textfont = {
    ...(
      t.textfont && typeof t.textfont === "object" && !Array.isArray(t.textfont)
        ? t.textfont as Record<string, unknown>
        : {}
    ),
    size: resolveLabelFontSize(settings),
  };
  const formatAt = (index: number) => `%{customdata[${index}]}`;
  const numericAt = (index: number) => `%{customdata[${index}]}`;

  switch (settings.dataLabelMode) {
    case "value":
      t.text = prepared.valueValues.map(value => formatLabelValue(value, decimalPlaces));
      t.texttemplate = "%{text}";
      t.textposition = pos || (t.type === "bar" ? "outside" : "top");
      break;
    case "series":
      t.texttemplate = formatAt(prepared.fieldIndex.series);
      t.textposition = pos || "top";
      break;
    case "model":
      t.texttemplate = formatAt(prepared.fieldIndex.model);
      t.textposition = pos || "top";
      break;
    case "sales":
      t.text = prepared.salesValues.map(value => formatLabelValue(value, decimalPlaces));
      t.texttemplate = "%{text}";
      t.textposition = pos || "top";
      break;
    case "model+value":
      t.texttemplate = `${formatAt(prepared.fieldIndex.model)}: ${numericAt(prepared.fieldIndex.value)}`;
      t.textposition = pos || "top";
      break;
    case "model+sales":
      t.texttemplate = `${formatAt(prepared.fieldIndex.model)}: ${numericAt(prepared.fieldIndex.sales)}`;
      t.textposition = pos || "top";
      break;
    default:
      break;
  }
  if (t.type === "bar") {
    t.textangle = 0;
  } else if (t.mode && typeof t.mode === "string") {
    const parts = new Set(t.mode.split("+"));
    parts.add("text");
    t.mode = [...parts].join("+");
  }
  return t as Data;
}

/* ── B9: apply data labels to trace-level properties ── */
export function applyDataLabelsToTraces(traces: Data[], settings: ExportSettings): Data[] {
  if (settings.dataLabelMode === "off") return traces;
  const labelStrategy = normalizeExportLabelStrategy(settings.dataLabelOverlapStrategy);
  if (labelStrategy === "clean") {
    return traces.map((trace) => clearTraceLabels({ ...trace } as Record<string, unknown>) as Data);
  }
  if (labelStrategy !== "smart_top" && labelStrategy !== "selected") {
    return traces.map((trace) => applyDataLabelsToTrace(trace, settings));
  }

  const smartCandidates: SmartLabelCandidate[] = [];
  const baseTraces = traces.map((trace, traceIndex) => {
    const t = { ...trace } as Record<string, unknown>;
    const pointCount = inferPointCount(t);
    const xValues = toFiniteNumberArray(t.x, pointCount);
    const yValues = toFiniteNumberArray(t.y, pointCount);
    if (!canUseSmartScatterLabels(t) || pointCount === 0 || !xValues || !yValues) {
      return applyDataLabelsToTrace(trace, settings);
    }
    const prepared = prepareExportLabels(t, pointCount, settings);
    smartCandidates.push(...collectSmartLabelCandidates(t, traceIndex, prepared, xValues, yValues));
    const markerTrace = clearTraceLabels(t);
    markerTrace.customdata = prepared.customdata;
    return markerTrace as Data;
  });

  return [
    ...baseTraces,
    ...buildSmartLabelTraces(rankSmartLabelCandidates(smartCandidates), settings, labelStrategy),
  ];
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
  showDimensionControls?: boolean;
  collapsible?: boolean;
  defaultOpen?: boolean;
}

export function ExportPanel({
  value: s,
  onChange,
  graphDiv,
  seriesNames,
  labelModeOptions,
  showExportButton = true,
  showDimensionControls = true,
  collapsible = true,
  defaultOpen = false,
}: Props) {
  const [open, setOpen] = useState(defaultOpen);
  const set = <K extends keyof ExportSettings>(k: K, v: ExportSettings[K]) => onChange({ ...s, [k]: v });
  const resolvedLabelModes = labelModeOptions && labelModeOptions.length > 0 ? labelModeOptions : DEFAULT_LABEL_MODES;
  const safeLabelMode = resolvedLabelModes.includes(s.dataLabelMode) ? s.dataLabelMode : "off";
  const safeLabelStrategy = normalizeExportLabelStrategy(s.dataLabelOverlapStrategy);
  const bodyOpen = !collapsible || open;

  useEffect(() => {
    if (safeLabelMode !== s.dataLabelMode) {
      onChange({ ...s, dataLabelMode: safeLabelMode });
    }
  }, [onChange, s, safeLabelMode]);

  return (
    <div className={`export-panel${collapsible ? "" : " export-panel--static"}`}>
      {collapsible ? (
        <button type="button" className="btn btn-sm btn-secondary" onClick={() => setOpen(!open)}>
          {open ? "▾ 收起导出设置" : "▸ 导出图设置"}
        </button>
      ) : null}
      {bodyOpen ? (
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
              <DebouncedNumberInput
                value={s.fontSize}
                min={8}
                max={24}
                style={{ width: 50 }}
                onCommit={(value) => {
                  if (value !== null) set("fontSize", value);
                }}
              />
            </div>
            <div className="filter-group"><label>标签字号</label>
              <DebouncedNumberInput
                value={resolveLabelFontSize(s)}
                min={7}
                max={28}
                style={{ width: 50 }}
                onCommit={(value) => {
                  if (value !== null) set("labelFontSize", value);
                }}
              />
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
          {showDimensionControls ? (
            <div className="export-row">
              <div className="filter-group"><label>导出宽度</label>
                <DebouncedNumberInput
                  value={s.exportWidth}
                  min={400}
                  max={2400}
                  step={100}
                  style={{ width: 70 }}
                  onCommit={(value) => {
                    if (value !== null) set("exportWidth", value);
                  }}
                />
              </div>
              <div className="filter-group"><label>导出高度</label>
                <DebouncedNumberInput
                  value={s.exportHeight}
                  min={300}
                  max={1800}
                  step={100}
                  style={{ width: 70 }}
                  onCommit={(value) => {
                    if (value !== null) set("exportHeight", value);
                  }}
                />
              </div>
            </div>
          ) : null}
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
            <div className="filter-group"><label>标签策略</label>
              <select
                value={safeLabelStrategy}
                onChange={e => set("dataLabelOverlapStrategy", e.target.value as ExportLabelOverlapStrategy)}
              >
                {LABEL_OVERLAP_STRATEGIES.map(item => (
                  <option key={item.value} value={item.value}>{item.label}</option>
                ))}
              </select>
            </div>
            <div className="filter-group"><label>小数位</label>
              <DebouncedNumberInput
                value={s.decimalPlaces}
                min={0}
                max={4}
                style={{ width: 50 }}
                onCommit={(value) => {
                  if (value !== null) set("decimalPlaces", value);
                }}
              />
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
      ) : null}
    </div>
  );
}
