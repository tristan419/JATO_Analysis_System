import {
  useEffect,
  useMemo,
  useState,
  type ReactElement,
} from "react";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import {
  resolveCountryChatDeckScope,
  type CountryChatDeckLens,
} from "../contexts/countryChatHelpers";
import type { CountryChatTranscriptMessage } from "../contexts/CountryChatContext";
import type {
  TimeSeriesPoint,
} from "../types";
import type {
  CountryChatAnalysisMeta,
  CountryChatDeckResponse,
  CountryChatMarketEvent,
  CountryChatNewsDigest,
  CountryChatSnapshot,
} from "../types/countryChat";
import { ptColor } from "../utils/colors";
import {
  buildCategoryAxis,
  TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT,
} from "../utils/plotlyDefaults";
import {
  LazyPlotlyChart as PlotlyChart,
  preloadPlotlyChartRuntime,
} from "./LazyPlotlyChart";
import { LoadingSurface } from "./LoadingSurface";

const PALETTE = [
  "#2563eb",
  "#0f766e",
  "#d97706",
  "#dc2626",
  "#7c3aed",
  "#db2777",
  "#0891b2",
  "#65a30d",
  "#ea580c",
  "#4f46e5",
];

const MONTH_ORDER: Record<string, number> = {
  Jan: 1,
  Feb: 2,
  Mar: 3,
  Apr: 4,
  May: 5,
  Jun: 6,
  Jul: 7,
  Aug: 8,
  Sep: 9,
  Oct: 10,
  Nov: 11,
  Dec: 12,
};

interface RankDatum {
  label: string;
  value: number;
}

interface ShareTrendDatum {
  label: string;
  suv: number;
  sedan: number;
}

interface MatrixDatum {
  columns: string[];
  rows: Array<{ label: string; values: string[] }>;
}

interface StackedDataset {
  rows: Array<Record<string, string | number>>;
  series: string[];
  xKey: string;
}

interface MigrationDataset {
  rows: Array<Record<string, string | number>>;
  years: string[];
}

interface ScatterPoint {
  x: number;
  y: number;
  z: number;
  label: string;
  group: string;
}

interface HeatmapDataset {
  rows: string[];
  columns: string[];
  values: Map<string, number>;
  max: number;
}

interface DeckEmptyReason {
  title: string;
  detail: string;
}

type GenericRecord = Record<string, unknown>;

function isRecord(value: unknown): value is GenericRecord {
  return typeof value === "object" && value !== null;
}

function toRecordArray(value: unknown): GenericRecord[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function toNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string") {
    const normalized = value.trim();
    if (!normalized) {
      return null;
    }
    const parsed = Number(normalized);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function toText(value: unknown): string {
  if (typeof value === "string") {
    return value.trim();
  }
  if (typeof value === "number") {
    return String(value);
  }
  return "";
}

function formatMetric(value: number | undefined): string {
  if (value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return value.toLocaleString();
}

function formatPercent(value: number | null): string {
  if (value === null) {
    return "-";
  }
  const scaled = Math.abs(value) <= 1 ? value * 100 : value;
  return `${scaled.toFixed(1)}%`;
}

function formatDateLabel(value: string | null | undefined): string {
  const text = toText(value);
  if (!text) {
    return "最新同步";
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    return text;
  }
  return new Intl.DateTimeFormat("zh-CN", {
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
  }).format(parsed);
}

function newsProviderLabel(provider: string | null | undefined): string {
  const normalized = toText(provider).toLowerCase();
  if (!normalized) {
    return "新闻快照";
  }
  if (normalized === "gemini") {
    return "Gemini 摘要";
  }
  if (normalized === "rss-live") {
    return "Live RSS";
  }
  if (normalized === "rss-stored") {
    return "RSS 快照";
  }
  if (normalized === "rss-fallback") {
    return "RSS 直出";
  }
  return titleCaseLabel(normalized);
}

function normalizeMarketEvents(source: unknown): CountryChatMarketEvent[] {
  const events: CountryChatMarketEvent[] = [];
  for (const record of toRecordArray(source)) {
    const title = toText(record.title);
    const url = toText(record.url);
    if (!title || !url) {
      continue;
    }
    const tags = Array.isArray(record.tags)
      ? record.tags.map((item) => toText(item)).filter(Boolean)
      : [];
    events.push({
      sourceCode: toText(record.sourceCode) || undefined,
      countryCode: toText(record.countryCode) || undefined,
      countryLabel: toText(record.countryLabel) || undefined,
      publisher: toText(record.publisher) || undefined,
      title,
      summary: toText(record.summary) || undefined,
      url,
      publishedAt: toText(record.publishedAt) || undefined,
      tags,
    });
  }
  return events;
}

function normalizeNewsDigest(source: unknown): CountryChatNewsDigest | null {
  if (!isRecord(source)) {
    return null;
  }
  const articleCount = toNumber(source.articleCount);
  if (articleCount === null) {
    return null;
  }
  const highlights = Array.isArray(source.highlights)
    ? source.highlights.map((item) => toText(item)).filter(Boolean)
    : [];
  return {
    countryCode: toText(source.countryCode) || undefined,
    countryLabel: toText(source.countryLabel) || undefined,
    articleCount,
    updatedAt: toText(source.updatedAt) || undefined,
    headline: toText(source.headline) || undefined,
    summary: toText(source.summary) || undefined,
    highlights,
    stale: Boolean(source.stale),
    summaryProvider: toText(source.summaryProvider) || undefined,
    summaryModel: toText(source.summaryModel) || undefined,
    syncTimestamp: toText(source.syncTimestamp) || undefined,
  };
}

function rankItemsFromUnknown(
  source: unknown,
  labelKeys: string[] = ["label", "brand", "Brand", "model", "Model", "Powertrain"],
  valueKeys: string[] = ["value", "volume", "Sales", "sales", "Value"],
): RankDatum[] {
  const records = isRecord(source) && Array.isArray(source.items)
    ? toRecordArray(source.items)
    : toRecordArray(source);
  return records
    .map((record) => {
      const label = labelKeys
        .map((key) => toText(record[key]))
        .find(Boolean) ?? "";
      const value = valueKeys
        .map((key) => toNumber(record[key]))
        .find((item) => item !== null) ?? null;
      return label && value !== null ? { label, value } : null;
    })
    .filter((item): item is RankDatum => item !== null)
    .sort((left, right) => right.value - left.value);
}

function shareTrendFromUnknown(source: unknown): ShareTrendDatum[] {
  const records = isRecord(source) && Array.isArray(source.items)
    ? toRecordArray(source.items)
    : toRecordArray(source);
  return records
    .map((record) => {
      const label = toText(record.label) || toText(record.period);
      const suv = toNumber(record.suvSharePct);
      const sedan = toNumber(record.sedanSharePct);
      if (!label || suv === null || sedan === null) {
        return null;
      }
      return {
        label,
        suv: Math.abs(suv) <= 1 ? suv * 100 : suv,
        sedan: Math.abs(sedan) <= 1 ? sedan * 100 : sedan,
      };
    })
    .filter((item): item is ShareTrendDatum => item !== null);
}

function matrixFromUnknown(source: unknown): MatrixDatum | null {
  if (!isRecord(source)) {
    return null;
  }
  const columns = Array.isArray(source.columns)
    ? source.columns.map((column) => toText(column)).filter(Boolean)
    : [];
  const rows = toRecordArray(source.rows)
    .map((row) => {
      const label = toText(row.label) || toText(row.metricKey) || toText(row.segment);
      const cells = Array.isArray(row.cells)
        ? row.cells.map((cell) => {
            if (!isRecord(cell)) {
              return "-";
            }
            return toText(cell.display)
              || formatMetric(toNumber(cell.value) ?? undefined)
              || "-";
          })
        : columns.map((column) => {
            const raw = row[column];
            const numeric = toNumber(raw);
            return numeric !== null ? formatMetric(numeric) : toText(raw) || "-";
          });
      return label ? { label, values: cells } : null;
    })
    .filter((item): item is { label: string; values: string[] } => item !== null);
  if (rows.length === 0) {
    return null;
  }
  return {
    columns: columns.length > 0 ? columns : rows[0]?.values.map((_, index) => `列 ${index + 1}`) ?? [],
    rows,
  };
}

function stackedDatasetFromUnknown(
  source: unknown,
  xCandidates: string[],
  seriesCandidates: string[],
  valueCandidates: string[] = ["Sales", "sales", "value", "Value"],
): StackedDataset | null {
  const records = toRecordArray(source);
  if (records.length === 0) {
    return null;
  }
  const xKey = xCandidates.find((candidate) => records.some((record) => record[candidate] !== undefined));
  const seriesKey = seriesCandidates.find((candidate) => records.some((record) => record[candidate] !== undefined));
  const valueKey = valueCandidates.find((candidate) => records.some((record) => record[candidate] !== undefined));
  if (!xKey || !seriesKey || !valueKey) {
    return null;
  }

  const pivot = new Map<string, Record<string, string | number>>();
  const seriesNames = new Set<string>();
  for (const record of records) {
    const xValue = toText(record[xKey]);
    const seriesValue = toText(record[seriesKey]);
    const metricValue = toNumber(record[valueKey]);
    if (!xValue || !seriesValue || metricValue === null) {
      continue;
    }
    seriesNames.add(seriesValue);
    const row = pivot.get(xValue) ?? { [xKey]: xValue };
    row[seriesValue] = (toNumber(row[seriesValue]) ?? 0) + metricValue;
    pivot.set(xValue, row);
  }

  const series = Array.from(seriesNames);
  const rows = Array.from(pivot.values());
  if (series.length === 0 || rows.length === 0) {
    return null;
  }
  return { rows, series, xKey };
}

function migrationDatasetFromUnknown(source: unknown): MigrationDataset | null {
  const records = toRecordArray(source);
  if (records.length === 0) {
    return null;
  }

  const bandMap = new Map<string, Record<string, string | number>>();
  const years = new Set<string>();
  for (const record of records) {
    const band = toText(record.priceBand) || toText(record.PriceBand);
    const year = toText(record.year) || toText(record.Year);
    const sales = toNumber(record.sales) ?? toNumber(record.Sales);
    if (!band || !year || sales === null) {
      continue;
    }
    years.add(year);
    const row = bandMap.get(band) ?? { priceBand: band };
    row[year] = sales;
    bandMap.set(band, row);
  }
  const migrationRows = Array.from(bandMap.values());
  const migrationYears = Array.from(years).sort((left, right) => left.localeCompare(right));
  if (migrationRows.length === 0 || migrationYears.length === 0) {
    return null;
  }
  return { rows: migrationRows, years: migrationYears };
}

function heatmapDatasetFromUnknown(source: unknown): HeatmapDataset | null {
  const records = toRecordArray(source);
  if (records.length === 0) {
    return null;
  }

  const rows = new Set<string>();
  const columns = new Set<string>();
  const values = new Map<string, number>();
  let max = 0;

  for (const record of records) {
    const row = toText(record.year) || toText(record.Year);
    const column = toText(record.month) || toText(record.Month);
    const value = toNumber(record.value) ?? toNumber(record.Value);
    if (!row || !column || value === null) {
      continue;
    }
    rows.add(row);
    columns.add(column);
    values.set(`${row}::${column}`, value);
    max = Math.max(max, value);
  }

  const orderedColumns = Array.from(columns).sort((left, right) => {
    const monthCompare = (MONTH_ORDER[left] ?? 999) - (MONTH_ORDER[right] ?? 999);
    return monthCompare || left.localeCompare(right);
  });
  const orderedRows = Array.from(rows).sort((left, right) => left.localeCompare(right));
  if (orderedColumns.length === 0 || orderedRows.length === 0) {
    return null;
  }
  return { rows: orderedRows, columns: orderedColumns, values, max };
}

function buildScatterDataset(
  source: unknown,
  options: {
    xKeys: string[];
    yKeys: string[];
    zKeys: string[];
    labelKeys: string[];
    groupKeys: string[];
  },
): ScatterPoint[] {
  return toRecordArray(source)
    .map((record) => {
      const x = options.xKeys.map((key) => toNumber(record[key])).find((value) => value !== null);
      const y = options.yKeys.map((key) => toNumber(record[key])).find((value) => value !== null);
      const z = options.zKeys.map((key) => toNumber(record[key])).find((value) => value !== null) ?? 1;
      const label = options.labelKeys.map((key) => toText(record[key])).find(Boolean)
        || [toText(record.Brand), toText(record.Model)].filter(Boolean).join(" ");
      const group = options.groupKeys.map((key) => toText(record[key])).find(Boolean) || "Series";
      if (x === null || y === null || !label) {
        return null;
      }
      return { x, y, z, label, group };
    })
    .filter((item): item is ScatterPoint => item !== null);
}

function titleCaseLabel(value: string): string {
  return value.replace(/([a-z])([A-Z])/g, "$1 $2");
}

function buildDeckLayout(
  overrides: Partial<PlotlyLayout> = {},
): Partial<PlotlyLayout> {
  return {
    ...CHART_LAYOUT,
    ...overrides,
    margin: {
      l: 56,
      r: 22,
      t: 24,
      b: 52,
      ...(CHART_LAYOUT.margin ?? {}),
      ...(overrides.margin ?? {}),
    },
    legend: {
      ...(CHART_LAYOUT.legend ?? {}),
      orientation: "h",
      yanchor: "bottom",
      y: 1.02,
      xanchor: "left",
      x: 0,
      ...(overrides.legend ?? {}),
    },
    hoverlabel: {
      bgcolor: "#0f172a",
      font: { color: "#f8fafc" },
      ...(CHART_LAYOUT.hoverlabel ?? {}),
      ...(overrides.hoverlabel ?? {}),
    },
  };
}

function buildBubbleSizes(
  points: ScatterPoint[],
  compact: boolean,
): number[] {
  const values = points.map((point) => Math.max(point.z, 1));
  const max = Math.max(...values);
  const min = Math.min(...values);
  if (!Number.isFinite(max) || !Number.isFinite(min)) {
    return points.map(() => (compact ? 14 : 18));
  }
  return values.map((value) => {
    if (max === min) {
      return compact ? 14 : 18;
    }
    const ratio = (value - min) / (max - min);
    return (compact ? 12 : 14) + ratio * (compact ? 16 : 22);
  });
}

function SectionCard({
  title,
  subtitle,
  fullWidth = false,
  children,
}: {
  title: string;
  subtitle?: string;
  fullWidth?: boolean;
  children: ReactElement | ReactElement[];
}) {
  return (
    <section className={`copilot-analysis-card${fullWidth ? " is-full" : ""}`}>
      <div className="copilot-analysis-card-head">
        <div>
          <strong>{title}</strong>
          {subtitle ? <span>{subtitle}</span> : null}
        </div>
      </div>
      {children}
    </section>
  );
}

function RankBarCard({
  title,
  data,
  compact,
  usePowertrainColors = false,
}: {
  title: string;
  data: RankDatum[];
  compact: boolean;
  usePowertrainColors?: boolean;
}) {
  const top = data.slice(0, compact ? 8 : 10);
  if (top.length === 0) {
    return null;
  }
  const rows = [...top].reverse();
  const trace: Data = {
    type: "bar",
    orientation: "h",
    x: rows.map((item) => item.value),
    y: rows.map((item) => item.label),
    marker: {
      color: rows.map((item, index) => (
        usePowertrainColors
          ? ptColor(item.label, PALETTE[index % PALETTE.length])
          : PALETTE[index % PALETTE.length]
      )),
    },
    text: rows.map((item) => item.value.toLocaleString()),
    textposition: "outside",
    cliponaxis: false,
    hovertemplate: "%{y}<br>销量 %{x:,}<extra></extra>",
  };
  return (
    <SectionCard title={title} subtitle={`${top.length} 条排序视图`}>
      <PlotlyChart
        data={[trace]}
        layout={buildDeckLayout({
          margin: { l: 108, r: 28, t: 18, b: 24 },
          showlegend: false,
          xaxis: { title: { text: "销量" }, zeroline: false },
          yaxis: { automargin: true, type: "category" },
        })}
        height={Math.max(220, top.length * 32)}
      />
    </SectionCard>
  );
}

function TrendCard({
  title,
  data,
  compact,
}: {
  title: string;
  data: TimeSeriesPoint[];
  compact: boolean;
}) {
  if (data.length < 2) {
    return null;
  }
  const trace: Data = {
    type: "scatter",
    mode: "lines+markers",
    x: data.map((item) => item.time),
    y: data.map((item) => item.value),
    line: { color: "#2563eb", width: 2.5 },
    marker: { color: "#2563eb", size: 6 },
    hovertemplate: "%{x}<br>销量 %{y:,}<extra></extra>",
  };
  return (
    <SectionCard title={title} subtitle={`${data.length} 个时间点`}>
      <PlotlyChart
        data={[trace]}
        layout={buildDeckLayout({
          xaxis: buildCategoryAxis(data.map((item) => item.time)),
          yaxis: { title: { text: "销量" } },
          showlegend: false,
        })}
        height={compact ? 220 : 260}
      />
    </SectionCard>
  );
}

function ShareTrendCard({ data, compact }: { data: ShareTrendDatum[]; compact: boolean }) {
  if (data.length < 2) {
    return null;
  }
  const traces: Data[] = [
    {
      type: "scatter",
      mode: "lines",
      name: "SUV",
      x: data.map((item) => item.label),
      y: data.map((item) => item.suv),
      stackgroup: "share",
      line: { color: "#2563eb", width: 2.2 },
      fillcolor: "rgba(37, 99, 235, 0.34)",
      hovertemplate: "%{x}<br>SUV %{y:.1f}%<extra></extra>",
    },
    {
      type: "scatter",
      mode: "lines",
      name: "Sedan",
      x: data.map((item) => item.label),
      y: data.map((item) => item.sedan),
      stackgroup: "share",
      line: { color: "#f97316", width: 2.2 },
      fillcolor: "rgba(249, 115, 22, 0.26)",
      hovertemplate: "%{x}<br>Sedan %{y:.1f}%<extra></extra>",
    },
  ];
  return (
    <SectionCard title="SUV / Sedan 结构趋势" subtitle="市场结构份额对比">
      <PlotlyChart
        data={traces}
        layout={buildDeckLayout({
          xaxis: buildCategoryAxis(data.map((item) => item.label)),
          yaxis: { title: { text: "MS" }, ticksuffix: "%" },
        })}
        height={compact ? 220 : 260}
      />
    </SectionCard>
  );
}

function ScatterCard({
  title,
  points,
  compact,
  xLabel,
  yLabel,
  target,
}: {
  title: string;
  points: ScatterPoint[];
  compact: boolean;
  xLabel: string;
  yLabel: string;
  target?: ScatterPoint | null;
}) {
  if (points.length === 0) {
    return null;
  }
  const grouped = Array.from(new Set(points.map((point) => point.group)));
  const sizes = buildBubbleSizes(points, compact);
  const sizedPoints = points.map((point, index) => ({
    ...point,
    markerSize: sizes[index],
  }));
  const traces: Data[] = grouped.map((group, index) => {
    const groupPoints = sizedPoints.filter((point) => point.group === group);
    return {
      type: "scatter",
      mode: "markers",
      name: group,
      x: groupPoints.map((point) => point.x),
      y: groupPoints.map((point) => point.y),
      text: groupPoints.map((point) => point.label),
      customdata: groupPoints.map((point) => point.z),
      marker: {
        color: PALETTE[index % PALETTE.length],
        size: groupPoints.map((point) => point.markerSize),
        opacity: 0.78,
        line: { width: 1, color: "rgba(15, 23, 42, 0.18)" },
      },
      hovertemplate:
        "%{text}<br>"
        + xLabel + ": %{x:,}<br>"
        + yLabel + ": %{y:,}<br>"
        + "销量 %{customdata:,}<extra></extra>",
    };
  });
  if (target) {
    traces.push({
      type: "scatter",
      mode: "markers",
      name: "Target",
      x: [target.x],
      y: [target.y],
      text: [target.label],
      marker: {
        color: "#dc2626",
        size: compact ? 16 : 20,
        symbol: "diamond-open",
        line: { width: 2, color: "#dc2626" },
      },
      hovertemplate:
        "%{text}<br>"
        + xLabel + ": %{x:,}<br>"
        + yLabel + ": %{y:,}<extra></extra>",
    });
  }
  return (
    <SectionCard title={title} subtitle={`${points.length} 个点位`} fullWidth>
      <PlotlyChart
        data={traces}
        layout={buildDeckLayout({
          xaxis: { title: { text: xLabel }, zeroline: false },
          yaxis: { title: { text: yLabel }, zeroline: false },
        })}
        height={compact ? 260 : 320}
      />
    </SectionCard>
  );
}

function StackedBarCard({
  title,
  dataset,
  compact,
}: {
  title: string;
  dataset: StackedDataset;
  compact: boolean;
}) {
  const traces: Data[] = dataset.series.map((series, index) => ({
    type: "bar",
    name: series,
    x: dataset.rows.map((row) => String(row[dataset.xKey] ?? "")),
    y: dataset.rows.map((row) => Number(row[series] ?? 0)),
    marker: {
      color: ptColor(series, PALETTE[index % PALETTE.length]),
    },
    hovertemplate: "%{x}<br>" + series + " %{y:,}<extra></extra>",
  }));
  return (
    <SectionCard title={title} subtitle={`${dataset.series.length} 个堆叠序列`} fullWidth>
      <PlotlyChart
        data={traces}
        layout={buildDeckLayout({
          barmode: "stack",
          xaxis: buildCategoryAxis(
            dataset.rows.map((row) => String(row[dataset.xKey] ?? "")),
            { tickangle: -30 },
          ),
          yaxis: { title: { text: "销量" } },
        })}
        height={compact ? 260 : 320}
      />
    </SectionCard>
  );
}

function MigrationCard({ dataset, compact }: { dataset: MigrationDataset; compact: boolean }) {
  const traces: Data[] = dataset.years.map((year, index) => ({
    type: "scatter",
    mode: "lines+markers",
    name: year,
    x: dataset.rows.map((row) => String(row.priceBand ?? "")),
    y: dataset.rows.map((row) => Number(row[year] ?? 0)),
    line: { color: PALETTE[index % PALETTE.length], width: 2.3 },
    marker: { size: 6 },
    hovertemplate: "%{x}<br>" + year + " %{y:,}<extra></extra>",
  }));
  return (
    <SectionCard title="价格带迁移" subtitle={`${dataset.years.length} 个年度序列`} fullWidth>
      <PlotlyChart
        data={traces}
        layout={buildDeckLayout({
          xaxis: buildCategoryAxis(
            dataset.rows.map((row) => String(row.priceBand ?? "")),
            { tickangle: -30 },
          ),
          yaxis: { title: { text: "销量" } },
        })}
        height={compact ? 250 : 310}
      />
    </SectionCard>
  );
}

function MatrixCard({ title, matrix }: { title: string; matrix: MatrixDatum }) {
  return (
    <SectionCard title={title} subtitle={`${matrix.rows.length} 行`} fullWidth>
      <div className="copilot-analysis-table-wrap">
        <table className="copilot-analysis-table">
          <thead>
            <tr>
              <th>维度</th>
              {matrix.columns.map((column) => (
                <th key={column}>{column}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {matrix.rows.map((row) => (
              <tr key={row.label}>
                <th>{row.label}</th>
                {row.values.map((value, index) => (
                  <td key={`${row.label}-${index}`}>{value}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </SectionCard>
  );
}

function HeatmapCard({ dataset }: { dataset: HeatmapDataset }) {
  const zValues = dataset.rows.map((row) => (
    dataset.columns.map((column) => dataset.values.get(`${row}::${column}`) ?? 0)
  ));
  const trace: Data = {
    type: "heatmap",
    x: dataset.columns,
    y: dataset.rows,
    z: zValues,
    colorscale: [
      [0, "rgba(226, 232, 240, 0.16)"],
      [1, "rgba(37, 99, 235, 0.92)"],
    ],
    hovertemplate: "%{y} / %{x}<br>销量 %{z:,}<extra></extra>",
    colorbar: { title: { text: "销量" } },
  };
  return (
    <SectionCard title="季节性热力图" subtitle={`${dataset.rows.length} × ${dataset.columns.length} 网格`} fullWidth>
      <PlotlyChart
        data={[trace]}
        layout={buildDeckLayout({
          xaxis: buildCategoryAxis(dataset.columns),
          yaxis: { autorange: "reversed" },
        })}
        height={320}
      />
    </SectionCard>
  );
}

function NewsDigestCard({ digest }: { digest: CountryChatNewsDigest }) {
  return (
    <SectionCard
      title="新闻摘要层"
      subtitle={`最近 ${digest.articleCount} 条预抓取资讯`}
      fullWidth
    >
      <div className="copilot-analysis-news-digest">
        <div className="copilot-analysis-news-meta">
          <span className="copilot-analysis-news-badge">
            {newsProviderLabel(digest.summaryProvider)}
          </span>
          {digest.summaryModel ? (
            <span className="copilot-analysis-news-badge is-muted">
              {digest.summaryModel}
            </span>
          ) : null}
          {digest.updatedAt ? (
            <span className="copilot-analysis-news-badge is-muted">
              更新时间 {formatDateLabel(digest.updatedAt)}
            </span>
          ) : null}
          {digest.syncTimestamp ? (
            <span className="copilot-analysis-news-badge is-muted">
              同步于 {formatDateLabel(digest.syncTimestamp)}
            </span>
          ) : null}
          {digest.stale ? (
            <span className="copilot-analysis-news-badge is-stale">
              快照偏旧
            </span>
          ) : null}
        </div>
        {digest.headline ? (
          <strong className="copilot-analysis-news-headline">
            {digest.headline}
          </strong>
        ) : null}
        {digest.summary ? (
          <p className="copilot-analysis-news-summary">{digest.summary}</p>
        ) : null}
        {digest.highlights && digest.highlights.length > 0 ? (
          <ul className="copilot-analysis-news-highlights">
            {digest.highlights.map((highlight) => (
              <li key={highlight}>{highlight}</li>
            ))}
          </ul>
        ) : null}
      </div>
    </SectionCard>
  );
}

function NewsTimelineCard({
  events,
  compact,
}: {
  events: CountryChatMarketEvent[];
  compact: boolean;
}) {
  const visibleEvents = events.slice(0, compact ? 4 : 6);
  if (visibleEvents.length === 0) {
    return null;
  }
  return (
    <SectionCard
      title="市场事件流"
      subtitle={`${visibleEvents.length} 条新闻 / 政策 / 竞争事件`}
      fullWidth
    >
      <div className="copilot-analysis-news-list">
        {visibleEvents.map((event) => (
          <article key={event.url} className="copilot-analysis-news-item">
            <div className="copilot-analysis-news-item-head">
              <div>
                <strong>{event.publisher || "Market feed"}</strong>
                <span>{formatDateLabel(event.publishedAt)}</span>
              </div>
              <a
                className="copilot-analysis-news-link"
                href={event.url}
                target="_blank"
                rel="noreferrer"
              >
                原文
              </a>
            </div>
            <h4 className="copilot-analysis-news-title">{event.title}</h4>
            {event.summary ? (
              <p className="copilot-analysis-news-summary">{event.summary}</p>
            ) : null}
            {event.tags && event.tags.length > 0 ? (
              <div className="copilot-analysis-news-tags">
                {event.tags.map((tag) => (
                  <span key={`${event.url}-${tag}`} className="copilot-analysis-news-tag">
                    {tag}
                  </span>
                ))}
              </div>
            ) : null}
          </article>
        ))}
      </div>
    </SectionCard>
  );
}

function buildPositioningTarget(snapshot: CountryChatSnapshot): ScatterPoint | null {
  if (!isRecord(snapshot.positioningMap) || !isRecord(snapshot.positioningMap.target)) {
    return null;
  }
  const x = toNumber(snapshot.positioningMap.target.Length);
  const y = toNumber(snapshot.positioningMap.target.MSRP);
  if (x === null || y === null) {
    return null;
  }
  return {
    x,
    y,
    z: 4,
    label: "Target",
    group: "Target",
  };
}

interface DeckSectionDefinition {
  id: Exclude<CountryChatDeckLens, "all">;
  title: string;
  subtitle: string;
  cards: ReactElement[];
  emptyReason: DeckEmptyReason;
}

function joinLabels(labels: string[]): string {
  return labels.length > 0 ? labels.join("、") : "图表数据";
}

function countAvailableViews(snapshot: CountryChatSnapshot | null): number {
  if (!snapshot) {
    return 0;
  }
  let count = 0;
  const rankKeys = [
    snapshot.topBrands,
    snapshot.topModels,
    snapshot.powertrainMix,
    rankItemsFromUnknown(snapshot.ytdBrandRanking),
    rankItemsFromUnknown(snapshot.monthlyBrandRanking),
    rankItemsFromUnknown(snapshot.nevRangeDistribution),
    rankItemsFromUnknown(snapshot.bevShareBySegment),
  ];
  count += rankKeys.filter((items) => items.length > 0).length;
  count += snapshot.yearSeries.length > 1 ? 1 : 0;
  count += snapshot.monthSeries.length > 1 ? 1 : 0;
  count += shareTrendFromUnknown(snapshot.suvSedanTrend).length > 1 ? 1 : 0;
  count += matrixFromUnknown(snapshot.segmentMatrix)?.rows.length ? 1 : 0;
  count += matrixFromUnknown(snapshot.originAnalysis?.matrix)?.rows.length ? 1 : 0;
  count += buildScatterDataset(snapshot.positioningMap?.items, {
    xKeys: ["Length"],
    yKeys: ["MSRP"],
    zKeys: ["Sales"],
    labelKeys: ["Model", "Brand"],
    groupKeys: ["cluster", "Segment"],
  }).length > 0 ? 1 : 0;
  count += buildScatterDataset(snapshot.modelVersionBubble, {
    xKeys: ["Length"],
    yKeys: ["MSRP"],
    zKeys: ["Sales"],
    labelKeys: ["Version", "Trim"],
    groupKeys: ["Powertrain", "Trim"],
  }).length > 0 ? 1 : 0;
  count += rankItemsFromUnknown(snapshot.marketScanScope?.modelPerformance?.versionDistribution).length > 0 ? 1 : 0;
  count += stackedDatasetFromUnknown(snapshot.powertrainVsPrice, ["PriceBand"], ["Powertrain"]) ? 1 : 0;
  count += stackedDatasetFromUnknown(snapshot.segmentShareByLength, ["LengthBand"], ["Segment"]) ? 1 : 0;
  count += migrationDatasetFromUnknown(snapshot.priceMigration) ? 1 : 0;
  count += heatmapDatasetFromUnknown(snapshot.seasonalityHeatmap) ? 1 : 0;
  count += normalizeNewsDigest(snapshot.newsDigest) ? 1 : 0;
  count += normalizeMarketEvents(snapshot.marketEvents).length > 0 ? 1 : 0;
  return count;
}

function analysisMetaFromSources(
  snapshot: CountryChatSnapshot | null,
  deck: CountryChatDeckResponse | null,
): CountryChatAnalysisMeta {
  const snapshotMeta = isRecord(snapshot?.analysisMeta)
    ? snapshot.analysisMeta as CountryChatAnalysisMeta
    : {};
  const controlMeta = isRecord(deck?.controls)
    ? deck?.controls as CountryChatAnalysisMeta
    : {};
  return {
    ...snapshotMeta,
    ...controlMeta,
  };
}

function normalizeYearList(meta: CountryChatAnalysisMeta): number[] {
  const source = Array.isArray(meta.availableYears) ? meta.availableYears : [];
  return source
    .map((value) => toNumber(value))
    .filter((value): value is number => value !== null)
    .sort((left, right) => left - right);
}

function normalizeModelList(meta: CountryChatAnalysisMeta): string[] {
  const source = Array.isArray(meta.availableModels) ? meta.availableModels : [];
  return source.map((value) => toText(value)).filter(Boolean);
}

function clampModelTopN(value: number | null | undefined): number {
  const resolved = value ?? 24;
  return Math.max(8, Math.min(60, Math.round(resolved)));
}

function DeckSection({
  title,
  subtitle,
  cards,
  emptyReason,
}: {
  title: string;
  subtitle: string;
  cards: ReactElement[];
  emptyReason: DeckEmptyReason;
}) {
  return (
    <section className="copilot-analysis-section">
      <div className="copilot-analysis-section-head">
        <strong>{title}</strong>
        <span>{subtitle}</span>
      </div>
      {cards.length > 0 ? (
        <div className="copilot-analysis-grid">{cards}</div>
      ) : (
        <div className="copilot-analysis-section-empty">
          <strong>{emptyReason.title}</strong>
          <span>{emptyReason.detail}</span>
        </div>
      )}
    </section>
  );
}

export function CountryChatAnalysisDeck({
  message,
  compact = false,
  defaultExpanded = false,
}: {
  message: CountryChatTranscriptMessage;
  compact?: boolean;
  defaultExpanded?: boolean;
}) {
  const initialSnapshot = message.contextSnapshot ?? null;
  const country = message.country ?? initialSnapshot?.country ?? "";
  const initialMeta = analysisMetaFromSources(initialSnapshot, null);
  const [expanded, setExpanded] = useState(defaultExpanded);
  const [loading, setLoading] = useState(false);
  const [loadError, setLoadError] = useState("");
  const [deck, setDeck] = useState<CountryChatDeckResponse | null>(null);
  const deckScope = useMemo(
    () => resolveCountryChatDeckScope(deck?.intentRoute ?? message.intentRoute),
    [deck?.intentRoute, message.intentRoute],
  );
  const [activeLens, setActiveLens] = useState<CountryChatDeckLens>(deckScope.defaultLens);
  const [selectedYear, setSelectedYear] = useState<number | undefined>(() => {
    const value = toNumber(initialMeta.selectedYear);
    return value ?? undefined;
  });
  const [selectedModel, setSelectedModel] = useState(() => toText(initialMeta.selectedModel));
  const [modelTopN, setModelTopN] = useState(() => clampModelTopN(toNumber(initialMeta.modelTopN)));

  useEffect(() => {
    if (expanded) {
      preloadPlotlyChartRuntime().catch(() => undefined);
    }
  }, [expanded]);

  useEffect(() => {
    setExpanded(defaultExpanded);
    setDeck(null);
    setLoadError("");
    setActiveLens(deckScope.defaultLens);
    setSelectedYear(toNumber(initialMeta.selectedYear) ?? undefined);
    setSelectedModel(toText(initialMeta.selectedModel));
    setModelTopN(clampModelTopN(toNumber(initialMeta.modelTopN)));
  }, [
    deckScope.defaultLens,
    defaultExpanded,
    initialMeta.modelTopN,
    initialMeta.selectedModel,
    initialMeta.selectedYear,
    message.id,
  ]);

  useEffect(() => {
    if (!expanded || !country) {
      return;
    }
    let cancelled = false;
    setLoading(true);
    setLoadError("");
    api.countryChatDeck({
      country,
      question: message.question,
      intents: message.focusedIntents ?? message.intents,
      extracted_params: message.extractedParams ?? undefined,
      ...(selectedYear ? { selected_year: selectedYear } : {}),
      ...(selectedModel.trim() ? { selected_model: selectedModel.trim() } : {}),
      model_top_n: modelTopN,
    })
      .then((response) => {
        if (cancelled) {
          return;
        }
        setDeck(response);
        const nextMeta = analysisMetaFromSources(response.contextSnapshot, response);
        const nextYear = toNumber(nextMeta.selectedYear);
        const nextModel = toText(nextMeta.selectedModel);
        const nextTopN = clampModelTopN(toNumber(nextMeta.modelTopN));
        if (nextYear !== null && nextYear !== selectedYear) {
          setSelectedYear(nextYear);
        }
        if (nextModel && nextModel !== selectedModel) {
          setSelectedModel(nextModel);
        }
        if (nextTopN !== modelTopN) {
          setModelTopN(nextTopN);
        }
      })
      .catch((reason: unknown) => {
        if (!cancelled) {
          setLoadError(reason instanceof Error ? reason.message : String(reason));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [
    country,
    expanded,
    message.extractedParams,
    message.focusedIntents,
    message.intents,
    message.question,
    modelTopN,
    selectedModel,
    selectedYear,
  ]);

  const snapshot = deck?.contextSnapshot ?? initialSnapshot;
  const meta = useMemo(
    () => analysisMetaFromSources(snapshot, deck),
    [deck, snapshot],
  );
  const availableViews = useMemo(() => countAvailableViews(snapshot), [snapshot]);
  const availableYears = useMemo(() => normalizeYearList(meta), [meta]);
  const availableModels = useMemo(() => normalizeModelList(meta), [meta]);
  const scopeYear = selectedYear ?? toNumber(meta.selectedYear) ?? undefined;
  const scopeModel = selectedModel.trim() || toText(meta.selectedModel);
  const scopeTopN = clampModelTopN(modelTopN);
  const deckNewsDigest = useMemo(
    () => normalizeNewsDigest(snapshot?.newsDigest),
    [snapshot?.newsDigest],
  );
  const deckMarketEvents = useMemo(
    () => normalizeMarketEvents(snapshot?.marketEvents),
    [snapshot?.marketEvents],
  );

  const sections = useMemo(() => {
    if (!snapshot) {
      return [] as DeckSectionDefinition[];
    }

    const yearSeries = snapshot.yearSeries ?? [];
    const monthSeries = snapshot.monthSeries ?? [];
    const topBrands = rankItemsFromUnknown(snapshot.topBrands);
    const topModels = rankItemsFromUnknown(snapshot.topModels);
    const powertrainMix = rankItemsFromUnknown(snapshot.powertrainMix);
    const ytdRank = rankItemsFromUnknown(snapshot.ytdBrandRanking);
    const monthlyRank = rankItemsFromUnknown(snapshot.monthlyBrandRanking);
    const nevRange = rankItemsFromUnknown(
      snapshot.nevRangeDistribution,
      ["label", "RangeBandLabel", "brand"],
      ["Value", "value", "GrowthWindow", "Sales", "sales", "volume"],
    );
    const bevShare = rankItemsFromUnknown(snapshot.bevShareBySegment);
    const shareTrend = shareTrendFromUnknown(snapshot.suvSedanTrend);
    const segmentMatrix = matrixFromUnknown(snapshot.segmentMatrix);
    const originMatrix = matrixFromUnknown(snapshot.originAnalysis?.matrix);
    const positioningPoints = buildScatterDataset(snapshot.positioningMap?.items, {
      xKeys: ["Length"],
      yKeys: ["MSRP"],
      zKeys: ["Sales"],
      labelKeys: ["Model", "Brand"],
      groupKeys: ["cluster", "Segment", "Brand"],
    });
    const positioningTarget = buildPositioningTarget(snapshot);
    const priceScatter = buildScatterDataset(snapshot.priceDistribution, {
      xKeys: ["Length", "BatteryCapacity", "PricePerMeter"],
      yKeys: ["MSRP", "Sales"],
      zKeys: ["Sales", "SegmentSharePct"],
      labelKeys: ["DisplayName", "Model", "Brand"],
      groupKeys: ["Segment", "Powertrain", "Brand"],
    });
    const modelVersionBubble = buildScatterDataset(snapshot.modelVersionBubble, {
      xKeys: ["Length"],
      yKeys: ["MSRP"],
      zKeys: ["Sales"],
      labelKeys: ["Version", "Trim"],
      groupKeys: ["Powertrain", "Trim"],
    });
    const scopedVersionMix = rankItemsFromUnknown(
      snapshot.marketScanScope?.modelPerformance?.versionDistribution,
    );
    const scopedVersionAxis = toText(snapshot.marketScanScope?.modelPerformance?.versionAxis) || "version";
    const bubbleScatter = buildScatterDataset(snapshot.powertrainBubble, {
      xKeys: ["Length"],
      yKeys: ["MSRP"],
      zKeys: ["Sales"],
      labelKeys: ["DisplayName", "Model", "Brand"],
      groupKeys: ["Powertrain", "Brand"],
    });
    const tcoScatter = buildScatterDataset(snapshot.estimatedTco, {
      xKeys: ["MSRP"],
      yKeys: ["EstimatedTCO"],
      zKeys: ["Sales"],
      labelKeys: ["Model", "Brand", "Powertrain"],
      groupKeys: ["Powertrain", "Brand"],
    });
    const powertrainVsPrice = stackedDatasetFromUnknown(snapshot.powertrainVsPrice, ["PriceBand"], ["Powertrain"]);
    const segmentByLength = stackedDatasetFromUnknown(snapshot.segmentShareByLength, ["LengthBand"], ["Segment"]);
    const migration = migrationDatasetFromUnknown(snapshot.priceMigration);
    const heatmap = heatmapDatasetFromUnknown(snapshot.seasonalityHeatmap);
    const newsDigest = deckNewsDigest;
    const marketEvents = deckMarketEvents;

    const workbenchCards: ReactElement[] = [];
    const marketCards: ReactElement[] = [];
    const intelligenceCards: ReactElement[] = [];
    const trendCards: ReactElement[] = [];
    const workbenchMissing: string[] = [];
    const marketMissing: string[] = [];
    const intelligenceMissing: string[] = [];
    const trendMissing: string[] = [];

    if (positioningPoints.length === 0) {
      workbenchMissing.push("竞品定位图");
    }
    if (modelVersionBubble.length === 0) {
      workbenchMissing.push("版本气泡");
    }
    if (snapshot.marketScanScope?.focusModel && scopedVersionMix.length === 0) {
      workbenchMissing.push("Scoped 版本 / trim 分布");
    }
    if (priceScatter.length === 0) {
      workbenchMissing.push("价格 / 尺寸分布");
    }
    if (bubbleScatter.length === 0) {
      workbenchMissing.push("动力总成气泡图");
    }
    if (!powertrainVsPrice) {
      workbenchMissing.push("动力 × 价格带");
    }
    if (tcoScatter.length === 0) {
      workbenchMissing.push("估算 TCO");
    }

    if (topBrands.length === 0) {
      marketMissing.push("品牌销量排名");
    }
    if (topModels.length === 0) {
      marketMissing.push("车型销量排名");
    }
    if (powertrainMix.length === 0) {
      marketMissing.push("动力总成结构");
    }
    if (ytdRank.length === 0) {
      marketMissing.push("YTD 品牌排名");
    }
    if (monthlyRank.length === 0) {
      marketMissing.push("月度品牌排名");
    }
    if (bevShare.length === 0) {
      marketMissing.push("BEV 渗透细分");
    }
    if (nevRange.length === 0) {
      marketMissing.push("NEV 续航分布");
    }
    if (!segmentByLength) {
      marketMissing.push("细分 × 车长带");
    }
    if (!segmentMatrix) {
      marketMissing.push("细分市场矩阵");
    }
    if (!originMatrix) {
      marketMissing.push("车系阵营矩阵");
    }
    if (!newsDigest) {
      intelligenceMissing.push("新闻摘要层");
    }
    if (marketEvents.length === 0) {
      intelligenceMissing.push("市场事件流");
    }

    if (monthSeries.length <= 1) {
      trendMissing.push("月度销量趋势");
    }
    if (yearSeries.length <= 1) {
      trendMissing.push("年度销量趋势");
    }
    if (shareTrend.length <= 1) {
      trendMissing.push("SUV / Sedan 结构趋势");
    }
    if (!migration) {
      trendMissing.push("价格带迁移");
    }
    if (!heatmap) {
      trendMissing.push("季节性热力图");
    }

    if (positioningPoints.length > 0) {
      workbenchCards.push(
        <ScatterCard
          key="positioning"
          title="竞品定位图"
          points={positioningPoints}
          compact={compact}
          xLabel="Length"
          yLabel="MSRP"
          target={positioningTarget}
        />,
      );
    }
    if (modelVersionBubble.length > 0) {
      workbenchCards.push(
        <ScatterCard
          key="model-version-bubble"
          title={scopeModel ? `Model Version Bubble · ${scopeModel}` : "Model Version Bubble"}
          points={modelVersionBubble}
          compact={compact}
          xLabel="Length"
          yLabel="MSRP"
        />,
      );
    }
    if (scopedVersionMix.length > 0) {
      workbenchCards.push(
        <RankBarCard
          key="scoped-version-mix"
          title={scopeModel ? `${scopeModel} ${scopedVersionAxis === "trim" ? "Trim" : "Version"} 分布` : "Scoped Version / Trim 分布"}
          data={scopedVersionMix}
          compact={compact}
        />,
      );
    }
    if (priceScatter.length > 0) {
      workbenchCards.push(
        <ScatterCard
          key="price-scatter"
          title="价格 / 尺寸分布"
          points={priceScatter}
          compact={compact}
          xLabel="Length (mm)"
          yLabel="MSRP"
        />,
      );
    }
    if (bubbleScatter.length > 0) {
      workbenchCards.push(
        <ScatterCard
          key="powertrain-bubble"
          title="动力总成气泡图"
          points={bubbleScatter}
          compact={compact}
          xLabel="Length"
          yLabel="MSRP"
        />,
      );
    }
    if (powertrainVsPrice) {
      workbenchCards.push(
        <StackedBarCard
          key="pt-price"
          title="动力 × 价格带"
          dataset={powertrainVsPrice}
          compact={compact}
        />,
      );
    }
    if (tcoScatter.length > 0) {
      workbenchCards.push(
        <ScatterCard
          key="tco"
          title="估算 TCO 散点图"
          points={tcoScatter}
          compact={compact}
          xLabel="MSRP"
          yLabel="Estimated TCO"
        />,
      );
    }

    if (topBrands.length > 0) {
      marketCards.push(<RankBarCard key="brands" title="品牌销量排名" data={topBrands} compact={compact} />);
    }
    if (topModels.length > 0) {
      marketCards.push(<RankBarCard key="models" title="车型销量排名" data={topModels} compact={compact} />);
    }
    if (powertrainMix.length > 0) {
      marketCards.push(
        <RankBarCard
          key="powertrain"
          title="动力总成结构"
          data={powertrainMix}
          compact={compact}
          usePowertrainColors
        />,
      );
    }
    if (ytdRank.length > 0) {
      marketCards.push(<RankBarCard key="ytd" title="YTD 品牌排名" data={ytdRank} compact={compact} />);
    }
    if (monthlyRank.length > 0) {
      marketCards.push(<RankBarCard key="monthly" title="月度品牌排名" data={monthlyRank} compact={compact} />);
    }
    if (bevShare.length > 0) {
      marketCards.push(<RankBarCard key="bev-share" title="BEV 渗透细分" data={bevShare} compact={compact} />);
    }
    if (nevRange.length > 0) {
      marketCards.push(<RankBarCard key="nev-range" title="NEV 续航分布" data={nevRange} compact={compact} />);
    }
    if (segmentByLength) {
      marketCards.push(
        <StackedBarCard
          key="segment-length"
          title="细分 × 车长带"
          dataset={segmentByLength}
          compact={compact}
        />,
      );
    }
    if (segmentMatrix) {
      marketCards.push(<MatrixCard key="segment-matrix" title="细分市场矩阵" matrix={segmentMatrix} />);
    }
    if (originMatrix) {
      marketCards.push(<MatrixCard key="origin-matrix" title="车系阵营矩阵" matrix={originMatrix} />);
    }

    if (newsDigest) {
      intelligenceCards.push(
        <NewsDigestCard key="news-digest" digest={newsDigest} />,
      );
    }
    if (marketEvents.length > 0) {
      intelligenceCards.push(
        <NewsTimelineCard
          key="market-events"
          events={marketEvents}
          compact={compact}
        />,
      );
    }

    if (monthSeries.length > 1) {
      trendCards.push(<TrendCard key="month" title="月度销量趋势" data={monthSeries} compact={compact} />);
    }
    if (yearSeries.length > 1) {
      trendCards.push(<TrendCard key="year" title="年度销量趋势" data={yearSeries} compact={compact} />);
    }
    if (shareTrend.length > 1) {
      trendCards.push(<ShareTrendCard key="share-trend" data={shareTrend} compact={compact} />);
    }
    if (migration) {
      trendCards.push(<MigrationCard key="migration" dataset={migration} compact={compact} />);
    }
    if (heatmap) {
      trendCards.push(<HeatmapCard key="heatmap" dataset={heatmap} />);
    }

    return [
      {
        id: "workbench",
        title: "PM 工作台",
        subtitle: "先看当前年份的定位、价格带、版本气泡与经营成本。",
        cards: workbenchCards,
        emptyReason: {
          title: "当前工作台还没有可画图的数据",
          detail: `缺失项：${joinLabels(workbenchMissing)}。这里不一定和 Dashboard 100% 同步，因为助手拿的是问题驱动的 snapshot；像 MSRP、版本、TCO 这些图还依赖额外 enrich 和价格补全数据。`,
        },
      },
      {
        id: "market",
        title: "市场结构",
        subtitle: "品牌、车型、动力与细分结构放在同一视角里比较。",
        cards: marketCards,
        emptyReason: {
          title: "当前市场结构区没有可展示结果",
          detail: `缺失项：${joinLabels(marketMissing)}。通常是当前国家快照没有返回对应聚合，或者筛选条件把结果压空了。`,
        },
      },
      {
        id: "intelligence",
        title: "市场情报",
        subtitle: "把预抓取新闻、政策事件和 Gemini 摘要直接并入助手视图。",
        cards: intelligenceCards,
        emptyReason: {
          title: "当前没有可展示的新闻快照",
          detail: `缺失项：${joinLabels(intelligenceMissing)}。需要先运行 sync_country_news_digest.py 预抓取任务；问答链路默认不会实时抓外网。`,
        },
      },
      {
        id: "trend",
        title: "趋势与温度",
        subtitle: "保留时间维度，用来判断趋势延续性与季节性波动。",
        cards: trendCards,
        emptyReason: {
          title: "当前趋势区缺少足够时间序列",
          detail: `缺失项：${joinLabels(trendMissing)}。趋势图至少需要 2 个时间点；热力图和迁移图也要求对应数据集已返回。`,
        },
      },
    ];
  }, [compact, deckMarketEvents, deckNewsDigest, scopeModel, snapshot]);

  const scopedSections = useMemo(
    () => sections.filter((section) => deckScope.visibleLenses.includes(section.id as CountryChatDeckLens)),
    [deckScope.visibleLenses, sections],
  );
  const scopedViewCount = useMemo(
    () => scopedSections.reduce((sum, section) => sum + section.cards.length, 0),
    [scopedSections],
  );
  const visibleSections = useMemo(() => {
    if (activeLens === "all" && deckScope.visibleLenses.includes("all")) {
      return scopedSections;
    }
    const fallbackLens = deckScope.visibleLenses.includes(activeLens)
      ? activeLens
      : deckScope.defaultLens;
    return scopedSections.filter((section) => section.id === fallbackLens);
  }, [activeLens, deckScope.defaultLens, deckScope.visibleLenses, scopedSections]);

  if (!snapshot) {
    return null;
  }

  return (
    <div className={`copilot-analysis-deck${compact ? " is-compact" : ""}`}>
      <button
        type="button"
        className={`copilot-analysis-toggle${expanded ? " is-open" : ""}`}
        onClick={() => setExpanded((current) => !current)}
      >
        <span>{expanded ? "收起完整分析" : `展开完整分析 · ${Math.max(scopedViewCount || availableViews, 1)} 图`}</span>
        <strong>{expanded ? "−" : "+"}</strong>
      </button>
      {expanded ? (
        <div className="copilot-analysis-panel">
          <div className="copilot-analysis-kpis">
            <div className="copilot-analysis-kpi">
              <span>Country</span>
              <strong>{snapshot.country}</strong>
            </div>
            <div className="copilot-analysis-kpi">
              <span>Period</span>
              <strong>{snapshot.periodLabel || snapshot.resolvedPeriod || "Live"}</strong>
            </div>
            <div className="copilot-analysis-kpi">
              <span>Scope Year</span>
              <strong>{scopeYear ? String(scopeYear) : "Latest"}</strong>
            </div>
            <div className="copilot-analysis-kpi">
              <span>Model Lens</span>
              <strong>{scopeModel || "Top-selling model"}</strong>
            </div>
            {snapshot.originAnalysis?.summaryText ? (
              <div className="copilot-analysis-kpi is-wide">
                <span>Origin Readout</span>
                <strong>{snapshot.originAnalysis.summaryText}</strong>
              </div>
            ) : null}
            {snapshot.overviewSummary && isRecord(snapshot.overviewSummary) ? (
              <div className="copilot-analysis-kpi is-wide">
                <span>Market Pulse</span>
                <strong>
                  {[
                    `当月 ${formatMetric(toNumber(snapshot.overviewSummary.currentMonthVolume) ?? undefined)}`,
                    `YoY ${formatPercent(toNumber(snapshot.overviewSummary.currentMonthYoY))}`,
                    `YTD ${formatMetric(toNumber(snapshot.overviewSummary.ytdVolume) ?? undefined)}`,
                  ].join(" · ")}
                </strong>
              </div>
            ) : null}
            {deckNewsDigest ? (
              <div className="copilot-analysis-kpi is-wide">
                <span>News Pulse</span>
                <strong>
                  {[
                    deckNewsDigest.headline || deckNewsDigest.summary || `${deckMarketEvents.length} 条新闻快照`,
                    newsProviderLabel(deckNewsDigest.summaryProvider),
                    deckNewsDigest.stale ? "缓存偏旧" : null,
                  ].filter(Boolean).join(" · ")}
                </strong>
              </div>
            ) : null}
          </div>

          <div className="copilot-analysis-controls">
            <label className="copilot-analysis-control">
              <span>年份</span>
              <select
                value={scopeYear ? String(scopeYear) : ""}
                onChange={(event) => {
                  const next = toNumber(event.target.value);
                  setSelectedYear(next ?? undefined);
                }}
              >
                {availableYears.map((year) => (
                  <option key={year} value={String(year)}>{year}</option>
                ))}
              </select>
            </label>
            <label className="copilot-analysis-control is-wide">
              <span>Model Version Bubble</span>
              <select
                value={scopeModel}
                onChange={(event) => setSelectedModel(event.target.value)}
              >
                {!scopeModel ? <option value="">自动选择</option> : null}
                {availableModels.map((model) => (
                  <option key={model} value={model}>{model}</option>
                ))}
              </select>
            </label>
            <label className="copilot-analysis-control">
              <span>Version Top N</span>
              <input
                type="number"
                min={8}
                max={60}
                value={scopeTopN}
                onChange={(event) => setModelTopN(clampModelTopN(toNumber(event.target.value)))}
              />
            </label>
            <div className="copilot-analysis-control is-wide">
              <span>视图焦点</span>
              <div className="copilot-analysis-lenses">
                {[
                  { id: "all", label: "全部" },
                  { id: "workbench", label: "PM 工作台" },
                  { id: "market", label: "市场结构" },
                  { id: "intelligence", label: "市场情报" },
                  { id: "trend", label: "趋势监控" },
                ].filter((item) => deckScope.visibleLenses.includes(item.id as CountryChatDeckLens)).map((item) => (
                  <button
                    key={item.id}
                    type="button"
                    className={`copilot-analysis-lens${activeLens === item.id ? " is-active" : ""}`}
                    onClick={() => setActiveLens(item.id as CountryChatDeckLens)}
                  >
                    {item.label}
                  </button>
                ))}
              </div>
            </div>
          </div>

          <div className="copilot-analysis-scope-note">
            {meta.yearLockedByQuestion
              ? `当前问题锁定到 ${scopeYear ? `${scopeYear} 年` : "指定时间范围"}，你也可以手动切换。`
              : (meta.defaultLatestYearApplied
                ? `当前默认使用最新可用年份 ${scopeYear ?? ""}。`
                : "当前图表按完整可用数据展示。")}
          </div>

          {loading ? (
            <LoadingSurface
              mode="inline"
              kicker="Assistant Deck"
              label="正在补齐完整图表数据"
              detail="会按国家与当前问题参数补全趋势、结构、定位与价格图层。"
            />
          ) : null}
          {loadError ? <div className="copilot-analysis-error">{loadError}</div> : null}

          <div className="copilot-analysis-sections">
            {visibleSections.map((section) => (
              <DeckSection
                key={section.id}
                title={section.title}
                subtitle={section.subtitle}
                cards={section.cards}
                emptyReason={section.emptyReason}
              />
            ))}
          </div>

          {!loading && availableViews === 0 ? (
            <div className="copilot-analysis-empty">当前消息还没有可展示的图表数据。</div>
          ) : null}
        </div>
      ) : null}
    </div>
  );
}
