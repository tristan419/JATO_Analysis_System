import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useSearchParams } from "react-router-dom";
import type { Data, Layout as PlotlyLayout } from "plotly.js";

import { api } from "../api/client";
import { CollapsibleDeckHero } from "../components/CollapsibleDeckHero";
import { DEFAULT_EXPORT, ExportPanel, buildExportLabelModeOptions, type ExportSettings } from "../components/ExportPanel";
import { LazyPlotlyChart as PlotlyChart, preloadPlotlyChartRuntime } from "../components/LazyPlotlyChart";
import { LoadingSurface } from "../components/LoadingSurface";
import { SERIES_COLORS, fuelColor, originColor } from "../utils/colors";
import { TRANSPARENT_CHART_LAYOUT as CHART_LAYOUT } from "../utils/plotlyDefaults";
import type {
  MarketScanBodyShareTrendItem,
  MarketScanDeckResponse,
  MarketScanDrilldownPage,
  MarketScanFuelPanel,
  MarketScanFuelTrendItem,
  MarketScanOriginBrandGroup,
  MarketScanMatrix,
  MarketScanMatrixRow,
  MarketScanOverviewPage,
  MarketScanOverviewTrendItem,
  MarketScanPageKey,
  MarketScanRankingGroup,
  MarketScanRankingItem,
  MarketScanSegmentPage,
  MarketScanSuvSegmentShareTrendItem,
} from "../types";

const DEFAULT_FUEL_TYPES = ["ICE", "MHEV", "HEV", "PHEV", "BEV", "LPG"];
const DEFAULT_MARKET_SCAN_COUNTRY = "瑞典";
const TAB_ITEMS: Array<{
  key: MarketScanPageKey;
  code: string;
  label: string;
  sublabel: string;
}> = [
  { key: "overview", code: "01", label: "Overview", sublabel: "市场总量" },
  { key: "origin", code: "02", label: "Origin", sublabel: "车系走势" },
  { key: "segment", code: "03", label: "Segment", sublabel: "级别结构" },
  { key: "drilldown", code: "04", label: "Drilldown", sublabel: "细分下钻" },
  { key: "suvA", code: "05", label: "SUV-A", sublabel: "A级 SUV" },
  { key: "suvB", code: "06", label: "SUV-B", sublabel: "B级 SUV" },
];

const DEFAULT_MARKET_SCAN_EXPORT: ExportSettings = {
  ...DEFAULT_EXPORT,
  exportWidth: 1920,
  exportHeight: 1080,
  dataLabelMode: "value",
  dataLabelPosition: "top",
  decimalPlaces: 1,
};
const MIN_MARKET_SCAN_RANKING_LIMIT = 10;
const MARKET_SCAN_RANKING_LIMIT_OPTIONS = [10, 15, 20, 30] as const;
const SUV_SEGMENT_SHARE_ORDER = ["SUV-A00", "SUV-A0", "SUV-A", "≥SUV-B"] as const;
const SUV_SEGMENT_SHARE_META: Record<(typeof SUV_SEGMENT_SHARE_ORDER)[number], { label: string; color: string }> = {
  "SUV-A00": { label: "SUV-A00", color: "#0f766e" },
  "SUV-A0": { label: "SUV-A0", color: "#14b8a6" },
  "SUV-A": { label: "SUV-A", color: "#84cc16" },
  "≥SUV-B": { label: "≥SUV-B", color: "#f59e0b" },
};

function normalizeMarketScanRankingLimit(value: number | string | null | undefined): number {
  const numericValue = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(numericValue)) {
    return MIN_MARKET_SCAN_RANKING_LIMIT;
  }
  return Math.max(MIN_MARKET_SCAN_RANKING_LIMIT, Math.round(numericValue));
}

interface HeroMetric {
  label: string;
  value: string;
  detail: string;
  tone?: string;
}

interface PanelProps {
  eyebrow?: string;
  title: string;
  subtitle?: string;
  children: ReactNode;
  actions?: ReactNode;
}

function MarketScanDeckSkeleton() {
  return (
    <section className="market-scan-state-card market-scan-state-card--skeleton" aria-hidden="true">
      <LoadingSurface
        mode="inline"
        kicker="Deck"
        label="正在生成市场扫描页面"
        detail="后端会按国家、月份和燃料组合动态聚合 Parquet 数据。"
      />
      <div className="market-scan-skeleton-hero">
        <div className="market-scan-skeleton-copy">
          <span className="market-scan-skeleton-block market-scan-skeleton-block--eyebrow" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--title" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--body" />
        </div>
        <div className="market-scan-skeleton-chip-row">
          {Array.from({ length: 4 }, (_, index) => (
            <span
              key={`skeleton-chip-${index}`}
              className="market-scan-skeleton-block market-scan-skeleton-block--chip"
            />
          ))}
        </div>
      </div>
      <div className="market-scan-skeleton-tabs">
        {Array.from({ length: 6 }, (_, index) => (
          <span
            key={`skeleton-tab-${index}`}
            className="market-scan-skeleton-block market-scan-skeleton-block--tab"
          />
        ))}
      </div>
      <div className="market-scan-skeleton-grid market-scan-skeleton-grid--metrics">
        {Array.from({ length: 4 }, (_, index) => (
          <div key={`skeleton-metric-${index}`} className="market-scan-skeleton-panel">
            <span className="market-scan-skeleton-block market-scan-skeleton-block--metric-label" />
            <span className="market-scan-skeleton-block market-scan-skeleton-block--metric-value" />
            <span className="market-scan-skeleton-block market-scan-skeleton-block--metric-detail" />
          </div>
        ))}
      </div>
      <div className="market-scan-skeleton-grid market-scan-skeleton-grid--content">
        <div className="market-scan-skeleton-panel market-scan-skeleton-panel--wide">
          <span className="market-scan-skeleton-block market-scan-skeleton-block--panel-title" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--chart" />
        </div>
        <div className="market-scan-skeleton-panel market-scan-skeleton-panel--stack">
          <span className="market-scan-skeleton-block market-scan-skeleton-block--panel-title" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
          <span className="market-scan-skeleton-block market-scan-skeleton-block--list-row" />
        </div>
      </div>
    </section>
  );
}

function formatVolume(value: number | null | undefined): string {
  return Number(value ?? 0).toLocaleString("en-US");
}

function formatPercent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "-";
  }
  return `${(value * 100).toFixed(digits)}%`;
}

function toneClassName(tone?: string): string {
  if (!tone) {
    return "is-neutral";
  }
  if (tone === "positive") {
    return "is-positive";
  }
  if (tone === "negative") {
    return "is-negative";
  }
  if (tone === "new") {
    return "is-new";
  }
  return "is-neutral";
}

function rankingItemLabel(item: MarketScanRankingItem): string {
  return item.brand ?? item.model ?? "-";
}

function topCell(row?: MarketScanMatrixRow): { key: string; value: number | null } | null {
  if (!row || row.cells.length === 0) {
    return null;
  }
  return row.cells.reduce<{ key: string; value: number | null } | null>((winner, cell) => {
    if (!winner || (cell.value ?? 0) > (winner.value ?? 0)) {
      return { key: cell.key, value: cell.value };
    }
    return winner;
  }, null);
}

function matrixRow(matrix: MarketScanMatrix, metricKey: string): MarketScanMatrixRow | undefined {
  return matrix.rows.find((row) => row.metricKey === metricKey);
}

function buildHeroMetrics(deck: MarketScanDeckResponse, pageKey: MarketScanPageKey): HeroMetric[] {
  if (pageKey === "overview") {
    const { summary } = deck.results.overview;
    return [
      {
        label: deck.metadata.labels.currentMonthShort,
        value: formatVolume(summary.currentMonthVolume),
        detail: "当月销量",
      },
      {
        label: "YoY",
        value: summary.currentMonthYoY.display,
        detail: "当月同比",
        tone: summary.currentMonthYoY.tone,
      },
      {
        label: deck.metadata.labels.currentYtd,
        value: formatVolume(summary.ytdVolume),
        detail: `${deck.metadata.labels.ytdWindow} 累计`,
      },
      {
        label: "YTD YoY",
        value: summary.ytdYoY.display,
        detail: "累计同比",
        tone: summary.ytdYoY.tone,
      },
    ];
  }

  if (pageKey === "origin") {
    const currentRow = matrixRow(deck.results.origin.matrix, "current_volume");
    const leader = topCell(currentRow);
    return [
      {
        label: deck.metadata.labels.currentMonthShort,
        value: formatVolume(
          currentRow?.cells.reduce((sum, cell) => sum + Number(cell.value ?? 0), 0) ?? 0,
        ),
        detail: "车系总量",
      },
      {
        label: "Leading Origin",
        value: leader?.key ?? "-",
        detail: leader ? `${formatVolume(leader.value)} 台` : "暂无数据",
      },
      {
        label: "Tracked Series",
        value: String(deck.results.origin.trend.series.length),
        detail: "纳入走势的车系数量",
      },
    ];
  }

  if (pageKey === "segment") {
    const currentRow = matrixRow(deck.results.segment.matrix, "current_volume");
    const leader = topCell(currentRow);
    const lastPoint = deck.results.segment.bodyShareTrend.items[
      deck.results.segment.bodyShareTrend.items.length - 1
    ];
    return [
      {
        label: deck.metadata.labels.currentMonthShort,
        value: formatPercent(lastPoint?.suvSharePct ?? null),
        detail: "SUV 占比",
      },
      {
        label: "Sedan Share",
        value: formatPercent(lastPoint?.sedanSharePct ?? null),
        detail: "轿车占比",
      },
      {
        label: "Top Bucket",
        value: leader?.key ?? "-",
        detail: leader ? `${formatVolume(leader.value)} 台` : "暂无数据",
      },
    ];
  }

  const drilldown = deck.results[pageKey] as MarketScanDrilldownPage;
  const leader = drilldown.totalRanking.items[0];
  const lastYtd = drilldown.ytdFuelTrend.items[drilldown.ytdFuelTrend.items.length - 1];
  return [
    {
      label: drilldown.segmentLabel,
      value: leader ? rankingItemLabel(leader) : "-",
      detail: leader ? `榜首 ${formatVolume(leader.volume)} 台` : "暂无榜首车型",
    },
    {
      label: "Leader Share",
      value: leader ? formatPercent(leader.sharePct) : "-",
      detail: "榜首份额",
    },
    {
      label: "YTD Window",
      value: lastYtd?.label ?? deck.metadata.labels.currentYtd,
      detail: lastYtd ? `${formatVolume(lastYtd.totalVolume)} 台` : "暂无累计趋势",
    },
  ];
}

function pageNarrative(deck: MarketScanDeckResponse, pageKey: MarketScanPageKey): string {
  if (pageKey === "overview") {
    return deck.results.overview.summary.subheadline;
  }
  if (pageKey === "origin") {
    return deck.results.origin.summaryText;
  }
  if (pageKey === "segment") {
    return deck.results.segment.summaryText;
  }
  return (deck.results[pageKey] as MarketScanDrilldownPage).summaryText;
}

function buildSparseText(
  pointCount: number,
  render: (index: number) => string,
  stride = 4,
): string[] {
  return Array.from({ length: pointCount }, (_, index) => {
    if (index === pointCount - 1 || index === 0 || (index + 1) % stride === 0) {
      return render(index);
    }
    return "";
  });
}

function buildLastPointText(
  pointCount: number,
  render: (index: number) => string,
): string[] {
  return Array.from({ length: pointCount }, (_, index) => (
    index === pointCount - 1 ? render(index) : ""
  ));
}

function sanitizeFileNameSegment(value: string): string {
  return value.trim().replace(/[^a-zA-Z0-9\u4e00-\u9fff]+/g, "-").replace(/^-+|-+$/g, "") || "slide";
}

function shiftMonthPeriod(period: string, deltaMonths: number): string {
  const [yearText, monthText] = period.split("-");
  const year = Number(yearText);
  const month = Number(monthText);
  if (!Number.isFinite(year) || !Number.isFinite(month)) {
    return period;
  }
  const shifted = new Date(Date.UTC(year, month - 1 + deltaMonths, 1));
  return `${shifted.getUTCFullYear()}-${String(shifted.getUTCMonth() + 1).padStart(2, "0")}`;
}

function buildOverviewTrendData(
  items: MarketScanOverviewTrendItem[],
  fuelOrder: string[],
  showDataLabels: boolean,
): Data[] {
  const ordered = [...items].sort((left, right) => left.period.localeCompare(right.period));
  const trailingItems = ordered.slice(-12);
  if (trailingItems.length === 0) {
    return [];
  }

  const itemByPeriod = new Map(ordered.map((item) => [item.period, item]));
  const labels = trailingItems.map((item) => item.label);
  const currentYear = trailingItems[trailingItems.length - 1]?.period.slice(0, 4) ?? "Current";
  const priorYear = String(Number(currentYear) - 1);
  const priorItems = trailingItems.map((item) => itemByPeriod.get(shiftMonthPeriod(item.period, -12)));

  const traces: Data[] = [];
  fuelOrder.forEach((fuel) => {
    traces.push({
      type: "bar",
      name: `${currentYear} ${fuel}`,
      legendgroup: fuel,
      offsetgroup: currentYear,
      x: labels,
      y: trailingItems.map((item) => item.fuelMix[fuel] ?? 0),
      marker: { color: fuelColor(fuel) },
      hovertemplate: `%{x}<br>${currentYear} ${fuel}: %{y:,.0f} 台<extra></extra>`,
    } as Data);
    traces.push({
      type: "bar",
      name: `${priorYear} ${fuel}`,
      legendgroup: fuel,
      offsetgroup: priorYear,
      showlegend: false,
      opacity: 0.34,
      x: labels,
      y: priorItems.map((item) => item?.fuelMix[fuel] ?? 0),
      marker: { color: fuelColor(fuel) },
      hovertemplate: `%{x}<br>${priorYear} ${fuel}: %{y:,.0f} 台<extra></extra>`,
    } as Data);
  });

  traces.push({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: `${currentYear} Total`,
    x: labels,
    y: trailingItems.map((item) => item.totalVolume),
    line: { color: "#0f172a", width: 2.8 },
    marker: { color: "#0f172a", size: 6 },
    text: showDataLabels
      ? buildSparseText(trailingItems.length, (index) => formatVolume(trailingItems[index].totalVolume), 3)
      : undefined,
    textposition: "top center",
    textfont: { size: 10, color: "#0f172a" },
    hovertemplate: `%{x}<br>${currentYear} Total: %{y:,.0f} 台<extra></extra>`,
  });
  traces.push({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: `${priorYear} Total`,
    x: labels,
    y: priorItems.map((item) => item?.totalVolume ?? 0),
    line: { color: "#64748b", width: 2, dash: "dot" },
    marker: { color: "#64748b", size: 5 },
    text: showDataLabels
      ? buildSparseText(priorItems.length, (index) => formatVolume(priorItems[index]?.totalVolume ?? 0), 3)
      : undefined,
    textposition: "bottom center",
    textfont: { size: 9, color: "#64748b" },
    hovertemplate: `%{x}<br>${priorYear} Total: %{y:,.0f} 台<extra></extra>`,
  });

  return traces;
}

function buildOriginTrendData(
  series: MarketScanDeckResponse["results"]["origin"]["trend"]["series"],
  showDataLabels: boolean,
): Data[] {
  return series.map((entry) => ({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: entry.origin,
    x: entry.points.map((point) => point.label),
    y: entry.points.map((point) => point.volume),
    line: { color: originColor(entry.origin), width: 2.4 },
    marker: { color: originColor(entry.origin), size: 5 },
    text: showDataLabels
      ? buildLastPointText(
          entry.points.length,
          (index) => `${entry.origin} ${formatVolume(entry.points[index].volume)}`,
        )
      : undefined,
    textposition: "middle right",
    textfont: { size: 10 },
    hovertemplate: `%{x}<br>${entry.origin}: %{y:,.0f} 台<extra></extra>`,
  }));
}

function buildOriginBrandTrendData(
  group: MarketScanOriginBrandGroup,
  showDataLabels: boolean,
): Data[] {
  return group.series.map((entry, index) => ({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines+markers",
    name: entry.brand,
    x: entry.points.map((point) => point.label),
    y: entry.points.map((point) => point.volume),
    line: { color: SERIES_COLORS[index % SERIES_COLORS.length], width: 2.2 },
    marker: { color: SERIES_COLORS[index % SERIES_COLORS.length], size: 4 },
    text: showDataLabels
      ? buildLastPointText(
          entry.points.length,
          (pointIndex) => `${entry.brand} ${formatVolume(entry.points[pointIndex]?.volume ?? 0)}`,
        )
      : undefined,
    textposition: "middle right",
    textfont: { size: 9 },
    hovertemplate: `%{x}<br>${group.origin} · ${entry.brand}: %{y:,.0f} 台<extra></extra>`,
  }));
}

function buildBodyShareData(
  items: MarketScanBodyShareTrendItem[],
  showDataLabels: boolean,
  labelDigits = 1,
): Data[] {
  const labels = items.map((item) => item.label);
  return [
    {
      type: "scatter",
      mode: showDataLabels ? "text+lines+markers" : "lines",
      stackgroup: "share",
      fill: "tonexty",
      name: "SUV",
      x: labels,
      y: items.map((item) => item.suvSharePct),
      line: { color: "#0f766e", width: 2.2 },
      marker: { color: "#0f766e", size: 4 },
      text: showDataLabels
        ? items.map((item) => formatPercent(item.suvSharePct, labelDigits))
        : undefined,
      textposition: "top center",
      textfont: { size: 9 },
      hovertemplate: "%{x}<br>SUV: %{y:.1%}<extra></extra>",
    },
    {
      type: "scatter",
      mode: showDataLabels ? "text+lines+markers" : "lines",
      stackgroup: "share",
      fill: "tonexty",
      name: "Sedan",
      x: labels,
      y: items.map((item) => item.sedanSharePct),
      line: { color: "#b45309", width: 2.2 },
      marker: { color: "#b45309", size: 4 },
      text: showDataLabels
        ? items.map((item) => formatPercent(item.sedanSharePct, labelDigits))
        : undefined,
      textposition: "bottom center",
      textfont: { size: 9 },
      hovertemplate: "%{x}<br>Sedan: %{y:.1%}<extra></extra>",
    },
  ];
}

function buildSuvSegmentShareData(
  items: MarketScanSuvSegmentShareTrendItem[],
  showDataLabels: boolean,
  labelDigits = 1,
): Data[] {
  const labels = items.map((item) => item.label);
  return SUV_SEGMENT_SHARE_ORDER.map((segment) => ({
    type: "scatter",
    mode: showDataLabels ? "text+lines+markers" : "lines",
    stackgroup: "suv-segment-share",
    fill: "tonexty",
    name: SUV_SEGMENT_SHARE_META[segment].label,
    x: labels,
    y: items.map((item) => item.segmentSharePct[segment] ?? 0),
    line: { color: SUV_SEGMENT_SHARE_META[segment].color, width: 2.2 },
    marker: { color: SUV_SEGMENT_SHARE_META[segment].color, size: 4 },
    text: showDataLabels
      ? buildLastPointText(
          items.length,
          (index) => formatPercent(items[index]?.segmentSharePct[segment] ?? 0, labelDigits),
        )
      : undefined,
    textposition: "middle right",
    textfont: { size: 9 },
    hovertemplate: `%{x}<br>${SUV_SEGMENT_SHARE_META[segment].label}: %{y:.1%}<extra></extra>`,
  }));
}

function buildFuelTrendData(
  items: MarketScanFuelTrendItem[],
  fuelOrder: string[],
  showDataLabels: boolean,
): Data[] {
  const labels = items.map((item) => item.label);
  const traces: Data[] = fuelOrder.map((fuel) => ({
    type: "bar",
    name: fuel,
    x: labels,
    y: items.map((item) => item.fuelMix[fuel] ?? 0),
    marker: { color: fuelColor(fuel) },
    hovertemplate: `%{x}<br>${fuel}: %{y:,.0f} 台<extra></extra>`,
  }));
  if (showDataLabels) {
    traces.push({
      type: "scatter",
      mode: "text",
      name: "Total Labels",
      x: labels,
      y: items.map((item) => item.totalVolume),
      text: items.map((item) => formatVolume(item.totalVolume)),
      textposition: "top center",
      textfont: { size: 10, color: "#0f172a" },
      hoverinfo: "skip",
      showlegend: false,
    });
  }
  return traces;
}

function dominantFuelForRanking(item: MarketScanRankingItem): string {
  const entries = Object.entries(item.fuelMix ?? {});
  if (entries.length === 0) {
    return "ICE";
  }
  entries.sort((left, right) => Number(right[1]) - Number(left[1]));
  return entries[0]?.[0] ?? "ICE";
}

function driveShareText(item: MarketScanRankingItem): string {
  return `4WD ${driveShareDisplay(item)}`;
}

function driveSharePct(item: MarketScanRankingItem): number {
  const pct =
    typeof item.driveSharePct === "number"
      ? item.driveSharePct
      : Number(item.driveMix?.["4WD"] ?? 0) / Math.max(item.volume || 1, 1);
  return Number.isFinite(pct) ? Math.max(0, pct) : 0;
}

function driveShareDisplay(item: MarketScanRankingItem): string {
  return item.driveShareDisplay ?? formatPercent(driveSharePct(item));
}

function marketShareLabel(item: MarketScanRankingItem): string {
  return `MS ${item.shareDisplay ?? formatPercent(item.sharePct)}`;
}

const STACKED_FUEL_ORDER = ["ICE", "MHEV", "HEV", "PHEV", "BEV", "LPG"];

function buildTotalRankingChartData(items: MarketScanRankingItem[]): Data[] {
  const ordered = [...items].reverse();
  const labels = ordered.map((item) => rankingItemLabel(item));
  const totalVolumeInSegment = items.reduce((sum, item) => sum + (item.volume || 0), 0) || 1;

  const traces: Data[] = STACKED_FUEL_ORDER.map((fuel, fuelIdx) => {
    const volumes = ordered.map((item) => (item.fuelMix?.[fuel] ?? 0));
    const sharePcts = volumes.map((vol) => vol / totalVolumeInSegment);
    const isLastFuel = fuelIdx === STACKED_FUEL_ORDER.length - 1;
    return {
      type: "bar" as const,
      orientation: "h" as const,
      name: fuel,
      x: sharePcts,
      y: labels,
      marker: { color: fuelColor(fuel) },
      text: isLastFuel
        ? ordered.map(
            (item) =>
              `${marketShareLabel(item)} · ${formatVolume(item.volume)} · ${driveShareText(item)}`,
          )
        : ordered.map(() => ""),
      textposition: "outside" as const,
      textfont: { size: 10 },
      cliponaxis: false,
      customdata: volumes.map((vol, i) => [vol, ordered[i]?.volume ?? 0]),
      hovertemplate: `%{y}<br>${fuel}: %{customdata[0]:,.0f} 台<br>总销量 %{customdata[1]:,.0f} 台<extra></extra>`,
    };
  });

  return traces;
}

function Panel({ eyebrow, title, subtitle, children, actions }: PanelProps) {
  return (
    <section className="market-scan-panel">
      <header className="market-scan-panel-head">
        <div>
          {eyebrow ? <span className="market-scan-panel-eyebrow">{eyebrow}</span> : null}
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
        {actions ? <div className="market-scan-panel-actions">{actions}</div> : null}
      </header>
      <div className="market-scan-panel-body">{children}</div>
    </section>
  );
}

function MetricCard({
  label,
  value,
  detail,
  tone,
}: {
  label: string;
  value: string;
  detail: string;
  tone?: string;
}) {
  return (
    <div className={`market-scan-metric-card ${toneClassName(tone)}`}>
      <span className="market-scan-metric-label">{label}</span>
      <strong className="market-scan-metric-value">{value}</strong>
      <span className="market-scan-metric-detail">{detail}</span>
    </div>
  );
}

function RankingGroup({
  group,
  compact = false,
  fuelType,
}: {
  group: MarketScanRankingGroup;
  compact?: boolean;
  fuelType?: string;
}) {
  if (group.items.length === 0) {
    return <div className="market-scan-empty">暂无排行数据。</div>;
  }

  const barColor = fuelType ? fuelColor(fuelType) : "#0f766e";
  const isFuelRanking = Boolean(fuelType);

  if (compact) {
    return (
      <div
        className={`market-scan-ranking-list market-scan-ranking-scrollable${isFuelRanking ? " market-scan-ranking-list--fuel" : ""}`}
      >
        {group.items.map((item) => {
          const currentDriveSharePct = driveSharePct(item);
          const hasDriveShare = typeof item.driveSharePct === "number" || item.driveMix?.["4WD"] !== undefined;
          const driveShareLabel = driveShareDisplay(item);
          const shareLabel = marketShareLabel(item);

          return (
            <article
              key={`${rankingItemLabel(item)}-${item.rank}`}
              className={`market-scan-ranking-row${isFuelRanking ? " market-scan-ranking-row--fuel" : ""}`}
            >
              <div className="market-scan-ranking-row-rank">{String(item.rank).padStart(2, "0")}</div>
              <div className="market-scan-ranking-row-info">
                <div className="market-scan-ranking-row-head">
                  <span className="market-scan-ranking-row-name">{rankingItemLabel(item)}</span>
                  <div className="market-scan-ranking-row-nums">
                    <span>{formatVolume(item.volume)}</span>
                    <span className="market-scan-tag">{shareLabel}</span>
                  </div>
                </div>
                <div
                  className="market-scan-ranking-row-bar"
                  title={hasDriveShare ? `${shareLabel} · 4WD ${driveShareLabel}` : shareLabel}
                >
                  <span
                    className="market-scan-ranking-row-bar-fill"
                    style={{ width: `${Math.max(1, item.barPct * 100)}%`, background: barColor }}
                  >
                    {hasDriveShare ? (
                      <span
                        className="market-scan-4wd-fill"
                        style={{ width: `${currentDriveSharePct * 100}%` }}
                      />
                    ) : null}
                  </span>
                </div>
              </div>
              <div className="market-scan-ranking-row-side">
                <span className={`market-scan-tone-text ${toneClassName(item.yoy.tone)}`}>
                  YoY {item.yoy.tone === "positive" ? "▲ " : item.yoy.tone === "negative" ? "▼ " : ""}
                  {item.yoy.display}
                </span>
                {item.mom ? (
                  <span className={`market-scan-tone-text ${toneClassName(item.mom.tone)}`}>
                    MoM {item.mom.tone === "positive" ? "▲ " : item.mom.tone === "negative" ? "▼ " : ""}
                    {item.mom.display}
                  </span>
                ) : null}
                {hasDriveShare ? (
                  <span className="market-scan-ranking-row-drive-chip">
                    <span className="market-scan-ranking-row-drive-chip-label">4WD</span>
                    <strong className="market-scan-ranking-row-drive-chip-value">{driveShareLabel}</strong>
                  </span>
                ) : null}
              </div>
            </article>
          );
        })}
      </div>
    );
  }

  return (
    <div className="market-scan-ranking-stack market-scan-ranking-scrollable">
      {group.items.map((item) => (
        <article key={`${rankingItemLabel(item)}-${item.rank}`} className="market-scan-ranking-card">
          <div className="market-scan-ranking-main">
            <div className="market-scan-ranking-rank">{String(item.rank).padStart(2, "0")}</div>
            <div className="market-scan-ranking-copy">
              <strong>{rankingItemLabel(item)}</strong>
              <span>
                {group.currentLabel ? `${group.currentLabel} ` : ""}
                {formatVolume(item.volume)} 台
                {item.shareDisplay ? ` · ${item.shareDisplay}` : ""}
              </span>
            </div>
          </div>
          <div className="market-scan-ranking-side">
            <span className={`market-scan-tone-pill ${toneClassName(item.yoy.tone)}`}>
              YoY {item.yoy.display}
            </span>
            {item.mom ? (
              <span className={`market-scan-tone-pill ${toneClassName(item.mom.tone)}`}>
                MoM {item.mom.display}
              </span>
            ) : null}
          </div>
          <div className="market-scan-ranking-bar">
            <span style={{ width: `${Math.max(5, item.barPct * 100)}%`, background: barColor }} />
          </div>
        </article>
      ))}
    </div>
  );
}

function MatrixTable({ matrix }: { matrix: MarketScanMatrix }) {
  if (matrix.rows.length === 0) {
    return <div className="market-scan-empty">暂无矩阵数据。</div>;
  }

  return (
    <div className="market-scan-table-wrap">
      <table className="market-scan-matrix-table">
        <thead>
          <tr>
            <th>Metric</th>
            {matrix.columns.map((column) => (
              <th key={column}>{column}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {matrix.rows.map((row) => (
            <tr key={row.metricKey}>
              <th>{row.label}</th>
              {row.cells.map((cell) => (
                <td key={`${row.metricKey}-${cell.key}`} className={toneClassName(cell.tone)}>
                  {cell.display}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function OverviewSection({
  labels: _labels,
  page,
  fuelOrder,
  showDataLabels,
  compact = false,
}: {
  labels: MarketScanDeckResponse["metadata"]["labels"];
  page: MarketScanOverviewPage;
  fuelOrder: string[];
  showDataLabels: boolean;
  compact?: boolean;
}) {
  return (
    <div className="market-scan-grid market-scan-grid--three">
      <Panel
        eyebrow="Trend"
        title="Rolling 12M Volume / Powertrain"
        subtitle="当前月往前 12 个月，对照去年同期双柱，并保留动总堆叠与总量折线。"
      >
        <PlotlyChart
          data={buildOverviewTrendData(page.trend.items, fuelOrder, showDataLabels)}
          layout={{
            ...CHART_LAYOUT,
            barmode: "stack",
            xaxis: { type: "category" },
            yaxis: { title: { text: "销量" } },
          }}
          height={compact ? 292 : 430}
        />
      </Panel>

      <Panel
        eyebrow="Ranking"
        title={page.monthlyBrandRanking.title}
        subtitle={`${page.monthlyBrandRanking.currentLabel ?? "当月"} vs ${page.monthlyBrandRanking.priorLabel ?? "去年同期"}`}
      >
        <RankingGroup group={page.monthlyBrandRanking} compact={compact} />
      </Panel>

      <Panel
        eyebrow="Ranking"
        title={page.ytdBrandRanking.title}
        subtitle={`${page.ytdBrandRanking.currentLabel ?? "累计"} vs ${page.ytdBrandRanking.priorLabel ?? "去年累计"}`}
      >
        <RankingGroup group={page.ytdBrandRanking} compact={compact} />
      </Panel>
    </div>
  );
}

function OriginSection({
  page,
  showDataLabels,
  compact = false,
}: {
  page: MarketScanDeckResponse["results"]["origin"];
  showDataLabels: boolean;
  compact?: boolean;
}) {
  const trendPanel = (
    <Panel
      eyebrow="Trend"
      title="Origin Volume Trend"
      subtitle="欧系、日系、韩系、美系、中系与其他车系的月度走势。"
    >
      <PlotlyChart
        data={buildOriginTrendData(page.trend.series, showDataLabels)}
        layout={{
          ...CHART_LAYOUT,
          xaxis: { type: "category" },
          yaxis: { title: { text: "销量" } },
        }}
        height={compact ? 282 : 420}
      />
      </Panel>
  );
  const selectedBrandTrendGroups = (() => {
    const rankedOrigins = [...page.trend.series]
      .map((entry) => ({
        origin: entry.origin,
        latestVolume: entry.points[entry.points.length - 1]?.volume ?? 0,
      }))
      .sort((left, right) => right.latestVolume - left.latestVolume);
    const topOrigin = rankedOrigins[0]?.origin ?? null;
    const preferredOrigins = [topOrigin, "中系"].filter(
      (origin, index, items): origin is string => Boolean(origin) && items.indexOf(origin) === index,
    );
    const selected = preferredOrigins
      .map((origin) => page.brandTrend.groups.find((group) => group.origin === origin))
      .filter((group): group is NonNullable<typeof group> => Boolean(group));
    return selected.length > 0 ? selected : page.brandTrend.groups.slice(0, 2);
  })();
  const brandTrendPanel =
    selectedBrandTrendGroups.length > 0 ? (
      <Panel
        eyebrow="Trend"
        title="Origin Brand Trend"
        subtitle="默认展示当前第一名车系与中系品牌走势，各自保留最新月销量 Top 4 品牌。"
      >
        <div className="market-scan-grid market-scan-grid--two">
          {selectedBrandTrendGroups.map((group) => (
            <div key={group.origin} className="market-scan-subpanel">
              <h3>{group.origin}</h3>
              <PlotlyChart
                data={buildOriginBrandTrendData(group, showDataLabels)}
                layout={{
                  ...CHART_LAYOUT,
                  xaxis: { type: "category" },
                  yaxis: { title: { text: "销量" } },
                  legend: {
                    orientation: "h",
                    y: -0.22,
                    x: 0.5,
                    xanchor: "center",
                    font: { size: 10 },
                  },
                }}
                height={compact ? 220 : 260}
              />
            </div>
          ))}
        </div>
      </Panel>
    ) : null;
  const matrixPanel = (
    <Panel eyebrow="Matrix" title="Origin Scorecard" subtitle="当月、同比、累计、累计同比矩阵。">
      <MatrixTable matrix={page.matrix} />
    </Panel>
  );

  return (
    <>
      <section className="market-scan-callout">{page.summaryText}</section>
      {compact ? (
        <>
          <div className="market-scan-grid market-scan-grid--two-wide">
            {trendPanel}
            {matrixPanel}
          </div>
          {brandTrendPanel}
        </>
      ) : (
        <>
          {trendPanel}
          {brandTrendPanel}
          {matrixPanel}
        </>
      )}
    </>
  );
}

function SegmentSection({
  page,
  showDataLabels,
  labelDigits = 1,
  compact = false,
}: {
  page: MarketScanSegmentPage;
  showDataLabels: boolean;
  labelDigits?: number;
  compact?: boolean;
}) {
  return (
    <>
      <section className="market-scan-callout">{page.summaryText}</section>
      <div className="market-scan-grid market-scan-grid--two">
        <Panel
          eyebrow="Trend"
          title="SUV vs Sedan Share"
          subtitle="观察车身结构的月度切换。"
        >
          <PlotlyChart
            data={buildBodyShareData(page.bodyShareTrend.items, showDataLabels, labelDigits)}
            layout={{
              ...CHART_LAYOUT,
              xaxis: { type: "category" },
              yaxis: { title: { text: "占比" }, tickformat: ".0%", range: [0, 1] },
            }}
            height={compact ? 268 : 400}
          />
        </Panel>
        <Panel
          eyebrow="Trend"
          title="SUV Segment Share"
          subtitle="把 SUV 市占拆成 SUV-A00 / SUV-A0 / SUV-A / ≥SUV-B。"
        >
          <PlotlyChart
            data={buildSuvSegmentShareData(page.suvSegmentShareTrend.items, showDataLabels, labelDigits)}
            layout={{
              ...CHART_LAYOUT,
              xaxis: { type: "category" },
              yaxis: { title: { text: "占比" }, tickformat: ".0%", range: [0, 1] },
            }}
            height={compact ? 268 : 400}
          />
        </Panel>
      </div>
      <Panel eyebrow="Matrix" title="Segment Matrix" subtitle="不同长度级别的当月与累计表现。">
        <MatrixTable matrix={page.matrix} />
      </Panel>
    </>
  );
}

function FuelPanel({ panel, compact = false }: { panel: MarketScanFuelPanel; compact?: boolean }) {
  const dense = true;

  return (
    <Panel
      eyebrow={panel.fuelType}
      title={`${panel.fuelType} Share Ranking`}
      subtitle={panel.ytdTitle}
    >
      <RankingGroup
        group={{ title: panel.ytdTitle, currentLabel: panel.ytdTitle, items: panel.ytdRanking }}
        compact={dense || compact}
        fuelType={panel.fuelType}
      />
    </Panel>
  );
}

function DrilldownSection({
  page,
  fuelOrder,
  showDataLabels,
  compact = false,
  rankingLimit = 10,
  onRankingLimitChange,
}: {
  page: MarketScanDrilldownPage;
  fuelOrder: string[];
  showDataLabels: boolean;
  compact?: boolean;
  rankingLimit?: number;
  onRankingLimitChange?: (limit: number) => void;
}) {
  const normalizedRankingLimit = normalizeMarketScanRankingLimit(rankingLimit);

  return (
    <>
      <section className="market-scan-callout">
        <span>{page.summaryText}</span>
        {onRankingLimitChange ? (
          <label className="market-scan-ranking-limit-control">
            Top
            <select
              value={normalizedRankingLimit}
              onChange={(e) => onRankingLimitChange(normalizeMarketScanRankingLimit(e.target.value))}
            >
              {MARKET_SCAN_RANKING_LIMIT_OPTIONS.map((n) => (
                <option key={n} value={n}>{n}</option>
              ))}
            </select>
          </label>
        ) : null}
      </section>
      <div className="market-scan-grid market-scan-grid--two-wide">
        <Panel
          eyebrow="Ranking"
          title={page.totalRanking.title}
          subtitle="按当前国家与细分市场 2026 累计份额排序"
        >
          {page.totalRanking.items.length > 0 ? (
            <div className="market-scan-ranking-chart-shell">
              <PlotlyChart
                data={buildTotalRankingChartData(page.totalRanking.items)}
                layout={{
                  ...CHART_LAYOUT,
                  barmode: "stack",
                  margin: { l: 110, r: 120, t: 12, b: 28 },
                  xaxis: { title: { text: "" }, tickformat: ".0%" },
                  yaxis: { automargin: true },
                  legend: { orientation: "h", y: -0.12, x: 0.5, xanchor: "center", font: { size: 10 } },
                }}
                height={compact ? 252 : Math.max(280, page.totalRanking.items.length * 36 + 80)}
              />
            </div>
          ) : (
            <div className="market-scan-empty">暂无车型排行。</div>
          )}
        </Panel>
        <Panel
          eyebrow="Trend"
          title="YTD Fuel Trend"
          subtitle="观察同一累计窗口下各燃料路线的堆叠变化。"
        >
          <PlotlyChart
            data={buildFuelTrendData(page.ytdFuelTrend.items, fuelOrder, showDataLabels)}
            layout={{
              ...CHART_LAYOUT,
              barmode: "stack",
              xaxis: { type: "category" },
              yaxis: { title: { text: "累计销量" } },
            }}
            height={compact ? 252 : 400}
          />
        </Panel>
      </div>
      <div className="market-scan-grid market-scan-grid--five market-scan-fuel-panel-row">
        {page.fuelPanels.map((panel) => (
          <FuelPanel key={`${page.segment}-${panel.fuelType}`} panel={panel} compact={compact} />
        ))}
      </div>
    </>
  );
}

export function MarketScanPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const marketScanLabelModeOptions = useMemo(
    () => buildExportLabelModeOptions({ showValue: true, showSeries: false }),
    [],
  );
  const [deck, setDeck] = useState<MarketScanDeckResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [exportError, setExportError] = useState("");
  const [exportingSlide, setExportingSlide] = useState(false);
  const [exportSettings, setExportSettings] = useState<ExportSettings>({ ...DEFAULT_MARKET_SCAN_EXPORT });
  const [exportToolsOpen, setExportToolsOpen] = useState(false);
  const [heroCollapsed, setHeroCollapsed] = useState(false);
  const [activePage, setActivePage] = useState<MarketScanPageKey>(
    () => (searchParams.get("activePage") as MarketScanPageKey) || "overview",
  );
  const [selectedCountry, setSelectedCountry] = useState<string | null>(
    () => searchParams.get("country") || DEFAULT_MARKET_SCAN_COUNTRY,
  );
  const [selectedPeriod, setSelectedPeriod] = useState<string | null>(
    () => searchParams.get("period"),
  );
  const [selectedFuelTypes, setSelectedFuelTypes] = useState<string[]>(
    () => {
      const ft = searchParams.get("fuelTypes");
      return ft ? ft.split(",") : DEFAULT_FUEL_TYPES;
    },
  );
  const [selectedDrilldownSegment, setSelectedDrilldownSegment] = useState(
    () => searchParams.get("drilldownSegment") || "SUV A0",
  );
  const [rankingLimit, setRankingLimit] = useState(
    () => {
      const rl = searchParams.get("rankingLimit");
      return normalizeMarketScanRankingLimit(rl);
    },
  );
  const [reloadToken, setReloadToken] = useState(0);
  const requestRef = useRef(0);
  const slideRef = useRef<HTMLDivElement | null>(null);

  // Sync filter state back to URL search params
  const syncUrlParams = useCallback(() => {
    const params = new URLSearchParams();
    if (selectedCountry) params.set("country", selectedCountry);
    if (selectedPeriod) params.set("period", selectedPeriod);
    if (activePage !== "overview") params.set("activePage", activePage);
    if (selectedDrilldownSegment !== "SUV A0") params.set("drilldownSegment", selectedDrilldownSegment);
    if (rankingLimit !== MIN_MARKET_SCAN_RANKING_LIMIT) params.set("rankingLimit", String(rankingLimit));
    const ft = selectedFuelTypes.slice().sort().join(",");
    const defaultFt = DEFAULT_FUEL_TYPES.slice().sort().join(",");
    if (ft !== defaultFt) params.set("fuelTypes", selectedFuelTypes.join(","));
    setSearchParams(params, { replace: true });
  }, [activePage, rankingLimit, selectedCountry, selectedDrilldownSegment, selectedFuelTypes, selectedPeriod, setSearchParams]);

  useEffect(() => {
    syncUrlParams();
  }, [syncUrlParams]);

  useEffect(() => {
    preloadPlotlyChartRuntime().catch(() => undefined);
  }, []);

  useEffect(() => {
    const requestId = ++requestRef.current;
    setLoading(true);
    setError("");

    api.marketScanDeck({
      country: selectedCountry || undefined,
      target_period: selectedPeriod || undefined,
      fuel_types: selectedFuelTypes,
      trend_window_months: 24,
      origin_window_months: 24,
      body_window_months: 24,
      ranking_limit: normalizeMarketScanRankingLimit(rankingLimit),
      drilldown_segment: selectedDrilldownSegment || undefined,
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
  }, [rankingLimit, reloadToken, selectedCountry, selectedDrilldownSegment, selectedFuelTypes, selectedPeriod]);

  useEffect(() => {
    if (!deck) {
      return;
    }

    if (
      selectedCountry
      && !deck.metadata.availableCountries.some((option) => option.value === selectedCountry)
    ) {
      setSelectedCountry(deck.metadata.selectedCountry);
    }

    if (
      selectedPeriod
      && !deck.metadata.availablePeriods.some((option) => option.value === selectedPeriod)
    ) {
      setSelectedPeriod(deck.metadata.resolvedPeriod);
    }

    if (
      selectedDrilldownSegment
      && !deck.metadata.availableSegments.some(
        (option) => option.value === selectedDrilldownSegment,
      )
    ) {
      setSelectedDrilldownSegment(deck.metadata.selectedDrilldownSegment);
    }

    const availableFuelSet = new Set(deck.metadata.availableFuelTypes);
    const normalizedFuelTypes = selectedFuelTypes.filter((fuel) => availableFuelSet.has(fuel));
    if (normalizedFuelTypes.length !== selectedFuelTypes.length && deck.metadata.selectedFuelTypes.length > 0) {
      setSelectedFuelTypes(deck.metadata.selectedFuelTypes);
    }
  }, [deck, selectedCountry, selectedDrilldownSegment, selectedFuelTypes, selectedPeriod]);

  const currentCountry = selectedCountry ?? deck?.metadata.selectedCountry ?? "";
  const currentPeriod = selectedPeriod ?? deck?.metadata.resolvedPeriod ?? "";
  const fuelOptions = deck?.metadata.availableFuelTypes ?? selectedFuelTypes;
  const activeFuelTypes = selectedFuelTypes.length > 0
    ? selectedFuelTypes
    : (deck?.metadata.selectedFuelTypes ?? DEFAULT_FUEL_TYPES);
  const showDataLabels = exportSettings.dataLabelMode !== "off";
  const labelDigits = Math.max(0, Math.min(2, exportSettings.decimalPlaces || 1));
  const heroMetrics = deck ? buildHeroMetrics(deck, activePage) : [];
  const narrative = deck ? pageNarrative(deck, activePage) : "按国家、月份与动力组合切换市场扫描页。";
  const activeTab = TAB_ITEMS.find((item) => item.key === activePage) ?? TAB_ITEMS[0];

  function toggleFuel(fuel: string) {
    setSelectedFuelTypes((current) => {
      if (current.includes(fuel)) {
        return current.length > 1 ? current.filter((item) => item !== fuel) : current;
      }
      return [...current, fuel];
    });
  }

  function renderActivePageContent(compact = false) {
    if (!deck) {
      return null;
    }
    if (activePage === "overview") {
      return (
        <OverviewSection
          labels={deck.metadata.labels}
          page={deck.results.overview}
          fuelOrder={deck.metadata.selectedFuelTypes}
          showDataLabels={showDataLabels}
          compact={compact}
        />
      );
    }
    if (activePage === "origin") {
      return (
        <OriginSection
          page={deck.results.origin}
          showDataLabels={showDataLabels}
          compact={compact}
        />
      );
    }
    if (activePage === "segment") {
      return (
        <SegmentSection
          page={deck.results.segment}
          showDataLabels={showDataLabels}
          labelDigits={labelDigits}
          compact={compact}
        />
      );
    }
    if (activePage === "drilldown") {
      return (
        <DrilldownSection
          page={deck.results.drilldown}
          fuelOrder={deck.metadata.selectedFuelTypes}
          showDataLabels={showDataLabels}
          compact={compact}
          rankingLimit={rankingLimit}
          onRankingLimitChange={setRankingLimit}
        />
      );
    }
    if (activePage === "suvA") {
      return (
        <DrilldownSection
          page={deck.results.suvA}
          fuelOrder={deck.metadata.selectedFuelTypes}
          showDataLabels={showDataLabels}
          compact={compact}
          rankingLimit={rankingLimit}
          onRankingLimitChange={setRankingLimit}
        />
      );
    }
    return (
      <DrilldownSection
        page={deck.results.suvB}
        fuelOrder={deck.metadata.selectedFuelTypes}
        showDataLabels={showDataLabels}
        compact={compact}
        rankingLimit={rankingLimit}
        onRankingLimitChange={setRankingLimit}
      />
    );
  }

  async function handleExportSlide() {
    if (!slideRef.current || !deck) {
      return;
    }
    try {
      setExportError("");
      setExportingSlide(true);
      const exportWidth = Math.max(960, exportSettings.exportWidth || 1920);
      const exportHeight = Math.max(540, exportSettings.exportHeight || 1080);
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
        backgroundColor: exportSettings.paperBg || "#eef4f7",
        width: exportWidth,
        height: exportHeight,
        canvasWidth: exportWidth,
        canvasHeight: exportHeight,
        style: {
          width: `${exportWidth}px`,
          height: `${exportHeight}px`,
        },
      });
      const link = document.createElement("a");
      link.href = dataUrl;
      link.download = [
        "market-scan",
        sanitizeFileNameSegment(deck.metadata.selectedCountryLabel),
        deck.metadata.resolvedPeriod,
        activeTab.key,
      ].join("-") + ".png";
      link.click();
    } catch (reason) {
      setExportError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setExportingSlide(false);
    }
  }

  return (
    <div className="market-scan-shell">
      <div className="market-scan-main">
        <CollapsibleDeckHero
          collapsed={heroCollapsed}
          onToggle={() => setHeroCollapsed((current) => !current)}
          expandedLabel="展开市场扫描控制区"
          collapsedLabel="收起市场扫描控制区"
          expandedTitle="展开市场扫描控制区"
          collapsedTitle="收起市场扫描控制区"
          className="header-card dashboard-hero market-scan-hero"
          shellClassName="dashboard-hero-shell market-scan-hero-shell"
          head={(
            <div className="dashboard-hero-copy market-scan-hero-copy">
              <span className="page-kicker">Market Scan</span>
              <h1>{deck?.metadata.labels.pageTitle ?? "Market Scan Deck"}</h1>
              <p>{narrative}</p>
              <div className="market-scan-hero-ribbon">
                <span className="market-scan-hero-chip">
                  国家 {deck?.metadata.selectedCountryLabel ?? "Sweden"}
                </span>
                <span className="market-scan-hero-chip">
                  月份 {deck?.metadata.labels.currentMonthShort ?? "Latest"}
                </span>
                <span className="market-scan-hero-chip">
                  动力 {activeFuelTypes.join(" / ")}
                </span>
                <span className="market-scan-hero-chip">
                  下钻 {deck?.metadata.selectedDrilldownSegment ?? selectedDrilldownSegment}
                </span>
                {loading && deck ? (
                  <span className="market-scan-hero-chip market-scan-hero-chip--live">Refreshing</span>
                ) : null}
              </div>
            </div>
          )}
          body={(
            <div className="market-scan-hero-body-grid">
              <div className="market-scan-controls-grid">
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
                    {(deck?.metadata.availablePeriods ?? []).map((period) => (
                      <option key={period.value} value={period.value}>
                        {period.label}
                      </option>
                    ))}
                  </select>
                </label>

                <label className="market-scan-field">
                  <span>Drilldown</span>
                  <select
                    value={selectedDrilldownSegment}
                    onChange={(event) => setSelectedDrilldownSegment(event.target.value)}
                    disabled={!deck}
                  >
                    {(deck?.metadata.availableSegments ?? []).map((segment) => (
                      <option key={segment.value} value={segment.value}>
                        {segment.label}
                      </option>
                    ))}
                  </select>
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
                      className="btn btn-ghost btn-sm"
                      onClick={() => {
                        setSelectedCountry(null);
                        setSelectedPeriod(null);
                        setSelectedFuelTypes(DEFAULT_FUEL_TYPES);
                        setSelectedDrilldownSegment("SUV A0");
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

              <div className="market-scan-hero-metrics">
                {heroMetrics.map((metric) => (
                  <MetricCard
                    key={`${metric.label}-${metric.detail}`}
                    label={metric.label}
                    value={metric.value}
                    detail={metric.detail}
                    tone={metric.tone}
                  />
                ))}
              </div>
            </div>
          )}
        />

        <nav className="market-scan-tab-strip" aria-label="Market Scan Pages">
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
            <strong>Market Scan 加载失败</strong>
            <p>{error}</p>
            <button
              type="button"
              className="btn btn-secondary btn-sm"
              onClick={() => setReloadToken((value) => value + 1)}
            >
              重试
            </button>
          </section>
        ) : null}

        {loading && !deck ? <MarketScanDeckSkeleton /> : null}

        {exportError ? (
          <section className="market-scan-state-card market-scan-state-card--error">
            <strong>PNG 导出失败</strong>
            <p>{exportError}</p>
          </section>
        ) : null}

        {deck ? (
          <div className="market-scan-content" aria-busy={loading}>
            {loading ? (
              <div className="market-scan-refresh-layer">
                <LoadingSurface
                  mode="overlay"
                  kicker="Refreshing"
                  label="正在刷新当前 Market Scan 结果"
                  detail="已命中相同条件时会优先走后端缓存。"
                />
              </div>
            ) : null}
            <div className="market-scan-slide-shell">
              <div
                ref={slideRef}
                className={`market-scan-slide-frame market-scan-slide-frame--${activePage}${exportingSlide ? " is-exporting" : ""}`}
              >
                <header className="market-scan-slide-head">
                  <div className="market-scan-slide-copy">
                    <span className="market-scan-slide-kicker">{activeTab.code} {activeTab.label}</span>
                    <h2>{deck.metadata.labels.pageTitle}</h2>
                    <p>{narrative}</p>
                  </div>
                  <div className="market-scan-slide-meta">
                    <span className="market-scan-slide-tag">国家 {deck.metadata.selectedCountryLabel}</span>
                    <span className="market-scan-slide-tag">月份 {deck.metadata.labels.currentMonthShort}</span>
                    <span className="market-scan-slide-tag">动力 {activeFuelTypes.join(" / ")}</span>
                    <span className="market-scan-slide-tag">下钻 {deck.metadata.selectedDrilldownSegment}</span>
                  </div>
                </header>
                <div className="market-scan-slide-body">
                  {heroMetrics.length > 0 ? (
                    <div className="market-scan-metric-grid market-scan-metric-grid--slide">
                      {heroMetrics.map((metric) => (
                        <MetricCard
                          key={`slide-${metric.label}-${metric.detail}`}
                          label={metric.label}
                          value={metric.value}
                          detail={metric.detail}
                          tone={metric.tone}
                        />
                      ))}
                    </div>
                  ) : null}
                  <div className="market-scan-slide-content">
                    {renderActivePageContent(true)}
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
                <span>导出当前页 / 导出图设置</span>
                <span>{exportToolsOpen ? "收起" : "展开"}</span>
              </button>
              {exportToolsOpen ? (
                <div className="market-scan-toolbar market-scan-toolbar--bottom">
                  <div className="market-scan-toolbar-group market-scan-toolbar-group--settings">
                    <button
                      type="button"
                      className="btn btn-primary btn-sm"
                      onClick={() => { void handleExportSlide(); }}
                      disabled={exportingSlide}
                    >
                      {exportingSlide ? "正在导出 PNG..." : "导出当前页 PNG"}
                    </button>
                    <ExportPanel
                      value={exportSettings}
                      onChange={setExportSettings}
                      labelModeOptions={marketScanLabelModeOptions}
                      showExportButton={false}
                    />
                  </div>
                  <div className="market-scan-toolbar-meta">
                    <span className="market-scan-toolbar-chip">Slide Layout</span>
                    <span className="market-scan-toolbar-chip">{exportSettings.exportWidth} x {exportSettings.exportHeight}</span>
                    <span className="market-scan-toolbar-chip">标签 {showDataLabels ? "On" : "Off"}</span>
                    <span className="market-scan-toolbar-chip">{activeTab.label}</span>
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
