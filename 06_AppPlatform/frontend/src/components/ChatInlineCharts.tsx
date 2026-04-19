import React, { useMemo } from "react";
import {
  BarChart,
  Bar,
  LineChart,
  Line,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

import type { CountryChatRenderHint, CountryChatSnapshot } from "../types/countryChat";

/* ------------------------------------------------------------------ */
/*  Palette                                                           */
/* ------------------------------------------------------------------ */

const PALETTE = [
  "#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
  "#ec4899", "#06b6d4", "#84cc16", "#f97316", "#6366f1",
];

/* ------------------------------------------------------------------ */
/*  Shared tooltip style                                              */
/* ------------------------------------------------------------------ */

const tooltipStyle = {
  contentStyle: {
    fontSize: 11,
    borderRadius: 6,
    background: "rgba(255,255,255,0.92)",
    border: "1px solid #e2e8f0",
    backdropFilter: "blur(8px)",
    boxShadow: "0 4px 12px rgba(0,0,0,0.08)",
  },
  labelStyle: { fontSize: 11, fontWeight: 600 },
};

/* ------------------------------------------------------------------ */
/*  Mini chart cards                                                  */
/* ------------------------------------------------------------------ */

function MiniTrendChart({
  data,
  title,
}: {
  data: { time: string; value: number }[];
  title: string;
}) {
  if (!data || data.length < 2) return null;
  return (
    <div className="ccw-inline-chart">
      <div className="ccw-inline-chart-title">{title}</div>
      <ResponsiveContainer width="100%" height={100}>
        <LineChart data={data} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <XAxis
            dataKey="time"
            tick={{ fontSize: 9 }}
            tickLine={false}
            axisLine={false}
            interval="preserveStartEnd"
          />
          <Tooltip
            {...tooltipStyle}
            formatter={(v) => [Number(v).toLocaleString(), "销量"]}
          />
          <Line
            type="monotone"
            dataKey="value"
            stroke="#3b82f6"
            strokeWidth={2}
            dot={false}
            activeDot={{ r: 3 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}

function MiniBrandBar({
  data,
  title,
}: {
  data: { label: string; value: number }[];
  title: string;
}) {
  if (!data || data.length === 0) return null;
  const top = data.slice(0, 8);
  return (
    <div className="ccw-inline-chart">
      <div className="ccw-inline-chart-title">{title}</div>
      <ResponsiveContainer width="100%" height={Math.max(80, top.length * 22 + 8)}>
        <BarChart
          data={top}
          layout="vertical"
          margin={{ top: 2, right: 8, bottom: 0, left: 0 }}
        >
          <XAxis type="number" hide />
          <YAxis
            type="category"
            dataKey="label"
            tick={{ fontSize: 10 }}
            tickLine={false}
            axisLine={false}
            width={60}
          />
          <Tooltip
            {...tooltipStyle}
            formatter={(v) => [Number(v).toLocaleString(), "销量"]}
          />
          <Bar dataKey="value" radius={[0, 3, 3, 0]} barSize={14}>
            {top.map((_, i) => (
              <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
            ))}
          </Bar>
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}

function MiniPieChart({
  data,
  title,
}: {
  data: { label: string; value: number }[];
  title: string;
}) {
  if (!data || data.length === 0) return null;
  const top = data.slice(0, 6);
  return (
    <div className="ccw-inline-chart">
      <div className="ccw-inline-chart-title">{title}</div>
      <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
        <ResponsiveContainer width={90} height={90}>
          <PieChart>
            <Pie
              data={top}
              dataKey="value"
              nameKey="label"
              cx="50%"
              cy="50%"
              outerRadius={38}
              innerRadius={18}
              strokeWidth={1}
            >
              {top.map((_, i) => (
                <Cell key={i} fill={PALETTE[i % PALETTE.length]} />
              ))}
            </Pie>
            <Tooltip
              {...tooltipStyle}
              formatter={(v) => [Number(v).toLocaleString(), "销量"]}
            />
          </PieChart>
        </ResponsiveContainer>
        <div className="ccw-inline-legend">
          {top.map((d, i) => (
            <div key={d.label} className="ccw-inline-legend-item">
              <span
                className="ccw-inline-legend-dot"
                style={{ background: PALETTE[i % PALETTE.length] }}
              />
              <span>{d.label}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function MiniSummaryCard({ title, lines }: { title: string; lines: string[] }) {
  if (lines.length === 0) return null;
  return (
    <div className="ccw-inline-chart">
      <div className="ccw-inline-chart-title">{title}</div>
      <div className="ccw-inline-summary-card">
        {lines.slice(0, 3).map((line) => (
          <div key={line} className="ccw-inline-summary-line">{line}</div>
        ))}
      </div>
    </div>
  );
}

function MiniPositioningCard({ snapshot, title }: { snapshot: CountryChatSnapshot; title: string }) {
  const positioningMap = snapshot.positioningMap;
  if (!positioningMap) return null;
  const target = positioningMap.target ?? undefined;
  const nearby = Array.isArray(positioningMap.items) ? positioningMap.items.slice(0, 3) : [];
  const peerCorridor = positioningMap.peerCorridor ?? null;
  const lines: string[] = [];
  if (target) {
    const length = Number(target.Length ?? 0);
    const msrp = Number(target.MSRP ?? 0);
    if (length > 0 || msrp > 0) {
      lines.push(`目标 ${length > 0 ? `${Math.round(length)}mm` : "-"} / ${msrp > 0 ? msrp.toLocaleString("en-US") : "-"}`);
    }
  }
  if (peerCorridor) {
    const p25 = Number(peerCorridor.msrpP25 ?? 0);
    const median = Number(peerCorridor.msrpMedian ?? 0);
    const p75 = Number(peerCorridor.msrpP75 ?? 0);
    const stanceLabel = String(peerCorridor.stanceLabel ?? "").trim();
    if (stanceLabel) {
      lines.push(`姿态 ${stanceLabel}`);
    }
    if (p25 > 0 || p75 > 0) {
      lines.push(`Peer ${p25 > 0 ? p25.toLocaleString("en-US") : "-"} - ${p75 > 0 ? p75.toLocaleString("en-US") : "-"} · 中位 ${median > 0 ? median.toLocaleString("en-US") : "-"}`);
    }
  }
  nearby.forEach((item) => {
    const brand = String(item.Brand ?? "").trim();
    const model = String(item.Model ?? "").trim();
    const msrp = Number(item.MSRP ?? 0);
    if (brand || model) {
      lines.push(`${[brand, model].filter(Boolean).join(" ")} · ${msrp > 0 ? msrp.toLocaleString("en-US") : "-"}`);
    }
  });
  return <MiniSummaryCard title={title} lines={lines} />;
}

function MiniSegmentFuelCard({ snapshot, title }: { snapshot: CountryChatSnapshot; title: string }) {
  const lookup = snapshot.segmentFuelLookup as {
    resolvedSegmentLabel?: string;
    fuelType?: string;
    fuelRanking?: Array<{ model?: string; volume?: number; shareDisplay?: string }>;
  } | undefined;
  if (!lookup || !Array.isArray(lookup.fuelRanking) || lookup.fuelRanking.length === 0) {
    return null;
  }
  const lines: string[] = [];
  const header = [lookup.resolvedSegmentLabel, lookup.fuelType].filter(Boolean).join(" · ");
  if (header) {
    lines.push(header);
  }
  lookup.fuelRanking.slice(0, 3).forEach((item) => {
    const model = String(item.model ?? "").trim();
    if (!model) {
      return;
    }
    const volume = Number(item.volume ?? 0);
    const share = String(item.shareDisplay ?? "").trim();
    lines.push(`${model} · ${volume > 0 ? volume.toLocaleString("en-US") : "-"}${share ? ` · ${share}` : ""}`);
  });
  return <MiniSummaryCard title={title} lines={lines} />;
}

function MiniNewsDigestCard({ snapshot, title }: { snapshot: CountryChatSnapshot; title: string }) {
  const digest = snapshot.newsDigest;
  if (!digest) return null;
  const lines = [digest.headline, digest.summary].filter((item): item is string => Boolean(item)).slice(0, 2);
  return <MiniSummaryCard title={title} lines={lines} />;
}

function MiniMarketScanScopeCard({ snapshot, title }: { snapshot: CountryChatSnapshot; title: string }) {
  const scope = snapshot.marketScanScope;
  if (!scope || !Array.isArray(scope.totalRanking) || scope.totalRanking.length === 0) {
    return null;
  }
  const lines: string[] = [];
  if (scope.resolvedSegmentLabel) {
    lines.push(`${scope.resolvedSegmentLabel} · ${scope.pageKey}`);
  }
  scope.totalRanking.slice(0, 3).forEach((item) => {
    const model = String(item.model ?? "").trim();
    if (!model) {
      return;
    }
    const volume = Number(item.volume ?? 0);
    const share = String(item.shareDisplay ?? "").trim();
    lines.push(`${model} · ${volume > 0 ? volume.toLocaleString("en-US") : "-"}${share ? ` · ${share}` : ""}`);
  });
  return <MiniSummaryCard title={title} lines={lines} />;
}

function MiniModelPerformanceCard({ snapshot, title }: { snapshot: CountryChatSnapshot; title: string }) {
  const performance = snapshot.marketScanScope?.modelPerformance;
  if (!performance || !performance.model) {
    return null;
  }
  const lines: string[] = [];
  if (performance.rank || performance.shareDisplay || performance.yoyDisplay) {
    lines.push(
      [
        performance.rank ? `第${performance.rank}` : "",
        performance.shareDisplay ? `份额 ${performance.shareDisplay}` : "",
        performance.yoyDisplay ? `同比 ${performance.yoyDisplay}` : "",
      ].filter(Boolean).join(" · "),
    );
  }
  if (Array.isArray(performance.channelMix) && performance.channelMix.length > 0) {
    lines.push(
      `渠道 ${performance.channelMix
        .slice(0, 2)
        .map((item) => `${item.label} ${item.sharePct.toFixed(1)}%`)
        .join(" / ")}`,
    );
  }
  if (performance.awdShareDisplay) {
    lines.push(`4WD ${performance.awdShareDisplay}`);
  }
  if (Array.isArray(performance.bodyStyleDistribution) && performance.bodyStyleDistribution.length > 0) {
    lines.push(
      `车身 ${performance.bodyStyleDistribution
        .slice(0, 2)
        .map((item) => `${item.label} ${item.sharePct.toFixed(1)}%`)
        .join(" / ")}`,
    );
  }
  return <MiniSummaryCard title={title} lines={lines.filter(Boolean)} />;
}

function MiniModelVersionMixChart({ snapshot, title }: { snapshot: CountryChatSnapshot; title: string }) {
  const performance = snapshot.marketScanScope?.modelPerformance;
  const distribution = Array.isArray(performance?.versionDistribution)
    ? performance.versionDistribution
      .filter((item) => item && item.label && Number(item.value) > 0)
      .map((item) => ({ label: item.label, value: Number(item.value) }))
    : [];
  if (distribution.length === 0) {
    return null;
  }
  return <MiniBrandBar data={distribution.slice(0, 4)} title={title} />;
}

/* ------------------------------------------------------------------ */
/*  Intent → chart selection                                          */
/* ------------------------------------------------------------------ */

export function ChatInlineCharts({
  snapshot,
  intents,
  renderHints,
  compact = false,
}: {
  snapshot: CountryChatSnapshot;
  intents?: string[];
  renderHints?: CountryChatRenderHint[];
  compact?: boolean;
}) {
  const charts = useMemo(() => {
    const hintedCharts: React.ReactElement[] = [];
    const normalizedHints = Array.isArray(renderHints) ? renderHints : [];
    if (normalizedHints.length > 0) {
      normalizedHints.forEach((hint) => {
        switch (hint.kind) {
          case "brands-bar":
            if (snapshot.topBrands?.length > 0) {
              hintedCharts.push(
                <MiniBrandBar key={hint.kind} data={snapshot.topBrands} title={hint.title} />,
              );
            }
            break;
          case "monthly-trend":
            if (snapshot.monthSeries?.length > 2) {
              hintedCharts.push(
                <MiniTrendChart key={hint.kind} data={snapshot.monthSeries} title={hint.title} />,
              );
            }
            break;
          case "powertrain-pie":
            if (snapshot.powertrainMix?.length > 0) {
              hintedCharts.push(
                <MiniPieChart key={hint.kind} data={snapshot.powertrainMix} title={hint.title} />,
              );
            }
            break;
          case "positioning-summary":
            hintedCharts.push(
              <MiniPositioningCard key={hint.kind} snapshot={snapshot} title={hint.title} />,
            );
            break;
          case "segment-fuel-summary":
            hintedCharts.push(
              <MiniSegmentFuelCard key={hint.kind} snapshot={snapshot} title={hint.title} />,
            );
            break;
          case "news-digest":
            hintedCharts.push(
              <MiniNewsDigestCard key={hint.kind} snapshot={snapshot} title={hint.title} />,
            );
            break;
          case "market-scan-summary":
            hintedCharts.push(
              <MiniMarketScanScopeCard key={hint.kind} snapshot={snapshot} title={hint.title} />,
            );
            break;
          case "model-performance-summary":
            hintedCharts.push(
              <MiniModelPerformanceCard key={hint.kind} snapshot={snapshot} title={hint.title} />,
            );
            break;
          case "model-version-mix":
            hintedCharts.push(
              <MiniModelVersionMixChart key={hint.kind} snapshot={snapshot} title={hint.title} />,
            );
            break;
          default:
            break;
        }
      });
    }
    const compactHintedCharts = hintedCharts.filter(Boolean).slice(0, compact ? 2 : 3);
    if (compactHintedCharts.length > 0) {
      return compactHintedCharts;
    }

    const intentSet = new Set(intents ?? []);
    const result: React.ReactElement[] = [];

    // Brand ranking → bar chart
    if (
      intentSet.has("brand-ranking") ||
      intentSet.has("competitive") ||
      intentSet.has("general-summary")
    ) {
      if (snapshot.topBrands?.length > 0) {
        result.push(
          <MiniBrandBar key="brands" data={snapshot.topBrands} title="品牌排名 TOP" />,
        );
      }
    }

    // Trend → line chart
    if (
      intentSet.has("trend-summary") ||
      intentSet.has("general-summary") ||
      intentSet.has("segment-analysis")
    ) {
      if (snapshot.monthSeries?.length > 2) {
        result.push(
          <MiniTrendChart key="trend" data={snapshot.monthSeries} title="月度销量趋势" />,
        );
      }
    }

    // Powertrain mix → pie chart
    if (
      intentSet.has("powertrain-mix") ||
      intentSet.has("nev-analysis") ||
      intentSet.has("general-summary")
    ) {
      if (snapshot.powertrainMix?.length > 0) {
        result.push(
          <MiniPieChart key="powertrain" data={snapshot.powertrainMix} title="动力类型分布" />,
        );
      }
    }

    // Fallback: if no specific intent matched, show trend + brands
    if (result.length === 0) {
      if (snapshot.monthSeries?.length > 2) {
        result.push(
          <MiniTrendChart key="trend-fb" data={snapshot.monthSeries} title="月度销量趋势" />,
        );
      }
      if (snapshot.topBrands?.length > 0) {
        result.push(
          <MiniBrandBar key="brands-fb" data={snapshot.topBrands} title="品牌排名 TOP" />,
        );
      }
    }

    return result.slice(0, compact ? 2 : 3);
  }, [compact, intents, renderHints, snapshot]);

  if (charts.length === 0) return null;

  return <div className="ccw-inline-charts">{charts}</div>;
}
