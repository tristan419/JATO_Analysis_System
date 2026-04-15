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

import type { CountryChatSnapshot } from "../types";

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

/* ------------------------------------------------------------------ */
/*  Intent → chart selection                                          */
/* ------------------------------------------------------------------ */

export function ChatInlineCharts({
  snapshot,
  intents,
}: {
  snapshot: CountryChatSnapshot;
  intents?: string[];
}) {
  const charts = useMemo(() => {
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

    return result;
  }, [snapshot, intents]);

  if (charts.length === 0) return null;

  return <div className="ccw-inline-charts">{charts}</div>;
}
