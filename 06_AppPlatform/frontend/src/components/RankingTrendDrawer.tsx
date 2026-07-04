import { useEffect, useState, useMemo } from "react";
import type { RankingTrendResponse } from "../types";
import { api } from "../api/client";
import { LazyPlotlyChart as PlotlyChart } from "./LazyPlotlyChart";
import { LoadingSurface } from "./LoadingSurface";

type SortMode = "sales" | "growth";

interface Props {
  open: boolean;
  brand: string;
  model?: string;
  sourceTable: string;
  country: string;
  segment?: string;
  fuelTypes?: string[];
  onClose: () => void;
  onBack?: () => void;
  onModelClick?: (model: string) => void;
}

function formatSales(n: number): string {
  return n >= 1000 ? `${(n / 1000).toFixed(1)}k` : String(Math.round(n));
}

function formatGrowth(n: number): string {
  if (n === 0) return "0%";
  return `${n > 0 ? "+" : ""}${n.toFixed(1)}%`;
}

export function RankingTrendPopover({
  open, brand, model, sourceTable, country, segment,
  fuelTypes, onClose, onBack, onModelClick,
}: Props) {
  const [data, setData] = useState<RankingTrendResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [sortBy, setSortBy] = useState<SortMode>("sales");

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") onClose(); };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  useEffect(() => {
    if (!open || !brand) return;
    setLoading(true);
    setError("");
    const params: Record<string, string> = { country, brand, source_table: sourceTable, sort_by: sortBy };
    if (model) params.model = model;
    if (segment) params.segment = segment;
    if (fuelTypes?.length) params.fuel_types = fuelTypes.join(",");
    api.rankingTrend(params)
      .then(setData)
      .catch((e: Error) => setError(e.message))
      .finally(() => setLoading(false));
  }, [open, brand, model, sourceTable, country, segment, fuelTypes, sortBy]);

  // Use models[] (all) if available, fall back to topModels[] (legacy)
  const allModels = useMemo(() => data?.models ?? data?.topModels ?? [], [data]);

  if (!open) return null;

  const trend = data?.trend ?? [];
  const summary = data?.summary;
  const chartData = trend.length > 0 ? [{
    type: "scatter" as const,
    mode: "lines+markers" as const,
    x: trend.map((t) => `${t.month}-01`),
    y: trend.map((t) => t.sales),
    name: "Sales",
    line: { color: "#1c69d4", width: 2 },
    marker: { size: 5, color: "#1c69d4" },
  }] : [];

  return (
    <div className="ranking-popover-backdrop" onClick={onClose}>
      <div className="ranking-popover" onClick={(e) => e.stopPropagation()}>
        <header className="ranking-popover-head">
          <div className="ranking-popover-head-left">
            {onBack ? (
              <button type="button" className="ranking-popover-back" onClick={onBack} title="返回">←</button>
            ) : null}
            <div>
              <span className="ranking-popover-title">
                {model ? `${brand} ${model}` : brand}
              </span>
              <span className="ranking-popover-subtitle">
                {data?.context.country}{segment ? ` · ${segment}` : ""}{" · "}
                {sourceTable === "ytd_brand_ranking" ? "YTD" : "Monthly"}
              </span>
            </div>
          </div>
          <button type="button" className="ranking-popover-close" onClick={onClose}>×</button>
        </header>

        <div className="ranking-popover-body">
          {loading ? (
            <LoadingSurface mode="inline" kicker="Trend" label="加载趋势中" />
          ) : error ? (
            <div className="market-scan-state-card market-scan-state-card--error">
              <strong>加载失败</strong><p>{error}</p>
            </div>
          ) : (
            <>
              {summary ? (
                <div className="ranking-popover-summary">
                  <span>当月 <strong>{formatSales(summary.currentMonthSales)}</strong></span>
                  <span>YTD <strong>{formatSales(summary.ytdSales)}</strong></span>
                  <span>Share <strong>{(summary.marketShare * 100).toFixed(1)}%</strong></span>
                </div>
              ) : null}

              {chartData.length > 0 ? (
                <div className="ranking-popover-chart">
                  <PlotlyChart
                    data={chartData}
                    layout={{
                      margin: { l: 40, r: 8, t: 4, b: 36 },
                      xaxis: { type: "date", tickformat: "%y.%m", tickangle: -45, nticks: 8 },
                      yaxis: { title: { text: "" } },
                      height: 200,
                      paper_bgcolor: "transparent",
                      plot_bgcolor: "transparent",
                    }}
                    config={{ displayModeBar: false, responsive: true }}
                  />
                </div>
              ) : trend.length === 0 && !loading ? (
                <div className="market-scan-empty">无趋势数据</div>
              ) : null}

              {allModels.length > 0 && !model ? (
                <div className="ranking-popover-models">
                  <div className="ranking-popover-models-head">
                    <span className="ranking-popover-section-label">
                      Models ({allModels.length})
                    </span>
                    <div className="ranking-popover-sort">
                      <button
                        type="button"
                        className={`ranking-popover-sort-chip${sortBy === "sales" ? " is-active" : ""}`}
                        onClick={() => setSortBy("sales")}
                      >
                        销量
                      </button>
                      <button
                        type="button"
                        className={`ranking-popover-sort-chip${sortBy === "growth" ? " is-active" : ""}`}
                        onClick={() => setSortBy("growth")}
                      >
                        增长
                      </button>
                    </div>
                  </div>
                  <div className="ranking-popover-models-list">
                    {allModels.map((m) => (
                      <button key={m.model} type="button"
                        className="ranking-popover-model-chip"
                        onClick={() => onModelClick?.(m.model)}>
                        <span className="ranking-popover-model-chip-name">{m.model}</span>
                        <span className="ranking-popover-model-chip-stats">
                          <span className="ranking-popover-model-chip-sales">
                            {formatSales(m.sales)} · {(m.shareWithinBrand * 100).toFixed(0)}%
                          </span>
                          {sortBy === "growth" && (
                            <span className={`ranking-popover-model-chip-growth${(m.growth ?? 0) > 0 ? " is-positive" : (m.growth ?? 0) < 0 ? " is-negative" : ""}`}>
                              {formatGrowth(m.growth ?? 0)}
                            </span>
                          )}
                        </span>
                      </button>
                    ))}
                  </div>
                </div>
              ) : null}
            </>
          )}
        </div>
      </div>
    </div>
  );
}
